/*
 * Copyright 2026 Data Dynamics Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.trino.service;

import io.datadynamics.nifi.trino.api.TrinoQueryInfo;
import org.apache.nifi.logging.ComponentLog;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Trino 드라이버의 query id·통계를 <b>리플렉션(메서드 이름)</b>으로 읽는다.
 * <p>
 * 번들 드라이버와 외부 드라이버(다른 클래스로더의 {@code io.trino.jdbc.*})가 모두 동작하도록 클래스 동일성(unwrap 캐스팅)에
 * 의존하지 않는다. 쓰는 메서드: {@code TrinoResultSet.getStats()}, {@code TrinoStatement.setProgressMonitor(Consumer)},
 * {@code QueryStats.getQueryId/getState/getProcessedRows/getProcessedBytes/getCpuTimeMillis/getWallTimeMillis/getQueuedTimeMillis}.
 * 메서드가 없는 드라이버면 기능을 조용히 생략하고(query info = null) 경고를 한 번만 남긴다.
 * <p>
 * {@code TrinoStatement}는 업데이트 문장의 query id 를 직접 노출하지 않으므로, 서비스가 돌려주는 {@link Connection}을 프록시로 감싸
 * 생성되는 문장마다 progress monitor 를 등록하고 최신 통계를 보관한다(문장이 GC 되면 함께 사라짐).
 */
final class QueryStatsTracker {

    private static final String TRINO_RESULT_SET = "io.trino.jdbc.TrinoResultSet";
    private static final String TRINO_STATEMENT = "io.trino.jdbc.TrinoStatement";
    private static final String[] STATS_GETTERS = {"getQueryId", "getState", "getProcessedRows", "getProcessedBytes",
            "getCpuTimeMillis", "getWallTimeMillis", "getQueuedTimeMillis"};

    private final Map<Object, Object> lastStats = Collections.synchronizedMap(new WeakHashMap<>());
    private final Map<Class<?>, Optional<Method>> getStatsMethods = new ConcurrentHashMap<>();
    private final Map<Class<?>, Optional<Method>> progressMonitorMethods = new ConcurrentHashMap<>();
    private final Map<Class<?>, Optional<Method[]>> statsGetters = new ConcurrentHashMap<>();
    private final Class<?> trinoResultSetClass;
    private final Class<?> trinoStatementClass;
    private final ComponentLog logger;
    private final AtomicBoolean warned = new AtomicBoolean();

    /**
     * @param driverClassLoader 드라이버를 로드한 클래스로더(unwrap 대상 클래스 이름 조회용)
     */
    QueryStatsTracker(final ClassLoader driverClassLoader, final ComponentLog logger) {
        this.trinoResultSetClass = classOrNull(TRINO_RESULT_SET, driverClassLoader);
        this.trinoStatementClass = classOrNull(TRINO_STATEMENT, driverClassLoader);
        this.logger = logger;
    }

    Connection wrap(final Connection connection) {
        return (Connection) Proxy.newProxyInstance(QueryStatsTracker.class.getClassLoader(),
                new Class<?>[]{Connection.class}, new TrackingHandler(connection));
    }

    TrinoQueryInfo getQueryInfo(final ResultSet resultSet) {
        if (resultSet == null) {
            return null;
        }
        final Object target = resolve(resultSet, getStatsMethods, "getStats", trinoResultSetClass);
        if (target == null) {
            warnOnce("ResultSet.getStats()");
            return null;
        }
        try {
            return toInfo(getStatsMethods.get(target.getClass()).orElseThrow().invoke(target));
        } catch (final ReflectiveOperationException | RuntimeException e) {
            warnOnce("ResultSet.getStats() 호출 실패: " + e);
            return null;
        }
    }

    TrinoQueryInfo getQueryInfo(final Statement statement) {
        if (statement == null) {
            return null;
        }
        final Object target = resolve(statement, progressMonitorMethods, "setProgressMonitor", trinoStatementClass);
        if (target == null) {
            return null;
        }
        return toInfo(lastStats.get(target));
    }

    private void track(final Object statement) {
        if (!(statement instanceof Statement jdbcStatement)) {
            return;
        }
        final Object target = resolve(jdbcStatement, progressMonitorMethods, "setProgressMonitor", trinoStatementClass);
        if (target == null) {
            warnOnce("Statement.setProgressMonitor(Consumer)");
            return;
        }
        final Consumer<Object> monitor = stats -> lastStats.put(target, stats);
        try {
            progressMonitorMethods.get(target.getClass()).orElseThrow().invoke(target, monitor);
        } catch (final ReflectiveOperationException | RuntimeException e) {
            warnOnce("Statement.setProgressMonitor 호출 실패: " + e);
        }
    }

    /**
     * 객체 자신이 메서드를 가지면 그 객체, 아니면 Trino 클래스로 unwrap 한 객체. 둘 다 아니면 null.
     */
    private Object resolve(final java.sql.Wrapper wrapper, final Map<Class<?>, Optional<Method>> cache,
                           final String methodName, final Class<?> unwrapClass) {
        if (lookup(cache, wrapper.getClass(), methodName).isPresent()) {
            return wrapper;
        }
        if (unwrapClass == null) {
            return null;
        }
        try {
            if (wrapper.isWrapperFor(unwrapClass)) {
                final Object unwrapped = wrapper.unwrap(unwrapClass);
                if (unwrapped != null && lookup(cache, unwrapped.getClass(), methodName).isPresent()) {
                    return unwrapped;
                }
            }
        } catch (final SQLException | RuntimeException ignored) {
            // unwrap 불가
        }
        return null;
    }

    private static Optional<Method> lookup(final Map<Class<?>, Optional<Method>> cache, final Class<?> type, final String name) {
        return cache.computeIfAbsent(type, t -> {
            final Method method = findPublicMethod(t, name);
            return Optional.ofNullable(method);
        });
    }

    /**
     * public 클래스·인터페이스에 선언된 public 메서드를 찾는다(구현 클래스가 비공개여도 호출 가능하게).
     */
    private static Method findPublicMethod(final Class<?> type, final String name) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            if (!java.lang.reflect.Modifier.isPublic(current.getModifiers())) {
                continue;
            }
            for (final Method method : current.getDeclaredMethods()) {
                if (!method.getName().equals(name) || !java.lang.reflect.Modifier.isPublic(method.getModifiers())) {
                    continue;
                }
                final Class<?>[] parameters = method.getParameterTypes();
                if (name.equals("setProgressMonitor") ? parameters.length == 1 && parameters[0] == Consumer.class : parameters.length == 0) {
                    return method;
                }
            }
        }
        return null;
    }

    private TrinoQueryInfo toInfo(final Object stats) {
        if (stats == null) {
            return null;
        }
        final Optional<Method[]> getters = statsGetters.computeIfAbsent(stats.getClass(), type -> {
            final Method[] methods = new Method[STATS_GETTERS.length];
            for (int i = 0; i < STATS_GETTERS.length; i++) {
                methods[i] = findPublicMethod(type, STATS_GETTERS[i]);
                if (methods[i] == null) {
                    return Optional.empty();
                }
            }
            return Optional.of(methods);
        });
        if (getters.isEmpty()) {
            warnOnce("QueryStats getter");
            return null;
        }
        try {
            final Method[] m = getters.get();
            return new TrinoQueryInfo((String) m[0].invoke(stats), (String) m[1].invoke(stats),
                    ((Number) m[2].invoke(stats)).longValue(), ((Number) m[3].invoke(stats)).longValue(),
                    ((Number) m[4].invoke(stats)).longValue(), ((Number) m[5].invoke(stats)).longValue(),
                    ((Number) m[6].invoke(stats)).longValue());
        } catch (final ReflectiveOperationException | RuntimeException e) {
            warnOnce("QueryStats 읽기 실패: " + e);
            return null;
        }
    }

    private void warnOnce(final String missing) {
        if (logger != null && warned.compareAndSet(false, true)) {
            logger.warn("JDBC 드라이버에 Trino 진행 통계 API({})가 없거나 쓸 수 없어 query id·통계를 생략한다. 쿼리 실행에는 영향이 없다", missing);
        }
    }

    private static Class<?> classOrNull(final String name, final ClassLoader loader) {
        try {
            return Class.forName(name, false, loader);
        } catch (final ClassNotFoundException | LinkageError e) {
            return null;
        }
    }

    private final class TrackingHandler implements InvocationHandler {
        private final Connection target;

        private TrackingHandler(final Connection target) {
            this.target = target;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            final String name = method.getName();
            if (args == null && name.equals("hashCode")) {
                return System.identityHashCode(proxy);
            }
            if (args != null && args.length == 1 && name.equals("equals")) {
                return proxy == args[0];
            }
            if (args == null && name.equals("toString")) {
                return "TrackedConnection[" + target + "]";
            }
            final Object result;
            try {
                result = method.invoke(target, args);
            } catch (final InvocationTargetException e) {
                throw e.getCause();
            }
            if (name.equals("createStatement") || name.equals("prepareStatement") || name.equals("prepareCall")) {
                track(result);
            }
            return result;
        }
    }
}
