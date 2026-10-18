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

import io.trino.jdbc.NonRegisteringTrinoDriver;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * 서비스가 쓰는 JDBC 드라이버와(외부 jar 인 경우) 그 전용 클래스로더.
 * <ul>
 *   <li>위치 미지정 + Trino 드라이버 클래스명: 번들된 trino-jdbc 483 의 {@link NonRegisteringTrinoDriver}.</li>
 *   <li>위치 미지정 + 다른 클래스명: 서비스 NAR 클래스로더에서 로드.</li>
 *   <li>위치 지정: {@link ChildFirstClassLoader}에서 로드.</li>
 * </ul>
 * {@code io.trino.jdbc.TrinoDriver}는 static 초기화에서 DriverManager 에 전역 등록하므로, 같은 jar 에 부모 클래스
 * {@code io.trino.jdbc.NonRegisteringTrinoDriver}가 있으면 그것을 대신 인스턴스화한다(동작 동일, 등록 없음).
 */
final class DriverHandle implements Closeable {

    static final String NON_REGISTERING_TRINO_DRIVER = "io.trino.jdbc.NonRegisteringTrinoDriver";
    static final String TRINO_DRIVER = "io.trino.jdbc.TrinoDriver";
    static final Set<String> TRINO_DRIVER_CLASSES = Set.of(NON_REGISTERING_TRINO_DRIVER, TRINO_DRIVER);

    private final Driver driver;
    private final ChildFirstClassLoader classLoader;

    private DriverHandle(final Driver driver, final ChildFirstClassLoader classLoader) {
        this.driver = driver;
        this.classLoader = classLoader;
    }

    /**
     * 번들 드라이버.
     */
    static DriverHandle bundled(final Driver driver) {
        return new DriverHandle(driver, null);
    }

    /**
     * @param locations 외부 jar 위치(비어 있으면 서비스 클래스로더 사용)
     * @throws DriverLoadException 클래스를 찾을 수 없거나 Driver 가 아니거나 인스턴스화에 실패한 경우
     */
    static DriverHandle load(final String className, final List<URL> locations, final ClassLoader serviceClassLoader) {
        final ChildFirstClassLoader loader = locations.isEmpty() ? null
                : new ChildFirstClassLoader(locations.toArray(new URL[0]), serviceClassLoader);
        final ClassLoader effective = loader == null ? serviceClassLoader : loader;
        try {
            final Class<?> requested;
            try {
                requested = Class.forName(className, false, effective);
            } catch (final ClassNotFoundException | LinkageError e) {
                throw new DriverLoadException("드라이버 클래스 " + className + " 를 찾을 수 없다"
                        + (loader == null ? "(번들 클래스패스)" : "(지정한 드라이버 위치)") + ": " + e, e);
            }
            if (!Driver.class.isAssignableFrom(requested)) {
                throw new DriverLoadException(className + " 는 java.sql.Driver 구현이 아니다");
            }
            Class<?> toInstantiate = requested;
            if (TRINO_DRIVER.equals(className)) {
                final Class<?> parent = requested.getSuperclass();
                if (parent != null && NON_REGISTERING_TRINO_DRIVER.equals(parent.getName())) {
                    toInstantiate = parent;
                }
            }
            try {
                final Constructor<?> constructor = toInstantiate.getDeclaredConstructor();
                constructor.setAccessible(true);
                final Driver driver = (Driver) constructor.newInstance();
                return new DriverHandle(driver, loader);
            } catch (final ReflectiveOperationException | LinkageError | RuntimeException e) {
                throw new DriverLoadException(className + " 인스턴스를 만들 수 없다: " + e, e);
            }
        } catch (final DriverLoadException e) {
            closeQuietly(loader);
            throw e;
        }
    }

    Driver driver() {
        return driver;
    }

    /** 외부 jar 클래스로더(번들 드라이버면 null). */
    ChildFirstClassLoader classLoader() {
        return classLoader;
    }

    boolean isExternal() {
        return classLoader != null;
    }

    /**
     * @throws DriverLoadException 드라이버가 URL 을 받아들이지 않는 경우
     */
    void requireAcceptsUrl(final String url) {
        final boolean accepts;
        try {
            accepts = driver.acceptsURL(url);
        } catch (final SQLException e) {
            throw new DriverLoadException("드라이버 acceptsURL 실패: " + e.getMessage(), e);
        }
        if (!accepts) {
            throw new DriverLoadException(driver.getClass().getName() + " 가 URL 을 받아들이지 않는다: " + url);
        }
    }

    @Override
    public void close() {
        if (driver instanceof Closeable closeable) {
            try {
                closeable.close();
            } catch (final IOException | RuntimeException ignored) {
                // 종료 실패는 무시
            }
        }
        closeQuietly(classLoader);
    }

    private static void closeQuietly(final ChildFirstClassLoader loader) {
        if (loader != null) {
            try {
                loader.close();
            } catch (final IOException ignored) {
                // 무시
            }
        }
    }

    /**
     * 드라이버 로드 실패(사용자 표시용 메시지).
     */
    static final class DriverLoadException extends RuntimeException {
        DriverLoadException(final String message) {
            super(message);
        }

        DriverLoadException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
