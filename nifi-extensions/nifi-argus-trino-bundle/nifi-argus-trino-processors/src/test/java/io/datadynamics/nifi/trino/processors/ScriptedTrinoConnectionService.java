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
package io.datadynamics.nifi.trino.processors;

import io.datadynamics.nifi.trino.api.TrinoConnectionService;
import io.datadynamics.nifi.trino.api.TrinoQueryInfo;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 실행한 SQL 을 기록하고, 필요하면 cancel() 이 오거나 gate 가 열릴 때까지 실행을 붙잡아 두는 가짜 서비스(느린 문장 흉내).
 */
public class ScriptedTrinoConnectionService extends AbstractControllerService implements TrinoConnectionService {

    final List<String> executed = new CopyOnWriteArrayList<>();
    final AtomicInteger cancels = new AtomicInteger();
    final Semaphore started = new Semaphore(0);
    final AtomicInteger queryCounter = new AtomicInteger();
    volatile CountDownLatch gate;
    volatile long updateCount = 7;
    volatile String failWith;

    private final Map<Statement, TrinoQueryInfo> infos = new ConcurrentHashMap<>();

    @Override
    public Connection getConnection() throws ProcessException {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "createStatement" -> newStatement();
                    case "isClosed", "isWrapperFor" -> false;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> null;
                });
    }

    private Statement newStatement() {
        final boolean[] canceled = {false};
        final Statement[] self = new Statement[1];
        self[0] = (Statement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "execute" -> {
                        final String sql = (String) args[0];
                        executed.add(sql);
                        final TrinoQueryInfo info = new TrinoQueryInfo("scripted_" + queryCounter.incrementAndGet(), "RUNNING", 0, 0, 0, 0, 0);
                        infos.put(self[0], info);
                        started.release();
                        final CountDownLatch currentGate = gate;
                        if (currentGate != null && !currentGate.await(30, TimeUnit.SECONDS)) {
                            throw new SQLException("테스트 gate 시간 초과");
                        }
                        if (canceled[0]) {
                            infos.put(self[0], new TrinoQueryInfo(info.queryId(), "FAILED", 0, 0, 0, 0, 0));
                            throw new SQLException("Query was canceled", "", 0x0003);
                        }
                        if (failWith != null) {
                            infos.put(self[0], new TrinoQueryInfo(info.queryId(), "FAILED", 0, 0, 0, 0, 0));
                            throw new SQLException(failWith, "", 42);
                        }
                        infos.put(self[0], new TrinoQueryInfo(info.queryId(), "FINISHED", 5, 50, 1, 9, 0));
                        yield false;
                    }
                    case "cancel" -> {
                        canceled[0] = true;
                        cancels.incrementAndGet();
                        final CountDownLatch currentGate = gate;
                        if (currentGate != null) {
                            currentGate.countDown();
                        }
                        yield null;
                    }
                    case "getLargeUpdateCount" -> updateCount;
                    case "getUpdateCount" -> (int) updateCount;
                    case "isClosed", "isWrapperFor" -> false;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> null;
                });
        return self[0];
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:trino://scripted:8080";
    }

    @Override
    public TrinoQueryInfo getQueryInfo(final ResultSet resultSet) {
        return null;
    }

    @Override
    public TrinoQueryInfo getQueryInfo(final Statement statement) {
        return infos.get(statement);
    }
}
