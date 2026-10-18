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
package io.datadynamics.nifi.processors.hive;

import io.datadynamics.nifi.services.api.hive.Hive3DBCPService;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 쿼리별로 정해 둔 결과를 돌려주는 가짜 Hive3DBCPService. JDBC 인터페이스를 동적 프록시로 흉내 낸다.
 * <p>
 * 핸들러는 실행한 SQL 을 받아 {@link Result} 를 돌려준다(결과 집합, 결과 없음, 또는 SQLException).
 */
class ScriptedHive3DBCPService extends AbstractControllerService implements Hive3DBCPService {

    /**
     * 한 문장의 결과.
     */
    record Result(List<String> labels, List<List<String>> rows, SQLException error) {
        static Result rows(final List<String> labels, final List<List<String>> rows) {
            return new Result(labels, rows, null);
        }

        static Result none() {
            return new Result(null, null, null);
        }

        static Result error(final String message, final int code) {
            return new Result(null, null, new SQLException(message, "42000", code));
        }

        boolean hasResultSet() {
            return labels != null;
        }
    }

    private volatile Function<String, Result> handler = sql -> Result.none();
    private final List<String> executed = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean connectionFailure;

    void setHandler(final Function<String, Result> handler) {
        this.handler = handler;
    }

    void setConnectionFailure(final boolean connectionFailure) {
        this.connectionFailure = connectionFailure;
    }

    List<String> executed() {
        return new ArrayList<>(executed);
    }

    void clearExecuted() {
        executed.clear();
    }

    @Override
    public String getConnectionURL() {
        return "jdbc:hive2://scripted:10000/default";
    }

    @Override
    public Connection getConnection(final Map<String, String> attributes) throws ProcessException {
        return getConnection();
    }

    @Override
    public Connection getConnection() throws ProcessException {
        if (connectionFailure) {
            throw new ProcessException("연결 풀 비활성");
        }
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {
            switch (method.getName()) {
                case "createStatement":
                    return statement();
                case "isClosed":
                case "isReadOnly":
                    return false;
                case "isValid":
                case "getAutoCommit":
                    return true;
                default:
                    return defaultValue(method.getReturnType());
            }
        });
    }

    private Statement statement() {
        final Result[] current = new Result[1];
        return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class}, (proxy, method, args) -> {
            switch (method.getName()) {
                case "execute": {
                    current[0] = run((String) args[0]);
                    return current[0].hasResultSet();
                }
                case "executeQuery": {
                    current[0] = run((String) args[0]);
                    if (!current[0].hasResultSet()) {
                        throw new SQLException("The query did not generate a result set!");
                    }
                    return resultSet(current[0]);
                }
                case "getResultSet":
                    return current[0] != null && current[0].hasResultSet() ? resultSet(current[0]) : null;
                case "getUpdateCount":
                    return -1;
                default:
                    return defaultValue(method.getReturnType());
            }
        });
    }

    private Result run(final String sql) throws SQLException {
        executed.add(sql);
        final Result result = handler.apply(sql);
        if (result == null) {
            throw new SQLException("스크립트에 없는 SQL: " + sql);
        }
        if (result.error() != null) {
            throw result.error();
        }
        return result;
    }

    private ResultSet resultSet(final Result result) {
        final int[] cursor = {-1};
        final ResultSetMetaData metaData = (ResultSetMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getColumnCount" -> result.labels().size();
                    case "getColumnLabel", "getColumnName" -> result.labels().get((Integer) args[0] - 1);
                    default -> defaultValue(method.getReturnType());
                });
        return (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
            switch (method.getName()) {
                case "next":
                    cursor[0]++;
                    return cursor[0] < result.rows().size();
                case "getMetaData":
                    return metaData;
                case "getString":
                case "getObject": {
                    final List<String> row = result.rows().get(cursor[0]);
                    final int index = (Integer) args[0] - 1;
                    return index < row.size() ? row.get(index) : null;
                }
                default:
                    return defaultValue(method.getReturnType());
            }
        });
    }

    private static Object defaultValue(final Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class || type == long.class || type == short.class || type == byte.class) {
            return type == long.class ? 0L : 0;
        }
        if (type == double.class || type == float.class) {
            return 0.0;
        }
        return null;
    }

    // ---- 결과 만들기 도우미 ----

    static List<List<String>> rows(final String[]... rows) {
        final List<List<String>> list = new ArrayList<>();
        for (final String[] row : rows) {
            list.add(Arrays.asList(row));
        }
        return list;
    }
}
