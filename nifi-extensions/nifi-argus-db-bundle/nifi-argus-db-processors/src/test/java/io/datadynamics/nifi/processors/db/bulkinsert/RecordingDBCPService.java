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
package io.datadynamics.nifi.processors.db.bulkinsert;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Oracle 전용 SQL(INSERT ALL ... FROM dual)을 H2로 실행할 수 없으므로, JDBC Connection/Statement를
 * 동적 프록시로 흉내 내어 실행된 SQL과 트랜잭션 호출(commit/rollback/close/autoCommit)을 기록하는 테스트용 DBCPService.
 * PreparedStatement는 실행 시점의 SQL과 바인딩된 파라미터(인덱스 순)를 기록한다.
 */
public class RecordingDBCPService extends AbstractControllerService implements DBCPService {

    static final String JDBC_URL = "jdbc:oracle:thin:@recording:1521/TEST";

    final List<String> executedSql = new ArrayList<>();
    // executedSql과 같은 순서로, 실행 시점에 바인딩된 파라미터 목록 (Statement.execute(sql)는 빈 목록)
    final List<List<Object>> executedParams = new ArrayList<>();
    final List<String> preparedSql = new ArrayList<>();
    final List<Integer> queryTimeouts = new ArrayList<>();
    final List<Boolean> autoCommitSets = new ArrayList<>();
    int commits;
    int rollbacks;
    int connectionCloses;
    int statementCloses;
    boolean initialAutoCommit = true;
    // 실행할 SQL을 받아 던질 예외를 반환(null이면 정상 실행)
    Function<String, SQLException> executeFailure = sql -> null;

    /**
     * setNull()로 바인딩된 값을 나타낸다.
     *
     * @param sqlType {@link java.sql.Types}
     */
    record SqlNull(int sqlType) {
    }

    @Override
    public String getIdentifier() {
        return "dbcp";
    }

    @Override
    public Connection getConnection() throws ProcessException {
        final boolean[] autoCommit = {initialAutoCommit};
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {
            switch (method.getName()) {
                case "getAutoCommit":
                    return autoCommit[0];
                case "setAutoCommit":
                    autoCommit[0] = (Boolean) args[0];
                    autoCommitSets.add((Boolean) args[0]);
                    return null;
                case "commit":
                    commits++;
                    return null;
                case "rollback":
                    rollbacks++;
                    return null;
                case "close":
                    connectionCloses++;
                    return null;
                case "createStatement":
                    return createStatement();
                case "prepareStatement":
                    return prepareStatement((String) args[0]);
                case "getMetaData":
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                            (p, m, a) -> "getURL".equals(m.getName()) ? JDBC_URL : null);
                case "isClosed":
                    return false;
                case "toString":
                    return "RecordingConnection";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        });
    }

    private Statement createStatement() {
        return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class}, (proxy, method, args) -> {
            switch (method.getName()) {
                case "setQueryTimeout":
                    queryTimeouts.add((Integer) args[0]);
                    return null;
                case "execute":
                    final String sql = (String) args[0];
                    executedSql.add(sql);
                    executedParams.add(List.of());
                    final SQLException failure = executeFailure.apply(sql);
                    if (failure != null) {
                        throw failure;
                    }
                    return false;
                case "close":
                    statementCloses++;
                    return null;
                case "toString":
                    return "RecordingStatement";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        });
    }

    private PreparedStatement prepareStatement(final String sql) {
        preparedSql.add(sql);
        final Map<Integer, Object> params = new TreeMap<>();
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
            switch (method.getName()) {
                case "setQueryTimeout":
                    queryTimeouts.add((Integer) args[0]);
                    return null;
                case "setNull":
                    params.put((Integer) args[0], new SqlNull((Integer) args[1]));
                    return null;
                case "setObject":
                case "setString":
                case "setInt":
                case "setLong":
                case "setDouble":
                case "setBoolean":
                case "setBigDecimal":
                case "setDate":
                case "setTime":
                case "setTimestamp":
                case "setByte":
                case "setBytes":
                    params.put((Integer) args[0], args[1]);
                    return null;
                case "clearParameters":
                    params.clear();
                    return null;
                case "execute":
                    if (args != null && args.length > 0) {
                        throw new UnsupportedOperationException("execute(String) on PreparedStatement");
                    }
                    executedSql.add(sql);
                    executedParams.add(new ArrayList<>(params.values()));
                    final SQLException failure = executeFailure.apply(sql);
                    if (failure != null) {
                        throw failure;
                    }
                    return false;
                case "close":
                    statementCloses++;
                    return null;
                case "toString":
                    return "RecordingPreparedStatement";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    throw new UnsupportedOperationException(method.getName());
            }
        });
    }
}
