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
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 실제 Trino(기본 trinodb/trino:483) 컨테이너에 연결하는 통합 테스트. Docker 가 없으면 건너뛴다.
 */
@Testcontainers(disabledWithoutDocker = true)
class StandardTrinoConnectionServiceIntegrationTest {

    @Container
    private static final TrinoContainer TRINO = new TrinoContainer(
            DockerImageName.parse(System.getProperty("trino.test.image", "trinodb/trino:483")));

    private TestRunner runner;
    private StandardTrinoConnectionService service;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardTrinoConnectionService();
        runner.addControllerService("trino", service);
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL,
                "jdbc:trino://" + TRINO.getHost() + ":" + TRINO.getMappedPort(8080));
        runner.setProperty(service, StandardTrinoConnectionService.USER, "etl");
        runner.setProperty(service, StandardTrinoConnectionService.DEFAULT_CATALOG, "tpch");
        runner.setProperty(service, StandardTrinoConnectionService.DEFAULT_SCHEMA, "tiny");
        runner.setProperty(service, StandardTrinoConnectionService.SOURCE, "argus-flow-it");
        runner.setProperty(service, StandardTrinoConnectionService.CLIENT_TAGS, "etl,nightly");
        runner.setProperty(service, StandardTrinoConnectionService.TIME_ZONE, "Asia/Seoul");
        runner.setProperty(service, "session.query_max_run_time", "2h");
    }

    @Test
    void verifyRunsSelectOne() {
        final List<ConfigVerificationResult> results = runner.verify(service, Collections.emptyMap());
        assertEquals(3, results.size(), results.toString());
        results.forEach(r -> assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, r.getOutcome(), r.toString()));
        assertTrue(results.get(1).getExplanation().contains("번들"), results.get(1).getExplanation());
        assertTrue(results.get(2).getExplanation().contains("etl"), results.get(2).getExplanation());

        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_LOCATION, uncheckedTrinoJar());
        final List<ConfigVerificationResult> external = runner.verify(service, Collections.emptyMap());
        external.forEach(r -> assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, r.getOutcome(), r.toString()));
        assertTrue(external.get(1).getExplanation().contains("child-first"), external.get(1).getExplanation());
    }

    private static String uncheckedTrinoJar() {
        try {
            return ExternalDriverTest.trinoJar();
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void verifyFailsWhenServerUnreachable() {
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:trino://127.0.0.1:1");
        final List<ConfigVerificationResult> results = runner.verify(service, Collections.emptyMap());
        assertEquals(ConfigVerificationResult.Outcome.FAILED, results.get(results.size() - 1).getOutcome());
    }

    @Test
    void queryInfoForResultSetAndUpdate() throws Exception {
        runner.enableControllerService(service);
        try (Connection connection = service.getConnection(); Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM nation")) {
                assertTrue(resultSet.next());
                assertEquals(25, resultSet.getLong(1));
                while (resultSet.next()) {
                    // drain
                }
                final TrinoQueryInfo info = service.getQueryInfo(resultSet);
                assertNotNull(info);
                assertNotNull(info.queryId());
                assertEquals("FINISHED", info.state());
                assertTrue(info.processedRows() >= 25, info.toString());
            }

            final long updated = statement.executeLargeUpdate("CREATE TABLE memory.default.nation_copy AS SELECT * FROM nation");
            assertEquals(25, updated);
            final TrinoQueryInfo updateInfo = service.getQueryInfo(statement);
            assertNotNull(updateInfo, "업데이트 문장의 query id 를 progress monitor 로 기록해야 한다");
            assertNotNull(updateInfo.queryId());
            statement.execute("DROP TABLE memory.default.nation_copy");
        }
    }

    @Test
    void connectionPropertiesReachTheServer() throws Exception {
        runner.setProperty(service, StandardTrinoConnectionService.SESSION_USER, "alice");
        runner.enableControllerService(service);
        final String queryId;
        try (Connection connection = service.getConnection(); Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT current_user, current_catalog, current_schema, current_timezone()")) {
                assertTrue(rs.next());
                assertEquals("alice", rs.getString(1));
                assertEquals("tpch", rs.getString(2));
                assertEquals("tiny", rs.getString(3));
                assertEquals("Asia/Seoul", rs.getString(4));
                queryId = service.getQueryInfo(rs).queryId();
            }
            final Map<String, String> session = new HashMap<>();
            try (ResultSet rs = statement.executeQuery("SHOW SESSION LIKE 'query_max_run_time'")) {
                while (rs.next()) {
                    session.put(rs.getString(1), rs.getString(2));
                }
            }
            assertEquals("2h", session.get("query_max_run_time"));
        }
        try (Connection connection = service.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT source, \"user\" FROM system.runtime.queries WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("argus-flow-it", rs.getString(1));
                assertEquals("alice", rs.getString(2));
            }
        }
    }

    @Test
    void externalChildFirstDriverSupportsQueryInfoAndCancel() throws Exception {
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_LOCATION, ExternalDriverTest.trinoJar());
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "io.trino.jdbc.TrinoDriver");
        runner.setProperty(service, StandardTrinoConnectionService.SOURCE, "ext-driver-it");
        runner.enableControllerService(service);
        assertTrue(service.currentDriverHandle().isExternal());
        assertTrue(service.currentDriverHandle().driver().getClass().getClassLoader() instanceof ChildFirstClassLoader);

        try (Connection connection = service.getConnection(); Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM nation")) {
                assertTrue(resultSet.next());
                assertEquals(25, resultSet.getLong(1));
                assertTrue(!resultSet.next());
                final TrinoQueryInfo info = service.getQueryInfo(resultSet);
                assertNotNull(info, "외부 클래스로더 드라이버에서도 리플렉션으로 통계를 읽는다");
                assertEquals("FINISHED", info.state());
                assertTrue(info.processedRows() >= 25);
            }
            assertEquals(25, statement.executeLargeUpdate("CREATE TABLE memory.default.nation_ext AS SELECT * FROM nation"));
            final TrinoQueryInfo updateInfo = service.getQueryInfo(statement);
            assertNotNull(updateInfo);
            assertNotNull(updateInfo.queryId());
        }

        // 취소: 다른 스레드에서 오래 걸리는 쿼리를 돌리고 Statement.cancel()
        try (Connection connection = service.getConnection(); Statement statement = connection.createStatement()) {
            final java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
            final Thread worker = new Thread(() -> {
                try (ResultSet rs = statement.executeQuery("SELECT count(*) FROM tpch.sf1000.lineitem WHERE comment LIKE '%argus%'")) {
                    rs.next();
                } catch (final Throwable t) {
                    failure.set(t);
                }
            });
            worker.start();
            final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
            while (service.getQueryInfo(statement) == null && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertNotNull(service.getQueryInfo(statement), "실행 중인 쿼리의 query id 를 progress monitor 로 받는다");
            final String queryId = service.getQueryInfo(statement).queryId();
            statement.cancel();
            worker.join(60_000);
            assertNotNull(failure.get(), "취소된 쿼리는 예외로 끝난다");

            String errorCode = null;
            while (System.nanoTime() < deadline && !"USER_CANCELED".equals(errorCode)) {
                try (Connection check = service.getConnection();
                     PreparedStatement ps = check.prepareStatement("SELECT error_code FROM system.runtime.queries WHERE query_id = ?")) {
                    ps.setString(1, queryId);
                    try (ResultSet rs = ps.executeQuery()) {
                        errorCode = rs.next() ? rs.getString(1) : null;
                    }
                }
                Thread.sleep(200);
            }
            assertEquals("USER_CANCELED", errorCode);
        }
        runner.disableControllerService(service);
    }
}
