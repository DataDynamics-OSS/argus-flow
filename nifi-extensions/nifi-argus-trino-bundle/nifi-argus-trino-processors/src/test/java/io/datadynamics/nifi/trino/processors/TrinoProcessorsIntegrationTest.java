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

import io.datadynamics.nifi.trino.service.StandardTrinoConnectionService;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 실제 Trino(기본 trinodb/trino:483)로 세 프로세서를 확인한다. iceberg/delta/hive 카탈로그는 파일 메타스토어 + 로컬 파일시스템.
 * Docker 가 없으면 건너뛴다.
 */
@Testcontainers(disabledWithoutDocker = true)
class TrinoProcessorsIntegrationTest {

    @Container
    private static final TrinoContainer TRINO = new TrinoContainer(
            DockerImageName.parse(System.getProperty("trino.test.image", "trinodb/trino:483")))
            .withCopyFileToContainer(MountableFile.forClasspathResource("trino/catalog/iceberg.properties"), "/etc/trino/catalog/iceberg.properties")
            .withCopyFileToContainer(MountableFile.forClasspathResource("trino/catalog/delta.properties"), "/etc/trino/catalog/delta.properties")
            .withCopyFileToContainer(MountableFile.forClasspathResource("trino/catalog/hive.properties"), "/etc/trino/catalog/hive.properties")
            .withTmpFs(Map.of("/tmp/trino-it", "rw,mode=1777"))
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("bash", "-c",
                    "mkdir -p /tmp/trino-it/iceberg /tmp/trino-it/delta /tmp/trino-it/hive && exec /usr/lib/trino/bin/run-trino"))
            .withStartupTimeout(Duration.ofMinutes(3));

    private static String url() {
        return "jdbc:trino://" + TRINO.getHost() + ":" + TRINO.getMappedPort(8080);
    }

    @BeforeAll
    static void createTables() throws Exception {
        sql("CREATE SCHEMA iceberg.it");
        sql("CREATE TABLE iceberg.it.events (id bigint, name varchar)");
        sql("INSERT INTO iceberg.it.events VALUES (1, 'a')");
        sql("INSERT INTO iceberg.it.events VALUES (2, 'b')");
        sql("CREATE SCHEMA delta.it");
        sql("CREATE TABLE delta.it.events (id bigint, name varchar)");
        sql("CREATE SCHEMA hive.it");
        sql("CREATE TABLE hive.it.events (id bigint, dt varchar) WITH (partitioned_by = ARRAY['dt'])");
        sql("INSERT INTO hive.it.events VALUES (1, '2026-10-18')");
    }

    @AfterAll
    static void noop() {
        // 컨테이너는 Testcontainers 가 정리한다
    }

    private static void sql(final String statement) throws SQLException {
        final Properties props = new Properties();
        props.setProperty("user", "it");
        try (Connection connection = DriverManager.getConnection(url(), props); Statement st = connection.createStatement()) {
            st.execute(statement);
        }
    }

    private static TestRunner runner(final Class<? extends org.apache.nifi.processor.Processor> processor, final String source) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final StandardTrinoConnectionService service = new StandardTrinoConnectionService();
        runner.addControllerService("trino", service);
        runner.setProperty(service, "Trino URL", url());
        runner.setProperty(service, "User", "etl");
        runner.setProperty(service, "Source", source);
        runner.enableControllerService(service);
        runner.setProperty(TrinoProcessorProperties.TRINO_CONNECTION_SERVICE, "trino");
        return runner;
    }

    // ---- QueryTrinoRecord ----

    @Test
    void queryTrinoRecordWritesQueryIdAndStats() throws Exception {
        final TestRunner runner = runner(QueryTrinoRecord.class, "it-query");
        final MockRecordWriter writer = new MockRecordWriter(null, false);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(QueryTrinoRecord.RECORD_WRITER, "writer");
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT nationkey, name FROM tpch.tiny.nation ORDER BY nationkey");
        runner.setProperty(QueryTrinoRecord.MAX_ROWS_PER_FLOWFILE, "10");
        runner.setIncomingConnection(false);
        runner.run();

        final List<MockFlowFile> outputs = runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS);
        assertEquals(3, outputs.size());
        assertEquals(25, outputs.stream().mapToInt(f -> Integer.parseInt(f.getAttribute("record.count"))).sum());
        final MockFlowFile first = outputs.get(0);
        assertNotNull(first.getAttribute(TrinoAttributes.QUERY_ID));
        first.assertAttributeEquals(TrinoAttributes.QUERY_STATE, "FINISHED");
        assertTrue(Long.parseLong(first.getAttribute(TrinoAttributes.PROCESSED_ROWS)) >= 25);
        assertTrue(first.getContent().startsWith("0,ALGERIA\n"), first.getContent());
        assertTrue(runner.getProvenanceEvents().get(0).getTransitUri().endsWith("#" + first.getAttribute(TrinoAttributes.QUERY_ID)));
    }

    @Test
    void queryTrinoRecordStopCancelsQuery() throws Exception {
        final TestRunner runner = runner(QueryTrinoRecord.class, "it-cancel");
        final MockRecordWriter writer = new MockRecordWriter(null, false);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(QueryTrinoRecord.RECORD_WRITER, "writer");
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT count(*) FROM tpch.sf1000.lineitem WHERE comment LIKE '%argus%'");
        runner.setIncomingConnection(false);

        final Thread trigger = new Thread(() -> runner.run(1, false, true));
        trigger.start();
        await(() -> !queryStates("it-cancel", "RUNNING").isEmpty() || !queryStates("it-cancel", "QUEUED").isEmpty()
                || !queryStates("it-cancel", "PLANNING").isEmpty() || !queryStates("it-cancel", "STARTING").isEmpty(), 60);
        runner.unSchedule();
        trigger.join(TimeUnit.SECONDS.toMillis(60));

        assertEquals(0, runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS).size());
        await(() -> queryStates("it-cancel", "FAILED").contains("USER_CANCELED"), 30);
    }

    @Test
    void queryTrinoRecordWithExternalChildFirstDriver() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(QueryTrinoRecord.class);
        final StandardTrinoConnectionService service = new StandardTrinoConnectionService();
        runner.addControllerService("trino", service);
        runner.setProperty(service, "Trino URL", url());
        runner.setProperty(service, "User", "etl");
        runner.setProperty(service, "Database Driver Location(s)", java.nio.file.Path.of(io.trino.jdbc.NonRegisteringTrinoDriver.class
                .getProtectionDomain().getCodeSource().getLocation().toURI()).toString());
        runner.setProperty(service, "Database Driver Class Name", "io.trino.jdbc.TrinoDriver");
        runner.enableControllerService(service);
        runner.setProperty(TrinoProcessorProperties.TRINO_CONNECTION_SERVICE, "trino");
        final MockRecordWriter writer = new MockRecordWriter(null, false);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(QueryTrinoRecord.RECORD_WRITER, "writer");
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT count(*) FROM tpch.tiny.region");
        runner.setIncomingConnection(false);
        runner.run();
        final MockFlowFile out = runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("5\n");
        assertNotNull(out.getAttribute(TrinoAttributes.QUERY_ID), "외부 드라이버에서도 query id 를 기록한다");
        out.assertAttributeEquals(TrinoAttributes.QUERY_STATE, "FINISHED");
    }

    // ---- ExecuteTrinoStatement ----

    @Test
    void executeTrinoStatementCtasSynchronousAndBackground() throws Exception {
        final TestRunner runner = runner(ExecuteTrinoStatement.class, "it-execute");
        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteTrinoStatement.SQL_STATEMENT, "CREATE TABLE memory.default.nation_sync AS SELECT * FROM tpch.tiny.nation;");
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_SUCCESS, 1);
        final MockFlowFile sync = runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0);
        sync.assertAttributeEquals(TrinoAttributes.UPDATE_COUNT, "25");
        assertNotNull(sync.getAttribute(TrinoAttributes.QUERY_ID));

        final TestRunner background = runner(ExecuteTrinoStatement.class, "it-execute-bg");
        background.setProperty(TrinoProcessorProperties.EXECUTION_MODE, "BACKGROUND");
        background.setProperty(ExecuteTrinoStatement.SQL_STATEMENT, "INSERT INTO memory.default.nation_sync SELECT * FROM tpch.tiny.nation WHERE regionkey = ${region}");
        background.enqueue("", Map.of("region", "1"));
        background.run(1, false, true);
        await(() -> background.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).size() == 1, 60);
        final MockFlowFile bg = background.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0);
        bg.assertAttributeEquals(TrinoAttributes.UPDATE_COUNT, "5");
        assertNotNull(bg.getAttribute(TrinoAttributes.QUERY_ID));
        background.stop();
    }

    @Test
    void executeTrinoStatementSqlErrorGoesToFailure() throws Exception {
        final TestRunner runner = runner(ExecuteTrinoStatement.class, "it-execute-error");
        runner.enqueue("DROP TABLE memory.default.does_not_exist");
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_FAILURE).get(0);
        assertTrue(failed.getAttribute(TrinoAttributes.ERROR_MESSAGE).contains("does not exist"), failed.getAttributes().toString());
        failed.assertAttributeExists(TrinoAttributes.ERROR_CODE);
        assertNotNull(failed.getAttribute(TrinoAttributes.QUERY_ID), "실패한 쿼리도 query id 를 남긴다");
    }

    // ---- MaintainTrinoTable ----

    private static MockFlowFile maintain(final String format, final String operation, final String catalog,
                                         final Map<String, String> extra, final boolean expectSuccess) throws Exception {
        final TestRunner runner = runner(MaintainTrinoTable.class, "it-maintain");
        runner.setProperty(MaintainTrinoTable.TABLE_FORMAT, format);
        runner.setProperty(MaintainTrinoTable.OPERATION, operation);
        runner.setProperty(MaintainTrinoTable.CATALOG, "${catalog}");
        runner.setProperty(MaintainTrinoTable.SCHEMA, "it");
        runner.setProperty(MaintainTrinoTable.TABLE, "events");
        extra.forEach(runner::setProperty);
        runner.enqueue("", Map.of("catalog", catalog));
        runner.run();
        final var relationship = expectSuccess ? TrinoProcessorProperties.REL_SUCCESS : TrinoProcessorProperties.REL_FAILURE;
        runner.assertAllFlowFilesTransferred(relationship, 1);
        return runner.getFlowFilesForRelationship(relationship).get(0);
    }

    @Test
    void icebergMaintenance() throws Exception {
        final MockFlowFile optimize = maintain("ICEBERG", "OPTIMIZE", "iceberg", Map.of(), true);
        assertNotNull(optimize.getAttribute(TrinoAttributes.QUERY_ID));
        optimize.assertAttributeEquals(TrinoAttributes.MAINTENANCE_OPERATION, "OPTIMIZE");
        optimize.assertAttributeExists(TrinoAttributes.WALL_TIME_MS);

        maintain("ICEBERG", "EXPIRE_SNAPSHOTS", "iceberg", Map.of("Retention Threshold", "7d"), true);
        maintain("ICEBERG", "REMOVE_ORPHAN_FILES", "iceberg", Map.of("Retention Threshold", "7d"), true);
        maintain("ICEBERG", "ANALYZE", "iceberg", Map.of(), true);

        final MockFlowFile tooShort = maintain("ICEBERG", "EXPIRE_SNAPSHOTS", "iceberg", Map.of("Retention Threshold", "1d"), false);
        assertTrue(tooShort.getAttribute(TrinoAttributes.ERROR_MESSAGE).contains("minimum retention"), tooShort.getAttributes().toString());
    }

    @Test
    void deltaLakeMaintenance() throws Exception {
        // 로컬 파일시스템 Delta 쓰기는 483 에서 막혀 있다. OPTIMIZE 는 SQL 이 분석·계획까지 통과해 쓰기 단계에서 거부되는지 확인한다
        final MockFlowFile optimize = maintain("DELTA_LAKE", "OPTIMIZE", "delta", Map.of("File Size Threshold", "64MB"), false);
        assertTrue(optimize.getAttribute(TrinoAttributes.ERROR_MESSAGE).contains("Writes are not enabled"), optimize.getAttributes().toString());
        optimize.assertAttributeEquals(TrinoAttributes.MAINTENANCE_SQL,
                "ALTER TABLE \"delta\".\"it\".\"events\" EXECUTE optimize(file_size_threshold => '64MB')");
        maintain("DELTA_LAKE", "VACUUM", "delta", Map.of("Retention Threshold", "7d"), true);
        maintain("DELTA_LAKE", "ANALYZE", "delta", Map.of(), true);
    }

    @Test
    void hiveMaintenance() throws Exception {
        maintain("HIVE", "SYNC_PARTITION_METADATA", "hive", Map.of("Sync Mode", "FULL"), true);
        final MockFlowFile analyze = maintain("HIVE", "ANALYZE", "hive", Map.of(), true);
        analyze.assertAttributeExists(TrinoAttributes.UPDATE_COUNT);
    }

    @Test
    void maintenanceInBackgroundMode() throws Exception {
        final TestRunner runner = runner(MaintainTrinoTable.class, "it-maintain-bg");
        runner.setProperty(MaintainTrinoTable.TABLE_FORMAT, "ICEBERG");
        runner.setProperty(MaintainTrinoTable.OPERATION, "OPTIMIZE");
        runner.setProperty(MaintainTrinoTable.CATALOG, "iceberg");
        runner.setProperty(MaintainTrinoTable.SCHEMA, "it");
        runner.setProperty(MaintainTrinoTable.TABLE, "events");
        runner.setProperty(TrinoProcessorProperties.EXECUTION_MODE, "BACKGROUND");
        runner.setIncomingConnection(false);
        runner.run(1, false, true);
        await(() -> runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).size() == 1, 60);
        runner.stop();
    }

    // ---- helpers ----

    private static List<String> queryStates(final String source, final String state) {
        final Properties props = new Properties();
        props.setProperty("user", "it");
        try (Connection connection = DriverManager.getConnection(url(), props);
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT coalesce(error_code, state) FROM system.runtime.queries WHERE source = ? AND state = ?")) {
            statement.setString(1, source);
            statement.setString(2, state);
            try (ResultSet rs = statement.executeQuery()) {
                final List<String> result = new java.util.ArrayList<>();
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
                return result;
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void await(final BooleanSupplier condition, final int seconds) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("조건이 " + seconds + "초 안에 충족되지 않았다");
            }
            Thread.sleep(200);
        }
    }
}
