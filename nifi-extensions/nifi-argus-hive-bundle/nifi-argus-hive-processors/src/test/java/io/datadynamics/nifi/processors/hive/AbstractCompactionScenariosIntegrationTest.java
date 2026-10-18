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

import io.datadynamics.nifi.processors.hive.compaction.CompactionRequests;
import io.datadynamics.nifi.processors.hive.compaction.CompletionState;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import org.apache.nifi.util.TestRunners;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hive 3.1·4.0 공통 시나리오. 같은 컨테이너를 쓰므로 테이블 이름이 겹치지 않게 하고 순서를 고정한다
 * (Worker 가 하나라서 앞 시험의 compaction 이 끝나야 다음 것이 시작된다).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
abstract class AbstractCompactionScenariosIntegrationTest extends AbstractCompactHive3TableIntegrationTest {

    /** {@code SHOW COMPACTIONS} 의 JDBC 컬럼 라벨(컨테이너에서 관찰한 값). */
    protected abstract List<String> expectedShowCompactionsLabels();

    /** 같은 대상에 대한 중복 COMPACT 가 SQLException 이면 true(Hive 4), 조용히 무시되면 false(Hive 3.1). */
    protected abstract boolean duplicateCompactThrows();

    /** MAJOR 시나리오에서 기다릴 완료 상태. Cleaner 가 도는 구성이면 SUCCEEDED. */
    protected abstract CompletionState majorCompletionState();

    /** REBALANCE 가 되는 구성인지(Hive 4 + query-based compaction). */
    protected abstract boolean supportsRebalance();

    private static final List<String> SESSION_KEYS = List.of("mapreduce.job.reduces", "hive.tez.auto.reducer.parallelism",
            "hive.compute.query.using.stats", "hive.merge.tezfiles", "hive.merge.mapfiles", "hive.merge.mapredfiles");

    private static Map<String, String> sessionValues(final Connection connection) throws SQLException {
        final Map<String, String> values = new java.util.LinkedHashMap<>();
        try (Statement statement = connection.createStatement()) {
            for (final String key : SESSION_KEYS) {
                try (java.sql.ResultSet resultSet = statement.executeQuery("SET " + key)) {
                    resultSet.next();
                    values.put(key, resultSet.getString(1));
                }
            }
        }
        return values;
    }

    private void insertRows(final String table, final String partition, final int from, final int to) throws Exception {
        final StringBuilder values = new StringBuilder();
        for (int i = from; i <= to; i++) {
            values.append(values.length() == 0 ? "" : ", ").append("(").append(i).append(", 'v").append(i).append("')");
        }
        sql("INSERT INTO " + table + (partition == null ? "" : " PARTITION (" + partition + ")") + " VALUES " + values);
    }

    @BeforeEach
    void ready() throws Exception {
        awaitReady();
    }

    private void createAcidTable(final String table, final int inserts) throws Exception {
        sql("CREATE TABLE " + table + " (id INT, v STRING) PARTITIONED BY (dt STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')");
        for (int i = 1; i <= inserts; i++) {
            sql("INSERT INTO " + table + " PARTITION (dt='2026-10-01') VALUES (" + i + ", 'v" + i + "'), (" + (100 + i) + ", 'w" + i + "')");
        }
        // 다른 파티션은 영향을 받지 않아야 한다
        sql("INSERT INTO " + table + " PARTITION (dt='2026-10-02') VALUES (0, 'other')");
    }

    @Test
    @Order(1)
    void jdbcBehaviourOfCompactAndShowCompactions() throws Exception {
        sql("CREATE TABLE it_jdbc_probe (id INT) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "INSERT INTO it_jdbc_probe VALUES (1)", "INSERT INTO it_jdbc_probe VALUES (2)");
        try (Connection connection = service().connect(); Statement statement = connection.createStatement()) {
            // ALTER TABLE ... COMPACT 는 결과 집합 없이 끝나고 id 를 돌려주지 않는다
            assertFalse(statement.execute("ALTER TABLE it_jdbc_probe COMPACT 'major'"));
            System.out.printf("[%s] COMPACT updateCount=%d%n", getClass().getSimpleName(), statement.getUpdateCount());
            if (duplicateCompactThrows()) {
                final SQLException e = assertThrows(SQLException.class, () -> statement.execute("ALTER TABLE it_jdbc_probe COMPACT 'major'"));
                System.out.printf("[%s] duplicate COMPACT -> state=%s code=%d message=%s%n", getClass().getSimpleName(), e.getSQLState(),
                        e.getErrorCode(), e.getMessage());
                assertTrue(CompactionRequests.parseAlreadyScheduledId(e.getMessage()).isPresent(), e.getMessage());
            } else {
                assertFalse(statement.execute("ALTER TABLE it_jdbc_probe COMPACT 'major'"));
            }
        }
        final List<List<String>> rows = query("SHOW COMPACTIONS");
        System.out.printf("[%s] SHOW COMPACTIONS labels=%s%n", getClass().getSimpleName(), rows.get(0));
        rows.subList(1, rows.size()).forEach(row -> System.out.printf("[%s]   %s%n", getClass().getSimpleName(), row));
        assertEquals(expectedShowCompactionsLabels(), rows.get(0));
        // 중복 요청은 새 compaction 을 만들지 않는다
        assertEquals(1, rows.stream().filter(row -> row.contains("it_jdbc_probe")).count());
    }

    @Test
    @Order(2)
    void majorCompactionThroughInProgressLoopWithRestart() throws Exception {
        createAcidTable("it_major", 3);
        final List<String> before = listDir("it_major/dt=2026-10-01");
        assertEquals(3, withPrefix(before, "delta_").size(), before.toString());
        final long rowsBefore = count("it_major", "dt='2026-10-01'");

        final TestRunner runner = runner();
        runner.setProperty(CompactHive3Table.COMPLETION_STATE, majorCompletionState().name());
        final Drive drive = drive(runner, target("it_major", "dt='2026-10-01'"), Duration.ofMinutes(5), 2000, () -> {
            try {
                final TestRunner restarted = runner();
                restarted.setProperty(CompactHive3Table.COMPLETION_STATE, majorCompletionState().name());
                return restarted;
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        });
        assertRoutedTo(drive, CompactHive3Table.REL_SUCCESS);
        final MockFlowFile result = drive.result();
        result.assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "MAJOR");
        result.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_BEFORE, Long.toString(rowsBefore));
        result.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, Long.toString(rowsBefore));
        result.assertAttributeEquals(CompactHive3Table.ATTR_STATE, majorCompletionState() == CompletionState.SUCCEEDED ? "succeeded" : "ready for cleaning");
        assertTrue(drive.triggers() > 1, "in progress 루프를 거쳐야 한다");
        assertTrue(drive.states().contains("initiated") || drive.states().contains("working"), drive.states().toString());

        final List<String> after = listDir("it_major/dt=2026-10-01");
        System.out.printf("[%s] it_major dt=2026-10-01 before=%s after=%s%n", getClass().getSimpleName(), before, after);
        assertEquals(1, withPrefix(after, "base_").size(), after.toString());
        if (majorCompletionState() == CompletionState.SUCCEEDED) {
            assertTrue(withPrefix(after, "delta_").isEmpty(), "Cleaner 가 옛 delta 를 지워야 한다: " + after);
        }
        assertEquals(rowsBefore, count("it_major", "dt='2026-10-01'"));
        assertEquals(1, count("it_major", "dt='2026-10-02'"));
        final String id = result.getAttribute(CompactHive3Table.ATTR_ID);
        assertTrue(query("SHOW COMPACTIONS").stream().anyMatch(row -> row.get(0).equals(id) && row.contains("it_major") && row.contains("MAJOR")));
    }

    @Test
    @Order(3)
    void minorCompactionMergesDeltas() throws Exception {
        createAcidTable("it_minor", 3);
        final List<String> before = listDir("it_minor/dt=2026-10-01");
        final TestRunner runner = runner();
        runner.setProperty(CompactHive3Table.COMPACTION_TYPE, "MINOR");
        final Drive drive = drive(runner, target("it_minor", "dt='2026-10-01'"));
        assertRoutedTo(drive, CompactHive3Table.REL_SUCCESS);
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "MINOR");
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "6");
        final List<String> after = listDir("it_minor/dt=2026-10-01");
        System.out.printf("[%s] it_minor dt=2026-10-01 before=%s after=%s%n", getClass().getSimpleName(), before, after);
        // delta_0000001_0000001 ... 세 개가 범위 delta(delta_<min>_<max>) 하나로 합쳐진다
        assertTrue(after.stream().anyMatch(d -> d.matches("delta_(\\d+)_(\\d+).*") && !d.replaceAll("delta_(\\d+)_(\\d+).*", "$1")
                .equals(d.replaceAll("delta_(\\d+)_(\\d+).*", "$2"))), after.toString());
        assertTrue(withPrefix(after, "base_").isEmpty(), after.toString());
        assertEquals(6, count("it_minor", "dt='2026-10-01'"));
    }

    @Test
    @Order(4)
    void nonAcidOrcTableIsConcatenated() throws Exception {
        sql("CREATE TABLE it_orc (id INT) PARTITIONED BY (dt STRING) STORED AS ORC");
        for (int i = 0; i < 3; i++) {
            sql("INSERT INTO it_orc PARTITION (dt='2026-10-01') VALUES (" + i + ")");
        }
        assertEquals(3, listDir("it_orc/dt=2026-10-01").size());
        final Drive drive = drive(runner(), target("it_orc", "dt='2026-10-01'"));
        assertRoutedTo(drive, CompactHive3Table.REL_SUCCESS);
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "CONCATENATE");
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_BEFORE, "3");
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "3");
        drive.result().assertAttributeNotExists(CompactHive3Table.ATTR_ID);
        assertEquals(1, drive.triggers());
        final List<String> after = listDir("it_orc/dt=2026-10-01");
        System.out.printf("[%s] it_orc after CONCATENATE=%s%n", getClass().getSimpleName(), after);
        assertEquals(1, after.size(), after.toString());
    }

    @Test
    @Order(5)
    void unsupportedTablesAndBadInputFail() throws Exception {
        sql("CREATE TABLE it_text (id INT) STORED AS TEXTFILE",
                "CREATE TABLE it_orc_plain (id INT) STORED AS ORC",
                "CREATE TABLE it_acid_p (id INT) PARTITIONED BY (dt STRING, hour STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "INSERT INTO it_acid_p PARTITION (dt='a', hour='01') VALUES (1)");

        final Map<String, String> expected = new java.util.LinkedHashMap<>();
        expected.put("it_text|", "TextInputFormat");
        expected.put("it_acid_p|dt='a'", "[dt, hour]");
        expected.put("it_acid_p|", "파티션 테이블이므로");
        expected.put("it_acid_p|dt='a',hour='99'", "compaction 요청 실패");
        expected.put("it_text|dt='a'", "파티션 테이블이 아니므로");
        expected.put("it_missing|", "테이블 정보를 읽지 못했다");
        expected.put("it-bad|", "[A-Za-z0-9_]+");
        for (final Map.Entry<String, String> entry : expected.entrySet()) {
            final String[] parts = entry.getKey().split("\\|", -1);
            final Drive drive = drive(runner(), target(parts[0], parts[1].isEmpty() ? null : parts[1]));
            assertRoutedTo(drive, CompactHive3Table.REL_FAILURE);
            final String error = drive.result().getAttribute(CompactHive3Table.ATTR_ERROR);
            assertTrue(error.contains(entry.getValue()), entry.getKey() + " -> " + error);
        }

        final TestRunner acidOnly = runner();
        acidOnly.setProperty(CompactHive3Table.TABLE_TYPE_HANDLING, "ACID_ONLY");
        final Drive drive = drive(acidOnly, target("it_orc_plain", null));
        assertRoutedTo(drive, CompactHive3Table.REL_FAILURE);
        assertTrue(drive.result().getAttribute(CompactHive3Table.ATTR_ERROR).contains("ACID_ONLY"));
    }

    @Test
    @Order(6)
    void rewriteProducesTargetFileCountAndRestoresSession() throws Exception {
        sql("CREATE TABLE it_rewrite (id INT, v STRING) PARTITIONED BY (dt STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')");
        insertRows("it_rewrite", "dt='2026-10-01'", 1, 10);
        insertRows("it_rewrite", "dt='2026-10-01'", 11, 20);
        insertRows("it_rewrite", "dt='2026-10-01'", 21, 30);
        final IsolatedHiveJdbcService service = service();
        final Connection session = service.shareOneConnection();
        try {
            final Map<String, String> before = sessionValues(session);
            final TestRunner runner = runner(service);
            runner.setProperty(CompactHive3Table.COMPACTION_TYPE, "REWRITE");
            runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "3");
            runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "id");
            final Drive drive = drive(runner, target("it_rewrite", "dt='2026-10-01'"));
            assertRoutedTo(drive, CompactHive3Table.REL_SUCCESS);
            drive.result().assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "REWRITE");
            drive.result().assertAttributeEquals(CompactHive3Table.ATTR_TARGET_FILE_COUNT, "3");
            drive.result().assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_BEFORE, "30");
            drive.result().assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "30");
            assertEquals(1, drive.triggers());
            final Map<String, String> after = sessionValues(session);
            System.out.printf("[%s] session before=%s after=%s%n", getClass().getSimpleName(), before, after);
            assertEquals(before, after, "같은 세션의 설정이 원래 값으로 돌아와야 한다");
        } finally {
            session.close();
        }
        final List<String> files = newestBaseFiles("it_rewrite/dt=2026-10-01");
        System.out.printf("[%s] it_rewrite dirs=%s newest base files=%s%n", getClass().getSimpleName(), listDir("it_rewrite/dt=2026-10-01"), files);
        assertEquals(3, files.size(), files.toString());
        assertEquals(30, count("it_rewrite", "dt='2026-10-01'"));
    }

    @Test
    @Order(7)
    void rewriteSizesFileCountFromHdfs() throws Exception {
        sql("CREATE TABLE it_rewrite_txt (id INT, v STRING) PARTITIONED BY (dt STRING) STORED AS TEXTFILE");
        for (int i = 0; i < 4; i++) {
            insertRows("it_rewrite_txt", "dt='a'", i * 10 + 1, i * 10 + 10);
        }
        final java.nio.file.Path partition = warehouse().resolve("it_rewrite_txt/dt=a");
        final List<String> before = listDir("it_rewrite_txt/dt=a");
        assertEquals(4, before.size(), before.toString());
        long total = 0;
        for (final String file : before) {
            total += java.nio.file.Files.size(partition.resolve(file));
        }
        final TestRunner runner = runner();
        runner.setProperty(CompactHive3Table.COMPACTION_TYPE, "REWRITE");
        runner.setProperty(CompactHive3Table.TARGET_FILE_SIZE, ((total + 1) / 2) + " B");
        runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "id");
        final Drive drive = drive(runner, target("it_rewrite_txt", "dt='a'"));
        assertRoutedTo(drive, CompactHive3Table.REL_SUCCESS);
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_PARTITION_SIZE_BYTES, Long.toString(total));
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_TARGET_FILE_COUNT, "2");
        final List<String> after = listDir("it_rewrite_txt/dt=a");
        System.out.printf("[%s] it_rewrite_txt total=%d before=%s after=%s%n", getClass().getSimpleName(), total, before, after);
        assertEquals(2, after.size(), after.toString());
        assertEquals(40, count("it_rewrite_txt", "dt='a'"));
    }

    @Test
    @Order(8)
    void listsCompactionCandidates() throws Exception {
        sql("CREATE TABLE it_cand (id INT, v STRING) PARTITIONED BY (dt STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')");
        for (int i = 0; i < 3; i++) {
            insertRows("it_cand", "dt='a'", i * 10, i * 10 + 1);
            insertRows("it_cand", "dt='c'", i * 10, i * 10 + 1);
        }
        insertRows("it_cand", "dt='b'", 1, 2);
        // dt=c 는 MAJOR 로 합친다(Hive 4 는 Cleaner 가 없어 옛 delta 가 남지만 세지 않아야 한다)
        final TestRunner compact = runner();
        compact.setProperty(CompactHive3Table.COMPLETION_STATE, majorCompletionState().name());
        assertRoutedTo(drive(compact, target("it_cand", "dt='c'")), CompactHive3Table.REL_SUCCESS);
        System.out.printf("[%s] it_cand dt=c after MAJOR=%s%n", getClass().getSimpleName(), listDir("it_cand/dt=c"));

        final TestRunner fresh = listRunner("it_cand");
        fresh.run();
        assertEquals(0, fresh.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).size(), "방금 적재한 파티션은 빠져야 한다");

        final TestRunner lister = listRunner("it_cand");
        lister.setProperty(ListHiveCompactionCandidates.MINIMUM_PARTITION_AGE, "0 sec");
        lister.run();
        final List<org.apache.nifi.util.MockFlowFile> candidates = lister.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS);
        candidates.forEach(c -> System.out.printf("[%s] candidate %s%n", getClass().getSimpleName(), c.getAttributes()));
        assertEquals(2, candidates.size());
        candidates.get(0).assertAttributeEquals(ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, "dt='a'");
        candidates.get(0).assertAttributeEquals(ListHiveCompactionCandidates.ATTR_RECOMMENDED_TYPE, "MAJOR");
        candidates.get(0).assertAttributeEquals(ListHiveCompactionCandidates.ATTR_DELTA_COUNT, "3");
        candidates.get(1).assertAttributeEquals(ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, "dt='b'");
        assertTrue(candidates.get(0).getAttribute(ListHiveCompactionCandidates.ATTR_PARTITION_LOCATION).endsWith("it_cand/dt=a"));

        // 후보를 그대로 CompactHive3Table 로
        final TestRunner chained = runner();
        chained.setProperty(CompactHive3Table.DATABASE, "${hive.compaction.database}");
        chained.setProperty(CompactHive3Table.TABLE, "${hive.compaction.table}");
        chained.setProperty(CompactHive3Table.PARTITION_SPEC, "${hive.compaction.partition.spec}");
        chained.setProperty(CompactHive3Table.COMPLETION_STATE, majorCompletionState().name());
        final Drive drive = drive(chained, candidates.get(0).getAttributes());
        assertRoutedTo(drive, CompactHive3Table.REL_SUCCESS);

        final TestRunner again = listRunner("it_cand");
        again.setProperty(ListHiveCompactionCandidates.MINIMUM_PARTITION_AGE, "0 sec");
        again.setProperty(ListHiveCompactionCandidates.PARTITION_FILTER, "dt='a'");
        again.run();
        assertEquals(0, again.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).size(), "합친 파티션은 더 후보가 아니다");
    }

    @Test
    @Order(9)
    void rebalance() throws Exception {
        sql("CREATE TABLE it_rebal (id INT, v STRING) PARTITIONED BY (dt STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')");
        insertRows("it_rebal", "dt='a'", 1, 10);
        insertRows("it_rebal", "dt='a'", 11, 20);
        final TestRunner runner = runner();
        runner.setProperty(CompactHive3Table.COMPACTION_TYPE, "REBALANCE");
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "3");
        final Drive drive = drive(runner, target("it_rebal", "dt='a'"));
        if (!supportsRebalance()) {
            assertRoutedTo(drive, CompactHive3Table.REL_FAILURE);
            assertTrue(drive.result().getAttribute(CompactHive3Table.ATTR_ERROR).contains("Hive 4"));
            return;
        }
        assertRoutedTo(drive, CompactHive3Table.REL_SUCCESS);
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "REBALANCE");
        drive.result().assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "20");
        final List<String> files = newestBaseFiles("it_rebal/dt=a");
        System.out.printf("[%s] it_rebal dirs=%s newest base files=%s%n", getClass().getSimpleName(), listDir("it_rebal/dt=a"), files);
        assertEquals(3, files.size(), files.toString());
        final String id = drive.result().getAttribute(CompactHive3Table.ATTR_ID);
        assertTrue(query("SHOW COMPACTIONS").stream().anyMatch(row -> row.get(0).equals(id) && row.contains("REBALANCE")));
    }
}
