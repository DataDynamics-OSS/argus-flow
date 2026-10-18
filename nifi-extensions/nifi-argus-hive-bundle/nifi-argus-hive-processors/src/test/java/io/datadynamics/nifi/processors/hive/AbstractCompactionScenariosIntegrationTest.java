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
}
