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
package io.datadynamics.nifi.processors.hive.compaction;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompactionSqlAndRequestsTest {

    private static final PartitionSpec DT = PartitionSpec.parse("dt='2026-10-01'");

    @Test
    void buildsStatements() {
        assertEquals("SHOW TBLPROPERTIES `db`.`t`", CompactionSql.showTblProperties("db", "t"));
        assertEquals("DESCRIBE FORMATTED `db`.`t`", CompactionSql.describeFormatted("db", "t"));
        assertEquals("SELECT COUNT(*) FROM `db`.`t` WHERE `dt` = '2026-10-01'", CompactionSql.countRows("db", "t", DT));
        assertEquals("SELECT COUNT(*) FROM `db`.`t`", CompactionSql.countRows("db", "t", PartitionSpec.empty()));
        assertEquals("ALTER TABLE `db`.`t` PARTITION (`dt`='2026-10-01') COMPACT 'major'", CompactionSql.compact("db", "t", DT, CompactionType.MAJOR));
        assertEquals("ALTER TABLE `db`.`t` COMPACT 'minor'", CompactionSql.compact("db", "t", PartitionSpec.empty(), CompactionType.MINOR));
        assertEquals("ALTER TABLE `db`.`t` PARTITION (`dt`='2026-10-01') CONCATENATE", CompactionSql.concatenate("db", "t", DT));
    }

    @Test
    void rejectsUnsafeIdentifiers() {
        for (final String bad : new String[]{"", "a b", "a`b", "a.b", "t;drop", "테이블", null}) {
            assertThrows(IllegalArgumentException.class, () -> CompactionSql.compact("db", bad, DT, CompactionType.MAJOR), String.valueOf(bad));
            assertThrows(IllegalArgumentException.class, () -> CompactionSql.countRows(bad, "t", DT), String.valueOf(bad));
        }
        assertTrue(HiveIdentifiers.isValid("Sales_2026"));
    }

    private static CompactionRecord record(final long id, final String table, final String partition, final String state) {
        return new CompactionRecord(id, "default", table, partition, "MAJOR", state, null, null, null, null, null, null, null);
    }

    @Test
    void findsNewlyCreatedCompactionAfterMaxIdBefore() {
        final List<CompactionRecord> before = List.of(record(1, "t", "dt=2026-10-01", "succeeded"), record(2, "t", "dt=2026-10-02", "working"),
                record(3, "other", null, "initiated"));
        assertEquals(1, CompactionRequests.maxId(before, "default", "t", DT));
        assertEquals(0, CompactionRequests.maxId(before, "default", "t", PartitionSpec.parse("dt='x'")));

        final List<CompactionRecord> after = List.of(record(5, "t", "dt=2026-10-01", "initiated"), record(4, "t", "dt=2026-10-01", "initiated"),
                record(1, "t", "dt=2026-10-01", "succeeded"), record(6, "other", null, "initiated"));
        assertEquals(4, CompactionRequests.findRequested(after, 1, "DEFAULT", "T", DT).orElseThrow().id());
    }

    @Test
    void fallsBackToExistingInProgressCompactionWhenRequestWasDeduplicated() {
        final List<CompactionRecord> after = List.of(record(7, "t", "dt=2026-10-01", "working"), record(3, "t", "dt=2026-10-01", "succeeded"));
        assertEquals(7, CompactionRequests.findRequested(after, 7, "default", "t", DT).orElseThrow().id());
        final List<CompactionRecord> done = List.of(record(7, "t", "dt=2026-10-01", "ready for cleaning"));
        assertTrue(CompactionRequests.findRequested(done, 7, "default", "t", DT).isEmpty());
    }

    @Test
    void parsesAlreadyScheduledMessage() {
        // apache/hive:4.0.1 실제 메시지
        final String hive4 = "Error while compiling statement: FAILED: Execution Error, return code 10432 from org.apache.hadoop.hive.ql.ddl.DDLTask. "
                + "Compaction request for default.t_acidCompactionId: -1 is refused, details: Compaction is already scheduled with state='initiated' and id=2.";
        assertEquals(2, CompactionRequests.parseAlreadyScheduledId(hive4).getAsLong());
        assertEquals(15, CompactionRequests.parseAlreadyScheduledId("Compaction already enqueued with id 15").getAsLong());
        assertFalse(CompactionRequests.parseAlreadyScheduledId("Partition not found {dt=nope}").isPresent());
        assertFalse(CompactionRequests.parseAlreadyScheduledId(null).isPresent());
    }

    @Test
    void classifiesStates() {
        assertEquals(CompactionPhase.INITIATED, CompactionPhase.of("initiated"));
        assertEquals(CompactionPhase.WORKING, CompactionPhase.of(" Working "));
        assertEquals(CompactionPhase.READY_FOR_CLEANING, CompactionPhase.of("ready for cleaning"));
        assertEquals(CompactionPhase.READY_FOR_CLEANING, CompactionPhase.of("READY_FOR_CLEANING"));
        assertEquals(CompactionPhase.SUCCEEDED, CompactionPhase.of("succeeded"));
        for (final String failed : new String[]{"failed", "attempted", "did not initiate", "refused", "aborted"}) {
            assertEquals(CompactionPhase.FAILED, CompactionPhase.of(failed), failed);
        }
        assertEquals(CompactionPhase.UNKNOWN, CompactionPhase.of("something new"));
        assertEquals(CompactionPhase.UNKNOWN, CompactionPhase.of(null));

        assertTrue(CompactionPhase.READY_FOR_CLEANING.isComplete(CompletionState.READY_FOR_CLEANING));
        assertFalse(CompactionPhase.READY_FOR_CLEANING.isComplete(CompletionState.SUCCEEDED));
        assertTrue(CompactionPhase.SUCCEEDED.isComplete(CompletionState.SUCCEEDED));
        assertTrue(CompactionPhase.SUCCEEDED.isComplete(CompletionState.READY_FOR_CLEANING));
        assertFalse(CompactionPhase.WORKING.isComplete(CompletionState.READY_FOR_CLEANING));
    }
}
