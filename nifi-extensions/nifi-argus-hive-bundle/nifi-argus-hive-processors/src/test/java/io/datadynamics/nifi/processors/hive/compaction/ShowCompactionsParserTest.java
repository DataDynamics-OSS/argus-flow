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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 컨테이너(apache/hive:3.1.3, 4.0.1)에서 Hive JDBC 로 받은 라벨·값 그대로 해석한다.
 */
class ShowCompactionsParserTest {

    static final List<String> HIVE3_LABELS = List.of("compactionid", "dbname", "tabname", "partname", "type", "state", "workerid",
            "starttime", "duration", "hadoopjobid");
    static final List<String> HIVE4_LABELS = List.of("compactionid", "dbname", "tabname", "partname", "type", "state", "workerhost",
            "workerid", "enqueuetime", "starttime", "duration", "hadoopjobid", "errormessage", "initiatorhost", "initiatorid", "poolname",
            "txnid", "nexttxnid", "committime", "hightestwriteid");

    @Test
    void parsesHive3LayoutAndSkipsHeaderRow() {
        final List<CompactionRecord> records = ShowCompactionsParser.parse(HIVE3_LABELS, List.of(
                List.of("CompactionId", "Database", "Table", "Partition", "Type", "State", "Worker", "Start Time", "Duration(ms)", "HadoopJobId"),
                List.of("3", "default", "t_acid", "dt=2026-10-01", "MINOR", "initiated", " --- ", " --- ", " --- ", "None"),
                List.of("1", "default", "t_acid", "dt=2026-10-01", "MAJOR", "succeeded", " --- ", "1792331574265", "10515", "job_local785333722_0001"),
                List.of("2", "default", "t_np", " --- ", "MINOR", "working", "5278ee40c331-66", "1792331580070", " --- ", "job_local1772322166_0002")));
        assertEquals(3, records.size());
        final CompactionRecord first = records.get(0);
        assertEquals(3, first.id());
        assertEquals("initiated", first.state());
        assertNull(first.worker());
        assertNull(first.errorMessage());
        assertEquals("10515", records.get(1).duration());
        assertNull(records.get(2).partition());
        assertEquals("5278ee40c331-66", records.get(2).worker());
    }

    @Test
    void parsesHive4Layout() {
        final List<CompactionRecord> records = ShowCompactionsParser.parse(HIVE4_LABELS, List.of(
                List.of("1", "default", "t_np", " --- ", "MINOR", "refused", "c1a72821b979", "64", "1792332548850", "1792332554948", "142",
                        " --- ", "None of the compaction thresholds met, compaction request is refused!", "c1a72821b979", "manual", "default",
                        "12", "0", "0", " --- ")));
        final CompactionRecord record = records.get(0);
        assertEquals("refused", record.state());
        assertEquals("c1a72821b979", record.workerHost());
        assertEquals("64", record.worker());
        assertEquals("1792332548850", record.enqueueTime());
        assertEquals("142", record.duration());
        assertEquals("None of the compaction thresholds met, compaction request is refused!", record.errorMessage());
    }

    @Test
    void labelsAreMatchedCaseAndSpaceInsensitivelyInAnyOrder() {
        final List<CompactionRecord> records = ShowCompactionsParser.parse(
                List.of("State", "Partition", "Table", "Database", "CompactionId", "Duration(ms)", "Error message", "Worker host", "x.Enqueue Time"),
                List.of(List.of("working", "dt=a", "T", "DB", "9", "5", "boom", "h1", "77")));
        final CompactionRecord record = records.get(0);
        assertEquals(9, record.id());
        assertEquals("DB", record.database());
        assertEquals("dt=a", record.partition());
        assertEquals("5", record.duration());
        assertEquals("boom", record.errorMessage());
        assertEquals("h1", record.workerHost());
        assertEquals("77", record.enqueueTime());
        assertEquals(true, record.isFor("db", "t", PartitionSpec.parse("dt='a'")));
    }

    @Test
    void missingRequiredColumnFails() {
        assertThrows(IllegalStateException.class, () -> ShowCompactionsParser.parse(List.of("dbname", "tabname", "state"), List.of()));
    }

    @Test
    void normalizesLabels() {
        assertEquals("durationms", ShowCompactionsParser.normalizeLabel("Duration(ms)"));
        assertEquals("enqueuetime", ShowCompactionsParser.normalizeLabel("compactions.Enqueue Time"));
        assertEquals("", ShowCompactionsParser.normalizeLabel(null));
    }
}
