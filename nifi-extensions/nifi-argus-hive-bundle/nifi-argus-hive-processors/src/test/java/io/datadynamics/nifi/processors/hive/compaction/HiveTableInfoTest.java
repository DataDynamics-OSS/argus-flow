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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hive 3.1.3 컨테이너의 DESCRIBE FORMATTED 출력(값 양옆 공백 포함)을 해석한다.
 */
class HiveTableInfoTest {

    private static String[] row(final String... cells) {
        return cells;
    }

    private static List<String[]> describe(final String inputFormat, final String tableType, final String... partitionColumns) {
        final List<String[]> rows = new ArrayList<>();
        rows.add(row("# col_name            ", "data_type           ", "comment             "));
        rows.add(row("id                  ", "int                 ", ""));
        rows.add(row("v                   ", "string              ", ""));
        rows.add(row("", null, null));
        if (partitionColumns.length > 0) {
            rows.add(row("# Partition Information", null, null));
            rows.add(row("# col_name            ", "data_type           ", "comment             "));
            for (final String column : partitionColumns) {
                rows.add(row(column + "                  ", "string              ", ""));
            }
            rows.add(row("", null, null));
        }
        rows.add(row("# Detailed Table Information", null, null));
        rows.add(row("Database:           ", "default             ", null));
        rows.add(row("Location:           ", "file:/opt/hive/data/warehouse/t_acid", null));
        rows.add(row("Table Type:         ", tableType + "       ", null));
        rows.add(row("Table Parameters:", null, null));
        rows.add(row("", "transactional       ", "true                "));
        rows.add(row("", null, null));
        rows.add(row("# Storage Information", null, null));
        rows.add(row("SerDe Library:      ", "org.apache.hadoop.hive.ql.io.orc.OrcSerde", null));
        rows.add(row("InputFormat:        ", inputFormat, null));
        rows.add(row("Storage Desc Params:", null, null));
        rows.add(row("", "serialization.format", "1                   "));
        return rows;
    }

    private static List<String[]> props(final String... keyValues) {
        final List<String[]> rows = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            rows.add(row(keyValues[i], keyValues[i + 1]));
        }
        return rows;
    }

    @Test
    void fullAcidPartitionedTable() {
        final HiveTableInfo info = HiveTableInfo.parse(
                props("bucketing_version", "2", "transactional", "true", "transactional_properties", "default"),
                describe("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "MANAGED_TABLE", "dt", "hour"));
        assertEquals(HiveTableInfo.Kind.FULL_ACID, info.kind());
        assertTrue(info.isAcid());
        assertEquals(List.of("dt", "hour"), info.partitionColumns());
        assertTrue(info.isManaged());
        assertEquals("MANAGED_TABLE", info.tableType());
    }

    @Test
    void insertOnlyAcidTable() {
        final HiveTableInfo info = HiveTableInfo.parse(props("TRANSACTIONAL", "TRUE", "transactional_properties", "insert_only"),
                describe("org.apache.hadoop.mapred.TextInputFormat", "MANAGED_TABLE"));
        assertEquals(HiveTableInfo.Kind.INSERT_ONLY_ACID, info.kind());
        assertFalse(info.isPartitioned());
    }

    @Test
    void nonAcidTables() {
        assertEquals(HiveTableInfo.Kind.NON_ACID_ORC, HiveTableInfo.parse(props("bucketing_version", "2"),
                describe("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "MANAGED_TABLE")).kind());
        assertEquals(HiveTableInfo.Kind.NON_ACID_ORC, HiveTableInfo.parse(props("transactional", "false"),
                describe("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "MANAGED_TABLE")).kind());
        final HiveTableInfo text = HiveTableInfo.parse(props(), describe("org.apache.hadoop.mapred.TextInputFormat", "MANAGED_TABLE"));
        assertEquals(HiveTableInfo.Kind.NON_ACID_OTHER, text.kind());
        assertEquals("org.apache.hadoop.mapred.TextInputFormat", text.inputFormat());
        final HiveTableInfo external = HiveTableInfo.parse(props("EXTERNAL", "TRUE"),
                describe("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "EXTERNAL_TABLE", "dt"));
        assertFalse(external.isManaged());
        assertEquals(List.of("dt"), external.partitionColumns());
    }
}
