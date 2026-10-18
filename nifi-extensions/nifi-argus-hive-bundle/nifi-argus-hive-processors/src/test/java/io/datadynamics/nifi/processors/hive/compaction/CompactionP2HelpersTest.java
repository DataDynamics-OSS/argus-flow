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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P2(REBALANCE·REWRITE·후보 탐색) 보조 기능.
 */
class CompactionP2HelpersTest {

    private static final PartitionSpec DT = PartitionSpec.parse("dt='2026-10-01'");

    @Test
    void fileCountFromSize() {
        assertEquals(1, FileCountPlanner.fromSize(0, 512));
        assertEquals(1, FileCountPlanner.fromSize(512, 512));
        assertEquals(2, FileCountPlanner.fromSize(513, 512));
        assertEquals(3, FileCountPlanner.fromSize(1536, 512));
        assertEquals(2048, FileCountPlanner.fromSize(1L << 40, 512L << 20));
        assertThrows(IllegalArgumentException.class, () -> FileCountPlanner.fromSize(1, 0));
        assertThrows(IllegalArgumentException.class, () -> FileCountPlanner.fromSize(-1, 10));
    }

    @Test
    void buildsRebalanceAndRewriteStatements() {
        assertEquals("ALTER TABLE `db`.`t` PARTITION (`dt`='2026-10-01') COMPACT 'rebalance' CLUSTERED INTO 4 BUCKETS",
                CompactionSql.rebalance("db", "t", DT, 4));
        assertEquals("ALTER TABLE `db`.`t` COMPACT 'rebalance' CLUSTERED INTO 1 BUCKETS", CompactionSql.rebalance("db", "t", PartitionSpec.empty(), 1));
        assertThrows(IllegalArgumentException.class, () -> CompactionSql.rebalance("db", "t", DT, 0));
        assertThrows(IllegalArgumentException.class, () -> CompactionSql.compact("db", "t", DT, CompactionType.REBALANCE));
        assertEquals("INSERT OVERWRITE TABLE `db`.`t` PARTITION (`dt`='2026-10-01') SELECT `id`, `name`, `amount` FROM `db`.`t` "
                        + "WHERE `dt` = '2026-10-01' DISTRIBUTE BY `id`",
                CompactionSql.rewrite("db", "t", DT, List.of("id", "name", "amount"), List.of("id")));
        assertEquals("INSERT OVERWRITE TABLE `db`.`t` SELECT `id` FROM `db`.`t` DISTRIBUTE BY `id`, `k`",
                CompactionSql.rewrite("db", "t", PartitionSpec.empty(), List.of("id"), List.of("id", "k")));
        assertThrows(IllegalArgumentException.class, () -> CompactionSql.rewrite("db", "t", DT, List.of("id"), List.of("rand()")));
        assertThrows(IllegalArgumentException.class, () -> CompactionSql.rewrite("db", "t", DT, List.of(), List.of("id")));
        assertThrows(IllegalArgumentException.class, () -> CompactionSql.rewrite("db", "t", DT, List.of("id"), List.of()));
        assertEquals("DESCRIBE FORMATTED `db`.`t` PARTITION (`dt`='2026-10-01')", CompactionSql.describeFormatted("db", "t", DT));
        assertEquals("SHOW PARTITIONS `db`.`t` PARTITION (`dt`='2026-10-01')", CompactionSql.showPartitions("db", "t", DT));
        assertEquals("SHOW PARTITIONS `db`.`t`", CompactionSql.showPartitions("db", "t", PartitionSpec.empty()));
    }

    private static String[] row(final String... cells) {
        return cells;
    }

    /** apache/hive:3.1.3 의 DESCRIBE FORMATTED r PARTITION (dt='a') 출력. */
    static List<String[]> hive3PartitionDescribe() {
        final List<String[]> rows = new ArrayList<>();
        rows.add(row("# col_name", "data_type", "comment"));
        rows.add(row("id", "int", ""));
        rows.add(row("v", "string", ""));
        rows.add(row("", null, null));
        rows.add(row("# Partition Information", null, null));
        rows.add(row("# col_name", "data_type", "comment"));
        rows.add(row("dt", "string", ""));
        rows.add(row("", null, null));
        rows.add(row("# Detailed Partition Information", null, null));
        rows.add(row("Partition Value:    ", "[a]                 ", null));
        rows.add(row("Database:           ", "default             ", null));
        rows.add(row("Table:              ", "r                   ", null));
        rows.add(row("Location:           ", "file:/data/warehouse/r/dt=a", null));
        rows.add(row("Partition Parameters:", null, null));
        rows.add(row("", "COLUMN_STATS_ACCURATE", "{}                  "));
        rows.add(row("", "numFiles            ", "2                   "));
        rows.add(row("", "totalSize           ", "1464                "));
        rows.add(row("", null, null));
        rows.add(row("# Storage Information", null, null));
        rows.add(row("InputFormat:        ", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", null));
        rows.add(row("Num Buckets:        ", "-1                  ", null));
        rows.add(row("Bucket Columns:     ", "[]                  ", null));
        return rows;
    }

    @Test
    void parsesColumnsLocationBucketsAndStatistics() {
        final HiveTableInfo partition = HiveTableInfo.parseDescribe(hive3PartitionDescribe(), Map.of("transactional", "true"));
        assertEquals(List.of("id", "v"), partition.columns());
        assertEquals(List.of("dt"), partition.partitionColumns());
        assertEquals("file:/data/warehouse/r/dt=a", partition.location());
        assertEquals(1464, partition.totalSize());
        assertFalse(partition.isBucketed());
        assertTrue(partition.isAcid());

        final List<String[]> bucketed = new ArrayList<>(hive3PartitionDescribe());
        bucketed.removeIf(r -> r[0].startsWith("Num Buckets") || r.length > 1 && "totalSize           ".equals(r[1]));
        bucketed.add(row("Num Buckets:        ", "4                   ", null));
        final HiveTableInfo info = HiveTableInfo.parseDescribe(bucketed, Map.of());
        assertTrue(info.isBucketed());
        assertEquals(4, info.numBuckets());
        assertEquals(-1, info.totalSize());
        assertNull(info.property("transactional"));
    }

    @Test
    void partitionSpecFromHivePartitionName() {
        final PartitionSpec spec = PartitionSpec.fromPartitionName("dt=2026-10-01/hour=03");
        assertEquals("dt='2026-10-01',hour='03'", spec.toCanonicalString());
        assertEquals("p='a:b/c'", PartitionSpec.fromPartitionName("p=a%3Ab%2Fc").toCanonicalString());
        assertTrue(PartitionSpec.fromPartitionName(null).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> PartitionSpec.fromPartitionName("nokey"));
        assertTrue(PartitionSpec.parse("dt='a'").isSubsetOf(List.of("dt", "hour")));
        assertFalse(PartitionSpec.parse("minute='1'").isSubsetOf(List.of("dt", "hour")));
        assertTrue(PartitionSpec.empty().isSubsetOf(List.of("dt")));
    }
}
