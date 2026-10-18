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

import io.datadynamics.nifi.trino.processors.MaintenanceSql.Operation;
import io.datadynamics.nifi.trino.processors.MaintenanceSql.SyncMode;
import io.datadynamics.nifi.trino.processors.MaintenanceSql.TableFormat;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MaintenanceSqlTest {

    private static String build(final TableFormat format, final Operation operation) {
        return MaintenanceSql.build(format, operation, "c", "s", "t", "128MB", null, "7d", SyncMode.ADD);
    }

    @Test
    void icebergOperations() {
        assertEquals("ALTER TABLE \"c\".\"s\".\"t\" EXECUTE optimize(file_size_threshold => '128MB')",
                build(TableFormat.ICEBERG, Operation.OPTIMIZE));
        assertEquals("ALTER TABLE \"c\".\"s\".\"t\" EXECUTE expire_snapshots(retention_threshold => '7d')",
                build(TableFormat.ICEBERG, Operation.EXPIRE_SNAPSHOTS));
        assertEquals("ALTER TABLE \"c\".\"s\".\"t\" EXECUTE remove_orphan_files(retention_threshold => '7d')",
                build(TableFormat.ICEBERG, Operation.REMOVE_ORPHAN_FILES));
        assertEquals("ANALYZE \"c\".\"s\".\"t\"", build(TableFormat.ICEBERG, Operation.ANALYZE));
    }

    @Test
    void deltaOperations() {
        assertEquals("ALTER TABLE \"c\".\"s\".\"t\" EXECUTE optimize(file_size_threshold => '128MB')",
                build(TableFormat.DELTA_LAKE, Operation.OPTIMIZE));
        assertEquals("CALL \"c\".system.vacuum('s', 't', '7d')", build(TableFormat.DELTA_LAKE, Operation.VACUUM));
        assertEquals("ANALYZE \"c\".\"s\".\"t\"", build(TableFormat.DELTA_LAKE, Operation.ANALYZE));
    }

    @Test
    void hiveOperations() {
        assertEquals("CALL \"c\".system.sync_partition_metadata('s', 't', 'ADD')",
                build(TableFormat.HIVE, Operation.SYNC_PARTITION_METADATA));
        assertEquals("CALL \"c\".system.sync_partition_metadata('s', 't', 'FULL')",
                MaintenanceSql.build(TableFormat.HIVE, Operation.SYNC_PARTITION_METADATA, "c", "s", "t", null, null, null, SyncMode.FULL));
        assertEquals("ANALYZE \"c\".\"s\".\"t\"", build(TableFormat.HIVE, Operation.ANALYZE));
    }

    @Test
    void optimizeWithPartitionFilter() {
        assertEquals("ALTER TABLE \"c\".\"s\".\"t\" EXECUTE optimize(file_size_threshold => '1GB') WHERE dt >= DATE '2026-10-01'",
                MaintenanceSql.build(TableFormat.ICEBERG, Operation.OPTIMIZE, "c", "s", "t", "1GB", " dt >= DATE '2026-10-01' ", "7d", null));
        assertThrows(IllegalArgumentException.class, () -> MaintenanceSql.build(TableFormat.ICEBERG, Operation.OPTIMIZE,
                "c", "s", "t", "1GB", "1 = 1; DROP TABLE x", "7d", null), "필터로 문장을 덧붙일 수 없다");
    }

    @Test
    void identifiersAndStringsAreEscaped() {
        assertEquals("ALTER TABLE \"ca\"\"t\".\"sch\"\"ema\".\"ta\"\"ble\" EXECUTE expire_snapshots(retention_threshold => '7d')",
                MaintenanceSql.build(TableFormat.ICEBERG, Operation.EXPIRE_SNAPSHOTS, "ca\"t", "sch\"ema", "ta\"ble", null, null, "7d", null));
        assertEquals("CALL \"de\"\"lta\".system.vacuum('o''brien', 'it''s', '7d')",
                MaintenanceSql.build(TableFormat.DELTA_LAKE, Operation.VACUUM, "de\"lta", "o'brien", "it's", null, null, "7d", null));
        assertEquals("\"a\"\"b\"", MaintenanceSql.quoteIdentifier("a\"b"));
        assertEquals("'a''b'", MaintenanceSql.quoteString("a'b"));
    }

    @Test
    void unsupportedCombinationsAreRejected() {
        final Set<String> supported = Set.of("ICEBERG:OPTIMIZE", "ICEBERG:EXPIRE_SNAPSHOTS", "ICEBERG:REMOVE_ORPHAN_FILES", "ICEBERG:ANALYZE",
                "DELTA_LAKE:OPTIMIZE", "DELTA_LAKE:VACUUM", "DELTA_LAKE:ANALYZE", "HIVE:SYNC_PARTITION_METADATA", "HIVE:ANALYZE");
        for (final TableFormat format : TableFormat.values()) {
            for (final Operation operation : EnumSet.allOf(Operation.class)) {
                final boolean expected = supported.contains(format.name() + ":" + operation.name());
                assertEquals(expected, operation.supports(format), format + ":" + operation);
                if (!expected) {
                    assertThrows(IllegalArgumentException.class, () -> build(format, operation), format + ":" + operation);
                }
            }
        }
    }

    @Test
    void argumentFormatsAreValidated() {
        assertThrows(IllegalArgumentException.class, () -> MaintenanceSql.build(TableFormat.ICEBERG, Operation.OPTIMIZE,
                "c", "s", "t", "128 megabytes", null, "7d", null));
        assertThrows(IllegalArgumentException.class, () -> MaintenanceSql.build(TableFormat.ICEBERG, Operation.EXPIRE_SNAPSHOTS,
                "c", "s", "t", null, null, "7 days", null));
        assertThrows(IllegalArgumentException.class, () -> MaintenanceSql.build(TableFormat.ICEBERG, Operation.ANALYZE,
                "c", " ", "t", null, null, null, null));
        assertTrue(MaintenanceSql.DATA_SIZE.matcher("1.5GB").matches());
        assertTrue(MaintenanceSql.DURATION.matcher("168h").matches());
    }
}
