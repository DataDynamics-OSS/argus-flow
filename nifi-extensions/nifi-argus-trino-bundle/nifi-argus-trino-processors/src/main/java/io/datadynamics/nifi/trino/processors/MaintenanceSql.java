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

import org.apache.nifi.components.DescribedValue;

import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * MaintainTrinoTable 의 유지보수 SQL 생성기. 식별자는 큰따옴표로 감싸고 내부 {@code "}는 {@code ""}로,
 * 문자열 인자는 작은따옴표로 감싸고 내부 {@code '}는 {@code ''}로 이스케이프한다.
 */
public final class MaintenanceSql {

    public enum TableFormat implements DescribedValue {
        ICEBERG("ICEBERG", "Iceberg", "Trino Iceberg 커넥터 카탈로그."),
        DELTA_LAKE("DELTA_LAKE", "Delta Lake", "Trino Delta Lake 커넥터 카탈로그."),
        HIVE("HIVE", "Hive", "Trino Hive 커넥터 카탈로그.");

        private final String value;
        private final String displayName;
        private final String description;

        TableFormat(final String value, final String displayName, final String description) {
            this.value = value;
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public enum Operation implements DescribedValue {
        OPTIMIZE("OPTIMIZE", "OPTIMIZE (compaction)",
                "작은 파일을 File Size Threshold 기준으로 병합한다. ICEBERG·DELTA_LAKE.",
                EnumSet.of(TableFormat.ICEBERG, TableFormat.DELTA_LAKE)),
        EXPIRE_SNAPSHOTS("EXPIRE_SNAPSHOTS", "EXPIRE_SNAPSHOTS",
                "Retention Threshold 보다 오래된 스냅샷을 만료한다. ICEBERG.", EnumSet.of(TableFormat.ICEBERG)),
        REMOVE_ORPHAN_FILES("REMOVE_ORPHAN_FILES", "REMOVE_ORPHAN_FILES",
                "메타데이터가 참조하지 않는 오래된 파일을 지운다. ICEBERG.", EnumSet.of(TableFormat.ICEBERG)),
        VACUUM("VACUUM", "VACUUM",
                "Retention Threshold 보다 오래된, 더 이상 참조되지 않는 파일을 지운다. DELTA_LAKE.", EnumSet.of(TableFormat.DELTA_LAKE)),
        SYNC_PARTITION_METADATA("SYNC_PARTITION_METADATA", "SYNC_PARTITION_METADATA",
                "스토리지의 파티션 디렉터리와 메타스토어 파티션을 맞춘다. HIVE.", EnumSet.of(TableFormat.HIVE)),
        ANALYZE("ANALYZE", "ANALYZE", "테이블 통계를 수집한다. 모든 포맷.", EnumSet.allOf(TableFormat.class));

        private final String value;
        private final String displayName;
        private final String description;
        private final Set<TableFormat> formats;

        Operation(final String value, final String displayName, final String description, final Set<TableFormat> formats) {
            this.value = value;
            this.displayName = displayName;
            this.description = description;
            this.formats = formats;
        }

        public boolean supports(final TableFormat format) {
            return formats.contains(format);
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public enum SyncMode implements DescribedValue {
        ADD("ADD", "ADD", "스토리지에는 있고 메타스토어에 없는 파티션을 추가한다."),
        DROP("DROP", "DROP", "메타스토어에는 있고 스토리지에 없는 파티션을 제거한다."),
        FULL("FULL", "FULL", "ADD 와 DROP 을 모두 수행한다.");

        private final String value;
        private final String displayName;
        private final String description;

        SyncMode(final String value, final String displayName, final String description) {
            this.value = value;
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    /** Trino DataSize 형식(예: 128MB, 1.5GB). */
    static final Pattern DATA_SIZE = Pattern.compile("^\\d+(\\.\\d+)?\\s*(B|kB|MB|GB|TB|PB)$");
    /** Trino Duration 형식(예: 7d, 168h, 30m). */
    static final Pattern DURATION = Pattern.compile("^\\d+(\\.\\d+)?\\s*(ns|us|ms|s|m|h|d)$");

    private MaintenanceSql() {
    }

    /**
     * @throws IllegalArgumentException 포맷·작업 조합이 맞지 않거나 인자가 잘못된 경우
     */
    public static String build(final TableFormat format, final Operation operation, final String catalog, final String schema,
                               final String table, final String fileSizeThreshold, final String partitionFilter,
                               final String retentionThreshold, final SyncMode syncMode) {
        if (!operation.supports(format)) {
            throw new IllegalArgumentException(format.getValue() + " 테이블은 " + operation.getValue() + " 를 지원하지 않는다");
        }
        requireName("Catalog", catalog);
        requireName("Schema", schema);
        requireName("Table", table);
        final String qualified = quoteIdentifier(catalog) + "." + quoteIdentifier(schema) + "." + quoteIdentifier(table);

        final String sql = switch (operation) {
            case OPTIMIZE -> {
                final String size = requireMatch("File Size Threshold", fileSizeThreshold, DATA_SIZE);
                final String base = "ALTER TABLE " + qualified + " EXECUTE optimize(file_size_threshold => " + quoteString(size) + ")";
                yield partitionFilter == null || partitionFilter.isBlank() ? base : base + " WHERE " + partitionFilter.trim();
            }
            case EXPIRE_SNAPSHOTS -> "ALTER TABLE " + qualified + " EXECUTE expire_snapshots(retention_threshold => "
                    + quoteString(requireMatch("Retention Threshold", retentionThreshold, DURATION)) + ")";
            case REMOVE_ORPHAN_FILES -> "ALTER TABLE " + qualified + " EXECUTE remove_orphan_files(retention_threshold => "
                    + quoteString(requireMatch("Retention Threshold", retentionThreshold, DURATION)) + ")";
            case VACUUM -> "CALL " + quoteIdentifier(catalog) + ".system.vacuum(" + quoteString(schema) + ", " + quoteString(table)
                    + ", " + quoteString(requireMatch("Retention Threshold", retentionThreshold, DURATION)) + ")";
            case SYNC_PARTITION_METADATA -> {
                if (syncMode == null) {
                    throw new IllegalArgumentException("Sync Mode 가 필요하다");
                }
                yield "CALL " + quoteIdentifier(catalog) + ".system.sync_partition_metadata(" + quoteString(schema) + ", "
                        + quoteString(table) + ", " + quoteString(syncMode.getValue()) + ")";
            }
            case ANALYZE -> "ANALYZE " + qualified;
        };
        // Partition Filter 가 문장을 끝내고 다른 문장을 덧붙이지 못하게 한다
        return SqlStatements.requireSingleStatement(sql);
    }

    public static String quoteIdentifier(final String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    public static String quoteString(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static void requireName(final String label, final String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(label + " 가 비어 있다");
        }
    }

    private static String requireMatch(final String label, final String value, final Pattern pattern) {
        if (value == null || !pattern.matcher(value.trim()).matches()) {
            throw new IllegalArgumentException(label + " 형식이 잘못됐다: " + value);
        }
        return value.trim();
    }
}
