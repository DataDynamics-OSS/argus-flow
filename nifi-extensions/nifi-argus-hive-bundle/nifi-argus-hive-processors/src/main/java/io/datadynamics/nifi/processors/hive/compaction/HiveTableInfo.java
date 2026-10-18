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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@code SHOW TBLPROPERTIES} 와 {@code DESCRIBE FORMATTED} 결과에서 compaction 판별에 필요한 정보만 뽑은 것.
 * <p>
 * 두 결과 모두 행을 문자열 배열로 받는다(JDBC 와 분리해 단위 테스트하기 위해). Hive 3.1·4.0 은 {@code DESCRIBE FORMATTED}
 * 값 양옆을 공백으로 채우므로 모든 셀을 trim 한다.
 */
public final class HiveTableInfo {

    /**
     * 테이블 종류.
     */
    public enum Kind {
        /** {@code transactional=true}, {@code transactional_properties} 없음/{@code default}. */
        FULL_ACID,
        /** {@code transactional=true}, {@code transactional_properties=insert_only}. */
        INSERT_ONLY_ACID,
        /** 트랜잭션 테이블이 아니고 InputFormat 이 ORC. */
        NON_ACID_ORC,
        /** 트랜잭션 테이블이 아니고 ORC 도 아님. */
        NON_ACID_OTHER
    }

    private final Map<String, String> properties;
    private final List<String> columns;
    private final List<String> partitionColumns;
    private final String inputFormat;
    private final String tableType;
    private final String location;
    private final int numBuckets;
    private final Map<String, String> parameters;

    HiveTableInfo(final Map<String, String> properties, final List<String> columns, final List<String> partitionColumns,
                  final String inputFormat, final String tableType, final String location, final int numBuckets,
                  final Map<String, String> parameters) {
        this.properties = Collections.unmodifiableMap(properties);
        this.columns = List.copyOf(columns);
        this.partitionColumns = List.copyOf(partitionColumns);
        this.inputFormat = inputFormat;
        this.tableType = tableType;
        this.location = location;
        this.numBuckets = numBuckets;
        this.parameters = Collections.unmodifiableMap(parameters);
    }

    /**
     * @param tblPropertiesRows {@code SHOW TBLPROPERTIES} 행(이름, 값)
     * @param describeRows      {@code DESCRIBE FORMATTED} 행(col_name, data_type, comment)
     */
    public static HiveTableInfo parse(final List<String[]> tblPropertiesRows, final List<String[]> describeRows) {
        final Map<String, String> properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (final String[] row : tblPropertiesRows) {
            if (row.length >= 2 && cell(row, 0) != null && !cell(row, 0).isEmpty()) {
                properties.put(cell(row, 0), cell(row, 1));
            }
        }

        final DescribeResult describe = parseDescribe(describeRows);
        return new HiveTableInfo(properties, describe.columns, describe.partitionColumns, describe.inputFormat, describe.tableType,
                describe.location, describe.numBuckets, describe.parameters);
    }

    /**
     * {@code DESCRIBE FORMATTED <t> PARTITION (...)} 결과만으로 만든다(파티션 Location·파라미터용). 테이블 속성은 비어 있다.
     */
    public static HiveTableInfo parseDescribe(final List<String[]> describeRows, final Map<String, String> tableProperties) {
        final DescribeResult describe = parseDescribe(describeRows);
        final Map<String, String> properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        properties.putAll(tableProperties);
        return new HiveTableInfo(properties, describe.columns, describe.partitionColumns, describe.inputFormat, describe.tableType,
                describe.location, describe.numBuckets, describe.parameters);
    }

    private static final class DescribeResult {
        final List<String> columns = new ArrayList<>();
        final List<String> partitionColumns = new ArrayList<>();
        final Map<String, String> parameters = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String inputFormat;
        String tableType;
        String location;
        int numBuckets = -1;
    }

    private static DescribeResult parseDescribe(final List<String[]> describeRows) {
        final DescribeResult result = new DescribeResult();
        // 맨 앞 컬럼 구역 → (빈 행) → # Partition Information → (빈 행) → # Detailed ... Information
        boolean inColumnSection = true;
        boolean inPartitionSection = false;
        for (final String[] row : describeRows) {
            final String name = cell(row, 0);
            final String value = cell(row, 1);
            if (name == null) {
                continue;
            }
            if (name.startsWith("#")) {
                final String section = name.substring(1).trim().toLowerCase(Locale.ROOT);
                if (section.equals("partition information")) {
                    inPartitionSection = true;
                    inColumnSection = false;
                } else if (!section.equals("col_name")) {
                    inPartitionSection = false;
                    inColumnSection = false;
                }
                continue;
            }
            if (inColumnSection) {
                if (name.isEmpty()) {
                    inColumnSection = false;
                } else {
                    result.columns.add(name);
                }
                continue;
            }
            if (inPartitionSection) {
                if (name.isEmpty()) {
                    inPartitionSection = false;
                } else {
                    result.partitionColumns.add(name);
                }
                continue;
            }
            if (name.isEmpty()) {
                // Table Parameters / Partition Parameters / Storage Desc Params 의 키·값 행
                if (value != null && !value.isEmpty()) {
                    result.parameters.putIfAbsent(value, cell(row, 2));
                }
            } else if (name.equalsIgnoreCase("InputFormat:")) {
                result.inputFormat = value;
            } else if (name.equalsIgnoreCase("Table Type:")) {
                result.tableType = value;
            } else if (name.equalsIgnoreCase("Location:")) {
                result.location = value;
            } else if (name.equalsIgnoreCase("Num Buckets:")) {
                try {
                    result.numBuckets = value == null ? -1 : Integer.parseInt(value);
                } catch (final NumberFormatException e) {
                    result.numBuckets = -1;
                }
            }
        }
        return result;
    }

    private static String cell(final String[] row, final int index) {
        if (row == null || index >= row.length || row[index] == null) {
            return null;
        }
        return row[index].trim();
    }

    public Kind kind() {
        if ("true".equalsIgnoreCase(property("transactional"))) {
            return "insert_only".equalsIgnoreCase(property("transactional_properties")) ? Kind.INSERT_ONLY_ACID : Kind.FULL_ACID;
        }
        return isOrc() ? Kind.NON_ACID_ORC : Kind.NON_ACID_OTHER;
    }

    public boolean isAcid() {
        final Kind kind = kind();
        return kind == Kind.FULL_ACID || kind == Kind.INSERT_ONLY_ACID;
    }

    public boolean isOrc() {
        return inputFormat != null && inputFormat.toLowerCase(Locale.ROOT).contains("orcinputformat");
    }

    /**
     * {@code MANAGED_TABLE} 인지. {@code CONCATENATE} 는 managed 테이블에서만 된다.
     */
    public boolean isManaged() {
        return tableType == null || tableType.equalsIgnoreCase("MANAGED_TABLE");
    }

    public String property(final String name) {
        return properties.get(name);
    }

    public List<String> partitionColumns() {
        return partitionColumns;
    }

    public boolean isPartitioned() {
        return !partitionColumns.isEmpty();
    }

    public String inputFormat() {
        return inputFormat;
    }

    public String tableType() {
        return tableType;
    }

    /**
     * 파티션 컬럼을 뺀 데이터 컬럼(정의 순서).
     */
    public List<String> columns() {
        return columns;
    }

    /**
     * {@code Location:} (테이블 또는 파티션).
     */
    public String location() {
        return location;
    }

    /**
     * {@code CLUSTERED BY ... INTO N BUCKETS} 로 만든 테이블인지. Hive 3·4 의 비버킷 테이블은 {@code Num Buckets: -1}.
     */
    public boolean isBucketed() {
        return numBuckets > 0;
    }

    public int numBuckets() {
        return numBuckets;
    }

    /**
     * {@code Table Parameters}/{@code Partition Parameters} 값(예: {@code totalSize}).
     */
    public String parameter(final String name) {
        return parameters.get(name);
    }

    /**
     * {@code totalSize} 통계(없거나 숫자가 아니면 -1).
     */
    public long totalSize() {
        final String value = parameter("totalSize");
        try {
            return value == null ? -1L : Long.parseLong(value.trim());
        } catch (final NumberFormatException e) {
            return -1L;
        }
    }
}
