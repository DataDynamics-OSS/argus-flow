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
    private final List<String> partitionColumns;
    private final String inputFormat;
    private final String tableType;

    HiveTableInfo(final Map<String, String> properties, final List<String> partitionColumns, final String inputFormat, final String tableType) {
        this.properties = Collections.unmodifiableMap(properties);
        this.partitionColumns = List.copyOf(partitionColumns);
        this.inputFormat = inputFormat;
        this.tableType = tableType;
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

        final List<String> partitionColumns = new ArrayList<>();
        String inputFormat = null;
        String tableType = null;
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
                } else if (!section.equals("col_name")) {
                    inPartitionSection = false;
                }
                continue;
            }
            if (inPartitionSection) {
                if (name.isEmpty()) {
                    inPartitionSection = false;
                } else {
                    partitionColumns.add(name);
                }
                continue;
            }
            if (name.equalsIgnoreCase("InputFormat:")) {
                inputFormat = value;
            } else if (name.equalsIgnoreCase("Table Type:")) {
                tableType = value;
            }
        }
        return new HiveTableInfo(properties, partitionColumns, inputFormat, tableType);
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
}
