/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/db/ColumnDescription.java
 */
package io.datadynamics.nifi.processors.db.putdatabaserecord;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC DatabaseMetaData로부터 조회한 테이블의 컬럼 하나에 대한 메타정보(이름, SQL 데이터 타입, 필수 여부,
 * 컬럼 크기, NULL 허용 여부)를 담는 불변 값 객체.
 * TableSchema가 테이블의 전체 컬럼 목록을 구성할 때 이 객체들을 사용한다.
 */
public class ColumnDescription {
    private final String columnName;
    private final int dataType;
    private final boolean required;
    private final Integer columnSize;
    private final boolean nullable;

    public ColumnDescription(final String columnName, final int dataType, final boolean required, final Integer columnSize, final boolean nullable) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.required = required;
        this.columnSize = columnSize;
        this.nullable = nullable;
    }

    /**
     * DatabaseMetaData#getColumns() 결과의 ResultSet 한 행으로부터 ColumnDescription을 생성한다.
     * NULL 허용 여부(IS_NULLABLE), 자동 증가 여부(IS_AUTOINCREMENT), 기본값(COLUMN_DEF) 정보를 종합하여
     * "이 컬럼이 INSERT 시 반드시 값이 필요한 필수 컬럼인지"를 판단한다.
     */
    public static ColumnDescription from(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData md = resultSet.getMetaData();
        List<String> columns = new ArrayList<>();

        for (int i = 1; i < md.getColumnCount() + 1; i++) {
            columns.add(md.getColumnName(i));
        }
        // COLUMN_DEF는 Oracle의 버그(NIFI-4279 참고)를 우회하기 위해 반드시 가장 먼저 읽어야 한다.
        // (다른 컬럼을 먼저 읽으면 Oracle 드라이버에서 COLUMN_DEF 값이 깨지는 문제가 있었음)
        final String defaultValue = resultSet.getString("COLUMN_DEF");
        final String columnName = resultSet.getString("COLUMN_NAME");
        final int dataType = resultSet.getInt("DATA_TYPE");
        final int colSize = resultSet.getInt("COLUMN_SIZE");

        final String nullableValue = resultSet.getString("IS_NULLABLE");
        final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
        String autoIncrementValue = "NO";

        // 일부 JDBC 드라이버는 IS_AUTOINCREMENT 컬럼 자체를 제공하지 않으므로 존재 여부를 먼저 확인한다.
        if (columns.contains("IS_AUTOINCREMENT")) {
            autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
        }

        final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
        // NULL을 허용하지 않고, 자동 증가 컬럼도 아니며, 기본값도 없는 경우에만 "필수 입력 컬럼"으로 간주한다.
        final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;

        return new ColumnDescription(columnName, dataType, required, colSize == 0 ? null : colSize, isNullable);
    }

    /**
     * 컬럼명을 정규화한다. translateColumnNames가 true이면 대소문자/언더스코어 차이로 인한 매칭 실패를
     * 방지하기 위해 대문자로 변환하고 언더스코어를 제거한 형태로 비교 가능하게 만든다.
     * (예: "user_id" 와 "USERID" 를 동일한 컬럼으로 인식시키기 위함)
     */
    public static String normalizeColumnName(final String colName, final boolean translateColumnNames) {
        return colName == null ? null : (translateColumnNames ? colName.toUpperCase().replace("_", "") : colName);
    }

    public int getDataType() {
        return dataType;
    }

    public String getColumnName() {
        return columnName;
    }

    public Integer getColumnSize() {
        return columnSize;
    }

    public boolean isRequired() {
        return required;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return "Column[name=" + columnName + ", dataType=" + dataType + ", required=" + required + ", columnSize=" + columnSize + "]";
    }
}