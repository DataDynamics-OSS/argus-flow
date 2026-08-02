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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/db/TableSchema.java
 */
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * 대상 데이터베이스 테이블 하나의 스키마 정보(컬럼 목록, 필수 컬럼, 기본 키 컬럼, 식별자 인용 문자열)를
 * JDBC DatabaseMetaData로부터 조회하여 보관하는 클래스.
 * PutDatabaseRecord가 레코드를 SQL로 변환할 때 실제 테이블 구조와 대조하기 위해 사용한다.
 */
public class TableSchema {
    private final List<String> requiredColumnNames;
    private final Set<String> primaryKeyColumnNames;
    private final Map<String, ColumnDescription> columns;
    private final String quotedIdentifierString;
    private final String tableName;

    public TableSchema(final String tableName, final List<ColumnDescription> columnDescriptions, final boolean translateColumnNames,
                       final Set<String> primaryKeyColumnNames, final String quotedIdentifierString) {
        this.tableName = tableName;
        this.columns = new LinkedHashMap<>();
        this.primaryKeyColumnNames = primaryKeyColumnNames;
        this.quotedIdentifierString = quotedIdentifierString;

        this.requiredColumnNames = new ArrayList<>();
        for (final ColumnDescription desc : columnDescriptions) {
            columns.put(ColumnDescription.normalizeColumnName(desc.getColumnName(), translateColumnNames), desc);
            if (desc.isRequired()) {
                requiredColumnNames.add(desc.getColumnName());
            }
        }
    }

    /**
     * JDBC 연결을 통해 실제 데이터베이스로부터 테이블의 컬럼 메타데이터와 기본 키 정보를 조회하여
     * TableSchema를 구성한다. 컬럼 조회 결과가 비어 있으면 테이블 자체가 없는 것인지(TableNotFoundException),
     * 아니면 테이블은 있지만 권한 등의 이유로 컬럼 메타데이터만 못 가져온 것인지를 구분해서 처리한다.
     * updateKeys가 명시적으로 지정된 경우에는 DB의 실제 기본 키 대신 그 값을 기본 키 컬럼으로 사용한다
     * (예: 기본 키가 없는 테이블에 대해 UPSERT/UPDATE 기준 컬럼을 사용자가 직접 지정하는 경우).
     */
    public static TableSchema from(final Connection conn, final String catalog, final String schema, final String tableName,
                                   final boolean translateColumnNames, final String updateKeys, ComponentLog log) throws SQLException {
        final DatabaseMetaData dmd = conn.getMetaData();

        try (final ResultSet colrs = dmd.getColumns(catalog, schema, tableName, "%")) {
            final List<ColumnDescription> cols = new ArrayList<>();
            while (colrs.next()) {
                final ColumnDescription col = ColumnDescription.from(colrs);
                cols.add(col);
            }
            // 컬럼이 하나도 조회되지 않았다면, 테이블 자체가 존재하지 않는 것인지 확인한다.
            if (cols.isEmpty()) {
                try (final ResultSet tblrs = dmd.getTables(catalog, schema, tableName, null)) {
                    List<String> qualifiedNameSegments = new ArrayList<>();
                    if (catalog != null) {
                        qualifiedNameSegments.add(catalog);
                    }
                    if (schema != null) {
                        qualifiedNameSegments.add(schema);
                    }
                    if (tableName != null) {
                        qualifiedNameSegments.add(tableName);
                    }
                    if (!tblrs.next()) {

                        throw new TableNotFoundException("Table "
                                + String.join(".", qualifiedNameSegments)
                                + " not found, ensure the Catalog, Schema, and/or Table Names match those in the database exactly");
                    } else {
                        log.warn("Table "
                                + String.join(".", qualifiedNameSegments)
                                + " found but no columns were found, if this is not expected then check the user permissions for getting table metadata from the database");
                    }
                }
            }

            final Set<String> primaryKeyColumns = new HashSet<>();
            if (updateKeys == null) {
                // 사용자가 별도의 Update Keys를 지정하지 않았다면 DB 메타데이터의 실제 기본 키를 사용한다.
                try (final ResultSet pkrs = dmd.getPrimaryKeys(catalog, schema, tableName)) {

                    while (pkrs.next()) {
                        final String colName = pkrs.getString("COLUMN_NAME");
                        primaryKeyColumns.add(colName);
                    }
                }
            } else {
                // Update Keys 필드를 콤마 기준으로 파싱하고 컬럼명을 정규화한다.
                for (final String updateKey : updateKeys.split(",")) {
                    primaryKeyColumns.add(ColumnDescription.normalizeColumnName(updateKey.trim(), translateColumnNames));
                }
            }

            return new TableSchema(tableName, cols, translateColumnNames, primaryKeyColumns, dmd.getIdentifierQuoteString());
        }
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, ColumnDescription> getColumns() {
        return columns;
    }

    public List<ColumnDescription> getColumnsAsList() {
        return new ArrayList<>(columns.values());
    }

    public List<String> getRequiredColumnNames() {
        return requiredColumnNames;
    }

    public Set<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }

    public String getQuotedIdentifierString() {
        return quotedIdentifierString;
    }

    @Override
    public String toString() {
        return "TableSchema[columns=" + columns.values() + "]";
    }
}