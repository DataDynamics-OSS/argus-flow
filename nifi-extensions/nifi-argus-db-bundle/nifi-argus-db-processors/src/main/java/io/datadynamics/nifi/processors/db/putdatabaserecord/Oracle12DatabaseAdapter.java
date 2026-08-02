/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/db/impl/Oracle12DatabaseAdapter.java
 */
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
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.OTHER;
import static java.sql.Types.SQLXML;
import static java.sql.Types.VARCHAR;

/**
 * Oracle 12c 이상 버전을 대상으로 하는 DatabaseAdapter.
 * {@link OracleDatabaseAdapter}(구버전 Oracle)와의 핵심 차이는 다음과 같다:
 * <li>페이징에 12c부터 지원되는 표준적인 "OFFSET ... ROWS FETCH NEXT ... ROWS ONLY" 구문을 사용한다
 * (ROWNUM 기반 중첩 SELECT가 필요 없다).</li>
 * <li>UPSERT를 지원하며, Oracle 고유의 "MERGE INTO ... USING ... ON ... WHEN [NOT] MATCHED" 구문으로 구현한다.</li>
 */
public class Oracle12DatabaseAdapter implements DatabaseAdapter {
    @Override
    public String getName() {
        return "Oracle 12+";
    }

    @Override
    public String getDescription() {
        return "Oracle 12 이상 버전에 호환되는 SQL을 생성합니다";
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause,
            Long limit, Long offset) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset, null);
    }

    /**
     * Oracle 12c 이상 방언의 SELECT 문을 생성한다. 구버전 Oracle과 달리 ROWNUM 중첩 SELECT 없이
     * "OFFSET n ROWS FETCH NEXT m ROWS ONLY" 구문으로 직접 페이징을 표현할 수 있다.
     */
    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause,
            Long limit, Long offset, String columnForPartitioning) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        final StringBuilder query = new StringBuilder("SELECT ");

        if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
            query.append("*");
        } else {
            query.append(columnNames);
        }
        query.append(" FROM ");
        query.append(tableName);

        if (!StringUtils.isEmpty(whereClause)) {
            query.append(" WHERE ");
            query.append(whereClause);
            if (!StringUtils.isEmpty(columnForPartitioning)) {
                query.append(" AND ");
                query.append(columnForPartitioning);
                query.append(" >= ");
                query.append(offset != null ? offset : "0");
                if (limit != null) {
                    query.append(" AND ");
                    query.append(columnForPartitioning);
                    query.append(" < ");
                    query.append((offset == null ? 0 : offset) + limit);
                }
            }
        }
        if (!StringUtils.isEmpty(orderByClause) && StringUtils.isEmpty(columnForPartitioning)) {
            query.append(" ORDER BY ");
            query.append(orderByClause);
        }
        if (StringUtils.isEmpty(columnForPartitioning)) {
            if (offset != null && offset > 0) {
                query.append(" OFFSET ");
                query.append(offset);
                query.append(" ROWS");
            }
            if (limit != null) {
                query.append(" FETCH NEXT ");
                query.append(limit);
                query.append(" ROWS ONLY");
            }
        }

        return query.toString();
    }

    @Override
    public String getTableAliasClause(String tableName) {
        return tableName;
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    /**
     * Oracle 12c 이상의 MERGE 구문을 이용해 UPSERT를 구현한다.
     * "MERGE INTO 테이블 USING (SELECT ? AS col1, ? AS col2, ... FROM DUAL) n ON (조건)
     * WHEN NOT MATCHED THEN INSERT (...) VALUES (...) WHEN MATCHED THEN UPDATE SET ..." 형태로,
     * DUAL 테이블에서 바인딩된 새 값들을 조회한 뒤 고유 키 조건으로 기존 행과 매칭시켜
     * 매칭되면 UPDATE, 아니면 INSERT를 수행한다.
     */
    @Override
    public String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames)
            throws IllegalArgumentException {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String newValuesAlias = "n";

        String columns = columnNames.stream().collect(Collectors.joining(", ? "));

        columns = "? " + columns;

        List<String> columnsAssignment = getColumnsAssignment(columnNames, newValuesAlias, table);

        List<String> conflictColumnsClause = getConflictColumnsClause(uniqueKeyColumnNames, columnsAssignment, table,
                newValuesAlias);
        String conflictClause = "(" + conflictColumnsClause.stream().collect(Collectors.joining(" AND ")) + ")";

        String insertStatement = columnNames.stream().collect(Collectors.joining(", "));
        String insertValues = newValuesAlias + "."
                + columnNames.stream().collect(Collectors.joining(", " + newValuesAlias + "."));

        columnsAssignment.removeAll(conflictColumnsClause);
        String updateStatement = columnsAssignment.stream().collect(Collectors.joining(", "));

        StringBuilder statementStringBuilder = new StringBuilder("MERGE INTO ").append(table).append(" USING (SELECT ")
                .append(columns).append(" FROM DUAL) ").append(newValuesAlias).append(" ON ").append(conflictClause)
                .append(" WHEN NOT MATCHED THEN INSERT (").append(insertStatement).append(") VALUES (")
                .append(insertValues).append(")").append(" WHEN MATCHED THEN UPDATE SET ").append(updateStatement);

        return statementStringBuilder.toString();
    }

    /**
     * MERGE 문의 ON 조건절(고유 키 컬럼들의 일치 조건)을 구성할 컬럼 할당 목록 중,
     * 실제 고유 키에 해당하는 것들만 추려낸다.
     */
    private List<String> getConflictColumnsClause(Collection<String> uniqueKeyColumnNames, List<String> conflictColumns,
            String table, String newTableAlias) {
        List<String> conflictColumnsClause = conflictColumns.stream()
                .filter(column -> uniqueKeyColumnNames.stream().anyMatch(
                        uniqueKey -> column.equalsIgnoreCase(getColumnAssignment(table, uniqueKey, newTableAlias))))
                .collect(Collectors.toList());

        // 개수가 일치하지 않으면(즉, 대소문자/언더스코어 차이로 매칭에 실패한 컬럼이 있으면) 정규화된 이름으로 재시도한다.
        if (conflictColumnsClause.size() != uniqueKeyColumnNames.size()) {

            // 정규화된 컬럼명으로 다시 시도
            conflictColumnsClause = conflictColumns.stream()
                    .filter((column -> uniqueKeyColumnNames.stream()
                            .anyMatch(uniqueKey -> normalizeColumnName(column).equalsIgnoreCase(
                                    normalizeColumnName(getColumnAssignment(table, uniqueKey, newTableAlias))))))
                    .collect(Collectors.toList());
        }

        return conflictColumnsClause;

    }

    private String normalizeColumnName(final String colName) {
        return colName == null ? null : colName.toUpperCase().replace("_", "");
    }

    private List<String> getColumnsAssignment(Collection<String> columnsNames, String newTableAlias, String table) {
        List<String> conflictClause = new ArrayList<>();

        for (String columnName : columnsNames) {

            StringBuilder statementStringBuilder = new StringBuilder();

            statementStringBuilder.append(getColumnAssignment(table, columnName, newTableAlias));

            conflictClause.add(statementStringBuilder.toString());

        }

        return conflictClause;
    }

    private String getColumnAssignment(String table, String columnName, String newTableAlias) {
        return table + "." + columnName + " = " + newTableAlias + "." + columnName;
    }

    /**
     * Oracle 문법에 맞게 "ALTER TABLE ... ADD (컬럼명 타입, ...)" 형태로 컬럼 추가 구문을 생성한다.
     */
    @Override
    public List<String> getAlterTableStatements(String tableName, List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
        StringBuilder createTableStatement = new StringBuilder();

        List<String> columnsAndDatatypes = new ArrayList<>(columnsToAdd.size());
        for (ColumnDescription column : columnsToAdd) {
            String dataType = getSQLForDataType(column.getDataType());
            StringBuilder sb = new StringBuilder()
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(dataType);
            columnsAndDatatypes.add(sb.toString());
        }

        createTableStatement.append("ALTER TABLE ")
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(tableName)
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(" ADD (")
                .append(String.join(", ", columnsAndDatatypes))
                .append(") ");

        return Collections.singletonList(createTableStatement.toString());
    }

    /**
     * Oracle에 맞는 컬럼 타입 이름으로 변환한다.
     * 문자열/CLOB/XML 계열 타입은 길이를 명시해야 하므로(Oracle 문서 기준 2000자) VARCHAR2(2000)으로 매핑한다.
     * VARCHAR 대신 VARCHAR2를 사용하는 이유는 두 타입 간 비교(comparison) 의미가 미묘하게 달라
     * 일관된 비교 시맨틱을 보장하기 위함이다.
     */
    @Override
    public String getSQLForDataType(int sqlType) {
        switch (sqlType) {
            case Types.DOUBLE:
                return "DOUBLE PRECISION";
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case CLOB:
            case NCLOB:
            case OTHER:
            case SQLXML:
                return "VARCHAR2(2000)";
            default:
                return JDBCType.valueOf(sqlType).getName();
        }
    }

}
