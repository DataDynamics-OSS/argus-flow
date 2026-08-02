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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/db/DatabaseAdapter.java
 */
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import java.sql.JDBCType;
import java.util.*;

/**
 * RDBMS/JDBC 방언(dialect)별로 다르게 동작해야 하는 코드를 추상화한 인터페이스.
 * 데이터베이스마다 SELECT의 페이징 구문, UPSERT 구문, 식별자 인용 문자, 데이터 타입 매핑 방식이 다르기 때문에,
 * PutDatabaseRecord는 이 인터페이스를 통해 실제 SQL 생성을 각 DatabaseAdapter 구현체에 위임한다.
 * 대부분의 메서드는 ANSI SQL 표준을 따르는 기본(default) 구현을 제공하며, 각 구현체는 필요한 부분만 오버라이드한다.
 */
public interface DatabaseAdapter {

    String getName();

    String getDescription();

    /**
     * 지정된 절(clause)들을 적용한 SQL SELECT 문을 반환한다.
     *
     * @param tableName     조회할 테이블 이름
     * @param columnNames   테이블에서 가져올 컬럼 이름들
     * @param whereClause   문에 적용할 필터. WHERE 키워드는 포함하지 않아야 한다
     * @param orderByClause 결과 행 정렬에 사용할 컬럼/절. ORDER BY 키워드는 포함하지 않아야 한다
     * @param limit         LIMIT 절의 값 (즉, 반환할 행 수)
     * @param offset        OFFSET 절의 값 (즉, 건너뛸 행 수)
     * @return 지정된 절들이 적용된 SQL SELECT 문 문자열
     */
    String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset);

    /**
     * 지정된 절(clause)들을 적용한 SQL SELECT 문을 반환한다. 이 메서드를 오버라이드하는 경우,
     * 다른 오버로드 메서드들도 columnForPartitioning = false 로 이 메서드를 호출하도록 함께 오버라이드해야 한다.
     *
     * @param tableName             조회할 테이블 이름
     * @param columnNames           테이블에서 가져올 컬럼 이름들
     * @param whereClause           문에 적용할 필터. WHERE 키워드는 포함하지 않아야 한다
     * @param orderByClause         결과 행 정렬에 사용할 컬럼/절. ORDER BY 키워드는 포함하지 않아야 한다
     * @param limit                 LIMIT 절의 값 (즉, 반환할 행 수)
     * @param offset                OFFSET 절의 값 (즉, 건너뛸 행 수)
     * @param columnForPartitioning (선택) 컬럼명이 주어지면, limit/offset이 행 번호가 아니라 이 컬럼 값 자체를 기준으로 계산된다
     * @return 지정된 절들이 적용된 SQL SELECT 문 문자열
     */
    default String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset, String columnForPartitioning) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset);
    }

    /**
     * 이 어댑터가 UPSERT를 지원하는지 여부를 알려준다.
     *
     * @return UPSERT를 지원하면 true, 아니면 false
     */
    default boolean supportsUpsert() {
        return false;
    }

    /**
     * 이 어댑터가 INSERT_IGNORE를 지원하는지 여부를 알려준다.
     *
     * @return INSERT_IGNORE를 지원하면 true, 아니면 false
     */
    default boolean supportsInsertIgnore() {
        return false;
    }

    /**
     * PreparedStatement에 컬럼 값을 몇 번 바인딩해야 하는지 알려준다. MySQL과 같은 일부 DB는 UPSERT 구문에
     * 값을 두 번(INSERT 절과 UPDATE 절에 각각) 지정해야 하고, 다른 DB는 한 번만 지정하면 된다.
     *
     * @return UPSERT를 위해 PreparedStatement에 컬럼 값을 바인딩해야 하는 횟수. UPSERT를 지원하지 않으면 -1.
     */
    default int getTimesToAddColumnObjectsForUpsert() {
        return supportsUpsert() ? 1 : -1;
    }

    /**
     * SQL UPSERT 문(레코드가 존재하면 UPDATE, 없으면 INSERT)을 반환한다.
     * <br /><br />
     * 이를 수행하는 표준적인 방법이 없기 때문에 모든 어댑터가 지원하지는 않는다 - {@link #supportsUpsert()}와 함께 사용할 것!
     *
     * @param table                레코드를 갱신/삽입할 대상 테이블 이름.
     * @param columnNames          값을 채울 테이블 컬럼 이름들.
     * @param uniqueKeyColumnNames 고유 키(unique key)를 구성하는 컬럼 이름들.
     * @return 파라미터화된 jdbc SQL 문 문자열.
     * 파라미터의 순서와 개수는 전달된 컬럼 목록과 동일하다.
     */
    default String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        throw new UnsupportedOperationException("UPSERT is not supported for " + getName());
    }

    /**
     * SQL INSERT_IGNORE 문(레코드가 존재하면 무시, 없으면 INSERT)을 반환한다.
     * <br /><br />
     * 이를 수행하는 표준적인 방법이 없기 때문에 모든 어댑터가 지원하지는 않는다 - {@link #supportsInsertIgnore()}와 함께 사용할 것!
     *
     * @param table                레코드를 무시/삽입할 대상 테이블 이름.
     * @param columnNames          값을 채울 테이블 컬럼 이름들.
     * @param uniqueKeyColumnNames 고유 키(unique key)를 구성하는 컬럼 이름들.
     * @return 파라미터화된 jdbc SQL 문 문자열.
     * 파라미터의 순서와 개수는 전달된 컬럼 목록과 동일하다.
     */
    default String getInsertIgnoreStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        throw new UnsupportedOperationException("UPSERT is not supported for " + getName());
    }

    /**
     * <p>테이블/컬럼 이름과 같은 식별자 문자열을 감싸는 이스케이프 문자를 제거하여 순수한(bare) 식별자 문자열을 반환한다.</p>
     * <p>이 메서드의 기본 구현은 큰따옴표를 제거한다.
     * 대상 DB 엔진이 다른 이스케이프 문자를 사용한다면, 해당 DatabaseAdapter 구현체가 이 메서드를 오버라이드하여
     * 그 이스케이프 문자를 올바르게 제거하도록 해야 한다.</p>
     *
     * @param identifier 이스케이프 문자로 감싸져 있을 수 있는 식별자
     * @return 이스케이프가 제거된 식별자 문자열, 입력이 null이면 null
     */
    default String unwrapIdentifier(String identifier) {
        return identifier == null ? null : identifier.replaceAll("\"", "");
    }

    default String getTableAliasClause(String tableName) {
        return "AS " + tableName;
    }

    default String getTableQuoteString() {
        // ANSI 표준은 큰따옴표(double quote)이다
        return "\"";
    }

    default String getColumnQuoteString() {
        // ANSI 표준은 큰따옴표(double quote)이다
        return "\"";
    }

    default boolean supportsCreateTableIfNotExists() {
        return false;
    }

    /**
     * 지정된 테이블 스키마를 사용하여 CREATE TABLE 문을 생성한다.
     *
     * @param tableSchema      컬럼 정보를 포함한 테이블 스키마
     * @param quoteTableName   생성되는 DDL에서 테이블 이름을 인용(quote)할지 여부
     * @param quoteColumnNames 생성되는 DDL에서 컬럼 이름들을 인용(quote)할지 여부
     * @return 지정된 테이블을 생성하는 DDL 문자열
     */
    default String getCreateTableStatement(TableSchema tableSchema, boolean quoteTableName, boolean quoteColumnNames) {
        StringBuilder createTableStatement = new StringBuilder();

        List<ColumnDescription> columns = tableSchema.getColumnsAsList();
        List<String> columnsAndDatatypes = new ArrayList<>(columns.size());
        Set<String> primaryKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        for (ColumnDescription column : columns) {
            StringBuilder sb = new StringBuilder()
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(getSQLForDataType(column.getDataType()))
                    .append(column.isNullable() ? "" : " NOT NULL")
                    .append(primaryKeyColumnNames != null && primaryKeyColumnNames.contains(column.getColumnName()) ? " PRIMARY KEY" : "");
            columnsAndDatatypes.add(sb.toString());
        }

        createTableStatement.append("CREATE TABLE IF NOT EXISTS ")
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(tableSchema.getTableName())
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(" (")
                .append(String.join(", ", columnsAndDatatypes))
                .append(") ");

        return createTableStatement.toString();
    }

    /**
     * 기존 테이블에 새 컬럼들을 추가하는 ALTER TABLE 문(들)을 생성한다.
     * 여러 DB 방언에서 ADD COLUMN 구문의 표현이 다르므로(예: MySQL은 "ADD COLUMN", 기본값은 "ADD COLUMNS"),
     * 각 DatabaseAdapter 구현체가 필요에 따라 이 메서드를 오버라이드한다.
     */
    default List<String> getAlterTableStatements(String tableName, List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
        StringBuilder createTableStatement = new StringBuilder();

        List<String> columnsAndDatatypes = new ArrayList<>(columnsToAdd.size());
        for (ColumnDescription column : columnsToAdd) {
            StringBuilder sb = new StringBuilder()
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(getSQLForDataType(column.getDataType()));
            columnsAndDatatypes.add(sb.toString());
        }

        createTableStatement.append("ALTER TABLE ")
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(tableName)
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(" ADD COLUMNS (")
                .append(String.join(", ", columnsAndDatatypes))
                .append(") ");

        return Collections.singletonList(createTableStatement.toString());
    }

    /**
     * java.sql.Types의 SQL 타입 코드를 대상 DB 방언에서 사용하는 컬럼 타입 이름으로 변환한다.
     * 기본 구현은 JDBC 표준 이름을 그대로 사용하며, 각 DatabaseAdapter 구현체는 자신의 DB에 맞는
     * 타입 이름(예: Oracle의 VARCHAR2, MySQL/PostgreSQL의 TEXT 등)으로 오버라이드한다.
     */
    default String getSQLForDataType(int sqlType) {
        return JDBCType.valueOf(sqlType).getName();
    }
}