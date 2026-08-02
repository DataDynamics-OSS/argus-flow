/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/db/impl/MySQLDatabaseAdapter.java
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

import org.apache.nifi.util.StringUtils;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
 * MySQL과 호환되는 SQL을 생성하는 데이터베이스 어댑터.
 * MySQL 고유 문법을 활용한다는 점이 다른 어댑터와의 핵심 차이다:
 * <li>식별자 인용에 백틱(`)을 사용한다 (ANSI 표준의 큰따옴표 대신).</li>
 * <li>UPSERT는 "INSERT ... ON DUPLICATE KEY UPDATE" 구문으로 구현한다. 이 구문 특성상
 * INSERT 절과 UPDATE 절에 컬럼 값을 각각 한 번씩, 총 두 번 바인딩해야 한다({@link #getTimesToAddColumnObjectsForUpsert()}).</li>
 * <li>INSERT_IGNORE는 "INSERT IGNORE INTO" 구문으로 구현한다.</li>
 * <li>가변 길이 문자열 계열 타입(VARCHAR, CLOB 등)은 길이 제약이 있는 문자열 타입 대신 TEXT로 매핑한다.</li>
 */
public class MySQLDatabaseAdapter extends GenericDatabaseAdapter {
    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public String getDescription() {
        return "MySQL과 호환되는 SQL을 생성합니다";
    }

    @Override
    public String unwrapIdentifier(String identifier) {
        // 큰따옴표와 백틱(`)을 모두 제거한다. MySQL은 표준 큰따옴표 대신 백틱으로 식별자를 감싸는 경우가 많기 때문이다.
        return identifier == null ? null : identifier.replaceAll("[\"`]", "");
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public boolean supportsInsertIgnore() {
        return true;
    }

    /**
     * PreparedStatement에 컬럼 값을 몇 번 바인딩해야 하는지 알려준다. MySQL과 같은 일부 DB는 UPSERT 구문에서
     * 값을 두 번(INSERT 절과 UPDATE 절에 각각) 지정해야 하고, 다른 DB는 한 번만 지정하면 된다.
     *
     * @return UPSERT를 위해 PreparedStatement에 컬럼 값을 바인딩해야 하는 횟수. UPSERT를 지원하지 않으면 -1.
     */
    @Override
    public int getTimesToAddColumnObjectsForUpsert() {
        return 2;
    }

    /**
     * MySQL 고유의 "INSERT INTO ... VALUES (...) ON DUPLICATE KEY UPDATE ..." 구문으로 UPSERT 문을 생성한다.
     * 별도의 MERGE/ON CONFLICT 구문이 없는 대신, 고유 키 충돌 시 자동으로 UPDATE 절이 적용되는 MySQL만의 방식이다.
     */
    @Override
    public String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String columns = columnNames.stream()
                .collect(Collectors.joining(", "));

        String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        List<String> updateValues = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            updateValues.add(columnNames.get(i) + " = ?");
        }
        String parameterizedUpdateValues = String.join(", ", updateValues);

        StringBuilder statementStringBuilder = new StringBuilder("INSERT INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")")
                .append(" ON DUPLICATE KEY UPDATE ")
                .append(parameterizedUpdateValues);
        return statementStringBuilder.toString();
    }

    /**
     * MySQL 고유의 "INSERT IGNORE INTO" 구문으로 INSERT_IGNORE 문을 생성한다.
     * 고유 키 충돌이 발생해도 오류를 내지 않고 해당 행을 조용히 무시(ignore)한다.
     */
    @Override
    public String getInsertIgnoreStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("Column names cannot be null or empty");
        }
        if (uniqueKeyColumnNames == null || uniqueKeyColumnNames.isEmpty()) {
            throw new IllegalArgumentException("Key column names cannot be null or empty");
        }

        String columns = columnNames.stream()
                .collect(Collectors.joining(", "));

        String parameterizedInsertValues = columnNames.stream()
                .map(__ -> "?")
                .collect(Collectors.joining(", "));

        StringBuilder statementStringBuilder = new StringBuilder("INSERT IGNORE INTO ")
                .append(table)
                .append("(").append(columns).append(")")
                .append(" VALUES ")
                .append("(").append(parameterizedInsertValues).append(")");
        return statementStringBuilder.toString();
    }

    @Override
    public String getTableQuoteString() {
        // MySQL은 ANSI 표준 큰따옴표 대신 백틱(`)으로 테이블/컬럼 식별자를 감싼다.
        return "`";
    }

    @Override
    public String getColumnQuoteString() {
        return "`";
    }

    @Override
    public boolean supportsCreateTableIfNotExists() {
        return true;
    }

    /**
     * MySQL 문법에 맞게 "ALTER TABLE ... ADD COLUMN 컬럼명 타입, ADD COLUMN ..." 형태로 컬럼 추가 구문을 생성한다.
     * 기본(DatabaseAdapter) 구현의 "ADD COLUMNS (...)" 괄호 묶음 형태와 달리, 각 컬럼마다 별도의 ADD COLUMN 절을 사용한다.
     */
    @Override
    public List<String> getAlterTableStatements(final String tableName, final List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
        List<String> columnsAndDatatypes = new ArrayList<>(columnsToAdd.size());
        for (ColumnDescription column : columnsToAdd) {
            String dataType = getSQLForDataType(column.getDataType());
            StringBuilder sb = new StringBuilder("ADD COLUMN ")
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(dataType);
            columnsAndDatatypes.add(sb.toString());
        }

        StringBuilder alterTableStatement = new StringBuilder();
        return Collections.singletonList(alterTableStatement.append("ALTER TABLE ")
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(tableName)
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(" ")
                .append(String.join(", ", columnsAndDatatypes))
                .toString());
    }

    /**
     * MySQL에 맞는 컬럼 타입 이름으로 변환한다.
     * 문자열/CLOB/XML 계열 타입은 길이 제한 문제를 피하기 위해 모두 TEXT로 매핑한다
     * (MySQL의 VARCHAR는 길이 지정이 필요하고 최대 길이 제약이 있는 반면 TEXT는 그런 제약이 없다).
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
                return "TEXT";
            default:
                return JDBCType.valueOf(sqlType).getName();
        }
    }
}
