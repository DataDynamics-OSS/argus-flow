/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/db/impl/OracleDatabaseAdapter.java
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

import org.apache.commons.lang3.StringUtils;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
 * Oracle 호환 SQL을 생성하는 DatabaseAdapter. 이 어댑터는 Oracle 12c 이전 버전까지 지원해야 하므로
 * OFFSET/FETCH 구문(12c 이상에서 도입) 대신 ROWNUM 의사 컬럼을 이용한 중첩 SELECT 방식으로 페이징을 구현하는 점이
 * {@link Oracle12DatabaseAdapter}와의 핵심 차이다. 또한 UPSERT를 지원하지 않는다(기본 구현 그대로 사용).
 */
public class OracleDatabaseAdapter implements DatabaseAdapter {
    @Override
    public String getName() {
        return "Oracle";
    }

    @Override
    public String getDescription() {
        return "Oracle과 호환되는 SQL을 생성합니다";
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset, null);
    }

    /**
     * Oracle(12c 미만 호환) 방언의 SELECT 문을 생성한다.
     * limit/offset이 지정되고 columnForPartitioning이 없는 경우, ROWNUM은 정렬 이전 단계에서 부여되는 특성 때문에
     * "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (원본 쿼리) a WHERE ROWNUM <= 상한) WHERE rnum > 하한" 형태의
     * 중첩 SELECT(nested select)로 감싸야 정확한 페이징 결과를 얻을 수 있다.
     */
    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset, String columnForPartitioning) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }

        final StringBuilder query = new StringBuilder();
        boolean nestedSelect = (limit != null || offset != null) && StringUtils.isEmpty(columnForPartitioning);
        if (nestedSelect) {
            // 결과를 ROWNUM으로 제한하기 위해 중첩 SELECT 쿼리가 필요하다.
            query.append("SELECT ");
            if (StringUtils.isEmpty(columnNames) || columnNames.trim().equals("*")) {
                query.append("*");
            } else {
                query.append(columnNames);
            }
            query.append(" FROM (SELECT a.*, ROWNUM rnum FROM (");
        }

        query.append("SELECT ");
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
        if (nestedSelect) {
            query.append(") a");
            long offsetVal = 0;
            if (offset != null) {
                offsetVal = offset;
            }
            if (limit != null) {
                query.append(" WHERE ROWNUM <= ");
                query.append(offsetVal + limit);
            }
            query.append(") WHERE rnum > ");
            query.append(offsetVal);
        }

        return query.toString();
    }

    @Override
    public String getTableAliasClause(String tableName) {
        return tableName;
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
