/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/db/impl/GenericDatabaseAdapter.java
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

/**
 * ANSI SQL 표준을 따르는 SQL을 생성하는 범용(generic) 데이터베이스 어댑터.
 * 특정 DB 방언(dialect) 전용 기능(UPSERT 등)은 지원하지 않으며, DatabaseAdapter 인터페이스의 기본 구현을
 * 대부분 그대로 사용한다. 다른 DB 전용 어댑터(MySQL/PostgreSQL 등)의 상위 클래스로도 사용된다.
 * 페이징은 ANSI 표준에 가까운 LIMIT/OFFSET 절로 구현한다.
 */
public class GenericDatabaseAdapter implements DatabaseAdapter {
    @Override
    public String getName() {
        return "Generic";
    }

    @Override
    public String getDescription() {
        return "ANSI SQL을 생성합니다";
    }

    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset, null);
    }

    /**
     * ANSI SQL 방언의 SELECT 문을 생성한다.
     * columnForPartitioning이 지정된 경우, LIMIT/OFFSET 대신 해당 컬럼 값의 범위(>= offset, < offset+limit)를
     * WHERE 절에 추가하는 방식으로 파티셔닝(구간 분할) 조회를 수행한다. 이는 행 번호 기반 페이징보다
     * 대용량 테이블에서 성능이 안정적이기 때문이다. columnForPartitioning이 없으면 표준 LIMIT/OFFSET을 사용한다.
     */
    @Override
    public String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset, String columnForPartitioning) {
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
            // 표준 LIMIT/OFFSET 절을 사용한 페이징. (MySQL/PostgreSQL 등이 이 방식을 그대로 상속받아 사용한다)
            if (limit != null) {
                query.append(" LIMIT ");
                query.append(limit);
            }
            if (offset != null && offset > 0) {
                query.append(" OFFSET ");
                query.append(offset);
            }
        }
        return query.toString();
    }
}
