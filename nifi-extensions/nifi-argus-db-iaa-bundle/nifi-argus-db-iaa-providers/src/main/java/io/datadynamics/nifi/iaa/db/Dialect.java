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
package io.datadynamics.nifi.iaa.db;

import java.util.Locale;

/**
 * JDBC URL 로부터 판별하는 DB 방언. 방언마다 스키마 DDL 이 다르다
 * (MariaDB 의 ENGINE=InnoDB, H2 의 MERGE INTO 등).
 */
public enum Dialect {

    POSTGRESQL("db/postgresql/V1__baseline.sql"),
    MARIADB("db/mariadb/V1__baseline.sql"),
    H2("db/h2/V1__baseline.sql");

    private final String baselineResource;

    Dialect(final String baselineResource) {
        this.baselineResource = baselineResource;
    }

    /** 이 방언의 baseline DDL classpath 경로. */
    public String getBaselineResource() {
        return baselineResource;
    }

    /**
     * JDBC URL 에서 방언을 판별한다. MySQL URL 도 MariaDB 로 취급한다 — MariaDB 드라이버가
     * 양쪽을 모두 처리하고 DDL 도 호환된다.
     *
     * @throws IllegalArgumentException 지원하지 않는 URL
     */
    public static Dialect fromJdbcUrl(final String jdbcUrl) {
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("JDBC URL 이 지정되지 않았습니다");
        }
        final String url = jdbcUrl.toLowerCase(Locale.ROOT);
        if (url.startsWith("jdbc:postgresql:")) {
            return POSTGRESQL;
        }
        if (url.startsWith("jdbc:mariadb:") || url.startsWith("jdbc:mysql:")) {
            return MARIADB;
        }
        if (url.startsWith("jdbc:h2:")) {
            return H2;
        }
        throw new IllegalArgumentException(
                "지원하지 않는 JDBC URL 입니다. postgresql 또는 mariadb 를 사용하십시오: " + jdbcUrl);
    }
}
