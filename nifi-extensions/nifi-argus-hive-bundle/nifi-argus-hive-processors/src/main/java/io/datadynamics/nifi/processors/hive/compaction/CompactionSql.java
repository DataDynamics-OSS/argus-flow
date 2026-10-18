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

import java.util.Locale;

/**
 * {@code CompactHive3Table} 이 실행하는 HiveQL 을 만든다. 식별자는 {@link HiveIdentifiers} 로 검증·인용하고,
 * 파티션 값은 {@link PartitionSpec} 이 이스케이프한다. 문장 끝에 세미콜론을 붙이지 않는다(Hive JDBC 는 거부한다).
 */
public final class CompactionSql {

    /** 통계 기반 COUNT(*) 응답을 끈다. 켜 두면 행 수 검증이 무의미하다. */
    public static final String DISABLE_STATS_ANSWER = "SET hive.compute.query.using.stats=false";

    public static final String SHOW_COMPACTIONS = "SHOW COMPACTIONS";

    private CompactionSql() {
    }

    /**
     * {@code `db`.`table`}.
     */
    public static String qualifiedName(final String database, final String table) {
        return HiveIdentifiers.quote("데이터베이스", database) + "." + HiveIdentifiers.quote("테이블", table);
    }

    public static String showTblProperties(final String database, final String table) {
        return "SHOW TBLPROPERTIES " + qualifiedName(database, table);
    }

    public static String describeFormatted(final String database, final String table) {
        return "DESCRIBE FORMATTED " + qualifiedName(database, table);
    }

    public static String countRows(final String database, final String table, final PartitionSpec spec) {
        final String where = spec.toWhereClause();
        return "SELECT COUNT(*) FROM " + qualifiedName(database, table) + (where.isEmpty() ? "" : " " + where);
    }

    /**
     * {@code ALTER TABLE ... [PARTITION (...)] COMPACT 'major'|'minor'}. {@code AND WAIT} 는 쓰지 않는다.
     */
    public static String compact(final String database, final String table, final PartitionSpec spec, final CompactionType type) {
        return "ALTER TABLE " + qualifiedName(database, table) + partition(spec) + " COMPACT '" + type.name().toLowerCase(Locale.ROOT) + "'";
    }

    /**
     * {@code ALTER TABLE ... [PARTITION (...)] CONCATENATE}. 동기 실행이다.
     */
    public static String concatenate(final String database, final String table, final PartitionSpec spec) {
        return "ALTER TABLE " + qualifiedName(database, table) + partition(spec) + " CONCATENATE";
    }

    private static String partition(final PartitionSpec spec) {
        final String clause = spec.toPartitionClause();
        return clause.isEmpty() ? "" : " " + clause;
    }
}
