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

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * {@code CompactHive3Table} 이 실행하는 HiveQL 을 만든다. 식별자는 {@link HiveIdentifiers} 로 검증·인용하고,
 * 파티션 값은 {@link PartitionSpec} 이 이스케이프한다. 문장 끝에 세미콜론을 붙이지 않는다(Hive JDBC 는 거부한다).
 */
public final class CompactionSql {

    public static final String SHOW_COMPACTIONS = "SHOW COMPACTIONS";

    public static final String SELECT_VERSION = "SELECT version()";

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
     *
     * @throws IllegalArgumentException REBALANCE·REWRITE (각각 {@link #rebalance}, {@link #rewrite})
     */
    public static String compact(final String database, final String table, final PartitionSpec spec, final CompactionType type) {
        if (type != CompactionType.MAJOR && type != CompactionType.MINOR) {
            throw new IllegalArgumentException(type + " 는 COMPACT 'major'|'minor' 가 아니다");
        }
        return "ALTER TABLE " + qualifiedName(database, table) + partition(spec) + " COMPACT '" + type.name().toLowerCase(Locale.ROOT) + "'";
    }

    /**
     * {@code ALTER TABLE ... [PARTITION (...)] COMPACT 'rebalance' CLUSTERED INTO N BUCKETS} (Hive 4).
     */
    public static String rebalance(final String database, final String table, final PartitionSpec spec, final int buckets) {
        if (buckets < 1) {
            throw new IllegalArgumentException("버킷 수는 1 이상이어야 한다: " + buckets);
        }
        return "ALTER TABLE " + qualifiedName(database, table) + partition(spec) + " COMPACT 'rebalance' CLUSTERED INTO " + buckets + " BUCKETS";
    }

    /**
     * {@code INSERT OVERWRITE TABLE t [PARTITION (...)] SELECT <데이터 컬럼> FROM t [WHERE <파티션>] DISTRIBUTE BY <키>}.
     * 출력 파일 수는 세션의 리듀서 수({@code mapreduce.job.reduces})가 정한다.
     *
     * @param columns      파티션 컬럼을 뺀 데이터 컬럼(정의 순서)
     * @param distributeBy 분배 키 컬럼(식별자만, 함수·{@code rand()} 불가)
     */
    public static String rewrite(final String database, final String table, final PartitionSpec spec, final List<String> columns,
                                 final List<String> distributeBy) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("SELECT 할 데이터 컬럼이 없다");
        }
        if (distributeBy.isEmpty()) {
            throw new IllegalArgumentException("Distribute By Columns 가 비었다");
        }
        final String qualified = qualifiedName(database, table);
        final String select = columns.stream().map(c -> HiveIdentifiers.quote("컬럼", c)).collect(Collectors.joining(", "));
        final String distribute = distributeBy.stream().map(c -> HiveIdentifiers.quote("Distribute By 컬럼", c)).collect(Collectors.joining(", "));
        final String where = spec.toWhereClause();
        return "INSERT OVERWRITE TABLE " + qualified + partition(spec) + " SELECT " + select + " FROM " + qualified
                + (where.isEmpty() ? "" : " " + where) + " DISTRIBUTE BY " + distribute;
    }

    /**
     * {@code DESCRIBE FORMATTED t PARTITION (...)}. 빈 지정이면 테이블 DESCRIBE.
     */
    public static String describeFormatted(final String database, final String table, final PartitionSpec spec) {
        return describeFormatted(database, table) + partition(spec);
    }

    /**
     * {@code SHOW PARTITIONS t [PARTITION (부분 지정)]}.
     */
    public static String showPartitions(final String database, final String table, final PartitionSpec filter) {
        return "SHOW PARTITIONS " + qualifiedName(database, table) + partition(filter);
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
