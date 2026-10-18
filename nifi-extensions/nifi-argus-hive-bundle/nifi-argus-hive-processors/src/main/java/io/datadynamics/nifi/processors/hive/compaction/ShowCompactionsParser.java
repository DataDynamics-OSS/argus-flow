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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code SHOW COMPACTIONS} 결과를 컬럼 이름으로 해석한다.
 * <p>
 * 컬럼 순서와 구성은 Hive 버전마다 다르다. JDBC 컬럼 라벨은 Hive 3.1.3·4.0.1 모두 소문자로 붙여 쓴 이름이다.
 * <ul>
 *     <li>Hive 3.1: {@code compactionid, dbname, tabname, partname, type, state, workerid, starttime, duration, hadoopjobid}
 *     — 그리고 첫 행에 사람이 읽는 머리글({@code CompactionId, Database, Table, Partition, ..., Duration(ms), HadoopJobId})을
 *     데이터 행으로 돌려준다.</li>
 *     <li>Hive 4.0: {@code compactionid, dbname, tabname, partname, type, state, workerhost, workerid, enqueuetime, starttime,
 *     duration, hadoopjobid, errormessage, initiatorhost, initiatorid, poolname, txnid, nexttxnid, committime, hightestwriteid}.</li>
 * </ul>
 * 라벨은 소문자로 바꾸고 영문자·숫자 외 문자를 지운 뒤 별칭 표로 찾는다(예: {@code Duration(ms)} → {@code durationms} →
 * duration). id 칸이 숫자가 아닌 행(Hive 3.1 머리글 행)은 건너뛴다.
 */
public final class ShowCompactionsParser {

    enum Column {
        ID("compactionid", "id"),
        DATABASE("dbname", "database", "db", "databasename"),
        TABLE("tabname", "table", "tablename"),
        PARTITION("partname", "partition", "partitionname"),
        TYPE("type", "compactiontype"),
        STATE("state"),
        WORKER("workerid", "worker"),
        WORKER_HOST("workerhost"),
        ENQUEUE_TIME("enqueuetime"),
        START_TIME("starttime"),
        DURATION("duration", "durationms"),
        HADOOP_JOB_ID("hadoopjobid"),
        ERROR_MESSAGE("errormessage", "error");

        private final String[] aliases;

        Column(final String... aliases) {
            this.aliases = aliases;
        }
    }

    private static final Map<String, Column> ALIASES = new HashMap<>();

    static {
        for (final Column column : Column.values()) {
            for (final String alias : column.aliases) {
                ALIASES.put(alias, column);
            }
        }
    }

    private ShowCompactionsParser() {
    }

    /**
     * JDBC 결과를 읽는다. 커서를 끝까지 소비한다.
     *
     * @throws SQLException          JDBC 오류
     * @throws IllegalStateException id·DB·테이블·상태 컬럼을 찾지 못함
     */
    public static List<CompactionRecord> parse(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final List<String> labels = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            final String label = metaData.getColumnLabel(i);
            labels.add(label == null || label.isEmpty() ? metaData.getColumnName(i) : label);
        }
        final List<List<String>> rows = new ArrayList<>();
        while (resultSet.next()) {
            final List<String> row = new ArrayList<>(labels.size());
            for (int i = 1; i <= labels.size(); i++) {
                row.add(resultSet.getString(i));
            }
            rows.add(row);
        }
        return parse(labels, rows);
    }

    /**
     * 라벨과 행(문자열)으로 해석한다.
     */
    public static List<CompactionRecord> parse(final List<String> labels, final List<List<String>> rows) {
        final Map<Column, Integer> index = new HashMap<>();
        for (int i = 0; i < labels.size(); i++) {
            final Column column = ALIASES.get(normalizeLabel(labels.get(i)));
            if (column != null) {
                index.putIfAbsent(column, i);
            }
        }
        for (final Column required : new Column[]{Column.ID, Column.DATABASE, Column.TABLE, Column.STATE}) {
            if (!index.containsKey(required)) {
                throw new IllegalStateException("SHOW COMPACTIONS 결과에서 " + required.aliases[0] + " 컬럼을 찾지 못했다. 컬럼: " + labels);
            }
        }
        final List<CompactionRecord> records = new ArrayList<>();
        for (final List<String> row : rows) {
            final String idText = value(row, index.get(Column.ID));
            final Long id = parseLong(idText);
            if (id == null) {
                // Hive 3.1 머리글 행 등
                continue;
            }
            records.add(new CompactionRecord(id,
                    value(row, index.get(Column.DATABASE)),
                    value(row, index.get(Column.TABLE)),
                    value(row, index.get(Column.PARTITION)),
                    value(row, index.get(Column.TYPE)),
                    value(row, index.get(Column.STATE)),
                    value(row, index.get(Column.WORKER)),
                    value(row, index.get(Column.WORKER_HOST)),
                    value(row, index.get(Column.ENQUEUE_TIME)),
                    value(row, index.get(Column.START_TIME)),
                    value(row, index.get(Column.DURATION)),
                    value(row, index.get(Column.HADOOP_JOB_ID)),
                    value(row, index.get(Column.ERROR_MESSAGE))));
        }
        return records;
    }

    static String normalizeLabel(final String label) {
        if (label == null) {
            return "";
        }
        String normalized = label.toLowerCase(Locale.ROOT);
        // 일부 드라이버는 "테이블.컬럼" 형태로 돌려준다
        final int dot = normalized.lastIndexOf('.');
        if (dot >= 0) {
            normalized = normalized.substring(dot + 1);
        }
        return normalized.replaceAll("[^a-z0-9]", "");
    }

    private static String value(final List<String> row, final Integer index) {
        if (index == null || index >= row.size()) {
            return null;
        }
        final String raw = row.get(index);
        if (raw == null) {
            return null;
        }
        final String trimmed = raw.trim();
        if (trimmed.isEmpty() || trimmed.equals("---") || trimmed.equalsIgnoreCase("NULL")) {
            return null;
        }
        return trimmed;
    }

    private static Long parseLong(final String text) {
        if (text == null) {
            return null;
        }
        try {
            return Long.parseLong(text);
        } catch (final NumberFormatException e) {
            return null;
        }
    }
}
