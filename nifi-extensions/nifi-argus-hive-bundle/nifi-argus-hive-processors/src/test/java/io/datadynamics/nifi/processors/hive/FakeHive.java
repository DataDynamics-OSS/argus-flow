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
package io.datadynamics.nifi.processors.hive;

import io.datadynamics.nifi.processors.hive.ScriptedHive3DBCPService.Result;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link CompactHive3TableTest} 용 Hive 흉내. 실제 컨테이너에서 관찰한 Hive 3.1.3·4.0.1 의 응답 모양
 * ({@code SHOW COMPACTIONS} 라벨·머리글 행, 중복 요청 처리, {@code DESCRIBE FORMATTED} 의 공백 채움)을 따른다.
 */
final class FakeHive {

    enum Version { HIVE3, HIVE4 }

    static final class Table {
        final Map<String, String> properties = new LinkedHashMap<>();
        final List<String> partitionColumns = new ArrayList<>();
        String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
        String tableType = "MANAGED_TABLE";
        long rowCount;
    }

    static final class Compaction {
        final long id;
        final String db;
        final String table;
        final String partition;
        final String type;
        String state = "initiated";
        String errorMessage;

        Compaction(final long id, final String db, final String table, final String partition, final String type) {
            this.id = id;
            this.db = db;
            this.table = table;
            this.partition = partition;
            this.type = type;
        }
    }

    private static final Pattern QUALIFIED = Pattern.compile("`(\\w+)`\\.`(\\w+)`");
    private static final Pattern PARTITION = Pattern.compile("PARTITION \\((.*)\\)");
    private static final Pattern PART_VALUE = Pattern.compile("`(\\w+)`='((?:[^'\\\\]|\\\\.)*)'");

    final Version version;
    final Map<String, Table> tables = new LinkedHashMap<>();
    final List<Compaction> compactions = new ArrayList<>();
    long nextId = 1;
    /** COMPACT 실행 시 만들지 않음(요청이 사라진 상황 흉내). */
    boolean dropRequests;
    /** COMPACT 실행 시 던질 오류. */
    Result compactError;
    /** CONCATENATE 뒤 행 수 변화(행 수 불일치 흉내). */
    long rowDeltaAfterCompaction;

    FakeHive(final Version version) {
        this.version = version;
    }

    Table acidTable(final String qualified, final String... partitionColumns) {
        final Table table = new Table();
        table.properties.put("bucketing_version", "2");
        table.properties.put("transactional", "true");
        table.properties.put("transactional_properties", "default");
        table.partitionColumns.addAll(List.of(partitionColumns));
        tables.put(qualified, table);
        return table;
    }

    Table plainTable(final String qualified, final String inputFormat, final String... partitionColumns) {
        final Table table = new Table();
        table.properties.put("bucketing_version", "2");
        table.inputFormat = inputFormat;
        table.partitionColumns.addAll(List.of(partitionColumns));
        tables.put(qualified, table);
        return table;
    }

    Compaction compaction(final long id) {
        return compactions.stream().filter(c -> c.id == id).findFirst().orElseThrow();
    }

    Compaction addCompaction(final String db, final String table, final String partition, final String type, final String state) {
        final Compaction compaction = new Compaction(nextId++, db, table, partition, type);
        compaction.state = state;
        compactions.add(compaction);
        return compaction;
    }

    Result handle(final String sql) {
        if (sql.startsWith("SET ")) {
            return Result.none();
        }
        if (sql.equals("SHOW COMPACTIONS")) {
            return showCompactions();
        }
        final Matcher qualified = QUALIFIED.matcher(sql);
        if (!qualified.find()) {
            return null;
        }
        final String db = qualified.group(1);
        final String tableName = qualified.group(2);
        final Table table = tables.get(db + "." + tableName);
        if (table == null) {
            return Result.error("Table not found " + db + "." + tableName, 10001);
        }
        if (sql.startsWith("SHOW TBLPROPERTIES")) {
            final List<List<String>> rows = new ArrayList<>();
            table.properties.forEach((k, v) -> rows.add(List.of(k, v)));
            return Result.rows(List.of("prpt_name", "prpt_value"), rows);
        }
        if (sql.startsWith("DESCRIBE FORMATTED")) {
            return describe(db, table);
        }
        if (sql.startsWith("SELECT COUNT(*)")) {
            return Result.rows(List.of("_c0"), List.of(List.of(Long.toString(table.rowCount))));
        }
        if (sql.endsWith("CONCATENATE")) {
            table.rowCount += rowDeltaAfterCompaction;
            return Result.none();
        }
        if (sql.contains(" COMPACT '")) {
            if (compactError != null) {
                return compactError;
            }
            final String partition = partitionName(sql);
            final String type = sql.substring(sql.lastIndexOf("COMPACT '") + 9, sql.length() - 1).toUpperCase(Locale.ROOT);
            for (final Compaction c : compactions) {
                if (c.db.equals(db) && c.table.equals(tableName) && java.util.Objects.equals(c.partition, partition)
                        && (c.state.equals("initiated") || c.state.equals("working"))) {
                    if (version == Version.HIVE4) {
                        return Result.error("Error while compiling statement: FAILED: Execution Error, return code 10432 from "
                                + "org.apache.hadoop.hive.ql.ddl.DDLTask. Compaction request for " + db + "." + tableName
                                + "CompactionId: -1 is refused, details: Compaction is already scheduled with state='" + c.state
                                + "' and id=" + c.id + ".", 10432);
                    }
                    // Hive 3.1 은 조용히 무시한다
                    return Result.none();
                }
            }
            if (!dropRequests) {
                compactions.add(new Compaction(nextId++, db, tableName, partition, type));
            }
            table.rowCount += rowDeltaAfterCompaction;
            return Result.none();
        }
        return null;
    }

    private static String partitionName(final String sql) {
        final Matcher partition = PARTITION.matcher(sql);
        if (!partition.find()) {
            return null;
        }
        final Matcher values = PART_VALUE.matcher(partition.group(1));
        final List<String> parts = new ArrayList<>();
        while (values.find()) {
            parts.add(values.group(1) + "=" + values.group(2).replace("\\'", "'").replace("\\\\", "\\"));
        }
        return String.join("/", parts);
    }

    private Result showCompactions() {
        final List<List<String>> rows = new ArrayList<>();
        if (version == Version.HIVE3) {
            rows.add(List.of("CompactionId", "Database", "Table", "Partition", "Type", "State", "Worker", "Start Time", "Duration(ms)", "HadoopJobId"));
            for (final Compaction c : compactions) {
                rows.add(List.of(Long.toString(c.id), c.db, c.table, c.partition == null ? " --- " : c.partition, c.type, c.state,
                        " --- ", " --- ", " --- ", "None"));
            }
            return Result.rows(List.of("compactionid", "dbname", "tabname", "partname", "type", "state", "workerid", "starttime",
                    "duration", "hadoopjobid"), rows);
        }
        for (final Compaction c : compactions) {
            rows.add(List.of(Long.toString(c.id), c.db, c.table, c.partition == null ? " --- " : c.partition, c.type, c.state,
                    " --- ", " --- ", "1792332698058", " --- ", " --- ", " --- ", c.errorMessage == null ? " --- " : c.errorMessage,
                    "host", "manual", "default", "0", "0", "0", " --- "));
        }
        return Result.rows(List.of("compactionid", "dbname", "tabname", "partname", "type", "state", "workerhost", "workerid",
                "enqueuetime", "starttime", "duration", "hadoopjobid", "errormessage", "initiatorhost", "initiatorid", "poolname",
                "txnid", "nexttxnid", "committime", "hightestwriteid"), rows);
    }

    private static Result describe(final String db, final Table table) {
        final List<List<String>> rows = new ArrayList<>();
        rows.add(List.of("# col_name            ", "data_type           ", "comment             "));
        rows.add(List.of("id                  ", "int                 ", ""));
        rows.add(java.util.Arrays.asList("", null, null));
        if (!table.partitionColumns.isEmpty()) {
            rows.add(java.util.Arrays.asList("# Partition Information", null, null));
            rows.add(List.of("# col_name            ", "data_type           ", "comment             "));
            for (final String column : table.partitionColumns) {
                rows.add(List.of(column + "                  ", "string              ", ""));
            }
            rows.add(java.util.Arrays.asList("", null, null));
        }
        rows.add(java.util.Arrays.asList("# Detailed Table Information", null, null));
        rows.add(java.util.Arrays.asList("Database:           ", db + "             ", null));
        rows.add(java.util.Arrays.asList("Table Type:         ", table.tableType + "       ", null));
        rows.add(java.util.Arrays.asList("Table Parameters:", null, null));
        table.properties.forEach((k, v) -> rows.add(List.of("", k + "   ", v + "   ")));
        rows.add(java.util.Arrays.asList("", null, null));
        rows.add(java.util.Arrays.asList("# Storage Information", null, null));
        rows.add(java.util.Arrays.asList("InputFormat:        ", table.inputFormat, null));
        rows.add(java.util.Arrays.asList("Storage Desc Params:", null, null));
        rows.add(List.of("", "serialization.format", "1                   "));
        return Result.rows(List.of("col_name", "data_type", "comment"), rows);
    }
}
