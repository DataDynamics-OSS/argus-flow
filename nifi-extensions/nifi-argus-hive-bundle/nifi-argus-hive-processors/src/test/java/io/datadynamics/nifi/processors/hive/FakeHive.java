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
        int numBuckets = -1;
        final List<String> columns = new ArrayList<>(List.of("id"));
        String location;
        long totalSize = -1;
        /** 파티션 이름(k=v/k2=v2) → Location. */
        final Map<String, String> partitions = new LinkedHashMap<>();
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

    final Version hiveVersion;
    final Map<String, Table> tables = new LinkedHashMap<>();
    final List<Compaction> compactions = new ArrayList<>();
    long nextId = 1;
    /** COMPACT 실행 시 만들지 않음(요청이 사라진 상황 흉내). */
    boolean dropRequests;
    /** COMPACT 실행 시 던질 오류. */
    Result compactError;
    /** CONCATENATE 뒤 행 수 변화(행 수 불일치 흉내). */
    long rowDeltaAfterCompaction;
    /** SELECT version() 결과. */
    String version;
    /** 세션 설정(SET/RESET). */
    final Map<String, String> session = new LinkedHashMap<>();
    final Map<String, String> sessionDefaults = new LinkedHashMap<>();
    /** 마지막 INSERT OVERWRITE 때의 mapreduce.job.reduces. */
    String rewriteReducers;
    /** INSERT OVERWRITE 때의 세션 설정 사본. */
    Map<String, String> rewriteSession;
    /** INSERT OVERWRITE 실행 시 던질 오류. */
    Result rewriteError;

    FakeHive(final Version version) {
        this.hiveVersion = version;
        this.version = version == Version.HIVE4 ? "4.0.1 r3af4517eb8cfd9407ad34ed78a0b48b57dfaa264" : "3.1.3 r4df4d75bf1e16fe0af75aad0b4179c34c07fc975";
        sessionDefaults.put("hive.compute.query.using.stats", "true");
        sessionDefaults.put("hive.tez.auto.reducer.parallelism", "false");
        sessionDefaults.put("mapreduce.job.reduces", "-1");
        sessionDefaults.put("hive.merge.tezfiles", "false");
        sessionDefaults.put("hive.merge.mapfiles", "true");
        sessionDefaults.put("hive.merge.mapredfiles", "false");
        session.putAll(sessionDefaults);
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
        if (sql.equals("SELECT version()")) {
            return Result.rows(List.of("_c0"), List.of(List.of(version)));
        }
        if (sql.startsWith("SET ")) {
            final String body = sql.substring(4).trim();
            final int eq = body.indexOf('=');
            if (eq > 0) {
                session.put(body.substring(0, eq), body.substring(eq + 1));
                return Result.none();
            }
            final String value = session.get(body);
            return Result.rows(List.of("set"), List.of(List.of(value == null ? body + " is undefined" : body + "=" + value)));
        }
        if (sql.startsWith("RESET ")) {
            final String key = sql.substring(6).trim();
            if (sessionDefaults.containsKey(key)) {
                session.put(key, sessionDefaults.get(key));
            } else {
                session.remove(key);
            }
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
            final String partition = partitionName(sql);
            if (partition != null) {
                final String location = table.partitions.get(partition);
                if (location == null) {
                    return Result.error("Partition not found {" + partition + "}", 10006);
                }
                return describe(db, table, location);
            }
            return describe(db, table, table.location);
        }
        if (sql.startsWith("SHOW PARTITIONS")) {
            final String filter = partitionName(sql);
            final List<List<String>> rows = new ArrayList<>();
            for (final String name : table.partitions.keySet()) {
                if (filter == null || List.of(name.split("/")).containsAll(List.of(filter.split("/")))) {
                    rows.add(List.of(name));
                }
            }
            return Result.rows(List.of("partition"), rows);
        }
        if (sql.startsWith("INSERT OVERWRITE")) {
            if (rewriteError != null) {
                return rewriteError;
            }
            rewriteReducers = session.get("mapreduce.job.reduces");
            rewriteSession = new LinkedHashMap<>(session);
            table.rowCount += rowDeltaAfterCompaction;
            return Result.none();
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
            final int start = sql.lastIndexOf("COMPACT '") + 9;
            final String type = sql.substring(start, sql.indexOf('\'', start)).toUpperCase(Locale.ROOT);
            for (final Compaction c : compactions) {
                if (c.db.equals(db) && c.table.equals(tableName) && java.util.Objects.equals(c.partition, partition)
                        && (c.state.equals("initiated") || c.state.equals("working"))) {
                    if (hiveVersion == Version.HIVE4) {
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
        if (hiveVersion == Version.HIVE3) {
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

    private static Result describe(final String db, final Table table, final String location) {
        final List<List<String>> rows = new ArrayList<>();
        rows.add(List.of("# col_name            ", "data_type           ", "comment             "));
        for (final String column : table.columns) {
            rows.add(List.of(column + "                  ", "int                 ", ""));
        }
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
        rows.add(java.util.Arrays.asList("Location:           ", location, null));
        rows.add(java.util.Arrays.asList("Table Type:         ", table.tableType + "       ", null));
        rows.add(java.util.Arrays.asList("Table Parameters:", null, null));
        table.properties.forEach((k, v) -> rows.add(List.of("", k + "   ", v + "   ")));
        if (table.totalSize >= 0) {
            rows.add(List.of("", "totalSize           ", table.totalSize + "                "));
        }
        rows.add(java.util.Arrays.asList("", null, null));
        rows.add(java.util.Arrays.asList("# Storage Information", null, null));
        rows.add(java.util.Arrays.asList("InputFormat:        ", table.inputFormat, null));
        rows.add(java.util.Arrays.asList("Num Buckets:        ", table.numBuckets + "                  ", null));
        rows.add(java.util.Arrays.asList("Storage Desc Params:", null, null));
        rows.add(List.of("", "serialization.format", "1                   "));
        return Result.rows(List.of("col_name", "data_type", "comment"), rows);
    }
}
