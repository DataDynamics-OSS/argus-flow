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
package io.datadynamics.nifi.trino.processors;

import io.datadynamics.nifi.trino.api.TrinoQueryInfo;

import java.sql.SQLException;
import java.util.Map;

/**
 * 프로세서가 기록하는 FlowFile 속성 이름.
 */
public final class TrinoAttributes {

    public static final String QUERY_ID = "trino.query.id";
    public static final String QUERY_STATE = "trino.query.state";
    public static final String PROCESSED_ROWS = "trino.processed.rows";
    public static final String PROCESSED_BYTES = "trino.processed.bytes";
    public static final String CPU_TIME_MS = "trino.cpu.time.ms";
    public static final String WALL_TIME_MS = "trino.wall.time.ms";
    public static final String QUEUED_TIME_MS = "trino.queued.time.ms";
    public static final String UPDATE_COUNT = "trino.update.count";
    public static final String RESULT_ROW_COUNT = "trino.result.row.count";
    public static final String ERROR_MESSAGE = "trino.error.message";
    public static final String ERROR_CODE = "trino.error.code";
    public static final String MAINTENANCE_SQL = "trino.maintenance.sql";
    public static final String MAINTENANCE_OPERATION = "trino.maintenance.operation";
    public static final String RECORD_COUNT = "record.count";

    private TrinoAttributes() {
    }

    static void putQueryInfo(final Map<String, String> attributes, final TrinoQueryInfo info) {
        if (info == null) {
            return;
        }
        if (info.queryId() != null) {
            attributes.put(QUERY_ID, info.queryId());
        }
        if (info.state() != null) {
            attributes.put(QUERY_STATE, info.state());
        }
        attributes.put(PROCESSED_ROWS, Long.toString(info.processedRows()));
        attributes.put(PROCESSED_BYTES, Long.toString(info.processedBytes()));
        attributes.put(CPU_TIME_MS, Long.toString(info.cpuTimeMillis()));
        attributes.put(WALL_TIME_MS, Long.toString(info.wallTimeMillis()));
        attributes.put(QUEUED_TIME_MS, Long.toString(info.queuedTimeMillis()));
    }

    static void putError(final Map<String, String> attributes, final Throwable error, final TrinoQueryInfo info) {
        final SQLException sqlException = findSqlException(error);
        final Throwable reported = sqlException != null ? sqlException : error;
        attributes.put(ERROR_MESSAGE, String.valueOf(reported.getMessage()));
        if (sqlException != null) {
            attributes.put(ERROR_CODE, Integer.toString(sqlException.getErrorCode()));
        }
        if (info != null && info.queryId() != null) {
            attributes.put(QUERY_ID, info.queryId());
        }
    }

    static SQLException findSqlException(final Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof SQLException sqlException) {
                return sqlException;
            }
            current = current.getCause();
        }
        return null;
    }

    static String transitUri(final String jdbcUrl, final TrinoQueryInfo info) {
        final String base = jdbcUrl == null ? "jdbc:trino://unknown" : jdbcUrl;
        return info == null || info.queryId() == null ? base : base + "#" + info.queryId();
    }
}
