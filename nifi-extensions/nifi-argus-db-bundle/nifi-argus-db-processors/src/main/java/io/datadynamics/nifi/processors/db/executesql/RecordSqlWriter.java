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
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/sql/RecordSqlWriter.java
 */
package io.datadynamics.nifi.processors.db.executesql;


import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.ResultSetRecordSet;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.datadynamics.nifi.processors.db.executesql.JdbcCommon.AvroConversionOptions;
import static io.datadynamics.nifi.processors.db.executesql.JdbcCommon.ResultSetRowCallback;

/**
 * JDBC ResultSet을 Avro 스키마로 먼저 변환한 뒤, NiFi의 Record API(RecordSetWriterFactory)를
 * 통해 사용자가 선택한 임의의 출력 포맷(CSV, JSON 등)으로 기록하는 SqlWriter 구현체.
 * DefaultAvroSqlWriter가 항상 Avro 바이너리로 출력하는 것과 달리, 이 구현체는 Record Writer의
 * 종류에 따라 최종 출력 포맷이 달라진다.
 */
public class RecordSqlWriter implements SqlWriter {

    private final RecordSetWriterFactory recordSetWriterFactory;
    private final AtomicReference<WriteResult> writeResultRef;
    private final AvroConversionOptions options;
    private final int maxRowsPerFlowFile;
    private final Map<String, String> originalAttributes;
    private ResultSetRecordSet fullRecordSet;
    private RecordSchema writeSchema;
    private String mimeType;

    public RecordSqlWriter(RecordSetWriterFactory recordSetWriterFactory, AvroConversionOptions options, int maxRowsPerFlowFile, Map<String, String> originalAttributes) {
        this.recordSetWriterFactory = recordSetWriterFactory;
        this.writeResultRef = new AtomicReference<>();
        this.maxRowsPerFlowFile = maxRowsPerFlowFile;
        this.options = options;
        this.originalAttributes = originalAttributes;
    }

    /**
     * ResultSet을 Record 기반의 RecordSet으로 감싼 뒤, Record Writer를 사용해 outputStream에
     * 기록한다. 처음 호출 시에만 스키마를 추론하여 캐시해두고(fullRecordSet), 이후 호출에서는
     * 캐시된 스키마를 재사용한다. maxRowsPerFlowFile이 설정된 경우 결과 집합을 해당 개수만큼
     * 잘라서(limit) 기록한다.
     */
    @Override
    public long writeResultSet(ResultSet resultSet, OutputStream outputStream, ComponentLog logger, ResultSetRowCallback callback) throws Exception {
        final RecordSet recordSet;
        try {
            if (fullRecordSet == null) {
                final Schema avroSchema = JdbcCommon.createSchema(resultSet, options);
                final RecordSchema recordAvroSchema = AvroTypeUtil.createSchema(avroSchema);
                fullRecordSet = new ResultSetRecordSetWithCallback(resultSet, recordAvroSchema, callback, options.getDefaultPrecision(), options.getDefaultScale(), options.isUseLogicalTypes());
                writeSchema = recordSetWriterFactory.getSchema(originalAttributes, fullRecordSet.getSchema());
            }
            recordSet = (maxRowsPerFlowFile > 0) ? fullRecordSet.limit(maxRowsPerFlowFile) : fullRecordSet;

        } catch (final SQLException | SchemaNotFoundException | IOException e) {
            throw new ProcessException(e);
        }
        try (final RecordSetWriter resultSetWriter = recordSetWriterFactory.createWriter(logger, writeSchema, outputStream, Collections.emptyMap())) {
            writeResultRef.set(resultSetWriter.write(recordSet));
            if (mimeType == null) {
                mimeType = resultSetWriter.getMimeType();
            }
            return writeResultRef.get().getRecordCount();
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Map<String, String> getAttributesToAdd() {
        Map<String, String> attributesToAdd = new HashMap<>();
        attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), mimeType);

        // Record Writer가 반환한 속성이 있으면 함께 추가한다
        final WriteResult result = writeResultRef.get();
        if (result != null) {
            if (result.getAttributes() != null) {
                attributesToAdd.putAll(result.getAttributes());
            }

            attributesToAdd.put("record.count", String.valueOf(result.getRecordCount()));
        }
        return attributesToAdd;
    }

    @Override
    public void updateCounters(ProcessSession session) {
        final WriteResult result = writeResultRef.get();
        if (result != null) {
            session.adjustCounter("Records Written", result.getRecordCount(), false);
        }
    }

    @Override
    public void writeEmptyResultSet(OutputStream outputStream, ComponentLog logger) throws IOException {
        try (final RecordSetWriter resultSetWriter = recordSetWriterFactory.createWriter(logger, writeSchema, outputStream, Collections.emptyMap())) {
            mimeType = resultSetWriter.getMimeType();
            resultSetWriter.beginRecordSet();
            resultSetWriter.finishRecordSet();
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getMimeType() {
        return mimeType;
    }

    /**
     * ResultSet의 각 행을 Record로 변환할 때마다 콜백(예: 최댓값 추적)을 함께 호출할 수 있도록
     * ResultSetRecordSet을 확장한 내부 구현체.
     */
    private static class ResultSetRecordSetWithCallback extends ResultSetRecordSet {

        private final ResultSetRowCallback callback;

        ResultSetRecordSetWithCallback(ResultSet rs, RecordSchema readerSchema, ResultSetRowCallback callback,
                                       final int defaultPrecision, final int defaultScale, final boolean useLogicalTypes) throws SQLException {
            super(rs, readerSchema, defaultPrecision, defaultScale, useLogicalTypes);
            this.callback = callback;
        }

        @Override
        public Record next() throws IOException {
            try {
                if (hasMoreRows()) {
                    ResultSet rs = getResultSet();
                    final Record record = createRecord(rs);
                    if (callback != null) {
                        callback.processRow(rs);
                    }
                    setMoreRows(rs.next());
                    return record;
                } else {
                    return null;
                }
            } catch (final SQLException e) {
                throw new IOException("Could not obtain next record from ResultSet", e);
            }
        }
    }
}