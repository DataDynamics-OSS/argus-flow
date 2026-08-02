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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/WriteCSVResult.java
 */
package io.datadynamics.nifi.services.record.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * NiFi Record 집합을 CSV 형식으로 직렬화하여 출력 스트림에 기록하는 RecordSetWriter 구현체다.
 * 내부적으로 Apache Commons CSV의 CSVPrinter를 사용하며, 필요 시 헤더 라인을 함께 기록하고
 * 스키마에 정의되지 않은 필드까지 그대로 기록하는 원시(raw) 기록 모드(RawRecordWriter)도 지원한다.
 */
public class WriteCSVResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private final RecordSchema recordSchema;
    private final SchemaAccessWriter schemaWriter;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final CSVPrinter printer;
    // writeRecord() 호출마다 재사용되는 필드 값 배열 (매 호출마다 새 배열을 생성하지 않기 위함)
    private final Object[] fieldValues;
    private final boolean includeHeaderLine;
    // 헤더 라인이 이미 기록되었는지 여부 (한 번만 기록되도록 보장)
    private boolean headerWritten = false;
    // writeRawRecord() 사용 시 지연 계산되어 캐싱되는 필드명 배열(스키마 필드 + 레코드의 추가 필드)
    private String[] fieldNames;

    public WriteCSVResult(final CSVFormat csvFormat, final RecordSchema recordSchema, final SchemaAccessWriter schemaWriter, final OutputStream out,
        final String dateFormat, final String timeFormat, final String timestampFormat, final boolean includeHeaderLine, final String charSet) throws IOException {

        super(out);
        this.recordSchema = recordSchema;
        this.schemaWriter = schemaWriter;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.includeHeaderLine = includeHeaderLine;

        final CSVFormat formatWithHeader = csvFormat.builder().setSkipHeaderRecord(true).get();
        final OutputStreamWriter streamWriter = new OutputStreamWriter(out, charSet);
        printer = new CSVPrinter(streamWriter, formatWithHeader);

        fieldValues = new Object[recordSchema.getFieldCount()];
    }

    // 필드의 타입(DATE/TIME/TIMESTAMP)에 맞는 포맷 문자열을 반환한다. 그 외 타입은 필드 자체의 포맷을 사용한다.
    private String getFormat(final RecordField field) {
        final DataType dataType = field.getDataType();
        return switch (dataType.getFieldType()) {
            case DATE -> dateFormat;
            case TIME -> timeFormat;
            case TIMESTAMP -> timestampFormat;
            default -> dataType.getFormat();
        };

    }

    // 레코드 집합 기록을 시작할 때 스키마 접근 관련 헤더(예: 스키마 텍스트)를 출력 스트림에 기록한다.
    @Override
    protected void onBeginRecordSet() throws IOException {
        schemaWriter.writeHeader(recordSchema, getOutputStream());
    }

    // 레코드 집합 기록을 마칠 때 호출된다. 레코드가 하나도 없어 헤더 라인이 아직 기록되지 않았다면 여기서 기록한다.
    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        // 헤더가 아직 기록되지 않았다면(기록해야 하는 상황이라면) 지금 기록한다.
        includeHeaderIfNecessary(null, true);
        return schemaWriter.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {
        printer.close();
    }

    @Override
    public void flush() throws IOException {
        printer.flush();
    }

    // 스키마에 정의된 필드명과 레코드에만 존재하는 추가 필드명을 합쳐 최종 필드명 배열을 만든다(원시 기록 모드용).
    // 한 번 계산되면 인스턴스 내에서 캐싱되어 재사용된다.
    private String[] getFieldNames(final Record record) {
        if (fieldNames != null) {
            return fieldNames;
        }

        final Set<String> allFields = new LinkedHashSet<>();
        // 스키마에 정의된 필드를 먼저 기록하고, 그 뒤에 추가 필드를 기록해야 한다.
        allFields.addAll(recordSchema.getFieldNames());
        allFields.addAll(record.getRawFieldNames());
        fieldNames = allFields.toArray(new String[0]);
        return fieldNames;
    }

    // includeHeaderLine이 true이고 아직 헤더를 기록하지 않은 경우에 한해 CSV 헤더 라인을 기록한다.
    private void includeHeaderIfNecessary(final Record record, final boolean includeOnlySchemaFields) throws IOException {
        if (headerWritten || !includeHeaderLine) {
            return;
        }

        final Object[] fieldNames;
        if (includeOnlySchemaFields) {
            fieldNames = recordSchema.getFieldNames().toArray(new Object[0]);
        } else {
            fieldNames = getFieldNames(record);
        }

        printer.printRecord(fieldNames);
        headerWritten = true;
    }

    /**
     * 스키마에 정의된 필드 순서대로 레코드 값을 추출하여 CSV 한 행으로 기록한다.
     * 스키마에 없는 필드(원시 필드)는 기록하지 않는다.
     */
    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // 활성화된(진행 중인) 레코드 집합을 기록하는 상황이 아니라면, 스키마 정보를 반드시 기록해야 한다.
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }

        includeHeaderIfNecessary(record, true);

        int i = 0;
        for (final RecordField recordField : recordSchema.getFields()) {
            fieldValues[i++] = getFieldValue(record, recordField);
        }

        printer.printRecord(fieldValues);
        return schemaWriter.getAttributes(recordSchema);
    }

    // 숫자 계열 타입은 문자열로 변환하지 않고 Number 값을 그대로 사용하며, 그 외 타입은 지정된 포맷으로 문자열 변환한다.
    private Object getFieldValue(final Record record, final RecordField recordField) {
        final RecordFieldType fieldType = recordField.getDataType().getFieldType();

        switch (fieldType) {
            case BIGINT:
            case BYTE:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case LONG:
            case INT:
            case SHORT:
                final Object value = record.getValue(recordField);
                if (value instanceof Number) {
                    return value;
                }
                break;
        }

        return record.getAsString(recordField, getFormat(recordField));
    }

    /**
     * 스키마에 정의되지 않은 필드까지 포함하여 레코드의 모든 원시(raw) 필드를 CSV 한 행으로 기록한다.
     * 스키마에 존재하는 필드는 해당 필드의 포맷을 사용하고, 그렇지 않은 필드는 기본 문자열 변환을 사용한다.
     */
    @Override
    public WriteResult writeRawRecord(final Record record) throws IOException {
        // 활성화된(진행 중인) 레코드 집합을 기록하는 상황이 아니라면, 스키마 정보를 반드시 기록해야 한다.
        if (!isActiveRecordSet()) {
            schemaWriter.writeHeader(recordSchema, getOutputStream());
        }

        includeHeaderIfNecessary(record, false);

        final String[] fieldNames = getFieldNames(record);
        // 가능하다면 레코드마다 새로운 Object[]를 생성하지 않도록 한다. 다만 레코드의 컬럼 개수가 스키마와
        // 다르다면 다른 선택지가 없으므로 이 경우에는 새로운 Object[]를 생성한다.
        final Object[] recordFieldValues = (fieldNames.length == this.fieldValues.length) ? this.fieldValues : new String[fieldNames.length];

        int i = 0;
        for (final String fieldName : fieldNames) {
            final Optional<RecordField> recordField = recordSchema.getField(fieldName);
            if (recordField.isPresent()) {
                recordFieldValues[i++] = record.getAsString(fieldName, getFormat(recordField.get()));
            } else {
                recordFieldValues[i++] = record.getAsString(fieldName);
            }
        }

        printer.printRecord(recordFieldValues);
        final Map<String, String> attributes = schemaWriter.getAttributes(recordSchema);
        return WriteResult.of(incrementRecordCount(), attributes);
    }

    @Override
    public String getMimeType() {
        return "text/csv";
    }
}
