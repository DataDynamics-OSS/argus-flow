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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/CSVRecordReader.java
 */
package io.datadynamics.nifi.services.record.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SchemaValidationException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * org.apache.nifi.csv.CSVRecordReader (NiFi 2.10.0)를 포크하여 필드(컬럼) 개수 유효성 검사 기능을
 * 추가한 클래스다. 이 기능이 활성화된 상태에서 파싱된 컬럼 개수가 설정된(또는 스키마에서 도출된)
 * 필드 개수와 다르면 해당 레코드 읽기를 실패 처리한다. 내부적으로 Apache Commons CSV의
 * CSVParser를 사용하여 실제 파싱을 수행한다.
 */
public class CSVRecordReader extends AbstractCSVRecordReader {
    private final CSVParser csvParser;

    // 헤더로부터 계산된 RecordField 목록. 최초 1회만 계산하여 캐싱한다.
    private List<RecordField> recordFields;

    // 필드 개수 검증 옵션(fieldCount, failOnMismatchFieldCount)과 큰따옴표 trim 여부를 모두 지정하는 생성자.
    // hasHeader 여부에 따라 헤더 라인을 스키마의 필드명으로 사용할지, CSV 파일 자체의 헤더를 사용할지 결정한다.
    public CSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                           final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding, final boolean trimDoubleQuote,
                           final Integer fieldCount, final boolean failOnMismatchFieldCount) throws IOException {
        super(logger, schema, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, trimDoubleQuote, fieldCount, failOnMismatchFieldCount);

        final InputStream bomInputStream = BOMInputStream.builder().setInputStream(in).get();
        final Reader reader = new InputStreamReader(bomInputStream, encoding);

        CSVFormat.Builder withHeader;
        if (hasHeader) {
            withHeader = csvFormat.builder().setSkipHeaderRecord(true);

            if (ignoreHeader) {
                // 헤더 라인은 건너뛰되, 필드명은 CSV 파일이 아닌 스키마에서 가져온다.
                withHeader = withHeader.setHeader(schema.getFieldNames().toArray(new String[0]));
            } else {
                // CSV 파일 자체의 헤더 라인을 필드명으로 사용한다.
                withHeader = withHeader.setHeader();
            }
        } else {
            // 헤더 라인이 없으므로 스키마의 필드명을 그대로 헤더로 사용한다.
            withHeader = csvFormat.builder().setHeader(schema.getFieldNames().toArray(new String[0]));
        }

        csvParser = CSVParser.builder()
                .setReader(reader)
                .setFormat(withHeader.get())
                .get();
    }

    // trimDoubleQuote를 true로 고정한 편의 생성자.
    public CSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                           final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding,
                           final Integer fieldCount, final boolean failOnMismatchFieldCount) throws IOException {
        this(in, logger, schema, csvFormat, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, encoding, true, fieldCount, failOnMismatchFieldCount);
    }

    /**
     * CSVParser에서 다음 레코드를 읽어 NiFi Record로 변환한다.
     * failOnMismatchFieldCount가 활성화되어 있고 파싱된 컬럼 개수가 fieldCount와 다르면 예외를 던진다.
     * 스키마에 없는 여분의 컬럼은 dropUnknownFields 값에 따라 버리거나 "unknown_field_index_N" 이름으로 보존한다.
     */
    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        try {
            final RecordSchema schema = getSchema();

            final List<RecordField> recordFields = getRecordFields();
            final int numFieldNames = recordFields.size();
            for (final CSVRecord csvRecord : csvParser) {
                if (csvRecord.size() > 0 && failOnMismatchFieldCount && fieldCount != null && fieldCount != csvRecord.size()) {
                    throw new SchemaValidationException(String.format(
                            "CSV 파일의 컬럼 개수와 파싱한 컬럼 개수가 상이합니다. 원래 컬럼 개수 : %s, 파싱한 컬럼 개수 : %s", fieldCount, csvRecord.size()));
                }

                final Map<String, Object> values = new LinkedHashMap<>(recordFields.size() * 2);
                for (int i = 0; i < csvRecord.size(); i++) {
                    final String rawValue = csvRecord.get(i);

                    final String rawFieldName;
                    final DataType dataType;
                    if (i >= numFieldNames) {
                        if (!dropUnknownFields) {
                            values.put("unknown_field_index_" + i, rawValue);
                        }

                        continue;
                    } else {
                        final RecordField recordField = recordFields.get(i);
                        rawFieldName = recordField.getFieldName();
                        dataType = recordField.getDataType();
                    }

                    final Object value;
                    if (coerceTypes) {
                        value = convert(rawValue, dataType, rawFieldName);
                    } else {
                        // The CSV Reader is going to return all fields as Strings, because CSV doesn't have any way to
                        // dictate a field type. As a result, we will use the schema that we have to attempt to convert
                        // the value into the desired type if it's a simple type.
                        value = convertSimpleIfPossible(rawValue, dataType, rawFieldName);
                    }

                    values.put(rawFieldName, value);
                }

                return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
            }
        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record", e);
        }

        return null;
    }

    // CSV 헤더의 컬럼 순서에 맞춰 RecordField 목록을 구성한다. 스키마에 정의된 필드는 그대로 사용하고,
    // 스키마에 없는 컬럼은 STRING 타입의 RecordField로 새로 만든다.
    private List<RecordField> getRecordFields() {
        if (this.recordFields != null) {
            return this.recordFields;
        }

        // 필드의 순서(인덱스)를 키로 하는 SortedMap을 사용하여 올바른 순서의 필드명 리스트를 얻는다.
        final SortedMap<Integer, String> sortedMap = new TreeMap<>();
        for (final Map.Entry<String, Integer> entry : csvParser.getHeaderMap().entrySet()) {
            sortedMap.put(entry.getValue(), entry.getKey());
        }

        final List<RecordField> fields = new ArrayList<>();
        final List<String> rawFieldNames = new ArrayList<>(sortedMap.values());
        for (final String rawFieldName : rawFieldNames) {
            final Optional<RecordField> option = schema.getField(rawFieldName);
            if (option.isPresent()) {
                fields.add(option.get());
            } else {
                fields.add(new RecordField(rawFieldName, RecordFieldType.STRING.getDataType()));
            }
        }

        this.recordFields = fields;
        return fields;
    }

    @Override
    public void close() throws IOException {
        csvParser.close();
    }
}
