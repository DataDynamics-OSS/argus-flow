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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/JacksonCSVRecordReader.java
 */
package io.datadynamics.nifi.services.record.csv;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.DuplicateHeaderMode;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Jackson Dataformats CSV 라이브러리를 사용하여 CSV를 파싱하는 RecordReader 구현체다.
 * Apache Commons CSV 기반의 CSVRecordReader보다 대체로 더 높은 처리 성능을 제공하지만,
 * 지원하는 CSV 포맷 옵션의 범위는 다소 제한적일 수 있다.
 */
public class JacksonCSVRecordReader extends AbstractCSVRecordReader {
    // Jackson이 파싱한 CSV 행(문자열 배열) 스트림
    private final MappingIterator<String[]> recordStream;
    // 헤더에서 얻은(또는 스키마에서 가져온) 원본 필드명 목록. 최초 레코드 처리 시 1회 설정된다.
    private List<String> rawFieldNames = null;
    // 헤더에 중복된 컬럼명이 존재하는 것을 허용할지 여부
    private boolean allowDuplicateHeaderNames;

    // CsvMapper는 무거운 객체이므로 클래스 전체에서 하나만 생성하여 공유한다.
    private static volatile CsvMapper mapper = new CsvMapper().enable(CsvParser.Feature.WRAP_AS_ARRAY);

    // CSVFormat 설정(구분자, 줄바꿈 문자, 주석 마커, 인용 부호, 이스케이프 문자 등)을 Jackson의 CsvSchema로 변환하여
    // ObjectReader를 구성하고, 이를 이용해 입력 스트림으로부터 레코드 스트림을 생성한다.
    public JacksonCSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                                  final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding, final boolean trimDoubleQuote) throws IOException {
        super(logger, schema, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, trimDoubleQuote);

        final Reader reader = new InputStreamReader(BOMInputStream.builder().setInputStream(in).get(), encoding);

        CsvSchema.Builder csvSchemaBuilder = CsvSchema.builder()
            .setColumnSeparator(csvFormat.getDelimiterString().charAt(0))
                .setLineSeparator((csvFormat.getRecordSeparator() == null) ? "\n" : csvFormat.getRecordSeparator())
                // Jackson CSV에서 주석을 사용하려면 주석 마커가 정확히 '#'으로 설정되어 있어야 한다.
                .setAllowComments("#" .equals(CharUtils.toString(csvFormat.getCommentMarker())))
                // 모든 코드 경로에서 setUseHeader(false)를 호출하는 이유는 Jackson의 데이터 바인딩/매핑 방식 때문이다.
                // 헤더를 이용한 매핑을 사용하면 컬럼이 누락되거나 초과된 경우 올바르게 처리되지 않을 수 있다.
                .setUseHeader(false);

        csvSchemaBuilder = (csvFormat.getQuoteCharacter() == null) ? csvSchemaBuilder : csvSchemaBuilder.setQuoteChar(csvFormat.getQuoteCharacter());
        csvSchemaBuilder = (csvFormat.getEscapeCharacter() == null) ? csvSchemaBuilder : csvSchemaBuilder.setEscapeChar(csvFormat.getEscapeCharacter());

        if (hasHeader) {
            if (ignoreHeader) {
                csvSchemaBuilder = csvSchemaBuilder.setSkipFirstDataRow(true);
            }
        }
        allowDuplicateHeaderNames = csvFormat.getDuplicateHeaderMode() == DuplicateHeaderMode.ALLOW_ALL;

        CsvSchema csvSchema = csvSchemaBuilder.build();

        // 나머지 설정 옵션들을 mapper에 추가한다.
        List<CsvParser.Feature> features = new ArrayList<>();
        features.add(CsvParser.Feature.INSERT_NULLS_FOR_MISSING_COLUMNS);
        if (csvFormat.getIgnoreEmptyLines()) {
            features.add(CsvParser.Feature.SKIP_EMPTY_LINES);
        }
        if (csvFormat.getTrim()) {
            features.add(CsvParser.Feature.TRIM_SPACES);
        }

        ObjectReader objReader = mapper.readerFor(String[].class)
                .with(csvSchema)
                .withFeatures(features.toArray(new CsvParser.Feature[features.size()]));

        recordStream = objReader.readValues(reader);
    }

    // trimDoubleQuote를 true로 고정한 편의 생성자.
    public JacksonCSVRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema, final CSVFormat csvFormat, final boolean hasHeader, final boolean ignoreHeader,
                                  final String dateFormat, final String timeFormat, final String timestampFormat, final String encoding) throws IOException {
        this(in, logger, schema, csvFormat, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, encoding, true);
    }

    /**
     * Jackson의 레코드 스트림에서 다음 CSV 행을 읽어 NiFi Record로 변환한다.
     * 최초 호출 시 헤더 라인 처리(필드명 확정 및 중복 검사)를 수행하고, 빈 라인은 건너뛴다.
     * 이후 각 컬럼 값을 스키마의 데이터 타입에 맞게 변환하여 MapRecord를 구성한다.
     */
    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        final RecordSchema schema = getSchema();

        if (recordStream.hasNext()) {
            String[] csvRecord = recordStream.next();

            // 첫 번째로 읽은 레코드가 헤더 이름들인 경우(그리고 그것을 사용하는 경우), 이후 반복에서
            // 값 맵을 만들 때 사용할 수 있도록 필드명 목록을 저장해 둔다.
            if (rawFieldNames == null) {
                if (!hasHeader || ignoreHeader) {
                    rawFieldNames = schema.getFieldNames();
                } else {
                    rawFieldNames = Arrays.asList(csvRecord);
                    if (rawFieldNames.size() > schema.getFieldCount() && !allowDuplicateHeaderNames) {
                        final Set<String> deDupe = new HashSet<>(schema.getFieldCount());
                        for (final String name : rawFieldNames) {
                            if (!deDupe.add(name)) {
                                throw new IllegalArgumentException(String.format(
                                        "헤더에 중복된 이름이 있습니다: \"%s\" (전체: %s). 이것이 유효한 경우라면 CSVFormat.withAllowDuplicateHeaderNames()를 사용하십시오.",
                                        name, rawFieldNames
                                ));
                            }
                        }
                    }

                    // 레코드 개수가 정확히 유지되도록 스트림을 한 칸 더 진행시킨다(헤더 라인을 실제 데이터에서 제외).
                    if (recordStream.hasNext()) {
                        csvRecord = recordStream.next();
                    } else {
                        return null;
                    }
                }
            }

            // 빈 라인이 있는지 확인하고 이를 무시한다.
            boolean foundRecord = true;
            if (csvRecord == null || (csvRecord.length == 1 && StringUtils.isEmpty(csvRecord[0]))) {
                foundRecord = false;
                while (recordStream.hasNext()) {
                    csvRecord = recordStream.next();

                    if (csvRecord != null && !(csvRecord.length == 1 && StringUtils.isEmpty(csvRecord[0]))) {
                        // 비어있지 않은 레코드(행)이므로 처리를 계속한다.
                        foundRecord = true;
                        break;
                    }
                }
            }

            // 레코드를 찾지 못했다면 파일의 나머지 부분이 모두 빈 라인으로 구성된 것이므로 반환할 레코드가 없다.
            if (!foundRecord) {
                return null;
            }

            final Map<String, Object> values = new HashMap<>(rawFieldNames.size() * 2);
            final int numFieldNames = rawFieldNames.size();
            for (int i = 0; i < csvRecord.length; i++) {
                final String rawFieldName = numFieldNames <= i ? "unknown_field_index_" + i : rawFieldNames.get(i);
                String rawValue = (i >= csvRecord.length) ? null : csvRecord[i];

                final Optional<DataType> dataTypeOption = schema.getDataType(rawFieldName);

                if (!dataTypeOption.isPresent() && dropUnknownFields) {
                    continue;
                }

                final Object value;
                if (coerceTypes && dataTypeOption.isPresent()) {
                    value = convert(rawValue, dataTypeOption.get(), rawFieldName);
                } else if (dataTypeOption.isPresent()) {
                    // CSV Reader는 모든 필드를 문자열로 반환한다. CSV에는 필드 타입을 지정할 방법이 없기 때문이다.
                    // 따라서 보유한 스키마를 이용해, 단순 타입인 경우에는 원하는 타입으로 변환을 시도한다.
                    value = convertSimpleIfPossible(rawValue, dataTypeOption.get(), rawFieldName);
                } else {
                    value = rawValue;
                }

                values.put(rawFieldName, value);
            }

            return new MapRecord(schema, values, coerceTypes, dropUnknownFields);
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        recordStream.close();
    }
}
