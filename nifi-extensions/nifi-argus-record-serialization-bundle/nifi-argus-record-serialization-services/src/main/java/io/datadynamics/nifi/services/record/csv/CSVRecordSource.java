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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/CSVRecordSource.java
 */
package io.datadynamics.nifi.services.record.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.csv.CSVUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.inference.RecordSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 스키마 추론(SchemaInferenceEngine) 과정에서 CSV 입력 스트림을 순차적으로 읽어
 * CSVRecordAndFieldNames 형태로 하나씩 제공하는 RecordSource 구현체다.
 * 생성 시점에 헤더 라인을 먼저 파싱하여 필드명 목록을 확보해 두고, 이후 next() 호출마다
 * 실제 데이터 행을 하나씩 순회하며 반환한다.
 */
public class CSVRecordSource implements RecordSource<CSVRecordAndFieldNames> {
    private final Iterator<CSVRecord> csvRecordIterator;
    private final List<String> fieldNames;

    // 입력 스트림을 지정된 CSV 포맷(헤더 포함, trim 적용)으로 파싱하여 헤더의 필드명 목록을 미리 추출하고,
    // 데이터 행을 순회할 수 있는 Iterator를 준비한다.
    public CSVRecordSource(final InputStream in, final PropertyContext context, final Map<String, String> variables) throws IOException {
        final String charset = context.getProperty(CSVUtils.CHARSET).getValue();

        final Reader reader;
        try {
            reader = new InputStreamReader(BOMInputStream.builder().setInputStream(in).get(), charset);
        } catch (UnsupportedEncodingException e) {
            throw new ProcessException(e);
        }

        final CSVFormat csvFormat = CSVUtils.createCSVFormat(context, variables).builder().setHeader().setSkipHeaderRecord(true).setTrim(true).get();
        final CSVParser csvParser = CSVParser.builder()
                .setReader(reader)
                .setFormat(csvFormat)
                .get();
        fieldNames = List.copyOf(csvParser.getHeaderMap().keySet());

        csvRecordIterator = csvParser.iterator();
    }

    // 다음 CSV 레코드를 필드명 목록과 함께 반환한다. 더 이상 읽을 레코드가 없으면 null을 반환한다.
    @Override
    public CSVRecordAndFieldNames next() {
        if (csvRecordIterator.hasNext()) {
            final CSVRecord record = csvRecordIterator.next();
            return new CSVRecordAndFieldNames(record, fieldNames);
        }

        return null;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
}
