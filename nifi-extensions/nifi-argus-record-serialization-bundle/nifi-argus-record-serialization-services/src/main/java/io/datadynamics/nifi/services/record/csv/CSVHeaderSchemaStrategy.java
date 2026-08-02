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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/CSVHeaderSchemaStrategy.java
 */
package io.datadynamics.nifi.services.record.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.csv.CSVUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CSV 파일의 첫 번째(헤더) 라인을 읽어 컬럼명을 필드명으로 사용하는 스키마 접근 전략이다.
 * 모든 필드는 STRING 타입으로 간주하며, 실제 값 검증 목적으로만 사용된다.
 */
public class CSVHeaderSchemaStrategy implements SchemaAccessStrategy {
    // 이 전략이 별도로 제공하는 스키마 필드는 없음 (헤더에서 이름만 추출하므로)
    private static final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);

    private final PropertyContext context;

    public CSVHeaderSchemaStrategy(final PropertyContext context) {
        this.context = context;
    }

    /**
     * 입력 스트림의 CSV 헤더 라인을 파싱하여, 헤더의 각 컬럼명을 STRING 타입 필드로 갖는
     * RecordSchema를 생성한다. context가 없으면(검증 전용이 아닌 실제 사용 시) 예외를 발생시킨다.
     */
    @Override
    public RecordSchema getSchema(Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException {
        if (this.context == null) {
            throw new SchemaNotFoundException("스키마 접근 전략은 유효성 검사 목적으로만 사용되므로 스키마를 가져올 수 없습니다");
        }

        try {
            CSVFormat csvFormat = CSVUtils.createCSVFormat(context, variables);
            if (!csvFormat.getSkipHeaderRecord()) {
                csvFormat = csvFormat.builder().setHeader().setSkipHeaderRecord(true).get();
            }

            try (final InputStream bomInputStream = BOMInputStream.builder().setInputStream(contentStream).get();
                 final Reader reader = new InputStreamReader(bomInputStream);
                final CSVParser csvParser = CSVParser.builder().setReader(reader).setFormat(csvFormat).get()) {

                final List<RecordField> fields = new ArrayList<>();
                for (final String columnName : csvParser.getHeaderMap().keySet()) {
                    fields.add(new RecordField(columnName, RecordFieldType.STRING.getDataType(), true));
                }

                return new SimpleRecordSchema(fields);
            }
        } catch (final Exception e) {
            throw new SchemaNotFoundException("CSV의 헤더 라인을 읽는 데 실패했습니다", e);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
