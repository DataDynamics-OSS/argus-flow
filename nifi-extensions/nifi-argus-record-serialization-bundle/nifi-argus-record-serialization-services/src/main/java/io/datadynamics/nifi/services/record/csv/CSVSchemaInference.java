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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/CSVSchemaInference.java
 */
package io.datadynamics.nifi.services.record.csv;

import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.schema.inference.FieldTypeInference;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.SchemaInferenceUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CSV 데이터의 각 행을 순회하며 컬럼별로 가능한 데이터 타입을 누적 추론하여
 * 최종적으로 RecordSchema를 생성하는 SchemaInferenceEngine 구현체다.
 * 값이 없는 CSV(레코드가 하나도 없는 경우)에 대해서는 모든 필드를 STRING 타입으로 간주한다.
 */
public class CSVSchemaInference implements SchemaInferenceEngine<CSVRecordAndFieldNames> {

    // DATE/TIME/TIMESTAMP 타입 추론 시 사용할 포맷 정보
    private final TimeValueInference timeValueInference;

    public CSVSchemaInference(final TimeValueInference timeValueInference) {
        this.timeValueInference = timeValueInference;
    }

    /**
     * RecordSource로부터 레코드를 끝까지 순회하며 각 필드별로 가능한 데이터 타입을 누적 수집한 뒤,
     * 이를 기반으로 최종 RecordSchema를 생성하여 반환한다.
     */
    @Override
    public RecordSchema inferSchema(final RecordSource<CSVRecordAndFieldNames> recordSource) throws IOException {
        final Map<String, FieldTypeInference> typeMap = new LinkedHashMap<>();
        while (true) {
            final CSVRecordAndFieldNames recordAndFieldNames = recordSource.next();
            if (recordAndFieldNames == null) {
                // 레코드가 하나도 없다면(빈 CSV) 모든 필드의 데이터 타입을 String으로 가정한다.
                if (typeMap.isEmpty()) {
                    if (recordSource instanceof CSVRecordSource) {
                        CSVRecordSource csvRecordSource = (CSVRecordSource) recordSource;
                        for (String fieldName : csvRecordSource.getFieldNames()) {
                            typeMap.put(fieldName, new FieldTypeInference());
                        }
                    }
                }
                break;
            }

            inferSchema(recordAndFieldNames, typeMap);
        }
        return createSchema(typeMap);
    }

    // 하나의 CSV 레코드에 대해 각 필드의 값을 검사하여 가능한 데이터 타입을 typeMap에 누적 반영한다.
    private void inferSchema(final CSVRecordAndFieldNames recordAndFieldNames, final Map<String, FieldTypeInference> typeMap) {
        final CSVRecord csvRecord = recordAndFieldNames.getRecord();
        for (final String fieldName : recordAndFieldNames.getFieldNames()) {
            final String value = csvRecord.get(fieldName);
            if (value == null) {
                return;
            }

            final FieldTypeInference typeInference = typeMap.computeIfAbsent(fieldName, key -> new FieldTypeInference());
            final String trimmed = trim(value);
            final DataType dataType = SchemaInferenceUtil.getDataType(trimmed, timeValueInference);
            typeInference.addPossibleDataType(dataType);
        }
    }

    // 값이 큰따옴표로 감싸져 있으면 앞뒤의 큰따옴표를 제거한다.
    private String trim(String value) {
        return (value.length() > 1) && value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    // 필드별로 누적된 타입 추론 결과를 바탕으로 최종 RecordSchema를 생성한다. 모든 필드는 nullable로 취급한다.
    private RecordSchema createSchema(final Map<String, FieldTypeInference> inferences) {
        final List<RecordField> recordFields = new ArrayList<>(inferences.size());
        inferences.forEach((fieldName, type) -> recordFields.add(new RecordField(fieldName, type.toDataType(), true)));
        return new SimpleRecordSchema(recordFields);
    }
}
