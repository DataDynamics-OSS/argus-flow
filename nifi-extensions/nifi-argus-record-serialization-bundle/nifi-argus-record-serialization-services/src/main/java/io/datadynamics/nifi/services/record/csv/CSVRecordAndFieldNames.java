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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/CSVRecordAndFieldNames.java
 */
package io.datadynamics.nifi.services.record.csv;

import org.apache.commons.csv.CSVRecord;

import java.util.List;

/**
 * 스키마 추론(CSVSchemaInference) 과정에서 사용되는 단순 데이터 홀더 클래스로,
 * 파싱된 하나의 CSVRecord와 해당 레코드가 속한 CSV의 필드(컬럼)명 목록을 함께 묶어서 전달한다.
 * 필드명 목록을 레코드마다 별도로 재계산하지 않고 재사용할 수 있도록 하기 위한 목적이다.
 */
public class CSVRecordAndFieldNames {
    /** 파싱된 하나의 CSV 행 데이터 */
    private final CSVRecord record;
    /** 해당 CSV의 헤더에서 얻은 필드(컬럼)명 목록 */
    private final List<String> fieldNames;

    public CSVRecordAndFieldNames(final CSVRecord record, final List<String> fieldNames) {
        this.record = record;
        this.fieldNames = fieldNames;
    }

    public CSVRecord getRecord() {
        return record;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
}
