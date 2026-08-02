/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/test/java/org/apache/nifi/processors/iceberg/util/IcebergTestUtils.java
 */
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
package org.apache.nifi.processors.iceberg.util;

import com.google.common.collect.Lists;
import org.apache.commons.lang.Validate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Iceberg 관련 테스트들에서 공통으로 사용하는 검증 및 테스트 데이터 생성 유틸리티 모음.
 * 테이블에 기록된 레코드/데이터 파일/파티션 폴더가 기대한 결과와 일치하는지 확인하는 헬퍼 메서드와,
 * 테스트용 레코드를 손쉽게 만들 수 있는 RecordsBuilder를 제공한다.
 */
public class IcebergTestUtils {

    /**
     * 테이블에 기대한 레코드들이 실제로 존재하는지 검증한다.
     * 결과 비교 시 순서에 의한 플레이키(flaky) 테스트를 방지하기 위해 지정된 컬럼(sortBy) 기준으로 정렬 후 비교한다.
     *
     * Validates whether the table contains the expected records. The results should be sorted by a unique key, so we do not end up with flaky tests.
     *
     * @param table    The table we should read the records from
     * @param expected The expected list of Records
     * @param sortBy   The column position by which we will sort
     * @throws IOException Exceptions when reading the table data
     */
    public static void validateData(Table table, List<Record> expected, int sortBy) throws IOException {
        List<Record> records = Lists.newArrayListWithExpectedSize(expected.size());
        try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
            iterable.forEach(records::add);
        }

        validateData(expected, records, sortBy);
    }

    /**
     * 두 레코드 집합(expected, actual)이 동일한지 검증한다.
     * 순서 차이로 인한 플레이키 테스트를 방지하기 위해 지정된 컬럼(sortBy) 기준으로 정렬한 뒤 항목별로 비교한다.
     *
     * Validates whether the 2 sets of records are the same. The results should be sorted by a unique key, so we do not end up with flaky tests.
     *
     * @param expected The expected list of Records
     * @param actual   The actual list of Records
     * @param sortBy   The column position by which we will sort
     */
    public static void validateData(List<Record> expected, List<Record> actual, int sortBy) {
        List<Record> sortedExpected = Lists.newArrayList(expected);
        List<Record> sortedActual = Lists.newArrayList(actual);
        // 지정된 필드(sortBy) 기준으로 정렬 - Sort based on the specified field
        sortedExpected.sort(Comparator.comparingInt(record -> record.get(sortBy).hashCode()));
        sortedActual.sort(Comparator.comparingInt(record -> record.get(sortBy).hashCode()));

        assertEquals(sortedExpected.size(), sortedActual.size());
        for (int i = 0; i < expected.size(); ++i) {
            assertEquals(sortedExpected.get(i), sortedActual.get(i));
        }
    }

    /**
     * 테이블({@link Table}) 하위에 생성된 데이터 파일의 개수가 기대치와 일치하는지 검증한다.
     * 숨김 파일(.으로 시작하는 파일)은 집계에서 제외한다.
     *
     * Validates the number of files under a {@link Table}
     *
     * @param tableLocation     The location of table we are checking
     * @param numberOfDataFiles The expected number of data files (TABLE_LOCATION/data/*)
     */
    public static void validateNumberOfDataFiles(String tableLocation, int numberOfDataFiles) throws IOException {
        List<Path> dataFiles = Files.walk(Paths.get(tableLocation + "/data"))
                .filter(Files::isRegularFile)
                .filter(path -> !path.getFileName().toString().startsWith("."))
                .collect(Collectors.toList());

        assertEquals(numberOfDataFiles, dataFiles.size());
    }

    // 파티셔닝된 테이블에서 기대한 파티션 경로(폴더)들이 실제로 파일시스템 상에 존재하는지 검증한다.
    // 예: date_year=2015, timestampMicros_month=2016-01 등의 파티션 디렉터리 생성 여부 확인.
    public static void validatePartitionFolders(String tableLocation, List<String> partitionPaths) {
        for (String partitionPath : partitionPaths) {
            Path path = Paths.get(tableLocation + "/data/" + partitionPath);
            assertTrue(Files.exists(path),"The expected path doesn't exists: " + path);
        }
    }

    // 테스트에서 사용할 Iceberg Record 목록을 스키마에 맞춰 손쉽게 생성하기 위한 빌더 클래스.
    // newInstance(schema)로 시작해 add(...)를 체이닝 호출한 뒤 build()로 불변 리스트를 얻는다.
    public static class RecordsBuilder {

        private final List<Record> records = new ArrayList<>();
        private final Schema schema;

        private RecordsBuilder(Schema schema) {
            this.schema = schema;
        }

        // 스키마의 컬럼 순서에 맞춰 값들을 채운 레코드 하나를 생성해 목록에 추가한다.
        // 전달된 값의 개수가 스키마 컬럼 수와 다르면 예외가 발생한다.
        public RecordsBuilder add(Object... values) {
            Validate.isTrue(schema.columns().size() == values.length, "Number of provided values and schema length should be equal.");

            GenericRecord record = GenericRecord.create(schema);

            for (int i = 0; i < values.length; i++) {
                record.set(i, values[i]);
            }

            records.add(record);
            return this;
        }

        // 지금까지 추가된 레코드들을 수정 불가능한(unmodifiable) 리스트로 반환한다.
        public List<Record> build() {
            return Collections.unmodifiableList(records);
        }

        public static RecordsBuilder newInstance(Schema schema) {
            return new RecordsBuilder(schema);
        }
    }

}
