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
package io.datadynamics.nifi.services.record.csv;

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link CSVReader} 컨트롤러 서비스에 대한 테스트 클래스.
 * <p>
 * CSV 데이터로부터 레코드 스키마를 추론하여 읽어들이는 기본 동작, FIELD_COUNT 및
 * FAIL_ON_MISMATCH_FIELD_COUNT 속성을 통한 필드 개수 검증(일치/불일치) 동작,
 * USE_SCHEMA_FOR_FIELD_COUNT 속성을 통해 스키마의 필드 개수를 우선 적용하는 동작,
 * 그리고 CSV_PARSER 속성으로 Jackson CSV 파서를 선택했을 때의 동작을 검증한다.
 */
class TestCSVReaderService {

    // 테스트에서 공통으로 사용할 CSV 원본 데이터 (헤더 3개 컬럼 + 데이터 2개 행)
    private static final String CSV = "id,name,amount\n1,a,3.5\n2,b,4.0\n";

    // CSVReader 컨트롤러 서비스를 등록하고 주어진 속성들을 설정한 뒤 활성화하는 공통 헬퍼 메서드
    private CSVReader enableReader(final TestRunner runner, final CSVReader reader, final String[]... properties) throws Exception {
        runner.addControllerService("csv-reader", reader);
        for (final String[] property : properties) {
            runner.setProperty(reader, property[0], property[1]);
        }
        runner.enableControllerService(reader);
        return reader;
    }

    /**
     * 별도의 스키마 지정 없이 CSVReader가 헤더로부터 스키마를 추론하여 각 레코드의 필드 값을
     * 순서대로(id, name) 정상적으로 읽어내고, 마지막에는 더 이상 레코드가 없어 null을 반환하는지 검증한다.
     */
    @Test
    void testReadWithInferredSchema() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final CSVReader reader = enableReader(runner, new CSVReader());

        final byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        try (final InputStream in = new ByteArrayInputStream(bytes);
             final RecordReader recordReader = reader.createRecordReader(Collections.emptyMap(), in, bytes.length, runner.getLogger())) {

            final Record first = recordReader.nextRecord();
            assertNotNull(first);
            assertEquals("1", first.getAsString("id"));
            assertEquals("a", first.getAsString("name"));

            final Record second = recordReader.nextRecord();
            assertNotNull(second);
            assertEquals("2", second.getAsString("id"));
            assertEquals("b", second.getAsString("name"));

            assertNull(recordReader.nextRecord());
        }
    }

    /**
     * FIELD_COUNT를 실제 CSV 필드 개수(3)와 동일하게 설정하고 FAIL_ON_MISMATCH_FIELD_COUNT를 활성화했을 때,
     * 필드 개수가 일치하므로 오류 없이 모든 레코드를 정상적으로 읽어낼 수 있는지 검증한다.
     */
    @Test
    void testFieldCountMatchReadsRecords() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final CSVReader reader = new CSVReader();
        runner.addControllerService("csv-reader", reader);
        runner.setProperty(reader, CSVReader.FIELD_COUNT, "3");
        runner.setProperty(reader, CSVReader.FAIL_ON_MISMATCH_FIELD_COUNT, "true");
        runner.enableControllerService(reader);

        final byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        try (final InputStream in = new ByteArrayInputStream(bytes);
             final RecordReader recordReader = reader.createRecordReader(Collections.emptyMap(), in, bytes.length, runner.getLogger())) {

            assertNotNull(recordReader.nextRecord());
            assertNotNull(recordReader.nextRecord());
            assertNull(recordReader.nextRecord());
        }
    }

    /**
     * FIELD_COUNT를 실제 CSV 필드 개수(3)와 다르게(4로) 설정하고 FAIL_ON_MISMATCH_FIELD_COUNT를 활성화했을 때,
     * 필드 개수 불일치로 인해 레코드를 읽는 시점에 MalformedRecordException이 발생하는지 검증한다.
     */
    @Test
    void testFieldCountMismatchFails() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final CSVReader reader = new CSVReader();
        runner.addControllerService("csv-reader", reader);
        runner.setProperty(reader, CSVReader.FIELD_COUNT, "4");
        runner.setProperty(reader, CSVReader.FAIL_ON_MISMATCH_FIELD_COUNT, "true");
        runner.enableControllerService(reader);

        final byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        try (final InputStream in = new ByteArrayInputStream(bytes);
             final RecordReader recordReader = reader.createRecordReader(Collections.emptyMap(), in, bytes.length, runner.getLogger())) {

            assertThrows(MalformedRecordException.class, recordReader::nextRecord);
        }
    }

    /**
     * USE_SCHEMA_FOR_FIELD_COUNT를 활성화하면 FIELD_COUNT에 잘못된 값(9)이 설정되어 있더라도
     * 실제로는 추론된 스키마의 필드 개수(3)가 우선 적용되어 필드 개수 검증이 무시되고
     * 정상적으로 모든 레코드를 읽을 수 있는지 검증한다.
     */
    @Test
    void testUseSchemaForFieldCountMatchesInferredSchema() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final CSVReader reader = new CSVReader();
        runner.addControllerService("csv-reader", reader);
        runner.setProperty(reader, CSVReader.USE_SCHEMA_FOR_FIELD_COUNT, "true");
        runner.setProperty(reader, CSVReader.FIELD_COUNT, "9");
        runner.setProperty(reader, CSVReader.FAIL_ON_MISMATCH_FIELD_COUNT, "true");
        runner.enableControllerService(reader);

        // 스키마는 3개의 필드를 가지므로, 설정된 (잘못된) 개수 9는 무시되어야 함
        final byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        try (final InputStream in = new ByteArrayInputStream(bytes);
             final RecordReader recordReader = reader.createRecordReader(Collections.emptyMap(), in, bytes.length, runner.getLogger())) {

            assertNotNull(recordReader.nextRecord());
            assertNotNull(recordReader.nextRecord());
            assertNull(recordReader.nextRecord());
        }
    }

    /**
     * CSV_PARSER 속성을 JACKSON_CSV로 설정했을 때, 기본 파서(Commons CSV)가 아닌 Jackson 기반 파서로도
     * CSV 레코드가 정상적으로 파싱되어 필드 값을 읽어낼 수 있는지 검증한다.
     */
    @Test
    void testJacksonParserReadsRecords() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final CSVReader reader = new CSVReader();
        runner.addControllerService("csv-reader", reader);
        runner.setProperty(reader, CSVReader.CSV_PARSER, CSVReader.JACKSON_CSV.getValue());
        runner.enableControllerService(reader);

        final byte[] bytes = CSV.getBytes(StandardCharsets.UTF_8);
        try (final InputStream in = new ByteArrayInputStream(bytes);
             final RecordReader recordReader = reader.createRecordReader(Collections.emptyMap(), in, bytes.length, runner.getLogger())) {

            final Record first = recordReader.nextRecord();
            assertNotNull(first);
            assertEquals("1", first.getAsString("id"));
            assertEquals("a", first.getAsString("name"));
        }
    }
}
