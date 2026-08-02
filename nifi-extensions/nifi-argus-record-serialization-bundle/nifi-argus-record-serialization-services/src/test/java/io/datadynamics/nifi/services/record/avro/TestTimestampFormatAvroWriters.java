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
package io.datadynamics.nifi.services.record.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link TimestampFormatAvroTypeUtil} 및 {@link TimestampFormatAvroRecordSetWriter}에 대한 테스트 클래스.
 * <p>
 * 문자열 형태로 표현된 타임스탬프 값을 Avro 스키마에 정의된 사용자 지정 패턴(또는 기본 패턴)으로 파싱하여
 * Avro 레코드의 long(timestamp-millis) 값으로 변환하는 로직과, 시간대 보정을 위한 시간 추가(add hours) 기능,
 * 그리고 RecordSetWriter를 통한 실제 Avro 파일 쓰기/읽기 왕복(round trip) 동작을 검증한다.
 */
class TestTimestampFormatAvroWriters {

    // "column.all.pattern" properties 속성에 사용자 지정 타임스탬프 패턴("yyyy/MM/dd HH:mm:ss")이 정의된 Avro 스키마
    private static final String SCHEMA_WITH_PATTERN = "{"
            + "\"type\":\"record\",\"name\":\"rec\",\"namespace\":\"test\","
            + "\"properties\":{\"column.all.pattern\":\"yyyy/MM/dd HH:mm:ss\"},"
            + "\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}"
            + "]}";

    // 사용자 지정 패턴이 정의되지 않은 Avro 스키마 (기본 타임스탬프 패턴이 사용되어야 함)
    private static final String SCHEMA_WITHOUT_PATTERN = "{"
            + "\"type\":\"record\",\"name\":\"rec\",\"namespace\":\"test\","
            + "\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}"
            + "]}";

    // 테스트에서 사용할 NiFi 레코드 스키마 생성 (id: LONG, ts: STRING 형태의 원본 타임스탬프 문자열)
    private static RecordSchema recordSchema() {
        final List<RecordField> fields = Arrays.asList(
                new RecordField("id", RecordFieldType.LONG.getDataType()),
                new RecordField("ts", RecordFieldType.STRING.getDataType()));
        return new SimpleRecordSchema(fields);
    }

    // 지정된 id와 타임스탬프 문자열 값을 갖는 테스트용 레코드를 생성
    private static Record record(final Object id, final Object ts) {
        final Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("ts", ts);
        return new MapRecord(recordSchema(), values);
    }

    // 주어진 LocalDateTime을 시스템 기본 시간대 기준의 epoch 밀리초 값으로 변환 (검증 기대값 계산용)
    private static long expectedMillis(final LocalDateTime localDateTime) {
        return Timestamp.valueOf(localDateTime).getTime();
    }

    /**
     * 사용자 지정 타임스탬프 패턴("yyyy/MM/dd HH:mm:ss")으로 문자열 값을 파싱하고,
     * addHours 파라미터(9시간)만큼 시간을 더한 밀리초 값이 Avro 레코드에 올바르게 설정되는지 검증한다.
     */
    @Test
    void testCreateAvroRecordWithCustomPatternAndAddHours() {
        final Schema avroSchema = new Schema.Parser().parse(SCHEMA_WITH_PATTERN);
        final Record record = record(1L, "2024/01/02 03:04:05");

        final GenericRecord avroRecord = TimestampFormatAvroTypeUtil.createAvroRecord(record, avroSchema, "properties", 9);

        final long expected = expectedMillis(LocalDateTime.of(2024, 1, 2, 3, 4, 5)) + 9L * 60 * 60 * 1000;
        assertEquals(1L, avroRecord.get("id"));
        assertEquals(expected, avroRecord.get("ts"));
    }

    /**
     * 스키마에 사용자 지정 패턴이 없을 때 기본 타임스탬프 패턴("yyyy-MM-dd HH:mm:ss")으로 파싱되고,
     * 시간 추가 값이 0일 때는 원본 시각 그대로 밀리초 값이 설정되는지 검증한다.
     */
    @Test
    void testCreateAvroRecordWithDefaultPattern() {
        final Schema avroSchema = new Schema.Parser().parse(SCHEMA_WITHOUT_PATTERN);
        final Record record = record(2L, "2024-01-02 03:04:05");

        final GenericRecord avroRecord = TimestampFormatAvroTypeUtil.createAvroRecord(record, avroSchema, "properties", 0);

        assertEquals(expectedMillis(LocalDateTime.of(2024, 1, 2, 3, 4, 5)), avroRecord.get("ts"));
    }

    /**
     * getTimestampPattern 메서드가 스키마의 properties 속성에서 사용자 지정 패턴을 올바르게 추출하는지,
     * 그리고 패턴이 정의되지 않았거나 properties 속성 이름 자체가 null인 경우 기본 패턴으로 대체(fallback)되는지 검증한다.
     */
    @Test
    void testGetTimestampPattern() {
        final Schema withPattern = new Schema.Parser().parse(SCHEMA_WITH_PATTERN);
        assertEquals("yyyy/MM/dd HH:mm:ss", TimestampFormatAvroTypeUtil.getTimestampPattern(withPattern, "properties", "ts"));

        final Schema withoutPattern = new Schema.Parser().parse(SCHEMA_WITHOUT_PATTERN);
        assertEquals(TimestampFormatAvroTypeUtil.DEFAULT_TIMESTAMP_FORMAT, TimestampFormatAvroTypeUtil.getTimestampPattern(withoutPattern, "properties", "ts"));
        assertEquals(TimestampFormatAvroTypeUtil.DEFAULT_TIMESTAMP_FORMAT, TimestampFormatAvroTypeUtil.getTimestampPattern(withPattern, null, "ts"));
    }

    /**
     * TimestampFormatAvroRecordSetWriter 컨트롤러 서비스를 실제로 등록/활성화한 뒤, RecordSetWriter로
     * 레코드를 Avro 바이트 스트림에 기록하고 다시 DataFileStream으로 읽어들이는 전체 왕복(round trip) 과정을 검증한다.
     * Avro 리더 구현체에 따라 timestamp-millis 논리 타입이 Long 또는 Instant로 반환될 수 있으므로 두 경우를 모두 처리한다.
     */
    @Test
    void testWriterServiceRoundTrip() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TimestampFormatAvroRecordSetWriter writerFactory = new TimestampFormatAvroRecordSetWriter();
        runner.addControllerService("avro-writer", writerFactory);
        runner.setProperty(writerFactory, TimestampFormatAvroRecordSetWriter.TIMESTAMP_FORMAT_PROPERTY_NAME, "properties");
        runner.setProperty(writerFactory, TimestampFormatAvroRecordSetWriter.ADD_HOURS, "9");
        runner.enableControllerService(writerFactory);

        final Schema avroSchema = new Schema.Parser().parse(SCHEMA_WITH_PATTERN);
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema, SCHEMA_WITH_PATTERN, org.apache.nifi.serialization.record.SchemaIdentifier.EMPTY);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final RecordSetWriter writer = writerFactory.createWriter(runner.getLogger(), recordSchema, out, Collections.emptyMap())) {
            writer.beginRecordSet();
            writer.write(record(1L, "2024/01/02 03:04:05"));
            writer.finishRecordSet();
        }

        try (final DataFileStream<GenericRecord> reader = new DataFileStream<>(new ByteArrayInputStream(out.toByteArray()), new GenericDatumReader<>())) {
            assertNotNull(reader.getSchema());
            final GenericRecord read = reader.next();
            assertEquals(1L, read.get("id"));
            final Object ts = read.get("ts");
            final long expected = expectedMillis(LocalDateTime.of(2024, 1, 2, 3, 4, 5)) + 9L * 60 * 60 * 1000;
            // Avro 리더의 논리 타입 처리 방식에 따라 이 값은 Long 또는 Instant 형태로 반환될 수 있음
            if (ts instanceof Long) {
                assertEquals(expected, ts);
            } else {
                assertInstanceOf(java.time.Instant.class, ts);
                assertEquals(expected, ((java.time.Instant) ts).toEpochMilli());
            }
        }
    }
}
