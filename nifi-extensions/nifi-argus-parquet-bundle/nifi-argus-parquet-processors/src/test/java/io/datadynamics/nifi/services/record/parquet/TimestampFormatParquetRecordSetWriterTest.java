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
package io.datadynamics.nifi.services.record.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link TimestampFormatParquetRecordSetWriter}의 add-hours 보정이 필드의 Avro logical type 단위
 * (timestamp-millis는 밀리초, timestamp-micros는 마이크로초)로 적용되는지 Parquet 쓰기/읽기 왕복으로 검증한다.
 */
class TimestampFormatParquetRecordSetWriterTest {

    // millis/micros 타임스탬프 필드를 함께 가진 Avro 스키마. 패턴은 properties의 column.all.pattern으로 지정한다.
    private static final String SCHEMA_TEXT = "{"
            + "\"type\":\"record\",\"name\":\"rec\",\"namespace\":\"test\","
            + "\"properties\":{\"column.all.pattern\":\"yyyy/MM/dd HH:mm:ss\"},"
            + "\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"ts_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
            + "{\"name\":\"ts_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}}"
            + "]}";

    @TempDir
    Path tempDir;

    // 원본 타임스탬프가 문자열로 들어오는 NiFi 레코드를 만든다.
    private static Record record(final long id, final String ts) {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.LONG.getDataType()),
                new RecordField("ts_millis", RecordFieldType.STRING.getDataType()),
                new RecordField("ts_micros", RecordFieldType.STRING.getDataType())));
        final Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("ts_millis", ts);
        values.put("ts_micros", ts);
        return new MapRecord(schema, values);
    }

    // Parquet 리더 설정에 따라 timestamp 값이 Long 또는 Instant로 반환될 수 있어 둘 다 처리한다.
    private static long asMillis(final Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        assertInstanceOf(Instant.class, value);
        return ((Instant) value).toEpochMilli();
    }

    private static long asMicros(final Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        assertInstanceOf(Instant.class, value);
        return ChronoUnit.MICROS.between(Instant.EPOCH, (Instant) value);
    }

    private GenericRecord writeAndReadBack(final String addHours) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TimestampFormatParquetRecordSetWriter writerFactory = new TimestampFormatParquetRecordSetWriter();
        runner.addControllerService("parquet-writer", writerFactory);
        runner.setProperty(writerFactory, TimestampFormatParquetRecordSetWriter.TIMESTAMP_FORMAT_PROPERTY_NAME, "properties");
        runner.setProperty(writerFactory, TimestampFormatParquetRecordSetWriter.ADD_HOURS, addHours);
        runner.enableControllerService(writerFactory);

        final Schema avroSchema = new Schema.Parser().parse(SCHEMA_TEXT);
        final RecordSchema recordSchema = org.apache.nifi.avro.AvroTypeUtil.createSchema(avroSchema, SCHEMA_TEXT, SchemaIdentifier.EMPTY);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final RecordSetWriter writer = writerFactory.createWriter(runner.getLogger(), recordSchema, out, Collections.emptyMap())) {
            writer.beginRecordSet();
            writer.write(record(1L, "2024/01/02 03:04:05"));
            writer.finishRecordSet();
        }

        final Path parquetFile = tempDir.resolve("out-" + addHours + ".parquet");
        Files.write(parquetFile, out.toByteArray());
        try (ParquetReader<GenericRecord> reader =
                     AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(parquetFile.toUri())).build()) {
            final GenericRecord read = reader.read();
            assertNotNull(read);
            return read;
        }
    }

    /**
     * add-hours=9일 때 timestamp-millis 필드에는 9 * 3,600,000ms, timestamp-micros 필드에는
     * 9 * 3,600,000,000us가 더해져 두 필드 모두 정확히 9시간 뒤 시각을 가리키는지 검증한다.
     */
    @Test
    void addHoursAppliedInFieldUnit() throws Exception {
        final GenericRecord read = writeAndReadBack("9");

        final long baseMillis = Timestamp.valueOf(LocalDateTime.of(2024, 1, 2, 3, 4, 5)).getTime();
        assertEquals(1L, read.get("id"));
        assertEquals(baseMillis + 9L * 3_600_000L, asMillis(read.get("ts_millis")));
        assertEquals(baseMillis * 1000L + 9L * 3_600_000_000L, asMicros(read.get("ts_micros")));
    }

    /**
     * add-hours=0이면 millis/micros 필드 모두 원래 시각 그대로 기록되는지 검증한다.
     */
    @Test
    void noAddHours() throws Exception {
        final GenericRecord read = writeAndReadBack("0");

        final long baseMillis = Timestamp.valueOf(LocalDateTime.of(2024, 1, 2, 3, 4, 5)).getTime();
        assertEquals(baseMillis, asMillis(read.get("ts_millis")));
        assertEquals(baseMillis * 1000L, asMicros(read.get("ts_micros")));
    }
}
