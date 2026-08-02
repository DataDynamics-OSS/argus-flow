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
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link AvroTypeUtil} 클래스에 대한 테스트.
 * NiFi 2.x에서 DataTypeUtils.toTimestamp()/getDateFormat() API가 제거됨에 따라
 * FieldConverter 기반으로 마이그레이션한 Timestamp 패턴 변환 로직을 검증한다.
 * 구체적으로는 NiFi Record의 문자열 형태 타임스탬프 값을, Avro 스키마의
 * properties에 지정된 날짜 패턴을 이용해 파싱하고, 필요 시 시간대 보정(시간 더하기)까지
 * 적용해서 Avro GenericRecord의 timestamp-millis 값(long)으로 변환하는 로직을 검증한다.
 */
public class AvroTypeUtilTest {

    // 타임스탬프 필드(ts)와 문자열 필드(name)로 구성된 Avro 스키마.
    // properties의 "column.all.pattern" 값이 Timestamp 문자열 파싱에 사용할 패턴이다.
    private static final String SCHEMA_JSON = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"r\",\n" +
            "  \"fields\" : [\n" +
            "    { \"name\" : \"ts\", \"type\" : { \"type\" : \"long\", \"logicalType\" : \"timestamp-millis\" } },\n" +
            "    { \"name\" : \"name\", \"type\" : \"string\" }\n" +
            "  ],\n" +
            "  \"properties\" : { \"column.all.pattern\" : \"yyyyMMddHHmmss\" }\n" +
            "}";

    /**
     * 테스트에서 사용할 NiFi Record를 생성하는 헬퍼 메서드.
     * ts(TIMESTAMP)와 name(STRING) 두 개의 필드를 가지며,
     * ts 값은 각 테스트 케이스에서 파싱 대상 문자열/값으로 주입된다.
     */
    private Record record(Object tsValue) {
        final RecordSchema recordSchema = new SimpleRecordSchema(List.of(
                new RecordField("ts", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType())
        ));
        final Map<String, Object> values = new HashMap<>();
        values.put("ts", tsValue);
        values.put("name", "argus");
        return new MapRecord(recordSchema, values);
    }

    /**
     * Avro 스키마의 properties에 지정된 패턴("column.all.pattern" -> "yyyyMMddHHmmss")을
     * 사용하여 Record의 ts 문자열 값("20240102030405")을 올바르게 파싱하고,
     * 시간대 보정 없이(addHours=0) Avro GenericRecord의 timestamp-millis 값으로
     * 변환하는지 검증한다. 아울러 name 필드는 변환 없이 그대로 복사되는지도 확인한다.
     */
    @Test
    void timestampPatternFromSchemaProperties() throws Exception {
        final Schema avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
        final GenericRecord rec = AvroTypeUtil.createAvroRecord(record("20240102030405"), avroSchema, "properties", 0);

        final long expected = Timestamp.valueOf(LocalDateTime.of(2024, 1, 2, 3, 4, 5)).getTime();
        assertEquals(expected, rec.get("ts"));
        assertEquals("argus", rec.get("name"));
    }

    /**
     * createAvroRecord()의 addHours 파라미터(여기서는 9시간)가 파싱된 타임스탬프에
     * 정확히 더해져서 반영되는지 검증한다. 이는 UTC와 KST(UTC+9) 같은 시간대 차이를
     * 보정하기 위한 기능으로, 기대값을 파싱 결과에 9시간(9 * 3600 * 1000ms)을
     * 더한 값으로 계산해 비교한다.
     */
    @Test
    void addHoursApplied() throws Exception {
        final Schema avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
        final GenericRecord rec = AvroTypeUtil.createAvroRecord(record("20240102030405"), avroSchema, "properties", 9);

        final long expected = Timestamp.valueOf(LocalDateTime.of(2024, 1, 2, 3, 4, 5)).getTime() + 9L * 3600L * 1000L;
        assertEquals(expected, rec.get("ts"));
    }

    /**
     * Avro 스키마 properties에 존재하지 않는 property key("no-such-key")를 지정했을 때,
     * 예외를 던지지 않고 기본 타임스탬프 패턴(yyyy-MM-dd HH:mm:ss)으로 폴백하여
     * 정상적으로 파싱/변환되는지 검증한다.
     */
    @Test
    void defaultPatternWhenPropertyKeyMissing() throws Exception {
        final Schema avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
        // 스키마에 존재하지 않는 property key를 지정하면 기본 패턴(yyyy-MM-dd HH:mm:ss)을 사용한다.
        final GenericRecord rec = AvroTypeUtil.createAvroRecord(record("2024-01-02 03:04:05"), avroSchema, "no-such-key", 0);

        final long expected = Timestamp.valueOf(LocalDateTime.of(2024, 1, 2, 3, 4, 5)).getTime();
        assertEquals(expected, rec.get("ts"));
    }
}
