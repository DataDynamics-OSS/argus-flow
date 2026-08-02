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
package io.datadynamics.nifi.processors.parquet;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link ParquetUtils} 유틸리티 클래스에 대한 테스트.
 * Avro 스키마에서 INT96(레거시 타임스탬프) 타입으로 정의된 컬럼을
 * 정확히 식별해내는지 검증한다.
 */
public class ParquetUtilsTest {

    /**
     * Avro 스키마의 필드 중 "fixed" 타입이면서 이름이 INT96인 필드(registration_dttm)를
     * INT96 타임스탬프 컬럼으로 정확히 추출하는지 검증한다.
     * INT96은 Parquet/Hive에서 과거에 타임스탬프를 표현하던 12바이트 고정 길이 타입이며,
     * 다른 nullable 필드(id, first_name 등)는 결과에 포함되지 않아야 한다.
     */
    @Test
    void timestampColumns() {
        // 테스트용 Hive 스타일 Avro 스키마 JSON.
        // registration_dttm 필드만 INT96(fixed, size=12)로 정의되어 있고,
        // 나머지 필드는 int/string/double 등 일반 nullable 타입이다.
        String avroSchemaJson = "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"hive_schema\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"registration_dttm\",\n" +
                "    \"type\" : [ \"null\", {\n" +
                "      \"type\" : \"fixed\",\n" +
                "      \"name\" : \"INT96\",\n" +
                "      \"doc\" : \"INT96 represented as byte[12]\",\n" +
                "      \"size\" : 12\n" +
                "    } ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"id\",\n" +
                "    \"type\" : [ \"null\", \"int\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"first_name\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"last_name\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"email\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"gender\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"ip_address\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"cc\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"country\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"birthdate\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"salary\",\n" +
                "    \"type\" : [ \"null\", \"double\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"title\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  }, {\n" +
                "    \"name\" : \"comments\",\n" +
                "    \"type\" : [ \"null\", \"string\" ],\n" +
                "    \"default\" : null\n" +
                "  } ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(avroSchemaJson);
        // INT96 타입으로 정의된 컬럼 이름 목록을 조회한다.
        String[] columns = ParquetUtils.getTimestampInt96Columns(schema);
        // registration_dttm 하나만 INT96 컬럼으로 검출되어야 한다.
        assertEquals(1, columns.length);
        assertEquals("registration_dttm", columns[0]);
    }
}
