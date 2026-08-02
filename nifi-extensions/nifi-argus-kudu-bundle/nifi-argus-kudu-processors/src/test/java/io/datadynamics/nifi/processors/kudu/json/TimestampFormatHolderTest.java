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
package io.datadynamics.nifi.processors.kudu.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * JSON으로 표현된 컬럼별 Timestamp Format 설정을 {@link TimestampFormatHolder}로 로딩했을 때
 * 컬럼명으로 패턴 및 유형을 올바르게 조회할 수 있는지 검증합니다.
 */
public class TimestampFormatHolderTest {

    @Test
    public void getPattern() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = "{\n" +
                    "  \"formats\": [\n" +
                    "    {\n" +
                    "      \"column-name\": \"COL_TIMESTAMP\",\n" +
                    "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                    "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}";
            TimestampFormats timestampFormats = mapper.readValue(json, TimestampFormats.class);
            TimestampFormatHolder holder = new TimestampFormatHolder(timestampFormats);

            Assertions.assertEquals("yyyy-MM-dd HH:mm:ss", holder.getPattern("COL_TIMESTAMP"));
        } catch (Exception e) {
            Assertions.assertFalse(true);
        }
    }

    @Test
    public void getType() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = "{\n" +
                    "  \"formats\": [\n" +
                    "    {\n" +
                    "      \"column-name\": \"COL_TIMESTAMP\",\n" +
                    "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                    "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}";
            TimestampFormats timestampFormats = mapper.readValue(json, TimestampFormats.class);
            TimestampFormatHolder holder = new TimestampFormatHolder(timestampFormats);

            Assertions.assertEquals("TIMESTAMP_MILLIS", holder.getType("COL_TIMESTAMP").toString());
        } catch (Exception e) {
            Assertions.assertFalse(true);
        }
    }

}