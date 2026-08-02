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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Jackson ObjectMapper를 사용하여 JSON 문자열이 {@link TimestampFormats}로 정상 역직렬화되는지 검증합니다.
 */
public class TimestampFormatsTest {

    @Test
    public void deserialize() throws JsonProcessingException {
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
        System.out.println(json);
        TimestampFormats timestampFormats = mapper.readValue(json, TimestampFormats.class);

        List<TimestampFormat> formats = timestampFormats.getFormats();
        Assertions.assertEquals(1, formats.size());
    }

}
