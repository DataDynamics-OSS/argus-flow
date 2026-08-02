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
package io.datadynamics.nifi.processors.kudu.validator;

import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * {@link JsonValidator}가 유효한 JSON과 유효하지 않은(문법이 깨진) JSON을 올바르게 구분하는지 검증합니다.
 */
public class JsonValidatorTest {

    @Test
    public void validate_valid() {
        JsonValidator validator = new JsonValidator();
        String json = "{\n" +
                "  \"formats\": [\n" +
                "    {\n" +
                "      \"column-name\": \"COL_TIMESTAMP\",\n" +
                "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        ValidationResult result = validator.validate("helloworld", json, null);
        Assertions.assertTrue(result.isValid());
    }

    @Test
    public void validate_invalid() {
        JsonValidator validator = new JsonValidator();
        String json = "{\n" +
                "  \"formats\": [\n" +
                "    {\n" +
                "      \"column-name\": \"COL_TIMESTAMP\",\n" +
                "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                "      \"type: \"TIMESTAMP_MILLIS\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        ValidationResult result = validator.validate("helloworld", json, null);
        Assertions.assertFalse(result.isValid());
    }

}