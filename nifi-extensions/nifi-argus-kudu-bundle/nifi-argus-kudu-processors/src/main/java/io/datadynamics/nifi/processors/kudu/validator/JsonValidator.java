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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

/**
 * PropertyDescriptor 값이 유효한 JSON 형식인지 검증하는 Validator입니다.
 * PutKudu의 '컬럼별 Timestamp Format(JSON 형식)' 속성과 같이 JSON 문자열을 입력받는 속성에 사용합니다.
 */
public class JsonValidator implements Validator {

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * 입력값을 JSON Tree로 파싱해보고, 파싱에 성공하면 유효한 것으로 판단합니다.
     */
    @Override
    public ValidationResult validate(String subject, String value, ValidationContext context) {
        try {
            mapper.readTree(value.getBytes());
            return (new ValidationResult.Builder()).valid(true).input(value).subject(subject).build();
        } catch (Exception e) {
            return (new ValidationResult.Builder()).valid(false).input(value).explanation("JSON Format이어야 합니다.").build();
        }
    }

}
