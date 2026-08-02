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

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.time.format.DateTimeFormatter;

/**
 * PropertyDescriptor에 입력된 값이 {@link DateTimeFormatter}로 해석 가능한 유효한 날짜/시간 패턴인지 검증하는 Validator입니다.
 * PutKudu의 'Timestamp 컬럼의 기본 날짜 패턴'과 같은 패턴 문자열 속성에 사용합니다.
 */
public class TimestampValidator implements Validator {

    /**
     * 입력값으로 DateTimeFormatter를 생성해보고, 예외 없이 생성되면 유효한 패턴으로 판단합니다.
     */
    @Override
    public ValidationResult validate(String subject, String value, ValidationContext context) {
        try {
            DateTimeFormatter.ofPattern(value);
            return (new ValidationResult.Builder()).valid(true).input(value).subject(subject).build();
        } catch (Exception e) {
            return (new ValidationResult.Builder()).valid(false).input(value).explanation("DateTimeFormatter 또는 SimpleDateFormat으로 변환이 가능한 패턴이어야 합니다. (예; yyyy-MM-dd HH:mm:ss.SSS)").build();
        }
    }

}