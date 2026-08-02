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

/**
 * Timestamp 컬럼의 정밀도(초/밀리초/마이크로초) 유형과 그에 대응하는 기본 파싱 패턴을 정의하는 열거형입니다.
 */
public enum TimestampType {

    DATE("yyyy-MM-dd"), TIMESTAMP("yyyy-MM-dd HH:mm:ss"), TIMESTAMP_MILLIS("yyyy-MM-dd HH:mm:ss.SSS"), TIMESTAMP_MICROS("yyyy-MM-dd HH:mm:ss.SSSSSS");

    /**
     * 이 유형에 대응하는 기본 날짜/시간 파싱 패턴
     */
    private final String pattern;

    TimestampType(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }

}