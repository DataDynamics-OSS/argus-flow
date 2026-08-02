/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/record/field/FieldConverter.java
 */
package io.datadynamics.nifi.processors.kudu.converter;

import java.util.Optional;

/**
 * 선택적인 포맷 파싱을 지원하며 필드의 자료형을 변환하기 위한 범용 Field Converter 인터페이스
 *
 * @param <I> 입력 필드 타입
 * @param <O> 출력 필드 타입
 */
public interface FieldConverter<I, O> {

    /**
     * 선택적인 포맷 파싱을 사용하여 필드를 출력 필드 타입으로 변환합니다.
     *
     * @param field            변환할 입력 필드
     * @param pattern          파싱시 사용할 선택적 포맷 패턴
     * @param name             추적을 위한 입력 필드명
     * @param addHour          추가할 시간(Hour)
     * @param timestampPattern Timestamp 패턴
     * @return 변환된 필드. 입력 필드가 null이거나 빈 값이면 null을 반환할 수 있습니다.
     */
    O convertField(I field, Optional<String> pattern, String name, int addHour, String timestampPattern);
}