/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/converter/DataConverter.java
 */
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
package org.apache.nifi.processors.iceberg.converter;

/**
 * NiFi 레코드(Record)와 Iceberg 레코드 간의 데이터 변환을 담당하는 추상 클래스.
 * <p>
 * 스키마 트리를 순회하며 생성되는 모든 변환기(converter)의 공통 상위 타입으로,
 * 소스(S) 타입의 값을 대상(T) 타입의 값으로 변환하는 단일 책임을 가진다.
 * 필드 단위 변환기의 경우 소스/대상 필드 이름을 함께 보관하여, 상위 레코드
 * 변환기가 NiFi 레코드에서 값을 조회하고 Iceberg 레코드에 값을 기록할 때
 * 사용할 필드 이름을 알 수 있도록 한다.
 *
 * @param <S> 변환 전(소스) 데이터 타입, 보통 NiFi 레코드 필드 값
 * @param <T> 변환 후(대상) 데이터 타입, 보통 Iceberg가 기대하는 값
 */
public abstract class DataConverter<S, T> {

    // 이 변환기가 값을 읽어올 NiFi 레코드 스키마상의 필드 이름
    private String sourceFieldName;
    // 이 변환기가 값을 기록할 Iceberg 스키마상의 필드 이름
    private String targetFieldName;

    public String getSourceFieldName() {
        return sourceFieldName;
    }

    public String getTargetFieldName() {
        return targetFieldName;
    }

    public void setSourceFieldName(String sourceFieldName) {
        this.sourceFieldName = sourceFieldName;
    }

    public void setTargetFieldName(String targetFieldName) {
        this.targetFieldName = targetFieldName;
    }

    // 실제 값 변환 로직. 각 하위 구현체가 소스 타입 값을 대상 타입 값으로 변환한다.
    abstract T convert(S data);
}
