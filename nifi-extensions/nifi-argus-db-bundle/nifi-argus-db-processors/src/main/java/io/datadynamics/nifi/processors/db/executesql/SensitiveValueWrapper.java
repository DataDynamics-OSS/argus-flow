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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-database-utils/src/main/java/org/apache/nifi/util/db/SensitiveValueWrapper.java
 */
package io.datadynamics.nifi.processors.db.executesql;

/**
 * 동적 속성(Dynamic Property) 값이 민감(sensitive) 정보인지 여부를 함께 담아 전달하기 위한
 * 간단한 래퍼(Wrapper) 클래스. 값 자체와 민감 여부 플래그를 한 쌍으로 유지하여, 로깅이나
 * 속성 처리 시 민감 값이 노출되지 않도록 구분하는 데 사용된다.
 */
public class SensitiveValueWrapper {

    private final String value;
    private final boolean sensitive;

    public SensitiveValueWrapper(final String value, final boolean sensitive) {
        this.value = value;
        this.sensitive = sensitive;
    }

    public String getValue() {
        return value;
    }

    public boolean isSensitive() {
        return sensitive;
    }
}