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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/util/hive/ValidationResources.java
 */
package io.datadynamics.nifi.processors.hive.util;

import org.apache.hadoop.conf.Configuration;

/**
 * 로드된 설정(Configuration)을 유지하기 위한 헬퍼 클래스로, 필요한 경우가 아니면 매번 재로드하는 것을 방지한다.
 * 프로퍼티 검증(validation) 시점마다 Hadoop Configuration 리소스를 다시 파싱하는 비용을 줄이기 위해
 * 마지막으로 로드된 리소스 경로와 그 결과 Configuration 객체를 함께 캐시해 둔다.
 */
public class ValidationResources {

    // 설정 로드에 사용된 리소스 경로(콤마로 구분된 목록 등). 이 값이 변경되었는지 비교하여 재로드 여부를 판단한다.
    private final String configResources;
    // configResources로부터 로드된 Hadoop Configuration 객체.
    private final Configuration configuration;

    public ValidationResources(String configResources, Configuration configuration) {
        this.configResources = configResources;
        this.configuration = configuration;
    }

    public String getConfigResources() {
        return configResources;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
