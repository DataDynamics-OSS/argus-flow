/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/IcebergUtils.java
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
package org.apache.nifi.processors.iceberg;

import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Iceberg 프로세서 전반에서 사용하는 정적 유틸리티 메서드 모음.
 * Hadoop 설정 파일 로딩, 동적 속성 수집, 예외 원인 체인 탐색 기능을 제공한다.
 */
public class IcebergUtils {

    /**
     * 지정된 경로들의 설정 파일을 읽어 하나의 Hadoop Configuration으로 병합한다.
     * 뒤에 로드되는 파일의 속성이 앞서 로드된 동일 속성을 덮어쓴다.
     *
     * @param configFilePaths 콤마로 구분된 설정 파일 경로 목록
     * @return 병합된 설정
     */
    public static Configuration getConfigurationFromFiles(List<String> configFilePaths) {
        final Configuration conf = new Configuration();
        if (configFilePaths != null) {
            for (final String configFile : configFilePaths) {
                conf.addResource(new Path(configFile.trim()));
            }
        }
        return conf;
    }

    /**
     * 프로세서 컨텍스트에서 값이 비어 있지 않은 모든 동적 속성을 수집한다.
     * Expression Language가 포함된 값은 주어진 FlowFile의 속성을 기준으로 평가한 뒤 결과 값을 사용한다.
     *
     * @param context  프로세서 컨텍스트
     * @param flowFile 속성 표현식을 평가할 때 사용할 FlowFile
     * @return 동적 속성 이름과 평가된 값으로 이루어진 Map
     */
    public static Map<String, String> getDynamicProperties(ProcessContext context, FlowFile flowFile) {
        return context.getProperties().entrySet().stream()
                // 값이 비어 있지 않은 동적 속성만 필터링
                .filter(e -> e.getKey().isDynamic()
                        && StringUtils.isNotBlank(e.getValue())
                        && StringUtils.isNotBlank(context.getProperty(e.getKey()).evaluateAttributeExpressions(flowFile).getValue())
                )
                // 속성 이름과 평가된 값으로 Map 생성
                .collect(Collectors.toMap(
                        e -> e.getKey().getName(),
                        e -> context.getProperty(e.getKey()).evaluateAttributeExpressions(flowFile).getValue()
                ));
    }

    /**
     * 예외의 원인(cause) 체인을 순회하며, 지정된 타입에 해당하고 주어진 조건(predicate)을 만족하는
     * 첫 번째 Throwable을 반환한다. 해당하는 원인이 없으면 {@link Optional#empty()}를 반환한다.
     *
     * @param t 원인을 검사할 대상 Throwable
     * @return 조건을 만족하는 원인 Throwable
     */
    public static <T extends Throwable> Optional<T> findCause(Throwable t, Class<T> expectedCauseType, Predicate<T> causePredicate) {
        return Throwables.getCausalChain(t).stream()
                .filter(expectedCauseType::isInstance)
                .map(expectedCauseType::cast)
                .filter(causePredicate)
                .findFirst();
    }
}
