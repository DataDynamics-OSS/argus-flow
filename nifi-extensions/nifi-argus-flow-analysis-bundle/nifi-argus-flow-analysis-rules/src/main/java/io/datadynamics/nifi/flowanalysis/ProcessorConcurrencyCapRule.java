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
/*
 * Argus Flow — NiFi Flow Analysis Rule (신규 구현)
 *
 * Flow Analysis Rule API(org.apache.nifi.flowanalysis)는 Apache NiFi 공개 API이며,
 * 아래 규칙 로직은 사내에서 새로 작성했다.
 */
package io.datadynamics.nifi.flowanalysis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * <b>프로세서 타입별 동시성(Concurrent Tasks) 상한 검사.</b>
 *
 * <p>지정한 타입(정규식)·버전(정규식)에 해당하는 프로세서의 Concurrent Tasks가 상한(기본 1)을
 * 넘으면 위반을 표시한다. 특정 프로세서에 높은 동시성을 주면 외부 시스템 커넥션 폭증·순서 뒤섞임·
 * 리소스 경합이 생기는데, 이를 정책으로 강제한다. 예: DB 계열 프로세서는 커넥션 풀 보호를 위해
 * 동시성 1~2로 제한.</p>
 *
 * <p>타입/버전을 정규식으로 받으므로 규칙 인스턴스를 여러 개 등록해 프로세서별로 다른 상한을
 * 적용할 수 있다.</p>
 */
@Tags({"argus", "concurrency", "concurrent-tasks", "governance"})
@CapabilityDescription("지정 타입/버전(정규식) 프로세서의 Concurrent Tasks가 상한(기본 1)을 초과하면 위반을 표시한다.")
public class ProcessorConcurrencyCapRule extends AbstractFlowAnalysisRule {

    /** 검사 대상 프로세서 타입(정규식). 전체 클래스명 기준으로 매칭한다. */
    public static final PropertyDescriptor TARGET_TYPE = new PropertyDescriptor.Builder()
            .name("target-type")
            .displayName("대상 프로세서 타입(정규식)")
            .description("동시성 상한을 적용할 프로세서 타입에 대한 정규식. 프로세서 전체 클래스명과 매칭한다(예: .*PutDatabaseRecord).")
            .required(true)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    /** 검사 대상 버전(정규식). 기본은 모든 버전. */
    public static final PropertyDescriptor TARGET_VERSION = new PropertyDescriptor.Builder()
            .name("target-version")
            .displayName("대상 버전(정규식)")
            .description("대상 프로세서 버전에 대한 정규식. 기본값은 모든 버전.")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    /** 허용할 최대 Concurrent Tasks. */
    public static final PropertyDescriptor MAX_CONCURRENT_TASKS = new PropertyDescriptor.Builder()
            .name("max-concurrent-tasks")
            .displayName("최대 Concurrent Tasks")
            .description("대상 프로세서에 허용할 최대 Concurrent Tasks 수.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            List.of(TARGET_TYPE, TARGET_VERSION, MAX_CONCURRENT_TASKS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component,
                                                                FlowAnalysisRuleContext context) {
        final Collection<ComponentAnalysisResult> results = new ArrayList<>();
        if (!(component instanceof VersionedProcessor)) {
            return results;
        }
        final VersionedProcessor processor = (VersionedProcessor) component;

        final Pattern typePattern = Pattern.compile(context.getProperty(TARGET_TYPE).getValue());
        final Pattern versionPattern = Pattern.compile(context.getProperty(TARGET_VERSION).getValue());
        final int maxConcurrent = context.getProperty(MAX_CONCURRENT_TASKS).asInteger();

        final String type = processor.getType() == null ? "" : processor.getType();
        final String version = processor.getBundle() == null ? "" : nullToEmpty(processor.getBundle().getVersion());
        if (!typePattern.matcher(type).matches() || !versionPattern.matcher(version).matches()) {
            return results;
        }

        final Integer concurrentTasks = processor.getConcurrentlySchedulableTaskCount();
        if (concurrentTasks != null && concurrentTasks > maxConcurrent) {
            results.add(new ComponentAnalysisResult(
                    "processor-concurrency-cap",
                    "'" + type.substring(type.lastIndexOf('.') + 1) + "'의 Concurrent Tasks(" + concurrentTasks
                            + ")가 허용 상한(" + maxConcurrent + ")을 초과합니다. 커넥션 폭증·순서 뒤섞임·리소스 경합의 위험이 있습니다."
            ));
        }
        return results;
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
