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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * <b>Timer-Driven 스레드 풀 상한 검사.</b>
 *
 * <p>NiFi의 전역 Timer-Driven 스레드 수(Max Timer Driven Thread Count)가 가용 CPU 코어 수의
 * N배(기본 4배)를 초과하면 위반을 표시한다. 스레드 풀을 코어 수 대비 과도하게 키우면 실제
 * 처리량은 늘지 않고 컨텍스트 스위칭·힙 압박만 커져 오히려 불안정해진다. 운영에서 흔히
 * 저지르는 "스레드만 늘리면 빨라진다"는 오설정을 조기에 잡는다.</p>
 *
 * <p>전역 설정이므로 루트 프로세스 그룹에 대해서만 1회 평가한다.</p>
 */
@Tags({"argus", "thread", "pool", "performance", "governance"})
@CapabilityDescription("전역 Timer-Driven 스레드 수가 가용 CPU 코어 수의 배수(기본 4배)를 초과하면 위반을 표시한다.")
public class TimerThreadPoolCeilingRule extends AbstractFlowAnalysisRule {

    /** 코어 수에 곱할 배수. 기본 4 — CPU 코어당 4스레드를 상한으로 본다. */
    public static final PropertyDescriptor CORE_MULTIPLIER = new PropertyDescriptor.Builder()
            .name("core-multiplier")
            .displayName("코어 배수 상한")
            .description("가용 CPU 코어 수에 곱해 스레드 상한을 계산할 배수. 스레드 수가 (코어 수 × 이 값)을 넘으면 위반.")
            .required(true)
            .defaultValue("4")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(CORE_MULTIPLIER);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup,
                                                               FlowAnalysisRuleContext context) {
        // 하위 그룹은 상위 그룹 식별자를 가진다. 전역 설정 검사이므로 루트(부모 없음)에서만 평가한다.
        if (processGroup.getGroupIdentifier() != null) {
            return Collections.emptySet();
        }

        final int multiplier = context.getProperty(CORE_MULTIPLIER).asInteger();
        final int availableCores = availableProcessors();
        final int ceiling = multiplier * availableCores;
        final int configured = context.getFlowAnalysisContext().getMaxTimerDrivenThreadCount();

        final Collection<GroupAnalysisResult> results = new ArrayList<>();
        if (configured > ceiling) {
            results.add(GroupAnalysisResult.forGroup(
                    "timer-thread-pool-ceiling",
                    "Timer-Driven 스레드 수(" + configured + ")가 권장 상한(코어 " + availableCores
                            + " × " + multiplier + " = " + ceiling + ")을 초과합니다. "
                            + "스레드 과다는 처리량 향상 없이 컨텍스트 스위칭·불안정을 유발합니다."
            ).build());
        }
        return results;
    }

    /** 테스트에서 코어 수를 고정할 수 있도록 분리한 protected 훅. */
    protected int availableProcessors() {
        return ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
    }
}
