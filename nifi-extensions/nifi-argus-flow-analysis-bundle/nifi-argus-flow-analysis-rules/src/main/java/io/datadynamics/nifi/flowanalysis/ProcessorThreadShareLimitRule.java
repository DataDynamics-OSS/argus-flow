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
 * <b>단일 프로세서의 스레드 점유율 제한 검사.</b>
 *
 * <p>한 프로세서의 Concurrent Tasks 수가 전역 Timer-Driven 스레드 풀의 일정 비율(기본 50%)을
 * 넘으면 위반을 표시한다. 프로세서 하나가 공유 스레드 풀을 과점하면 같은 노드의 다른 프로세서가
 * 스레드를 얻지 못해 기아(starvation) 상태가 되고, 플로우 전체 지연으로 번진다.</p>
 *
 * <p>{@link TimerThreadPoolCeilingRule}이 풀 전체 크기를 본다면, 이 규칙은 그 풀을 한 프로세서가
 * 얼마나 가져가는지를 본다 — 두 규칙은 상호 보완적이다.</p>
 */
@Tags({"argus", "thread", "concurrency", "starvation", "governance"})
@CapabilityDescription("한 프로세서의 Concurrent Tasks가 전역 스레드 풀의 지정 비율(기본 50%)을 초과하면 위반을 표시한다.")
public class ProcessorThreadShareLimitRule extends AbstractFlowAnalysisRule {

    /** 단일 프로세서가 점유할 수 있는 전역 스레드 풀의 최대 비율(%). */
    public static final PropertyDescriptor MAX_THREAD_SHARE_PERCENT = new PropertyDescriptor.Builder()
            .name("max-thread-share-percent")
            .displayName("최대 스레드 점유율(%)")
            .description("한 프로세서의 Concurrent Tasks가 전역 Timer-Driven 스레드 풀에서 차지할 수 있는 최대 비율(%).")
            .required(true)
            .defaultValue("50")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(MAX_THREAD_SHARE_PERCENT);

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

        final int poolSize = context.getFlowAnalysisContext().getMaxTimerDrivenThreadCount();
        final int sharePercent = context.getProperty(MAX_THREAD_SHARE_PERCENT).asInteger();
        // 점유 상한(스레드 수). 정수 나눗셈이라 소수점은 버린다(보수적으로 낮게 잡힘).
        final int threadLimit = Math.floorDiv(poolSize * sharePercent, 100);

        final Integer concurrentTasks = processor.getConcurrentlySchedulableTaskCount();
        if (concurrentTasks != null && concurrentTasks > threadLimit) {
            results.add(new ComponentAnalysisResult(
                    "processor-thread-share",
                    "'" + simpleType(processor) + "'의 Concurrent Tasks(" + concurrentTasks
                            + ")가 전역 스레드 풀의 " + sharePercent + "%(=" + threadLimit
                            + " 스레드, 풀 " + poolSize + ")를 초과합니다. 다른 프로세서의 스레드 기아를 유발할 수 있습니다."
            ));
        }
        return results;
    }

    private static String simpleType(VersionedProcessor processor) {
        final String type = processor.getType();
        return type == null ? "" : type.substring(type.lastIndexOf('.') + 1);
    }
}
