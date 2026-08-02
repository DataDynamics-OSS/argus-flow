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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;

/**
 * <b>리스팅 프로세서의 0초 스케줄 방지 검사.</b>
 *
 * <p>ListHDFS/ListSFTP/ListFile/ListDatabaseTables 등 "목록 조회형" 프로세서가 Run Schedule
 * 0초(가능한 한 자주)로 설정되면, 원격 소스(HDFS·FTP·오브젝트 스토리지·DB)를 초당 수백~수천 번
 * 반복 조회해 소스 시스템에 과부하와 throttling을 유발한다. 실무에서 가장 흔한 성능 사고 중 하나다.</p>
 *
 * <p>대상 프로세서 목록과, 타이머 구동(Timer-Driven) 전략에서 스케줄이 0인 경우만 검사한다
 * (CRON 구동은 대상 아님).</p>
 */
@Tags({"argus", "list", "scheduling", "source-load", "governance"})
@CapabilityDescription("리스팅 계열 프로세서가 Run Schedule 0초(Timer-Driven)로 설정되면 소스 과부하 위험으로 위반을 표시한다.")
public class ListingScheduleGuardRule extends AbstractFlowAnalysisRule {

    /** 기본 검사 대상 — NiFi 표준 리스팅 프로세서들의 단순 클래스명. */
    private static final List<String> DEFAULT_LISTING_PROCESSORS = List.of(
            "ListFile", "ListFTP", "ListSFTP", "ListHDFS",
            "ListDatabaseTables", "ListGCSBucket", "ListS3",
            "ListAzureBlobStorage_v12", "ListAzureDataLakeStorage"
    );

    public static final PropertyDescriptor TARGET_PROCESSORS = new PropertyDescriptor.Builder()
            .name("target-processors")
            .displayName("검사 대상 프로세서")
            .description("0초 스케줄을 금지할 리스팅 프로세서의 단순 클래스명 목록(쉼표 구분).")
            .required(true)
            .defaultValue(String.join(",", DEFAULT_LISTING_PROCESSORS))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(TARGET_PROCESSORS);

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

        final Set<String> targets = Arrays.stream(context.getProperty(TARGET_PROCESSORS).getValue().split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        final String simpleType = simpleType(processor);
        if (!targets.contains(simpleType)) {
            return results;
        }

        // 타이머 구동이면서 스케줄 주기가 0으로 시작하면(0 sec, 0 ms 등) 위반.
        if (isTimerDriven(processor) && startsWithZero(processor.getSchedulingPeriod())) {
            results.add(new ComponentAnalysisResult(
                    "listing-schedule-guard",
                    "'" + simpleType + "'의 Run Schedule이 0으로 설정돼 있습니다. 리스팅 프로세서의 0초 스케줄은 "
                            + "원격 소스를 과도하게 반복 조회해 과부하·throttling을 유발합니다. 유의미한 주기(예: 1 min 이상)를 권장합니다."
            ));
        }
        return results;
    }

    private static boolean isTimerDriven(VersionedProcessor processor) {
        final String strategy = processor.getSchedulingStrategy();
        if (strategy == null) {
            return false;
        }
        try {
            return SchedulingStrategy.valueOf(strategy) == SchedulingStrategy.TIMER_DRIVEN;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static boolean startsWithZero(String schedulingPeriod) {
        return schedulingPeriod != null && schedulingPeriod.trim().startsWith("0");
    }

    private static String simpleType(VersionedProcessor processor) {
        final String type = processor.getType();
        return type == null ? "" : type.substring(type.lastIndexOf('.') + 1);
    }
}
