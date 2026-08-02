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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * <b>Iceberg 싱크 앞단 Merge 요구 검사.</b>
 *
 * <p>PutIceberg 계열 싱크 프로세서의 상류 N홉(기본 3) 안에 Merge 계열 프로세서(MergeContent·
 * MergeRecord 등)가 없으면 위반을 표시한다. 작은 FlowFile을 개별로 Iceberg에 쓰면 데이터/메타데이터
 * 파일이 잘게 쪼개지는 <em>small-file 문제</em>가 생겨 테이블 메타데이터가 폭증하고 쿼리 성능이
 * 급격히 저하된다. 싱크 앞에서 배치로 병합하도록 강제한다.</p>
 *
 * <p>연결 그래프를 상류로 거슬러 탐색해야 하므로 프로세스 그룹 단위로 평가한다(그룹 경계를 넘는
 * 상류는 추적하지 않는다).</p>
 */
@Tags({"argus", "iceberg", "merge", "small-file", "governance"})
@CapabilityDescription("PutIceberg 계열 싱크의 상류 N홉 안에 Merge 계열 프로세서가 없으면 small-file 위험으로 위반을 표시한다.")
public class IcebergSinkMergeRule extends AbstractFlowAnalysisRule {

    public static final PropertyDescriptor SINK_TYPE_PATTERN = new PropertyDescriptor.Builder()
            .name("sink-type-pattern")
            .displayName("싱크 타입 패턴(정규식)")
            .description("Merge 선행을 요구할 싱크 프로세서의 단순 클래스명 정규식.")
            .required(true)
            .defaultValue("PutIceberg.*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor MERGE_TYPE_PATTERN = new PropertyDescriptor.Builder()
            .name("merge-type-pattern")
            .displayName("Merge 타입 패턴(정규식)")
            .description("상류에서 찾을 병합 프로세서의 단순 클래스명 정규식.")
            .required(true)
            .defaultValue("Merge.*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_DISTANCE = new PropertyDescriptor.Builder()
            .name("max-distance")
            .displayName("최대 탐색 홉 수")
            .description("각 싱크에서 상류로 Merge 프로세서를 찾을 최대 연결 홉(hop) 수.")
            .required(true)
            .defaultValue("3")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            List.of(SINK_TYPE_PATTERN, MERGE_TYPE_PATTERN, MAX_DISTANCE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup,
                                                               FlowAnalysisRuleContext context) {
        final Pattern sinkPattern = Pattern.compile(context.getProperty(SINK_TYPE_PATTERN).getValue());
        final Pattern mergePattern = Pattern.compile(context.getProperty(MERGE_TYPE_PATTERN).getValue());
        final int maxDistance = context.getProperty(MAX_DISTANCE).asInteger();

        // 컴포넌트 id → 프로세서 (상류 타입 판정용)
        final Map<String, VersionedProcessor> processorsById = new HashMap<>();
        for (VersionedProcessor processor : processGroup.getProcessors()) {
            processorsById.put(processor.getIdentifier(), processor);
        }

        // destination id → 직접 상류(source) id 목록 (역방향 인접 리스트)
        final Map<String, List<String>> upstreamOf = new HashMap<>();
        for (VersionedConnection connection : processGroup.getConnections()) {
            final ConnectableComponent source = connection.getSource();
            final ConnectableComponent destination = connection.getDestination();
            if (source != null && destination != null && source.getId() != null && destination.getId() != null) {
                upstreamOf.computeIfAbsent(destination.getId(), k -> new ArrayList<>()).add(source.getId());
            }
        }

        final Collection<GroupAnalysisResult> results = new ArrayList<>();
        for (VersionedProcessor processor : processGroup.getProcessors()) {
            if (sinkPattern.matcher(simpleType(processor)).matches()
                    && !hasUpstreamMerge(processor.getIdentifier(), upstreamOf, processorsById, mergePattern, maxDistance)) {
                results.add(GroupAnalysisResult.forComponent(
                        processor,
                        "iceberg-sink-merge",
                        "'" + simpleType(processor) + "' 상류 " + maxDistance + "홉 안에 Merge 계열 프로세서가 없습니다. "
                                + "작은 FlowFile을 개별 기록하면 Iceberg small-file 문제(메타데이터 폭증·쿼리 저하)가 발생합니다. "
                                + "싱크 앞단에 MergeContent/MergeRecord 배치 병합을 두세요."
                ).build());
            }
        }
        return results;
    }

    /** 싱크에서 상류로 BFS하며 maxDistance 홉 안에 merge 패턴 프로세서가 있는지 확인한다. */
    private boolean hasUpstreamMerge(String sinkId,
                                     Map<String, List<String>> upstreamOf,
                                     Map<String, VersionedProcessor> processorsById,
                                     Pattern mergePattern,
                                     int maxDistance) {
        final Set<String> visited = new HashSet<>();
        // (컴포넌트 id, 현재까지의 홉 수)
        final Deque<String> frontier = new ArrayDeque<>();
        final Deque<Integer> depths = new ArrayDeque<>();
        frontier.add(sinkId);
        depths.add(0);
        visited.add(sinkId);

        while (!frontier.isEmpty()) {
            final String currentId = frontier.poll();
            final int depth = depths.poll();
            if (depth >= maxDistance) {
                continue;
            }
            for (String upstreamId : upstreamOf.getOrDefault(currentId, List.of())) {
                if (!visited.add(upstreamId)) {
                    continue;
                }
                final VersionedProcessor upstream = processorsById.get(upstreamId);
                if (upstream != null && mergePattern.matcher(simpleType(upstream)).matches()) {
                    return true;
                }
                // 프로세서가 아니거나(포트·Funnel) Merge가 아니면 계속 상류로 탐색.
                frontier.add(upstreamId);
                depths.add(depth + 1);
            }
        }
        return false;
    }

    private static String simpleType(VersionedProcessor processor) {
        final String type = processor.getType();
        return type == null ? "" : type.substring(type.lastIndexOf('.') + 1);
    }
}
