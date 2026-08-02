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
import java.util.HashSet;
import java.util.Set;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;

/**
 * <b>막다른(dead-end) Funnel 탐지.</b>
 *
 * <p>유입 연결은 있는데 유출 연결이 하나도 없는 Funnel을 찾아 위반을 표시한다. 이런 Funnel로
 * 흘러든 FlowFile은 빠져나갈 곳이 없어 큐에 계속 쌓이고, 백프레셔가 걸리면 상류 프로세서까지
 * 멈춰 <em>데이터 정체가 조용히 전파</em>된다. 흔히 개발 중 연결을 지우다 남긴 Funnel이 원인이다.</p>
 *
 * <p>연결 구조를 봐야 하므로 컴포넌트 단위가 아니라 프로세스 그룹 단위로 평가한다.</p>
 */
@Tags({"argus", "funnel", "dead-end", "backpressure", "governance"})
@CapabilityDescription("유입은 있으나 유출이 없는 막다른 Funnel을 탐지해 위반을 표시한다(데이터 정체·백프레셔 유발).")
public class DeadEndFunnelRule extends AbstractFlowAnalysisRule {

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup,
                                                               FlowAnalysisRuleContext context) {
        final Collection<GroupAnalysisResult> results = new ArrayList<>();

        // 이 그룹의 연결을 훑어 각 컴포넌트가 source/destination으로 쓰인 적이 있는지 집합으로 모은다.
        final Set<String> hasOutgoing = new HashSet<>();  // source 로 등장한 컴포넌트 id
        final Set<String> hasIncoming = new HashSet<>();  // destination 으로 등장한 컴포넌트 id
        for (VersionedConnection connection : processGroup.getConnections()) {
            final ConnectableComponent source = connection.getSource();
            if (source != null && source.getId() != null) {
                hasOutgoing.add(source.getId());
            }
            final ConnectableComponent destination = connection.getDestination();
            if (destination != null && destination.getId() != null) {
                hasIncoming.add(destination.getId());
            }
        }

        for (VersionedFunnel funnel : processGroup.getFunnels()) {
            final String id = funnel.getIdentifier();
            // 유입은 있고 유출은 없는 Funnel = 막다른 길.
            if (hasIncoming.contains(id) && !hasOutgoing.contains(id)) {
                results.add(GroupAnalysisResult.forComponent(
                        funnel,
                        "dead-end-funnel",
                        "Funnel(" + id + ")이 유입 연결만 있고 유출 연결이 없습니다(막다른 길). "
                                + "유입된 FlowFile이 빠져나갈 수 없어 큐 적체·백프레셔로 상류가 정지될 수 있습니다."
                ).build());
            }
        }
        return results;
    }
}
