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
package io.datadynamics.nifi.flowanalysis;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.VersionedControllerServiceLookup;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.FlowAnalysisContext;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.util.MockPropertyValue;

/**
 * Flow Analysis Rule 단위 테스트용 경량 하네스.
 *
 * nifi-mock에는 flowanalysis 컨텍스트 목이 없어, PropertyContext/FlowAnalysisRuleContext를
 * 최소 구현하고 Versioned* 컴포넌트를 세터로 조립하는 헬퍼를 제공한다.
 */
final class RuleTestSupport {

    private RuleTestSupport() {
    }

    /** 프로퍼티 미설정 시 디스크립터 기본값을 돌려주는 최소 컨텍스트. */
    static final class TestContext implements FlowAnalysisRuleContext {
        private final Map<PropertyDescriptor, String> properties = new HashMap<>();
        private int maxTimerDrivenThreadCount = 10;

        TestContext set(PropertyDescriptor descriptor, String value) {
            properties.put(descriptor, value);
            return this;
        }

        TestContext maxTimerDrivenThreads(int count) {
            this.maxTimerDrivenThreadCount = count;
            return this;
        }

        @Override
        public PropertyValue getProperty(PropertyDescriptor descriptor) {
            final String raw = properties.getOrDefault(descriptor, descriptor.getDefaultValue());
            return new MockPropertyValue(raw);
        }

        @Override
        public Map<String, String> getAllProperties() {
            final Map<String, String> out = new HashMap<>();
            properties.forEach((descriptor, value) -> out.put(descriptor.getName(), value));
            return out;
        }

        @Override
        public Map<PropertyDescriptor, String> getProperties() {
            return properties;
        }

        @Override
        public String getRuleName() {
            return "test-rule";
        }

        @Override
        public StateManager getStateManager() {
            return null;
        }

        @Override
        public FlowAnalysisContext getFlowAnalysisContext() {
            return new FlowAnalysisContext() {
                @Override
                public VersionedControllerServiceLookup getVersionedControllerServiceLookup() {
                    return null;
                }

                @Override
                public int getMaxTimerDrivenThreadCount() {
                    return maxTimerDrivenThreadCount;
                }

                @Override
                public boolean isClustered() {
                    return false;
                }

                @Override
                public Optional<String> getClusterNodeIdentifier() {
                    return Optional.empty();
                }
            };
        }
    }

    // ---- Versioned* 조립 헬퍼 ----

    static VersionedProcessor processor(String id, String type, Integer concurrentTasks) {
        final VersionedProcessor p = new VersionedProcessor();
        p.setIdentifier(id);
        p.setName(id);
        p.setType(type);
        p.setConcurrentlySchedulableTaskCount(concurrentTasks);
        return p;
    }

    static VersionedProcessor listProcessor(String id, String type, String schedulingStrategy, String schedulingPeriod) {
        final VersionedProcessor p = processor(id, type, 1);
        p.setSchedulingStrategy(schedulingStrategy);
        p.setSchedulingPeriod(schedulingPeriod);
        return p;
    }

    static VersionedFunnel funnel(String id) {
        final VersionedFunnel f = new VersionedFunnel();
        f.setIdentifier(id);
        f.setName(id);
        return f;
    }

    static VersionedConnection connection(String sourceId, String destinationId) {
        final VersionedConnection c = new VersionedConnection();
        // VersionedConnection의 equals가 identifier 기반이라, Set에 담을 때 중복 제거되지 않도록 고유 id 부여
        c.setIdentifier(sourceId + "->" + destinationId);
        c.setSource(connectable(sourceId));
        c.setDestination(connectable(destinationId));
        return c;
    }

    private static ConnectableComponent connectable(String id) {
        final ConnectableComponent cc = new ConnectableComponent();
        cc.setId(id);
        cc.setType(ConnectableComponentType.PROCESSOR);
        return cc;
    }

    @SafeVarargs
    static <T> Set<T> setOf(T... items) {
        return new java.util.LinkedHashSet<>(java.util.Arrays.asList(items));
    }
}
