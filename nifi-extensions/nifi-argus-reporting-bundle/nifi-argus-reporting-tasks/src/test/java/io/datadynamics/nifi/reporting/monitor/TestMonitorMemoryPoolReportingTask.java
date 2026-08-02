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
package io.datadynamics.nifi.reporting.monitor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockReportingContext;
import org.apache.nifi.util.MockReportingInitializationContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link MonitorMemoryPoolReportingTask}에 대한 테스트.
 * 유효한 Memory Pool 이름으로 설정했을 때 정상적으로 트리거되는지,
 * 그리고 존재하지 않는 Memory Pool 이름을 설정했을 때 초기화 예외가 발생하는지를 검증한다.
 */
public class TestMonitorMemoryPoolReportingTask {

    // 현재 JVM에서 collection usage threshold 감시를 지원하는 Memory Pool 중 첫 번째 이름을 찾는다.
    // 지원하는 풀이 없는 실행 환경(JVM 구현체에 따라 다름)에서는 관련 테스트를 건너뛰기 위해 사용한다.
    private static Optional<String> findSupportedMemoryPool() {
        return ManagementFactory.getMemoryPoolMXBeans().stream()
                .filter(MemoryPoolMXBean::isCollectionUsageThresholdSupported)
                .map(MemoryPoolMXBean::getName)
                .findFirst();
    }

    /**
     * 실제로 collection usage threshold를 지원하는 Memory Pool 이름과 임계치(99%)를 설정한 뒤,
     * onConfigured/onTrigger/onStopped 흐름이 예외 없이 정상적으로 수행되는지 확인한다.
     */
    @Test
    public void testConfigureAndTriggerWithValidMemoryPool() throws Exception {
        final Optional<String> memoryPool = findSupportedMemoryPool();
        Assumptions.assumeTrue(memoryPool.isPresent(), "Collection usage threshold를 지원하는 Memory Pool이 없습니다.");

        final MonitorMemoryPoolReportingTask task = new MonitorMemoryPoolReportingTask();
        task.initialize(new MockReportingInitializationContext("id", "MonitorMemoryPool", new MockComponentLog("id", task)));

        final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
        configuredProperties.put(MonitorMemoryPoolReportingTask.MEMORY_POOL_PROPERTY, memoryPool.get());
        configuredProperties.put(MonitorMemoryPoolReportingTask.THRESHOLD_PROPERTY, "99%");
        configuredProperties.put(MonitorMemoryPoolReportingTask.REPORTING_INTERVAL, "10 millis");
        task.onConfigured(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

        final MockReportingContext context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
        context.setProperty(MonitorMemoryPoolReportingTask.MEMORY_POOL_PROPERTY.getName(), memoryPool.get());
        context.setProperty(MonitorMemoryPoolReportingTask.THRESHOLD_PROPERTY.getName(), "99%");
        context.setProperty(MonitorMemoryPoolReportingTask.EXTERNAL_HTTP_URL_ENABLE.getName(), "false");

        assertDoesNotThrow(() -> task.onTrigger(context));

        task.onStopped();
    }

    /**
     * 존재하지 않는 Memory Pool 이름("No Such Pool")을 설정한 뒤 onConfigured를 호출하면
     * InitializationException이 발생하는지 확인한다.
     */
    @Test
    public void testUnknownMemoryPoolThrowsInitializationException() throws Exception {
        final MonitorMemoryPoolReportingTask task = new MonitorMemoryPoolReportingTask();
        task.initialize(new MockReportingInitializationContext("id", "MonitorMemoryPool", new MockComponentLog("id", task)));

        final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
        configuredProperties.put(MonitorMemoryPoolReportingTask.MEMORY_POOL_PROPERTY, "No Such Pool");
        configuredProperties.put(MonitorMemoryPoolReportingTask.THRESHOLD_PROPERTY, "99%");
        configuredProperties.put(MonitorMemoryPoolReportingTask.REPORTING_INTERVAL, "10 millis");

        assertThrows(InitializationException.class,
                () -> task.onConfigured(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap())));
    }
}
