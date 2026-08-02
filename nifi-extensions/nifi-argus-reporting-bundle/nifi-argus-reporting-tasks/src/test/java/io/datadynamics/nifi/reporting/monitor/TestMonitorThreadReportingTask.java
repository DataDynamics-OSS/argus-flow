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

import io.datadynamics.nifi.reporting.TestHttpServerSupport;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockReportingContext;
import org.apache.nifi.util.MockReportingInitializationContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MonitorThreadReportingTask}에 대한 테스트.
 * 현재 스레드 개수가 임계치를 초과했을 때만 HTTP 알림이 전송되고,
 * 임계치 이하일 때는 알림이 전송되지 않는지를 검증한다.
 */
public class TestMonitorThreadReportingTask {

    /**
     * 임계치를 1로 설정하여(실제 JVM 스레드 수는 항상 1보다 많으므로) 항상 초과되도록 만든 뒤
     * onTrigger를 호출하면, 임베디드 테스트 서버로 HTTP 알림이 전송되고 그 body에
     * JVMTheadUsage 타입과 현재 스레드 수 정보가 포함되는지 확인한다.
     */
    @Test
    public void testSendsHttpNotificationWhenThresholdExceeded() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final MonitorThreadReportingTask task = new MonitorThreadReportingTask();
            task.initialize(new MockReportingInitializationContext("id", "MonitorThread", new MockComponentLog("id", task)));

            final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
            configuredProperties.put(MonitorThreadReportingTask.REPORTING_INTERVAL, "10 millis");
            task.onConfigured(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

            final MockReportingContext context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
            context.setProperty(MonitorThreadReportingTask.THRESHOLD_PROPERTY.getName(), "1");
            context.setProperty(MonitorThreadReportingTask.EXTERNAL_HTTP_URL_ENABLE.getName(), "true");
            context.setProperty(MonitorThreadReportingTask.EXTERNAL_HTTP_URL.getName(), httpServer.getUrl());

            task.onTrigger(context);

            assertEquals(1, httpServer.getRequests().size());
            final String body = httpServer.getRequests().poll().body();
            assertTrue(body.contains("\"type\":\"JVMTheadUsage\""));
            assertTrue(body.contains("\"currentThreadCount\""));
        }
    }

    /**
     * 임계치를 Integer.MAX_VALUE로 설정하여 절대 초과할 수 없도록 만든 뒤 onTrigger를 호출하면,
     * 테스트 서버로 어떠한 HTTP 요청도 전송되지 않는지(알림이 발생하지 않는지) 확인한다.
     */
    @Test
    public void testDoesNotNotifyWhenBelowThreshold() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final MonitorThreadReportingTask task = new MonitorThreadReportingTask();
            task.initialize(new MockReportingInitializationContext("id", "MonitorThread", new MockComponentLog("id", task)));

            final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
            configuredProperties.put(MonitorThreadReportingTask.REPORTING_INTERVAL, "10 millis");
            task.onConfigured(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

            final MockReportingContext context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
            context.setProperty(MonitorThreadReportingTask.THRESHOLD_PROPERTY.getName(), String.valueOf(Integer.MAX_VALUE));
            context.setProperty(MonitorThreadReportingTask.EXTERNAL_HTTP_URL_ENABLE.getName(), "true");
            context.setProperty(MonitorThreadReportingTask.EXTERNAL_HTTP_URL.getName(), httpServer.getUrl());

            task.onTrigger(context);

            assertEquals(0, httpServer.getRequests().size());
        }
    }
}
