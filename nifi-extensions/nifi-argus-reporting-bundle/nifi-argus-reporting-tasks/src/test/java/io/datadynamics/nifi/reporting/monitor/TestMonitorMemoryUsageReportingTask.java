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
import org.apache.nifi.components.ValidationResult;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MonitorMemoryUsageReportingTask}에 대한 테스트.
 * JVM 힙 사용량이 임계치를 초과했을 때 HTTP 알림이 전송되는지,
 * 그리고 THRESHOLD_PROPERTY의 검증 로직(퍼센트/데이터 크기 형식 허용, 그 외 형식 거부)이
 * 올바르게 동작하는지를 검증한다.
 */
public class TestMonitorMemoryUsageReportingTask {

    /**
     * 임계치를 극단적으로 낮게("1 B") 설정하여 항상 초과되도록 만든 뒤 onTrigger를 호출하면,
     * 임베디드 테스트 서버로 HTTP 알림이 전송되고 그 body에 JVMHeapUsage 타입과 사용량 정보가
     * 포함되는지 확인한다.
     */
    @Test
    public void testSendsHttpNotificationWhenThresholdExceeded() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final MonitorMemoryUsageReportingTask task = new MonitorMemoryUsageReportingTask();
            task.initialize(new MockReportingInitializationContext("id", "MonitorMemoryUsage", new MockComponentLog("id", task)));

            final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
            configuredProperties.put(MonitorMemoryUsageReportingTask.THRESHOLD_PROPERTY, "1 B");
            configuredProperties.put(MonitorMemoryUsageReportingTask.REPORTING_INTERVAL, "10 millis");
            task.onConfigured(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

            final MockReportingContext context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
            context.setProperty(MonitorMemoryUsageReportingTask.THRESHOLD_PROPERTY.getName(), "1 B");
            context.setProperty(MonitorMemoryUsageReportingTask.EXTERNAL_HTTP_URL_ENABLE.getName(), "true");
            context.setProperty(MonitorMemoryUsageReportingTask.EXTERNAL_HTTP_URL.getName(), httpServer.getUrl());

            task.onTrigger(context);

            assertEquals(1, httpServer.getRequests().size());
            final String body = httpServer.getRequests().poll().body();
            assertTrue(body.contains("\"type\":\"JVMHeapUsage\""));
            assertTrue(body.contains("\"used\""));
        }
    }

    /**
     * THRESHOLD_PROPERTY의 Validator가 퍼센트 표기("65%")와 데이터 크기 표기("100 MB")는
     * 유효한 값으로 인정하고, 형식에 맞지 않는 값("invalid")은 유효하지 않은 값으로 판정하는지 확인한다.
     */
    @Test
    public void testThresholdValidator() {
        final ValidationResult percentResult = MonitorMemoryUsageReportingTask.THRESHOLD_PROPERTY.getValidators().get(0)
                .validate("메모리 사용율", "65%", null);
        assertTrue(percentResult.isValid());

        final ValidationResult dataSizeResult = MonitorMemoryUsageReportingTask.THRESHOLD_PROPERTY.getValidators().get(0)
                .validate("메모리 사용율", "100 MB", null);
        assertTrue(dataSizeResult.isValid());

        final ValidationResult invalidResult = MonitorMemoryUsageReportingTask.THRESHOLD_PROPERTY.getValidators().get(0)
                .validate("메모리 사용율", "invalid", null);
        assertFalse(invalidResult.isValid());
    }
}
