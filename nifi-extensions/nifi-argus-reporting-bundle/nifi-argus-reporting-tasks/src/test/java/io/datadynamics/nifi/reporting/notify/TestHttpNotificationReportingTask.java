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
package io.datadynamics.nifi.reporting.notify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HttpNotificationReportingTask}에 대한 테스트.
 * JVM 메트릭 포함 여부에 따라 알림 body의 구성이 달라지는지, 그리고 알림 대상 서버가
 * 응답하지 않는 상황에서도 예외 없이 정상적으로 처리되는지를 검증한다.
 */
public class TestHttpNotificationReportingTask {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 테스트마다 초기화된 HttpNotificationReportingTask 인스턴스를 생성하는 헬퍼 메서드.
    private HttpNotificationReportingTask initializedTask() throws Exception {
        final HttpNotificationReportingTask task = new HttpNotificationReportingTask();
        task.initialize(new MockReportingInitializationContext("id", "HttpNotification", new MockComponentLog("id", task)));
        return task;
    }

    /**
     * JVM 메트릭 포함 옵션을 활성화한 상태로 onTrigger를 호출하면, 전송된 요청 body의 JSON에
     * 타입/제목/본문/호스트명이 올바르게 담기고 jvmHeap, jvmThread 등 JVM 메트릭 필드도 함께
     * 포함되는지, 그리고 요청 헤더에 알림 타입이 실려있는지 확인한다.
     */
    @Test
    public void testSendsNotificationWithJvmMetrics() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final HttpNotificationReportingTask task = initializedTask();

            final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
            configuredProperties.put(HttpNotificationReportingTask.PROP_URL, httpServer.getUrl());
            task.onScheduled(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

            final MockReportingContext context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
            context.setProperty(HttpNotificationReportingTask.PROP_URL.getName(), httpServer.getUrl());
            context.setProperty(HttpNotificationReportingTask.PROP_NOTIFICATION_TYPE.getName(), "HEALTH_CHECK");
            context.setProperty(HttpNotificationReportingTask.PROP_SUBJECT.getName(), "테스트 제목");
            context.setProperty(HttpNotificationReportingTask.PROP_MESSAGE.getName(), "테스트 본문");
            context.setProperty(HttpNotificationReportingTask.PROP_JVM_METRICS.getName(), "true");

            task.onTrigger(context);

            assertEquals(1, httpServer.getRequests().size());
            final TestHttpServerSupport.ReceivedRequest received = httpServer.getRequests().poll();

            final JsonNode json = objectMapper.readTree(received.body());
            assertEquals("HEALTH_CHECK", json.get("type").asText());
            assertEquals("테스트 제목", json.get("subject").asText());
            assertEquals("테스트 본문", json.get("message").asText());
            assertNotNull(json.get("hostname"));
            assertTrue(json.get("jvmMetricsInclude").asBoolean());
            assertNotNull(json.get("jvmHeap"));
            assertNotNull(json.get("jvmThread"));

            assertEquals("HEALTH_CHECK", received.headers().get(HttpNotificationReportingTask.NOTIFICATION_TYPE_KEY.toLowerCase()).get(0));
        }
    }

    /**
     * JVM 메트릭 포함 옵션을 비활성화한 상태로 onTrigger를 호출하면, 전송된 요청 body의 JSON에
     * jvmMetricsInclude가 false로 표시되고 jvmHeap 필드가 아예 없거나 null로 처리되는지 확인한다.
     */
    @Test
    public void testSendsNotificationWithoutJvmMetrics() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final HttpNotificationReportingTask task = initializedTask();

            final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
            configuredProperties.put(HttpNotificationReportingTask.PROP_URL, httpServer.getUrl());
            task.onScheduled(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

            final MockReportingContext context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
            context.setProperty(HttpNotificationReportingTask.PROP_URL.getName(), httpServer.getUrl());
            context.setProperty(HttpNotificationReportingTask.PROP_NOTIFICATION_TYPE.getName(), "NIFI_STARTED");
            context.setProperty(HttpNotificationReportingTask.PROP_SUBJECT.getName(), "제목");
            context.setProperty(HttpNotificationReportingTask.PROP_JVM_METRICS.getName(), "false");

            task.onTrigger(context);

            assertEquals(1, httpServer.getRequests().size());
            final JsonNode json = objectMapper.readTree(httpServer.getRequests().poll().body());
            assertEquals("NIFI_STARTED", json.get("type").asText());
            assertFalse(json.get("jvmMetricsInclude").asBoolean());
            assertTrue(json.get("jvmHeap") == null || json.get("jvmHeap").isNull());
        }
    }

    /**
     * 접속할 수 없는 대상 URL(포트 1)과 짧은 타임아웃을 설정한 상태로 onTrigger를 호출했을 때,
     * 연결 실패가 예외로 전파되지 않고 내부적으로 로깅 처리되어 정상 종료되는지 확인한다.
     */
    @Test
    public void testDoesNotThrowWhenServerUnreachable() throws Exception {
        final HttpNotificationReportingTask task = initializedTask();

        final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
        configuredProperties.put(HttpNotificationReportingTask.PROP_URL, "http://127.0.0.1:1/notify");
        configuredProperties.put(HttpNotificationReportingTask.PROP_CONNECTION_TIMEOUT, "500 millis");
        configuredProperties.put(HttpNotificationReportingTask.PROP_WRITE_TIMEOUT, "500 millis");
        task.onScheduled(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

        final MockReportingContext context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
        context.setProperty(HttpNotificationReportingTask.PROP_URL.getName(), "http://127.0.0.1:1/notify");

        // 실패시에도 예외를 던지지 않고 로그로 처리해야 한다.
        task.onTrigger(context);
    }
}
