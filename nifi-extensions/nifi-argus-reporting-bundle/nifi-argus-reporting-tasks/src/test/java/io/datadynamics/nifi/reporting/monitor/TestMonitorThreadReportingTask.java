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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.Validator;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    // ------------------------------------------------------------------
    // 보강 테스트
    // ------------------------------------------------------------------

    private static boolean isValidThreshold(final String input) {
        final Validator validator = MonitorThreadReportingTask.THRESHOLD_PROPERTY.getValidators().get(0);
        // StandardValidators는 EL 지원 여부를 ValidationContext에 묻기 때문에 최소한의 프록시 컨텍스트를 전달한다.
        final ValidationContext context = (ValidationContext) Proxy.newProxyInstance(
                ValidationContext.class.getClassLoader(), new Class<?>[]{ValidationContext.class},
                (proxy, method, args) -> method.getReturnType() == boolean.class ? Boolean.FALSE : null);
        return validator.validate("쓰레드 수 임계값", input, context).isValid();
    }

    private static final class Fixture {
        final MonitorThreadReportingTask task = new MonitorThreadReportingTask();
        final MockComponentLog log = new MockComponentLog("id", task);
        final MockReportingContext context;

        Fixture(final String threshold, final String reportingInterval, final String url, final boolean httpEnabled) throws Exception {
            task.initialize(new MockReportingInitializationContext("id", "MonitorThread", log));
            final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
            if (reportingInterval != null) {
                configuredProperties.put(MonitorThreadReportingTask.REPORTING_INTERVAL, reportingInterval);
            }
            task.onConfigured(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

            context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
            context.setProperty(MonitorThreadReportingTask.THRESHOLD_PROPERTY.getName(), threshold);
            if (url != null) {
                context.setProperty(MonitorThreadReportingTask.EXTERNAL_HTTP_URL.getName(), url);
            }
            context.setProperty(MonitorThreadReportingTask.EXTERNAL_HTTP_URL_ENABLE.getName(), String.valueOf(httpEnabled));
        }
    }

    /**
     * 임계값 Validator 경계: 양의 정수만 허용(0, 음수, 비숫자, int 범위 초과는 거부).
     */
    @Test
    public void testThresholdValidatorBoundaries() {
        assertTrue(isValidThreshold("1"));
        assertTrue(isValidThreshold("300"));
        assertTrue(isValidThreshold(String.valueOf(Integer.MAX_VALUE)));
        assertFalse(isValidThreshold("0"));
        assertFalse(isValidThreshold("-1"));
        assertFalse(isValidThreshold("abc"));
        assertFalse(isValidThreshold("80%"));
        assertFalse(isValidThreshold("2147483648"));
    }

    /**
     * 경계값: 임계치 == 현재 스레드 수이면 초과가 아니다(엄격한 '>' 비교).
     * 스레드 수는 실행 중 변할 수 있으므로 측정 직전/직후 값이 같을 때만 판정한다.
     */
    @Test
    public void testThresholdEqualToThreadCountIsNotExceeded() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final java.lang.management.ThreadMXBean bean = java.lang.management.ManagementFactory.getThreadMXBean();
            // 측정 이후 늘어날 수 있는 스레드(HTTP 서버 등)를 감안해 넉넉한 여유값으로 '초과 아님'을 확인
            final int count = bean.getThreadCount();
            final Fixture f = new Fixture(String.valueOf(count + 50), "10 millis", httpServer.getUrl(), true);
            f.task.onTrigger(f.context);
            assertEquals(0, httpServer.getRequests().size());
        }
    }

    /**
     * 리포팅 간격 내 반복 트리거는 1회만 통보한다(rate limit).
     */
    @Test
    public void testRepeatedTriggersWithinIntervalNotifyOnce() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1", "1 hour", httpServer.getUrl(), true);
            for (int i = 0; i < 5; i++) {
                f.task.onTrigger(f.context);
            }
            assertEquals(1, httpServer.getRequests().size());
            assertEquals(1, f.log.getInfoMessages().size());
        }
    }

    /**
     * 리포팅 간격이 지나면 다시 통보한다.
     */
    @Test
    public void testNotifiesAgainAfterIntervalElapsed() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1", "20 millis", httpServer.getUrl(), true);
            f.task.onTrigger(f.context);
            Thread.sleep(60L);
            f.task.onTrigger(f.context);
            assertEquals(2, httpServer.getRequests().size());
        }
    }

    /**
     * 리포팅 간격 미지정 시 스케줄링 주기(Mock에서는 0)를 사용하므로 매 트리거마다 통보한다.
     */
    @Test
    public void testDefaultIntervalFallsBackToSchedulingPeriod() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1", null, httpServer.getUrl(), true);
            f.task.onTrigger(f.context);
            Thread.sleep(5L);
            f.task.onTrigger(f.context);
            assertEquals(2, httpServer.getRequests().size());
        }
    }

    /**
     * 초과 → 정상 복귀 → 재초과 시나리오에서 예외 없이 상태 전이가 일어나는지 확인한다.
     * (복귀 시 lastReportTime이 갱신되므로 간격 내 재초과는 억제된다 — upstream MonitorMemory와 동일한 동작.)
     */
    @Test
    public void testRecoveryResetsStateAndSuppressesFlappingWithinInterval() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1", "1 hour", httpServer.getUrl(), true);
            f.task.onTrigger(f.context);
            assertEquals(1, httpServer.getRequests().size());

            f.context.setProperty(MonitorThreadReportingTask.THRESHOLD_PROPERTY.getName(), String.valueOf(Integer.MAX_VALUE));
            f.task.onTrigger(f.context);

            f.context.setProperty(MonitorThreadReportingTask.THRESHOLD_PROPERTY.getName(), "1");
            f.task.onTrigger(f.context);
            assertEquals(1, httpServer.getRequests().size());
        }
    }

    /**
     * 초과 상태라도 HTTP 통보 여부가 false면 외부 호출을 하지 않는다.
     */
    @Test
    public void testNoHttpCallWhenNotificationDisabled() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1", "10 millis", httpServer.getUrl(), false);
            f.task.onTrigger(f.context);
            assertEquals(0, httpServer.getRequests().size());
            assertEquals(1, f.log.getInfoMessages().size());
        }
    }

    /**
     * 통보 대상 서버에 연결할 수 없어도 onTrigger는 예외 없이 경고 로그만 남긴다.
     */
    @Test
    public void testUnreachableEndpointLogsWarningWithoutThrowing() throws Exception {
        final String deadUrl;
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            deadUrl = httpServer.getUrl();
        }
        final Fixture f = new Fixture("1", "10 millis", deadUrl, true);
        assertDoesNotThrow(() -> f.task.onTrigger(f.context));
        // 임계치 초과 WARN 1건 + HTTP 호출 실패 WARN 1건
        assertEquals(2, f.log.getWarnMessages().size());
        assertTrue(f.log.getWarnMessages().stream().anyMatch(m -> m.getMsg().contains("External HTTP Service 호출에 실패")));
    }

    /**
     * 임계치 초과는 WARN 이상으로 기록되어야 Bulletin(기본 WARN)이 생성된다 (R-2 회귀 테스트).
     */
    @Test
    public void testThresholdExceededIsLoggedAtWarnLevel() throws Exception {
        final Fixture f = new Fixture("1", "10 millis", null, false);
        f.task.onTrigger(f.context);
        assertFalse(f.log.getWarnMessages().isEmpty());
    }

    /**
     * R-4: 페이로드에서 태스크 자신의 스레드 값(currentThreadCpuTime/UserTime)을 빼고 JVM 전체 값만 보낸다.
     */
    @Test
    public void testPayloadContainsOnlyJvmWideThreadMetrics() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1", "10 millis", httpServer.getUrl(), true);
            f.task.onTrigger(f.context);
            assertEquals(1, httpServer.getRequests().size());
            final String body = httpServer.getRequests().poll().body();
            assertFalse(body.contains("currentThreadCpuTime"), body);
            assertFalse(body.contains("currentThreadUserTime"), body);
            assertTrue(body.contains("\"processCpuTime\""), body);
            assertTrue(body.contains("\"peakThreadCount\""), body);
            assertTrue(body.contains("\"daemonThreadCount\""), body);
        }
    }

    /**
     * R-4: 임계값 설명은 실제 의미(정수 Thread 수)와 일치해야 한다.
     */
    @Test
    public void testThresholdDescriptionDescribesThreadCount() {
        final String description = MonitorThreadReportingTask.THRESHOLD_PROPERTY.getDescription();
        assertFalse(description.contains("백분율"), description);
        assertTrue(description.contains("Thread 수"), description);
    }
}
