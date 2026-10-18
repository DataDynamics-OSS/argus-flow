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
import org.apache.nifi.components.Validator;
import org.junit.jupiter.api.Test;

import java.lang.management.MemoryUsage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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

    // ------------------------------------------------------------------
    // 보강 테스트
    // ------------------------------------------------------------------

    private static final Validator THRESHOLD_VALIDATOR = MonitorMemoryUsageReportingTask.THRESHOLD_PROPERTY.getValidators().get(0);

    private static boolean isValidThreshold(final String input) {
        return THRESHOLD_VALIDATOR.validate("메모리 사용율", input, null).isValid();
    }

    private static final class Fixture {
        final MonitorMemoryUsageReportingTask task = new MonitorMemoryUsageReportingTask();
        final MockComponentLog log = new MockComponentLog("id", task);
        final MockReportingContext context;

        Fixture(final String threshold, final String reportingInterval, final String url, final boolean httpEnabled) throws Exception {
            task.initialize(new MockReportingInitializationContext("id", "MonitorMemoryUsage", log));
            final Map<PropertyDescriptor, String> configuredProperties = new HashMap<>();
            configuredProperties.put(MonitorMemoryUsageReportingTask.THRESHOLD_PROPERTY, threshold);
            if (reportingInterval != null) {
                configuredProperties.put(MonitorMemoryUsageReportingTask.REPORTING_INTERVAL, reportingInterval);
            }
            task.onConfigured(new MockConfigurationContext(configuredProperties, null, Collections.emptyMap()));

            context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
            context.setProperty(MonitorMemoryUsageReportingTask.THRESHOLD_PROPERTY.getName(), threshold);
            if (reportingInterval != null) {
                context.setProperty(MonitorMemoryUsageReportingTask.REPORTING_INTERVAL.getName(), reportingInterval);
            }
            if (url != null) {
                context.setProperty(MonitorMemoryUsageReportingTask.EXTERNAL_HTTP_URL.getName(), url);
            }
            context.setProperty(MonitorMemoryUsageReportingTask.EXTERNAL_HTTP_URL_ENABLE.getName(), String.valueOf(httpEnabled));
        }
    }

    /**
     * 백분율 경계값: 0%~99%(1~2자리)만 허용하고 100%, 음수, 공백 포함, 소수점 백분율은 거부한다.
     */
    @Test
    public void testThresholdValidatorPercentageBoundaries() {
        assertTrue(isValidThreshold("0%"));
        assertTrue(isValidThreshold("1%"));
        assertTrue(isValidThreshold("99%"));
        assertFalse(isValidThreshold("100%"));
        assertFalse(isValidThreshold("-1%"));
        assertFalse(isValidThreshold("65 %"));
        assertFalse(isValidThreshold("65.5%"));
        assertFalse(isValidThreshold("%"));
        assertFalse(isValidThreshold(""));
    }

    /**
     * 데이터 크기 표기: 단위가 필수이고 소수를 허용하며, 단위는 대문자(B/KB/MB/GB/TB)만 허용된다.
     */
    @Test
    public void testThresholdValidatorDataSizeForms() {
        assertTrue(isValidThreshold("1 B"));
        assertTrue(isValidThreshold("1.5 GB"));
        assertTrue(isValidThreshold("512MB"));
        assertFalse(isValidThreshold("100 mb")); // DataUnit.DATA_SIZE_PATTERN은 대소문자를 구분한다
        assertFalse(isValidThreshold("100"));
        assertFalse(isValidThreshold("-1 MB"));
        assertFalse(isValidThreshold("1 XB"));
    }

    /**
     * 임계치를 현실적으로 도달 불가능한 데이터 크기로 설정하면 반복 트리거해도 알림이 없다.
     */
    @Test
    public void testNoNotificationWhenBelowDataSizeThreshold() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1000 TB", "10 millis", httpServer.getUrl(), true);
            for (int i = 0; i < 3; i++) {
                f.task.onTrigger(f.context);
            }
            assertEquals(0, httpServer.getRequests().size());
            assertTrue(f.log.getInfoMessages().isEmpty());
        }
    }

    /**
     * "0%" 경계값은 calculatedThreshold=0 → 사용량이 0보다 크면 항상 초과로 판정된다.
     */
    @Test
    public void testZeroPercentAlwaysExceeds() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("0%", "10 millis", httpServer.getUrl(), true);
            f.task.onTrigger(f.context);
            assertEquals(1, httpServer.getRequests().size());
            final String body = httpServer.getRequests().poll().body();
            assertTrue(body.contains("\"threshold\":\"0%\""), body);
            assertTrue(body.contains("\"calculatedThreshold\":0"), body);
        }
    }

    /**
     * 초과 상태라도 HTTP 통보 여부가 false면 외부 호출을 하지 않는다.
     */
    @Test
    public void testNoHttpCallWhenNotificationDisabled() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1 B", "10 millis", httpServer.getUrl(), false);
            f.task.onTrigger(f.context);
            assertEquals(0, httpServer.getRequests().size());
        }
    }

    /**
     * 통보 대상 서버에 연결할 수 없어도 onTrigger는 예외를 던지지 않고 경고 로그만 남긴다.
     */
    @Test
    public void testUnreachableEndpointLogsWarningWithoutThrowing() throws Exception {
        final String deadUrl;
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            deadUrl = httpServer.getUrl();
        }
        final Fixture f = new Fixture("1 B", "10 millis", deadUrl, true);
        assertDoesNotThrow(() -> f.task.onTrigger(f.context));
        // 임계치 초과 WARN 1건 + HTTP 호출 실패 WARN 1건
        assertEquals(2, f.log.getWarnMessages().size());
        assertTrue(f.log.getWarnMessages().stream().anyMatch(m -> m.getMsg().contains("External HTTP Service 호출에 실패")));
    }

    /**
     * 리포팅 간격 미지정 시 스케줄링 주기를 기본값으로 사용하며 onConfigured가 실패하지 않는다.
     */
    @Test
    public void testOnConfiguredWithoutReportingInterval() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1 B", null, httpServer.getUrl(), true);
            f.task.onTrigger(f.context);
            assertEquals(1, httpServer.getRequests().size());
        }
    }

    /**
     * 리포팅 간격(1시간) 내 반복 트리거 시 알림은 1회여야 한다 (R-1 회귀 테스트).
     */
    @Test
    public void testReportingIntervalSuppressesRepeatedNotifications() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1 B", "1 hour", httpServer.getUrl(), true);
            for (int i = 0; i < 3; i++) {
                f.task.onTrigger(f.context);
            }
            assertEquals(1, httpServer.getRequests().size());
            assertEquals(1, f.log.getWarnMessages().size());
        }
    }

    /**
     * 리포팅 간격이 지나면 다시 통보한다.
     */
    @Test
    public void testNotifiesAgainAfterIntervalElapsed() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("1 B", "20 millis", httpServer.getUrl(), true);
            f.task.onTrigger(f.context);
            Thread.sleep(60L);
            f.task.onTrigger(f.context);
            assertEquals(2, httpServer.getRequests().size());
        }
    }

    /**
     * 초과 상태에서 임계치 아래로 내려오면 복귀 메시지를 INFO로 한 번 남기고, 이후에는 추가 로그가 없다.
     */
    @Test
    public void testRecoveryMessageWhenUsageDropsBelowThreshold() throws Exception {
        final long mb = 1024L * 1024L;
        final AtomicReference<MemoryUsage> usage = new AtomicReference<>(new MemoryUsage(64 * mb, 900 * mb, 1000 * mb, 1000 * mb));
        final Fixture f = new Fixture("50%", "10 millis", null, false);
        f.task.setHeapMemoryUsageSupplier(usage::get);

        f.task.onTrigger(f.context);
        assertEquals(1, f.log.getWarnMessages().size());
        assertTrue(f.log.getWarnMessages().get(0).getMsg().contains("exceeded"));

        usage.set(new MemoryUsage(64 * mb, 100 * mb, 1000 * mb, 1000 * mb));
        final int infoBefore = f.log.getInfoMessages().size();
        f.task.onTrigger(f.context);
        f.task.onTrigger(f.context);
        assertEquals(1, f.log.getWarnMessages().size());
        assertEquals(infoBefore + 1, f.log.getInfoMessages().size());
        assertTrue(f.log.getInfoMessages().get(infoBefore).getMsg().contains("no longer exceeding"));
    }

    /**
     * 임계치 초과는 WARN 이상으로 기록되어야 Reporting Task Bulletin(기본 WARN)이 생성된다 (R-2 회귀 테스트).
     */
    @Test
    public void testThresholdExceededIsLoggedAtWarnLevel() throws Exception {
        final Fixture f = new Fixture("1 B", "10 millis", null, false);
        f.task.onTrigger(f.context);
        assertFalse(f.log.getWarnMessages().isEmpty());
    }

    /**
     * 백분율 임계치는 최대 힙(max) 대비여야 한다(MonitorMemoryPoolReportingTask와 동일 기준, R-3 회귀 테스트).
     * committed 대비로는 초과(60/64 = 93%)지만 max 대비로는 여유(60/1024 = 6%)인 경우 알림이 없어야 한다.
     */
    @Test
    public void testPercentageThresholdIsRelativeToMaxHeap() throws Exception {
        final long mb = 1024L * 1024L;
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("65%", "10 millis", httpServer.getUrl(), true);
            f.task.setHeapMemoryUsageSupplier(() -> new MemoryUsage(64 * mb, 60 * mb, 64 * mb, 1024 * mb));
            f.task.onTrigger(f.context);
            assertEquals(0, httpServer.getRequests().size());
            assertTrue(f.log.getWarnMessages().isEmpty());

            // max 대비로도 임계치를 넘으면 알림이 발생하며 calculatedThreshold는 max 기준이다.
            f.task.setHeapMemoryUsageSupplier(() -> new MemoryUsage(64 * mb, 700 * mb, 800 * mb, 1000 * mb));
            f.task.onTrigger(f.context);
            assertEquals(1, httpServer.getRequests().size());
            final String body = httpServer.getRequests().poll().body();
            assertTrue(body.contains("\"calculatedThreshold\":" + (long) (1000 * mb * 0.65D)), body);
        }
    }

    /**
     * 최대 힙 크기가 정의되지 않은 경우(max == -1)에는 committed 대비로 계산한다.
     */
    @Test
    public void testPercentageThresholdFallsBackToCommittedWhenMaxUndefined() throws Exception {
        final long mb = 1024L * 1024L;
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final Fixture f = new Fixture("65%", "10 millis", httpServer.getUrl(), true);
            f.task.setHeapMemoryUsageSupplier(() -> new MemoryUsage(64 * mb, 60 * mb, 64 * mb, -1L));
            f.task.onTrigger(f.context);
            assertEquals(1, httpServer.getRequests().size());
            final String body = httpServer.getRequests().poll().body();
            assertTrue(body.contains("\"calculatedThreshold\":" + (long) (64 * mb * 0.65D)), body);
        }
    }
}
