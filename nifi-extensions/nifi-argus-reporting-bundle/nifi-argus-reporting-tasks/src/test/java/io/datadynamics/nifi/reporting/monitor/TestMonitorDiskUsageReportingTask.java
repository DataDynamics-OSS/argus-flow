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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockReportingContext;
import org.apache.nifi.util.MockReportingInitializationContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MonitorDiskUsageReportingTask}에 대한 테스트.
 * 디스크 사용률이 임계치를 초과했을 때 로그 경고와 HTTP 알림이 올바르게 발생하는지,
 * 그리고 실제 ReportingContext를 통한 onTrigger 흐름이 예외 없이 동작하는지를 검증한다.
 */
public class TestMonitorDiskUsageReportingTask {

    /**
     * 임계치를 0%로 지정하여 항상 초과되도록 만든 뒤 checkThreshold를 직접 호출했을 때,
     * ComponentLog.warn(...)이 정확히 한 번 호출되는지 확인한다.
     */
    @Test
    public void testGeneratesMessageIfTooFull() {
        final AtomicInteger callCounter = new AtomicInteger(0);

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        Mockito.doAnswer(invocation -> {
            callCounter.incrementAndGet();
            return null;
        }).when(logger).warn(Mockito.anyString());

        final MonitorDiskUsageReportingTask task = initializedTask();
        task.checkThreshold("Test Path", Paths.get("."), 0, logger, false, null);
        assertEquals(1, callCounter.get());
    }

    /**
     * HTTP 알림 전송 옵션을 활성화한 상태에서 checkThreshold를 호출하면,
     * 임베디드 테스트 서버로 실제 HTTP 요청이 전송되고 그 body에 디스크 사용량 타입과
     * 경로 이름이 올바르게 포함되는지 검증한다.
     */
    @Test
    public void testSendsHttpNotificationWhenEnabled() throws Exception {
        try (TestHttpServerSupport httpServer = new TestHttpServerSupport()) {
            final MonitorDiskUsageReportingTask task = initializedTask();
            task.onScheduled(new MockConfigurationContext(Collections.emptyMap(), null, Collections.emptyMap()));

            final ComponentLog logger = Mockito.mock(ComponentLog.class);
            task.checkThreshold("Test Path", Paths.get("."), 0, logger, true, httpServer.getUrl());

            assertEquals(1, httpServer.getRequests().size());
            final String body = httpServer.getRequests().poll().body();
            assertTrue(body.contains("\"type\":\"DiskUsage\""));
            assertTrue(body.contains("\"pathName\":\"Test Path\""));
        }
    }

    /**
     * MockReportingContext에 임계치(0%), 대상 경로, 표시 이름 등의 프로퍼티를 설정하고
     * HTTP 알림은 비활성화한 상태로 onTrigger를 호출했을 때 예외 없이 정상 동작하는지 확인한다.
     */
    @Test
    public void testOnTriggerWithMockReportingContext() throws Exception {
        final MonitorDiskUsageReportingTask task = initializedTask();
        task.onScheduled(new MockConfigurationContext(Collections.emptyMap(), null, Collections.emptyMap()));

        final MockReportingContext context = new MockReportingContext(Collections.emptyMap(), new MockStateManager(task));
        context.setProperty(MonitorDiskUsageReportingTask.DIR_THRESHOLD.getName(), "0%");
        context.setProperty(MonitorDiskUsageReportingTask.DIR_LOCATION.getName(), ".");
        context.setProperty(MonitorDiskUsageReportingTask.DIR_DISPLAY_NAME.getName(), "CWD");
        context.setProperty(MonitorDiskUsageReportingTask.EXTERNAL_HTTP_URL_ENABLE.getName(), "false");

        task.onTrigger(context);
    }

    // 테스트마다 초기화된 MonitorDiskUsageReportingTask 인스턴스를 생성하는 헬퍼 메서드.
    private MonitorDiskUsageReportingTask initializedTask() {
        final MonitorDiskUsageReportingTask task = new MonitorDiskUsageReportingTask();
        try {
            task.initialize(new MockReportingInitializationContext("id", "MonitorDiskUsage", new MockComponentLog("id", task)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return task;
    }
}
