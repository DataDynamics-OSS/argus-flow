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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datadynamics.nifi.reporting.HttpNotifyUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * NiFi 1.x의 bootstrap notification 확장점(AbstractNotificationService) 기반이었던
 * CustomNiFiNotificationService를 NiFi 2.x에서는 해당 확장점이 제거되어 ReportingTask로 재구현한 클래스.
 * 설정된 URL로 알림 유형/제목/본문과 호스트명 및 JVM 상태(Heap/Thread)를 JSON payload로 POST 전송한다.
 */
@Tags({"dd", "custom", "notify", "notification", "http", "alert", "jvm"})
@CapabilityDescription("설정된 URL로 알림(알림 유형/제목/본문)과 호스트명 및 JVM 상태(Heap, Thread)를 JSON payload로 HTTP POST 전송하는 리포팅 태스크입니다. "
        + "NiFi 2.x에서 제거된 bootstrap notification 확장점 기반의 CustomNiFiNotificationService를 대체합니다. "
        + "동적 프로퍼티를 추가하면 JSON payload의 properties 항목에 포함되어 전송됩니다.")
public class HttpNotificationReportingTask extends AbstractReportingTask {

    // HTTP 요청 헤더에 알림 유형을 실어 보낼 때 사용하는 헤더 이름.
    public static final String NOTIFICATION_TYPE_KEY = "notification.type";
    // HTTP 요청 헤더에 알림 제목을 실어 보낼 때 사용하는 헤더 이름.
    public static final String NOTIFICATION_SUBJECT_KEY = "notification.subject";

    public static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("URL")
            .description("알람을 전송할 URL")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    public static final PropertyDescriptor PROP_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("커넥션 타임아웃")
            .description("원격 서비스에 접속을 위한 대기시간")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    public static final PropertyDescriptor PROP_WRITE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("응답 대기시간")
            .description("원격 서비스에 전송한 요청의 응답을 대기하는 시간")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    public static final PropertyDescriptor PROP_NOTIFICATION_TYPE = new PropertyDescriptor.Builder()
            .name("알림 유형")
            .description("전송할 알림의 유형입니다. (예; NIFI_STARTED, NIFI_STOPPED, NIFI_DIED, HEALTH_CHECK 등)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("HEALTH_CHECK")
            .build();
    public static final PropertyDescriptor PROP_SUBJECT = new PropertyDescriptor.Builder()
            .name("제목")
            .description("전송할 알림의 제목입니다.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("NiFi Notification")
            .build();
    public static final PropertyDescriptor PROP_MESSAGE = new PropertyDescriptor.Builder()
            .name("본문")
            .description("전송할 알림의 본문 메시지입니다.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .build();
    public static final PropertyDescriptor PROP_JVM_METRICS = new PropertyDescriptor.Builder()
            .name("JVM Heap & Thread 정보 수집")
            .description("JVM Heap Size 및 Thread 개수의 수집 여부")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    private static final List<PropertyDescriptor> supportedProperties;
    // 알림 payload를 JSON으로 직렬화하는 데 사용한다.
    public static final ObjectMapper mapper = new ObjectMapper();

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROP_URL);
        properties.add(PROP_CONNECTION_TIMEOUT);
        properties.add(PROP_WRITE_TIMEOUT);
        properties.add(PROP_NOTIFICATION_TYPE);
        properties.add(PROP_SUBJECT);
        properties.add(PROP_MESSAGE);
        properties.add(PROP_JVM_METRICS);
        supportedProperties = Collections.unmodifiableList(properties);
    }

    // onScheduled 시점에 생성되어 이후 onTrigger에서 재사용되는 HttpClient. 스레드 안전을 위해 AtomicReference로 보관한다.
    private final AtomicReference<HttpClient> httpClientReference = new AtomicReference<>();
    // 외부 HTTP 서비스 호출 시 응답을 기다리는 최대 시간(밀리초).
    private volatile long requestTimeoutMillis = 10_000L;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedProperties;
    }

    // 사전에 정의되지 않은 프로퍼티 이름이 들어오면 동적 프로퍼티로 허용한다.
    // 동적 프로퍼티는 알림 payload의 properties 항목에 포함되어 함께 전송된다.
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .build();
    }

    // Reporting Task가 스케줄링될 때 커넥션/응답 타임아웃 프로퍼티를 읽어 HttpClient를 (재)생성한다.
    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        httpClientReference.set(null);

        final long connectTimeout = context.getProperty(PROP_CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        requestTimeoutMillis = context.getProperty(PROP_WRITE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        httpClientReference.set(HttpNotifyUtils.createHttpClient(connectTimeout));
    }

    /**
     * 매 스케줄 주기마다 실행되어 설정된 알림 유형/제목/본문, 호스트명, 동적 프로퍼티,
     * (설정 시) JVM Heap/Thread 상태를 JSON payload로 구성하여 지정된 URL에 HTTP POST로 전송한다.
     * 전송 실패 시 예외를 삼키고 에러 로그만 남긴다(Reporting Task 자체가 중단되지 않도록 함).
     */
    @Override
    public void onTrigger(final ReportingContext context) {
        final String url = context.getProperty(PROP_URL).evaluateAttributeExpressions().getValue();
        final String notificationType = context.getProperty(PROP_NOTIFICATION_TYPE).evaluateAttributeExpressions().getValue();
        final String subject = context.getProperty(PROP_SUBJECT).evaluateAttributeExpressions().getValue();
        final String message = context.getProperty(PROP_MESSAGE).evaluateAttributeExpressions().getValue();
        final boolean isCollectJVMMetrics = Boolean.TRUE.equals(context.getProperty(PROP_JVM_METRICS).evaluateAttributeExpressions().asBoolean());

        try {
            final Map<String, Object> params = new HashMap<>();
            params.put("hostname", HttpNotifyUtils.getHostname());
            params.put("type", notificationType);
            params.put("subject", subject);
            params.put("message", message);
            params.put("properties", getDynamicProperties(context));
            if (isCollectJVMMetrics) {
                params.put("jvmHeap", getJvmHeap());
                params.put("jvmThread", getThreadCount());
                params.put("jvmMetricsInclude", true);
            } else {
                params.put("jvmMetricsInclude", false);
            }

            final String json = mapper.writeValueAsString(params);
            getLogger().info("{}", String.format("Type : %s, Subject : %s, Message : %s, Detail : \n%s", notificationType, subject, message, json));

            final Map<String, String> headers = new HashMap<>();
            headers.put(NOTIFICATION_SUBJECT_KEY, encodeHeaderValue(subject));
            headers.put(NOTIFICATION_TYPE_KEY, encodeHeaderValue(notificationType));

            final HttpResponse<String> response = HttpNotifyUtils.postJson(httpClientReference.get(), url, json, requestTimeoutMillis, headers);
            if (!HttpNotifyUtils.isSuccessful(response)) {
                getLogger().warn("Failed to send HTTP Notification. Received an unsuccessful status code response '{}'. The message was '{}'", response.statusCode(), response.body());
            }
        } catch (Exception e) {
            getLogger().error("Failed to send Http Notification. URL : {}", url, e);
        }
    }

    /**
     * JDK HttpClient는 non-ASCII 헤더 값을 허용하지 않으므로(한국어 제목 등),
     * 출력 가능한 ASCII 범위를 벗어나는 값은 UTF-8 기반 URL 인코딩하여 전송한다.
     */
    static String encodeHeaderValue(final String value) {
        if (value == null) {
            return "";
        }
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if (c < 0x20 || c > 0x7E) {
                return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        return value;
    }

    // 사용자가 설정한 동적 프로퍼티 목록을 이름-값(Expression Language 평가 후) 형태의 Map으로 변환한다.
    // 이 Map은 알림 payload의 properties 항목으로 포함된다.
    Map<String, String> getDynamicProperties(final ReportingContext context) {
        final Map<String, String> params = new HashMap<>();
        final Map<PropertyDescriptor, String> configuredProperties = context.getProperties();
        for (PropertyDescriptor propertyDescriptor : configuredProperties.keySet()) {
            if (propertyDescriptor.isDynamic()) {
                final String propertyValue = context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue();
                params.put(propertyDescriptor.getDisplayName(), propertyValue);
            }
        }
        return params;
    }

    // 현재 JVM의 Heap 메모리 사용 현황(commited/used/max/init)을 수집하여 Map으로 반환한다.
    public static Map<String, Object> getJvmHeap() {
        final Map<String, Object> params = new HashMap<>();

        final MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        final MemoryUsage heapMemoryUsage = memBean.getHeapMemoryUsage();

        params.put("committedMemory", heapMemoryUsage.getCommitted());
        params.put("usedMemory", heapMemoryUsage.getUsed());
        params.put("maxMemory", heapMemoryUsage.getMax());
        params.put("initMemory", heapMemoryUsage.getInit());

        return params;
    }

    // 현재 JVM의 Thread 관련 통계(활성/피크/데몬 개수, CPU 사용 시간, ThreadGroup별 개수 등)를 수집하여 Map으로 반환한다.
    public static Map<String, Object> getThreadCount() {
        final Map<String, Object> params = new HashMap<>();
        params.put("activeThread", Thread.activeCount());
        params.put("peakThreadCount", ManagementFactory.getThreadMXBean().getPeakThreadCount());
        params.put("daemonThreadCount", ManagementFactory.getThreadMXBean().getDaemonThreadCount());
        params.put("currentThreadUserTime", ManagementFactory.getThreadMXBean().getCurrentThreadUserTime());
        params.put("currentThreadCpuTime", ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime());
        params.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());

        final Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
        final Set<Thread> threadSet = threadMap.keySet();
        final Map<String, AtomicInteger> threadGroupCountMap = new HashMap<>();
        for (Thread t : threadSet) {
            final ThreadGroup threadGroup = t.getThreadGroup();
            if (threadGroup == null) {
                continue;
            }
            threadGroupCountMap.computeIfAbsent(threadGroup.getName(), k -> new AtomicInteger(0)).incrementAndGet();
        }

        final Map<String, Integer> threadGroupActiveThreadCountMap = new HashMap<>();
        for (Map.Entry<String, AtomicInteger> entry : threadGroupCountMap.entrySet()) {
            threadGroupActiveThreadCountMap.put(entry.getKey(), entry.getValue().get());
        }

        params.put("activeThreadCountOfThreadGroup", threadGroupActiveThreadCountMap);
        return params;
    }
}
