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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datadynamics.nifi.reporting.HttpNotifyUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JVM의 현재 활성 Thread 개수를 감시하는 Reporting Task.
 * {@link ThreadMXBean#getThreadCount()} 값이 설정된 임계값을 초과하면 로그 경고를 남기고,
 * 필요 시 외부 HTTP 서비스로 Thread 관련 상세 정보를 JSON payload로 전송한다.
 */
@Tags({"dd", "custom", "monitor", "thread", "jvm", "warning"})
@CapabilityDescription("JVM의 Thread 수를 확인합니다. Thread 수가 구성 가능한 일부 임계값을 초과하는 경우 로그 메시지 및 시스템 수준 게시판을 통해 쓰레스 수가 임계값을 초과한다고 경고합니다.")
public class MonitorThreadReportingTask extends AbstractReportingTask {

    public static final PropertyDescriptor THRESHOLD_PROPERTY = new PropertyDescriptor.Builder()
            .name("쓰레드 수 임계값")
            .displayName("쓰레드 수 임계값")
            .description("경고를 생성하는 임계값을 나타냅니다. 백분율 또는 데이터 크기일 수 있습니다.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("300")
            .build();
    public static final PropertyDescriptor REPORTING_INTERVAL = new PropertyDescriptor.Builder()
            .name("리포팅 간격")
            .displayName("리포팅 간격")
            .description("설정한 쓰레드 최대 개수의 임계값을 초과하는 경우 Bulletin에 레포팅하는 간격을 설정합니다. (예; 2000 nanos, 2000 millis, 20 secs, 5 mins, 1 hrs, 1 days)")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor EXTERNAL_HTTP_URL = new PropertyDescriptor.Builder()
            .name("외부에 통보할 HTTP URL")
            .description("외부 서비스에 HTTP URL을 호출하여 정보를 전달합니다.")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    public static final PropertyDescriptor EXTERNAL_HTTP_URL_ENABLE = new PropertyDescriptor.Builder()
            .name("HTTP URL 통보 여부")
            .description("외부 서비스에 HTTP URL 통보 여부입니다.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .dependsOn(EXTERNAL_HTTP_URL)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor HTTP_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("HTTP Connection 타임아웃")
            .description("원격 서비스 연결을 위한 최대 대기 시간입니다.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    public static final PropertyDescriptor HTTP_WRITE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("HTTP Write 타임아웃")
            .description("원격 서비스가 전송한 요청을 읽는 데 걸리는 최대 대기 시간입니다.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10s")
            .build();
    private final static List<PropertyDescriptor> propertyDescriptors;
    // 외부 HTTP 서비스로 전송할 알림 payload를 JSON으로 직렬화하는 데 사용한다.
    public static final ObjectMapper mapper = new ObjectMapper();

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(THRESHOLD_PROPERTY);
        _propertyDescriptors.add(REPORTING_INTERVAL);
        _propertyDescriptors.add(EXTERNAL_HTTP_URL_ENABLE);
        _propertyDescriptors.add(EXTERNAL_HTTP_URL);
        _propertyDescriptors.add(HTTP_CONNECTION_TIMEOUT);
        _propertyDescriptors.add(HTTP_WRITE_TIMEOUT);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
    }

    // onConfigured 시점에 생성되어 이후 onTrigger에서 재사용되는 HttpClient. 스레드 안전을 위해 AtomicReference로 보관한다.
    private final AtomicReference<HttpClient> httpClientReference = new AtomicReference<>();
    // 외부 HTTP 서비스 호출 시 응답을 기다리는 최대 시간(밀리초).
    private volatile long requestTimeoutMillis = 10_000L;
    // 마지막으로 임계값 초과 상태가 리포팅된 시각(밀리초). 리포팅 간격 제어에 사용된다.
    private volatile long lastReportTime;
    // 임계값 초과 상태가 반복적으로 리포팅되지 않도록 제한하는 최소 간격(밀리초).
    private volatile long reportingIntervalMillis;
    // 직전 확인 시점에 임계값을 초과한 상태였는지 여부.
    private volatile boolean lastValueWasExceeded;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    // Reporting Task 스케줄링 시점에 리포팅 간격 프로퍼티를 읽어들이고, 외부 알림 전송용 HttpClient를 초기화한다.
    @OnScheduled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        /////////////////////////////////////////
        // 리포팅 간격 계산
        /////////////////////////////////////////

        final Long reportingIntervalValue = context.getProperty(REPORTING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        if (reportingIntervalValue == null) {
            reportingIntervalMillis = context.getSchedulingPeriod(TimeUnit.MILLISECONDS);
        } else {
            reportingIntervalMillis = reportingIntervalValue;
        }

        /////////////////////////////////////////
        // 외부 HTTP 서비스용 HttpClient 초기화
        /////////////////////////////////////////

        httpClientReference.set(null);

        final long connectTimeout = context.getProperty(HTTP_CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        requestTimeoutMillis = context.getProperty(HTTP_WRITE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        httpClientReference.set(HttpNotifyUtils.createHttpClient(connectTimeout));
    }

    // 매 스케줄 주기마다 실행되어 JVM의 현재 Thread 개수가 설정된 임계값을 초과하는지 확인한다.
    @Override
    public void onTrigger(final ReportingContext context) {

        final boolean isExternalHttpUrlEnable = Boolean.TRUE.equals(context.getProperty(EXTERNAL_HTTP_URL_ENABLE).evaluateAttributeExpressions().asBoolean());
        final String externalHttpUrl = context.getProperty(EXTERNAL_HTTP_URL).evaluateAttributeExpressions().getValue();

        final Integer thresholdValue = context.getProperty(THRESHOLD_PROPERTY).asInteger();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        if (threadMXBean.getThreadCount() > thresholdValue) {

            // Reporting Interval을 확인
            if (System.currentTimeMillis() < reportingIntervalMillis + lastReportTime && lastReportTime > 0L) {
                return;
            }

            // Reporting Interval을 위한 check point
            lastReportTime = System.currentTimeMillis();
            lastValueWasExceeded = true;

            /////////////////////////////////////////
            // Thread 개수 및 상세 정보 수집
            /////////////////////////////////////////

            final Map<String, Object> params = new HashMap<>();
            params.put("hostname", HttpNotifyUtils.getHostname());
            params.put("type", "JVMTheadUsage");
            params.put("threshold", thresholdValue);
            params.put("currentThreadCount", threadMXBean.getThreadCount());
            params.put("totalStartedThreadCount", threadMXBean.getTotalStartedThreadCount());
            params.put("peakThreadCount", threadMXBean.getPeakThreadCount());
            params.put("daemonThreadCount", threadMXBean.getDaemonThreadCount());
            params.put("allThreadIdsCount", threadMXBean.getAllThreadIds().length);
            params.put("currentThreadCpuTime", threadMXBean.getCurrentThreadCpuTime());
            params.put("currentThreadUserTime", threadMXBean.getCurrentThreadUserTime());

            getLogger().info("Thread Reporting Task : {}", params);

            /////////////////////////////////////////
            // 외부 HTTP 서비스로 알림 전송
            /////////////////////////////////////////

            if (isExternalHttpUrlEnable && externalHttpUrl != null) {
                try {
                    final String json = mapper.writeValueAsString(params);
                    final HttpResponse<String> response = HttpNotifyUtils.postJson(httpClientReference.get(), externalHttpUrl, json, requestTimeoutMillis, null);
                    if (!HttpNotifyUtils.isSuccessful(response)) {
                        getLogger().warn("{}", String.format("External HTTP Service 호출에 실패했습니다. URL : %s, Status Code : %s, Response Body : %s", externalHttpUrl, response.statusCode(), response.body()));
                    }
                } catch (Exception e) {
                    getLogger().warn("{}", String.format("External HTTP Service 호출에 실패했습니다. URL : %s", externalHttpUrl), e);
                }
            }
        } else if (lastValueWasExceeded) {
            // Reporting Interval을 위한 초기화
            lastValueWasExceeded = false;
            lastReportTime = System.currentTimeMillis();
        }
    }
}
