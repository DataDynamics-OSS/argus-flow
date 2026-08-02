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
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * JVM 전체 Heap 메모리 사용량을 감시하는 Reporting Task.
 * {@link MemoryMXBean}에서 얻은 Heap 사용량이 설정된 임계값(백분율 또는 데이터 크기)을 초과하면
 * 로그 경고를 남기고, 필요 시 외부 HTTP 서비스로 알림을 전송한다.
 * MonitorMemoryPoolReportingTask와 달리 특정 메모리 풀이 아닌 Heap 전체를 대상으로 한다.
 */
@Tags({"dd", "custom", "monitor", "memory", "usage", "heap", "jvm", "gc", "garbage collection", "warning"})
@CapabilityDescription("JVM에서 사용 가능한 Java 힙의 양을 확인합니다. 사용된 공간의 양이 구성 가능한 일부 임계값을 초과하는 경우 로그 메시지 및 시스템 수준 게시판을 통해 메모리 풀이 이 임계값을 초과한다고 경고합니다.")
public class MonitorMemoryUsageReportingTask extends AbstractReportingTask {

    // 임계값이 백분율(예; 65%) 형식인지 확인하는 정규식.
    public static final Pattern PERCENTAGE_PATTERN = Pattern.compile("\\d{1,2}%");

    // 임계값이 데이터 크기(예; 100 MB) 형식인지 확인하는 정규식.
    public static final Pattern DATA_SIZE_PATTERN = DataUnit.DATA_SIZE_PATTERN;
    public static final PropertyDescriptor THRESHOLD_PROPERTY = new PropertyDescriptor.Builder()
            .name("메모리 사용율")
            .displayName("메모리 사용율")
            .description("경고를 생성하는 임계값을 나타냅니다. 백분율 또는 데이터 크기일 수 있습니다.")
            .required(true)
            .addValidator(new ThresholdValidator())
            .defaultValue("65%")
            .build();
    public static final PropertyDescriptor REPORTING_INTERVAL = new PropertyDescriptor.Builder()
            .name("리포팅 간격")
            .displayName("리포팅 간격")
            .description("설정한 메모리 사용율 임계값을 초과하는 경우 Bulletin에 레포팅하는 간격을 설정합니다. (예; 2000 nanos, 2000 millis, 20 secs, 5 mins, 1 hrs, 1 days)")
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

    // 사용자가 설정한 원본 임계값 문자열(백분율 또는 데이터 크기 표기).
    private volatile String threshold = "80%";

    // threshold 문자열을 실제 바이트 수로 환산한 값. 초과 여부 판정에 사용된다.
    private volatile long calculatedThreshold;

    // 마지막으로 임계값 초과 상태가 감지된 시각(밀리초). 리포팅 간격 제어에 사용된다.
    private volatile long lastReportTime;

    // 임계값 초과 상태가 반복적으로 리포팅되지 않도록 제한하는 최소 간격(밀리초).
    private volatile long reportingIntervalMillis;

    // 직전 확인 시점에 임계값을 초과한 상태였는지 여부.
    private volatile boolean lastValueWasExceeded;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    // Reporting Task 스케줄링 시점에 임계값/리포팅 간격 프로퍼티를 읽어들이고, 외부 알림 전송용 HttpClient를 초기화한다.
    @OnScheduled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final String thresholdValue = context.getProperty(THRESHOLD_PROPERTY).getValue().trim();
        threshold = thresholdValue;

        final Long reportingIntervalValue = context.getProperty(REPORTING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        if (reportingIntervalValue == null) {
            reportingIntervalMillis = context.getSchedulingPeriod(TimeUnit.MILLISECONDS);
        } else {
            reportingIntervalMillis = reportingIntervalValue;
        }

        /////////////////////////////////////////
        // External HTTP Service
        /////////////////////////////////////////

        httpClientReference.set(null);

        final long connectTimeout = context.getProperty(HTTP_CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        requestTimeoutMillis = context.getProperty(HTTP_WRITE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        httpClientReference.set(HttpNotifyUtils.createHttpClient(connectTimeout));
    }

    // 매 스케줄 주기마다 실행되어 JVM Heap 사용량이 설정된 임계값을 초과하는지 확인한다.
    @Override
    public void onTrigger(final ReportingContext context) {
        final boolean isExternalHttpUrlEnable = Boolean.TRUE.equals(context.getProperty(EXTERNAL_HTTP_URL_ENABLE).evaluateAttributeExpressions().asBoolean());
        final String externalHttpUrl = context.getProperty(EXTERNAL_HTTP_URL).evaluateAttributeExpressions().getValue();

        final String thresholdValue = context.getProperty(THRESHOLD_PROPERTY).getValue().trim();
        threshold = thresholdValue;

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();

        if (DATA_SIZE_PATTERN.matcher(thresholdValue).matches()) {
            calculatedThreshold = DataUnit.parseDataSize(thresholdValue, DataUnit.B).longValue();
        } else {
            final String percentage = thresholdValue.substring(0, thresholdValue.length() - 1);
            final double pct = Double.parseDouble(percentage) / 100D;
            calculatedThreshold = (long) (heapMemoryUsage.getCommitted() * pct);
        }

        if (heapMemoryUsage.getUsed() > calculatedThreshold) {
            lastReportTime = System.currentTimeMillis();
            lastValueWasExceeded = true;

            /////////////////////////////////////////
            // 메모리 사용량 정보 수집
            /////////////////////////////////////////

            final Map<String, Object> params = new HashMap<>();
            params.put("hostname", HttpNotifyUtils.getHostname());
            params.put("type", "JVMHeapUsage");
            params.put("threshold", thresholdValue);
            params.put("calculatedThreshold", calculatedThreshold);
            params.put("max", heapMemoryUsage.getMax());
            params.put("used", heapMemoryUsage.getUsed());
            params.put("init", heapMemoryUsage.getInit());
            params.put("committed", heapMemoryUsage.getCommitted());

            getLogger().info("JVM Heap Memory Reporting Task : {}", params);

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
            lastValueWasExceeded = false;
            lastReportTime = System.currentTimeMillis();
        }
    }

    // 이 Reporting Task는 정지 시 별도로 해제할 상태가 없다.
    @OnStopped
    public void onStopped() {
    }

    // 임계값 프로퍼티 값이 백분율(예; 65%) 또는 데이터 크기(예; 100 MB) 형식인지 검증하는 Validator.
    private static class ThresholdValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {

            if (!PERCENTAGE_PATTERN.matcher(input).matches() && !DATA_SIZE_PATTERN.matcher(input).matches()) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Valid value is a number in the range of 0-99 followed by a percent sign (e.g. 65%) or a Data Size (e.g. 100 MB)").build();
            }

            return new ValidationResult.Builder().input(input).subject(subject).valid(true).build();
        }
    }
}
