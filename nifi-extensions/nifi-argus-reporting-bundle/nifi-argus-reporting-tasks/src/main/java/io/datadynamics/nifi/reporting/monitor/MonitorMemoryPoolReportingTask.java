/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-reporting-tasks/src/main/java/org/apache/nifi/controller/MonitorMemory.java
 */
package io.datadynamics.nifi.reporting.monitor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datadynamics.nifi.reporting.HttpNotifyUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
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
import org.apache.nifi.util.FormatUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * 특정 JVM 메모리 풀(예; Old Gen)의 사용률(Collection Usage)을 감시하는 Reporting Task.
 * JMX의 {@link MemoryPoolMXBean} Collection Usage Threshold 기능을 이용해 임계값 초과 여부를 판단하며,
 * 초과 시 로그 경고/Bulletin을 발생시키고 필요 시 외부 HTTP 서비스로 알림을 전송한다.
 */
@Tags({"dd", "custom", "monitor", "memory", "heap", "jvm", "gc", "garbage collection", "warning"})
@CapabilityDescription("특정 JVM 메모리 풀에 대해 JVM에서 사용 가능한 Java 힙의 양을 확인합니다. 사용된 공간의 양이 구성 가능한 일부 임계값을 초과하는 경우 로그 메시지 및 시스템 수준 게시판을 통해 메모리 풀이 이 임계값을 초과한다고 경고합니다.")
public class MonitorMemoryPoolReportingTask extends AbstractReportingTask {

    // 임계값이 백분율(예; 65%) 형식인지 확인하는 정규식.
    public static final Pattern PERCENTAGE_PATTERN = Pattern.compile("\\d{1,2}%");
    // 임계값이 데이터 크기(예; 100 MB) 형식인지 확인하는 정규식.
    public static final Pattern DATA_SIZE_PATTERN = DataUnit.DATA_SIZE_PATTERN;
    public static final Pattern TIME_PERIOD_PATTERN = FormatUtils.TIME_DURATION_PATTERN;
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
            .description("설정한 Memory Pool의 사용율 임계값을 초과하는 경우 Bulletin에 레포팅하는 간격을 설정합니다. (예; 2000 nanos, 2000 millis, 20 secs, 5 mins, 1 hrs, 1 days)")
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
    // 다양한 JVM/GC 구현체에서 사용하는 Old Generation 메모리 풀의 이름 목록.
    // MEMORY_POOL_PROPERTY의 기본값을 정할 때 이 중 하나가 존재하면 우선적으로 선택한다.
    private static final List<String> GC_OLD_GEN_POOLS = Collections.unmodifiableList(Arrays.asList("Tenured Gen", "PS Old Gen", "G1 Old Gen", "CMS Old Gen", "ZHeap"));
    // MEMORY_POOL_PROPERTY에 선택 가능한 값 목록(allowableValues)으로 사용된다.
    private static final AllowableValue[] memPoolAllowableValues;

    static {
        // Collection Usage Threshold를 지원하는 메모리 풀만 허용한다. 지원하지 않으면 애초에 리포팅할 수 없기 때문이다.
        memPoolAllowableValues = ManagementFactory.getMemoryPoolMXBeans()
                .stream()
                .filter(MemoryPoolMXBean::isCollectionUsageThresholdSupported)
                .map(MemoryPoolMXBean::getName)
                .map(AllowableValue::new)
                .toArray(AllowableValue[]::new);
        defaultMemoryPool = Arrays.stream(memPoolAllowableValues)
                .map(AllowableValue::getValue)
                .filter(GC_OLD_GEN_POOLS::contains)
                .findFirst()
                .orElse(null);
    }

    private final static List<PropertyDescriptor> propertyDescriptors;
    // 외부 HTTP 서비스로 전송할 알림 payload를 JSON으로 직렬화하는 데 사용한다.
    public static final ObjectMapper mapper = new ObjectMapper();
    // GC_OLD_GEN_POOLS 중 현재 JVM에 존재하는 첫 번째 풀 이름. MEMORY_POOL_PROPERTY의 기본값으로 사용된다.
    private static String defaultMemoryPool;
    public static final PropertyDescriptor MEMORY_POOL_PROPERTY = new PropertyDescriptor.Builder()
            .name("Memory Pool")
            .displayName("메모리 풀")
            .description("모니터링할 JVM 메모리 풀의 이름입니다. 메모리 풀에 허용되는 값은 플랫폼 및 JVM에 따라 다르며 다양한 Java 버전 및 게시된 문서에 따라 다를 수 있습니다. 현재 실행 중인 호스트 플랫폼 및 JVM에서 사용할 수 없는 메모리 풀을 사용하도록 구성된 경우 이 보고 작업은 무효화됩니다.")
            .required(true)
            .allowableValues(memPoolAllowableValues)
            .defaultValue(defaultMemoryPool)
            .build();

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(MEMORY_POOL_PROPERTY);
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
    // 설정된 이름과 일치하여 현재 감시 중인 JVM 메모리 풀의 MXBean.
    private volatile MemoryPoolMXBean monitoredBean;
    // 사용자가 설정한 원본 임계값 문자열(백분율 또는 데이터 크기 표기).
    private volatile String threshold = "65%";
    // threshold 문자열을 실제 바이트 수로 환산한 값. Collection Usage Threshold 설정 및 초과 판정에 사용된다.
    private volatile long calculatedThreshold;
    // 마지막으로 Bulletin/알림을 발생시킨 시각(밀리초). 리포팅 간격 제어에 사용된다.
    private volatile long lastReportTime;
    // 임계값 초과 상태가 반복적으로 리포팅되지 않도록 제한하는 최소 간격(밀리초).
    private volatile long reportingIntervalMillis;
    // 직전 확인 시점에 임계값을 초과한 상태였는지 여부. 초과 상태에서 정상으로 복귀했을 때 로그를 남기기 위해 사용된다.
    private volatile boolean lastValueWasExceeded;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     * Reporting Task 스케줄링 시점에 감시 대상 메모리 풀을 찾아 Collection Usage Threshold를 설정하고,
     * 외부 알림 전송에 사용할 HttpClient를 초기화한다.
     * 설정된 이름의 메모리 풀을 현재 JVM에서 찾지 못하면 {@link InitializationException}을 던져 태스크를 무효화한다.
     */
    @OnScheduled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final String desiredMemoryPoolName = context.getProperty(MEMORY_POOL_PROPERTY).getValue();
        final String thresholdValue = context.getProperty(THRESHOLD_PROPERTY).getValue().trim();
        threshold = thresholdValue;

        final Long reportingIntervalValue = context.getProperty(REPORTING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        if (reportingIntervalValue == null) {
            reportingIntervalMillis = context.getSchedulingPeriod(TimeUnit.MILLISECONDS);
        } else {
            reportingIntervalMillis = reportingIntervalValue;
        }

        final List<MemoryPoolMXBean> memoryPoolBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (int i = 0; i < memoryPoolBeans.size() && monitoredBean == null; i++) {
            MemoryPoolMXBean memoryPoolBean = memoryPoolBeans.get(i);
            String memoryPoolName = memoryPoolBean.getName();
            if (desiredMemoryPoolName.equals(memoryPoolName)) {
                monitoredBean = memoryPoolBean;
                if (memoryPoolBean.isCollectionUsageThresholdSupported()) {
                    if (DATA_SIZE_PATTERN.matcher(thresholdValue).matches()) {
                        calculatedThreshold = DataUnit.parseDataSize(thresholdValue, DataUnit.B).longValue();
                    } else {
                        final String percentage = thresholdValue.substring(0, thresholdValue.length() - 1);
                        final double pct = Double.parseDouble(percentage) / 100D;
                        calculatedThreshold = (long) (monitoredBean.getCollectionUsage().getMax() * pct);
                    }

                    if (monitoredBean.isCollectionUsageThresholdSupported()) {
                        monitoredBean.setCollectionUsageThreshold(calculatedThreshold);
                    }
                }
            }
        }

        if (monitoredBean == null) {
            throw new InitializationException("Found no JVM Memory Pool with name " + desiredMemoryPoolName + "; will not monitor Memory Pool");
        }

        /////////////////////////////////////////
        // External HTTP Service
        /////////////////////////////////////////

        httpClientReference.set(null);

        final long connectTimeout = context.getProperty(HTTP_CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        requestTimeoutMillis = context.getProperty(HTTP_WRITE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        httpClientReference.set(HttpNotifyUtils.createHttpClient(connectTimeout));
    }

    // 매 스케줄 주기마다 실행되어 감시 중인 메모리 풀의 Collection Usage Threshold 초과 여부를 확인한다.
    @Override
    public void onTrigger(final ReportingContext context) {
        final MemoryPoolMXBean bean = monitoredBean;
        if (bean == null) {
            return;
        }

        final boolean isExternalHttpUrlEnable = Boolean.TRUE.equals(context.getProperty(EXTERNAL_HTTP_URL_ENABLE).evaluateAttributeExpressions().asBoolean());
        final String externalHttpUrl = context.getProperty(EXTERNAL_HTTP_URL).evaluateAttributeExpressions().getValue();

        final MemoryUsage usage = bean.getCollectionUsage();
        if (usage == null) {
            getLogger().warn("{} could not determine memory usage for pool with name {}", this, context.getProperty(MEMORY_POOL_PROPERTY));
            return;
        }

        final double percentageUsed = (double) usage.getUsed() / (double) usage.getMax() * 100D;
        // 일부 환경에서는 감시 대상 메모리 빈의 gcSensor가 실제 사용량이 임계값에 도달하기 전에 'on' 상태로 고착되어
        // 다음 GC가 발생하기 전까지 잘못된 초과 상태를 보고할 수 있다. 이를 방지하기 위해 계산된 임계값(calculatedThreshold)과의
        // 비교 조건을 추가로 확인한다.
        if (bean.isCollectionUsageThresholdSupported() && bean.isCollectionUsageThresholdExceeded() && usage.getUsed() > calculatedThreshold) {
            if (System.currentTimeMillis() < reportingIntervalMillis + lastReportTime && lastReportTime > 0L) {
                return;
            }

            lastReportTime = System.currentTimeMillis();
            lastValueWasExceeded = true;
            final String message = String.format("Memory Pool '%1$s' has exceeded the configured Threshold of %2$s, having used %3$s / %4$s (%5$.2f%%)",
                    bean.getName(), threshold, FormatUtils.formatDataSize(usage.getUsed()),
                    FormatUtils.formatDataSize(usage.getMax()), percentageUsed);

            getLogger().warn("{}", message);

            /////////////////////////////////////////
            // 메모리 풀 사용량 정보 수집
            /////////////////////////////////////////

            final Map<String, Object> params = new HashMap<>();
            params.put("hostname", HttpNotifyUtils.getHostname());
            params.put("type", "JVMHeapPoolUsage");
            params.put("threshold", threshold);
            params.put("percentageUsed", percentageUsed);
            params.put("memoryPoolName", bean.getName());
            params.put("used", usage.getUsed());
            params.put("max", usage.getMax());
            params.put("init", usage.getInit());
            params.put("commited", usage.getCommitted());

            getLogger().info("JVM Heap Memory Pool Reporting Task : {}", params);

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
            final String message = String.format("Memory Pool '%1$s' is no longer exceeding the configured Threshold of %2$s; currently using %3$s / %4$s (%5$.2f%%)",
                    bean.getName(), threshold, FormatUtils.formatDataSize(usage.getUsed()),
                    FormatUtils.formatDataSize(usage.getMax()), percentageUsed);

            getLogger().info("{}", message);
        }
    }

    // Reporting Task 정지 시 감시 대상 메모리 풀 참조를 해제한다.
    @OnStopped
    public void onStopped() {
        monitoredBean = null;
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
