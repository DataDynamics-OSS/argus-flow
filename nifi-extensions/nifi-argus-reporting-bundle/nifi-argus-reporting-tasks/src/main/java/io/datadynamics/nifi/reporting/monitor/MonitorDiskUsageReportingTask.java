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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-reporting-tasks/src/main/java/org/apache/nifi/controller/MonitorDiskUsage.java
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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.FormatUtils;

import java.io.File;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 지정된 디렉토리가 위치한 파티션의 디스크 사용률을 주기적으로 확인하는 Reporting Task.
 * 사용률이 설정된 임계값을 초과하면 로그 경고 및 시스템 Bulletin을 발생시키고,
 * 필요 시 외부 HTTP 서비스로도 동일한 알림 정보를 JSON payload로 전송한다.
 */
@Tags({"dd", "custom", "disk", "storage", "warning", "monitoring", "repo"})
@CapabilityDescription("지정된 디렉토리에 사용 가능한 저장 공간의 양을 확인하고 해당 디렉토리가 있는 파티션이 구성 가능한 저장 공간 임계값을 초과하는 경우 로그 메시지 및 시스템 수준 Bulletin을 통해 경고합니다. 또한 외부 서비스에 해당 알림을 발송합니다.")
public class MonitorDiskUsageReportingTask extends AbstractReportingTask {

    public static final PropertyDescriptor DIR_LOCATION = new PropertyDescriptor.Builder()
            .name("디렉토리 경로")
            .description("모니터링할 파티션의 디렉토리 경로입니다.")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
            .build();
    public static final PropertyDescriptor DIR_DISPLAY_NAME = new PropertyDescriptor.Builder()
            .name("Alert에 표시할 디렉토리 명")
            .description("Alert에 디렉터리에 대해 표시할 이름입니다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("Un-Named")
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
    // 임계값 프로퍼티에서 숫자(1~2자리)와 % 기호로 구성된 백분율 표기를 추출하기 위한 정규식.
    private static final Pattern PERCENT_PATTERN = Pattern.compile("(\\d+{1,2})%");
    public static final PropertyDescriptor DIR_THRESHOLD = new PropertyDescriptor.Builder()
            .name("임계치")
            .description("디렉터리가 있는 파티션의 디스크 사용량이 문제임을 나타내기 위해 Bulletin에 생성되는 임계값입니다.")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(PERCENT_PATTERN))
            .defaultValue("80%")
            .build();
    // 외부 HTTP 서비스로 전송할 알림 payload를 JSON으로 직렬화하는 데 사용한다.
    public static final ObjectMapper mapper = new ObjectMapper();

    // onScheduled 시점에 생성되어 이후 onTrigger에서 재사용되는 HttpClient. 스레드 안전을 위해 AtomicReference로 보관한다.
    private final AtomicReference<HttpClient> httpClientReference = new AtomicReference<>();
    // 외부 HTTP 서비스 호출 시 응답을 기다리는 최대 시간(밀리초).
    private volatile long requestTimeoutMillis = 10_000L;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DIR_THRESHOLD);
        descriptors.add(DIR_LOCATION);
        descriptors.add(DIR_DISPLAY_NAME);
        descriptors.add(EXTERNAL_HTTP_URL_ENABLE);
        descriptors.add(EXTERNAL_HTTP_URL);
        descriptors.add(HTTP_CONNECTION_TIMEOUT);
        descriptors.add(HTTP_WRITE_TIMEOUT);
        return descriptors;
    }

    // Reporting Task가 스케줄링될 때 HTTP Connection/Write 타임아웃 프로퍼티를 읽어 HttpClient를 (재)생성한다.
    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        httpClientReference.set(null);

        final long connectTimeout = context.getProperty(HTTP_CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        requestTimeoutMillis = context.getProperty(HTTP_WRITE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        httpClientReference.set(HttpNotifyUtils.createHttpClient(connectTimeout));
    }

    // 매 스케줄 주기마다 실행되어 설정된 디렉토리의 디스크 사용률을 확인한다.
    @Override
    public void onTrigger(final ReportingContext context) {
        final String thresholdValue = context.getProperty(DIR_THRESHOLD).getValue();
        final Matcher thresholdMatcher = PERCENT_PATTERN.matcher(thresholdValue.trim());
        thresholdMatcher.find();
        final String thresholdPercentageVal = thresholdMatcher.group(1);
        final int contentRepoThreshold = Integer.parseInt(thresholdPercentageVal);

        final File dir = new File(context.getProperty(DIR_LOCATION).getValue());
        final String dirName = context.getProperty(DIR_DISPLAY_NAME).getValue();
        final boolean isExternalHttpUrlEnable = Boolean.TRUE.equals(context.getProperty(EXTERNAL_HTTP_URL_ENABLE).evaluateAttributeExpressions().asBoolean());
        final String externalHttpUrl = context.getProperty(EXTERNAL_HTTP_URL).evaluateAttributeExpressions().getValue();

        checkThreshold(dirName, dir.toPath(), contentRepoThreshold, getLogger(), isExternalHttpUrlEnable, externalHttpUrl);
    }

    /**
     * 주어진 경로가 속한 파티션의 사용량(%)을 계산하여 임계값을 초과하는지 확인한다.
     * 초과 시 로그 경고 및 Bulletin을 발생시키고, 활성화되어 있다면 외부 HTTP 서비스로도 알림을 전송한다.
     *
     * @param pathName             Bulletin/알림에 표시할 디렉터리 이름
     * @param path                 사용량을 확인할 디렉터리 경로
     * @param threshold            초과 여부를 판단할 임계값(%)
     * @param logger               경고 로그를 기록할 ComponentLog
     * @param isExternalHttpUrlEnable 외부 HTTP 알림 전송 여부
     * @param externalHttpUrl      외부 알림을 전송할 URL
     */
    void checkThreshold(final String pathName, final Path path, final int threshold, final ComponentLog logger, final boolean isExternalHttpUrlEnable, final String externalHttpUrl) {
        final File file = path.toFile();
        final long totalBytes = file.getTotalSpace();
        final long freeBytes = file.getFreeSpace();
        final long usedBytes = totalBytes - freeBytes;

        final double usedPercent = (double) usedBytes / (double) totalBytes * 100D;

        if (usedPercent >= threshold) {
            final String usedSpace = FormatUtils.formatDataSize(usedBytes);
            final String totalSpace = FormatUtils.formatDataSize(totalBytes);
            final String freeSpace = FormatUtils.formatDataSize(freeBytes);

            final double freePercent = (double) freeBytes / (double) totalBytes * 100D;

            /////////////////////////////////////////
            // Bulletin Board(시스템 게시판)에 경고 로그 기록
            /////////////////////////////////////////

            final String message = String.format("%1$s exceeds configured threshold of %2$s%%, having %3$s / %4$s (%5$.2f%%) used and %6$s (%7$.2f%%) free", pathName, threshold, usedSpace, totalSpace, usedPercent, freeSpace, freePercent);
            logger.warn(message);

            /////////////////////////////////////////
            // 디스크 사용량 정보 수집
            /////////////////////////////////////////

            final Map<String, Object> params = new HashMap<>();
            params.put("hostname", HttpNotifyUtils.getHostname());
            params.put("type", "DiskUsage");
            params.put("pathName", pathName);
            params.put("threshold", threshold);
            params.put("usedSpace", usedSpace);
            params.put("totalSpace", totalSpace);
            params.put("usedPercent", usedPercent);
            params.put("freeSpace", freeSpace);
            params.put("freePercent", freePercent);

            getLogger().info("Disk Usage Reporting Task : {}", params);

            /////////////////////////////////////////
            // 외부 HTTP 서비스로 알림 전송
            /////////////////////////////////////////

            if (isExternalHttpUrlEnable && externalHttpUrl != null) {
                try {
                    final String json = mapper.writeValueAsString(params);
                    final HttpResponse<String> response = HttpNotifyUtils.postJson(httpClientReference.get(), externalHttpUrl, json, requestTimeoutMillis, null);
                    if (!HttpNotifyUtils.isSuccessful(response)) {
                        logger.warn("{}", String.format("External HTTP Service 호출에 실패했습니다. URL : %s, Status Code : %s, Response Body : %s", externalHttpUrl, response.statusCode(), response.body()));
                    }
                } catch (Exception e) {
                    logger.warn("{}", String.format("External HTTP Service 호출에 실패했습니다. URL : %s", externalHttpUrl), e);
                }
            }
        }
    }
}
