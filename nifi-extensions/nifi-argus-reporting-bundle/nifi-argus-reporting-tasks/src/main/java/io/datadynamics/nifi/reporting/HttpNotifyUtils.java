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
package io.datadynamics.nifi.reporting;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * NiFi 2.x 마이그레이션 과정에서 okhttp 의존성을 제거하고 JDK {@link java.net.http.HttpClient}를 사용하도록
 * 변경하면서 공통으로 사용하는 HTTP 유틸리티.
 */
public final class HttpNotifyUtils {

    // 유틸리티 클래스이므로 인스턴스 생성을 막는다.
    private HttpNotifyUtils() {
    }

    /**
     * 지정한 연결 타임아웃(밀리초)을 적용한 {@link HttpClient}를 새로 생성한다.
     * 각 Reporting Task는 {@code @OnScheduled} 시점에 이 메서드로 클라이언트를 초기화하여 재사용한다.
     */
    public static HttpClient createHttpClient(final long connectTimeoutMillis) {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMillis))
                .build();
    }

    /**
     * 주어진 JSON 문자열을 요청 본문으로 하여 지정된 URL에 HTTP POST 요청을 전송한다.
     * Content-Type은 application/json으로 고정되며, additionalHeaders가 주어지면 요청 헤더에 함께 추가한다.
     *
     * @param httpClient           요청을 전송할 HttpClient 인스턴스
     * @param url                  요청을 전송할 대상 URL
     * @param json                 전송할 JSON 본문
     * @param requestTimeoutMillis 요청 타임아웃(밀리초)
     * @param additionalHeaders    추가로 전송할 HTTP 헤더(없으면 null 가능)
     */
    public static HttpResponse<String> postJson(final HttpClient httpClient, final String url, final String json,
                                                final long requestTimeoutMillis, final Map<String, String> additionalHeaders)
            throws IOException, InterruptedException {
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(requestTimeoutMillis))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));
        if (additionalHeaders != null) {
            additionalHeaders.forEach(requestBuilder::header);
        }
        return httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    }

    // HTTP 상태 코드가 2xx 범위인지(성공 응답인지) 확인한다.
    public static boolean isSuccessful(final HttpResponse<?> response) {
        return response.statusCode() >= 200 && response.statusCode() < 300;
    }

    // 현재 서버(로컬 호스트)의 호스트명을 조회한다. 알림 payload에 어느 노드에서 발생한 이벤트인지 표시하기 위해 사용한다.
    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("서버의 호스트명을 확인할 수 없습니다.", e);
        }
    }
}
