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

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 테스트용 임베디드 HTTP 서버. 수신한 POST body와 헤더를 큐에 보관한다.
 */
public class TestHttpServerSupport implements AutoCloseable {

    /**
     * 임베디드 서버가 수신한 하나의 HTTP 요청을 표현하는 레코드.
     * 요청 body 문자열과 요청 헤더 맵을 함께 보관하여, 테스트 코드에서
     * 실제로 어떤 내용이 전송되었는지 검증할 수 있게 한다.
     */
    public record ReceivedRequest(String body, Map<String, java.util.List<String>> headers) {
    }

    private final HttpServer server;
    // 수신된 요청들을 순서대로 쌓아두는 큐. 여러 테스트가 동시에 서버를 사용할 수 있으므로 스레드 안전한 큐를 사용한다.
    private final ConcurrentLinkedQueue<ReceivedRequest> requests = new ConcurrentLinkedQueue<>();

    /**
     * 임의의 포트(0)로 로컬 HTTP 서버를 기동하고, 모든 경로("/")로 들어오는 요청을 가로채
     * body와 헤더를 큐에 저장한 뒤 항상 "{}" JSON 응답을 반환하도록 구성한다.
     */
    public TestHttpServerSupport() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (InputStream in = exchange.getRequestBody()) {
                final String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                requests.add(new ReceivedRequest(body, exchange.getRequestHeaders()));
            }
            final byte[] response = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
    }

    // 테스트 대상 ReportingTask가 알림을 전송할 목적지 URL. 실제로 기동된 서버의 랜덤 포트를 사용한다.
    public String getUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/notify";
    }

    // 지금까지 서버가 수신한 요청들을 조회한다. 테스트에서 poll()로 하나씩 꺼내 검증한다.
    public ConcurrentLinkedQueue<ReceivedRequest> getRequests() {
        return requests;
    }

    // 테스트 종료 시 임베디드 서버를 즉시(대기 없이) 종료한다.
    @Override
    public void close() {
        server.stop(0);
    }
}
