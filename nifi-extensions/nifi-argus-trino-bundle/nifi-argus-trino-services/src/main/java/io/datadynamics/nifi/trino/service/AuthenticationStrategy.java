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
package io.datadynamics.nifi.trino.service;

import org.apache.nifi.components.DescribedValue;

/**
 * Trino 인증 방식. 브라우저 기반 {@code externalAuthentication}은 서버 프로세서에 부적합해 지원하지 않는다.
 */
public enum AuthenticationStrategy implements DescribedValue {

    NONE("NONE", "인증 없음", "사용자 이름만 전달한다. TLS 없이도 쓸 수 있다(개발·내부망 전용)."),
    PASSWORD("PASSWORD", "비밀번호", "user/password 로 인증한다(LDAP·파일 인증 등). TLS 필수."),
    JWT("JWT", "JWT 토큰", "고정 JWT access token 으로 인증한다(accessToken). TLS 필수."),
    KERBEROS("KERBEROS", "Kerberos", "드라이버가 keytab·principal 로 직접 Kerberos 로그인(SPNEGO)한다. TLS 필수."),
    OAUTH2("OAUTH2", "OAuth2", "NiFi OAuth2 Access Token Provider 서비스가 발급한 토큰을 연결마다 accessToken 으로 전달한다. TLS 필수.");

    private final String value;
    private final String displayName;
    private final String description;

    AuthenticationStrategy(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * @return TLS 가 필요한 인증 방식인지 여부 (드라이버도 TLS 없는 자격 증명 전송을 거부한다)
     */
    public boolean requiresTls() {
        return this != NONE;
    }
}
