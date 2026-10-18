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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static io.datadynamics.nifi.trino.service.TrinoDriverProperties.*;

/**
 * 서비스 프로퍼티에서 해석한 연결 설정. NiFi 에 의존하지 않아 단위 테스트로 드라이버 프로퍼티 조립을 검증할 수 있다.
 * <p>
 * {@link #toString()}은 자격 증명을 출력하지 않는다.
 */
public final class TrinoConnectionSettings {

    /** 동적 프로퍼티 중 Trino 세션 프로퍼티로 보낼 접두사. */
    public static final String SESSION_PREFIX = "session.";

    private String url;
    private boolean trinoUrlFormat = true;
    private String defaultCatalog;
    private String defaultSchema;
    private AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.NONE;
    private String user;
    private String password;
    private String accessToken;
    private boolean oauth2ProviderConfigured;
    private String sessionUser;
    private String source;
    private String clientTags;
    private String roles;
    private String extraCredentials;
    private String timeZone;
    private String encoding;
    private boolean explicitPrepare = true;
    private String sslVerification = "FULL";
    private TlsSettings tls;
    private String kerberosRemoteServiceName;
    private String kerberosPrincipal;
    private String kerberosKeytabPath;
    private String kerberosConfigPath;
    private String kerberosServicePrincipalPattern;
    private Boolean kerberosUseCanonicalHostname;
    private final Map<String, String> dynamicProperties = new LinkedHashMap<>();

    /**
     * SSL Context Service 에서 꺼낸 truststore/keystore. 경로가 null 이면 해당 저장소를 설정하지 않는다.
     */
    public record TlsSettings(String trustStorePath, String trustStorePassword, String trustStoreType,
                              String keyStorePath, String keyStorePassword, String keyStoreType) {
        @Override
        public String toString() {
            return "TlsSettings[trustStorePath=" + trustStorePath + ", keyStorePath=" + keyStorePath + "]";
        }
    }

    // ---- setters (fluent) ----

    public TrinoConnectionSettings url(final String value) { this.url = value; return this; }
    /** false 면 외부 드라이버용으로 jdbc: 접두사만 요구한다(형식 판단은 driver.acceptsURL). */
    public TrinoConnectionSettings trinoUrlFormat(final boolean value) { this.trinoUrlFormat = value; return this; }
    public TrinoConnectionSettings defaultCatalog(final String value) { this.defaultCatalog = value; return this; }
    public TrinoConnectionSettings defaultSchema(final String value) { this.defaultSchema = value; return this; }
    public TrinoConnectionSettings authenticationStrategy(final AuthenticationStrategy value) { this.authenticationStrategy = value; return this; }
    public TrinoConnectionSettings user(final String value) { this.user = value; return this; }
    public TrinoConnectionSettings password(final String value) { this.password = value; return this; }
    public TrinoConnectionSettings accessToken(final String value) { this.accessToken = value; return this; }
    public TrinoConnectionSettings oauth2ProviderConfigured(final boolean value) { this.oauth2ProviderConfigured = value; return this; }
    public TrinoConnectionSettings sessionUser(final String value) { this.sessionUser = value; return this; }
    public TrinoConnectionSettings source(final String value) { this.source = value; return this; }
    public TrinoConnectionSettings clientTags(final String value) { this.clientTags = value; return this; }
    public TrinoConnectionSettings roles(final String value) { this.roles = value; return this; }
    public TrinoConnectionSettings extraCredentials(final String value) { this.extraCredentials = value; return this; }
    public TrinoConnectionSettings timeZone(final String value) { this.timeZone = value; return this; }
    public TrinoConnectionSettings encoding(final String value) { this.encoding = value; return this; }
    public TrinoConnectionSettings explicitPrepare(final boolean value) { this.explicitPrepare = value; return this; }
    public TrinoConnectionSettings sslVerification(final String value) { this.sslVerification = value; return this; }
    public TrinoConnectionSettings tls(final TlsSettings value) { this.tls = value; return this; }
    public TrinoConnectionSettings kerberosRemoteServiceName(final String value) { this.kerberosRemoteServiceName = value; return this; }
    public TrinoConnectionSettings kerberosPrincipal(final String value) { this.kerberosPrincipal = value; return this; }
    public TrinoConnectionSettings kerberosKeytabPath(final String value) { this.kerberosKeytabPath = value; return this; }
    public TrinoConnectionSettings kerberosConfigPath(final String value) { this.kerberosConfigPath = value; return this; }
    public TrinoConnectionSettings kerberosServicePrincipalPattern(final String value) { this.kerberosServicePrincipalPattern = value; return this; }
    public TrinoConnectionSettings kerberosUseCanonicalHostname(final Boolean value) { this.kerberosUseCanonicalHostname = value; return this; }

    public TrinoConnectionSettings dynamicProperty(final String name, final String value) {
        this.dynamicProperties.put(name, value);
        return this;
    }

    public AuthenticationStrategy getAuthenticationStrategy() {
        return authenticationStrategy;
    }

    // ---- URL ----

    /**
     * @return Default Catalog/Schema 를 경로로 붙인 최종 URL
     * @throws IllegalArgumentException URL 형식 오류
     */
    public TrinoJdbcUrl effectiveUrl() {
        return TrinoJdbcUrl.parse(url, trinoUrlFormat).withDefaults(defaultCatalog, defaultSchema);
    }

    /**
     * TLS 사용 여부: SSL Context Service 가 있거나 URL 쿼리에 {@code SSL=true}.
     */
    public boolean isTlsEnabled() {
        if (tls != null) {
            return true;
        }
        try {
            return "true".equalsIgnoreCase(TrinoJdbcUrl.parse(url, trinoUrlFormat).getQueryParameters().get(SSL));
        } catch (final IllegalArgumentException e) {
            return false;
        }
    }

    // ---- driver properties ----

    /**
     * 드라이버 {@link Properties}를 조립한다.
     *
     * @param oauth2AccessToken OAUTH2 방식에서 이번 연결에 쓸 토큰(그 외 방식은 무시)
     */
    public Properties toDriverProperties(final String oauth2AccessToken) {
        final Properties props = new Properties();
        putIfNotBlank(props, USER, user);
        switch (authenticationStrategy) {
            case PASSWORD -> putSecret(props, PASSWORD, password);
            case JWT -> putSecret(props, ACCESS_TOKEN, accessToken);
            case OAUTH2 -> putSecret(props, ACCESS_TOKEN, oauth2AccessToken);
            case KERBEROS -> {
                putIfNotBlank(props, KERBEROS_REMOTE_SERVICE_NAME, kerberosRemoteServiceName);
                putIfNotBlank(props, KERBEROS_PRINCIPAL, kerberosPrincipal);
                putIfNotBlank(props, KERBEROS_KEYTAB_PATH, kerberosKeytabPath);
                putIfNotBlank(props, KERBEROS_CONFIG_PATH, kerberosConfigPath);
                putIfNotBlank(props, KERBEROS_SERVICE_PRINCIPAL_PATTERN, kerberosServicePrincipalPattern);
                if (kerberosUseCanonicalHostname != null) {
                    props.setProperty(KERBEROS_USE_CANONICAL_HOSTNAME, kerberosUseCanonicalHostname.toString());
                }
            }
            case NONE -> {
                // 사용자만 전달
            }
        }
        putIfNotBlank(props, SESSION_USER, sessionUser);
        putIfNotBlank(props, SOURCE, source);
        putIfNotBlank(props, CLIENT_TAGS, normalizeCsv(clientTags));
        putIfNotBlank(props, ROLES, roles);
        putSecret(props, EXTRA_CREDENTIALS, extraCredentials);
        putIfNotBlank(props, TIMEZONE, timeZone);
        putIfNotBlank(props, ENCODING, encoding);
        props.setProperty(EXPLICIT_PREPARE, Boolean.toString(explicitPrepare));

        if (tls != null) {
            props.setProperty(SSL, "true");
            putIfNotBlank(props, SSL_TRUST_STORE_PATH, tls.trustStorePath());
            if (notBlank(tls.trustStorePath())) {
                putSecret(props, SSL_TRUST_STORE_PASSWORD, tls.trustStorePassword());
                putIfNotBlank(props, SSL_TRUST_STORE_TYPE, tls.trustStoreType());
            }
            putIfNotBlank(props, SSL_KEY_STORE_PATH, tls.keyStorePath());
            if (notBlank(tls.keyStorePath())) {
                putSecret(props, SSL_KEY_STORE_PASSWORD, tls.keyStorePassword());
                putIfNotBlank(props, SSL_KEY_STORE_TYPE, tls.keyStoreType());
            }
        }
        if (isTlsEnabled()) {
            putIfNotBlank(props, SSL_VERIFICATION, sslVerification);
        }

        final String sessionProperties = sessionPropertiesValue();
        if (!sessionProperties.isEmpty()) {
            props.setProperty(SESSION_PROPERTIES, sessionProperties);
        }
        passthroughProperties().forEach(props::setProperty);
        return props;
    }

    /**
     * 동적 프로퍼티 {@code session.<name>} 을 {@code name:value;name2:value2} 로 조립한다.
     */
    public String sessionPropertiesValue() {
        final StringBuilder sb = new StringBuilder();
        dynamicProperties.forEach((name, value) -> {
            if (name.startsWith(SESSION_PREFIX)) {
                if (!sb.isEmpty()) {
                    sb.append(';');
                }
                sb.append(name.substring(SESSION_PREFIX.length())).append(':').append(value == null ? "" : value.trim());
            }
        });
        return sb.toString();
    }

    /**
     * @return {@code session.} 이 아닌 동적 프로퍼티(그대로 드라이버 프로퍼티로 전달)
     */
    public Map<String, String> passthroughProperties() {
        final Map<String, String> result = new LinkedHashMap<>();
        dynamicProperties.forEach((name, value) -> {
            if (!name.startsWith(SESSION_PREFIX) && value != null) {
                result.put(name, value);
            }
        });
        return Collections.unmodifiableMap(result);
    }

    // ---- validation ----

    /**
     * @return 사용자 표시용 오류 메시지 목록(비어 있으면 유효)
     */
    public List<String> validate() {
        final List<String> errors = new ArrayList<>();

        TrinoJdbcUrl parsedUrl = null;
        try {
            parsedUrl = effectiveUrl();
        } catch (final IllegalArgumentException e) {
            errors.add("Trino URL: " + e.getMessage());
        }

        switch (authenticationStrategy) {
            case NONE, PASSWORD -> {
                if (!notBlank(user)) {
                    errors.add("User: " + authenticationStrategy.getValue() + " 인증에는 사용자가 필요하다");
                }
                if (authenticationStrategy == AuthenticationStrategy.PASSWORD && !notBlank(password)) {
                    errors.add("Password: PASSWORD 인증에는 비밀번호가 필요하다");
                }
            }
            case JWT -> {
                if (!notBlank(accessToken)) {
                    errors.add("Access Token: JWT 인증에는 토큰이 필요하다");
                }
            }
            case OAUTH2 -> {
                if (!oauth2ProviderConfigured) {
                    errors.add("OAuth2 Access Token Provider: OAUTH2 인증에는 토큰 제공 서비스가 필요하다");
                }
            }
            case KERBEROS -> {
                if (!notBlank(kerberosRemoteServiceName)) {
                    errors.add("Kerberos Remote Service Name: KERBEROS 인증에는 Trino 서비스 이름(예: trino)이 필요하다");
                }
                if (notBlank(kerberosKeytabPath) && !notBlank(kerberosPrincipal)) {
                    errors.add("Kerberos Principal: keytab 을 쓰려면 principal 이 필요하다");
                }
            }
        }
        if (authenticationStrategy.requiresTls() && !isTlsEnabled()) {
            errors.add("SSL Context Service: " + authenticationStrategy.getValue()
                    + " 인증은 TLS 가 필수다. SSL Context Service 를 설정하거나 URL 에 SSL=true 를 지정")
            ;
        }

        if (notBlank(timeZone)) {
            try {
                ZoneId.of(timeZone.trim());
            } catch (final DateTimeException e) {
                errors.add("Time Zone: 잘못된 시간대 ID: " + timeZone);
            }
        }

        // 동적 프로퍼티
        for (final Map.Entry<String, String> entry : dynamicProperties.entrySet()) {
            final String name = entry.getKey();
            final String value = entry.getValue() == null ? "" : entry.getValue().trim();
            if (name.startsWith(SESSION_PREFIX)) {
                final String sessionName = name.substring(SESSION_PREFIX.length());
                if (!isSessionToken(sessionName)) {
                    errors.add(name + ": 세션 프로퍼티 이름이 비었거나 공백·';'·':'·비ASCII 문자를 포함한다");
                }
                if (!isSessionToken(value)) {
                    errors.add(name + ": 세션 프로퍼티 값이 비었거나 공백·';'·':'·비ASCII 문자를 포함한다");
                }
            } else if (MANAGED.contains(name)) {
                errors.add(name + ": 서비스 전용 프로퍼티로 관리되는 드라이버 파라미터다. 동적 프로퍼티로 지정할 수 없다");
            } else if (FORBIDDEN.contains(name)) {
                errors.add(name + ": " + forbiddenReason(name));
            }
        }

        // URL 쿼리 파라미터 검사: 금지 키, 자격 증명, Properties 와의 중복
        if (parsedUrl != null) {
            final Set<String> propertyKeys = toDriverProperties("token").stringPropertyNames();
            final Set<String> duplicated = new TreeSet<>();
            for (final String key : parsedUrl.getQueryParameters().keySet()) {
                if (CREDENTIALS.contains(key)) {
                    errors.add("Trino URL: 자격 증명(" + key + ")을 URL 쿼리에 넣을 수 없다. URL 은 로그·provenance 에 남는다");
                } else if (FORBIDDEN.contains(key)) {
                    errors.add("Trino URL: " + key + " - " + forbiddenReason(key));
                } else if (propertyKeys.contains(key)) {
                    duplicated.add(key);
                }
            }
            if (!duplicated.isEmpty()) {
                errors.add("Trino URL: 같은 파라미터를 URL 쿼리와 서비스 프로퍼티 양쪽에 지정했다(드라이버가 거부함): " + duplicated);
            }
        }
        return errors;
    }

    private static String forbiddenReason(final String key) {
        return switch (key) {
            case CATALOG, SCHEMA -> "catalog/schema 는 URL 경로(jdbc:trino://host:port/catalog/schema)나 Default Catalog/Schema 로 지정한다";
            case EXTERNAL_AUTHENTICATION -> "브라우저 기반 외부 인증은 서버 프로세서에서 쓸 수 없어 지원하지 않는다";
            default -> "지원하지 않는 파라미터다";
        };
    }

    /**
     * Trino 드라이버의 map 파서가 허용하는 토큰: 비어 있지 않고, 공백 없는 출력 가능 ASCII, ';'·':' 제외.
     */
    static boolean isSessionToken(final String token) {
        if (token == null || token.isEmpty()) {
            return false;
        }
        for (int i = 0; i < token.length(); i++) {
            final char c = token.charAt(i);
            if (c <= 0x20 || c >= 0x7F || c == ';' || c == ':') {
                return false;
            }
        }
        return true;
    }

    private static String normalizeCsv(final String csv) {
        if (csv == null) {
            return null;
        }
        final List<String> parts = new ArrayList<>();
        for (final String part : csv.split(",")) {
            if (!part.isBlank()) {
                parts.add(part.trim());
            }
        }
        return String.join(",", parts);
    }

    private static void putIfNotBlank(final Properties props, final String key, final String value) {
        if (notBlank(value)) {
            props.setProperty(key, value.trim());
        }
    }

    /** 자격 증명은 공백을 다듬지 않고 그대로 전달한다. */
    private static void putSecret(final Properties props, final String key, final String value) {
        if (value != null && !value.isEmpty()) {
            props.setProperty(key, value);
        }
    }

    private static boolean notBlank(final String value) {
        return value != null && !value.isBlank();
    }

    @Override
    public String toString() {
        return "TrinoConnectionSettings[url=" + url + ", auth=" + authenticationStrategy + ", user=" + user
                + ", source=" + source + ", tls=" + tls + "]";
    }
}
