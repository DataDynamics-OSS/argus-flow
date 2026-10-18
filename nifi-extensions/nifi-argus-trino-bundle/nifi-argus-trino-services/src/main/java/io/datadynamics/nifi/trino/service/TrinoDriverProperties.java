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

import java.util.Set;

/**
 * Trino JDBC 483 드라이버 연결 파라미터 이름.
 * <p>
 * 기준 문서: <a href="https://docs.starburst.io/latest/client/jdbc.html">Starburst JDBC driver (Trino JDBC 483)</a>.
 * 이름은 {@code NonRegisteringTrinoDriver.getPropertyInfo()} 결과로 대조했다.
 */
public final class TrinoDriverProperties {

    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String SESSION_USER = "sessionUser";
    public static final String ACCESS_TOKEN = "accessToken";
    public static final String SOURCE = "source";
    public static final String CLIENT_TAGS = "clientTags";
    public static final String CLIENT_INFO = "clientInfo";
    public static final String APPLICATION_NAME_PREFIX = "applicationNamePrefix";
    public static final String SSL = "SSL";
    public static final String SSL_VERIFICATION = "SSLVerification";
    public static final String SSL_TRUST_STORE_PATH = "SSLTrustStorePath";
    public static final String SSL_TRUST_STORE_PASSWORD = "SSLTrustStorePassword";
    public static final String SSL_TRUST_STORE_TYPE = "SSLTrustStoreType";
    public static final String SSL_USE_SYSTEM_TRUST_STORE = "SSLUseSystemTrustStore";
    public static final String SSL_KEY_STORE_PATH = "SSLKeyStorePath";
    public static final String SSL_KEY_STORE_PASSWORD = "SSLKeyStorePassword";
    public static final String SSL_KEY_STORE_TYPE = "SSLKeyStoreType";
    public static final String HOSTNAME_IN_CERTIFICATE = "hostnameInCertificate";
    public static final String TIMEZONE = "timezone";
    public static final String SESSION_PROPERTIES = "sessionProperties";
    public static final String EXTRA_CREDENTIALS = "extraCredentials";
    public static final String ROLES = "roles";
    public static final String EXTRA_HEADERS = "extraHeaders";
    public static final String ENCODING = "encoding";
    public static final String EXPLICIT_PREPARE = "explicitPrepare";
    public static final String DISABLE_COMPRESSION = "disableCompression";
    public static final String VALIDATE_CONNECTION = "validateConnection";
    public static final String EXTERNAL_AUTHENTICATION = "externalAuthentication";
    public static final String CATALOG = "catalog";
    public static final String SCHEMA = "schema";

    public static final String KERBEROS_REMOTE_SERVICE_NAME = "KerberosRemoteServiceName";
    public static final String KERBEROS_PRINCIPAL = "KerberosPrincipal";
    public static final String KERBEROS_KEYTAB_PATH = "KerberosKeytabPath";
    public static final String KERBEROS_CONFIG_PATH = "KerberosConfigPath";
    public static final String KERBEROS_SERVICE_PRINCIPAL_PATTERN = "KerberosServicePrincipalPattern";
    public static final String KERBEROS_USE_CANONICAL_HOSTNAME = "KerberosUseCanonicalHostname";

    /**
     * 서비스 전용 프로퍼티가 관리하는 키. 동적 프로퍼티로 다시 지정할 수 없다.
     */
    public static final Set<String> MANAGED = Set.of(
            USER, PASSWORD, SESSION_USER, ACCESS_TOKEN, SOURCE, CLIENT_TAGS,
            SSL, SSL_VERIFICATION, SSL_TRUST_STORE_PATH, SSL_TRUST_STORE_PASSWORD, SSL_TRUST_STORE_TYPE,
            SSL_KEY_STORE_PATH, SSL_KEY_STORE_PASSWORD, SSL_KEY_STORE_TYPE,
            TIMEZONE, SESSION_PROPERTIES, EXTRA_CREDENTIALS, ROLES, ENCODING, EXPLICIT_PREPARE, VALIDATE_CONNECTION,
            KERBEROS_REMOTE_SERVICE_NAME, KERBEROS_PRINCIPAL, KERBEROS_KEYTAB_PATH, KERBEROS_CONFIG_PATH,
            KERBEROS_SERVICE_PRINCIPAL_PATTERN, KERBEROS_USE_CANONICAL_HOSTNAME);

    /**
     * 어디서도(URL·동적 프로퍼티) 지정할 수 없는 키.
     * catalog/schema 는 URL 경로로만 지정하고, 브라우저 기반 외부 인증은 지원하지 않는다.
     */
    public static final Set<String> FORBIDDEN = Set.of(EXTERNAL_AUTHENTICATION, CATALOG, SCHEMA);

    /**
     * URL 쿼리에 넣으면 안 되는 자격 증명 키(URL 은 로그·provenance 에 남는다).
     */
    public static final Set<String> CREDENTIALS = Set.of(
            PASSWORD, ACCESS_TOKEN, EXTRA_CREDENTIALS, SSL_TRUST_STORE_PASSWORD, SSL_KEY_STORE_PASSWORD);

    private TrinoDriverProperties() {
    }
}
