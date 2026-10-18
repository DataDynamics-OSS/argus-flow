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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrinoConnectionSettingsTest {

    private static final TrinoConnectionSettings.TlsSettings TLS =
            new TrinoConnectionSettings.TlsSettings("/etc/ts.jks", "tspw", "JKS", null, null, null);

    private static TrinoConnectionSettings base() {
        return new TrinoConnectionSettings().url("jdbc:trino://trino:8080").user("etl").source("argus-flow");
    }

    @Test
    void assemblesDocumentedParameterNames() {
        final Properties props = base()
                .authenticationStrategy(AuthenticationStrategy.PASSWORD)
                .password(" secret ")
                .tls(new TrinoConnectionSettings.TlsSettings("/etc/ts.p12", "tspw", "PKCS12", "/etc/ks.p12", "kspw", "PKCS12"))
                .sslVerification("CA")
                .sessionUser("alice")
                .clientTags(" etl, nightly ,")
                .roles("hive:admin")
                .extraCredentials("k:v")
                .timeZone("Asia/Seoul")
                .encoding("json+zstd")
                .explicitPrepare(false)
                .dynamicProperty("session.query_max_run_time", "2h")
                .dynamicProperty("session.iceberg.compression_codec", "ZSTD")
                .dynamicProperty("clientInfo", "nifi")
                .toDriverProperties(null);

        assertEquals("etl", props.getProperty("user"));
        assertEquals(" secret ", props.getProperty("password"), "비밀번호는 다듬지 않는다");
        assertEquals("alice", props.getProperty("sessionUser"));
        assertEquals("argus-flow", props.getProperty("source"));
        assertEquals("etl,nightly", props.getProperty("clientTags"));
        assertEquals("hive:admin", props.getProperty("roles"));
        assertEquals("k:v", props.getProperty("extraCredentials"));
        assertEquals("Asia/Seoul", props.getProperty("timezone"));
        assertEquals("json+zstd", props.getProperty("encoding"));
        assertEquals("false", props.getProperty("explicitPrepare"));
        assertEquals("true", props.getProperty("SSL"));
        assertEquals("CA", props.getProperty("SSLVerification"));
        assertEquals("/etc/ts.p12", props.getProperty("SSLTrustStorePath"));
        assertEquals("tspw", props.getProperty("SSLTrustStorePassword"));
        assertEquals("PKCS12", props.getProperty("SSLTrustStoreType"));
        assertEquals("/etc/ks.p12", props.getProperty("SSLKeyStorePath"));
        assertEquals("kspw", props.getProperty("SSLKeyStorePassword"));
        assertEquals("PKCS12", props.getProperty("SSLKeyStoreType"));
        assertEquals("query_max_run_time:2h;iceberg.compression_codec:ZSTD", props.getProperty("sessionProperties"));
        assertEquals("nifi", props.getProperty("clientInfo"));
        assertNull(props.getProperty("accessToken"));
    }

    @Test
    void noTlsMeansNoSslParameters() {
        final Properties props = base().toDriverProperties(null);
        assertNull(props.getProperty("SSL"));
        assertNull(props.getProperty("SSLVerification"));
        assertNull(props.getProperty("sessionProperties"));
        assertEquals("true", props.getProperty("explicitPrepare"));
    }

    @Test
    void sslFromUrlStillSendsVerification() {
        final TrinoConnectionSettings settings = new TrinoConnectionSettings().url("jdbc:trino://trino:443?SSL=true").user("u");
        assertTrue(settings.isTlsEnabled());
        assertEquals("FULL", settings.toDriverProperties(null).getProperty("SSLVerification"));
        assertTrue(settings.validate().isEmpty(), settings.validate().toString());
    }

    @Test
    void tokensPerStrategy() {
        assertEquals("jwt", base().authenticationStrategy(AuthenticationStrategy.JWT).accessToken("jwt").tls(TLS)
                .toDriverProperties("ignored").getProperty("accessToken"));
        assertEquals("oauth", base().authenticationStrategy(AuthenticationStrategy.OAUTH2).tls(TLS)
                .toDriverProperties("oauth").getProperty("accessToken"));
        assertNull(base().authenticationStrategy(AuthenticationStrategy.NONE).accessToken("jwt")
                .toDriverProperties("oauth").getProperty("accessToken"));
    }

    @Test
    void kerberosParameters() {
        final TrinoConnectionSettings settings = base().authenticationStrategy(AuthenticationStrategy.KERBEROS).tls(TLS)
                .kerberosRemoteServiceName("trino")
                .kerberosPrincipal("nifi@EXAMPLE.COM")
                .kerberosKeytabPath("/etc/nifi.keytab")
                .kerberosConfigPath("/etc/krb5.conf")
                .kerberosServicePrincipalPattern("${SERVICE}@${HOST}")
                .kerberosUseCanonicalHostname(false);
        final Properties props = settings.toDriverProperties(null);
        assertEquals("trino", props.getProperty("KerberosRemoteServiceName"));
        assertEquals("nifi@EXAMPLE.COM", props.getProperty("KerberosPrincipal"));
        assertEquals("/etc/nifi.keytab", props.getProperty("KerberosKeytabPath"));
        assertEquals("/etc/krb5.conf", props.getProperty("KerberosConfigPath"));
        assertEquals("${SERVICE}@${HOST}", props.getProperty("KerberosServicePrincipalPattern"));
        assertEquals("false", props.getProperty("KerberosUseCanonicalHostname"));
        assertTrue(settings.validate().isEmpty(), settings.validate().toString());

        assertHasError(base().authenticationStrategy(AuthenticationStrategy.KERBEROS).tls(TLS).kerberosKeytabPath("/k"),
                "Kerberos Remote Service Name");
        assertHasError(base().authenticationStrategy(AuthenticationStrategy.KERBEROS).tls(TLS)
                .kerberosRemoteServiceName("trino").kerberosKeytabPath("/k"), "Kerberos Principal");
    }

    @Test
    void credentialAuthenticationRequiresTls() {
        for (final AuthenticationStrategy strategy : List.of(AuthenticationStrategy.PASSWORD, AuthenticationStrategy.JWT,
                AuthenticationStrategy.KERBEROS, AuthenticationStrategy.OAUTH2)) {
            final TrinoConnectionSettings settings = base().authenticationStrategy(strategy).password("p").accessToken("t")
                    .oauth2ProviderConfigured(true).kerberosRemoteServiceName("trino");
            assertHasError(settings, "TLS");
            assertFalse(settings.tls(TLS).validate().stream().anyMatch(e -> e.contains("TLS")), strategy.name());
        }
    }

    @Test
    void urlAndPropertiesMustNotOverlap() {
        assertHasError(base().url("jdbc:trino://trino:8080?user=etl"), "양쪽");
        assertHasError(base().url("jdbc:trino://trino:8080?source=x"), "양쪽");
        assertHasError(base().url("jdbc:trino://trino:8080?SSL=true&SSLVerification=NONE"), "양쪽");
        assertHasError(base().url("jdbc:trino://trino:8080?sessionProperties=a:b").dynamicProperty("session.a", "b"), "양쪽");
        // 서비스가 설정하지 않는 키는 URL 에 있어도 된다
        assertTrue(base().url("jdbc:trino://trino:8080?clientInfo=a&disableCompression=true").validate().isEmpty());
    }

    @Test
    void credentialsMustNotAppearInUrl() {
        for (final String key : List.of("password", "accessToken", "extraCredentials", "SSLTrustStorePassword", "SSLKeyStorePassword")) {
            assertHasError(base().url("jdbc:trino://trino:8080?" + key + "=x"), "자격 증명");
        }
    }

    @Test
    void forbiddenAndManagedDynamicProperties() {
        assertHasError(base().dynamicProperty("user", "x"), "서비스 전용");
        assertHasError(base().dynamicProperty("sessionProperties", "a:b"), "서비스 전용");
        assertHasError(base().dynamicProperty("catalog", "hive"), "URL 경로");
        assertHasError(base().dynamicProperty("externalAuthentication", "true"), "브라우저");
        assertHasError(base().url("jdbc:trino://trino:8080?schema=x"), "URL 경로");
        assertHasError(base().url("jdbc:trino://trino:8080?externalAuthentication=true"), "브라우저");
    }

    @Test
    void sessionPropertyTokensAreValidated() {
        assertHasError(base().dynamicProperty("session.a", "x;y"), "세션 프로퍼티 값");
        assertHasError(base().dynamicProperty("session.a", "x:y"), "세션 프로퍼티 값");
        assertHasError(base().dynamicProperty("session.a", "x y"), "세션 프로퍼티 값");
        assertHasError(base().dynamicProperty("session.", "x"), "세션 프로퍼티 이름");
        assertHasError(base().dynamicProperty("session.a;b", "x"), "세션 프로퍼티 이름");
        assertTrue(base().dynamicProperty("session.query_max_run_time", "2h").validate().isEmpty());
    }

    @Test
    void otherValidation() {
        assertHasError(base().user(null), "User");
        assertHasError(base().timeZone("Mars/Base"), "Time Zone");
        assertHasError(base().defaultSchema("raw"), "Default Schema");
        assertHasError(base().url("jdbc:mysql://x"), "Trino URL");
        assertHasError(base().authenticationStrategy(AuthenticationStrategy.PASSWORD).tls(TLS), "Password");
        assertHasError(base().authenticationStrategy(AuthenticationStrategy.OAUTH2).tls(TLS), "OAuth2");
        assertTrue(base().authenticationStrategy(AuthenticationStrategy.JWT).tls(TLS).accessToken("t").user(null).validate().isEmpty());
    }

    @Test
    void toStringHidesSecrets() {
        final String text = base().authenticationStrategy(AuthenticationStrategy.PASSWORD).password("s3cr3t").accessToken("t0ken")
                .extraCredentials("x:c4red").tls(new TrinoConnectionSettings.TlsSettings("/ts", "tsPw9", "JKS", "/ks", "ksPw9", "JKS"))
                .toString();
        for (final String secret : List.of("s3cr3t", "t0ken", "c4red", "tsPw9", "ksPw9")) {
            assertFalse(text.contains(secret), text);
        }
    }

    private static void assertHasError(final TrinoConnectionSettings settings, final String fragment) {
        final List<String> errors = settings.validate();
        assertTrue(errors.stream().anyMatch(e -> e.contains(fragment)), "expected '" + fragment + "' in " + errors);
    }
}
