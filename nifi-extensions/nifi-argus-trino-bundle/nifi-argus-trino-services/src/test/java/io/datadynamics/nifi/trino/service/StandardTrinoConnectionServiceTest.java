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

import io.trino.jdbc.NonRegisteringTrinoDriver;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StandardTrinoConnectionServiceTest {

    private TestRunner runner;
    private RecordingService service;

    /** 서버에 접속하지 않고 드라이버에 넘긴 URL·Properties 를 기록한다(드라이버 connect 는 네트워크를 쓰지 않는다). */
    static class RecordingService extends StandardTrinoConnectionService {
        final List<String> urls = new ArrayList<>();
        final List<Properties> properties = new ArrayList<>();
        final AtomicInteger closedDrivers = new AtomicInteger();

        @Override
        protected NonRegisteringTrinoDriver createDriver() {
            return new NonRegisteringTrinoDriver() {
                @Override
                public Connection connect(final String url, final Properties info) throws SQLException {
                    urls.add(url);
                    properties.add(info);
                    return super.connect(url, info);
                }

                @Override
                public void close() throws java.io.IOException {
                    closedDrivers.incrementAndGet();
                    super.close();
                }
            };
        }
    }

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new RecordingService();
        runner.addControllerService("trino", service);
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:trino://trino.example.com:8080");
        runner.setProperty(service, StandardTrinoConnectionService.USER, "etl");
    }

    @Test
    void minimalConfigurationIsValidAndBuildsConnection() throws Exception {
        runner.setProperty(service, StandardTrinoConnectionService.DEFAULT_CATALOG, "tpch");
        runner.setProperty(service, StandardTrinoConnectionService.DEFAULT_SCHEMA, "tiny");
        runner.setProperty(service, "session.query_max_run_time", "2h");
        runner.setProperty(service, "clientInfo", "nifi");
        runner.assertValid(service);
        runner.enableControllerService(service);

        try (Connection connection = service.getConnection()) {
            assertNotNull(connection);
            assertTrue(connection.isWrapperFor(io.trino.jdbc.TrinoConnection.class));
        }
        assertEquals("jdbc:trino://trino.example.com:8080/tpch/tiny", service.getJdbcUrl());
        assertEquals("jdbc:trino://trino.example.com:8080/tpch/tiny", service.urls.get(0));
        final Properties props = service.properties.get(0);
        assertEquals("etl", props.getProperty("user"));
        assertEquals("argus-flow", props.getProperty("source"));
        assertEquals("query_max_run_time:2h", props.getProperty("sessionProperties"));
        assertEquals("nifi", props.getProperty("clientInfo"));
        assertNull(props.getProperty("validateConnection"));

        runner.disableControllerService(service);
        assertEquals(1, service.closedDrivers.get());
    }

    @Test
    void passwordRequiresTls(@TempDir final Path dir) throws Exception {
        final Path trustStore = dir.resolve("truststore.p12");
        final Process keytool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "test", "-keyalg", "RSA", "-keysize", "2048", "-dname", "CN=trino-test",
                "-validity", "2", "-storetype", "PKCS12", "-keystore", trustStore.toString(), "-storepass", "tspw123")
                .redirectErrorStream(true).start();
        assertEquals(0, keytool.waitFor());

        runner.setProperty(service, StandardTrinoConnectionService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.PASSWORD.getValue());
        runner.setProperty(service, StandardTrinoConnectionService.PASSWORD, "pw");
        runner.assertNotValid(service);

        final SSLContextService ssl = mock(SSLContextService.class);
        when(ssl.getIdentifier()).thenReturn("ssl");
        when(ssl.isTrustStoreConfigured()).thenReturn(true);
        when(ssl.getTrustStoreFile()).thenReturn(trustStore.toString());
        when(ssl.getTrustStorePassword()).thenReturn("tspw123");
        when(ssl.getTrustStoreType()).thenReturn("PKCS12");
        runner.addControllerService("ssl", ssl);
        runner.enableControllerService(ssl);
        runner.setProperty(service, StandardTrinoConnectionService.SSL_CONTEXT_SERVICE, "ssl");
        runner.assertValid(service);
        runner.enableControllerService(service);

        service.getConnection().close();
        final Properties props = service.properties.get(0);
        assertEquals("pw", props.getProperty("password"));
        assertEquals("true", props.getProperty("SSL"));
        assertEquals("FULL", props.getProperty("SSLVerification"));
        assertEquals(trustStore.toString(), props.getProperty("SSLTrustStorePath"));
        assertEquals("tspw123", props.getProperty("SSLTrustStorePassword"));
        assertEquals("PKCS12", props.getProperty("SSLTrustStoreType"));
        assertNull(props.getProperty("SSLKeyStorePath"));
        assertFalse(service.getJdbcUrl().contains("pw"));
    }

    @Test
    void oauth2TokenIsFetchedPerConnection() throws Exception {
        final OAuth2AccessTokenProvider provider = mock(OAuth2AccessTokenProvider.class);
        when(provider.getIdentifier()).thenReturn("oauth");
        final AtomicInteger calls = new AtomicInteger();
        when(provider.getAccessDetails()).thenAnswer(invocation -> {
            final AccessToken token = new AccessToken();
            token.setAccessToken("token-" + calls.incrementAndGet());
            return token;
        });
        runner.addControllerService("oauth", provider);
        runner.enableControllerService(provider);

        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:trino://trino.example.com?SSL=true");
        runner.setProperty(service, StandardTrinoConnectionService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2.getValue());
        runner.assertNotValid(service);
        runner.setProperty(service, StandardTrinoConnectionService.OAUTH2_ACCESS_TOKEN_PROVIDER, "oauth");
        runner.assertValid(service);
        runner.enableControllerService(service);

        service.getConnection().close();
        service.getConnection().close();
        assertEquals("token-1", service.properties.get(0).getProperty("accessToken"));
        assertEquals("token-2", service.properties.get(1).getProperty("accessToken"), "만료·갱신된 토큰을 연결마다 다시 받는다");
        assertEquals(2, calls.get());
    }

    @Test
    void kerberosConfiguration(@TempDir final Path dir) throws Exception {
        final Path keytab = Files.createFile(dir.resolve("nifi.keytab"));
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:trino://trino.example.com:8443?SSL=true");
        runner.setProperty(service, StandardTrinoConnectionService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.KERBEROS.getValue());
        runner.assertNotValid(service);
        runner.setProperty(service, StandardTrinoConnectionService.KERBEROS_REMOTE_SERVICE_NAME, "trino");
        runner.setProperty(service, StandardTrinoConnectionService.KERBEROS_PRINCIPAL, "nifi@EXAMPLE.COM");
        runner.setProperty(service, StandardTrinoConnectionService.KERBEROS_KEYTAB, dir.resolve("missing.keytab").toString());
        runner.assertNotValid(service);
        runner.setProperty(service, StandardTrinoConnectionService.KERBEROS_KEYTAB, keytab.toString());
        runner.assertValid(service);
        runner.enableControllerService(service);

        service.getConnection().close();
        final Properties props = service.properties.get(0);
        assertEquals("trino", props.getProperty("KerberosRemoteServiceName"));
        assertEquals("nifi@EXAMPLE.COM", props.getProperty("KerberosPrincipal"));
        assertEquals(keytab.toString(), props.getProperty("KerberosKeytabPath"));
        assertEquals("${SERVICE}@${HOST}", props.getProperty("KerberosServicePrincipalPattern"));
        assertEquals("true", props.getProperty("KerberosUseCanonicalHostname"));
    }

    @Test
    void invalidConfigurations() {
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:trino://trino:8080?user=etl");
        runner.assertNotValid(service);
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:trino://trino:8080?password=x");
        runner.assertNotValid(service);
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:trino://trino:8080");
        runner.assertValid(service);
        runner.setProperty(service, "session.bad", "a;b");
        runner.assertNotValid(service);
        runner.removeProperty(service, "session.bad");
        runner.setProperty(service, "externalAuthentication", "true");
        runner.assertNotValid(service);
        runner.removeProperty(service, "externalAuthentication");
        runner.setProperty(service, StandardTrinoConnectionService.DEFAULT_SCHEMA, "raw");
        runner.assertNotValid(service);
    }
}
