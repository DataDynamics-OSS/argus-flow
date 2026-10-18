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

import io.datadynamics.nifi.trino.api.TrinoConnectionService;
import io.datadynamics.nifi.trino.api.TrinoQueryInfo;
import io.trino.jdbc.NonRegisteringTrinoDriver;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;

import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Trino JDBC 드라이버 하나를 보유하고 {@link #getConnection()}마다 새 연결을 만드는 서비스(풀 없음).
 * <p>
 * 기준 문서: <a href="https://docs.starburst.io/latest/client/jdbc.html">Starburst JDBC driver (Trino JDBC 483)</a>.
 */
@Tags({"trino", "jdbc", "database", "connection", "sql", "lakehouse", "starburst"})
@CapabilityDescription("Trino JDBC 드라이버(483)로 연결을 제공한다. 연결 풀을 두지 않고 getConnection() 마다 새 연결을 만든다"
        + "(드라이버가 HTTP 클라이언트를 공유하므로 비용이 작다). DBCPService 를 구현하므로 ExecuteSQL 등 범용 SQL 프로세서에도 쓸 수 있다. "
        + "주의: 드라이버 버전은 클러스터 버전과 같거나 높아야 하며(350 미만 클러스터 미지원), spooling protocol 이 켜진 클러스터에서는 "
        + "NiFi JVM 이 spooling 객체 스토리지에 네트워크로 접근할 수 있어야 한다. 압축 해제 성능은 Java 22+ 에서 더 좋다(NiFi 2.10 은 Java 21). "
        + "메타데이터 조회에는 system.jdbc 스키마 조회 권한이 필요하다. 브라우저 기반 externalAuthentication 은 지원하지 않는다. "
        + "Database Driver Location(s) 로 Starburst JDBC 등 Trino 호환 외부 드라이버를 child-first 클래스로더로 쓸 수 있다(파라미터 이름은 Trino 드라이버 기준). "
        + "외부 드라이버에 Trino 진행 통계 API(getStats/setProgressMonitor)가 없으면 query id·통계만 생략된다.")
@SupportsSensitiveDynamicProperties
@DynamicProperties({
        @DynamicProperty(name = "session.<세션 프로퍼티 이름>", value = "세션 프로퍼티 값",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "Trino 세션 프로퍼티로 전달한다(sessionProperties=name:value;...). 예: session.query_max_run_time=2h, "
                        + "session.iceberg.compression_codec=ZSTD. 이름·값에 공백·';'·':' 를 쓸 수 없다."),
        @DynamicProperty(name = "<드라이버 파라미터 이름>", value = "파라미터 값",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "session. 으로 시작하지 않는 동적 프로퍼티는 그대로 JDBC 드라이버 파라미터로 전달한다"
                        + "(예: clientInfo, applicationNamePrefix, extraHeaders, disableCompression, hostnameInCertificate, "
                        + "SSLUseSystemTrustStore, socksProxy, httpProxy). 전용 프로퍼티가 있는 파라미터와 catalog/schema/externalAuthentication 은 지정할 수 없다.")
})
public class StandardTrinoConnectionService extends AbstractControllerService
        implements TrinoConnectionService, VerifiableControllerService {

    static final AllowableValue SSL_VERIFICATION_FULL = new AllowableValue("FULL", "FULL", "인증서 체인과 호스트 이름을 모두 검증한다(기본).");
    static final AllowableValue SSL_VERIFICATION_CA = new AllowableValue("CA", "CA", "인증서 체인만 검증하고 호스트 이름은 검증하지 않는다.");
    static final AllowableValue SSL_VERIFICATION_NONE = new AllowableValue("NONE", "NONE", "검증하지 않는다(테스트 전용, 운영 금지).");

    static final AllowableValue ENCODING_ZSTD = new AllowableValue("json+zstd", "json+zstd", "spooling 세그먼트를 zstd 로 압축(권장).");
    static final AllowableValue ENCODING_LZ4 = new AllowableValue("json+lz4", "json+lz4", "spooling 세그먼트를 lz4 로 압축.");
    static final AllowableValue ENCODING_JSON = new AllowableValue("json", "json", "spooling 세그먼트를 압축하지 않는다.");

    static final PropertyDescriptor TRINO_URL = new PropertyDescriptor.Builder()
            .name("Trino URL")
            .displayName("Trino URL")
            .description("jdbc:trino://host[:port][/catalog[/schema]] 형식. 포트를 생략하면 HTTP 80, SSL 이면 443. "
                    + "catalog/schema 는 URL 경로로만 지정한다(경로가 없으면 Default Catalog/Schema 로 경로를 만든다). "
                    + "URL 쿼리 파라미터는 서비스 프로퍼티와 겹치면 안 되며, 자격 증명(password, accessToken 등)은 넣을 수 없다. "
                    + "외부 드라이버(Database Driver Location(s) 지정 등)를 쓰면 jdbc: 접두사와 드라이버의 acceptsURL 로 형식을 판단한다.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .displayName("인증 방식")
            .description("Trino 인증 방식. NONE 외에는 TLS 가 필수다.")
            .required(true)
            .allowableValues(AuthenticationStrategy.class)
            .defaultValue(AuthenticationStrategy.NONE)
            .build();

    static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User")
            .displayName("사용자")
            .description("Trino 사용자(user). NONE·PASSWORD 에서는 필수. JWT·OAUTH2·KERBEROS 에서 비우면 토큰/principal 의 사용자를 쓴다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .displayName("비밀번호")
            .description("PASSWORD 인증의 비밀번호(password).")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.PASSWORD)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .displayName("Access Token")
            .description("JWT 인증의 access token(accessToken). 만료되는 토큰이면 OAUTH2 방식을 쓴다.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.JWT)
            .build();

    static final PropertyDescriptor OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("OAuth2 Access Token Provider")
            .displayName("OAuth2 Access Token Provider")
            .description("연결마다 토큰을 받아 accessToken 으로 전달할 NiFi OAuth2 토큰 제공 서비스. 만료 시 갱신은 제공 서비스가 맡는다.")
            .required(true)
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2)
            .build();

    static final PropertyDescriptor KERBEROS_REMOTE_SERVICE_NAME = new PropertyDescriptor.Builder()
            .name("Kerberos Remote Service Name")
            .displayName("Kerberos 원격 서비스 이름")
            .description("Trino coordinator 의 Kerberos 서비스 이름(KerberosRemoteServiceName). 예: trino")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KERBEROS)
            .build();

    static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("Kerberos Principal")
            .displayName("Kerberos Principal")
            .description("클라이언트 principal(KerberosPrincipal). keytab 을 쓰면 필수.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KERBEROS)
            .build();

    static final PropertyDescriptor KERBEROS_KEYTAB = new PropertyDescriptor.Builder()
            .name("Kerberos Keytab")
            .displayName("Kerberos Keytab")
            .description("클라이언트 keytab 파일 경로(KerberosKeytabPath). NiFi 노드마다 같은 경로에 있어야 한다.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KERBEROS)
            .build();

    static final PropertyDescriptor KERBEROS_CONFIG_FILE = new PropertyDescriptor.Builder()
            .name("Kerberos Config File")
            .displayName("Kerberos 설정 파일")
            .description("krb5.conf 경로(KerberosConfigPath). 비우면 JVM 기본 설정을 쓴다.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KERBEROS)
            .build();

    static final PropertyDescriptor KERBEROS_SERVICE_PRINCIPAL_PATTERN = new PropertyDescriptor.Builder()
            .name("Kerberos Service Principal Pattern")
            .displayName("Kerberos 서비스 Principal 패턴")
            .description("서버 principal 패턴(KerberosServicePrincipalPattern). ${SERVICE}·${HOST} 는 NiFi 표현식이 아니라 드라이버가 치환하는 자리표시자다.")
            .required(true)
            .defaultValue("${SERVICE}@${HOST}")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KERBEROS)
            .build();

    static final PropertyDescriptor KERBEROS_USE_CANONICAL_HOSTNAME = new PropertyDescriptor.Builder()
            .name("Kerberos Use Canonical Hostname")
            .displayName("Kerberos 정규 호스트 이름 사용")
            .description("서버 principal 을 만들 때 호스트 이름을 DNS 정규 이름으로 바꾼다(KerberosUseCanonicalHostname).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KERBEROS)
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("SSL Context Service")
            .description("설정하면 SSL=true 로 연결하고 truststore(SSLTrustStorePath/Password/Type)와, 있으면 keystore"
                    + "(SSLKeyStorePath/Password/Type)를 드라이버에 전달한다. 시스템 truststore 만 쓰려면 비우고 URL 에 SSL=true 를 지정한다.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    static final PropertyDescriptor SSL_VERIFICATION = new PropertyDescriptor.Builder()
            .name("SSL Verification")
            .displayName("SSL 검증 수준")
            .description("TLS 연결의 인증서 검증 수준(SSLVerification). TLS 를 쓸 때만 전달한다.")
            .required(true)
            .allowableValues(SSL_VERIFICATION_FULL, SSL_VERIFICATION_CA, SSL_VERIFICATION_NONE)
            .defaultValue(SSL_VERIFICATION_FULL.getValue())
            .build();

    static final PropertyDescriptor SESSION_USER = new PropertyDescriptor.Builder()
            .name("Session User")
            .displayName("세션 사용자")
            .description("인증 사용자 대신 쿼리를 실행할 사용자(sessionUser, impersonation). Trino 접근 제어에서 허용돼 있어야 한다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor SOURCE = new PropertyDescriptor.Builder()
            .name("Source")
            .displayName("Source")
            .description("쿼리 source(source). Trino UI·system.runtime.queries·resource group 선택에 쓰인다. applicationNamePrefix 보다 우선한다.")
            .required(false)
            .defaultValue("argus-flow")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor CLIENT_TAGS = new PropertyDescriptor.Builder()
            .name("Client Tags")
            .displayName("Client Tags")
            .description("쉼표로 구분한 클라이언트 태그(clientTags). resource group 라우팅에 쓰인다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor ROLES = new PropertyDescriptor.Builder()
            .name("Roles")
            .displayName("Roles")
            .description("카탈로그별 역할(roles). 형식: catalog1:role1;catalog2:role2")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor EXTRA_CREDENTIALS = new PropertyDescriptor.Builder()
            .name("Extra Credentials")
            .displayName("추가 자격 증명")
            .description("커넥터로 전달할 추가 자격 증명(extraCredentials). 형식: key1:value1;key2:value2")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DEFAULT_CATALOG = new PropertyDescriptor.Builder()
            .name("Default Catalog")
            .displayName("기본 카탈로그")
            .description("URL 경로에 catalog 가 없을 때 URL 경로(/catalog)로 붙인다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor DEFAULT_SCHEMA = new PropertyDescriptor.Builder()
            .name("Default Schema")
            .displayName("기본 스키마")
            .description("URL 경로에 schema 가 없을 때 URL 경로(/catalog/schema)로 붙인다. catalog 가 URL 이나 Default Catalog 로 정해져 있어야 한다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor TIME_ZONE = new PropertyDescriptor.Builder()
            .name("Time Zone")
            .displayName("세션 시간대")
            .description("세션 시간대(timezone). Java ZoneId 형식(예: Asia/Seoul, UTC). 비우면 NiFi JVM 기본 시간대.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor SPOOLING_ENCODING = new PropertyDescriptor.Builder()
            .name("Spooling Encoding")
            .displayName("Spooling 인코딩")
            .description("spooling protocol 의 세그먼트 인코딩(encoding). 비우면 클러스터 기본값. json+zstd 권장. "
                    + "spooling 이 켜진 클러스터에서는 NiFi 가 spooling 객체 스토리지에 직접 접근할 수 있어야 한다.")
            .required(false)
            .allowableValues(ENCODING_ZSTD, ENCODING_LZ4, ENCODING_JSON)
            .build();

    static final PropertyDescriptor EXPLICIT_PREPARE = new PropertyDescriptor.Builder()
            .name("Explicit Prepare")
            .displayName("명시적 PREPARE")
            .description("PreparedStatement 를 PREPARE 문으로 따로 준비한다(explicitPrepare). false 면 EXECUTE IMMEDIATE 를 써 왕복이 줄지만 "
                    + "Trino 431 이상 클러스터가 필요하다.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("Database Driver Location(s)")
            .displayName("드라이버 위치(들)")
            .description("외부 JDBC 드라이버 jar(와 의존 jar)의 파일/디렉터리 경로 또는 URL 쉼표 목록. Starburst JDBC 드라이버 등 Trino 호환 드라이버를 쓸 때 지정한다. "
                    + "비우면 번들된 Trino JDBC 483 을 쓴다. 지정한 jar 는 서비스 전용 child-first 클래스로더로 로드하므로 번들 드라이버와 패키지가 같아도 외부 jar 가 우선한다. "
                    + "드라이버 파라미터(user, password, SSL*, sessionProperties 등)는 Trino 드라이버 이름 기준으로 전달된다.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor DB_DRIVER_CLASS_NAME = new PropertyDescriptor.Builder()
            .name("Database Driver Class Name")
            .displayName("드라이버 클래스 이름")
            .description("java.sql.Driver 구현 클래스. 기본값은 번들 드라이버의 io.trino.jdbc.NonRegisteringTrinoDriver 이며, 문서상 이름 io.trino.jdbc.TrinoDriver 도 쓸 수 있다"
                    + "(TrinoDriver 는 DriverManager 전역 등록을 피하려고 같은 jar 의 NonRegisteringTrinoDriver 로 인스턴스화한다). "
                    + "외부 드라이버를 쓰는 경우 해당 드라이버의 클래스명(예: io.trino.jdbc.TrinoDriver)을 지정한다. "
                    + "참고: https://docs.starburst.io/latest/client/jdbc.html")
            .required(true)
            .defaultValue(DriverHandle.NON_REGISTERING_TRINO_DRIVER)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            TRINO_URL, DB_DRIVER_LOCATION, DB_DRIVER_CLASS_NAME, AUTHENTICATION_STRATEGY, USER, PASSWORD, ACCESS_TOKEN, OAUTH2_ACCESS_TOKEN_PROVIDER,
            KERBEROS_REMOTE_SERVICE_NAME, KERBEROS_PRINCIPAL, KERBEROS_KEYTAB, KERBEROS_CONFIG_FILE,
            KERBEROS_SERVICE_PRINCIPAL_PATTERN, KERBEROS_USE_CANONICAL_HOSTNAME,
            SSL_CONTEXT_SERVICE, SSL_VERIFICATION, SESSION_USER, SOURCE, CLIENT_TAGS, ROLES, EXTRA_CREDENTIALS,
            DEFAULT_CATALOG, DEFAULT_SCHEMA, TIME_ZONE, SPOOLING_ENCODING, EXPLICIT_PREPARE);

    /** 검증 단계에서 SSL Context Service 의 실제 저장소 대신 쓰는 자리표시자(키 집합 계산용). */
    private static final TrinoConnectionSettings.TlsSettings TLS_PLACEHOLDER =
            new TrinoConnectionSettings.TlsSettings("t", "t", "t", "k", "k", "k");

    private volatile QueryStatsTracker statsTracker;
    private volatile DriverHandle driverHandle;
    private volatile String driverCheckKey;
    private volatile String driverCheckError;
    private volatile TrinoConnectionSettings settings;
    private volatile String jdbcUrl;
    private volatile OAuth2AccessTokenProvider oauth2Provider;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .description(propertyDescriptorName.startsWith(TrinoConnectionSettings.SESSION_PREFIX)
                        ? "Trino 세션 프로퍼티" : "Trino JDBC 드라이버 파라미터")
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final TrinoConnectionSettings candidate = buildSettings(context, context.getProperties(),
                context.getProperty(SSL_CONTEXT_SERVICE).isSet() ? TLS_PLACEHOLDER : null);
        final List<ValidationResult> results = new ArrayList<>();
        final List<String> errors = candidate.validate();
        for (final String error : errors) {
            results.add(new ValidationResult.Builder().subject("Trino 연결 설정").valid(false).explanation(error).build());
        }
        if (errors.isEmpty() && !isBundledDriver(context)) {
            final String driverError = checkExternalDriver(context, candidate);
            if (driverError != null) {
                results.add(new ValidationResult.Builder().subject(DB_DRIVER_CLASS_NAME.getDisplayName()).valid(false)
                        .explanation(driverError).build());
            }
        }
        return results;
    }

    /**
     * 외부(또는 번들이 아닌) 드라이버를 임시로 로드해 클래스·Driver 구현·acceptsURL 을 확인한다. 같은 설정이면 결과를 재사용한다.
     */
    private String checkExternalDriver(final PropertyContext context, final TrinoConnectionSettings candidate) {
        final String url = candidate.effectiveUrl().toUrl();
        final List<URL> locations = driverLocations(context);
        final String className = context.getProperty(DB_DRIVER_CLASS_NAME).getValue().trim();
        final String key = locations + "|" + className + "|" + url;
        if (key.equals(driverCheckKey)) {
            return driverCheckError;
        }
        String error = null;
        try (DriverHandle handle = DriverHandle.load(className, locations, getClass().getClassLoader())) {
            handle.requireAcceptsUrl(url);
        } catch (final DriverHandle.DriverLoadException e) {
            error = e.getMessage();
        }
        driverCheckError = error;
        driverCheckKey = key;
        return error;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final TrinoConnectionSettings resolved = buildSettings(context, context.getProperties(), resolveTls(context));
        this.settings = resolved;
        this.jdbcUrl = resolved.effectiveUrl().toUrl();
        this.oauth2Provider = resolved.getAuthenticationStrategy() == AuthenticationStrategy.OAUTH2
                ? context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class)
                : null;
        final DriverHandle handle;
        try {
            handle = openDriver(context);
            handle.requireAcceptsUrl(jdbcUrl);
        } catch (final DriverHandle.DriverLoadException e) {
            throw new InitializationException("JDBC 드라이버를 준비할 수 없다: " + e.getMessage(), e);
        }
        this.driverHandle = handle;
        this.statsTracker = new QueryStatsTracker(handle.driver().getClass().getClassLoader(), getLogger());
        getLogger().info("Trino 연결 서비스 활성화: url={}, auth={}, driver={}{}", jdbcUrl, resolved.getAuthenticationStrategy(),
                handle.driver().getClass().getName(), handle.isExternal() ? " (외부 jar, child-first)" : "");
    }

    @OnDisabled
    public void onDisabled() {
        final DriverHandle current = driverHandle;
        driverHandle = null;
        settings = null;
        oauth2Provider = null;
        statsTracker = null;
        if (current != null) {
            current.close();
        }
    }

    @Override
    public Connection getConnection() throws ProcessException {
        final DriverHandle currentHandle = driverHandle;
        final TrinoConnectionSettings currentSettings = settings;
        final QueryStatsTracker tracker = statsTracker;
        if (currentHandle == null || currentSettings == null || tracker == null) {
            throw new ProcessException("Trino 연결 서비스가 활성화되지 않았다");
        }
        final Properties properties = currentSettings.toDriverProperties(currentAccessToken(oauth2Provider));
        try {
            final Connection connection = currentHandle.driver().connect(jdbcUrl, properties);
            if (connection == null) {
                throw new ProcessException("Trino 드라이버가 URL 을 받아들이지 않았다: " + jdbcUrl);
            }
            return tracker.wrap(connection);
        } catch (final SQLException e) {
            throw new ProcessException("Trino 연결 실패 (" + jdbcUrl + "): " + e.getMessage(), e);
        }
    }

    /** 테스트용: 현재 드라이버 핸들. */
    DriverHandle currentDriverHandle() {
        return driverHandle;
    }

    @Override
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public TrinoQueryInfo getQueryInfo(final ResultSet resultSet) {
        final QueryStatsTracker tracker = statsTracker;
        return tracker == null ? null : tracker.getQueryInfo(resultSet);
    }

    @Override
    public TrinoQueryInfo getQueryInfo(final Statement statement) {
        final QueryStatsTracker tracker = statsTracker;
        return tracker == null ? null : tracker.getQueryInfo(statement);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final TrinoConnectionSettings candidate;
        final String url;
        final Properties properties;
        try {
            candidate = buildSettings(context, context.getProperties(), resolveTls(context));
            final List<String> errors = candidate.validate();
            if (!errors.isEmpty()) {
                throw new IllegalArgumentException(String.join("; ", errors));
            }
            url = candidate.effectiveUrl().toUrl();
            final OAuth2AccessTokenProvider provider = candidate.getAuthenticationStrategy() == AuthenticationStrategy.OAUTH2
                    ? context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class)
                    : null;
            properties = candidate.toDriverProperties(currentAccessToken(provider));
            // validateConnection 은 X-Trino-User 헤더 없이 HEAD /v1/statement 를 보내므로 인증이 없는(NONE) 클러스터는
            // 항상 401 을 돌려준다(483 에서 확인). NONE 에서는 SELECT 1 자체로 연결을 확인한다.
            if (candidate.getAuthenticationStrategy() != AuthenticationStrategy.NONE) {
                properties.setProperty(TrinoDriverProperties.VALIDATE_CONNECTION, "true");
            }
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("드라이버 프로퍼티 조립")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("URL " + url + ", 파라미터 " + properties.stringPropertyNames().stream().sorted().toList())
                    .build());
        } catch (final RuntimeException e) {
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("드라이버 프로퍼티 조립")
                    .outcome(Outcome.FAILED)
                    .explanation(e.getMessage())
                    .build());
            return results;
        }

        final DriverHandle verifyHandle;
        try {
            verifyHandle = openDriver(context);
            verifyHandle.requireAcceptsUrl(url);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("JDBC 드라이버 로드")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation(verifyHandle.driver().getClass().getName() + " " + verifyHandle.driver().getMajorVersion()
                            + (verifyHandle.isExternal() ? " (외부 jar, child-first 클래스로더)" : " (번들)"))
                    .build());
        } catch (final DriverHandle.DriverLoadException e) {
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("JDBC 드라이버 로드")
                    .outcome(Outcome.FAILED)
                    .explanation(e.getMessage())
                    .build());
            return results;
        }
        try (Connection connection = verifyHandle.driver().connect(url, properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT 1, current_user")) {
            resultSet.next();
            final String currentUser = resultSet.getString(2);
            final String version = connection.getMetaData().getDatabaseProductVersion();
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Trino 연결 및 SELECT 1")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Trino " + version + " 에 사용자 " + currentUser + " 로 연결했다")
                    .build());
        } catch (final Exception e) {
            verificationLogger.warn("Trino 연결 확인 실패", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Trino 연결 및 SELECT 1")
                    .outcome(Outcome.FAILED)
                    .explanation("연결 실패: " + e.getMessage())
                    .build());
        } finally {
            verifyHandle.close();
        }
        return results;
    }

    // ---- helpers ----

    /**
     * 테스트에서 드라이버 호출을 기록하려고 재정의할 수 있다.
     */
    protected NonRegisteringTrinoDriver createDriver() {
        return new ServiceTrinoDriver();
    }

    /**
     * 위치가 없고 Trino 드라이버 클래스명이면 번들 드라이버, 아니면 {@link DriverHandle#load}.
     */
    private DriverHandle openDriver(final PropertyContext context) {
        if (isBundledDriver(context)) {
            return DriverHandle.bundled(createDriver());
        }
        return DriverHandle.load(context.getProperty(DB_DRIVER_CLASS_NAME).getValue().trim(), driverLocations(context),
                getClass().getClassLoader());
    }

    static boolean isBundledDriver(final PropertyContext context) {
        return !context.getProperty(DB_DRIVER_LOCATION).isSet()
                && DriverHandle.TRINO_DRIVER_CLASSES.contains(context.getProperty(DB_DRIVER_CLASS_NAME).getValue().trim());
    }

    private static List<URL> driverLocations(final PropertyContext context) {
        if (!context.getProperty(DB_DRIVER_LOCATION).isSet()) {
            return List.of();
        }
        final ResourceReferences references = context.getProperty(DB_DRIVER_LOCATION).evaluateAttributeExpressions().asResources();
        return references == null ? List.of() : references.flatten().asURLs();
    }

    static TrinoConnectionSettings buildSettings(final PropertyContext context, final Map<PropertyDescriptor, String> allProperties,
                                                 final TrinoConnectionSettings.TlsSettings tls) {
        final TrinoConnectionSettings result = new TrinoConnectionSettings()
                .url(env(context, TRINO_URL))
                .trinoUrlFormat(isBundledDriver(context))
                .authenticationStrategy(context.getProperty(AUTHENTICATION_STRATEGY).asAllowableValue(AuthenticationStrategy.class))
                .user(env(context, USER))
                .password(context.getProperty(PASSWORD).getValue())
                .accessToken(context.getProperty(ACCESS_TOKEN).getValue())
                .oauth2ProviderConfigured(context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).isSet())
                .kerberosRemoteServiceName(env(context, KERBEROS_REMOTE_SERVICE_NAME))
                .kerberosPrincipal(env(context, KERBEROS_PRINCIPAL))
                .kerberosKeytabPath(env(context, KERBEROS_KEYTAB))
                .kerberosConfigPath(env(context, KERBEROS_CONFIG_FILE))
                .kerberosServicePrincipalPattern(context.getProperty(KERBEROS_SERVICE_PRINCIPAL_PATTERN).getValue())
                .kerberosUseCanonicalHostname(context.getProperty(KERBEROS_USE_CANONICAL_HOSTNAME).asBoolean())
                .tls(tls)
                .sslVerification(context.getProperty(SSL_VERIFICATION).getValue())
                .sessionUser(env(context, SESSION_USER))
                .source(env(context, SOURCE))
                .clientTags(env(context, CLIENT_TAGS))
                .roles(env(context, ROLES))
                .extraCredentials(context.getProperty(EXTRA_CREDENTIALS).getValue())
                .defaultCatalog(env(context, DEFAULT_CATALOG))
                .defaultSchema(env(context, DEFAULT_SCHEMA))
                .timeZone(env(context, TIME_ZONE))
                .encoding(context.getProperty(SPOOLING_ENCODING).getValue())
                .explicitPrepare(context.getProperty(EXPLICIT_PREPARE).asBoolean());
        for (final PropertyDescriptor descriptor : allProperties.keySet()) {
            if (descriptor.isDynamic()) {
                result.dynamicProperty(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
            }
        }
        return result;
    }

    private static TrinoConnectionSettings.TlsSettings resolveTls(final PropertyContext context) {
        if (!context.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            return null;
        }
        final SSLContextService ssl = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        return new TrinoConnectionSettings.TlsSettings(
                ssl.isTrustStoreConfigured() ? ssl.getTrustStoreFile() : null, ssl.getTrustStorePassword(), ssl.getTrustStoreType(),
                ssl.isKeyStoreConfigured() ? ssl.getKeyStoreFile() : null, ssl.getKeyStorePassword(), ssl.getKeyStoreType());
    }

    private static String currentAccessToken(final OAuth2AccessTokenProvider provider) {
        if (provider == null) {
            return null;
        }
        final AccessToken accessToken = provider.getAccessDetails();
        if (accessToken == null || accessToken.getAccessToken() == null || accessToken.getAccessToken().isEmpty()) {
            throw new ProcessException("OAuth2 Access Token Provider 가 토큰을 돌려주지 않았다");
        }
        return accessToken.getAccessToken();
    }

    private static String env(final PropertyContext context, final PropertyDescriptor descriptor) {
        return context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
    }

    /**
     * {@link io.trino.jdbc.TrinoDriver}는 static 초기화에서 {@code DriverManager.registerDriver}로 전역 등록하므로
     * NAR 클래스로더가 DriverManager 에 붙잡히고 닫히지 않는 드라이버 인스턴스가 하나 더 생긴다.
     * 동작이 같은 {@link NonRegisteringTrinoDriver}(TrinoDriver 의 부모, 생성자가 protected)를 서비스 수명에 맞춰 직접 만들고 닫는다.
     */
    static final class ServiceTrinoDriver extends NonRegisteringTrinoDriver {
    }
}
