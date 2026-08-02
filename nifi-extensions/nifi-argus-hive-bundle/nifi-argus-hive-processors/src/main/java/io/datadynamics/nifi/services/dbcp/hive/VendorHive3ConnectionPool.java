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
package io.datadynamics.nifi.services.dbcp.hive;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPValidator;
import io.datadynamics.nifi.services.api.hive.VendorHiveDBCPService;
import io.datadynamics.nifi.processors.hive.HiveConfigurator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import io.datadynamics.nifi.processors.hive.util.AuthenticationFailedException;
import io.datadynamics.nifi.processors.hive.util.ValidationResources;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cloudera Hive 3.x용 데이터베이스 커넥션 풀 컨트롤러 서비스.
 * Apache Commons DBCP2({@link BasicDataSource})를 이용해 커넥션 풀링을 구현하며,
 * 내부적으로는 Cloudera 전용 JDBC 드라이버(com.cloudera.hive.jdbc.HS2Driver)를 사용한다.
 * 이 드라이버는 공개 Maven 저장소에서 배포되지 않고 라이선스상 번들에 포함할 수 없으므로 컴파일 타임
 * 의존성으로 포함하지 않고 클래스 이름 문자열로만 참조한다. 드라이버 jar는 런타임에 제공되어야 하며,
 * "드라이버 위치(들)" 프로퍼티로 지정(권장, 서비스별 격리·재기동 불필요)하거나 NiFi 클래스패스(lib/)에 둔다.
 * Kerberos 보안이 활성화된 Hive 환경에서는 KerberosUserService를 통해 인증을 수행한다.
 */
@RequiresInstanceClassLoading
@Tags({"cloudera", "hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Apache Hive 3.x용 데이터베이스 커넥션 풀링 서비스를 제공한다. 풀에서 커넥션을 요청하고 사용 후 반환할 수 있다.")
public class VendorHive3ConnectionPool extends AbstractControllerService implements VendorHiveDBCPService {
    // 풀에 유지할 최소 유휴 커넥션 수의 기본값(0: 유휴 커넥션을 미리 만들어 두지 않음)
    private static final String DEFAULT_MIN_IDLE = "0";

    // 풀에 유지할 최대 유휴 커넥션 수의 기본값
    private static final String DEFAULT_MAX_IDLE = "8";

    // 커넥션 최대 수명의 기본값(-1: 무제한)
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";

    // 유휴 커넥션 제거 스레드 실행 주기의 기본값(-1: 실행하지 않음)
    private static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);

    // 제거 대상이 되기까지의 최소 유휴 시간 기본값
    private static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME = "30 mins";

    // 소프트 최소 제거 대상 유휴 시간의 기본값(-1: 비활성화)
    private static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);

    static final PropertyDescriptor DATABASE_URL = (new PropertyDescriptor.Builder())
            .name("hive-db-connect-url")
            .displayName("데이터베이스 연결 URL")
            .description("데이터베이스에 연결할 때 사용하는 데이터베이스 연결 URL. 데이터베이스 시스템 이름, 호스트, 포트, 데이터베이스 이름 및 일부 파라미터를 포함할 수 있다. 데이터베이스 연결 URL의 정확한 구문은 Hive 문서에 명시되어 있다. 예를 들어 보안이 적용된 Hive Server에 연결할 때는 서버 principal이 연결 파라미터로 포함되는 경우가 많다.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = (new PropertyDescriptor.Builder())
            .name("hive-config-resources")
            .displayName("Hive 설정 리소스")
            .description("Hive 설정(예: hive-site.xml)을 포함하는 파일 또는 쉼표로 구분된 파일 목록. 이를 설정하지 않으면 Hadoop이 클래스패스에서 'hive-site.xml' 파일을 찾거나 기본 설정으로 대체한다. Kerberos 인증 등을 활성화하려면 설정 파일에 적절한 속성이 설정되어 있어야 함에 유의한다. 자세한 내용은 Hive 문서를 참고한다.")

            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, new ResourceType[0])
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    // 드라이버 위치: 라이선스상 Cloudera JDBC 드라이버를 번들에 포함할 수 없으므로,
    // CDP 사용자가 내려받은 드라이버 jar를 지정하게 한다. dynamicallyModifiesClasspath(true) +
    // @RequiresInstanceClassLoading 조합으로 지정한 jar가 이 서비스 인스턴스 전용 클래스로더에 추가된다.
    // 비워 두면 NiFi 클래스패스(lib/)에서 드라이버를 찾는다(재기동 필요).
    static final PropertyDescriptor DB_DRIVER_LOCATION = (new PropertyDescriptor.Builder())
            .name("hive-db-driver-locations")
            .displayName("드라이버 위치(들)")
            .description("Cloudera Hive JDBC 드라이버(com.cloudera.hive.jdbc.HS2Driver) jar와 그 의존 jar가 있는 " +
                    "파일/디렉터리 경로 또는 URL의 쉼표 구분 목록. 라이선스상 드라이버를 번들에 포함하지 않으므로, " +
                    "CDP 사용자는 Cloudera에서 내려받은 드라이버 jar를 여기에 지정한다. " +
                    "비워 두면 NiFi 클래스패스(lib/)에서 드라이버를 찾는다.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamicallyModifiesClasspath(true)
            .build();

    static final PropertyDescriptor DB_USER = (new PropertyDescriptor.Builder())
            .name("hive-db-user")
            .displayName("데이터베이스 사용자")
            .description("데이터베이스 사용자 이름")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor DB_PASSWORD = (new PropertyDescriptor.Builder())
            .name("hive-db-password")
            .displayName("비밀번호")
            .description("데이터베이스 사용자의 비밀번호")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor MAX_WAIT_TIME = (new PropertyDescriptor.Builder())
            .name("hive-max-wait-time")
            .displayName("최대 대기 시간")
            .description("사용 가능한 커넥션이 없을 때 풀이 커넥션 반환을 기다리는 최대 시간. 이 시간이 지나면 실패로 처리되며, -1이면 무한정 대기한다. ")

            .defaultValue("500 millis")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = (new PropertyDescriptor.Builder())
            .name("hive-max-total-connections")
            .displayName("최대 총 커넥션 수")
            .description("이 풀에서 동시에 할당할 수 있는 활성 커넥션의 최대 개수. 음수이면 제한이 없다.")

            .defaultValue("8")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor VALIDATION_QUERY = (new PropertyDescriptor.Builder())
            .name("Validation-query")
            .displayName("검증 쿼리")
            .description("커넥션을 반환하기 전에 유효성을 검증하는 데 사용되는 쿼리. 대여한 커넥션이 유효하지 않으면 폐기되고 새로운 유효한 커넥션이 반환된다. 참고: 검증을 사용하면 성능이 저하될 수 있다.")

            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MIN_IDLE = (new PropertyDescriptor.Builder())
            .displayName("최소 유휴 커넥션 수")
            .name("dbcp-min-idle-conns")
            .description("추가로 생성하지 않고 풀에 유휴 상태로 유지할 수 있는 최소 커넥션 개수. 0이면 생성하지 않는다.")
            .defaultValue(DEFAULT_MIN_IDLE)
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_IDLE = (new PropertyDescriptor.Builder())
            .displayName("최대 유휴 커넥션 수")
            .name("dbcp-max-idle-conns")
            .description("추가로 해제하지 않고 풀에 유휴 상태로 유지할 수 있는 최대 커넥션 개수. 음수이면 제한이 없다.")
            .defaultValue(DEFAULT_MAX_IDLE)
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = (new PropertyDescriptor.Builder())
            .displayName("최대 커넥션 수명")
            .name("dbcp-max-conn-lifetime")
            .description("커넥션의 최대 수명(밀리초). 이 시간을 초과하면 다음 활성화, 비활성화 또는 검증 테스트에서 커넥션이 실패한다. 0 이하의 값은 커넥션의 수명이 무한함을 의미한다.")
            .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = (new PropertyDescriptor.Builder())
            .displayName("유휴 커넥션 제거 실행 간격")
            .name("dbcp-time-between-eviction-runs")
            .description("유휴 커넥션 제거 스레드의 실행 사이에 대기하는 시간(밀리초). 0 이하이면 유휴 커넥션 제거 스레드가 실행되지 않는다.")

            .defaultValue(DEFAULT_EVICTION_RUN_PERIOD)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = (new PropertyDescriptor.Builder())
            .displayName("제거 대상이 되는 최소 유휴 시간")
            .name("dbcp-min-evictable-idle-time")
            .description("커넥션이 제거 대상이 되기 전까지 풀에서 유휴 상태로 머무를 수 있는 최소 시간.")
            .defaultValue(DEFAULT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = (new PropertyDescriptor.Builder())
            .displayName("소프트 최소 제거 대상 유휴 시간")
            .name("dbcp-soft-min-evictable-idle-time")
            .description("커넥션이 유휴 커넥션 제거 스레드에 의해 제거 대상이 되기 전까지 풀에서 유휴 상태로 머무를 수 있는 최소 시간이며, 풀에 최소 개수 이상의 유휴 커넥션이 남아 있어야 한다는 추가 조건이 있다. 이 옵션의 소프트가 아닌 버전(Minimum Evictable Idle Time)이 양수로 설정된 경우 유휴 커넥션 제거 스레드가 이를 먼저 검사한다. 즉 유휴 커넥션을 순회할 때 유휴 시간을 먼저 (풀 내 유휴 커넥션 개수는 고려하지 않고) 비교한 다음, 최소 유휴 커넥션 제약 조건을 포함하여 이 소프트 옵션과 비교한다.")

            .defaultValue(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final PropertyDescriptor KERBEROS_USER_SERVICE = (new PropertyDescriptor.Builder())
            .name("kerberos-user-service")
            .displayName("Kerberos 사용자 서비스")
            .description("Kerberos 인증에 사용할 Kerberos User 컨트롤러 서비스를 지정한다")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();

    private List<PropertyDescriptor> properties;

    private String connectionUrl = "unknown";

    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    private volatile BasicDataSource dataSource;

    private final HiveConfigurator hiveConfigurator = new HiveConfigurator();

    private volatile UserGroupInformation ugi;

    private final AtomicReference<KerberosUser> kerberosUserReference = new AtomicReference<>();

    // 컨트롤러 서비스가 지원하는 프로퍼티 목록을 초기화한다. NiFi가 서비스를 등록할 때 한 번 호출된다.
    protected void init(ControllerServiceInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(HIVE_CONFIGURATION_RESOURCES);
        props.add(DB_DRIVER_LOCATION);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(KERBEROS_USER_SERVICE);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(VALIDATION_QUERY);
        props.add(MIN_IDLE);
        props.add(MAX_IDLE);
        props.add(MAX_CONN_LIFETIME);
        props.add(EVICTION_RUN_PERIOD);
        props.add(MIN_EVICTABLE_IDLE_TIME);
        props.add(SOFT_MIN_EVICTABLE_IDLE_TIME);
        this.properties = props;
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    // Hive 설정 리소스 파일이 지정된 경우, 해당 설정 파일을 실제로 로드/파싱하여 유효성을 검증한다.
    // 검증 결과(파싱 실패 등)는 캐시(validationResourceHolder)를 통해 재사용되어 반복 로드를 방지한다.
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        boolean confFileProvided = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).isSet();
        List<ValidationResult> problems = new ArrayList<>();
        if (confFileProvided) {
            KerberosUserService kerberosUserService = (KerberosUserService) validationContext.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            String configFiles = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            problems.addAll(this.hiveConfigurator.validate(configFiles, kerberosUserService, this.validationResourceHolder, getLogger()));
        }
        return problems;
    }

    // 컨트롤러 서비스가 활성화(Enable)될 때 호출되어 실제 커넥션 풀(BasicDataSource)을 생성/구성한다.
    // 1) Hive 설정 리소스를 로드하고 동적 프로퍼티를 Hive 설정에 반영
    // 2) 보안이 활성화된 경우 Kerberos 인증 수행
    // 3) DBCP 풀의 각종 파라미터(최대/최소 커넥션 수, 타임아웃 등)를 설정
    @OnEnabled
    public void onConfigured(ConfigurationContext context) throws InitializationException {
        ComponentLog log = getLogger();
        String configFiles = context.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        HiveConf hiveConf = this.hiveConfigurator.getConfigurationFromFiles(configFiles);
        String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        // 표준 프로퍼티 외에 사용자가 추가한 동적 프로퍼티를 그대로 Hive 설정 객체에 반영한다.
        for (Map.Entry<PropertyDescriptor, String> entry : (Iterable<Map.Entry<PropertyDescriptor, String>>) context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic())
                hiveConf.set(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
        }
        // NOTE: the Cloudera JDBC driver (com.cloudera.hive.jdbc.HS2Driver) is referenced by name only;
        // the driver jar is not a compile-time dependency (it is not available in public Maven repositories)
        // and must be provided at runtime — either via the "드라이버 위치(들)" property (권장) or on the
        // NiFi classpath(lib/). 아래에서 인스턴스 클래스로더를 명시적으로 지정해 두 경로 모두 지원한다.
        String drv = "com.cloudera.hive.jdbc.HS2Driver";
        if (SecurityUtil.isSecurityEnabled((Configuration) hiveConf)) {
            KerberosUserService kerberosUserService = (KerberosUserService) context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            if (kerberosUserService == null)
                throw new InitializationException("Kerberos authentication is required by the specified configuration, Kerberos User Service must be configured");
            KerberosUser kerberosUser = kerberosUserService.createKerberosUser();
            try {
                this.ugi = this.hiveConfigurator.authenticate((Configuration) hiveConf, kerberosUser);
            } catch (AuthenticationFailedException ae) {
                log.error(ae.getMessage(), (Throwable) ae);
                throw new InitializationException(ae);
            }
            this.kerberosUserReference.set(kerberosUser);
            getLogger().info("Successfully logged in as principal " + kerberosUser.getPrincipal());
        }
        String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        Long maxWaitMillis = context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        this.dataSource = new BasicDataSource();
        this.dataSource.setDriverClassName(drv);
        // 드라이버는 이 서비스 인스턴스 전용 클래스로더에서 로드한다. "드라이버 위치(들)" 프로퍼티로
        // 지정한 jar는 dynamicallyModifiesClasspath로 이 클래스로더에 추가되며, 미지정 시에도
        // 상위(lib/) 클래스로더에서 해석된다. getConnection 호출 스레드의 TCCL에 의존하지 않도록 명시한다.
        this.dataSource.setDriverClassLoader(this.getClass().getClassLoader());
        this.connectionUrl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        if (validationQuery != null && !validationQuery.isEmpty()) {
            this.dataSource.setValidationQuery(validationQuery);
            this.dataSource.setTestOnBorrow(true);
        }
        this.dataSource.setUrl(this.connectionUrl);
        this.dataSource.setUsername(user);
        this.dataSource.setPassword(passw);
        this.dataSource.setMaxWaitMillis(maxWaitMillis.longValue());
        this.dataSource.setMaxTotal(maxTotal.intValue());
        this.dataSource.setMinIdle(minIdle.intValue());
        this.dataSource.setMaxIdle(maxIdle.intValue());
        this.dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis.longValue());
        this.dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis.longValue());
        this.dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis.longValue());
        this.dataSource.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis.longValue());
    }

    // 프로퍼티 값이 "-1"이면 무한대(-1)로 그대로 취급하고, 그 외에는 시간 기간 문자열을 밀리초로 변환한다.
    // "-1 millis"처럼 시간 단위 표기로는 무한대를 표현할 수 없어 별도 처리한다.
    private Long extractMillisWithInfinite(PropertyValue prop) {
        return Long.valueOf("-1".equals(prop.getValue()) ? -1L : prop.asTimePeriod(TimeUnit.MILLISECONDS).longValue());
    }

    // 컨트롤러 서비스가 비활성화(Disable)될 때 호출되어 커넥션 풀을 닫고 모든 열린 커넥션을 반환/해제한다.
    @OnDisabled
    public void shutdown() {
        try {
            this.dataSource.close();
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }

    // 풀에서 새로운 데이터베이스 커넥션을 가져온다.
    // Kerberos가 사용 중인 경우 TGT(Ticket Granting Ticket) 만료 여부를 확인하고 필요하면 재로그인한 뒤,
    // UserGroupInformation.doAs를 통해 인증된 사용자 컨텍스트 안에서 커넥션을 얻는다.
    public Connection getConnection() throws ProcessException {
        try {
            if (this.ugi != null) {
                getLogger().trace("getting UGI instance");
                if (this.kerberosUserReference.get() != null) {
                    KerberosUser kerberosUser = this.kerberosUserReference.get();
                    getLogger().debug("kerberosUser is " + kerberosUser);
                    try {
                        getLogger().debug("checking TGT on kerberosUser " + kerberosUser);
                        kerberosUser.checkTGTAndRelogin();
                    } catch (KerberosLoginException e) {
                        throw new ProcessException("Unable to relogin with kerberos credentials for " + kerberosUser.getPrincipal(), e);
                    }
                } else {
                    getLogger().debug("kerberosUser was null, will not refresh TGT with KerberosUser");
                    this.ugi.checkTGTAndReloginFromKeytab();
                }
                try {
                    return ugi.doAs((PrivilegedExceptionAction<Connection>) () -> dataSource.getConnection());
                } catch (UndeclaredThrowableException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof SQLException)
                        throw (SQLException) cause;
                    throw e;
                }
            }
            getLogger().info("Simple Authentication");
            return this.dataSource.getConnection();
        } catch (SQLException | java.io.IOException | InterruptedException e) {
            getLogger().error("Error getting Hive connection", e);
            throw new ProcessException(e);
        }
    }

    public String toString() {
        return "VendorHive3ConnectionPool[id=" + getIdentifier() + "]";
    }

    public String getConnectionURL() {
        return this.connectionUrl;
    }
}
