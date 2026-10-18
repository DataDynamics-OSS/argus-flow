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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.cloudera.hive.jdbc.HS2Driver;

/**
 * VendorHive3ConnectionPool 컨트롤러 서비스 단위 테스트.
 *
 * 실제 벤더 드라이버(com.cloudera.hive.jdbc.HS2Driver)는 라이선스상 클래스패스에 없으므로, 테스트 소스에
 * 같은 FQCN의 가짜 드라이버({@link HS2Driver}, H2 위임)를 두어 풀 구성/커넥션 획득/비활성화 동작을 검증한다.
 * 드라이버 부재 시 동작은 해당 클래스를 숨기는 격리 클래스로더로 검증한다.
 * Kerberos는 가짜 {@link KerberosUserService}/{@link KerberosUser}로 대체한다.
 *
 * 결함 수정 시 활성화된 회귀 테스트는 결함 ID(V-n)를 주석에 남긴다.
 */
class VendorHive3ConnectionPoolTest {

    private static final String POOL_ID = "vendor-hive-pool";
    private static final String KERBEROS_SERVICE_ID = "kerberos-user-service";
    private static final String KERBEROS_USER_SERVICE_PROPERTY = "kerberos-user-service";
    private static final String PASSWORD = "s3cr3t-P@ssw0rd";

    @TempDir
    Path tempDir;

    private TestRunner runner;
    private VendorHive3ConnectionPool pool;

    @BeforeEach
    void setUp() throws Exception {
        HS2Driver.reset();
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        pool = new VendorHive3ConnectionPool();
        runner.addControllerService(POOL_ID, pool);
    }

    @AfterEach
    void tearDown() {
        if (runner.isControllerServiceEnabled(pool)) {
            runner.disableControllerService(pool);
        }
        // Kerberos 테스트가 바꾼 UGI 전역 설정을 원복한다.
        UserGroupInformation.setConfiguration(new Configuration());
    }

    private void setUrl(final String suffix) {
        runner.setProperty(pool, VendorHive3ConnectionPool.DATABASE_URL, HS2Driver.URL_PREFIX + suffix);
    }

    private Path writeSiteXml(final String name, final boolean kerberos) throws IOException {
        final StringBuilder xml = new StringBuilder("<?xml version=\"1.0\"?>\n<configuration>\n");
        if (kerberos) {
            xml.append("  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>\n");
            // 테스트 환경에 krb5 기본 realm이 없어도 principal → short name 변환이 되도록 명시적 규칙을 둔다.
            xml.append("  <property><name>hadoop.security.auth_to_local</name>"
                    + "<value>RULE:[1:$1@$0](.*@EXAMPLE\\.COM)s/@.*//\nDEFAULT</value></property>\n");
        } else {
            xml.append("  <property><name>hadoop.security.authentication</name><value>simple</value></property>\n");
        }
        xml.append("</configuration>\n");
        final Path file = tempDir.resolve(name);
        Files.writeString(file, xml.toString(), StandardCharsets.UTF_8);
        return file;
    }

    private FakeKerberosUserService addKerberosService(final FakeKerberosUser user) throws Exception {
        final FakeKerberosUserService service = new FakeKerberosUserService(user);
        runner.addControllerService(KERBEROS_SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(pool, KERBEROS_USER_SERVICE_PROPERTY, KERBEROS_SERVICE_ID);
        return service;
    }

    private static int selectOne(final Connection connection) throws Exception {
        try (Statement s = connection.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String allMessages(final MockComponentLog log) {
        final List<LogMessage> all = new ArrayList<>();
        all.addAll(log.getTraceMessages());
        all.addAll(log.getDebugMessages());
        all.addAll(log.getInfoMessages());
        all.addAll(log.getWarnMessages());
        all.addAll(log.getErrorMessages());
        final StringBuilder sb = new StringBuilder();
        for (final LogMessage m : all) {
            sb.append(m.toString()).append('\n');
            if (m.getArgs() != null) {
                for (final Object arg : m.getArgs()) {
                    sb.append(arg).append('\n');
                }
            }
            Throwable t = m.getThrowable();
            while (t != null) {
                sb.append(t).append('\n');
                t = t.getCause();
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // 프로퍼티 검증
    // ------------------------------------------------------------------

    @Test
    void validWithOnlyDatabaseUrl() {
        setUrl("host:10000/default");
        runner.assertValid(pool);
    }

    @Test
    void invalidWithoutDatabaseUrl() {
        runner.assertNotValid(pool);
    }

    @Test
    void invalidWithEmptyDatabaseUrl() {
        runner.setProperty(pool, VendorHive3ConnectionPool.DATABASE_URL, "");
        runner.assertNotValid(pool);
    }

    @Test
    void invalidPoolSizingValues() {
        setUrl("host:10000/default");

        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_TOTAL_CONNECTIONS, "abc");
        runner.assertNotValid(pool);
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_TOTAL_CONNECTIONS, "-1");
        runner.assertValid(pool); // 음수 = 무제한(문서화된 동작)

        runner.setProperty(pool, VendorHive3ConnectionPool.MIN_IDLE, "-1");
        runner.assertNotValid(pool);
        runner.setProperty(pool, VendorHive3ConnectionPool.MIN_IDLE, "0");
        runner.assertValid(pool);

        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_IDLE, "x");
        runner.assertNotValid(pool);
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_IDLE, "-1");
        runner.assertValid(pool);
    }

    @Test
    void invalidTimePeriodValues() {
        setUrl("host:10000/default");

        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_WAIT_TIME, "soon");
        runner.assertNotValid(pool);
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_WAIT_TIME, "500 millis");
        runner.assertValid(pool);

        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_CONN_LIFETIME, "forever");
        runner.assertNotValid(pool);
    }

    @Test
    void minusOneAcceptedForInfiniteDbcpTimeProperties() {
        setUrl("host:10000/default");
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_CONN_LIFETIME, "-1");
        runner.setProperty(pool, VendorHive3ConnectionPool.EVICTION_RUN_PERIOD, "-1");
        runner.setProperty(pool, VendorHive3ConnectionPool.SOFT_MIN_EVICTABLE_IDLE_TIME, "-1");
        runner.setProperty(pool, VendorHive3ConnectionPool.MIN_EVICTABLE_IDLE_TIME, "10 mins");
        runner.assertValid(pool);
    }

    // V-5 회귀 테스트: MAX_WAIT_TIME -1(무한 대기)을 허용하고 활성화도 성공해야 한다.
    @Test
    void maxWaitMinusOneShouldMeanInfiniteWait() throws Exception {
        setUrl("host:10000/maxwaitinf");
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_WAIT_TIME, "-1");
        runner.assertValid(pool);
        runner.enableControllerService(pool);
        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }
    }

    @Test
    void maxWaitRejectsOtherNegativeOrInvalidValues() {
        setUrl("host:10000/default");
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_WAIT_TIME, "-2");
        runner.assertNotValid(pool);
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_WAIT_TIME, "abc");
        runner.assertNotValid(pool);
    }

    // V-4 회귀 테스트: 동적 프로퍼티를 추가해도 서비스가 유효해야 한다.
    @Test
    void dynamicPropertiesShouldBeAccepted() {
        setUrl("host:10000/default");
        runner.setProperty(pool, "hive.server2.proxy.user", "etl");
        runner.assertValid(pool);

        final PropertyDescriptor dynamic = pool.getPropertyDescriptor("hive.server2.proxy.user");
        assertTrue(dynamic.isDynamic());
        assertEquals(ExpressionLanguageScope.ENVIRONMENT, dynamic.getExpressionLanguageScope());
    }

    @Test
    void emptyDynamicPropertyValueIsInvalid() {
        setUrl("host:10000/default");
        runner.setProperty(pool, "hive.server2.proxy.user", "");
        runner.assertNotValid(pool);
    }

    // 동적 프로퍼티가 HiveConf에 반영되는지 확인한다. 설정 파일 없이 동적 프로퍼티만으로 Kerberos를 켜면
    // 활성화 시 Kerberos 로그인 경로를 타야 한다.
    @Test
    void dynamicPropertiesAreAppliedToHiveConf() throws Exception {
        setUrl("host:10000/dynkrb");
        runner.setProperty(pool, "hadoop.security.authentication", "kerberos");
        runner.setProperty(pool, "hadoop.security.auth_to_local", "RULE:[1:$1@$0](.*@EXAMPLE\\.COM)s/@.*//\nDEFAULT");
        final FakeKerberosUser user = new FakeKerberosUser();
        addKerberosService(user);
        runner.assertValid(pool);
        runner.enableControllerService(pool);

        assertEquals(1, user.loginCount.get());
        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }
        assertEquals(1, user.reloginCheckCount.get());
    }

    // 동적 프로퍼티 없이 Kerberos 키를 넣지 않으면 Kerberos 로그인 경로를 타지 않는다(위 테스트의 대조군).
    @Test
    void withoutDynamicPropertyKerberosIsNotUsed() throws Exception {
        setUrl("host:10000/dynsimple");
        final FakeKerberosUser user = new FakeKerberosUser();
        addKerberosService(user);
        runner.enableControllerService(pool);
        assertEquals(0, user.loginCount.get());
    }

    @Test
    void invalidWhenHiveConfigResourceDoesNotExist() {
        setUrl("host:10000/default");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                tempDir.resolve("no-such-hive-site.xml").toString());
        runner.assertNotValid(pool);
    }

    @Test
    void validWithNonSecureHiveConfigResource() throws Exception {
        setUrl("host:10000/default");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-simple.xml", false).toString());
        runner.assertValid(pool);
    }

    @Test
    void invalidWhenSecureConfigWithoutKerberosUserService() throws Exception {
        setUrl("host:10000/default");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-krb.xml", true).toString());
        runner.assertNotValid(pool);
    }

    // V-7(b): 설정 리소스 없이 동적 프로퍼티로 Kerberos를 켠 경우에도 Kerberos User Service를 요구한다.
    @Test
    void invalidWhenDynamicPropertyEnablesKerberosWithoutKerberosUserService() {
        setUrl("host:10000/default");
        runner.setProperty(pool, "hadoop.security.authentication", "kerberos");
        runner.assertNotValid(pool);
    }

    // V-7(b): 설정 리소스를 지정하지 않으면 클래스패스(TCCL)의 core-site.xml/hive-site.xml 보안 설정으로 판단한다.
    @Test
    void invalidWhenClasspathConfigIsSecureWithoutKerberosUserService() throws Exception {
        final Path dir = Files.createDirectories(tempDir.resolve("cp"));
        Files.writeString(dir.resolve("core-site.xml"), "<?xml version=\"1.0\"?>\n<configuration>\n"
                + "  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>\n"
                + "</configuration>\n", StandardCharsets.UTF_8);
        setUrl("host:10000/default");
        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (java.net.URLClassLoader cl = new java.net.URLClassLoader(new java.net.URL[]{dir.toUri().toURL()}, original)) {
            Thread.currentThread().setContextClassLoader(cl);
            runner.assertNotValid(pool);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void validWhenSecureConfigWithKerberosUserService() throws Exception {
        setUrl("host:10000/default");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-krb.xml", true).toString());
        addKerberosService(new FakeKerberosUser());
        runner.assertValid(pool);
    }

    // ------------------------------------------------------------------
    // 활성화 / 커넥션 획득 (가짜 드라이버)
    // ------------------------------------------------------------------

    @Test
    void connectionUrlIsUnknownUntilEnabled() {
        assertEquals("unknown", pool.getConnectionURL());
        setUrl("host:10000/url_before_enable");
        runner.enableControllerService(pool);
        assertEquals(HS2Driver.URL_PREFIX + "host:10000/url_before_enable", pool.getConnectionURL());
    }

    @Test
    void enabledPoolPassesUrlUserAndPasswordToVendorDriver() throws Exception {
        setUrl("host:10000/assembly");
        runner.setProperty(pool, VendorHive3ConnectionPool.DB_USER, "hive");
        runner.setProperty(pool, VendorHive3ConnectionPool.DB_PASSWORD, PASSWORD);
        runner.enableControllerService(pool);

        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }
        assertEquals(HS2Driver.URL_PREFIX + "host:10000/assembly", HS2Driver.lastUrl);
        assertNotNull(HS2Driver.lastInfo);
        assertEquals("hive", HS2Driver.lastInfo.getProperty("user"));
        assertEquals(PASSWORD, HS2Driver.lastInfo.getProperty("password"));
    }

    @Test
    void connectionsAreReusedFromPool() throws Exception {
        setUrl("host:10000/reuse");
        runner.enableControllerService(pool);

        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }
        // DBCP는 풀 초기화 시 커넥션 팩토리 검증용 커넥션을 1회 추가로 생성하므로 첫 대여 이후 값을 기준으로 삼는다.
        final int afterFirstBorrow = HS2Driver.CONNECT_COUNT.get();
        for (int i = 0; i < 5; i++) {
            try (Connection c = pool.getConnection()) {
                assertEquals(1, selectOne(c));
            }
        }
        assertEquals(afterFirstBorrow, HS2Driver.CONNECT_COUNT.get(), "반환된 커넥션은 풀에서 재사용되어야 한다");
    }

    @Test
    void maxTotalAndMaxWaitAreApplied() throws Exception {
        setUrl("host:10000/maxtotal");
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_TOTAL_CONNECTIONS, "1");
        runner.setProperty(pool, VendorHive3ConnectionPool.MAX_WAIT_TIME, "100 millis");
        runner.enableControllerService(pool);

        try (Connection first = pool.getConnection()) {
            assertNotNull(first);
            final long start = System.nanoTime();
            assertThrows(ProcessException.class, pool::getConnection);
            final long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;
            assertTrue(elapsedMillis >= 90, "MAX_WAIT_TIME 만큼 대기해야 한다: " + elapsedMillis);
            assertTrue(elapsedMillis < 5_000, "MAX_WAIT_TIME을 크게 넘겨 대기하면 안 된다: " + elapsedMillis);
        }
        // 반환 후에는 다시 획득 가능
        try (Connection again = pool.getConnection()) {
            assertEquals(1, selectOne(again));
        }
    }

    @Test
    void validationQueryIsAppliedOnBorrow() {
        setUrl("host:10000/validation");
        runner.setProperty(pool, VendorHive3ConnectionPool.VALIDATION_QUERY, "SELECT * FROM no_such_table");
        runner.enableControllerService(pool);

        // 검증 쿼리가 testOnBorrow로 적용되면 대여 시 검증 실패 → 커넥션 획득 실패
        assertThrows(ProcessException.class, pool::getConnection);
    }

    @Test
    void validValidationQueryAllowsBorrow() throws Exception {
        setUrl("host:10000/validation_ok");
        runner.setProperty(pool, VendorHive3ConnectionPool.VALIDATION_QUERY, "SELECT 1");
        runner.enableControllerService(pool);
        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }
    }

    @Test
    void urlNotAcceptedByVendorDriverFails() {
        runner.setProperty(pool, VendorHive3ConnectionPool.DATABASE_URL, "jdbc:mysql://host/db");
        runner.enableControllerService(pool);

        final ProcessException e = assertThrows(ProcessException.class, pool::getConnection);
        assertTrue(rootCauseChain(e).contains("No suitable driver"), rootCauseChain(e));
    }

    @Test
    void disableClosesPoolAndRejectsFurtherConnections() throws Exception {
        setUrl("host:10000/disable");
        runner.enableControllerService(pool);
        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }

        runner.disableControllerService(pool);

        assertThrows(ProcessException.class, pool::getConnection);
    }

    @Test
    void reEnableAfterDisableCreatesWorkingPool() throws Exception {
        setUrl("host:10000/reenable");
        runner.enableControllerService(pool);
        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }
        runner.disableControllerService(pool);
        runner.enableControllerService(pool);
        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }
    }

    @Test
    void passwordIsNeverLogged() throws Exception {
        setUrl("host:10000/nolog");
        runner.setProperty(pool, VendorHive3ConnectionPool.DB_USER, "hive");
        runner.setProperty(pool, VendorHive3ConnectionPool.DB_PASSWORD, PASSWORD);
        runner.setProperty(pool, VendorHive3ConnectionPool.VALIDATION_QUERY, "SELECT * FROM no_such_table");
        runner.enableControllerService(pool);
        // 실패 경로(에러 로그)까지 유도
        assertThrows(ProcessException.class, pool::getConnection);

        final String logged = allMessages(runner.getControllerServiceLogger(POOL_ID));
        assertFalse(logged.contains(PASSWORD), "비밀번호가 로그에 노출되면 안 된다");
    }

    // V-6 회귀 테스트
    @Test
    void simpleAuthenticationShouldNotLogAtInfoOnEveryBorrow() throws Exception {
        setUrl("host:10000/infolog");
        runner.enableControllerService(pool);
        for (int i = 0; i < 10; i++) {
            try (Connection c = pool.getConnection()) {
                assertNotNull(c);
            }
        }
        final long infoCount = runner.getControllerServiceLogger(POOL_ID).getInfoMessages().stream()
                .filter(m -> String.valueOf(m.getMsg()).contains("Simple Authentication"))
                .count();
        assertTrue(infoCount <= 1, "INFO 'Simple Authentication' 로그 " + infoCount + "건");
    }

    // ------------------------------------------------------------------
    // 드라이버 부재
    // ------------------------------------------------------------------

    @Test
    void missingVendorDriverFailsOnEnableWithClearMessage() throws Exception {
        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (HidingClassLoader hiding = new HidingClassLoader(getClass().getClassLoader())) {
            Thread.currentThread().setContextClassLoader(hiding);
            final Class<?> poolClass = hiding.loadClass(VendorHive3ConnectionPool.class.getName());
            assertTrue(poolClass.getClassLoader() == hiding, "격리 클래스로더에서 정의되어야 한다");
            assertThrows(ClassNotFoundException.class, () -> hiding.loadClass("com.cloudera.hive.jdbc.HS2Driver"));

            final VendorHiveConnectionPoolHandle isolated = new VendorHiveConnectionPoolHandle(
                    (org.apache.nifi.controller.ControllerService) poolClass.getDeclaredConstructor().newInstance());
            final TestRunner isolatedRunner = TestRunners.newTestRunner(NoOpProcessor.class);
            isolatedRunner.addControllerService("isolated", isolated.service);
            isolatedRunner.setProperty(isolated.service, VendorHive3ConnectionPool.DATABASE_URL.getName(),
                    HS2Driver.URL_PREFIX + "host:10000/missing");

            // V-7(a): 드라이버 부재는 첫 getConnection이 아니라 활성화 시점에 드러나야 한다.
            final Throwable t = assertThrows(Throwable.class, () -> isolatedRunner.enableControllerService(isolated.service));
            final String chain = rootCauseChain(t);
            assertTrue(chain.contains("InitializationException"), chain);
            assertTrue(chain.contains("com.cloudera.hive.jdbc.HS2Driver"), chain);
            assertTrue(chain.contains("드라이버 위치(들)") && chain.contains("README"), chain);
            assertFalse(isolatedRunner.isControllerServiceEnabled(isolated.service));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    // ------------------------------------------------------------------
    // 비활성화 / Kerberos
    // ------------------------------------------------------------------

    @Test
    void shutdownWithoutDataSourceShouldNotThrow() {
        assertDoesNotThrow(() -> pool.shutdown());
    }

    @Test
    void kerberosLoginFailureFailsEnable() throws Exception {
        setUrl("host:10000/krbfail");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-krb.xml", true).toString());
        final FakeKerberosUser user = new FakeKerberosUser();
        user.failLogin = true;
        addKerberosService(user);
        runner.assertValid(pool);

        final Throwable t = assertThrows(Throwable.class, () -> runner.enableControllerService(pool));
        assertTrue(String.valueOf(t.getMessage()).contains("Kerberos") || rootCauseChain(t).contains("Kerberos"),
                rootCauseChain(t));
        assertEquals(1, user.loginCount.get());
    }

    @Test
    void kerberosEnabledPoolChecksTgtOnEveryBorrow() throws Exception {
        setUrl("host:10000/krb");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-krb.xml", true).toString());
        final FakeKerberosUser user = new FakeKerberosUser();
        addKerberosService(user);
        runner.enableControllerService(pool);
        assertEquals(1, user.loginCount.get());

        try (Connection c = pool.getConnection()) {
            assertEquals(1, selectOne(c));
        }
        try (Connection c = pool.getConnection()) {
            assertNotNull(c);
        }
        assertEquals(2, user.reloginCheckCount.get());
    }

    @Test
    void kerberosReloginFailureSurfacesAsProcessException() throws Exception {
        setUrl("host:10000/krbrelogin");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-krb.xml", true).toString());
        final FakeKerberosUser user = new FakeKerberosUser();
        addKerberosService(user);
        runner.enableControllerService(pool);

        user.failRelogin = true;
        final ProcessException e = assertThrows(ProcessException.class, pool::getConnection);
        assertTrue(e.getMessage().contains(FakeKerberosUser.PRINCIPAL), e.getMessage());
    }

    // V-7(c): Kerberos 사용 시 DBCP evictor 스레드가 최소 유휴 커넥션을 보충할 때도 UGI(doAs) 안에서 커넥션을 만든다.
    @Test
    void evictorCreatedConnectionsUseKerberosSubject() throws Exception {
        setUrl("host:10000/krbevictor");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-krb.xml", true).toString());
        runner.setProperty(pool, VendorHive3ConnectionPool.MIN_IDLE, "3");
        runner.setProperty(pool, VendorHive3ConnectionPool.EVICTION_RUN_PERIOD, "20 millis");
        final FakeKerberosUser user = new FakeKerberosUser();
        addKerberosService(user);

        // DBCP evictor는 JVM 공용 스레드(commons-pool-evictor)에서 돈다. 그 스레드가 doAs 안에서 만들어지면
        // 생성 시점의 Subject를 상속하므로, 운영처럼 다른 풀이 doAs 밖에서 먼저 evictor 스레드를 띄운 상황을 재현한다.
        try (org.apache.commons.dbcp2.BasicDataSource other = new org.apache.commons.dbcp2.BasicDataSource()) {
            other.setUrl("jdbc:h2:mem:evictor_starter;DB_CLOSE_DELAY=-1");
            other.setTimeBetweenEvictionRunsMillis(1000L);
            try (Connection c = other.getConnection()) {
                assertNotNull(c);
            }
            runner.enableControllerService(pool);
            assertEvictorConnectionsUseSubject();
        }
    }

    private void assertEvictorConnectionsUseSubject() throws Exception {
        // 첫 대여로 풀이 생성되고 evictor 작업이 등록된다.
        try (Connection c = pool.getConnection()) {
            assertNotNull(c);
        }
        // 풀 생성 시 팩토리 검증용 1개 + 대여 1개(반환되어 유휴 1개) + evictor 보충 2개 = 4개
        final long deadline = System.currentTimeMillis() + 5_000L;
        while (HS2Driver.CONNECT_COUNT.get() < 4 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertTrue(HS2Driver.CONNECT_COUNT.get() >= 4, "evictor가 최소 유휴 커넥션을 보충해야 한다: " + HS2Driver.CONNECT_COUNT.get());
        for (final String subject : HS2Driver.CONNECT_SUBJECTS) {
            assertEquals(FakeKerberosUser.PRINCIPAL, subject, "모든 물리 커넥션은 Kerberos Subject 안에서 생성되어야 한다: " + HS2Driver.CONNECT_SUBJECTS);
        }
    }

    // 비보안 설정에서는 doAs 없이 커넥션을 만든다.
    @Test
    void simplePoolCreatesConnectionsWithoutSubject() throws Exception {
        setUrl("host:10000/simplesubject");
        runner.enableControllerService(pool);
        try (Connection c = pool.getConnection()) {
            assertNotNull(c);
        }
        assertFalse(HS2Driver.CONNECT_SUBJECTS.isEmpty());
        assertTrue(HS2Driver.CONNECT_SUBJECTS.stream().allMatch("<none>"::equals), String.valueOf(HS2Driver.CONNECT_SUBJECTS));
    }

    @Test
    void disableShouldLogoutKerberosUser() throws Exception {
        setUrl("host:10000/krblogout");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-krb.xml", true).toString());
        final FakeKerberosUser user = new FakeKerberosUser();
        addKerberosService(user);
        runner.enableControllerService(pool);

        runner.disableControllerService(pool);

        assertEquals(1, user.logoutCount.get());
    }

    @Test
    void reEnableWithoutSecurityShouldDropStaleKerberosState() throws Exception {
        setUrl("host:10000/krbstale");
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-krb.xml", true).toString());
        final FakeKerberosUser user = new FakeKerberosUser();
        addKerberosService(user);
        runner.enableControllerService(pool);
        runner.disableControllerService(pool);

        // 비보안 설정으로 전환 후 재활성화
        runner.setProperty(pool, VendorHive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                writeSiteXml("hive-site-simple.xml", false).toString());
        runner.removeProperty(pool, KERBEROS_USER_SERVICE_PROPERTY);
        runner.enableControllerService(pool);

        try (Connection c = pool.getConnection()) {
            assertNotNull(c);
        }
        assertEquals(0, user.reloginCheckCount.get(), "비보안 재활성화 후에는 이전 KerberosUser를 사용하면 안 된다");
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static String rootCauseChain(final Throwable t) {
        final StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        while (cur != null) {
            sb.append(cur.getClass().getName()).append(": ").append(cur.getMessage()).append(" | ");
            cur = cur.getCause();
        }
        return sb.toString();
    }

    /** 격리 클래스로더로 정의된 풀 인스턴스를 DBCPService 인터페이스로 다룬다. */
    private static final class VendorHiveConnectionPoolHandle {
        private final org.apache.nifi.controller.ControllerService service;

        VendorHiveConnectionPoolHandle(final org.apache.nifi.controller.ControllerService service) {
            this.service = service;
        }

        Connection getConnection() {
            return ((org.apache.nifi.dbcp.DBCPService) service).getConnection();
        }
    }

    /**
     * VendorHive3ConnectionPool만 child-first로 다시 정의하고 com.cloudera.* 는 숨기는 클래스로더.
     * 그 외 클래스(NiFi API, DBCP 등)는 부모에 위임해 타입 호환성을 유지한다.
     */
    private static final class HidingClassLoader extends ClassLoader implements AutoCloseable {
        private static final String HIDDEN_PREFIX = "com.cloudera.";
        private static final String ISOLATED_PREFIX = VendorHive3ConnectionPool.class.getName();

        HidingClassLoader(final ClassLoader parent) {
            super(parent);
        }

        @Override
        protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
            if (name.startsWith(HIDDEN_PREFIX)) {
                throw new ClassNotFoundException(name);
            }
            if (name.startsWith(ISOLATED_PREFIX)) {
                synchronized (getClassLoadingLock(name)) {
                    Class<?> c = findLoadedClass(name);
                    if (c == null) {
                        final String resource = name.replace('.', '/') + ".class";
                        try (InputStream in = getParent().getResourceAsStream(resource)) {
                            if (in == null) {
                                throw new ClassNotFoundException(name);
                            }
                            final byte[] bytes = in.readAllBytes();
                            c = defineClass(name, bytes, 0, bytes.length);
                        } catch (final IOException e) {
                            throw new ClassNotFoundException(name, e);
                        }
                    }
                    if (resolve) {
                        resolveClass(c);
                    }
                    return c;
                }
            }
            return super.loadClass(name, resolve);
        }

        @Override
        public void close() {
            // no resources
        }
    }

    /** 가짜 KerberosUserService. */
    private static final class FakeKerberosUserService extends AbstractControllerService implements KerberosUserService {
        private final FakeKerberosUser user;

        FakeKerberosUserService(final FakeKerberosUser user) {
            this.user = user;
        }

        @Override
        public KerberosUser createKerberosUser() {
            return user;
        }
    }

    /** 가짜 KerberosUser: 로그인/로그아웃/재로그인 호출을 기록하고 KerberosPrincipal Subject로 doAs 한다. */
    private static final class FakeKerberosUser implements KerberosUser {
        static final String PRINCIPAL = "hive@EXAMPLE.COM";

        final AtomicInteger loginCount = new AtomicInteger();
        final AtomicInteger logoutCount = new AtomicInteger();
        final AtomicInteger reloginCheckCount = new AtomicInteger();
        volatile boolean failLogin;
        volatile boolean failRelogin;
        private volatile boolean loggedIn;
        private final Subject subject;

        FakeKerberosUser() {
            subject = new Subject();
            subject.getPrincipals().add(new KerberosPrincipal(PRINCIPAL));
        }

        @Override
        public void login() {
            loginCount.incrementAndGet();
            if (failLogin) {
                throw new KerberosLoginException("Simulated Kerberos login failure for " + PRINCIPAL);
            }
            loggedIn = true;
        }

        @Override
        public void logout() {
            logoutCount.incrementAndGet();
            loggedIn = false;
        }

        @Override
        public <T> T doAs(final PrivilegedAction<T> action) {
            return Subject.doAs(subject, action);
        }

        @Override
        public <T> T doAs(final PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
            return Subject.doAs(subject, action);
        }

        @Override
        public boolean checkTGTAndRelogin() {
            reloginCheckCount.incrementAndGet();
            if (failRelogin) {
                throw new KerberosLoginException("Simulated relogin failure");
            }
            return false;
        }

        @Override
        public boolean isLoggedIn() {
            return loggedIn;
        }

        @Override
        public String getPrincipal() {
            return PRINCIPAL;
        }

        @Override
        public AppConfigurationEntry getConfigurationEntry() {
            return null;
        }
    }
}
