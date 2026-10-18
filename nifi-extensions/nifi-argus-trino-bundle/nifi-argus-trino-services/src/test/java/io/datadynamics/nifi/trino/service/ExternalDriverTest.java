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
import io.trino.jdbc.TrinoConnection;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Database Driver Location(s)/Class Name: child-first 로드, 실패 메시지, Trino API 없는 드라이버, 클래스로더 close.
 */
class ExternalDriverTest {

    /** 테스트 클래스패스의 trino-jdbc-483 jar(~/.m2) 를 '외부 jar' 로 쓴다. */
    static String trinoJar() throws Exception {
        return Path.of(NonRegisteringTrinoDriver.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
    }

    static String h2Jar() throws Exception {
        return Path.of(org.h2.Driver.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
    }

    private TestRunner runner;
    private StandardTrinoConnectionService service;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardTrinoConnectionService();
        runner.addControllerService("trino", service);
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:trino://trino.example.com:8080");
        runner.setProperty(service, StandardTrinoConnectionService.USER, "etl");
    }

    @Test
    void bundledDriverByDefault() throws Exception {
        runner.enableControllerService(service);
        final DriverHandle handle = service.currentDriverHandle();
        assertFalse(handle.isExternal());
        assertInstanceOf(StandardTrinoConnectionService.ServiceTrinoDriver.class, handle.driver());
        try (Connection connection = service.getConnection()) {
            assertTrue(connection.isWrapperFor(TrinoConnection.class));
        }
    }

    @Test
    void bundledTrinoDriverClassNameUsesNonRegisteringDriver() {
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "io.trino.jdbc.TrinoDriver");
        runner.enableControllerService(service);
        assertFalse(service.currentDriverHandle().isExternal());
    }

    @Test
    void externalJarIsLoadedChildFirst() throws Exception {
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_LOCATION, trinoJar());
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "io.trino.jdbc.TrinoDriver");
        runner.assertValid(service);
        runner.enableControllerService(service);

        final DriverHandle handle = service.currentDriverHandle();
        assertTrue(handle.isExternal());
        final ClassLoader driverLoader = handle.driver().getClass().getClassLoader();
        assertInstanceOf(ChildFirstClassLoader.class, driverLoader);
        assertNotSame(StandardTrinoConnectionService.class.getClassLoader(), driverLoader);
        assertNotSame(NonRegisteringTrinoDriver.class, handle.driver().getClass().getSuperclass(),
                "외부 jar 의 클래스는 번들 클래스와 다른 Class 여야 한다(child-first)");
        assertEquals("io.trino.jdbc.NonRegisteringTrinoDriver", handle.driver().getClass().getName(),
                "TrinoDriver 대신 같은 jar 의 NonRegisteringTrinoDriver 를 인스턴스화한다(전역 등록 회피)");

        try (Connection connection = service.getConnection()) {
            assertFalse(connection.isWrapperFor(TrinoConnection.class), "번들 TrinoConnection 과 다른 클래스로더의 연결이다");
            assertEquals("io.trino.jdbc.TrinoConnection", connection.unwrap(Connection.class).getClass().getName());
        }

        runner.disableControllerService(service);
        assertTrue(((ChildFirstClassLoader) driverLoader).isClosed(), "disable 시 클래스로더를 닫는다");
        assertNull(service.currentDriverHandle());
    }

    @Test
    void driverWithoutTrinoApiStillRunsQueries() throws Exception {
        runner.setProperty(service, StandardTrinoConnectionService.TRINO_URL, "jdbc:h2:mem:external_driver_test;DB_CLOSE_DELAY=-1");
        runner.setProperty(service, StandardTrinoConnectionService.USER, "sa");
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_LOCATION, h2Jar());
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "org.h2.Driver");
        runner.assertValid(service);
        runner.enableControllerService(service);
        assertInstanceOf(ChildFirstClassLoader.class, service.currentDriverHandle().driver().getClass().getClassLoader());

        try (Connection connection = service.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE t (id INT)");
            assertEquals(2, statement.executeUpdate("INSERT INTO t VALUES (1), (2)"));
            assertNull(service.getQueryInfo(statement), "Trino API 가 없으면 query info 는 null");
            try (ResultSet rs = statement.executeQuery("SELECT count(*) FROM t")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertNull(service.getQueryInfo(rs));
            }
        }
        final ChildFirstClassLoader loader = service.currentDriverHandle().classLoader();
        runner.disableControllerService(service);
        assertTrue(loader.isClosed());
    }

    @Test
    void invalidDriverConfigurationsAreReported(@TempDir final Path dir) throws Exception {
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_LOCATION, trinoJar());
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "com.example.NoSuchDriver");
        runner.assertNotValid(service);

        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "java.lang.String");
        runner.assertNotValid(service);

        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "io.trino.jdbc.TrinoDriver");
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_LOCATION, dir.resolve("missing.jar").toString());
        runner.assertNotValid(service);

        // 드라이버가 URL 을 받아들이지 않음
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_LOCATION, h2Jar());
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "org.h2.Driver");
        runner.assertNotValid(service);

        // 번들 드라이버가 아닌데 클래스가 없음(위치 미지정)
        runner.removeProperty(service, StandardTrinoConnectionService.DB_DRIVER_LOCATION);
        runner.setProperty(service, StandardTrinoConnectionService.DB_DRIVER_CLASS_NAME, "com.example.NoSuchDriver");
        runner.assertNotValid(service);
    }

    @Test
    void driverHandleFailureMessages(@TempDir final Path dir) throws Exception {
        final List<URL> jar = List.of(Path.of(trinoJar()).toUri().toURL());
        final ClassLoader parent = getClass().getClassLoader();
        final DriverHandle.DriverLoadException notFound = assertThrows(DriverHandle.DriverLoadException.class,
                () -> DriverHandle.load("com.example.NoSuchDriver", jar, parent));
        assertTrue(notFound.getMessage().contains("찾을 수 없다"), notFound.getMessage());
        final DriverHandle.DriverLoadException notDriver = assertThrows(DriverHandle.DriverLoadException.class,
                () -> DriverHandle.load("java.lang.String", jar, parent));
        assertTrue(notDriver.getMessage().contains("java.sql.Driver"), notDriver.getMessage());
        try (DriverHandle handle = DriverHandle.load("io.trino.jdbc.TrinoDriver", jar, parent)) {
            final DriverHandle.DriverLoadException rejected = assertThrows(DriverHandle.DriverLoadException.class,
                    () -> handle.requireAcceptsUrl("jdbc:h2:mem:x"));
            assertTrue(rejected.getMessage().contains("받아들이지 않는다"), rejected.getMessage());
            handle.requireAcceptsUrl("jdbc:trino://h:8080");
            assertNotNull(handle.classLoader());
        }
    }

    @Test
    void childFirstDelegationRules() {
        assertTrue(ChildFirstClassLoader.isParentFirst("java.sql.Driver"));
        assertTrue(ChildFirstClassLoader.isParentFirst("javax.net.ssl.SSLContext"));
        assertTrue(ChildFirstClassLoader.isParentFirst("org.apache.nifi.logging.ComponentLog"));
        assertTrue(ChildFirstClassLoader.isParentFirst("org.slf4j.Logger"));
        assertFalse(ChildFirstClassLoader.isParentFirst("io.trino.jdbc.TrinoDriver"));
    }
}
