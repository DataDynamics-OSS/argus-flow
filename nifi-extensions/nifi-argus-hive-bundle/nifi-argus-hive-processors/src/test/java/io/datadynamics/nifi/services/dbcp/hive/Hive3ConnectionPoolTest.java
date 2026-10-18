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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.MockPropertyConfiguration.CreatedControllerService;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import com.cloudera.hive.jdbc.HS2Driver;

import io.datadynamics.nifi.services.api.hive.Hive3DBCPService;
import io.datadynamics.nifi.services.api.hive.HiveDBCPService;
import io.datadynamics.nifi.services.api.hive.VendorHiveDBCPService;

/**
 * Hive3ConnectionPool 컨트롤러 서비스 단위 테스트.
 *
 * 서비스 인터페이스 계층(두 풀 모두 Hive3DBCPService), 가짜 HS2Driver(H2 위임)를 통한 커넥션 획득,
 * NiFi 1.28 Kerberos 프로퍼티의 migrateProperties 이전을 검증한다.
 */
class Hive3ConnectionPoolTest {

    @Test
    void bothPoolsImplementHive3DBCPService() {
        assertTrue(Hive3DBCPService.class.isAssignableFrom(Hive3ConnectionPool.class));
        assertTrue(Hive3DBCPService.class.isAssignableFrom(VendorHive3ConnectionPool.class));
        assertTrue(Hive3DBCPService.class.isAssignableFrom(VendorHiveDBCPService.class));
        assertTrue(HiveDBCPService.class.isAssignableFrom(Hive3DBCPService.class));
    }

    @Test
    void enablesAndProvidesConnections() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final Hive3ConnectionPool pool = new Hive3ConnectionPool();
        runner.addControllerService("pool", pool);
        final String url = HS2Driver.URL_PREFIX + "hive3pool_basic";
        runner.setProperty(pool, Hive3ConnectionPool.DATABASE_URL, url);
        runner.assertValid(pool);
        runner.enableControllerService(pool);
        try {
            assertEquals(url, pool.getConnectionURL());
            try (Connection c = pool.getConnection(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        } finally {
            runner.disableControllerService(pool);
        }
    }

    private static MockPropertyConfiguration migrate(final Map<String, String> properties) {
        final MockPropertyConfiguration config = new MockPropertyConfiguration(new HashMap<>(properties));
        new Hive3ConnectionPool().migrateProperties(config);
        return config;
    }

    @Test
    void migratesPrincipalAndKeytabToKeytabUserService() {
        final MockPropertyConfiguration config = migrate(Map.of(
                "hive-db-connect-url", "jdbc:hive2://h:10000/default",
                "Kerberos Principal", "nifi@EXAMPLE.COM",
                "Kerberos Keytab", "/etc/security/nifi.keytab"));

        final PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertEquals(1, result.getCreatedControllerServices().size());
        final CreatedControllerService created = result.getCreatedControllerServices().iterator().next();
        assertEquals("org.apache.nifi.kerberos.KerberosKeytabUserService", created.implementationClassName());
        assertEquals(Map.of("Kerberos Principal", "nifi@EXAMPLE.COM", "Kerberos Keytab", "/etc/security/nifi.keytab"),
                created.serviceProperties());
        assertEquals(created.id(), config.getRawPropertyValue("kerberos-user-service").orElseThrow());
        assertFalse(config.hasProperty("Kerberos Principal"));
        assertFalse(config.hasProperty("Kerberos Keytab"));
        assertEquals("jdbc:hive2://h:10000/default", config.getRawPropertyValue("hive-db-connect-url").orElseThrow());
    }

    @Test
    void migratesPrincipalAndPasswordToPasswordUserService() {
        final MockPropertyConfiguration config = migrate(Map.of(
                "Kerberos Principal", "nifi@EXAMPLE.COM",
                "Kerberos Password", "s3cr3t"));

        final CreatedControllerService created = config.toPropertyMigrationResult().getCreatedControllerServices().iterator().next();
        assertEquals("org.apache.nifi.kerberos.KerberosPasswordUserService", created.implementationClassName());
        assertEquals(Map.of("Kerberos Principal", "nifi@EXAMPLE.COM", "Kerberos Password", "s3cr3t"), created.serviceProperties());
        assertEquals(created.id(), config.getRawPropertyValue("kerberos-user-service").orElseThrow());
        assertFalse(config.hasProperty("Kerberos Password"));
    }

    @Test
    void removesKerberosCredentialsService() {
        final MockPropertyConfiguration config = migrate(Map.of("kerberos-credentials-service", "old-service-id"));

        final PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertTrue(result.getCreatedControllerServices().isEmpty());
        assertFalse(config.hasProperty("kerberos-credentials-service"));
        assertFalse(config.isPropertySet("kerberos-user-service"));
    }

    @Test
    void currentPropertiesAreUnchanged() {
        final Map<String, String> current = Map.of("hive-db-connect-url", "jdbc:hive2://h:10000/default", "kerberos-user-service", "svc");
        final MockPropertyConfiguration config = migrate(current);

        assertTrue(config.toPropertyMigrationResult().getCreatedControllerServices().isEmpty());
        assertEquals(current, config.getRawProperties());
    }
}
