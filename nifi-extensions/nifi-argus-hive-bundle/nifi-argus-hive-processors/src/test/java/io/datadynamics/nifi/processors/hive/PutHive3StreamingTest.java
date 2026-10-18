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
package io.datadynamics.nifi.processors.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.MockPropertyConfiguration.CreatedControllerService;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

/**
 * PutHive3Streaming의 NiFi 1.28 Kerberos 프로퍼티 마이그레이션(migrateProperties) 테스트.
 * 1.28 PutHive3Streaming에는 kerberos-credentials-service, kerberos-principal, kerberos-password가 있었다(keytab 없음).
 *
 * TestRunner.migrateProperties()는 생성 요청된 컨트롤러 서비스 클래스를 실제로 로드하는데,
 * KerberosPasswordUserService 구현체(nifi-kerberos-user-service)는 이 모듈의 클래스패스에 없으므로
 * nifi-mock의 {@link MockPropertyConfiguration}으로 직접 검증한다.
 */
class PutHive3StreamingTest {

    private static final String USER_SERVICE = "kerberos-user-service";

    private static MockPropertyConfiguration migrate(final Map<String, String> properties) {
        final MockPropertyConfiguration config = new MockPropertyConfiguration(new HashMap<>(properties));
        new PutHive3Streaming().migrateProperties(config);
        return config;
    }

    @Test
    void migratesPrincipalAndPasswordToPasswordUserService() {
        final MockPropertyConfiguration config = migrate(Map.of(
                "hive3-stream-metastore-uri", "thrift://metastore:9083",
                "kerberos-principal", "nifi@EXAMPLE.COM",
                "kerberos-password", "s3cr3t"));

        final PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertEquals(1, result.getCreatedControllerServices().size());
        final CreatedControllerService created = result.getCreatedControllerServices().iterator().next();
        assertEquals("org.apache.nifi.kerberos.KerberosPasswordUserService", created.implementationClassName());
        assertEquals(Map.of("Kerberos Principal", "nifi@EXAMPLE.COM", "Kerberos Password", "s3cr3t"), created.serviceProperties());

        assertEquals(created.id(), config.getRawPropertyValue(USER_SERVICE).orElseThrow());
        assertFalse(config.hasProperty("kerberos-principal"));
        assertFalse(config.hasProperty("kerberos-password"));
        assertEquals("thrift://metastore:9083", config.getRawPropertyValue("hive3-stream-metastore-uri").orElseThrow());
    }

    @Test
    void removesKerberosCredentialsService() {
        final MockPropertyConfiguration config = migrate(Map.of("kerberos-credentials-service", "old-service-id"));

        final PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertTrue(result.getCreatedControllerServices().isEmpty());
        assertTrue(result.getPropertiesRemoved().contains("kerberos-credentials-service"));
        assertFalse(config.hasProperty("kerberos-credentials-service"));
        assertFalse(config.isPropertySet(USER_SERVICE));
    }

    @Test
    void currentPropertiesAreUnchanged() {
        final Map<String, String> current = Map.of("hive3-stream-metastore-uri", "thrift://metastore:9083", USER_SERVICE, "svc");
        final MockPropertyConfiguration config = migrate(current);

        assertTrue(config.toPropertyMigrationResult().getCreatedControllerServices().isEmpty());
        assertEquals(current, config.getRawProperties());
    }
}
