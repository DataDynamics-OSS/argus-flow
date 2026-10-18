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
package io.datadynamics.nifi.processors.hive.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.MockPropertyConfiguration.CreatedControllerService;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

/**
 * {@link KerberosPropertiesMigration} 단위 테스트 (nifi-mock {@link MockPropertyConfiguration} 사용).
 * Hive3ConnectionPool 형식(principal/keytab/password 모두 있음)의 프로퍼티 이름으로 경계 조건을 검증한다.
 */
class KerberosPropertiesMigrationTest {

    private static final String CREDENTIALS = "kerberos-credentials-service";
    private static final String PRINCIPAL = "Kerberos Principal";
    private static final String KEYTAB = "Kerberos Keytab";
    private static final String PASSWORD = "Kerberos Password";
    private static final String USER_SERVICE = "kerberos-user-service";

    private static MockPropertyConfiguration migrate(final Map<String, String> properties) {
        final MockPropertyConfiguration config = new MockPropertyConfiguration(new HashMap<>(properties));
        KerberosPropertiesMigration.migrate(config, CREDENTIALS, PRINCIPAL, KEYTAB, PASSWORD, USER_SERVICE);
        return config;
    }

    private static void assertOldPropertiesGone(final MockPropertyConfiguration config) {
        for (String name : Set.of(CREDENTIALS, PRINCIPAL, KEYTAB, PASSWORD)) {
            assertFalse(config.hasProperty(name), name + " 프로퍼티가 제거되어야 한다");
        }
    }

    @Test
    void keytabTakesPrecedenceOverPassword() {
        // 1.28 검증은 둘 다 설정을 막았지만, 런타임은 keytab을 우선했다.
        final MockPropertyConfiguration config = migrate(Map.of(PRINCIPAL, "nifi@EXAMPLE.COM", KEYTAB, "/k.keytab", PASSWORD, "pw"));

        final PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertEquals(1, result.getCreatedControllerServices().size());
        final CreatedControllerService created = result.getCreatedControllerServices().iterator().next();
        assertEquals(KerberosPropertiesMigration.KEYTAB_USER_SERVICE, created.implementationClassName());
        assertEquals(Map.of("Kerberos Principal", "nifi@EXAMPLE.COM", "Kerberos Keytab", "/k.keytab"), created.serviceProperties());
        assertEquals(created.id(), config.getRawPropertyValue(USER_SERVICE).orElseThrow());
        assertOldPropertiesGone(config);
    }

    @Test
    void credentialsServiceIsRemovedWithoutCreatingService() {
        // Kerberos Credentials Service는 NiFi 2에 API가 없어 principal/keytab을 읽을 수 없다(1.28 런타임에서도 명시 값보다 우선).
        final MockPropertyConfiguration config = migrate(Map.of(CREDENTIALS, "old-credentials-service-id", PRINCIPAL, "nifi@EXAMPLE.COM", PASSWORD, "pw"));

        final PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertTrue(result.getCreatedControllerServices().isEmpty());
        assertFalse(config.isPropertySet(USER_SERVICE));
        assertTrue(result.getPropertiesRemoved().contains(CREDENTIALS));
        assertOldPropertiesGone(config);
    }

    @Test
    void existingUserServiceIsKept() {
        final MockPropertyConfiguration config = migrate(Map.of(USER_SERVICE, "existing-id", PRINCIPAL, "nifi@EXAMPLE.COM", KEYTAB, "/k.keytab"));

        assertTrue(config.toPropertyMigrationResult().getCreatedControllerServices().isEmpty());
        assertEquals("existing-id", config.getRawPropertyValue(USER_SERVICE).orElseThrow());
        assertOldPropertiesGone(config);
    }

    @Test
    void principalWithoutSecretCreatesNothing() {
        final MockPropertyConfiguration config = migrate(Map.of(PRINCIPAL, "nifi@EXAMPLE.COM"));

        assertTrue(config.toPropertyMigrationResult().getCreatedControllerServices().isEmpty());
        assertFalse(config.isPropertySet(USER_SERVICE));
        assertOldPropertiesGone(config);
    }

    @Test
    void expressionLanguageValuesAreCopiedRaw() {
        final MockPropertyConfiguration config = migrate(Map.of(PRINCIPAL, "${principal}", KEYTAB, "${keytab.path}"));

        final CreatedControllerService created = config.toPropertyMigrationResult().getCreatedControllerServices().iterator().next();
        assertEquals("${principal}", created.serviceProperties().get("Kerberos Principal"));
        assertEquals("${keytab.path}", created.serviceProperties().get("Kerberos Keytab"));
    }

    @Test
    void currentFlowIsUnchanged() {
        final MockPropertyConfiguration config = migrate(Map.of(USER_SERVICE, "existing-id"));

        final PropertyMigrationResult result = config.toPropertyMigrationResult();
        assertTrue(result.getCreatedControllerServices().isEmpty());
        assertTrue(result.getPropertiesUpdated().isEmpty());
        assertEquals(Map.of(USER_SERVICE, "existing-id"), config.getRawProperties());
    }
}
