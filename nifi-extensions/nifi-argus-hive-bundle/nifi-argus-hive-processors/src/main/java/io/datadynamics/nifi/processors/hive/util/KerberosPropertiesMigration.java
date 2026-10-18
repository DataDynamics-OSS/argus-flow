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

import org.apache.nifi.migration.PropertyConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * NiFi 1.28에서 제거된 Kerberos 프로퍼티(principal/keytab/password, Kerberos Credentials Service)를
 * NiFi 2.x의 Kerberos User Service 참조로 옮기는 마이그레이션 도우미.
 * <p>
 * 업스트림 NiFi 2.10(AbstractHadoopProcessor 등)은 같은 프로퍼티를 {@code removeProperty}로 삭제만 하고
 * 재사용할 수 있는 Kerberos 마이그레이션 헬퍼를 제공하지 않는다(nifi-migration-utils에는 ProxyServiceMigration만 있음).
 * 그래서 ProxyServiceMigration과 같은 방식으로 {@link PropertyConfiguration#createControllerService(String, Map)}를
 * 사용해 기존 값을 담은 Kerberos User 컨트롤러 서비스를 만들고 새 프로퍼티가 이를 참조하게 한다.
 * <ul>
 *     <li>principal + keytab → {@value #KEYTAB_USER_SERVICE} (Kerberos Principal / Kerberos Keytab)</li>
 *     <li>principal + password(keytab 없음) → {@value #PASSWORD_USER_SERVICE} (Kerberos Principal / Kerberos Password)</li>
 *     <li>Kerberos Credentials Service 참조 → NiFi 2에서 해당 서비스 API가 없어 값을 읽을 수 없으므로 업스트림처럼 삭제만 한다.
 *     1.28 런타임에서도 Credentials Service가 명시적 principal/keytab보다 우선했으므로, 이 경우에는 서비스를 새로 만들지 않는다.</li>
 * </ul>
 * 새 Kerberos User Service 프로퍼티가 이미 설정돼 있으면 서비스를 만들지 않고 옛 프로퍼티만 삭제한다.
 * 값은 raw 값(Expression Language 포함)을 그대로 옮긴다.
 */
public final class KerberosPropertiesMigration {

    static final String KEYTAB_USER_SERVICE = "org.apache.nifi.kerberos.KerberosKeytabUserService";
    static final String PASSWORD_USER_SERVICE = "org.apache.nifi.kerberos.KerberosPasswordUserService";

    // NiFi 2.10 KerberosKeytabUserService / KerberosPasswordUserService의 프로퍼티 이름
    static final String SERVICE_PRINCIPAL = "Kerberos Principal";
    static final String SERVICE_KEYTAB = "Kerberos Keytab";
    static final String SERVICE_PASSWORD = "Kerberos Password";

    private KerberosPropertiesMigration() {
    }

    /**
     * 옛 Kerberos 프로퍼티를 Kerberos User Service 참조로 마이그레이션한다.
     *
     * @param config                     마이그레이션 대상 프로퍼티 설정
     * @param credentialsServiceProperty 옛 Kerberos Credentials Service 프로퍼티 이름
     * @param principalProperty          옛 principal 프로퍼티 이름
     * @param keytabProperty             옛 keytab 프로퍼티 이름 (없는 컴포넌트는 {@code null})
     * @param passwordProperty           옛 password 프로퍼티 이름
     * @param userServiceProperty        새 Kerberos User Service 프로퍼티 이름
     */
    public static void migrate(final PropertyConfiguration config, final String credentialsServiceProperty,
                               final String principalProperty, final String keytabProperty,
                               final String passwordProperty, final String userServiceProperty) {
        final boolean credentialsServiceSet = config.isPropertySet(credentialsServiceProperty);
        final Optional<String> principal = rawValue(config, principalProperty);
        final Optional<String> keytab = keytabProperty == null ? Optional.empty() : rawValue(config, keytabProperty);
        final Optional<String> password = rawValue(config, passwordProperty);

        if (!config.isPropertySet(userServiceProperty) && !credentialsServiceSet && principal.isPresent()) {
            final Map<String, String> serviceProperties = new LinkedHashMap<>();
            serviceProperties.put(SERVICE_PRINCIPAL, principal.get());
            String serviceType = null;
            // 1.28 런타임과 같이 keytab을 password보다 우선한다.
            if (keytab.isPresent()) {
                serviceProperties.put(SERVICE_KEYTAB, keytab.get());
                serviceType = KEYTAB_USER_SERVICE;
            } else if (password.isPresent()) {
                serviceProperties.put(SERVICE_PASSWORD, password.get());
                serviceType = PASSWORD_USER_SERVICE;
            }
            if (serviceType != null) {
                final String serviceId = config.createControllerService(serviceType, serviceProperties);
                config.setProperty(userServiceProperty, serviceId);
            }
        }

        config.removeProperty(credentialsServiceProperty);
        config.removeProperty(principalProperty);
        if (keytabProperty != null) {
            config.removeProperty(keytabProperty);
        }
        config.removeProperty(passwordProperty);
    }

    private static Optional<String> rawValue(final PropertyConfiguration config, final String propertyName) {
        return config.getRawPropertyValue(propertyName).filter(value -> !value.isBlank());
    }
}
