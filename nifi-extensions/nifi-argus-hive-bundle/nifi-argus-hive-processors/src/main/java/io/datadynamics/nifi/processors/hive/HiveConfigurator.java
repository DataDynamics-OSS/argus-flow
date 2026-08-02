/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/util/hive/HiveConfigurator.java
 */
package io.datadynamics.nifi.processors.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosUser;
import io.datadynamics.nifi.processors.hive.util.AuthenticationFailedException;
import io.datadynamics.nifi.processors.hive.util.ValidationResources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hive 관련 프로세서 및 컨트롤러 서비스에서 공통으로 사용하는 설정 로딩/검증/Kerberos 인증 유틸리티.
 * hive-site.xml 등 설정 파일을 읽어 HiveConf/Configuration을 구성하고,
 * 보안(Kerberos)이 활성화된 환경에서 UserGroupInformation을 획득하는 책임을 진다.
 */
public class HiveConfigurator {

    // Cloudera 배포판 대응: Kerberos User Service 기반 인증 설정이 올바른지 검증한다
    public Collection<ValidationResult> validate(String configFiles, KerberosUserService kerberosUserService, AtomicReference<ValidationResources> validationResourceHolder, ComponentLog log) {
        List<ValidationResult> results = new ArrayList<>();
        Configuration hiveConfig = getConfigurationForValidation(validationResourceHolder, configFiles, log);
        boolean isSecurityEnabled = SecurityUtil.isSecurityEnabled(hiveConfig);
        if (isSecurityEnabled) {
            if (kerberosUserService == null) {
                results.add((new ValidationResult.Builder()).valid(false).subject(getClass().getSimpleName())
                        .explanation("Kerberos User Service must be provided when using a secure configuration").build());
            } else {
                results.add((new ValidationResult.Builder()).valid(true).build());
            }
        } else if (kerberosUserService != null) {
            log.warn("Configuration does not have security enabled, Kerberos User Service will be ignored");
        } else {
            results.add((new ValidationResult.Builder()).valid(true).build());
        }
        return results;
    }


    // 검증에 사용할 Configuration을 반환한다. 캐시된 리소스가 없거나 설정 파일 목록이 변경된 경우에만 다시 로드하여
    // 매 검증(validate)마다 파일을 반복해서 읽는 비용을 피한다.
    public Configuration getConfigurationForValidation(AtomicReference<ValidationResources> validationResourceHolder, String configFiles, ComponentLog log) {
        ValidationResources resources = validationResourceHolder.get();

        // 홀더에 리소스가 없거나, 홀더에 로드된 리소스가 현재 설정 파일 목록과 다르면
        // Configuration을 새로 로드하여 홀더에 반영한다
        if (resources == null || !configFiles.equals(resources.getConfigResources())) {
            log.debug("Reloading validation resources");
            resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
            validationResourceHolder.set(resources);
        }

        return resources.getConfiguration();
    }

    // 콤마로 구분된 설정 파일 목록(hive-site.xml 등)을 읽어 HiveConf 객체로 구성한다.
    public HiveConf getConfigurationFromFiles(final String configFiles) {
        final HiveConf hiveConfig = new HiveConf();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hiveConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hiveConfig;
    }

    // FileSystem을 미리 한 번 초기화(및 종료)하여 설정을 "예열"한다.
    // 이렇게 하면 이후 실제 사용 시점에 발생할 수 있는 초기화 지연이나 문제를 앞당겨 확인할 수 있다.
    public void preload(Configuration configuration) {
        try {
            FileSystem.get(configuration).close();
            UserGroupInformation.setConfiguration(configuration);
        } catch (IOException ioe) {
            // 이 Configuration을 이후에 사용할 때 어차피 동일한 오류로 실패하게 되므로 예외를 억제한다
        }
    }

    /**
     * 주어진 {@link Configuration}과 {@link KerberosUser}를 사용하여 {@link UserGroupInformation}을 획득한다.
     *
     * @param hiveConfig   획득한 UserGroupInformation에 적용할 Configuration
     * @param kerberosUser 인증에 사용할 KerberosUser
     * @return 주어진 KerberosUser의 Subject를 사용하여 생성된 UserGroupInformation 인스턴스
     * @throws AuthenticationFailedException 인증에 실패한 경우
     * @see SecurityUtil#getUgiForKerberosUser(Configuration, KerberosUser)
     */
    public UserGroupInformation authenticate(final Configuration hiveConfig, KerberosUser kerberosUser) throws AuthenticationFailedException {
        try {
            return SecurityUtil.getUgiForKerberosUser(hiveConfig, kerberosUser);
        } catch (IOException ioe) {
            throw new AuthenticationFailedException("Kerberos Authentication for Hive failed", ioe);
        }
    }

    /**
     * Apache NiFi 1.5.0부터, 이 클래스가 principal 인증에 사용하는
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}의 변경으로 인해
     * Hive 컨트롤러 서비스는 더 이상 명시적으로 재로그인(relogin)을 시도하지 않는다.
     * 자세한 내용은 {@link SecurityUtil#loginKerberos(Configuration, String, String)}의 문서를 참고한다.
     * <p/>
     * 이전 버전의 NiFi에서는 Hive 컨트롤러 서비스가 활성화될 때
     * {@link HiveConfigurator#authenticate(Configuration, String, String, long)}에 의해
     * {@link org.apache.nifi.hadoop.KerberosTicketRenewer}가 별도 스레드로 시작되었다.
     * 이렇게 별도 스레드에서 명시적으로 재로그인을 수행하면, 동일한 {@link UserGroupInformation} 인스턴스를
     * 참조하는 스레드에서 hadoop/Hive 코드가 암묵적으로 수행하는 재로그인 시도와 경쟁 상태(race condition)가
     * 발생할 수 있었다. 두 스레드 중 하나가 {@link UserGroupInformation} 내부의 {@link javax.security.auth.Subject}를
     * 초기화하거나 예기치 않은 상태로 만들어 버리면, 다른 스레드가 해당 Subject를 사용하려는 시점에
     * 인증 실패가 발생하여 Hive 컨트롤러 서비스가 복구 불가능한 상태에 빠질 수 있었다.
     *
     * @see SecurityUtil#loginKerberos(Configuration, String, String)
     * @deprecated {@link SecurityUtil#getUgiForKerberosUser(Configuration, KerberosUser)}를 사용할 것
     */
    @Deprecated
    public UserGroupInformation authenticate(final Configuration hiveConfig, String principal, String keyTab) throws AuthenticationFailedException {
        UserGroupInformation ugi;
        try {
            ugi = SecurityUtil.loginKerberos(hiveConfig, principal, keyTab);
        } catch (IOException ioe) {
            throw new AuthenticationFailedException("Kerberos Authentication for Hive failed", ioe);
        }
        return ugi;
    }

    /**
     * Apache NiFi 1.5.0부터 이 메서드는 더 이상 사용되지 않으며(deprecated), 현재는
     * {@link HiveConfigurator#authenticate(Configuration, String, String)}를 호출하는 래퍼 메서드일 뿐이다.
     * 더 이상 명시적 재로그인을 수행하기 위한 {@link org.apache.nifi.hadoop.KerberosTicketRenewer}를 시작하지 않는다.
     *
     * @see HiveConfigurator#authenticate(Configuration, String, String)
     */
    @Deprecated
    public UserGroupInformation authenticate(final Configuration hiveConfig, String principal, String keyTab, long ticketRenewalPeriod) throws AuthenticationFailedException {
        return authenticate(hiveConfig, principal, keyTab);
    }
}
