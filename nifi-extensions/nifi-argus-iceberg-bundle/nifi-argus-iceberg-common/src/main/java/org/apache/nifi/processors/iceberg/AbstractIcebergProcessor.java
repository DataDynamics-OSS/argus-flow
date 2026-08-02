/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/AbstractIcebergProcessor.java
 */
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
package org.apache.nifi.processors.iceberg;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.services.iceberg.IcebergCatalogService;
import org.ietf.jgss.GSSException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.hadoop.SecurityUtil.getUgiForKerberosUser;
import static org.apache.nifi.processors.iceberg.IcebergUtils.findCause;
import static org.apache.nifi.processors.iceberg.IcebergUtils.getConfigurationFromFiles;

/**
 * Iceberg 프로세서들의 공통 기반 클래스.
 * Kerberos 인증 초기화 및 재로그인 처리, 인증 예외 판별, 클래스로더 격리 키 제공 등
 * 모든 Iceberg 프로세서에 공통되는 기능을 제공한다.
 */
@RequiresInstanceClassLoading(cloneAncestorResources = true)
public abstract class AbstractIcebergProcessor extends AbstractProcessor implements ClassloaderIsolationKeyProvider {

    public static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder()
            .name("catalog-service")
            .displayName("카탈로그 서비스")
            .description("테이블의 메타데이터 파일에 대한 참조를 처리하는 데 사용할 컨트롤러 서비스를 지정합니다.")
            .identifiesControllerService(IcebergCatalogService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos 사용자 서비스")
            .description("Kerberos 인증에 사용할 Kerberos User 컨트롤러 서비스를 지정합니다.")
            .identifiesControllerService(KerberosUserService.class)
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("작업이 실패했고, 유효하지 않은 데이터나 스키마 등의 이유로 재시도해도 다시 실패할 것으로 예상되는 경우 FlowFile은 이 관계로 전달됩니다.")
            .build();

    // Kerberos 인증에 사용된 사용자 정보. static 참조이므로 동일 클래스로더 내 모든 프로세서 인스턴스가 공유한다.
    protected static final AtomicReference<KerberosUser> kerberosUserReference = new AtomicReference<>();
    // Kerberos 인증 후 발급받은 Hadoop UserGroupInformation(UGI)을 보관하는 참조.
    protected final AtomicReference<UserGroupInformation> ugiReference = new AtomicReference<>();

    // 프로세서가 스케줄될 때 Kerberos 자격 증명을 초기화한다.
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        initKerberosCredentials(context);
    }

    /**
     * Kerberos User 컨트롤러 서비스가 설정되어 있으면 KerberosUser를 생성(없으면 새로 생성, 있으면 기존 것을 재사용)하고,
     * 카탈로그 서비스의 Hadoop 설정 파일을 기반으로 UserGroupInformation을 생성하여 ugiReference에 저장한다.
     */
    protected synchronized void initKerberosCredentials(ProcessContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);

        if (kerberosUserService != null) {
            KerberosUser kerberosUser;
            if (kerberosUserReference.get() == null) {
                kerberosUser = kerberosUserService.createKerberosUser();
            } else {
                kerberosUser = kerberosUserReference.get();
            }
            try {
                ugiReference.set(getUgiForKerberosUser(getConfigurationFromFiles(catalogService.getConfigFilePaths()), kerberosUser));
            } catch (IOException e) {
                throw new ProcessException("Kerberos authentication failed", e);
            }
            kerberosUserReference.set(kerberosUser);
        }
    }

    /**
     * 세션에서 FlowFile을 가져와 실제 처리를 수행한다. Kerberos User가 설정되어 있는 경우
     * UGI의 doAs를 통해 해당 사용자 권한으로 doOnTrigger를 실행하고, 그렇지 않으면 바로 실행한다.
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final KerberosUser kerberosUser = kerberosUserReference.get();
        if (kerberosUser == null) {
            doOnTrigger(context, session, flowFile);
        } else {
            try {
                getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
                    doOnTrigger(context, session, flowFile);
                    return null;
                });

            } catch (Exception e) {
                if (!handleAuthErrors(e, session, context)) {
                    getLogger().error("Privileged action failed with kerberos user " + kerberosUser, e);
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                }
            }
        }
    }

    /**
     * Kerberos User 컨트롤러 서비스가 설정되어 있으면 해당 사용자의 principal을 클래스로더 격리 키로 반환하여,
     * 서로 다른 Kerberos 사용자를 사용하는 프로세서 인스턴스끼리 클래스로더를 공유하지 않도록 한다.
     */
    @Override
    public String getClassloaderIsolationKey(PropertyContext context) {
        try {
            final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            if (kerberosUserService != null) {
                final KerberosUser kerberosUser = kerberosUserService.createKerberosUser();
                return kerberosUser.getPrincipal();
            }
        } catch (IllegalStateException e) {
            // Kerberos 컨트롤러 서비스가 비활성화되어 있어 격리 키의 이 부분을 아직 결정할 수 없다.
        }

        return null;
    }

    // TGT(Ticket Granting Ticket)를 점검하고 필요하면 재로그인한 뒤 현재 UserGroupInformation을 반환한다.
    private UserGroupInformation getUgi() {
        SecurityUtil.checkTGTAndRelogin(getLogger(), kerberosUserReference.get());
        return ugiReference.get();
    }

    /**
     * 예외의 원인 체인에서 자격 증명이 없는(GSSException.NO_CRED) GSSException을 찾는다.
     * 해당 원인이 존재하면 Kerberos 재로그인을 시도하고, 세션을 롤백한 뒤 컨텍스트를 yield 처리한다.
     *
     * @return 인증 오류를 처리하여 재시도를 예약했으면 true, 인증 오류가 아니어서 처리하지 않았으면 false
     */
    protected boolean handleAuthErrors(Throwable t, ProcessSession session, ProcessContext context) {
        final Optional<GSSException> causeOptional = findCause(t, GSSException.class, gsse -> GSSException.NO_CRED == gsse.getMajor());
        if (causeOptional.isPresent()) {
            getLogger().info("No valid Kerberos credential found, retrying login", causeOptional.get());
            kerberosUserReference.get().checkTGTAndRelogin();
            session.rollback();
            context.yield();
            return true;
        }
        return false;
    }

    // 하위 클래스가 실제 FlowFile 처리 로직을 구현해야 하는 추상 메서드.
    protected abstract void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException;
}
