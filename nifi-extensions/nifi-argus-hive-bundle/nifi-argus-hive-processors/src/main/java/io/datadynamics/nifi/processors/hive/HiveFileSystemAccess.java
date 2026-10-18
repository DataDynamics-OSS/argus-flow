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

import io.datadynamics.nifi.processors.hive.util.AuthenticationFailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * 파티션 디렉토리를 읽기 위한 Hadoop FileSystem 접근. {@code hive3-config-resources}(core-site·hdfs-site·hive-site)와
 * {@code kerberos-user-service} 를 {@link PutHive3Streaming} 과 같은 방식({@link HiveConfigurator})으로 쓴다.
 * 보안 설정이면 KerberosUser 로 UGI 를 얻고, 호출마다 TGT 를 확인해 필요하면 재로그인한다.
 */
final class HiveFileSystemAccess {

    /** 파일시스템 작업. */
    interface FileSystemAction<T> {
        T run(FileSystem fileSystem, Path path) throws IOException;
    }

    private final Configuration configuration;
    private final UserGroupInformation ugi;
    private final KerberosUser kerberosUser;

    private HiveFileSystemAccess(final Configuration configuration, final UserGroupInformation ugi, final KerberosUser kerberosUser) {
        this.configuration = configuration;
        this.ugi = ugi;
        this.kerberosUser = kerberosUser;
    }

    /**
     * 설정을 읽고 로그인한다.
     *
     * @throws ProcessException 보안 설정인데 Kerberos User Service 가 없거나 인증 실패
     */
    static HiveFileSystemAccess open(final String configFiles, final KerberosUserService kerberosUserService, final ComponentLog log) {
        final HiveConfigurator configurator = new HiveConfigurator();
        final Configuration configuration = configurator.getConfigurationFromFiles(configFiles);
        configurator.preload(configuration);
        if (SecurityUtil.isSecurityEnabled(configuration)) {
            if (kerberosUserService == null) {
                throw new ProcessException("설정이 Kerberos 인증을 요구하므로 Kerberos 사용자 서비스(kerberos-user-service)를 지정해야 한다");
            }
            final KerberosUser kerberosUser = kerberosUserService.createKerberosUser();
            try {
                log.info("Hadoop FileSystem 접근을 위해 {} 로 Kerberos 로그인한다", kerberosUser.getPrincipal());
                return new HiveFileSystemAccess(configuration, configurator.authenticate(configuration, kerberosUser), kerberosUser);
            } catch (final AuthenticationFailedException e) {
                throw new ProcessException(e);
            }
        }
        try {
            return new HiveFileSystemAccess(configuration, SecurityUtil.loginSimple(configuration), null);
        } catch (final IOException e) {
            throw new ProcessException("Hadoop 사용자를 정하지 못했다", e);
        }
    }

    /**
     * {@code location} 의 FileSystem 으로 작업을 UGI 권한으로 실행한다.
     */
    <T> T doAs(final String location, final FileSystemAction<T> action) throws IOException {
        if (kerberosUser != null) {
            try {
                kerberosUser.checkTGTAndRelogin();
            } catch (final KerberosLoginException e) {
                throw new IOException("Kerberos 재로그인 실패: " + kerberosUser.getPrincipal(), e);
            }
        }
        final Path path = new Path(location);
        try {
            return ugi.doAs((PrivilegedExceptionAction<T>) () -> action.run(path.getFileSystem(configuration), path));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("중단됨", e);
        }
    }

    void close() {
        if (kerberosUser != null) {
            try {
                kerberosUser.logout();
            } catch (final KerberosLoginException e) {
                // 종료 중에는 무시한다
            }
        }
    }
}
