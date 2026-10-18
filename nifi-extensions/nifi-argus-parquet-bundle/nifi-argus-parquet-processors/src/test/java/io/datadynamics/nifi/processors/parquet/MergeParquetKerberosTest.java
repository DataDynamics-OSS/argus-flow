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
package io.datadynamics.nifi.processors.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MergeParquet의 Kerberos 지원(P-9) 테스트.
 * 실제 KDC 없이 가짜 KerberosUserService와 UGI를 사용하여 검증 규칙, UGI doAs 실행, TGT 재로그인, 로그아웃을 확인한다.
 */
class MergeParquetKerberosTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}");

    private static final String TEST_USER = "merge-kerberos-test";

    private FakeKerberosUser kerberosUser;

    @BeforeEach
    void setUp() {
        kerberosUser = new FakeKerberosUser();
        UgiRecordingFileSystem.USERS.clear();
    }

    private static File writeConfig(Path dir, String name, boolean kerberos, boolean recordingFs) throws IOException {
        final File conf = dir.resolve(name).toFile();
        final StringBuilder sb = new StringBuilder("<?xml version=\"1.0\"?>\n<configuration>\n");
        if (recordingFs) {
            sb.append("  <property><name>fs.defaultFS</name><value>ugifs:///</value></property>\n");
            sb.append("  <property><name>fs.ugifs.impl</name><value>").append(UgiRecordingFileSystem.class.getName()).append("</value></property>\n");
        } else {
            sb.append("  <property><name>fs.defaultFS</name><value>file:///</value></property>\n");
        }
        if (kerberos) {
            sb.append("  <property><name>hadoop.security.authentication</name><value>kerberos</value></property>\n");
        }
        sb.append("</configuration>\n");
        Files.writeString(conf.toPath(), sb.toString());
        return conf;
    }

    private static void writeParquet(File file, int count) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.toURI()))
                .withSchema(SCHEMA).withConf(new Configuration()).build()) {
            for (int i = 0; i < count; i++) {
                final GenericRecord r = new GenericData.Record(SCHEMA);
                r.put("id", (long) i);
                writer.write(r);
            }
        }
    }

    private TestRunner newRunner(File conf, File src, File tgt) {
        final TestRunner runner = TestRunners.newTestRunner(new TestableMergeParquet());
        runner.setProperty(MergeParquet.PROP_HADOOP_CONFIGURATION_RESOURCES, conf.getAbsolutePath());
        runner.setProperty(MergeParquet.PROP_HDFS_SOURCE_PATH, src.getAbsolutePath() + "/*.parquet");
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_DIR, tgt.getAbsolutePath());
        runner.setProperty(MergeParquet.PROP_HDFS_TARGET_FILENAME, "merged.parquet");
        return runner;
    }

    private void addKerberosUserService(TestRunner runner) throws Exception {
        final FakeKerberosUserService service = new FakeKerberosUserService(kerberosUser);
        runner.addControllerService("kerberos-user-service", service);
        runner.enableControllerService(service);
        runner.setProperty(MergeParquet.KERBEROS_USER_SERVICE, "kerberos-user-service");
    }

    /** hadoop.security.authentication=kerberos인데 Kerberos User Service가 없으면 invalid, 지정하면 valid여야 한다. */
    @Test
    void kerberosEnabledConfig_requiresKerberosUserService(@TempDir Path tmp) throws Exception {
        final File src = Files.createDirectories(tmp.resolve("src")).toFile();
        final File tgt = Files.createDirectories(tmp.resolve("tgt")).toFile();
        final TestRunner runner = newRunner(writeConfig(tmp, "core-site.xml", true, false), src, tgt);
        runner.assertNotValid();
        addKerberosUserService(runner);
        runner.assertValid();
    }

    /** 보안 설정이 아니면 Kerberos User Service 없이도 valid여야 한다. */
    @Test
    void simpleAuthConfig_validWithoutKerberosUserService(@TempDir Path tmp) throws Exception {
        final File src = Files.createDirectories(tmp.resolve("src")).toFile();
        final File tgt = Files.createDirectories(tmp.resolve("tgt")).toFile();
        final TestRunner runner = newRunner(writeConfig(tmp, "core-site.xml", false, false), src, tgt);
        runner.assertValid();
    }

    /**
     * Kerberos 설정이면 FileSystem 생성, Merge(읽기/쓰기/rename), commitAsync 콜백의 원본 삭제가 모두 UGI doAs 안에서 실행되고,
     * 실행마다 TGT를 확인하며, 중지 시 로그아웃해야 한다.
     */
    @Test
    void kerberosEnabled_mergeAndDeleteRunAsUgi_reloginAndLogout(@TempDir Path tmp) throws Exception {
        final File src = Files.createDirectories(tmp.resolve("src")).toFile();
        final File tgt = Files.createDirectories(tmp.resolve("tgt")).toFile();
        final File a = new File(src, "a.parquet");
        final File b = new File(src, "b.parquet");
        writeParquet(a, 2);
        writeParquet(b, 3);

        final TestRunner runner = newRunner(writeConfig(tmp, "core-site.xml", true, true), src, tgt);
        runner.setProperty(MergeParquet.PROP_KEEP_SOURCE_FILE, "false");
        addKerberosUserService(runner);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(MergeParquet.REL_SUCCESS, 1);
        assertTrue(new File(tgt, "merged.parquet").exists());
        assertFalse(a.exists(), "commitAsync 콜백에서 원본이 삭제되어야 한다");
        assertFalse(b.exists());

        final List<String> ops = List.copyOf(UgiRecordingFileSystem.USERS);
        assertTrue(ops.stream().anyMatch(s -> s.startsWith("open:")), "읽기가 기록되어야 한다: " + ops);
        assertTrue(ops.stream().anyMatch(s -> s.startsWith("create:")), "쓰기가 기록되어야 한다: " + ops);
        assertTrue(ops.stream().anyMatch(s -> s.startsWith("delete:")), "삭제가 기록되어야 한다: " + ops);
        for (String op : ops) {
            assertTrue(op.endsWith("@" + TEST_USER), "모든 파일 시스템 작업은 UGI doAs 안에서 실행되어야 한다: " + op);
        }

        assertEquals(1, kerberosUser.logins.get(), "onScheduled에서 한 번 로그인해야 한다");
        assertTrue(kerberosUser.relogins.get() >= 2, "onTrigger와 삭제 콜백에서 TGT를 확인해야 한다: " + kerberosUser.relogins.get());
        assertEquals(1, kerberosUser.logouts.get(), "중지 시 로그아웃해야 한다");
    }

    /** UGI 생성 시 실제 Kerberos 로그인 대신 가짜 KerberosUser로 로그인하고 원격 사용자 UGI를 사용한다. */
    static class TestableMergeParquet extends MergeParquet {
        @Override
        protected UserGroupInformation createUserGroupInformation(Configuration config, KerberosUser kerberosUser) {
            kerberosUser.login();
            return UserGroupInformation.createRemoteUser(TEST_USER);
        }
    }

    static class FakeKerberosUserService extends AbstractControllerService implements KerberosUserService {
        private final KerberosUser user;

        FakeKerberosUserService(KerberosUser user) {
            this.user = user;
        }

        @Override
        public KerberosUser createKerberosUser() {
            return user;
        }
    }

    static class FakeKerberosUser implements KerberosUser {
        final AtomicInteger logins = new AtomicInteger();
        final AtomicInteger logouts = new AtomicInteger();
        final AtomicInteger relogins = new AtomicInteger();
        private volatile boolean loggedIn;

        @Override
        public void login() {
            logins.incrementAndGet();
            loggedIn = true;
        }

        @Override
        public void logout() {
            logouts.incrementAndGet();
            loggedIn = false;
        }

        @Override
        public <T> T doAs(PrivilegedAction<T> action) {
            return action.run();
        }

        @Override
        public <T> T doAs(PrivilegedExceptionAction<T> action) throws java.security.PrivilegedActionException {
            try {
                return action.run();
            } catch (Exception e) {
                throw new java.security.PrivilegedActionException(e);
            }
        }

        @Override
        public boolean checkTGTAndRelogin() {
            relogins.incrementAndGet();
            return true;
        }

        @Override
        public boolean isLoggedIn() {
            return loggedIn;
        }

        @Override
        public String getPrincipal() {
            return "merge@EXAMPLE.COM";
        }

        @Override
        public AppConfigurationEntry getConfigurationEntry() {
            return null;
        }
    }

    /** 파일 시스템 작업마다 현재 UGI 사용자를 기록하는 로컬 파일 시스템(스킴: ugifs). */
    public static class UgiRecordingFileSystem extends RawLocalFileSystem {
        static final List<String> USERS = new CopyOnWriteArrayList<>();

        private static void record(String op) throws IOException {
            USERS.add(op + ":@" + UserGroupInformation.getCurrentUser().getShortUserName());
        }

        @Override
        public URI getUri() {
            return URI.create("ugifs:///");
        }

        @Override
        public String getScheme() {
            return "ugifs";
        }

        @Override
        public FSDataInputStream open(org.apache.hadoop.fs.Path f, int bufferSize) throws IOException {
            record("open");
            return super.open(f, bufferSize);
        }

        @Override
        public FSDataOutputStream create(org.apache.hadoop.fs.Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                         short replication, long blockSize, Progressable progress) throws IOException {
            record("create");
            return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
        }

        @Override
        public FSDataOutputStream create(org.apache.hadoop.fs.Path f, boolean overwrite, int bufferSize,
                                         short replication, long blockSize, Progressable progress) throws IOException {
            record("create");
            return super.create(f, overwrite, bufferSize, replication, blockSize, progress);
        }

        @Override
        public boolean delete(org.apache.hadoop.fs.Path p, boolean recursive) throws IOException {
            record("delete");
            return super.delete(p, recursive);
        }

        @Override
        public boolean rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst) throws IOException {
            record("rename");
            return super.rename(src, dst);
        }
    }
}
