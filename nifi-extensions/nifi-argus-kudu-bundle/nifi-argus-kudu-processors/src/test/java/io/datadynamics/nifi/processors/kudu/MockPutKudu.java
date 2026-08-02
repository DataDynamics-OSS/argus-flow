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
 *   nifi-nar-bundles/nifi-kudu-bundle/nifi-kudu-processors/src/test/java/org/apache/nifi/processors/kudu/MockPutKudu.java
 */
package io.datadynamics.nifi.processors.kudu;

import io.datadynamics.nifi.processors.kudu.json.TimestampFormatHolder;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.Record;

import javax.security.auth.login.AppConfigurationEntry;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 실제 Kudu 클러스터 없이 PutKudu의 동작을 검증하기 위한 테스트용 Mock 프로세서입니다.
 * KuduClient/KuduSession/Kudu Operation 생성 및 Kerberos 인증 관련 메서드를 Mockito 기반으로 대체(override)하여
 * 테스트 코드에서 원하는 Operation을 큐에 넣거나 로그인/로그아웃 여부 등을 검증할 수 있도록 합니다.
 */
public class MockPutKudu extends PutKudu {

    private KuduSession session;
    private LinkedList<Operation> opQueue;

    // Atomic reference is used as the set and use of the schema are in different thread
    private AtomicReference<Schema> tableSchema = new AtomicReference<>();

    private boolean loggedIn = false;
    private boolean loggedOut = false;

    public MockPutKudu() {
        this(mock(KuduSession.class));
    }

    public MockPutKudu(KuduSession session) {
        this.session = session;
        this.opQueue = new LinkedList<>();
    }

    public void queue(Operation... operations) {
        opQueue.addAll(Arrays.asList(operations));
    }

    /**
     * opQueue에 미리 넣어둔(queue()) Operation이 있으면 이를 그대로 반환하고,
     * 없으면 operationType에 맞는 Mockito Mock Operation을 생성하여 반환합니다.
     */
    @Override
    protected Operation createKuduOperation(OperationType operationType, Record record,
                                            List<String> fieldNames, boolean ignoreNull,
                                            boolean lowercaseFields, KuduTable kuduTable,
                                            TimestampFormatHolder holder,
                                            String defaultTimestampPatterns) {

        Operation operation = opQueue.poll();
        if (operation == null) {
            switch (operationType) {
                case INSERT:
                    operation = mock(Insert.class);
                    break;
                case INSERT_IGNORE:
                    operation = mock(InsertIgnore.class);
                    break;
                case UPSERT:
                    operation = mock(Upsert.class);
                    break;
                case UPDATE:
                    operation = mock(Update.class);
                    break;
                case UPDATE_IGNORE:
                    operation = mock(UpdateIgnore.class);
                    break;
                case DELETE:
                    operation = mock(Delete.class);
                    break;
                case DELETE_IGNORE:
                    operation = mock(DeleteIgnore.class);
                    break;
                default:
                    throw new IllegalArgumentException(String.format("OperationType: %s not supported by Kudu", operationType));
            }
        }
        return operation;
    }

    @Override
    protected boolean supportsIgnoreOperations() {
        return true;
    }

    @Override
    public KuduClient buildClient(ProcessContext context) {
        final KuduClient client = mock(KuduClient.class);

        try {
            when(client.openTable(anyString())).thenReturn(mock(KuduTable.class));
        } catch (final Exception e) {
            throw new AssertionError(e);
        }

        return client;
    }

    /**
     * 실제 Kudu 클라이언트 대신 setTableSchema()로 지정한 스키마를 반환하는 Mock 클라이언트/테이블을 사용하여 액션을 실행합니다.
     */
    @Override
    protected void executeOnKuduClient(Consumer<KuduClient> actionOnKuduClient) {
        final KuduClient client = mock(KuduClient.class);

        try {
            final KuduTable kuduTable = mock(KuduTable.class);
            when(client.openTable(anyString())).thenReturn(kuduTable);
            when(kuduTable.getSchema()).thenReturn(tableSchema.get());
        } catch (final Exception e) {
            throw new AssertionError(e);
        }

        actionOnKuduClient.accept(client);
    }

    public boolean loggedIn() {
        return loggedIn;
    }

    public boolean loggedOut() {
        return loggedOut;
    }

    /**
     * NiFi 2.x에서는 KerberosUserService 기반으로만 인증하므로,
     * 테스트에서 KerberosUserService Mock이 반환할 KerberosUser를 생성한다.
     */
    KerberosUser createMockKerberosUser(final String principal) {
        return new KerberosUser() {

            @Override
            public void login() {
                loggedIn = true;
            }

            @Override
            public void logout() {
                loggedOut = true;
            }

            @Override
            public <T> T doAs(final PrivilegedAction<T> action) throws IllegalStateException {
                return action.run();
            }

            @Override
            public <T> T doAs(PrivilegedAction<T> action, ClassLoader contextClassLoader) throws IllegalStateException {
                return action.run();
            }

            @Override
            public <T> T doAs(final PrivilegedExceptionAction<T> action) throws IllegalStateException, PrivilegedActionException {
                try {
                    return action.run();
                } catch (Exception e) {
                    throw new PrivilegedActionException(e);
                }
            }

            @Override
            public <T> T doAs(PrivilegedExceptionAction<T> action, ClassLoader contextClassLoader) throws IllegalStateException, PrivilegedActionException {
                try {
                    return action.run();
                } catch (Exception e) {
                    throw new PrivilegedActionException(e);
                }
            }

            @Override
            public boolean checkTGTAndRelogin() {
                return true;
            }

            @Override
            public boolean isLoggedIn() {
                return loggedIn && !loggedOut;
            }

            @Override
            public String getPrincipal() {
                return principal;
            }

            @Override
            public AppConfigurationEntry getConfigurationEntry() {
                return new AppConfigurationEntry("LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Collections.emptyMap());
            }
        };
    }

    @Override
    protected KuduSession createKuduSession(final KuduClient client) {
        return session;
    }

    void setTableSchema(final Schema tableSchema) {
        this.tableSchema.set(tableSchema);
    }
}