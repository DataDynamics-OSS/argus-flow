/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/test/java/org/apache/nifi/processors/iceberg/TestPutIcebergCustomValidation.java
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

import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processors.iceberg.catalog.TestHiveCatalogService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * PutIceberg 프로세서의 customValidate() 로직을 검증하는 테스트 클래스.
 * Kerberos 보안이 설정된 core-site.xml 사용 여부와 KerberosUserService 컨트롤러 서비스 등록 여부의
 * 조합에 따라 프로세서 설정이 유효(valid)한지 아닌지를 확인하고,
 * 스냅샷 요약(snapshot-property.*) 형식의 동적 프로퍼티 유효성 검증도 함께 다룬다.
 */
public class TestPutIcebergCustomValidation {

    private static final String RECORD_READER_NAME = "record-reader";
    private static final String KERBEROS_USER_SERVICE_NAME = "kerberos-user-service";
    private static final String CATALOG_SERVICE_NAME = "catalog-service";

    private static final String CATALOG_NAMESPACE = "catalogNamespace";
    private static final String TABLE_NAME = "tableName";

    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        PutIceberg processor = new PutIceberg();
        runner = TestRunners.newTestRunner(processor);
    }

    // RecordReader 컨트롤러 서비스를 등록/활성화하고 PutIceberg의 RECORD_READER 프로퍼티로 설정한다.
    private void initRecordReader() throws InitializationException {
        MockRecordParser readerFactory = new MockRecordParser();

        runner.addControllerService(RECORD_READER_NAME, readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutIceberg.RECORD_READER, RECORD_READER_NAME);
    }

    // 지정된 core-site.xml 설정 파일 경로(들)를 사용하는 테스트용 Hive 카탈로그 서비스를 등록/활성화한다.
    // secured-core-site.xml을 넘기면 Kerberos 보안이 설정된 상태를, unsecured-core-site.xml을 넘기면 비보안 상태를 시뮬레이션한다.
    private void initCatalogService(List<String> configFilePaths) throws InitializationException {
        TestHiveCatalogService catalogService = new TestHiveCatalogService.Builder().withConfigFilePaths(configFilePaths).build();

        runner.addControllerService(CATALOG_SERVICE_NAME, catalogService);
        runner.enableControllerService(catalogService);

        runner.setProperty(PutIceberg.CATALOG, CATALOG_SERVICE_NAME);
    }

    // Mock으로 만든 KerberosUserService 컨트롤러 서비스를 등록/활성화하고 PutIceberg의 KERBEROS_USER_SERVICE 프로퍼티로 설정한다.
    private void initKerberosUserService() throws InitializationException {
        KerberosUserService kerberosUserService = mock(KerberosUserService.class);
        when(kerberosUserService.getIdentifier()).thenReturn(KERBEROS_USER_SERVICE_NAME);

        runner.addControllerService(KERBEROS_USER_SERVICE_NAME, kerberosUserService);
        runner.enableControllerService(kerberosUserService);

        runner.setProperty(PutIceberg.KERBEROS_USER_SERVICE, KERBEROS_USER_SERVICE_NAME);
    }

    // Kerberos 보안이 설정된 core-site.xml을 사용하지만 KerberosUserService가 없는 경우,
    // customValidate()가 이를 감지해 프로세서 설정이 유효하지 않다고 판단해야 한다.
    @Test
    public void testCustomValidateWithKerberosSecurityConfigAndWithoutKerberosUserService() throws InitializationException {
        initRecordReader();
        initCatalogService(Collections.singletonList("src/test/resources/secured-core-site.xml"));

        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAMESPACE);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
        runner.assertNotValid();
    }

    // Kerberos 보안이 설정된 core-site.xml과 KerberosUserService가 모두 존재하는 경우,
    // 두 설정이 정합하므로 프로세서 설정이 유효해야 한다.
    @Test
    public void testCustomValidateWithKerberosSecurityConfigAndKerberosUserService() throws InitializationException {
        initRecordReader();
        initCatalogService(Collections.singletonList("src/test/resources/secured-core-site.xml"));

        initKerberosUserService();

        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAMESPACE);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
        runner.assertValid();
    }

    // Kerberos 보안 설정이 없는 core-site.xml을 사용하고 KerberosUserService도 없는 경우,
    // 별도의 Kerberos 관련 검증이 필요 없으므로 프로세서 설정이 유효해야 한다.
    @Test
    public void testCustomValidateWithoutKerberosSecurityConfigAndKerberosUserService() throws InitializationException {
        initRecordReader();
        initCatalogService(Collections.singletonList("src/test/resources/unsecured-core-site.xml"));

        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAMESPACE);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
        runner.assertValid();
    }

    // Kerberos 보안 설정이 없는 core-site.xml을 사용하는데 KerberosUserService를 추가로 설정한 경우,
    // 불필요/모순된 설정으로 간주되어 프로세서 설정이 유효하지 않아야 한다.
    @Test
    public void testCustomValidateWithoutKerberosSecurityConfigAndWithKerberosUserService() throws InitializationException {
        initRecordReader();
        initCatalogService(Collections.singletonList("src/test/resources/unsecured-core-site.xml"));

        initKerberosUserService();

        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAMESPACE);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);
        runner.assertNotValid();
    }

    // 접두사가 "snapshot-property."가 아닌(허용되지 않는) 동적 프로퍼티를 설정하면
    // 스냅샷 요약 동적 프로퍼티 검증에 실패하여 프로세서가 유효하지 않아야 한다.
    @Test
    public void testInvalidSnapshotSummaryDynamicProperty() throws InitializationException {
        initRecordReader();
        initCatalogService(Collections.singletonList("src/test/resources/unsecured-core-site.xml"));

        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAMESPACE);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);

        runner.setProperty("invalid.dynamic.property", "test value");
        runner.assertNotValid();
    }

    // "snapshot-property." 접두사를 가진 동적 프로퍼티는 허용되는 형식이므로
    // 프로세서 설정이 유효해야 한다.
    @Test
    public void testValidSnapshotSummaryDynamicProperty() throws InitializationException {
        initRecordReader();
        initCatalogService(Collections.singletonList("src/test/resources/unsecured-core-site.xml"));

        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, CATALOG_NAMESPACE);
        runner.setProperty(PutIceberg.TABLE_NAME, TABLE_NAME);

        runner.setProperty("snapshot-property.valid-property", "test value");
        runner.assertValid();
    }
}
