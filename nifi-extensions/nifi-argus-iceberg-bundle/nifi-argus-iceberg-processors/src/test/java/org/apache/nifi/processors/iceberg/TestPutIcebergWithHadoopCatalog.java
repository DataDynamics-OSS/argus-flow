/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/test/java/org/apache/nifi/processors/iceberg/TestPutIcebergWithHadoopCatalog.java
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

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import org.apache.nifi.processors.iceberg.catalog.TestHadoopCatalogService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.iceberg.PutIceberg.ICEBERG_RECORD_COUNT;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validateNumberOfDataFiles;
import static org.apache.nifi.processors.iceberg.util.IcebergTestUtils.validatePartitionFolders;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

/**
 * Hadoop 카탈로그를 사용하는 PutIceberg 프로세서의 파티션 트랜스폼(year/month/day) 동작을 검증하는 통합 테스트 클래스.
 * 날짜/타임스탬프 필드를 기준으로 파티셔닝된 테이블에 레코드를 기록했을 때,
 * 예상한 개수의 데이터 파일과 파티션 폴더 구조가 실제로 생성되는지 확인한다.
 * Windows 환경에서는 파일 경로/권한 처리 차이로 인해 테스트가 비활성화된다.
 */
public class TestPutIcebergWithHadoopCatalog {

    private TestRunner runner;
    private PutIceberg processor;
    private Schema inputSchema;
    private Catalog catalog;

    private static final Namespace NAMESPACE = Namespace.of("default");

    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(NAMESPACE, "date");

    private static final org.apache.iceberg.Schema DATE_SCHEMA = new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "timeMicros", Types.TimeType.get()),
            Types.NestedField.required(2, "timestampMicros", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "date", Types.DateType.get())
    );

    // 테스트용 Avro 스키마 파일(date.avsc)을 로드하고 PutIceberg 프로세서 인스턴스를 새로 생성한다.
    @BeforeEach
    public void setUp() throws Exception {
        String avroSchema = IOUtils.toString(Files.newInputStream(Paths.get("src/test/resources/date.avsc")), StandardCharsets.UTF_8);
        inputSchema = new Schema.Parser().parse(avroSchema);

        processor = new PutIceberg();
    }

    // Avro 스키마를 기반으로 레코드 리더 컨트롤러 서비스를 구성하고, 날짜/타임스탬프/타임 필드를 가진
    // 테스트용 레코드 4건(파티션 트랜스폼 결과를 검증하기 위한 서로 다른 연/월/일 값 포함)을 등록한다.
    private void initRecordReader() throws InitializationException {
        MockRecordParser readerFactory = new MockRecordParser();
        RecordSchema recordSchema = AvroTypeUtil.createSchema(inputSchema);

        for (RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField.getFieldName(), recordField.getDataType().getFieldType(), recordField.isNullable());
        }

        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2015-02-02 15:30:30.800"), Date.valueOf("2015-02-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2015-02-02 15:30:30.800"), Date.valueOf("2015-02-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2016-01-02 15:30:30.800"), Date.valueOf("2016-01-02"));
        readerFactory.addRecord(Time.valueOf("15:30:30"), Timestamp.valueOf("2017-01-10 15:30:30.800"), Date.valueOf("2017-01-10"));

        runner.addControllerService("mock-reader-factory", readerFactory);
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutIceberg.RECORD_READER, "mock-reader-factory");
    }

    // 지정된 파티션 스펙(spec)과 파일 포맷(fileFormat)으로 Hadoop 카탈로그에 DATE_SCHEMA 기반 테이블을 생성하고,
    // 해당 카탈로그 서비스를 컨트롤러 서비스로 등록하여 PutIceberg의 CATALOG 프로퍼티로 설정한다.
    private void initCatalog(PartitionSpec spec, FileFormat fileFormat) throws InitializationException, IOException {
        TestHadoopCatalogService catalogService = new TestHadoopCatalogService();
        IcebergCatalogFactory catalogFactory = new IcebergCatalogFactory(catalogService);
        catalog = catalogFactory.create();

        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(TableProperties.FORMAT_VERSION, "2");
        tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());

        catalog.createTable(TABLE_IDENTIFIER, DATE_SCHEMA, spec, tableProperties);

        runner.addControllerService("catalog-service", catalogService);
        runner.enableControllerService(catalogService);

        runner.setProperty(PutIceberg.CATALOG, "catalog-service");
    }

    // "date" 필드를 연(year) 단위로 파티셔닝했을 때, 서로 다른 연도(2015/2016/2017)를 가진 레코드들이
    // 각각 별도의 데이터 파일과 date_year=YYYY 형태의 파티션 폴더로 올바르게 분리되어 기록되는지 검증한다.
    @DisabledOnOs(WINDOWS)
    @Test
    public void onTriggerYearTransform() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .year("date")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        initCatalog(spec, FileFormat.PARQUET);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "default");
        runner.setProperty(PutIceberg.TABLE_NAME, "date");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(TABLE_IDENTIFIER);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList("date_year=2015", "date_year=2016", "date_year=2017"));
    }

    // "timestampMicros" 필드를 월(month) 단위로 파티셔닝했을 때, 서로 다른 연-월 조합(2015-02/2016-01/2017-01)을 가진
    // 레코드들이 각각 별도의 데이터 파일과 timestampMicros_month=YYYY-MM 형태의 파티션 폴더로 올바르게 분리되어 기록되는지 검증한다.
    @DisabledOnOs(WINDOWS)
    @Test
    public void onTriggerMonthTransform() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .month("timestampMicros")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        initCatalog(spec, FileFormat.ORC);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "default");
        runner.setProperty(PutIceberg.TABLE_NAME, "date");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(TABLE_IDENTIFIER);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList(
                "timestampMicros_month=2015-02", "timestampMicros_month=2016-01", "timestampMicros_month=2017-01"));
    }

    // "timestampMicros" 필드를 일(day) 단위로 파티셔닝했을 때, 서로 다른 날짜(2015-02-02/2016-01-02/2017-01-10)를 가진
    // 레코드들이 각각 별도의 데이터 파일과 timestampMicros_day=YYYY-MM-DD 형태의 파티션 폴더로 올바르게 분리되어 기록되는지 검증한다.
    @DisabledOnOs(WINDOWS)
    @Test
    public void onTriggerDayTransform() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(DATE_SCHEMA)
                .day("timestampMicros")
                .build();

        runner = TestRunners.newTestRunner(processor);
        initRecordReader();
        initCatalog(spec, FileFormat.AVRO);
        runner.setProperty(PutIceberg.CATALOG_NAMESPACE, "default");
        runner.setProperty(PutIceberg.TABLE_NAME, "date");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new byte[0]);
        runner.run();

        Table table = catalog.loadTable(TABLE_IDENTIFIER);

        runner.assertTransferCount(PutIceberg.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutIceberg.REL_SUCCESS).get(0);

        Assertions.assertTrue(table.spec().isPartitioned());
        Assertions.assertEquals("4", flowFile.getAttribute(ICEBERG_RECORD_COUNT));
        validateNumberOfDataFiles(table.location(), 3);
        validatePartitionFolders(table.location(), Arrays.asList(
                "timestampMicros_day=2015-02-02", "timestampMicros_day=2016-01-02", "timestampMicros_day=2017-01-10"));
    }
}
