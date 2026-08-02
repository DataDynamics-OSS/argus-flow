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
 *   nifi-nar-bundles/nifi-kudu-bundle/nifi-kudu-processors/src/test/java/org/apache/nifi/processors/kudu/TestPutKudu.java
 */
package io.datadynamics.nifi.processors.kudu;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datadynamics.nifi.processors.kudu.json.TimestampFormatHolder;
import io.datadynamics.nifi.processors.kudu.json.TimestampFormats;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.datadynamics.nifi.processors.kudu.PutKudu.FAILURE_STRATEGY_ROUTE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * PutKudu 프로세서의 동작을 검증하는 통합 테스트입니다. {@link MockPutKudu}와 Mockito를 이용해 실제 Kudu 클러스터
 * 없이 레코드 읽기, Kudu Operation 생성, Schema Drift 처리, Timestamp 컬럼 변환, 실패 전략(라우팅/롤백),
 * Kerberos 인증 흐름 등 PutKudu의 핵심 시나리오를 검증합니다.
 */
public class PutKuduTest {

    public static final String DEFAULT_TABLE_NAME = "Nifi-Kudu-Table";
    public static final String DEFAULT_MASTERS = "testLocalHost:7051";
    public static final String TABLE_SCHEMA = "id,stringVal,num32Val,doubleVal,decimalVal,dateVal";
    public static final String DATE_FIELD = "created";
    public static final String ISO_8601_YEAR_MONTH_DAY = "2000-01-01";
    public static final String ISO_8601_YEAR_MONTH_DAY_PATTERN = "yyyy-MM-dd";
    public static final String ISO_8601_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String TIMESTAMP_FIELD = "updated";
    public static final String TIMESTAMP_STANDARD = "2000-01-01 12:00:00";
    public static final String TIMESTAMP_MICROSECONDS = "2000-01-01 12:00:00.123456";
    private final java.sql.Date today = new java.sql.Date(System.currentTimeMillis());
    String json = "{\n" +
            "  \"formats\": [\n" +
            "    {\n" +
            "      \"column-name\": \"col_date_1\",\n" +
            "      \"timestamp-pattern\": \"yyyy-MM-dd\",\n" +
            "      \"type\": \"DATE\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"column-name\": \"col_date_2\",\n" +
            "      \"timestamp-pattern\": \"yyyyMMdd\",\n" +
            "      \"type\": \"DATE\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"column-name\": \"col_timestamp_1\",\n" +
            "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
            "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"column-name\": \"col_timestamp_2\",\n" +
            "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss.SSS\",\n" +
            "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"column-name\": \"col_timestamp_3\",\n" +
            "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss.SSSSSS\",\n" +
            "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"column-name\": \"col_timestamp_4\",\n" +
            "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss.SSS\",\n" +
            "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"column-name\": \"col_timestamp_5\",\n" +
            "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss.SSS\",\n" +
            "      \"type\": \"TIMESTAMP_MICROS\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";
    SimpleDateFormat formatter = new SimpleDateFormat(ISO_8601_TIMESTAMP_PATTERN);

    private TestRunner testRunner;

    private MockPutKudu processor;

    private MockRecordParser readerFactory;

    private static <T> void stubSlices(OngoingStubbing<T> stubbing, List<T> slices) {
        for (T slice : slices) {
            stubbing = stubbing.thenReturn(slice);
        }
    }

    /**
     * 각 테스트 실행 전에 MockPutKudu 프로세서와 TestRunner를 새로 생성하고 기본 속성을 설정합니다.
     */
    @BeforeEach
    public void setUp() {
        processor = new MockPutKudu();
        testRunner = TestRunners.newTestRunner(processor);
        setUpTestRunner(testRunner);
    }

    /**
     * 대부분의 테스트에서 공통으로 사용하는 기본 PutKudu 속성값(테이블명, 마스터 주소, Timestamp 패턴 등)을 설정합니다.
     */
    private void setUpTestRunner(TestRunner testRunner) {
        testRunner.setProperty(PutKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        testRunner.setProperty(PutKudu.KUDU_MASTERS, DEFAULT_MASTERS);
        testRunner.setProperty(PutKudu.IGNORE_NULL, "true");
        testRunner.setProperty(PutKudu.LOWERCASE_FIELD_NAMES, "true");
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");
        testRunner.setProperty(PutKudu.INSERT_OPERATION, OperationType.INSERT.toString());
        testRunner.setProperty(PutKudu.FAILURE_STRATEGY, FAILURE_STRATEGY_ROUTE.getValue());
        testRunner.setProperty(PutKudu.MAX_ROW_COUNT_PER_BATCH, "10");

        testRunner.setProperty(PutKudu.ADD_HOUR, "0");
        testRunner.setProperty(PutKudu.DEFAULT_TIMESTAMP_PATTERN, ISO_8601_TIMESTAMP_PATTERN);
        testRunner.setProperty(PutKudu.CUSTOM_COLUMN_TIMESTAMP_PATTERNS, json);
        testRunner.setProperty(PutKudu.ROW_LOGGING_COUNT, "10");
    }

    @AfterEach
    public void close() {
        testRunner = null;
    }

    /**
     * 지정한 개수만큼 정상적인 레코드를 생성하는 Mock RecordReader(Controller Service)를 등록합니다.
     */
    private void createRecordReader(int numOfRecord) throws InitializationException {
        readerFactory = new MockRecordParser();
        readerFactory.addSchemaField("id", RecordFieldType.INT);
        readerFactory.addSchemaField("stringVal", RecordFieldType.STRING);
        readerFactory.addSchemaField("num32Val", RecordFieldType.INT);
        readerFactory.addSchemaField("doubleVal", RecordFieldType.DOUBLE);
        readerFactory.addSchemaField(new RecordField("decimalVal", RecordFieldType.DECIMAL.getDecimalDataType(6, 3)));
        readerFactory.addSchemaField("dateVal", RecordFieldType.DATE);
        readerFactory.addSchemaField("timestampVal", RecordFieldType.TIMESTAMP);
        for (int i = 0; i < numOfRecord; i++) {
            readerFactory.addRecord(i, "val_" + i, 1000 + i, 100.88 + i, new BigDecimal("111.111").add(BigDecimal.valueOf(i)), today, "2022-11-11 11:11:11.111");
        }

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
    }

    /**
     * Timestamp 컬럼 파싱 오류를 유발하도록 의도적으로 잘못된 포맷의 값을 담은 레코드를 생성하는 Mock RecordReader를 등록합니다.
     */
    private void createRecordReaderAsError(int numOfRecord) throws InitializationException {
        readerFactory = new MockRecordParser();
        readerFactory.addSchemaField("id", RecordFieldType.INT);
        readerFactory.addSchemaField("stringVal", RecordFieldType.STRING);
        readerFactory.addSchemaField("num32Val", RecordFieldType.INT);
        readerFactory.addSchemaField("doubleVal", RecordFieldType.DOUBLE);
        readerFactory.addSchemaField(new RecordField("decimalVal", RecordFieldType.DECIMAL.getDecimalDataType(6, 3)));
        readerFactory.addSchemaField("dateVal", RecordFieldType.DATE);
        readerFactory.addSchemaField("timestampVal", RecordFieldType.TIMESTAMP);
        for (int i = 0; i < numOfRecord; i++) {
            readerFactory.addRecord(i, "val_" + i, 1000 + i, 100.88 + i, new BigDecimal("111.111").add(BigDecimal.valueOf(i)), today, formatter.format(today));
        }

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
    }


    @Test
    public void testWriteKuduWithDefaults() throws InitializationException {
        createRecordReaderAsError(1);

        final String filename = "testWriteKudu-" + System.currentTimeMillis();

        testRunner.enqueue("trigger");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);

        final MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "1");
        mockFlowFile.assertContentEquals("trigger");

        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provEvents.size());

        final ProvenanceEventRecord provEvent = provEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
    }

    @Test
    public void testCustomValidate() throws InitializationException {
        // NiFi 2.x: Kerberos 인증은 KerberosUserService로만 설정한다.
        createRecordReader(1);

        testRunner.assertValid();

        final KerberosUserService kerberosUserService = enableKerberosUserService(testRunner);
        testRunner.setProperty(PutKudu.KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());
        testRunner.assertValid();

        testRunner.removeProperty(PutKudu.KERBEROS_USER_SERVICE);
        testRunner.assertValid();
    }

    private KerberosUserService enableKerberosUserService(final TestRunner runner) throws InitializationException {
        final KerberosUserService kerberosUserService = mock(KerberosUserService.class);
        when(kerberosUserService.getIdentifier()).thenReturn("userService1");
        runner.addControllerService(kerberosUserService.getIdentifier(), kerberosUserService);
        runner.enableControllerService(kerberosUserService);
        return kerberosUserService;
    }

    @Test
    public void testKerberosEnabled() throws InitializationException {
        createRecordReader(1);

        // NiFi 2.x: KerberosUserService 기반 인증
        final KerberosUserService kerberosUserService = mock(KerberosUserService.class);
        when(kerberosUserService.getIdentifier()).thenReturn("kerb");
        when(kerberosUserService.createKerberosUser()).thenAnswer(invocation -> processor.createMockKerberosUser("unit-test-principal"));
        testRunner.addControllerService("kerb", kerberosUserService);
        testRunner.enableControllerService(kerberosUserService);

        testRunner.setProperty(PutKudu.KERBEROS_USER_SERVICE, "kerb");

        testRunner.run(1, false);

        final MockPutKudu proc = (MockPutKudu) testRunner.getProcessor();
        assertTrue(proc.loggedIn());
        assertFalse(proc.loggedOut());

        testRunner.run(1, true, false);
        assertTrue(proc.loggedOut());
    }

    @Test
    public void testInsecureClient() throws InitializationException {
        createRecordReader(1);

        testRunner.run(1, false);

        final MockPutKudu proc = (MockPutKudu) testRunner.getProcessor();
        assertFalse(proc.loggedIn());
        assertFalse(proc.loggedOut());

        testRunner.run(1, true, false);
        assertFalse(proc.loggedOut());
    }


    @Test
    public void testInvalidReaderShouldRouteToFailure() throws InitializationException, SchemaNotFoundException, MalformedRecordException, IOException {
        createRecordReader(0);

        // simulate throwing an IOException when the factory creates a reader which is what would happen when
        // invalid Avro is passed to the Avro reader factory
        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenThrow(new IOException("NOT AVRO"));

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");

        final String filename = "testInvalidAvroShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }

    @Test
    public void testValidSchemaShouldBeSuccessful() throws InitializationException {
        createRecordReader(10);
        final String filename = "testValidSchemaShouldBeSuccessful-" + System.currentTimeMillis();

        // don't provide my.schema as an attribute
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("my.schema", TABLE_SCHEMA);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
    }

    @Test
    public void testAddingMissingFieldsWhenHandleSchemaDriftIsAllowed() throws InitializationException {
        processor.setTableSchema(new Schema(Collections.emptyList()));
        createRecordReader(5);
        final String filename = "testAddingMissingFieldsWhenHandleSchemaDriftIsAllowed-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.setProperty(PutKudu.HANDLE_SCHEMA_DRIFT, "true");
        testRunner.enqueue("trigger", flowFileAttributes);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
    }

    @Test
    public void testMalformedRecordExceptionFromReaderShouldRouteToFailure() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        createRecordReader(10);

        final RecordReader recordReader = mock(RecordReader.class);
        when(recordReader.nextRecord()).thenThrow(new MalformedRecordException("ERROR"));

        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }

    @Test
    @Disabled("레거시(NiFi 1.x)부터 비활성화된 테스트: 문자열->INT 변환 시나리오가 현재 스키마 구성과 맞지 않음")
    public void testReadAsStringAndWriteAsInt() throws InitializationException {
        createRecordReader(0);
        // add the favorite color as a string
        readerFactory.addRecord(1, "name0", "0", "89.89", "111.111", today);

        final String filename = "testReadAsStringAndWriteAsInt-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
    }

    @Test
    public void testMissingColumnInReader() throws InitializationException {
        createRecordReader(0);
        readerFactory.addRecord("name0", "0", "89.89"); //missing id

        final String filename = "testMissingColumnInReader-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }


    @Test
    public void testInsertManyFlowFiles() throws Exception {
        createRecordReader(50);
        final String content1 = "{ \"field1\" : \"value1\", \"field2\" : \"value11\" }";
        final String content2 = "{ \"field1\" : \"value1\", \"field2\" : \"value11\" }";
        final String content3 = "{ \"field1\" : \"value3\", \"field2\" : \"value33\" }";

        testRunner.enqueue(content1.getBytes());
        testRunner.enqueue(content2.getBytes());
        testRunner.enqueue(content3.getBytes());

        testRunner.run(3);

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS);

        flowFiles.get(0).assertContentEquals(content1.getBytes());
        flowFiles.get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");

        flowFiles.get(1).assertContentEquals(content2.getBytes());
        flowFiles.get(1).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");

        flowFiles.get(2).assertContentEquals(content3.getBytes());
        flowFiles.get(2).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testUpsertFlowFiles() throws Exception {
        createRecordReader(50);
        testRunner.setProperty(PutKudu.INSERT_OPERATION, OperationType.UPSERT.toString());
        testRunner.enqueue("string".getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);

        flowFile.assertContentEquals("string".getBytes());
        flowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testDeleteFlowFiles() throws Exception {
        createRecordReader(50);
        testRunner.setProperty(PutKudu.INSERT_OPERATION, "${kudu.record.delete}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("kudu.record.delete", "DELETE");

        testRunner.enqueue("string".getBytes(), attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);

        flowFile.assertContentEquals("string".getBytes());
        flowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testUpdateFlowFiles() throws Exception {
        createRecordReader(50);
        testRunner.setProperty(PutKudu.INSERT_OPERATION, "${kudu.record.update}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("kudu.record.update", "UPDATE");

        testRunner.enqueue("string".getBytes(), attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);

        flowFile.assertContentEquals("string".getBytes());
        flowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testBuildRow() {
        buildPartialRow((long) 1, "foo", (short) 10, "id", "id", "SFO", null, false);
    }

    @Test
    public void testBuildPartialRowNullable() {
        buildPartialRow((long) 1, null, (short) 10, "id", "id", null, null, false);
    }

    @Test
    public void testBuildPartialRowNullPrimaryKey() {
        assertThrows(IllegalArgumentException.class, () -> buildPartialRow(null, "foo", (short) 10, "id", "id", "SFO", null, false));
    }

    @Test
    public void testBuildPartialRowNotNullable() {
        assertThrows(IllegalArgumentException.class, () -> buildPartialRow((long) 1, "foo", null, "id", "id", "SFO", null, false));
    }

    @Test
    public void testBuildPartialRowLowercaseFields() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "id", "ID", "SFO", null, true);
        assertEquals(1, row.getLong("id"));
        assertEquals("2023-01-01 20:11:11.111", row.getTimestamp("updated_at").toString());
    }

    @Test
    public void testBuildPartialTimestampFields() throws JsonProcessingException { // TODO
        ObjectMapper mapper = new ObjectMapper();
        TimestampFormats timestampFormats = mapper.readValue(json, TimestampFormats.class);
        TimestampFormatHolder holder = new TimestampFormatHolder(timestampFormats);

        PartialRow partialRow = buildPartialRowDateTimestamps((long) 1, "id", "ID", false, 9, holder, "yyyyMMddHHmmssSSS");
        System.out.println(partialRow);
    }

    @Test
    public void testBuildPartialRowLowercaseFieldsFalse() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "id", "ID", "SFO", null, false);
        assertThrows(IllegalArgumentException.class, () -> row.getLong("id"));
    }

    @Test
    public void testBuildPartialRowLowercaseFieldsKuduUpper() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "ID", "ID", "SFO", null, false);
        row.getLong("ID");
    }

    @Test
    public void testBuildPartialRowLowercaseFieldsKuduUpperFail() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "ID", "ID", "SFO", null, true);
        assertThrows(IllegalArgumentException.class, () -> row.getLong("ID"));
    }

    @Test
    public void testBuildPartialRowVarCharTooLong() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "id", "ID", "San Francisco", null, true);
        assertEquals("San", row.getVarchar("airport_code"), "Kudu client should truncate VARCHAR value to expected length");
    }

    @Test
    public void testBuildPartialRowWithDate() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "id", "ID", "San Francisco", today, true);
        // Comparing string representations of dates, because java.sql.Date does not override
        // java.util.Date.equals method and therefore compares milliseconds instead of
        // comparing dates, even though java.sql.Date is supposed to ignore time
        assertEquals(row.getDate("sql_date").toString(), today.toString(),
                String.format("Expecting the date to be %s, but got %s", today, row.getDate("sql_date").toString()));
    }

    @Test
    public void testBuildPartialRowWithDateDefaultTimeZone() throws ParseException {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(ISO_8601_YEAR_MONTH_DAY_PATTERN);
        final java.util.Date dateFieldValue = dateFormat.parse(ISO_8601_YEAR_MONTH_DAY);

        assertPartialRowDateFieldEquals(dateFieldValue);
    }

    @Test
    public void testBuildPartialRowWithDateToString() throws ParseException {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(ISO_8601_YEAR_MONTH_DAY_PATTERN);
        final java.util.Date dateFieldValue = dateFormat.parse(ISO_8601_YEAR_MONTH_DAY);

        final PartialRow row = buildPartialRowDateField(dateFieldValue, Type.STRING);
        final String column = row.getString(DATE_FIELD);
        assertEquals(ISO_8601_YEAR_MONTH_DAY, column, "Partial Row Field not matched");
    }

    @Test
    public void testBuildPartialRowWithDateString() {
        assertPartialRowDateFieldEquals(ISO_8601_YEAR_MONTH_DAY);
    }

    @Test
    public void testBuildPartialRowWithTimestampStandardString() {
        assertPartialRowTimestampFieldEquals(TIMESTAMP_STANDARD);
    }

    @Test
    public void testBuildPartialRowWithTimestampMicrosecondsString() {
        assertPartialRowTimestampMicroFieldEquals(TIMESTAMP_MICROSECONDS);
    }

    private void assertPartialRowTimestampFieldEquals(final Object timestampFieldValue) {
        final PartialRow row = _buildPartialRowTimestampField(timestampFieldValue, "yyyy-MM-dd HH:mm:ss");
        final Timestamp timestamp = row.getTimestamp(TIMESTAMP_FIELD);
        final Timestamp expected = Timestamp.valueOf(timestampFieldValue.toString());
        assertEquals(expected, timestamp, "Partial Row Timestamp Field not matched");
    }

    private void assertPartialRowTimestampMicroFieldEquals(final Object timestampFieldValue) {
        final PartialRow row = _buildPartialRowTimestampField(timestampFieldValue, "yyyy-MM-dd HH:mm:ss.SSSSSS");
        final Timestamp timestamp = row.getTimestamp(TIMESTAMP_FIELD);
        final Timestamp expected = Timestamp.valueOf(timestampFieldValue.toString());
        assertEquals(expected, timestamp, "Partial Row Timestamp Field not matched");
    }

    private PartialRow _buildPartialRowTimestampField(final Object timestampFieldValue, String timestampPattern) {
        final Schema kuduSchema = new Schema(Collections.singletonList(
                new ColumnSchema.ColumnSchemaBuilder(TIMESTAMP_FIELD, Type.UNIXTIME_MICROS).nullable(true).build()
        ));

        final RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField(TIMESTAMP_FIELD, RecordFieldType.TIMESTAMP.getDataType())
        ));

        final Map<String, Object> values = new HashMap<>();
        values.put(TIMESTAMP_FIELD, timestampFieldValue);
        final MapRecord record = new MapRecord(schema, values);

        final PartialRow row = kuduSchema.newPartialRow();
        processor.buildPartialRow(kuduSchema, row, record, schema.getFieldNames(), true, true, 0, null, timestampPattern); // FIXED
        return row;
    }

    private PartialRow buildPartialRowTimestampField(final Object timestampFieldValue) {
        final Schema kuduSchema = new Schema(Collections.singletonList(
                new ColumnSchema.ColumnSchemaBuilder(TIMESTAMP_FIELD, Type.UNIXTIME_MICROS).nullable(true).build()
        ));

        final RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField(TIMESTAMP_FIELD, RecordFieldType.TIMESTAMP.getDataType())
        ));

        final Map<String, Object> values = new HashMap<>();
        values.put(TIMESTAMP_FIELD, timestampFieldValue);
        final MapRecord record = new MapRecord(schema, values);

        final PartialRow row = kuduSchema.newPartialRow();
        processor.buildPartialRow(kuduSchema, row, record, schema.getFieldNames(), true, true, 0, null, "yyyy-MM-dd HH:mm:ss.SSSSSS"); // FIXED
        return row;
    }

    private void assertPartialRowDateFieldEquals(final Object dateFieldValue) {
        final PartialRow row = buildPartialRowDateField(dateFieldValue, Type.DATE);
        final java.sql.Date rowDate = row.getDate(DATE_FIELD);
        assertEquals(ISO_8601_YEAR_MONTH_DAY, rowDate.toString(), "Partial Row Date Field not matched");
    }

    private PartialRow buildPartialRowDateField(final Object dateFieldValue, final Type columnType) {
        final Schema kuduSchema = new Schema(Collections.singletonList(
                new ColumnSchema.ColumnSchemaBuilder(DATE_FIELD, columnType).nullable(true).build()
        ));

        final RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField(DATE_FIELD, RecordFieldType.DATE.getDataType())
        ));

        final Map<String, Object> values = new HashMap<>();
        values.put(DATE_FIELD, dateFieldValue);
        final MapRecord record = new MapRecord(schema, values);

        final PartialRow row = kuduSchema.newPartialRow();
        processor.buildPartialRow(kuduSchema, row, record, schema.getFieldNames(), true, true, 0, null, "yyyy-MM-dd");
        return row;
    }

    private PartialRow buildPartialRow(Long id, String name, Short age, String kuduIdName, String recordIdName, String airport_code, java.sql.Date sql_date, Boolean lowercaseFields) {
        return buildPartialRow(id, name, age, kuduIdName, recordIdName, airport_code, sql_date, lowercaseFields, 9, null, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    private PartialRow buildPartialRow(Long id, String name, Short age, String kuduIdName, String recordIdName, String airport_code, java.sql.Date sql_date, Boolean lowercaseFields, int addHour, TimestampFormatHolder holder, String defaultTimestampPatterns) {
        final Schema kuduSchema = new Schema(Arrays.asList(
                new ColumnSchema.ColumnSchemaBuilder(kuduIdName, Type.INT64).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).nullable(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("age", Type.INT16).nullable(false).build(),
                new ColumnSchema.ColumnSchemaBuilder("updated_at", Type.UNIXTIME_MICROS).nullable(false).build(),
                new ColumnSchema.ColumnSchemaBuilder("score", Type.DECIMAL).nullable(true).typeAttributes(
                        new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(9).scale(0).build()
                ).build(),
                new ColumnSchema.ColumnSchemaBuilder("airport_code", Type.VARCHAR).nullable(true).typeAttributes(
                        new ColumnTypeAttributes.ColumnTypeAttributesBuilder().length(3).build()
                ).build(),
                new ColumnSchema.ColumnSchemaBuilder("sql_date", Type.DATE).nullable(true).build()
        ));


        final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
                new RecordField(recordIdName, RecordFieldType.BIGINT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.SHORT.getDataType()),
                new RecordField("updated_at", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("score", RecordFieldType.LONG.getDataType()),
                new RecordField("airport_code", RecordFieldType.STRING.getDataType()),
                new RecordField("sql_date", RecordFieldType.DATE.getDataType())
        ));

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = null;
        try {
            date = formatter.parse("2023-01-01 11:11:11.111");
        } catch (Exception e) {
        }

        Map<String, Object> values = new HashMap<>();
        PartialRow row = kuduSchema.newPartialRow();
        values.put(recordIdName, id);
        values.put("name", name);
        values.put("age", age);
        values.put("updated_at", new Timestamp(date == null ? System.currentTimeMillis() : date.getTime()));
        values.put("score", 10000L);
        values.put("airport_code", airport_code);
        values.put("sql_date", sql_date);

        processor.buildPartialRow(kuduSchema, row, new MapRecord(schema, values), schema.getFieldNames(), true, lowercaseFields, addHour, holder, defaultTimestampPatterns);
        return row;
    }

    private PartialRow buildPartialRowDateTimestamps(Long id, String kuduIdName, String recordIdName, Boolean lowercaseFields, int addHour, TimestampFormatHolder holder, String defaultTimestampPatterns) {
        final Schema kuduSchema = new Schema(Arrays.asList(
                new ColumnSchema.ColumnSchemaBuilder(kuduIdName, Type.INT64).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("col_date_1", Type.DATE).nullable(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("col_date_2", Type.DATE).nullable(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("col_timestamp_1", Type.UNIXTIME_MICROS).nullable(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("col_timestamp_2", Type.UNIXTIME_MICROS).nullable(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("col_timestamp_3", Type.UNIXTIME_MICROS).nullable(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("col_timestamp_4", Type.UNIXTIME_MICROS).nullable(true).build()
        ));

        final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("col_date_1", RecordFieldType.DATE.getDataType()),
                new RecordField("col_date_2", RecordFieldType.DATE.getDataType()),
                new RecordField("col_timestamp_1", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("col_timestamp_2", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("col_timestamp_3", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("col_timestamp_4", RecordFieldType.TIMESTAMP.getDataType())
        ));

        Date date = null;
        try {
            date = formatter.parse("2023-01-01 11:11:11.111");
        } catch (Exception e) {
        }

        Map<String, Object> values = new HashMap<>();
        PartialRow row = kuduSchema.newPartialRow();
        values.put(recordIdName, id);
        values.put("col_date_1", date);
        values.put("col_date_2", "20230101");
        values.put("col_timestamp_1", "2023-01-01 11:11:11");
        values.put("col_timestamp_2", "2023-01-01 11:11:11.111");
        values.put("col_timestamp_3", "2023-01-01 11:11:11.111111");
        values.put("col_timestamp_4", date.getTime());

        processor.buildPartialRow(kuduSchema, row, new MapRecord(schema, values), schema.getFieldNames(), true, lowercaseFields, addHour, holder, defaultTimestampPatterns);
        return row;
    }

    private Tuple<Insert, OperationResponse> insert(boolean success) {
        Insert insert = mock(Insert.class);
        OperationResponse response = mock(OperationResponse.class, Mockito.RETURNS_DEEP_STUBS);
        when(response.hasRowError()).thenReturn(!success);
        if (!success) {
            when(response.getRowError().getOperation()).thenReturn(insert);
        }
        return new Tuple<>(insert, response);
    }

    private LinkedList<OperationResponse> queueInsert(MockPutKudu putKudu, KuduSession session, boolean sync, ResultCode... results) throws Exception {
        LinkedList<OperationResponse> responses = new LinkedList<>();
        for (ResultCode result : results) {
            boolean ok = result == ResultCode.OK;
            Tuple<Insert, OperationResponse> tuple = insert(ok);
            putKudu.queue(tuple.getKey());

            if (result == ResultCode.EXCEPTION) {
                when(session.apply(tuple.getKey())).thenThrow(mock(KuduException.class));
                // Stop processing the rest of the records on the first exception
                break;
            } else {
                responses.add(tuple.getValue());
                if (sync) {
                    when(session.apply(tuple.getKey())).thenReturn(ok ? null : tuple.getValue());

                    // In AUTO_FLUSH_SYNC mode, PutKudu immediately knows when an operation has failed.
                    // In that case, it does not process the rest of the records in the FlowFile.
                    if (result == ResultCode.FAIL) break;
                }
            }
        }
        return responses;
    }

    private void testKuduPartialFailure(SessionConfiguration.FlushMode flushMode, int batchSize) throws Exception {
        final int numFlowFiles = 4;
        final int numRecordsPerFlowFile = 3;
        final ResultCode[][] flowFileResults = new ResultCode[][]{
                new ResultCode[]{ResultCode.OK, ResultCode.OK, ResultCode.FAIL},

                // The last operation will not be submitted to Kudu if flush mode is AUTO_FLUSH_SYNC
                new ResultCode[]{ResultCode.OK, ResultCode.FAIL, ResultCode.OK},

                // Everything's okay
                new ResultCode[]{ResultCode.OK, ResultCode.OK, ResultCode.OK},

                // The last operation will not be submitted due to an exception from apply() call
                new ResultCode[]{ResultCode.OK, ResultCode.EXCEPTION, ResultCode.OK},
        };

        KuduSession session = mock(KuduSession.class);
        when(session.getFlushMode()).thenReturn(flushMode);
        MockPutKudu putKudu = new MockPutKudu(session);

        List<List<OperationResponse>> flowFileResponses = new ArrayList<>();
        boolean sync = flushMode == SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
        for (ResultCode[] results : flowFileResults) {
            flowFileResponses.add(queueInsert(putKudu, session, sync, results));
        }

        switch (flushMode) {
            case AUTO_FLUSH_SYNC:
                // flush() or close() returns an empty list
                when(session.close()).thenReturn(Collections.emptyList());
                break;
            case AUTO_FLUSH_BACKGROUND:
                // close() will be called for each batch of FlowFiles, however we do not check
                // the return value of it. Instead, we should check the pending errors of the session
                // as buffered operations may have already been flushed.
                when(session.close()).thenReturn(Collections.emptyList());

                List<RowErrorsAndOverflowStatus> pendingErrorResponses = new ArrayList<>();
                while (!flowFileResponses.isEmpty()) {
                    int sliceSize = Math.min(batchSize, flowFileResponses.size());
                    List<List<OperationResponse>> slice = flowFileResponses.subList(0, sliceSize);

                    RowErrorsAndOverflowStatus pendingErrorResponse = mock(RowErrorsAndOverflowStatus.class);
                    RowError[] rowErrors = slice.stream()
                            .flatMap(List::stream)
                            .filter(OperationResponse::hasRowError)
                            .map(OperationResponse::getRowError)
                            .toArray(RowError[]::new);
                    when(pendingErrorResponse.getRowErrors()).thenReturn(rowErrors);
                    pendingErrorResponses.add(pendingErrorResponse);

                    flowFileResponses = flowFileResponses.subList(sliceSize, flowFileResponses.size());
                }

                stubSlices(when(session.getPendingErrors()), pendingErrorResponses);
                break;
            case MANUAL_FLUSH:
                // close() will be called at the end of a batch, but flush() will also be called
                // whenever the mutation buffer of KuduSession becomes full. In PutKudu, we set
                // the size of the mutation buffer to match batchSize, so flush() is called only
                // when a FlowFile more than one record.
                List<List<OperationResponse>> flushes = new ArrayList<>();
                List<List<OperationResponse>> closes = new ArrayList<>();

                while (!flowFileResponses.isEmpty()) {
                    int sliceSize = Math.min(batchSize, flowFileResponses.size());
                    List<List<OperationResponse>> slice = flowFileResponses.subList(0, sliceSize);
                    flowFileResponses = flowFileResponses.subList(sliceSize, flowFileResponses.size());

                    List<OperationResponse> batch = new ArrayList<>();
                    for (OperationResponse response : slice.stream().flatMap(List::stream).collect(Collectors.toList())) {
                        if (batch.size() == batchSize) {
                            flushes.add(batch);
                            batch = new ArrayList<>();
                        }
                        batch.add(response);
                    }
                    if (flowFileResponses.isEmpty() && batch.size() == batchSize) {
                        // To handle the case where PutKudu ends the batch with flush()
                        // instead of close() due to the exception from the subsequent apply call.
                        flushes.add(batch);
                    } else if (batch.size() > 0) {
                        closes.add(batch);
                    }

                    if (!flushes.isEmpty()) stubSlices(when(session.flush()), flushes);
                    if (!closes.isEmpty()) stubSlices(when(session.close()), closes);
                }
                break;
        }

        testRunner = TestRunners.newTestRunner(putKudu);
        createRecordReader(numRecordsPerFlowFile);
        setUpTestRunner(testRunner);
        testRunner.setProperty(PutKudu.FLUSH_MODE, flushMode.name());

        IntStream.range(0, numFlowFiles).forEach(i -> testRunner.enqueue(""));
        testRunner.run(numFlowFiles);

        testRunner.assertTransferCount(PutKudu.REL_FAILURE, 3);

        List<MockFlowFile> failedFlowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_FAILURE);
        failedFlowFiles.get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "2");
        failedFlowFiles.get(1).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, sync ? "1" : "2");
        failedFlowFiles.get(2).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "1");

        testRunner.assertTransferCount(PutKudu.REL_SUCCESS, 1);
        testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "3");
    }

    private void testKuduPartialFailure(SessionConfiguration.FlushMode flushMode) throws Exception {
        // Test against different batch sizes (up until the point where every record can be buffered at once)
        for (int i = 1; i <= 11; i++) {
            testKuduPartialFailure(flushMode, i);
        }
    }

    @Test
    public void testKuduPartialFailuresOnAutoFlushSync() throws Exception {
        testKuduPartialFailure(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
    }

    @Test
    @Disabled("레거시(NiFi 1.x)부터 비활성화된 테스트: AUTO_FLUSH_BACKGROUND partial failure Mock 시나리오 불안정")
    public void testKuduPartialFailuresOnAutoFlushBackground() throws Exception {
        testKuduPartialFailure(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    @Test
    @Disabled("레거시(NiFi 1.x)부터 비활성화된 테스트: MANUAL_FLUSH partial failure Mock 시나리오 불안정")
    public void testKuduPartialFailuresOnManualFlush() throws Exception {
        testKuduPartialFailure(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    }

    enum ResultCode {
        OK,
        FAIL,
        EXCEPTION
    }
}