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
package io.datadynamics.nifi.processors.db.bulkinsert;

import io.datadynamics.nifi.processors.db.H2DBCPService;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.serialization.record.MockRecordFailureType;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.BatchUpdateException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BulkOracleInsertProcessor에 대한 테스트.
 * Oracle 전용 INSERT ALL 구문은 H2로 실행할 수 없으므로 {@link RecordingDBCPService}(JDBC 프록시)로
 * 생성된 SQL과 commit/rollback/close 호출을 검증한다.
 * 확인된 결함은 {@code @Disabled("BUG: ...")} 테스트로 재현 방법을 남겨 두고, 수정된 결함은 {@code @Disabled}를 풀어 회귀 테스트로 사용한다.
 */
public class BulkOracleInsertProcessorTest {

    private static final String AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "person",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
              ]
            }
            """;

    private TestRunner runner;

    @BeforeEach
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(BulkOracleInsertProcessor.class);
    }

    // id(int)와 바이트 컬럼 bin을 갖는 Avro 스키마. binType은 bin 필드의 Avro 타입 JSON.
    private static String binAvroSchema(final String binType) {
        return """
                {"type": "record", "name": "bin_record", "fields": [
                  {"name": "id", "type": "int"},
                  {"name": "bin", "type": %s}
                ]}
                """.formatted(binType);
    }

    // id(INT)와 ARRAY of BYTE(Avro bytes) 컬럼 bin을 갖는 레코드 파서
    private static MockRecordParser byteArrayParser() {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField(new RecordField("bin", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        return parser;
    }

    /**
     * 생성한 RecordReader의 close() 호출 횟수를 기록하는 MockRecordParser.
     */
    private static class CloseTrackingRecordParser extends MockRecordParser {
        int readerCloses;

        @Override
        public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
                throws IOException, SchemaNotFoundException {
            final RecordReader delegate = super.createRecordReader(variables, in, inputLength, logger);
            return new RecordReader() {
                @Override
                public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
                    return delegate.nextRecord(coerceTypes, dropUnknownFields);
                }

                @Override
                public RecordSchema getSchema() throws MalformedRecordException {
                    return delegate.getSchema();
                }

                @Override
                public void close() throws IOException {
                    readerCloses++;
                    delegate.close();
                }
            };
        }
    }

    private static CloseTrackingRecordParser closeTrackingPersonParser(final int records) {
        final CloseTrackingRecordParser parser = new CloseTrackingRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        for (int i = 1; i <= records; i++) {
            parser.addRecord(i, "name" + i);
        }
        return parser;
    }

    // 레코드 파서/기록용 DBCP를 등록하고 필수 프로퍼티를 설정한 뒤 기록용 DBCP를 반환한다.
    private RecordingDBCPService configure(final MockRecordParser parser) throws Exception {
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        final RecordingDBCPService dbcp = new RecordingDBCPService();
        runner.addControllerService("dbcp", dbcp);
        runner.enableControllerService(dbcp);

        runner.setProperty(BulkOracleInsertProcessor.DBCP_SERVICE, "dbcp");
        runner.setProperty(BulkOracleInsertProcessor.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkOracleInsertProcessor.SCHEMA_NAME, "SCOTT");
        runner.setProperty(BulkOracleInsertProcessor.TABLE_NAME, "PERSONS");
        runner.setProperty(BulkOracleInsertProcessor.AVRO_SCHEMA, AVRO_SCHEMA);
        return dbcp;
    }

    private static MockRecordParser personParser(final int records) {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        for (int i = 1; i <= records; i++) {
            parser.addRecord(i, "name" + i);
        }
        return parser;
    }

    private static int countInto(final List<String> sqls) {
        int count = 0;
        for (String sql : sqls) {
            count += sql.split("\nINTO ", -1).length - 1;
        }
        return count;
    }

    // 필수 프로퍼티(DBCP 서비스, 레코드 리더, 스키마명, 테이블명 등)를 설정하지 않은 경우
    // 프로세서가 유효하지 않은(invalid) 상태가 되는지 검증한다.
    @Test
    public void testInvalidWithoutRequiredProperties() {
        runner.assertNotValid();
    }

    // DBCP 서비스, 레코드 리더, 스키마명, 테이블명, Avro 스키마 등 필수 프로퍼티를 모두 설정했을 때
    // 프로세서가 유효한(valid) 상태가 되는지 검증한다.
    @Test
    public void testValidConfiguration() throws Exception {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        final H2DBCPService dbcp = new H2DBCPService("bulkoracle_test");
        runner.addControllerService("dbcp", dbcp);
        runner.enableControllerService(dbcp);

        runner.setProperty(BulkOracleInsertProcessor.DBCP_SERVICE, "dbcp");
        runner.setProperty(BulkOracleInsertProcessor.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkOracleInsertProcessor.SCHEMA_NAME, "SCOTT");
        runner.setProperty(BulkOracleInsertProcessor.TABLE_NAME, "PERSONS");
        runner.setProperty(BulkOracleInsertProcessor.AVRO_SCHEMA, AVRO_SCHEMA);

        runner.assertValid();
    }

    // 잘못된 프로퍼티 값(음수/문자 벌크 로우 수, 잘못된 시간 형식, 허용되지 않는 롤백 값)에 대해 invalid 상태가 되는지 검증한다.
    @Test
    public void testInvalidPropertyValues() throws Exception {
        configure(personParser(0));
        runner.assertValid();

        runner.setProperty(BulkOracleInsertProcessor.BULK_INSERT_ROWS, "-1");
        runner.assertNotValid();
        runner.setProperty(BulkOracleInsertProcessor.BULK_INSERT_ROWS, "many");
        runner.assertNotValid();
        runner.setProperty(BulkOracleInsertProcessor.BULK_INSERT_ROWS, "10");
        runner.assertValid();

        runner.setProperty(BulkOracleInsertProcessor.QUERY_TIMEOUT, "soon");
        runner.assertNotValid();
        runner.setProperty(BulkOracleInsertProcessor.QUERY_TIMEOUT, "0 seconds");
        runner.assertValid();

        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "maybe");
        runner.assertNotValid();
    }

    // 정상 경로: 3건을 한 번의 INSERT ALL로 실행하고 커밋, FlowFile은 success, SEND 프로비넌스(JDBC URL), 커넥션 반환을 검증한다.
    @Test
    public void testHappyPathSingleBulkStatement() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(3));
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(List.of("INSERT ALL\n"
                + "INTO SCOTT.PERSONS ( id, name ) VALUES ( ?, ? )\n"
                + "INTO SCOTT.PERSONS ( id, name ) VALUES ( ?, ? )\n"
                + "INTO SCOTT.PERSONS ( id, name ) VALUES ( ?, ? )\n"
                + "SELECT 1 FROM dual"), dbcp.executedSql);
        assertEquals(List.of(List.of(1, "name1", 2, "name2", 3, "name3")), dbcp.executedParams);
        assertEquals(1, dbcp.commits);
        assertEquals(0, dbcp.rollbacks);
        assertEquals(1, dbcp.connectionCloses);
        assertEquals(1, dbcp.statementCloses);
        // 원래 autoCommit=true 였으므로 false로 바꾼 뒤 true로 복원해야 한다.
        assertEquals(List.of(false, true), dbcp.autoCommitSets);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());
        assertEquals(ProvenanceEventType.SEND, events.get(0).getEventType());
        assertEquals(RecordingDBCPService.JDBC_URL, events.get(0).getTransitUri());
    }

    // 레코드가 0건이면 SQL을 실행하지 않고 커밋 후 success로 라우팅되는지 검증한다.
    @Test
    public void testEmptyContentExecutesNothing() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(0));
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertTrue(dbcp.executedSql.isEmpty());
        assertEquals(1, dbcp.commits);
    }

    // 원래 autoCommit=false인 커넥션은 처리 후 autoCommit을 다시 true로 바꾸지 않는지 검증한다.
    @Test
    public void testAutoCommitNotTouchedWhenOriginallyFalse() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        dbcp.initialAutoCommit = false;
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(List.of(false), dbcp.autoCommitSets);
    }

    // 스키마명/테이블명에 FlowFile 속성 Expression Language가 평가되는지 검증한다.
    @Test
    public void testExpressionLanguageForSchemaAndTable() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.setProperty(BulkOracleInsertProcessor.SCHEMA_NAME, "${target.schema}");
        runner.setProperty(BulkOracleInsertProcessor.TABLE_NAME, "${target.table}");
        runner.enqueue(new byte[0], Map.of("target.schema", "HR", "target.table", "EMP"));
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(1, dbcp.executedSql.size());
        assertTrue(dbcp.executedSql.get(0).contains("INTO HR.EMP ( id, name ) VALUES ( ?, ? )"), dbcp.executedSql.get(0));
        assertEquals(List.of(1, "name1"), dbcp.executedParams.get(0));
    }

    // 숫자/불리언/DECIMAL/DATE 타입이 바인딩 파라미터로 타입을 유지한 채 전달되는지 검증한다(날짜는 문자열이 아닌 java.sql.Date).
    @Test
    public void testMultipleFieldTypes() throws Exception {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("i", RecordFieldType.INT);
        parser.addSchemaField("l", RecordFieldType.LONG);
        parser.addSchemaField("d", RecordFieldType.DOUBLE);
        parser.addSchemaField("b", RecordFieldType.BOOLEAN);
        parser.addSchemaField("amt", RecordFieldType.DECIMAL);
        parser.addSchemaField("dt", RecordFieldType.DATE);
        parser.addSchemaField("s", RecordFieldType.STRING);
        parser.addRecord(7, 9007199254740993L, 1.5d, true, new BigDecimal("12.30"), Date.valueOf("2024-01-15"), "x");
        final RecordingDBCPService dbcp = configure(parser);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertTrue(dbcp.executedSql.get(0).contains(
                "INTO SCOTT.PERSONS ( i, l, d, b, amt, dt, s ) VALUES ( ?, ?, ?, ?, ?, ?, ? )"),
                dbcp.executedSql.get(0));
        assertEquals(Arrays.asList(7, 9007199254740993L, 1.5d, true, new BigDecimal("12.30"), Date.valueOf("2024-01-15"), "x"),
                dbcp.executedParams.get(0));
    }

    // 레코드 수가 벌크 로우 수와 정확히 같을 때(경계값) 한 번의 INSERT ALL로 모두 처리되는지 검증한다.
    @Test
    public void testRecordCountEqualsBulkSizeBoundary() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(100));
        runner.setProperty(BulkOracleInsertProcessor.BULK_INSERT_ROWS, "100");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(1, dbcp.executedSql.size());
        assertEquals(100, countInto(dbcp.executedSql));
    }

    // SQL 실행 중 일반 SQLException이 발생하면 JDBC 롤백 후 failure로 라우팅되고 에러 속성이 설정되며 커밋하지 않는지 검증한다.
    @Test
    public void testSqlErrorRoutesToFailure() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(2));
        dbcp.executeFailure = sql -> new SQLException("ORA-00942: table or view does not exist");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(BulkOracleInsertProcessor.REL_FAILURE).get(0);
        ff.assertAttributeEquals(BulkOracleInsertProcessor.BULK_INSERT_ERROR, "ORA-00942: table or view does not exist");
        assertEquals(0, dbcp.commits);
        assertEquals(1, dbcp.rollbacks);
        assertEquals(1, dbcp.connectionCloses);
        assertEquals(1, dbcp.statementCloses);
        runner.assertPenalizeCount(0);
    }

    // SQLTransientException 계열은 retry로 라우팅되고 페널티가 부여되는지 검증한다.
    @Test
    public void testTransientErrorRoutesToRetry() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(2));
        dbcp.executeFailure = sql -> new SQLTransientConnectionException("connection reset");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_RETRY, 1);
        runner.assertPenalizeCount(1);
        assertEquals(1, dbcp.rollbacks);
    }

    // BatchUpdateException의 원인(cause)이 일시적 예외이면 retry로 라우팅되는지 검증한다.
    @Test
    public void testBatchUpdateExceptionWithTransientCauseRoutesToRetry() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        dbcp.executeFailure = sql -> {
            final BatchUpdateException e = new BatchUpdateException("batch failed", new int[0]);
            e.initCause(new SQLTransientConnectionException("timeout"));
            return e;
        };
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_RETRY, 1);
    }

    // Record Reader 파싱 실패(MalformedRecordException)는 SQL을 실행하지 않고 failure로 라우팅되는지 검증한다.
    @Test
    public void testMalformedRecordRoutesToFailure() throws Exception {
        final MockRecordParser parser = personParser(3);
        parser.failAfter(0, MockRecordFailureType.MALFORMED_RECORD_EXCEPTION);
        final RecordingDBCPService dbcp = configure(parser);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        assertTrue(dbcp.executedSql.isEmpty());
        assertEquals(0, dbcp.commits);
        assertEquals(1, dbcp.rollbacks);
    }

    // 실패 시 롤백(rollback-on-failure=true)이면 FlowFile이 어떤 관계로도 전송되지 않고 입력 큐에 남는지 검증한다.
    @Test
    public void testRollbackOnFailureKeepsFlowFileQueued() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");
        dbcp.executeFailure = sql -> new SQLException("boom");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkOracleInsertProcessor.REL_SUCCESS, 0);
        runner.assertTransferCount(BulkOracleInsertProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(BulkOracleInsertProcessor.REL_RETRY, 0);
        runner.assertQueueNotEmpty();
        assertEquals(1, dbcp.rollbacks);
        assertEquals(1, dbcp.connectionCloses);
    }

    // ---------------------------------------------------------------------------------------------
    // 확인된 결함 재현 테스트 (수정된 결함은 회귀 테스트, 미수정 결함은 @Disabled)
    // ---------------------------------------------------------------------------------------------

    // [B-1] onTrigger는 session.get()으로 입력 FlowFile을 요구하므로 INPUT_REQUIRED로 선언되어야 한다.
    // (이전에는 INPUT_FORBIDDEN이라 NiFi에서 유입 연결을 만들 수 없어 아무 일도 하지 않았다. nifi-mock은 입력 요구사항을 강제하지 않음)
    @Test
    public void testInputRequirementAllowsIncomingFlowFiles() {
        final InputRequirement requirement = BulkOracleInsertProcessor.class.getAnnotation(InputRequirement.class);
        assertNotEquals(InputRequirement.Requirement.INPUT_FORBIDDEN, requirement.value());
        assertEquals(InputRequirement.Requirement.INPUT_REQUIRED, requirement.value());
    }

    // [B-2] 벌크 경계를 넘는 경우 모든 레코드가 배치로 나뉘어 삽입되어야 한다. 5건/벌크 2건이면 2+2+1건.
    // (이전에는 배치를 실행하는 순간 현재 레코드를 버리고 count를 리셋하지 않아 3~5가 유실되고 빈 INSERT ALL을 실행했다)
    @Test
    public void testAllRecordsInsertedAcrossBatches() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(5));
        runner.setProperty(BulkOracleInsertProcessor.BULK_INSERT_ROWS, "2");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(5, countInto(dbcp.executedSql));
        assertEquals(3, dbcp.executedSql.size());
        for (String sql : dbcp.executedSql) {
            assertTrue(sql.contains("\nINTO "), "empty INSERT ALL executed: " + sql);
        }
        assertEquals(List.of(List.of(1, "name1", 2, "name2"), List.of(3, "name3", 4, "name4"), List.of(5, "name5")), dbcp.executedParams);
        // 꽉 찬 배치용 PreparedStatement는 재사용하고 남은 데이터용은 별도로 생성한다.
        assertEquals(2, dbcp.preparedSql.size());
        assertEquals(2, dbcp.statementCloses);
    }

    // [B-8] Bulk Insert Rows=0이면 한 건도 삽입하지 않으므로 POSITIVE_INTEGER로 검증해 invalid가 되어야 한다.
    // (이전에는 NON_NEGATIVE로 0을 허용해 빈 INSERT ALL만 실행하고 success로 보냈다)
    @Test
    public void testZeroBulkRowsIsInvalid() throws Exception {
        configure(personParser(1));
        runner.setProperty(BulkOracleInsertProcessor.BULK_INSERT_ROWS, "0");
        runner.assertNotValid();
    }

    // [B-3] Oracle JDBC는 문장 끝의 ';'를 허용하지 않는다(ORA-00933/ORA-00911). 생성 SQL이 ';'로 끝나지 않아야 한다.
    @Test
    public void testGeneratedSqlHasNoTrailingSemicolon() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.enqueue(new byte[0]);
        runner.run();

        assertFalse(dbcp.executedSql.get(0).trim().endsWith(";"), dbcp.executedSql.get(0));
    }

    // [B-4] 작은따옴표가 포함된 값도 SQL 문에 들어가지 않고 바인딩 파라미터로 그대로 전달되어야 한다(SQL 인젝션 방지).
    @Test
    public void testQuoteInStringValueIsEscaped() throws Exception {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addRecord(1, "O'Brien'); DELETE FROM SCOTT.PERSONS; --");
        final RecordingDBCPService dbcp = configure(parser);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertFalse(dbcp.executedSql.get(0).contains("O'Brien"), dbcp.executedSql.get(0));
        assertFalse(dbcp.executedSql.get(0).contains("DELETE"), dbcp.executedSql.get(0));
        assertEquals(List.of(1, "O'Brien'); DELETE FROM SCOTT.PERSONS; --"), dbcp.executedParams.get(0));
    }

    // [B-5] NULL 값은 문자열 리터럴('NULL'/'null')이 아니라 컬럼 타입에 맞는 setNull()로 바인딩되어야 한다.
    @Test
    public void testNullValuesInsertedAsSqlNull() throws Exception {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addRecord(null, null);
        final RecordingDBCPService dbcp = configure(parser);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(List.of(new RecordingDBCPService.SqlNull(Types.INTEGER), new RecordingDBCPService.SqlNull(Types.VARCHAR)),
                dbcp.executedParams.get(0));
    }

    // [B-6] Query Timeout은 초 단위 API인 Statement.setQueryTimeout()에 초로 전달되어야 한다.
    // (이전에는 밀리초로 변환해 전달하여 30초 → 30000초가 되었다)
    @Test
    public void testQueryTimeoutPassedInSeconds() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.setProperty(BulkOracleInsertProcessor.QUERY_TIMEOUT, "30 seconds");
        runner.enqueue(new byte[0]);
        runner.run();

        assertEquals(List.of(30), dbcp.queryTimeouts);
    }

    // [B-7] 스키마명을 비워도 필수인 테이블명은 검증되어야 한다.
    // (이전에는 TABLE_NAME이 SCHEMA_NAME에 dependsOn 되어 있어 검증이 생략되고 실행 시 "INTO null.null"이 생성되었다)
    @Test
    public void testTableNameRequiredWithoutSchema() throws Exception {
        configure(personParser(1));
        runner.removeProperty(BulkOracleInsertProcessor.SCHEMA_NAME);
        runner.removeProperty(BulkOracleInsertProcessor.TABLE_NAME);
        runner.assertNotValid();
    }

    // [B-7] 스키마명 없이 테이블명만 설정하면 "null." 접두어 없이 테이블명만으로 INSERT해야 한다.
    @Test
    public void testInsertWithoutSchemaUsesTableNameOnly() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.removeProperty(BulkOracleInsertProcessor.SCHEMA_NAME);
        runner.setProperty(BulkOracleInsertProcessor.TABLE_NAME, "TABLE");
        runner.assertValid();
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(1, dbcp.executedSql.size());
        final String sql = dbcp.executedSql.get(0);
        assertTrue(sql.contains("\nINTO TABLE ( id, name ) VALUES ( ?, ? )"), sql);
        assertFalse(sql.contains("null."), sql);
        assertEquals(List.of(List.of(1, "name1")), dbcp.executedParams);
    }

    // [B-4] 스키마명/테이블명(EL 평가 결과)이 유효한 식별자가 아니면 SQL을 실행하지 않고 failure로 라우팅되어야 한다.
    @Test
    public void testInvalidTableNameRoutesToFailure() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.setProperty(BulkOracleInsertProcessor.TABLE_NAME, "${target.table}");
        runner.enqueue(new byte[0], Map.of("target.table", "PERSONS (id) VALUES (1) INTO X"));
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(BulkOracleInsertProcessor.REL_FAILURE).get(0);
        assertTrue(ff.getAttribute(BulkOracleInsertProcessor.BULK_INSERT_ERROR).contains("테이블명"),
                ff.getAttribute(BulkOracleInsertProcessor.BULK_INSERT_ERROR));
        assertTrue(dbcp.preparedSql.isEmpty());
        assertTrue(dbcp.executedSql.isEmpty());
        assertEquals(0, dbcp.commits);
        assertEquals(1, dbcp.rollbacks);
    }

    // [B-4] 스키마명에 '.'이나 공백 등이 포함되어도 failure로 라우팅되어야 한다.
    @Test
    public void testInvalidSchemaNameRoutesToFailure() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.setProperty(BulkOracleInsertProcessor.SCHEMA_NAME, "SCOTT.X --");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(BulkOracleInsertProcessor.REL_FAILURE).get(0);
        assertTrue(ff.getAttribute(BulkOracleInsertProcessor.BULK_INSERT_ERROR).contains("스키마명"),
                ff.getAttribute(BulkOracleInsertProcessor.BULK_INSERT_ERROR));
        assertTrue(dbcp.executedSql.isEmpty());
    }

    // [B-4] 레코드 스키마의 컬럼명이 유효한 식별자가 아니면 failure로 라우팅되어야 한다.
    @Test
    public void testInvalidColumnNameRoutesToFailure() throws Exception {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name) VALUES (1); --", RecordFieldType.STRING);
        parser.addRecord(1, "x");
        final RecordingDBCPService dbcp = configure(parser);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(BulkOracleInsertProcessor.REL_FAILURE).get(0);
        assertTrue(ff.getAttribute(BulkOracleInsertProcessor.BULK_INSERT_ERROR).contains("컬럼명"),
                ff.getAttribute(BulkOracleInsertProcessor.BULK_INSERT_ERROR));
        assertTrue(dbcp.executedSql.isEmpty());
    }

    // 큰따옴표로 감싼 인용 식별자와 $/#/_를 포함한 비인용 식별자는 허용되어야 한다.
    @Test
    public void testQuotedAndSpecialCharacterIdentifiersAccepted() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.setProperty(BulkOracleInsertProcessor.SCHEMA_NAME, "\"My Schema\"");
        runner.setProperty(BulkOracleInsertProcessor.TABLE_NAME, "T_$#1");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertTrue(dbcp.executedSql.get(0).contains("INTO \"My Schema\".T_$#1 ( id, name ) VALUES ( ?, ? )"), dbcp.executedSql.get(0));
    }

    // [B-9] TIMESTAMP 값은 문자열이 아니라 java.sql.Timestamp로 바인딩되어야 한다(NLS 포맷 비의존).
    @Test
    public void testTimestampBoundAsTimestamp() throws Exception {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("ts", RecordFieldType.TIMESTAMP);
        final java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2024-01-15 10:20:30.123");
        parser.addRecord(1, ts);
        final RecordingDBCPService dbcp = configure(parser);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(List.of(1, ts), dbcp.executedParams.get(0));
    }

    // [B-10] NiFi BYTE는 단일 Byte 값이며 ByteBuffer로 캐스팅하지 않고(이전에는 ClassCastException) Byte로 바인딩되어야 한다.
    @Test
    public void testSingleByteBoundAsByte() throws Exception {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("flag", RecordFieldType.BYTE);
        parser.addRecord(1, (byte) 7);
        parser.addRecord(2, null);
        final RecordingDBCPService dbcp = configure(parser);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(List.of(1, (byte) 7, 2, new RecordingDBCPService.SqlNull(Types.TINYINT)), dbcp.executedParams.get(0));
    }

    // [B-10] Avro bytes(ARRAY of BYTE, Object[] of Byte)는 "[Ljava.lang.Object;@..." 문자열이 아니라 byte[]로 setBytes 바인딩되어야 한다.
    // NULL은 VARBINARY로 setNull 되어야 한다. (union이 아닌 bytes 타입도 Decimal 판정에서 예외가 나지 않아야 한다)
    @Test
    public void testByteArrayBoundAsBytes() throws Exception {
        final MockRecordParser parser = byteArrayParser();
        parser.addRecord(1, new Object[]{(byte) 0xCA, (byte) 0xFE});
        parser.addRecord(2, null);
        final RecordingDBCPService dbcp = configure(parser);
        runner.setProperty(BulkOracleInsertProcessor.AVRO_SCHEMA, binAvroSchema("\"bytes\""));
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        final List<Object> params = dbcp.executedParams.get(0);
        assertEquals(4, params.size());
        assertEquals(1, params.get(0));
        assertArrayEquals(new byte[]{(byte) 0xCA, (byte) 0xFE}, (byte[]) params.get(1));
        assertEquals(2, params.get(2));
        assertEquals(new RecordingDBCPService.SqlNull(Types.VARBINARY), params.get(3));
    }

    // [B-10] ByteBuffer 값은 backing array 전체가 아니라 position~limit 구간만 사용해야 한다(array offset 고려).
    @Test
    public void testByteBufferSliceUsesOnlyRemainingBytes() throws Exception {
        final MockRecordParser parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("bin", RecordFieldType.BYTE);
        final ByteBuffer slice = ByteBuffer.wrap(new byte[]{9, 1, 2, 3, 9}, 1, 3).slice();
        parser.addRecord(1, slice);
        final RecordingDBCPService dbcp = configure(parser);
        runner.setProperty(BulkOracleInsertProcessor.AVRO_SCHEMA, binAvroSchema("[\"null\", \"bytes\"]"));
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) dbcp.executedParams.get(0).get(1));
    }

    // [B-10] Avro 스키마에서 Decimal Logical Type인 bytes 컬럼은 BigDecimal로 변환되어 바인딩되어야 한다.
    @Test
    public void testDecimalBytesBoundAsBigDecimal() throws Exception {
        final MockRecordParser parser = byteArrayParser();
        final byte[] unscaled = BigInteger.valueOf(1234).toByteArray();
        final Object[] boxed = new Object[unscaled.length];
        for (int i = 0; i < unscaled.length; i++) {
            boxed[i] = unscaled[i];
        }
        parser.addRecord(1, boxed);
        final RecordingDBCPService dbcp = configure(parser);
        runner.setProperty(BulkOracleInsertProcessor.AVRO_SCHEMA,
                binAvroSchema("[\"null\", {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 10, \"scale\": 2}]"));
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(List.of(1, new BigDecimal("12.34")), dbcp.executedParams.get(0));
    }

    // [B-10] 바이트 컬럼이 Avro 스키마에 없으면 NPE가 아니라 명확한 메시지와 함께 failure로 라우팅되어야 한다.
    @Test
    public void testByteArrayMissingInAvroSchemaRoutesToFailure() throws Exception {
        final MockRecordParser parser = byteArrayParser();
        parser.addRecord(1, new Object[]{(byte) 1});
        final RecordingDBCPService dbcp = configure(parser); // AVRO_SCHEMA에는 bin 필드가 없다(id, name)
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(BulkOracleInsertProcessor.REL_FAILURE).get(0);
        final String error = ff.getAttribute(BulkOracleInsertProcessor.BULK_INSERT_ERROR);
        assertTrue(error.contains("Avro 스키마") && error.contains("'bin'"), error);
        assertTrue(dbcp.executedSql.isEmpty());
        assertEquals(1, dbcp.rollbacks);
    }

    // [B-11] BatchUpdateException의 cause가 없어도 next exception 체인에 일시적 예외가 있으면 retry로 라우팅되어야 한다.
    @Test
    public void testBatchUpdateExceptionWithTransientNextExceptionRoutesToRetry() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        dbcp.executeFailure = sql -> {
            final BatchUpdateException e = new BatchUpdateException("batch failed", new int[0]);
            e.setNextException(new SQLException("ORA-xxxx"));
            e.getNextException().setNextException(new SQLTransientException("ORA-12170: TNS connect timeout"));
            return e;
        };
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_RETRY, 1);
        runner.assertPenalizeCount(1);
    }

    // [B-11] cause/next exception이 없는 BatchUpdateException은 NPE 없이 failure로 라우팅되고 메시지가 속성에 기록되어야 한다.
    @Test
    public void testBatchUpdateExceptionWithoutCauseRoutesToFailure() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        dbcp.executeFailure = sql -> new BatchUpdateException("ORA-00001: unique constraint violated", new int[0]);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(BulkOracleInsertProcessor.REL_FAILURE).get(0)
                .assertAttributeEquals(BulkOracleInsertProcessor.BULK_INSERT_ERROR, "ORA-00001: unique constraint violated");
        runner.assertPenalizeCount(0);
    }

    // [B-11] 일반 SQLException이라도 cause가 일시적 예외이면 retry로 라우팅되어야 한다.
    @Test
    public void testSqlExceptionWithTransientCauseRoutesToRetry() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        dbcp.executeFailure = sql -> new SQLException("wrapped", new SQLTransientConnectionException("reset"));
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_RETRY, 1);
    }

    // [B-11] 예외 메시지가 null이면 속성 값이 null이 되지 않고 예외 클래스명이 기록되어야 한다.
    @Test
    public void testNullExceptionMessageUsesClassName() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        dbcp.executeFailure = sql -> new SQLException();
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(BulkOracleInsertProcessor.REL_FAILURE).get(0)
                .assertAttributeEquals(BulkOracleInsertProcessor.BULK_INSERT_ERROR, SQLException.class.getName());
    }

    // [B-11] rollback-on-failure=true로 세션을 롤백하면 즉시 재처리 루프를 막기 위해 프로세서가 yield 되어야 한다.
    @Test
    public void testRollbackOnFailureYieldsProcessor() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "true");
        dbcp.executeFailure = sql -> new SQLException("boom");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertQueueNotEmpty();
        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
    }

    // rollback-on-failure=false(기본값)로 failure/retry에 라우팅하는 경우에는 yield 하지 않는다.
    @Test
    public void testRouteOnFailureDoesNotYield() throws Exception {
        final RecordingDBCPService dbcp = configure(personParser(1));
        dbcp.executeFailure = sql -> new SQLException("boom");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        assertFalse(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
    }

    // [B-12] 성공 시 RecordReader가 닫혀야 한다.
    @Test
    public void testRecordReaderClosedOnSuccess() throws Exception {
        final CloseTrackingRecordParser parser = closeTrackingPersonParser(3);
        configure(parser);
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_SUCCESS, 1);
        assertEquals(1, parser.readerCloses);
    }

    // [B-12] SQL 실행 실패 시에도 RecordReader가 닫혀야 한다.
    @Test
    public void testRecordReaderClosedOnFailure() throws Exception {
        final CloseTrackingRecordParser parser = closeTrackingPersonParser(3);
        final RecordingDBCPService dbcp = configure(parser);
        dbcp.executeFailure = sql -> new SQLException("boom");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkOracleInsertProcessor.REL_FAILURE, 1);
        assertEquals(1, parser.readerCloses);
    }
}
