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
package io.datadynamics.nifi.processors.db;

import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExecuteFastSQL 프로세서에 대한 테스트.
 * H2 인메모리 데이터베이스를 이용하여 SELECT 쿼리 결과를 CSV로 변환하는 기능을 검증한다.
 * 확인된 결함은 재현 테스트로 남겨 두었으며, 아직 수정되지 않은 결함만 {@code @Disabled("BUG: ...")} 상태로 유지한다.
 */
public class ExecuteFastSQLTest {

    private TestRunner runner;
    private H2DBCPService dbcp;

    // FRUITS 테이블을 생성하고 테스트 데이터를 적재한 뒤, ExecuteFastSQL 프로세서와 DBCP 컨트롤러 서비스를 구성한다.
    @BeforeEach
    public void setup() throws Exception {
        dbcp = new H2DBCPService("execfastsql_test");
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS FRUITS");
            stmt.execute("DROP TABLE IF EXISTS NUMBERS");
            stmt.execute("DROP TABLE IF EXISTS TYPES");
            stmt.execute("CREATE TABLE FRUITS (ID INT PRIMARY KEY, NAME VARCHAR(100))");
            stmt.execute("INSERT INTO FRUITS VALUES (1, 'apple')");
            stmt.execute("INSERT INTO FRUITS VALUES (2, 'banana')");
            stmt.execute("INSERT INTO FRUITS VALUES (3, 'cherry')");
        }

        runner = TestRunners.newTestRunner(ExecuteFastSQL.class);
        runner.addControllerService("dbcp", dbcp);
        runner.enableControllerService(dbcp);
        runner.setProperty(ExecuteFastSQL.DBCP_SERVICE, "dbcp");
    }

    // NUMBERS 테이블에 1..rowCount 범위의 행을 적재한다.
    private void createNumbers(final int rowCount) throws Exception {
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE NUMBERS (N INT PRIMARY KEY, LABEL VARCHAR(20))");
        }
        try (Connection con = dbcp.getConnection();
             PreparedStatement ps = con.prepareStatement("INSERT INTO NUMBERS VALUES (?, ?)")) {
            for (int i = 1; i <= rowCount; i++) {
                ps.setInt(1, i);
                ps.setString(2, "row-" + i);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // 모든 성공 FlowFile의 내용을 행 단위로 모아 반환한다(빈 줄 제외).
    private List<String> allRows() {
        final List<String> rows = new ArrayList<>();
        for (MockFlowFile ff : runner.getFlowFilesForRelationship(ExecuteFastSQL.REL_SUCCESS)) {
            final String content = new String(ff.toByteArray(), StandardCharsets.UTF_8);
            Arrays.stream(content.split("\\r?\\n")).filter(s -> !s.isEmpty()).forEach(rows::add);
        }
        return rows;
    }

    private int totalFetchCount() {
        int sum = 0;
        for (MockFlowFile ff : runner.getFlowFilesForRelationship(ExecuteFastSQL.REL_SUCCESS)) {
            sum += Integer.parseInt(ff.getAttribute("executesql.fetch.count"));
        }
        return sum;
    }

    // 단일 스레드로 SELECT 쿼리를 실행했을 때 결과가 CSV 형식의 FlowFile(들)로 REL_SUCCESS 관계로 전달되고,
    // mime.type 속성이 text/csv로 설정되며 모든 행의 데이터가 결과 내용에 포함되는지 검증한다.
    @Test
    public void testSelectToCsvSingleThread() {
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT * FROM FRUITS ORDER BY ID");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(false);
        runner.run();

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteFastSQL.REL_SUCCESS);
        assertTrue(flowFiles.size() >= 1);

        final StringBuilder allContent = new StringBuilder();
        for (MockFlowFile ff : flowFiles) {
            ff.assertAttributeEquals("mime.type", "text/csv");
            allContent.append(new String(ff.toByteArray()));
        }
        final String content = allContent.toString();
        assertTrue(content.contains("apple"));
        assertTrue(content.contains("banana"));
        assertTrue(content.contains("cherry"));
    }

    // Fetch Size가 결과 행 수보다 클 때 정확히 하나의 FlowFile에 모든 행이 순서대로(헤더 없이) 기록되고,
    // 행 수/시간 속성과 RECEIVE 프로비넌스 이벤트가 생성되는지 검증한다.
    @Test
    public void testExactCsvContentAndAttributes() {
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT ID, NAME FROM FRUITS ORDER BY ID");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(false);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_SUCCESS, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(ExecuteFastSQL.REL_SUCCESS).get(0);
        assertEquals(List.of("1,apple", "2,banana", "3,cherry"), allRows());
        ff.assertAttributeEquals("executesql.fetch.count", "3");
        ff.assertAttributeEquals("mime.type", "text/csv");
        ff.assertAttributeExists("executesql.query.fetch.time");
        ff.assertAttributeExists("executesql.query.total.time");
        runner.assertProvenanceEvent(ProvenanceEventType.RECEIVE);
    }

    // 결과가 0건인 쿼리는 어떤 FlowFile도 생성하지 않고(빈 FlowFile은 제거됨) 실패로도 라우팅하지 않는지 검증한다.
    @Test
    public void testEmptyResultProducesNoFlowFile() {
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT * FROM FRUITS WHERE ID < 0");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "10");
        runner.setIncomingConnection(false);
        runner.run();

        runner.assertTransferCount(ExecuteFastSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteFastSQL.REL_FAILURE, 0);
        assertTrue(runner.getProvenanceEvents().isEmpty());
    }

    // NULL 컬럼 값은 CSV에서 빈 필드로 기록되고, 콤마/따옴표/개행을 포함한 문자열은 CSV 규칙에 맞게 인용되는지 검증한다.
    @Test
    public void testNullAndCsvQuoting() throws Exception {
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("INSERT INTO FRUITS VALUES (4, NULL)");
            stmt.execute("INSERT INTO FRUITS VALUES (5, 'a,b')");
            stmt.execute("INSERT INTO FRUITS VALUES (6, 'say \"hi\"')");
        }
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT ID, NAME FROM FRUITS WHERE ID >= 4 ORDER BY ID");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(false);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_SUCCESS, 1);
        assertEquals(List.of("4,", "5,\"a,b\"", "6,\"say \"\"hi\"\"\""), allRows());
    }

    // 다양한 SQL 타입(INT, BIGINT, DECIMAL, DOUBLE, BOOLEAN, DATE, TIMESTAMP, VARCHAR)이 toString() 기반으로
    // CSV에 기록되는 형태를 검증한다(DECIMAL scale 보존, 날짜 ISO 형태).
    @Test
    public void testMultipleColumnTypes() throws Exception {
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE TYPES (I INT, B BIGINT, D DECIMAL(10,2), F DOUBLE, BO BOOLEAN, DT DATE, TS TIMESTAMP, S VARCHAR(10))");
            stmt.execute("INSERT INTO TYPES VALUES (1, 9007199254740993, 12.30, 1.5, TRUE, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:20:30', 'x')");
            stmt.execute("INSERT INTO TYPES VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        }
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT * FROM TYPES ORDER BY I NULLS LAST");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(false);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_SUCCESS, 1);
        final List<String> rows = allRows();
        assertEquals(2, rows.size());
        final String[] first = rows.get(0).split(",", -1);
        assertEquals("1", first[0]);
        assertEquals("9007199254740993", first[1]);
        assertEquals("12.30", first[2]);
        assertEquals("1.5", first[3]);
        assertEquals("true", first[4]);
        assertEquals("2024-01-15", first[5]);
        assertTrue(first[6].startsWith("2024-01-15") && first[6].contains("10:20:30"), first[6]);
        assertEquals("x", first[7]);
        assertEquals(",,,,,,,", rows.get(1));
    }

    // SQL SELECT 쿼리 속성에서 Expression Language(환경 변수)를 평가해 쿼리를 구성하는지 검증한다.
    @Test
    public void testExpressionLanguageInQuery() {
        runner.setEnvironmentVariableValue("fruit.table", "FRUITS");
        runner.setEnvironmentVariableValue("fruit.fetch", "100");
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT NAME FROM ${fruit.table} WHERE ID = 2");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "${fruit.fetch}");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setIncomingConnection(false);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_SUCCESS, 1);
        assertEquals(List.of("banana"), allRows());
    }

    // 결과 행 수가 Fetch Size와 정확히 같을 때(경계값) 모든 행이 하나의 FlowFile로 출력되는지 검증한다.
    @Test
    public void testRowCountEqualsFetchSizeBoundary() throws Exception {
        createNumbers(500);
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT N, LABEL FROM NUMBERS ORDER BY N");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "500");
        runner.setIncomingConnection(false);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_SUCCESS, 1);
        assertEquals(500, totalFetchCount());
        final List<String> rows = allRows();
        assertEquals(500, rows.size());
        assertEquals("1,row-1", rows.get(0));
        assertEquals("500,row-500", rows.get(499));
    }

    // 잘못된 설정값(Fetch Threads 0/문자, 음수 Fetch Size, 잘못된 시간 형식, DBCP 미설정)에 대해
    // 프로세서가 invalid 상태가 되는지 검증한다.
    @Test
    public void testInvalidConfiguration() {
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT * FROM FRUITS");
        runner.assertValid();

        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "0");
        runner.assertNotValid();
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "abc");
        runner.assertNotValid();
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "2");
        runner.assertValid();

        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "-1");
        runner.assertNotValid();
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "10");
        runner.assertValid();

        runner.setProperty(ExecuteFastSQL.QUERY_TIMEOUT, "ten seconds");
        runner.assertNotValid();
        runner.setProperty(ExecuteFastSQL.QUERY_TIMEOUT, "10 sec");
        runner.assertValid();

        final TestRunner noDbcp = TestRunners.newTestRunner(ExecuteFastSQL.class);
        noDbcp.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT 1");
        noDbcp.assertNotValid();
    }

    // 유입 FlowFile 없이 존재하지 않는 테이블을 조회하면 ProcessException이 발생(세션 롤백)하는지 검증한다.
    @Test
    public void testBadQueryWithoutInputThrows() {
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT * FROM NO_SUCH_TABLE");
        runner.setIncomingConnection(false);
        assertThrows(AssertionError.class, () -> runner.run());
        runner.assertTransferCount(ExecuteFastSQL.REL_SUCCESS, 0);
    }

    // ---------------------------------------------------------------------------------------------
    // 확인된 결함 재현 테스트 (F-1~F-8 수정 완료로 활성화, 미수정 결함만 @Disabled)
    // ---------------------------------------------------------------------------------------------

    // FetchThread 루프 조건이 `rs.next() && numberOfRows < fetchSize` 순서여서, 한 배치가 가득 찬 시점에
    // 이미 커서를 전진시킨 행이 기록되지 않고 버려진다. 3건/FetchSize=1이면 banana가 사라진다.
    @Test
    public void testNoRowLossAcrossFetchSizeBoundary() throws Exception {
        createNumbers(250);
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT N, LABEL FROM NUMBERS ORDER BY N");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(false);
        runner.run();

        assertEquals(250, allRows().size());
        assertEquals(250, totalFetchCount());
    }

    // 기본 Fetch Size(0)에서는 FetchThread의 배치 크기가 0이 되어 어떤 행도 출력되지 않는다(각 스레드가 한 행씩 소비 후 종료).
    @Test
    public void testDefaultFetchSizeEmitsAllRows() {
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT ID, NAME FROM FRUITS ORDER BY ID");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setIncomingConnection(false);
        runner.run();

        assertEquals(3, allRows().size());
    }

    // 유입 FlowFile을 처리한 뒤 원본을 transfer/remove 하지 않아 세션 커밋이 실패한다(모든 유입 FlowFile 경로가 동작 불가).
    @Test
    public void testIncomingFlowFileQueryContent() {
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(true);
        runner.enqueue("SELECT NAME FROM FRUITS WHERE ID = 3");

        assertDoesNotThrow(() -> runner.run());
        assertEquals(List.of("cherry"), allRows());
        runner.assertQueueEmpty();
    }

    // 유입 FlowFile 속성으로 Expression Language를 평가하는 경로(정상 동작이라면 success). 위와 같은 원인으로 실패한다.
    @Test
    public void testExpressionLanguageFromFlowFileAttributes() {
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT NAME FROM FRUITS WHERE ID = ${fruit.id}");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(true);
        runner.enqueue("", java.util.Map.of("fruit.id", "1"));

        assertDoesNotThrow(() -> runner.run());
        assertEquals(List.of("apple"), allRows());
    }

    // SQL 오류 시 SQLException은 failure로 라우팅되지 않고 ProcessException으로 재던져져 세션이 롤백되고,
    // 입력 FlowFile이 페널티 없이 큐로 되돌아가 무한 재시도된다. (catch(Exception)의 transfer도 throw로 롤백됨)
    @Test
    public void testBadQueryWithInputRoutesToFailure() {
        runner.setIncomingConnection(true);
        runner.enqueue("SELECT * FROM NO_SUCH_TABLE");

        assertDoesNotThrow(() -> runner.run());
        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
    }

    // [F-8] SQL SELECT 쿼리가 비어 있고 유입 연결도 없으면 프로세서 시작(@OnScheduled)이 거부되어야 한다.
    // (이전에는 그대로 실행되어 session.read(null)에서 실패했다. ValidationContext는 연결 정보를 제공하지 않으므로
    // customValidate가 아닌 스케줄 시점에 검사한다)
    @Test
    public void testNoQueryAndNoIncomingConnectionIsInvalid() {
        runner.setIncomingConnection(false);
        final AssertionError error = assertThrows(AssertionError.class, () -> runner.run());
        // @OnScheduled 예외는 InvocationTargetException으로 감싸져 cause 체인에 들어 있다.
        boolean found = false;
        for (Throwable t = error; t != null && !found; t = t.getCause()) {
            found = String.valueOf(t.getMessage()).contains(ExecuteFastSQL.NO_QUERY_NO_INPUT_MESSAGE);
        }
        assertTrue(found, String.valueOf(error));
        runner.assertTransferCount(ExecuteFastSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteFastSQL.REL_FAILURE, 0);
    }

    // [F-8] SQL SELECT 쿼리가 비어 있어도 유입 연결이 있으면 FlowFile 콘텐츠를 쿼리로 사용하여 정상 실행되어야 한다.
    @Test
    public void testNoQueryWithIncomingConnectionUsesContent() {
        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.enqueue("SELECT ID, NAME FROM FRUITS ORDER BY ID");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_SUCCESS, 1);
        assertEquals(List.of("1,apple", "2,banana", "3,cherry"), allRows());
    }

    // FetchThread 내부 예외(RuntimeException)는 ExecutorService가 삼켜 버려 onTrigger가 성공으로 끝난다.
    // 예: SELECT가 아닌 문장은 getResultSet()이 null → 스레드에서 NPE → 아무 신호 없이 종료(UPDATE는 이미 반영됨).
    @Test
    public void testFetchThreadFailureIsReported() throws Exception {
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "UPDATE FRUITS SET NAME = 'x' WHERE ID = 1");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(false);

        assertThrows(AssertionError.class, () -> runner.run());
    }

    // 유입 FlowFile이 있을 때 결과 집합을 반환하지 않는 문장(UPDATE)은 입력 FlowFile을 페널티 후 failure로 보내고,
    // 성공 FlowFile은 하나도 남기지 않는지 검증한다.
    @Test
    public void testNonSelectWithInputRoutesToFailure() {
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "2");
        runner.setIncomingConnection(true);
        runner.enqueue("UPDATE FRUITS SET NAME = 'x' WHERE ID = 1");

        assertDoesNotThrow(() -> runner.run());
        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
    }

    // 유입 FlowFile로 여러 배치가 생성될 때 모든 행이 success로 나가고 입력 FlowFile은 제거되는지 검증한다.
    @Test
    public void testIncomingFlowFileMultipleBatches() throws Exception {
        createNumbers(250);
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT N, LABEL FROM NUMBERS");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "4");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "40");
        runner.setIncomingConnection(true);
        runner.enqueue("");

        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_SUCCESS, 7);
        assertEquals(250, totalFetchCount());
        assertEquals(250, allRows().size());
        runner.assertQueueEmpty();
    }

    // [F-7] BINARY/VARBINARY 값은 Base64로 기록되어야 한다.
    // (이전에는 Object.toString()으로 기록되어 "[B@1a2b3c" 같은 객체 식별자가 CSV에 들어갔다)
    @Test
    public void testBinaryColumnIsEncoded() throws Exception {
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE TYPES (ID INT, BIN VARBINARY(4))");
            stmt.execute("INSERT INTO TYPES VALUES (1, X'CAFE')");
        }
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT ID, BIN FROM TYPES");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(false);
        runner.run();

        assertFalse(allRows().get(0).contains("[B@"), allRows().get(0));
        assertEquals(List.of("1,yv4="), allRows());
    }

    // [F-7] BLOB은 Base64로, CLOB은 문자열 내용으로 기록되어야 한다. CLOB의 쉼표/따옴표/줄바꿈은 CSV 규칙대로 Quote되어야 한다.
    @Test
    public void testLobColumnsAreEncoded() throws Exception {
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("CREATE TABLE TYPES (ID INT, B BLOB, C CLOB)");
            stmt.execute("INSERT INTO TYPES VALUES (1, X'00FF10', 'he said \"hi\", bye')");
            stmt.execute("INSERT INTO TYPES VALUES (2, NULL, NULL)");
        }
        runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT ID, B, C FROM TYPES ORDER BY ID");
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "100");
        runner.setIncomingConnection(false);
        runner.run();

        assertEquals(List.of("1,AP8Q,\"he said \"\"hi\"\", bye\"", "2,,"), allRows());
    }

    // 여러 FetchThread가 thread-safe 하지 않은 ProcessSession(create/write/remove/transfer)을 동시에 호출한다.
    // 다중 스레드에서 반복 실행하면 세션 상태 손상(ConcurrentModificationException, 누락)이 간헐적으로 발생한다.
    @Test
    public void testMultiThreadedFetchIsSafe() throws Exception {
        createNumbers(2000);
        for (int attempt = 0; attempt < 30; attempt++) {
            runner.clearTransferState();
            runner.setProperty(ExecuteFastSQL.SQL_SELECT_QUERY, "SELECT N, LABEL FROM NUMBERS");
            runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "8");
            runner.setProperty(ExecuteFastSQL.FETCH_SIZE, "50");
            runner.setIncomingConnection(false);
            assertDoesNotThrow(() -> runner.run());
            assertEquals(2000, allRows().size());
        }
    }

    // [F-9] FlowFile 콘텐츠의 쿼리는 플랫폼 기본 charset이 아니라 UTF-8로 읽어야 한다(한글 리터럴 포함 쿼리).
    @Test
    public void testQueryContentReadAsUtf8() throws Exception {
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("INSERT INTO FRUITS VALUES (4, '사과')");
        }
        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteFastSQL.FETCH_THREADS, "1");
        runner.enqueue("SELECT ID, NAME FROM FRUITS WHERE NAME = '사과'".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteFastSQL.REL_SUCCESS, 1);
        assertEquals(List.of("4,사과"), allRows());
    }
}
