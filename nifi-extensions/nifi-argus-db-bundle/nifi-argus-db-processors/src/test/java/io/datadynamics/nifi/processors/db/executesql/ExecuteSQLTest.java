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
package io.datadynamics.nifi.processors.db.executesql;

import io.datadynamics.nifi.processors.db.H2DBCPService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExecuteSQL 프로세서에 대한 테스트.
 * 유입 FlowFile 유무에 따른 SQL 쿼리 실행, 정상 조회 결과의 Avro 변환, 그리고 잘못된 쿼리에 대한
 * 실패 처리 흐름을 검증한다.
 */
public class ExecuteSQLTest {

    private TestRunner runner;
    private H2DBCPService dbcp;

    // PERSONS 테이블을 생성하고 테스트 데이터를 적재한 뒤, ExecuteSQL 프로세서와 DBCP 컨트롤러 서비스를 구성한다.
    @BeforeEach
    public void setup() throws Exception {
        dbcp = new H2DBCPService("execsql_test");
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS PERSONS");
            stmt.execute("CREATE TABLE PERSONS (ID INT PRIMARY KEY, NAME VARCHAR(100), CODE INT)");
            stmt.execute("INSERT INTO PERSONS VALUES (1, 'Alice', 10)");
            stmt.execute("INSERT INTO PERSONS VALUES (2, 'Bob', 20)");
        }

        runner = TestRunners.newTestRunner(ExecuteSQL.class);
        runner.addControllerService("dbcp", dbcp);
        runner.enableControllerService(dbcp);
        runner.setProperty(AbstractExecuteSQL.DBCP_SERVICE, "dbcp");
    }

    // 유입 FlowFile 없이 프로세서에 설정된 SQL_SELECT_QUERY만으로 쿼리가 실행되어 REL_SUCCESS로
    // FlowFile 1개가 전달되고, RESULT_ROW_COUNT 속성과 mime.type(application/avro-binary)이
    // 올바르게 설정되는지 검증한다.
    @Test
    public void testSelectQueryNoIncomingFlowFile() {
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM PERSONS ORDER BY ID");
        runner.setIncomingConnection(false);
        runner.run();

        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "2");
        flowFile.assertAttributeEquals("mime.type", "application/avro-binary");
    }

    // 유입 FlowFile의 내용을 SQL 쿼리로 사용하여 실행했을 때 REL_SUCCESS로 FlowFile 1개가 전달되고,
    // RESULT_ROW_COUNT 속성이 조회된 행 수와 일치하며 결과 FlowFile의 크기가 0보다 큰지 검증한다.
    @Test
    public void testSelectQueryWithIncomingFlowFile() {
        runner.setIncomingConnection(true);
        runner.enqueue("SELECT NAME FROM PERSONS WHERE ID = 2");
        runner.run();

        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "1");
        assertTrue(flowFile.getSize() > 0);
    }

    // 존재하지 않는 테이블을 조회하는 잘못된 SQL 쿼리를 실행했을 때 FlowFile이 REL_FAILURE
    // 관계로 전달되는지 검증한다.
    @Test
    public void testBadQueryRoutesToFailure() {
        runner.setIncomingConnection(true);
        runner.enqueue("SELECT * FROM NO_SUCH_TABLE");
        runner.run();

        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 1);
    }
}
