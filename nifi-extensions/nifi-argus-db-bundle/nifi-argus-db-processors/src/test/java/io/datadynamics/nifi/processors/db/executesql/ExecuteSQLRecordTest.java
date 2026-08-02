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
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

/**
 * ExecuteSQLRecord 프로세서에 대한 테스트.
 * H2 인메모리 데이터베이스에서 조회한 결과를 레코드 라이터(RecordWriter)를 통해 FlowFile로
 * 기록하는 기능을 검증한다.
 */
public class ExecuteSQLRecordTest {

    private TestRunner runner;
    private H2DBCPService dbcp;

    // ITEMS 테이블을 생성하고 테스트 데이터를 적재한 뒤, ExecuteSQLRecord 프로세서와 DBCP 컨트롤러 서비스,
    // 레코드 라이터(MockRecordWriter)를 구성한다.
    @BeforeEach
    public void setup() throws Exception {
        dbcp = new H2DBCPService("execsqlrecord_test");
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS ITEMS");
            stmt.execute("CREATE TABLE ITEMS (ID INT PRIMARY KEY, NAME VARCHAR(100))");
            stmt.execute("INSERT INTO ITEMS VALUES (1, 'aaa')");
            stmt.execute("INSERT INTO ITEMS VALUES (2, 'bbb')");
            stmt.execute("INSERT INTO ITEMS VALUES (3, 'ccc')");
        }

        runner = TestRunners.newTestRunner(ExecuteSQLRecord.class);
        runner.addControllerService("dbcp", dbcp);
        runner.enableControllerService(dbcp);
        runner.setProperty(AbstractExecuteSQL.DBCP_SERVICE, "dbcp");

        final MockRecordWriter writer = new MockRecordWriter(null, false);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
    }

    // SELECT 쿼리 실행 결과가 레코드 라이터를 통해 FlowFile 하나로 기록되고, RESULT_ROW_COUNT 속성이
    // 조회된 행 수(3)와 일치하며 기록된 내용에 각 행의 데이터가 포함되는지 검증한다.
    @Test
    public void testSelectQueryWithRecordWriter() {
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM ITEMS ORDER BY ID");
        runner.setIncomingConnection(false);
        runner.run();

        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "3");
        final String content = new String(flowFile.toByteArray());
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("aaa"));
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("ccc"));
    }
}
