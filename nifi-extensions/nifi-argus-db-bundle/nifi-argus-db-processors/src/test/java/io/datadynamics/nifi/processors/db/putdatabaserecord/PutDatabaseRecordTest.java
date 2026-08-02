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
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import io.datadynamics.nifi.processors.db.H2DBCPService;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PutDatabaseRecord 프로세서에 대한 테스트.
 * 레코드를 데이터베이스에 INSERT/UPDATE하는 기능과, 대상 테이블이 존재하지 않을 때
 * 실패 처리되는 흐름을 검증한다.
 */
public class PutDatabaseRecordTest {

    private TestRunner runner;
    private H2DBCPService dbcp;
    private MockRecordParser parser;

    // PERSONS 테이블을 생성하고, 레코드 파서(MockRecordParser)와 PutDatabaseRecord 프로세서,
    // DBCP 컨트롤러 서비스를 INSERT 문 타입으로 구성한다.
    @BeforeEach
    public void setup() throws Exception {
        dbcp = new H2DBCPService("putdbrecord_test");
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS PERSONS");
            stmt.execute("CREATE TABLE PERSONS (ID INT PRIMARY KEY, NAME VARCHAR(100), CODE INT)");
        }

        parser = new MockRecordParser();
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        runner = TestRunners.newTestRunner(PutDatabaseRecord.class);
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.addControllerService("dbcp", dbcp);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "parser");
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "dbcp");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE);
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "PERSONS");
    }

    // 여러 레코드를 INSERT했을 때 FlowFile이 REL_SUCCESS로 전달되고, 실제 데이터베이스에
    // 레코드 수만큼 행이 삽입되었으며 각 행의 값이 올바르게 저장되었는지 검증한다.
    @Test
    public void testInsertRecords() throws Exception {
        parser.addRecord(1, "Alice", 10);
        parser.addRecord(2, "Bob", 20);
        parser.addRecord(3, "Carol", 30);

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);

        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            final ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM PERSONS");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            final ResultSet rs2 = stmt.executeQuery("SELECT NAME FROM PERSONS WHERE ID = 2");
            assertTrue(rs2.next());
            assertEquals("Bob", rs2.getString(1));
        }
    }

    // 기존 행을 UPDATE_TYPE 문과 UPDATE_KEYS(id)를 이용해 갱신했을 때 FlowFile이 REL_SUCCESS로
    // 전달되고, 데이터베이스의 해당 행 값이 갱신된 내용으로 정확히 반영되었는지 검증한다.
    @Test
    public void testUpdateRecords() throws Exception {
        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            stmt.execute("INSERT INTO PERSONS VALUES (1, 'Alice', 10)");
        }

        parser.addRecord(1, "Alicia", 99);

        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE);
        runner.setProperty(PutDatabaseRecord.UPDATE_KEYS, "id");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1);

        try (Connection con = dbcp.getConnection(); Statement stmt = con.createStatement()) {
            final ResultSet rs = stmt.executeQuery("SELECT NAME, CODE FROM PERSONS WHERE ID = 1");
            assertTrue(rs.next());
            assertEquals("Alicia", rs.getString(1));
            assertEquals(99, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    // 존재하지 않는 테이블(NO_SUCH_TABLE)로 설정하여 레코드를 처리했을 때 REL_SUCCESS로는
    // FlowFile이 전달되지 않는지(즉, 성공 건수가 0인지) 검증한다.
    @Test
    public void testMissingTableRoutesToFailure() {
        parser.addRecord(1, "Alice", 10);

        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "NO_SUCH_TABLE");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0);
    }
}
