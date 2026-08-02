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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExecuteFastSQL 프로세서에 대한 테스트.
 * H2 인메모리 데이터베이스를 이용하여 SELECT 쿼리 결과를 CSV로 변환하는 기능을 검증한다.
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
}
