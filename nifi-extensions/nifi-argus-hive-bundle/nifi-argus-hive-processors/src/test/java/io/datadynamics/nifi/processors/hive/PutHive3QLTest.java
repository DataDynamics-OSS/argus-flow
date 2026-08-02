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
package io.datadynamics.nifi.processors.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

/**
 * PutHive3QL 프로세서 단위 테스트.
 *
 * H2 인메모리 DB 백엔드로, FlowFile 본문의 HiveQL(DML)이 실제로 실행되어 success로 전송되고
 * 데이터가 반영되는지 검증한다.
 */
class PutHive3QLTest {

    private MockHiveConnectionPool poolWithTable(final String db) throws Exception {
        final MockHiveConnectionPool pool = new MockHiveConnectionPool(db);
        try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (id INT)");
        }
        return pool;
    }

    private TestRunner runnerFor(final MockHiveConnectionPool pool) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new PutHive3QL());
        runner.addControllerService("pool", pool);
        runner.enableControllerService(pool);
        runner.setProperty(AbstractHive3QLProcessor.HIVE_DBCP_SERVICE, "pool");
        return runner;
    }

    @Test
    void executesInsertStatement() throws Exception {
        final MockHiveConnectionPool pool = poolWithTable("put_insert");
        final TestRunner runner = runnerFor(pool);

        runner.enqueue("INSERT INTO t (id) VALUES (42)".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutHive3QL.REL_SUCCESS, 1);
        try (Connection c = pool.getConnection();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM t WHERE id = 42")) {
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void invalidStatementRoutesToFailure() throws Exception {
        final MockHiveConnectionPool pool = poolWithTable("put_invalid");
        final TestRunner runner = runnerFor(pool);

        // 존재하지 않는 테이블 → SQL 오류 → failure
        runner.enqueue("INSERT INTO no_such_table (id) VALUES (1)".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutHive3QL.REL_FAILURE, 1);
    }
}
