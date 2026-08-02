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

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

/**
 * SelectHive3QL 프로세서 단위 테스트.
 *
 * H2 인메모리 DB를 백엔드로 하는 {@link MockHiveConnectionPool}로, SELECT 결과가 Avro로
 * 변환되어 success로 전송되는지 검증한다(표준 JDBC 경로).
 */
class SelectHive3QLTest {

    private MockHiveConnectionPool pool(final String db) throws Exception {
        final MockHiveConnectionPool pool = new MockHiveConnectionPool(db);
        try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE person (id INT, name VARCHAR(50))");
            s.execute("INSERT INTO person VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
        return pool;
    }

    private long countAvroRecords(final byte[] data) throws Exception {
        try (DataFileStream<GenericRecord> dfs =
                     new DataFileStream<>(new ByteArrayInputStream(data), new GenericDatumReader<>())) {
            long n = 0;
            while (dfs.hasNext()) {
                dfs.next();
                n++;
            }
            return n;
        }
    }

    @Test
    void selectQueryFromPropertyReturnsAvroRows() throws Exception {
        final MockHiveConnectionPool pool = pool("select_prop");

        final TestRunner runner = TestRunners.newTestRunner(new SelectHive3QL());
        runner.addControllerService("pool", pool);
        runner.enableControllerService(pool);
        runner.setProperty(AbstractHive3QLProcessor.HIVE_DBCP_SERVICE, "pool");
        runner.setProperty(SelectHive3QL.HIVEQL_SELECT_QUERY, "SELECT id, name FROM person ORDER BY id");
        runner.setIncomingConnection(false);

        runner.run();

        runner.assertAllFlowFilesTransferred(SelectHive3QL.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(SelectHive3QL.REL_SUCCESS).get(0);
        assertEquals(3L, countAvroRecords(runner.getContentAsByteArray(out)));
    }

    @Test
    void selectQueryFromFlowFileContent() throws Exception {
        final MockHiveConnectionPool pool = pool("select_flowfile");

        final TestRunner runner = TestRunners.newTestRunner(new SelectHive3QL());
        runner.addControllerService("pool", pool);
        runner.enableControllerService(pool);
        runner.setProperty(AbstractHive3QLProcessor.HIVE_DBCP_SERVICE, "pool");
        // 쿼리 프로퍼티를 비우면 FlowFile 본문을 쿼리로 사용한다
        runner.enqueue("SELECT id FROM person WHERE id >= 2".getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(SelectHive3QL.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(SelectHive3QL.REL_SUCCESS).get(0);
        assertEquals(2L, countAvroRecords(runner.getContentAsByteArray(out)));
    }
}
