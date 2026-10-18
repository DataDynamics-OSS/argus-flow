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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.Statement;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.datadynamics.nifi.processors.hive.util.HiveJdbcCommon;

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
            // Avro 이름 규칙([A-Za-z_][A-Za-z0-9_]*)에 맞지 않는 컬럼 이름 (H2 getColumnName은 별칭이 아닌 실제 컬럼명을 돌려준다)
            s.execute("CREATE TABLE weird (\"1st-id\" INT, \"person name\" VARCHAR(50), \"x.y:z\" INT)");
            s.execute("INSERT INTO weird VALUES (1, 'a', 10)");
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

    // 벤더 풀(VendorHive3ConnectionPool)과 포팅된 표준 풀(Hive3ConnectionPool) 모두 Hive3DBCPService로 연결·실행되어야 한다.
    @ParameterizedTest
    @EnumSource(HivePoolType.class)
    void acceptsBothConnectionPoolTypes(final HivePoolType poolType) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new SelectHive3QL());
        final DBCPService pool = poolType.addTo(runner, AbstractHive3QLProcessor.HIVE_DBCP_SERVICE, "select_pool_type");
        try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS person (id INT, name VARCHAR(50))");
            s.execute("DELETE FROM person");
            s.execute("INSERT INTO person VALUES (1, 'a'), (2, 'b')");
        }
        runner.setProperty(SelectHive3QL.HIVEQL_SELECT_QUERY, "SELECT id, name FROM person ORDER BY id");
        runner.setIncomingConnection(false);
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(SelectHive3QL.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(SelectHive3QL.REL_SUCCESS).get(0);
        assertEquals(2L, countAvroRecords(runner.getContentAsByteArray(out)));
    }

    // "테이블/컬럼 이름 정규화"가 켜져 있으면 Avro에서 쓸 수 없는 컬럼 이름도 정규화되어 Avro 스키마가 만들어져야 한다(업스트림 1.28 동작).
    @Test
    void normalizesColumnNamesForAvroWhenEnabled() throws Exception {
        final MockHiveConnectionPool pool = pool("select_normalize");

        final TestRunner runner = TestRunners.newTestRunner(new SelectHive3QL());
        runner.addControllerService("pool", pool);
        runner.enableControllerService(pool);
        runner.setProperty(AbstractHive3QLProcessor.HIVE_DBCP_SERVICE, "pool");
        runner.setProperty(HiveJdbcCommon.NORMALIZE_NAMES_FOR_AVRO, "true");
        runner.setProperty(SelectHive3QL.HIVEQL_SELECT_QUERY, "SELECT * FROM weird");
        runner.setIncomingConnection(false);

        runner.run();

        runner.assertAllFlowFilesTransferred(SelectHive3QL.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(SelectHive3QL.REL_SUCCESS).get(0);
        try (DataFileStream<GenericRecord> dfs = new DataFileStream<>(
                new ByteArrayInputStream(runner.getContentAsByteArray(out)), new GenericDatumReader<>())) {
            final Schema schema = dfs.getSchema();
            assertNotNull(schema.getField("_1st_id"));
            assertNotNull(schema.getField("person_name"));
            assertNotNull(schema.getField("y_z"));
            assertEquals(3, schema.getFields().size());
        }
    }

    @Test
    void createSchemaNormalizesColumnNamesOnlyWhenRequested() throws Exception {
        final MockHiveConnectionPool pool = pool("select_create_schema");
        try (Connection c = pool.getConnection(); Statement s = c.createStatement();
             java.sql.ResultSet rs = s.executeQuery("SELECT * FROM weird")) {
            final Schema normalized = HiveJdbcCommon.createSchema(rs, "rec", true, false);
            assertNotNull(normalized.getField("_1st_id"));
            assertNotNull(normalized.getField("person_name"));
            // Hive 형식("table.column")처럼 마지막 마침표 뒤만 컬럼명으로 쓰고, 그 뒤 정규화한다
            assertNotNull(normalized.getField("y_z"));
        }
    }
}
