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
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * BulkOracleInsertProcessor에 대한 테스트.
 * 필수 프로퍼티 검증(유효성 체크)과 정상적으로 프로퍼티가 설정되었을 때의 유효성을 검증한다.
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
}
