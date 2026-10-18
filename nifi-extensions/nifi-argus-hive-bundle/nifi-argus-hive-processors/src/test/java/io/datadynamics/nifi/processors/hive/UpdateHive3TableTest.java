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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.cloudera.hive.jdbc.HS2Driver;

/**
 * UpdateHive3Table 프로세서 단위 테스트.
 *
 * 벤더 풀(VendorHive3ConnectionPool)과 포팅된 표준 풀(Hive3ConnectionPool)을 모두 "hive3-dbcp-service"에 연결할 수
 * 있는지(검증 통과) 그리고 실행 시 풀에서 커넥션을 얻어 Hive DDL 경로까지 진행하는지 검증한다.
 * 백엔드(가짜 HS2Driver → H2)는 Hive 전용 구문(DESC FORMATTED)을 지원하지 않으므로, 커넥션 획득과 SHOW TABLES 이후
 * Hive DDL의 SQL 오류로 failure에 가는 것까지만 확인한다(풀 타입 문제였다면 검증 단계나 서비스 조회에서 실패한다).
 */
class UpdateHive3TableTest {

    @BeforeEach
    void setUp() {
        HS2Driver.reset();
    }

    @ParameterizedTest
    @EnumSource(HivePoolType.class)
    void acceptsBothConnectionPoolTypes(final HivePoolType poolType) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateHive3Table());
        final FixedSchemaReaderFactory reader = new FixedSchemaReaderFactory();
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(UpdateHive3Table.RECORD_READER, "reader");
        runner.setProperty(UpdateHive3Table.TABLE_NAME, "users");
        // onTrigger가 "hive3-record-writer"를 항상 조회하는데, nifi-mock은 의존성(Update Field Names=true)이
        // 충족되지 않은 프로퍼티 조회를 오류로 처리하므로 Writer를 설정한다(이 테스트 경로에서 Writer는 쓰이지 않음).
        final UnusedWriterFactory writer = new UnusedWriterFactory();
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(UpdateHive3Table.UPDATE_FIELD_NAMES, "true");
        // 같은 이유로 "hive3-create-table-management" 등이 의존하는 Create If Not Exists 전략을 쓴다.
        runner.setProperty(UpdateHive3Table.CREATE_TABLE, UpdateHive3Table.CREATE_IF_NOT_EXISTS);
        runner.setProperty(UpdateHive3Table.RECORD_WRITER_FACTORY, "writer");
        poolType.addTo(runner, UpdateHive3Table.HIVE_DBCP_SERVICE, "update_pool_type");
        runner.assertValid();

        final int connectsBefore = HS2Driver.CONNECT_COUNT.get();
        runner.enqueue(new byte[0]);
        runner.run();

        assertTrue(HS2Driver.CONNECT_COUNT.get() > connectsBefore, "풀을 통해 드라이버 커넥션을 얻어야 한다");
        runner.assertAllFlowFilesTransferred(UpdateHive3Table.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateHive3Table.REL_FAILURE).get(0);
        out.assertAttributeEquals(UpdateHive3Table.ATTR_OUTPUT_TABLE, "users");
        // SQL 단계(Hive DDL)의 오류여야 한다
        final boolean sqlError = runner.getLogger().getErrorMessages().stream()
                .anyMatch(m -> m.getMsg().contains("Exception while processing"));
        assertTrue(sqlError, "Hive DDL 단계의 SQL 오류로 failure에 가야 한다");
        assertEquals(0, runner.getFlowFilesForRelationship(UpdateHive3Table.REL_SUCCESS).size());
    }

    /**
     * 고정 스키마(id INT, name STRING)만 제공하는 테스트용 RecordReaderFactory.
     * UpdateHive3Table은 Reader를 스키마 판단에만 사용한다.
     */
    private static class FixedSchemaReaderFactory extends AbstractControllerService implements RecordReaderFactory {
        private static final RecordSchema SCHEMA = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType())));

        @Override
        public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in,
                                               final long inputLength, final ComponentLog logger) {
            return new RecordReader() {
                @Override
                public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) {
                    return null;
                }

                @Override
                public RecordSchema getSchema() {
                    return SCHEMA;
                }

                @Override
                public void close() {
                }
            };
        }
    }

    /**
     * 이 테스트 경로에서는 호출되지 않는 RecordSetWriterFactory.
     */
    private static class UnusedWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
        @Override
        public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out,
                                            final Map<String, String> variables) {
            throw new UnsupportedOperationException("테스트에서 사용하지 않음");
        }
    }
}
