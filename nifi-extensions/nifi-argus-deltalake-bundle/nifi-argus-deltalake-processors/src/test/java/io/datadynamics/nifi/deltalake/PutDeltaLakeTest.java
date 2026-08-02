/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.deltalake;

import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PutDeltaLake의 실제 쓰기 경로를 로컬 파일시스템(file://) Delta 테이블에 대해 검증한다.
 * Delta Kernel의 트랜잭션·Parquet 쓰기·커밋 API를 런타임으로 실행하므로 컴파일 이상의 확신을 준다.
 * 커밋 성공 + _delta_log JSON 로그 + Parquet 데이터 파일 존재로 쓰기 경로 정상 동작을 확인한다.
 */
class PutDeltaLakeTest {

    @Test
    void createsTableAndCommitsRecords(@TempDir final Path tempDir) throws Exception {
        final Path tableDir = tempDir.resolve("events");
        final String tablePath = tableDir.toUri().toString();

        final TestRunner runner = TestRunners.newTestRunner(PutDeltaLake.class);
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.INT);
        reader.addSchemaField("name", RecordFieldType.STRING);
        reader.addSchemaField("amount", RecordFieldType.DOUBLE);
        reader.addRecord(1, "alice", 10.5);
        reader.addRecord(2, "bob", 20.0);
        reader.addRecord(3, "carol", 30.25);

        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(PutDeltaLake.RECORD_READER, "reader");
        runner.setProperty(PutDeltaLake.TABLE_PATH, tablePath);

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutDeltaLake.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutDeltaLake.REL_SUCCESS).get(0)
                .assertAttributeEquals("deltalake.version", "0");
        runner.getFlowFilesForRelationship(PutDeltaLake.REL_SUCCESS).get(0)
                .assertAttributeEquals("record.count", "3");

        assertTrue(Files.exists(tableDir.resolve("_delta_log").resolve("00000000000000000000.json")),
                "버전 0 커밋 로그가 생성되어야 한다");
        assertTrue(hasParquetFile(tableDir), "최소 하나의 Parquet 데이터 파일이 있어야 한다");
    }

    @Test
    void appendsToExistingTable(@TempDir final Path tempDir) throws Exception {
        final Path tableDir = tempDir.resolve("events");
        final String tablePath = tableDir.toUri().toString();

        runOnce(tablePath, new Object[][]{{1, "a"}, {2, "b"}}, "0");
        final TestRunner second = runOnce(tablePath, new Object[][]{{3, "c"}}, "1");

        second.getFlowFilesForRelationship(PutDeltaLake.REL_SUCCESS).get(0)
                .assertAttributeEquals("deltalake.version", "1");
        assertTrue(Files.exists(tableDir.resolve("_delta_log").resolve("00000000000000000001.json")),
                "두 번째 append는 버전 1 커밋 로그를 남겨야 한다");
    }

    private TestRunner runOnce(final String tablePath, final Object[][] rows, final String expectedVersion)
            throws org.apache.nifi.reporting.InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(PutDeltaLake.class);
        final MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("id", RecordFieldType.INT);
        reader.addSchemaField("name", RecordFieldType.STRING);
        for (final Object[] row : rows) {
            reader.addRecord(row);
        }
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(PutDeltaLake.RECORD_READER, "reader");
        runner.setProperty(PutDeltaLake.TABLE_PATH, tablePath);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDeltaLake.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutDeltaLake.REL_SUCCESS).get(0)
                .assertAttributeEquals("deltalake.version", expectedVersion);
        return runner;
    }

    private static boolean hasParquetFile(final Path tableDir) throws IOException {
        try (Stream<Path> files = Files.walk(tableDir)) {
            return files.anyMatch(p -> p.getFileName().toString().endsWith(".parquet"));
        }
    }
}
