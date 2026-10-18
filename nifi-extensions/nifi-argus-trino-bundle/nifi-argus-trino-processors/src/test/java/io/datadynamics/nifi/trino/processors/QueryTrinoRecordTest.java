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
package io.datadynamics.nifi.trino.processors;

import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryTrinoRecordTest {

    private TestRunner runner;
    private H2TrinoConnectionService service;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(QueryTrinoRecord.class);
        service = new H2TrinoConnectionService();
        runner.addControllerService("trino", service);
        runner.enableControllerService(service);
        final MockRecordWriter writer = new MockRecordWriter(null, false);
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(TrinoProcessorProperties.TRINO_CONNECTION_SERVICE, "trino");
        runner.setProperty(QueryTrinoRecord.RECORD_WRITER, "writer");

        service.execute("CREATE TABLE nation (id INT, name VARCHAR(20))");
        for (int i = 1; i <= 5; i++) {
            service.execute("INSERT INTO nation VALUES (" + i + ", 'n" + i + "')");
        }
    }

    @Test
    void writesAllRowsWithQueryInfo() {
        runner.setIncomingConnection(false);
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT id, name FROM nation ORDER BY id;");
        runner.run();

        runner.assertAllFlowFilesTransferred(QueryTrinoRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("1,n1\n2,n2\n3,n3\n4,n4\n5,n5\n");
        out.assertAttributeEquals("record.count", "5");
        out.assertAttributeEquals("mime.type", "text/plain");
        out.assertAttributeEquals(TrinoAttributes.QUERY_ID, H2TrinoConnectionService.INFO.queryId());
        out.assertAttributeEquals(TrinoAttributes.QUERY_STATE, "FINISHED");
        out.assertAttributeEquals(TrinoAttributes.PROCESSED_ROWS, "10");
        out.assertAttributeEquals(TrinoAttributes.PROCESSED_BYTES, "100");
        out.assertAttributeEquals(TrinoAttributes.CPU_TIME_MS, "1");
        out.assertAttributeEquals(TrinoAttributes.WALL_TIME_MS, "2");
        out.assertAttributeEquals(TrinoAttributes.QUEUED_TIME_MS, "3");
        out.assertAttributeNotExists("fragment.identifier");
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals("jdbc:trino://h2-fake:8080#" + H2TrinoConnectionService.INFO.queryId(), runner.getProvenanceEvents().get(0).getTransitUri());
    }

    @Test
    void splitsByMaxRows() {
        runner.setIncomingConnection(false);
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT id FROM nation ORDER BY id");
        runner.setProperty(QueryTrinoRecord.MAX_ROWS_PER_FLOWFILE, "2");
        runner.run();

        final List<MockFlowFile> outputs = runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS);
        assertEquals(3, outputs.size());
        final String fragmentId = outputs.get(0).getAttribute("fragment.identifier");
        for (int i = 0; i < 3; i++) {
            outputs.get(i).assertAttributeEquals("fragment.index", Integer.toString(i));
            outputs.get(i).assertAttributeEquals("fragment.count", "3");
            outputs.get(i).assertAttributeEquals("fragment.identifier", fragmentId);
        }
        outputs.get(0).assertContentEquals("1\n2\n");
        outputs.get(2).assertContentEquals("5\n");
        outputs.get(2).assertAttributeEquals("record.count", "1");
    }

    @Test
    void exactMultipleDoesNotCreateEmptyTrailingFlowFile() {
        runner.setIncomingConnection(false);
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT id FROM nation WHERE id <= 4");
        runner.setProperty(QueryTrinoRecord.MAX_ROWS_PER_FLOWFILE, "2");
        runner.run();
        assertEquals(2, runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS).size());
    }

    @Test
    void emptyResultProducesOneFlowFile() {
        runner.setIncomingConnection(false);
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT id FROM nation WHERE id > 100");
        runner.setProperty(QueryTrinoRecord.MAX_ROWS_PER_FLOWFILE, "2");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryTrinoRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS).get(0);
        out.assertAttributeEquals("record.count", "0");
        out.assertAttributeEquals("fragment.count", "1");
    }

    @Test
    void queryFromContentAndOriginal() {
        runner.setIncomingConnection(true);
        runner.enqueue("SELECT name FROM nation WHERE id = 3", Map.of("k", "v"));
        runner.run();
        runner.assertTransferCount(QueryTrinoRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(QueryTrinoRecord.REL_ORIGINAL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("n3\n");
        out.assertAttributeEquals("k", "v");
        assertTrue(runner.getProvenanceEvents().stream().anyMatch(e -> e.getEventType().name().equals("FETCH")));
    }

    @Test
    void queryFromExpressionLanguage() {
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT name FROM nation WHERE id = ${nation.id}");
        runner.enqueue("", Map.of("nation.id", "4"));
        runner.run();
        runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS).get(0).assertContentEquals("n4\n");
    }

    @Test
    void sqlErrorWithInputGoesToFailure() {
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT nope FROM nation");
        runner.enqueue("");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryTrinoRecord.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_FAILURE).get(0);
        failed.assertAttributeExists(TrinoAttributes.ERROR_MESSAGE);
        failed.assertAttributeExists(TrinoAttributes.ERROR_CODE);
        failed.assertAttributeEquals(TrinoAttributes.QUERY_ID, H2TrinoConnectionService.INFO.queryId());
        assertTrue(failed.isPenalized());
    }

    @Test
    void sqlErrorWithoutInputYields() {
        runner.setIncomingConnection(false);
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT nope FROM nation");
        runner.run();
        assertEquals(0, runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_SUCCESS).size());
        assertEquals(0, runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_FAILURE).size());
        assertTrue(runner.isYieldCalled());
    }

    @Test
    void multipleStatementsGoToFailure() {
        runner.setIncomingConnection(true);
        runner.enqueue("SELECT 1; SELECT 2");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryTrinoRecord.REL_FAILURE, 1);
        assertTrue(runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_FAILURE).get(0)
                .getAttribute(TrinoAttributes.ERROR_MESSAGE).contains("여러 문장"));
    }

    @Test
    void writerFailureDropsPartialResults() throws Exception {
        final FailingRecordWriter failing = new FailingRecordWriter(4);
        runner.addControllerService("failing", failing);
        runner.enableControllerService(failing);
        runner.setProperty(QueryTrinoRecord.RECORD_WRITER, "failing");
        runner.setProperty(QueryTrinoRecord.SQL_QUERY, "SELECT id FROM nation");
        runner.setProperty(QueryTrinoRecord.MAX_ROWS_PER_FLOWFILE, "2");
        runner.enqueue("");
        runner.run();
        runner.assertTransferCount(QueryTrinoRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(QueryTrinoRecord.REL_FAILURE, 1);
        assertFalse(runner.getFlowFilesForRelationship(QueryTrinoRecord.REL_FAILURE).get(0)
                .getAttribute(TrinoAttributes.ERROR_MESSAGE).isEmpty());
    }

    @Test
    void refusesToStartWithoutQueryOrInput() {
        runner.setIncomingConnection(false);
        assertThrows(AssertionError.class, () -> runner.run());
    }
}
