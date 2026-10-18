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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecuteTrinoStatementTest {

    private TestRunner runner;
    private H2TrinoConnectionService service;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(ExecuteTrinoStatement.class);
        service = new H2TrinoConnectionService();
        runner.addControllerService("trino", service);
        runner.enableControllerService(service);
        runner.setProperty(TrinoProcessorProperties.TRINO_CONNECTION_SERVICE, "trino");
        service.execute("CREATE TABLE src (id INT)");
        service.execute("INSERT INTO src VALUES (1), (2), (3)");
    }

    @Test
    void ctasFromContentRecordsUpdateCount() throws Exception {
        runner.enqueue("CREATE TABLE dst AS SELECT * FROM src;", Map.of("a", "b"));
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0);
        out.assertAttributeExists(TrinoAttributes.UPDATE_COUNT);
        out.assertAttributeEquals(TrinoAttributes.QUERY_ID, H2TrinoConnectionService.INFO.queryId());
        out.assertAttributeEquals(TrinoAttributes.WALL_TIME_MS, "2");
        out.assertAttributeEquals("a", "b");
        out.assertAttributeNotExists(TrinoAttributes.RESULT_ROW_COUNT);
        assertEquals("REMOTE_INVOCATION", runner.getProvenanceEvents().get(0).getEventType().name());
    }

    @Test
    void insertFromPropertyWithExpressionLanguage() {
        runner.setProperty(ExecuteTrinoStatement.SQL_STATEMENT, "INSERT INTO src SELECT id + ${offset} FROM src");
        runner.enqueue("", Map.of("offset", "10"));
        runner.run();
        runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0)
                .assertAttributeEquals(TrinoAttributes.UPDATE_COUNT, "3");
    }

    @Test
    void resultSetIsDiscardedButCounted() {
        runner.setProperty(ExecuteTrinoStatement.SQL_STATEMENT, "SELECT * FROM src");
        runner.enqueue("");
        runner.run();
        final MockFlowFile out = runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0);
        out.assertAttributeEquals(TrinoAttributes.RESULT_ROW_COUNT, "3");
        out.assertAttributeNotExists(TrinoAttributes.UPDATE_COUNT);
        out.assertContentEquals("");
    }

    @Test
    void scheduledWithoutInput() {
        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteTrinoStatement.SQL_STATEMENT, "DELETE FROM src WHERE id = 1");
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0)
                .assertAttributeEquals(TrinoAttributes.UPDATE_COUNT, "1");
    }

    @Test
    void multipleStatementsGoToFailure() {
        runner.enqueue("DELETE FROM src; DROP TABLE src");
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_FAILURE).get(0);
        assertTrue(failed.getAttribute(TrinoAttributes.ERROR_MESSAGE).contains("여러 문장"));
        assertTrue(failed.isPenalized());
    }

    @Test
    void sqlErrorGoesToFailure() {
        runner.enqueue("DROP TABLE does_not_exist");
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_FAILURE).get(0);
        failed.assertAttributeExists(TrinoAttributes.ERROR_CODE);
        failed.assertAttributeExists(TrinoAttributes.ERROR_MESSAGE);
        assertTrue(failed.isPenalized());
    }

    @Test
    void sqlErrorWithoutInputYields() {
        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteTrinoStatement.SQL_STATEMENT, "DROP TABLE does_not_exist");
        runner.run();
        runner.assertTransferCount(TrinoProcessorProperties.REL_FAILURE, 0);
        assertTrue(runner.isYieldCalled());
    }

    @Test
    void refusesToStartWithoutStatementOrInput() {
        runner.setIncomingConnection(false);
        assertThrows(AssertionError.class, () -> runner.run());
    }
}
