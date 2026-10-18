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

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MaintainTrinoTableTest {

    private TestRunner runner;
    private ScriptedTrinoConnectionService service;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(MaintainTrinoTable.class);
        service = new ScriptedTrinoConnectionService();
        runner.addControllerService("trino", service);
        runner.enableControllerService(service);
        runner.setProperty(TrinoProcessorProperties.TRINO_CONNECTION_SERVICE, "trino");
        runner.setProperty(MaintainTrinoTable.TABLE_FORMAT, "ICEBERG");
        runner.setProperty(MaintainTrinoTable.OPERATION, "OPTIMIZE");
        runner.setProperty(MaintainTrinoTable.CATALOG, "${catalog}");
        runner.setProperty(MaintainTrinoTable.SCHEMA, "${schema}");
        runner.setProperty(MaintainTrinoTable.TABLE, "${table}");
    }

    @Test
    void optimizeUsesFlowFileAttributes() {
        runner.setProperty(MaintainTrinoTable.FILE_SIZE_THRESHOLD, "256MB");
        runner.setProperty(TrinoProcessorProperties.QUERY_TIMEOUT, "1 hour");
        runner.enqueue("", Map.of("catalog", "lake", "schema", "raw", "table", "ev\"ents"));
        runner.run();

        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_SUCCESS, 1);
        final String expected = "ALTER TABLE \"lake\".\"raw\".\"ev\"\"ents\" EXECUTE optimize(file_size_threshold => '256MB')";
        assertEquals(List.of(expected), service.executed);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_SUCCESS).get(0);
        out.assertAttributeEquals(TrinoAttributes.MAINTENANCE_SQL, expected);
        out.assertAttributeEquals(TrinoAttributes.MAINTENANCE_OPERATION, "OPTIMIZE");
        out.assertAttributeEquals(TrinoAttributes.QUERY_ID, "scripted_1");
        out.assertAttributeEquals(TrinoAttributes.QUERY_STATE, "FINISHED");
        out.assertAttributeEquals(TrinoAttributes.WALL_TIME_MS, "9");
        out.assertAttributeEquals(TrinoAttributes.UPDATE_COUNT, "7");
    }

    @Test
    void scheduledWithoutInputCreatesFlowFile() {
        runner.setIncomingConnection(false);
        runner.setProperty(MaintainTrinoTable.TABLE_FORMAT, "DELTA_LAKE");
        runner.setProperty(MaintainTrinoTable.OPERATION, "VACUUM");
        runner.setProperty(MaintainTrinoTable.CATALOG, "delta");
        runner.setProperty(MaintainTrinoTable.SCHEMA, "s");
        runner.setProperty(MaintainTrinoTable.TABLE, "t");
        runner.setProperty(MaintainTrinoTable.RETENTION_THRESHOLD, "168h");
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_SUCCESS, 1);
        assertEquals(List.of("CALL \"delta\".system.vacuum('s', 't', '168h')"), service.executed);
    }

    @Test
    void hiveSyncPartitionMetadata() {
        runner.setProperty(MaintainTrinoTable.TABLE_FORMAT, "HIVE");
        runner.setProperty(MaintainTrinoTable.OPERATION, "SYNC_PARTITION_METADATA");
        runner.setProperty(MaintainTrinoTable.SYNC_MODE, "FULL");
        runner.enqueue("", Map.of("catalog", "hive", "schema", "o'neil", "table", "t"));
        runner.run();
        assertEquals(List.of("CALL \"hive\".system.sync_partition_metadata('o''neil', 't', 'FULL')"), service.executed);
    }

    @Test
    void invalidCombinationsAndFormats() {
        runner.setProperty(MaintainTrinoTable.TABLE_FORMAT, "HIVE");
        runner.setProperty(MaintainTrinoTable.OPERATION, "VACUUM");
        runner.assertNotValid();
        runner.setProperty(MaintainTrinoTable.TABLE_FORMAT, "ICEBERG");
        runner.assertNotValid();
        runner.setProperty(MaintainTrinoTable.OPERATION, "EXPIRE_SNAPSHOTS");
        runner.assertValid();
        runner.setProperty(MaintainTrinoTable.RETENTION_THRESHOLD, "7 days");
        runner.assertNotValid();
        runner.setProperty(MaintainTrinoTable.RETENTION_THRESHOLD, "7d");
        runner.setProperty(MaintainTrinoTable.OPERATION, "OPTIMIZE");
        runner.setProperty(MaintainTrinoTable.FILE_SIZE_THRESHOLD, "big");
        runner.assertNotValid();
    }

    @Test
    void missingAttributeGoesToFailure() {
        runner.enqueue("", Map.of("catalog", "lake", "schema", "raw"));
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_FAILURE, 1);
        assertTrue(service.executed.isEmpty());
    }

    @Test
    void trinoErrorGoesToFailureWithQueryId() {
        service.failWith = "Retention specified (1.00d) is shorter than the minimum retention configured in the system (7.00d)";
        runner.setProperty(MaintainTrinoTable.OPERATION, "EXPIRE_SNAPSHOTS");
        runner.setProperty(MaintainTrinoTable.RETENTION_THRESHOLD, "1d");
        runner.enqueue("", Map.of("catalog", "lake", "schema", "raw", "table", "t"));
        runner.run();
        runner.assertAllFlowFilesTransferred(TrinoProcessorProperties.REL_FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(TrinoProcessorProperties.REL_FAILURE).get(0);
        failed.assertAttributeEquals(TrinoAttributes.ERROR_CODE, "42");
        failed.assertAttributeEquals(TrinoAttributes.QUERY_ID, "scripted_1");
        failed.assertAttributeEquals(TrinoAttributes.MAINTENANCE_OPERATION, "EXPIRE_SNAPSHOTS");
        assertTrue(failed.getAttribute(TrinoAttributes.ERROR_MESSAGE).contains("minimum retention"));
    }
}
