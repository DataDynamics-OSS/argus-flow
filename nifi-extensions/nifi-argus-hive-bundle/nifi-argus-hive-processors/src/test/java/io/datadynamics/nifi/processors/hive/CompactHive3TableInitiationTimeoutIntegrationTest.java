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

import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Worker 스레드가 없는 apache/hive:3.1.3 에서 요청이 'initiated' 에 머물다 Initiation Timeout 으로 failure 가 되는지 확인한다.
 */
@Testcontainers(disabledWithoutDocker = true)
class CompactHive3TableInitiationTimeoutIntegrationTest extends AbstractCompactHive3TableIntegrationTest {

    @Container
    private static final GenericContainer<?> HIVE = HiveTestContainers.hive3(0);

    @Override
    protected GenericContainer<?> hive() {
        return HIVE;
    }

    @Override
    protected String jdbcJarProperty() {
        return "hive.it.hive3.jdbc.jar";
    }

    @Override
    protected String defaultJdbcJar() {
        return "hive-jdbc-3.1.3-standalone.jar";
    }

    @Test
    void requestStuckInInitiatedFailsAfterInitiationTimeout() throws Exception {
        awaitReady();
        sql("CREATE TABLE it_no_worker (id INT) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "INSERT INTO it_no_worker VALUES (1)", "INSERT INTO it_no_worker VALUES (2)");
        final TestRunner runner = runner();
        runner.setProperty(CompactHive3Table.INITIATION_TIMEOUT, "15 sec");
        final Drive drive = drive(runner, target("it_no_worker", null), Duration.ofMinutes(2), 2000, null);
        assertRoutedTo(drive, CompactHive3Table.REL_FAILURE);
        assertEquals(java.util.List.of("initiated"), drive.states());
        final String error = drive.result().getAttribute(CompactHive3Table.ATTR_ERROR);
        assertTrue(error.contains("hive.compactor.initiator.on") && error.contains("hive.compactor.worker.threads"), error);
        assertTrue(drive.elapsedMs() >= 15_000);
    }

    @Test
    void listerSkipsPartitionsWithCompactionInProgress() throws Exception {
        awaitReady();
        sql("CREATE TABLE it_running (id INT) PARTITIONED BY (dt STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "INSERT INTO it_running PARTITION (dt='a') VALUES (1)", "INSERT INTO it_running PARTITION (dt='a') VALUES (2)",
                "INSERT INTO it_running PARTITION (dt='b') VALUES (1)", "INSERT INTO it_running PARTITION (dt='b') VALUES (2)",
                // Worker 가 없어 'initiated' 에 머문다
                "ALTER TABLE it_running PARTITION (dt='a') COMPACT 'major'");
        final TestRunner lister = listRunner("it_running");
        lister.setProperty(ListHiveCompactionCandidates.MINIMUM_PARTITION_AGE, "0 sec");
        lister.run();
        final java.util.List<org.apache.nifi.util.MockFlowFile> candidates = lister.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS);
        assertEquals(1, candidates.size());
        candidates.get(0).assertAttributeEquals(ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, "dt='b'");
        candidates.get(0).assertAttributeEquals(ListHiveCompactionCandidates.ATTR_DELTA_COUNT, "2");
    }
}
