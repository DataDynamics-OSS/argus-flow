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

import io.datadynamics.nifi.processors.hive.compaction.CompletionState;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * apache/hive:4.0.1 + hive-jdbc 4.0.1 로 CompactHive3Table 을 확인한다. Docker 가 없으면 건너뛴다.
 * 이 구성(내장 Metastore)은 Cleaner 가 돌지 않아 'ready for cleaning' 에서 멈춘다.
 */
@Testcontainers(disabledWithoutDocker = true)
class CompactHive3TableHive4IntegrationTest extends AbstractCompactionScenariosIntegrationTest {

    @Container
    private static final GenericContainer<?> HIVE = HiveTestContainers.hive4(true);

    @Override
    protected GenericContainer<?> hive() {
        return HIVE;
    }

    @Override
    protected String jdbcJarProperty() {
        return "hive.it.hive4.jdbc.jar";
    }

    @Override
    protected String defaultJdbcJar() {
        return "hive-jdbc-4.0.1-standalone.jar";
    }

    @Override
    protected List<String> expectedShowCompactionsLabels() {
        return List.of("compactionid", "dbname", "tabname", "partname", "type", "state", "workerhost", "workerid", "enqueuetime", "starttime",
                "duration", "hadoopjobid", "errormessage", "initiatorhost", "initiatorid", "poolname", "txnid", "nexttxnid", "committime",
                "hightestwriteid");
    }

    @Override
    protected boolean duplicateCompactThrows() {
        return true;
    }

    @Override
    protected boolean supportsRebalance() {
        return true;
    }

    @Override
    protected CompletionState majorCompletionState() {
        return CompletionState.READY_FOR_CLEANING;
    }

    @Test
    @Order(10)
    void succeededCompletionStateKeepsWaitingUntilCompactionTimeoutWithoutCleaner() throws Exception {
        sql("CREATE TABLE it_wait_cleaner (id INT) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "INSERT INTO it_wait_cleaner VALUES (1)", "INSERT INTO it_wait_cleaner VALUES (2)");
        final TestRunner runner = runner();
        runner.setProperty(CompactHive3Table.COMPLETION_STATE, CompletionState.SUCCEEDED.name());
        runner.setProperty(CompactHive3Table.COMPACTION_TIMEOUT, "60 sec");
        final Drive drive = drive(runner, target("it_wait_cleaner", null), Duration.ofMinutes(3), 2000, null);
        assertRoutedTo(drive, CompactHive3Table.REL_FAILURE);
        assertTrue(drive.states().contains("ready for cleaning"), drive.states().toString());
        assertTrue(drive.result().getAttribute(CompactHive3Table.ATTR_ERROR).contains("Compaction Timeout"));
    }
}
