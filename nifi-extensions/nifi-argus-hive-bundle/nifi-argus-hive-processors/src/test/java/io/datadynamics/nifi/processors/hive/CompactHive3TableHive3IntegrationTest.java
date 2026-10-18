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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

/**
 * apache/hive:3.1.3(번들의 Hive 3.1.3 과 같은 버전) + hive-jdbc 3.1.3 으로 CompactHive3Table 을 확인한다. Docker 가 없으면 건너뛴다.
 */
@Testcontainers(disabledWithoutDocker = true)
class CompactHive3TableHive3IntegrationTest extends AbstractCompactionScenariosIntegrationTest {

    @Container
    private static final GenericContainer<?> HIVE = HiveTestContainers.hive3(1);

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

    @Override
    protected List<String> expectedShowCompactionsLabels() {
        return List.of("compactionid", "dbname", "tabname", "partname", "type", "state", "workerid", "starttime", "duration", "hadoopjobid");
    }

    @Override
    protected boolean duplicateCompactThrows() {
        return false;
    }

    @Override
    protected CompletionState majorCompletionState() {
        return CompletionState.SUCCEEDED;
    }
}
