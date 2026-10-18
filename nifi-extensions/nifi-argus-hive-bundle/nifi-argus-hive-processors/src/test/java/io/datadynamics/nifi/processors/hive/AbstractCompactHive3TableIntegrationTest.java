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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 공식 apache/hive 이미지의 HiveServer2 로 {@link CompactHive3Table} 을 실제로 돌리는 통합 테스트의 공통 부분.
 * 하위 클래스가 컨테이너(버전)와 버전별 기대값을 정한다.
 */
abstract class AbstractCompactHive3TableIntegrationTest {

    static final String WAREHOUSE = "/opt/hive/data/warehouse";
    private static final Map<String, Driver> DRIVERS = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Boolean> READY = new ConcurrentHashMap<>();

    /** 대상 컨테이너. */
    protected abstract GenericContainer<?> hive();

    /** hive-jdbc standalone jar 파일 이름을 담은 시스템 프로퍼티 이름. */
    protected abstract String jdbcJarProperty();

    /** 기본 jar 파일 이름(시스템 프로퍼티가 없을 때, IDE 실행용). */
    protected abstract String defaultJdbcJar();

    protected String jdbcUrl() {
        return "jdbc:hive2://" + hive().getHost() + ":" + hive().getMappedPort(10000) + "/default";
    }

    protected IsolatedHiveJdbcService service() throws Exception {
        final String jar = System.getProperty(jdbcJarProperty(), defaultJdbcJar());
        final String dir = System.getProperty("hive.it.jdbc.dir", "target/hive-jdbc");
        final Driver driver = DRIVERS.computeIfAbsent(jar, name -> {
            try {
                return IsolatedHiveJdbcService.loadDriver(new File(dir, name));
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        });
        return new IsolatedHiveJdbcService(driver, jdbcUrl());
    }

    /** HiveServer2 가 포트를 연 뒤에도 세션을 받기까지 시간이 걸리므로 SELECT 1 이 될 때까지 기다린다. */
    protected void awaitReady() throws Exception {
        if (READY.containsKey(getClass())) {
            return;
        }
        final long deadline = System.currentTimeMillis() + Duration.ofMinutes(4).toMillis();
        SQLException last = null;
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = service().connect(); Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                resultSet.next();
                READY.put(getClass(), true);
                return;
            } catch (final SQLException e) {
                last = e;
                Thread.sleep(3000);
            }
        }
        throw new IllegalStateException("HiveServer2 가 준비되지 않았다: " + jdbcUrl(), last);
    }

    // ---- SQL 도우미 ----

    protected void sql(final String... statements) throws Exception {
        try (Connection connection = service().connect(); Statement statement = connection.createStatement()) {
            for (final String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    /** 첫 행은 라벨. */
    protected List<List<String>> query(final String sql) throws Exception {
        try (Connection connection = service().connect(); Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final List<List<String>> rows = new ArrayList<>();
            final List<String> labels = new ArrayList<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                labels.add(metaData.getColumnLabel(i));
            }
            rows.add(labels);
            while (resultSet.next()) {
                final List<String> row = new ArrayList<>();
                for (int i = 1; i <= labels.size(); i++) {
                    row.add(resultSet.getString(i));
                }
                rows.add(row);
            }
            return rows;
        }
    }

    protected long count(final String table, final String where) throws Exception {
        try (Connection connection = service().connect(); Statement statement = connection.createStatement()) {
            statement.execute("SET hive.compute.query.using.stats=false");
            try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + table + (where == null ? "" : " WHERE " + where))) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        }
    }

    /** 웨어하우스 디렉토리의 항목(숨김 파일 제외). */
    protected List<String> listDir(final String relativePath) throws Exception {
        final ExecResult result = hive().execInContainer("bash", "-c", "ls -1 " + WAREHOUSE + "/" + relativePath + " 2>/dev/null");
        return Arrays.stream(result.getStdout().split("\n")).map(String::trim).filter(s -> !s.isEmpty() && !s.startsWith("."))
                .collect(Collectors.toList());
    }

    protected static List<String> withPrefix(final List<String> entries, final String prefix) {
        return entries.stream().filter(e -> e.startsWith(prefix)).collect(Collectors.toList());
    }

    // ---- 프로세서 구동 ----

    protected TestRunner runner() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompactHive3Table.class);
        final IsolatedHiveJdbcService service = service();
        runner.addControllerService("hive", service);
        runner.enableControllerService(service);
        runner.setProperty(CompactHive3Table.HIVE_DBCP_SERVICE, "hive");
        runner.setProperty(CompactHive3Table.DATABASE, "default");
        runner.setProperty(CompactHive3Table.TABLE, "${table}");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "${spec}");
        return runner;
    }

    protected static Map<String, String> target(final String table, final String spec) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("table", table);
        if (spec != null) {
            attributes.put("spec", spec);
        }
        return attributes;
    }

    /** 한 번의 진행 결과. */
    record Drive(MockFlowFile result, org.apache.nifi.processor.Relationship relationship, List<String> states, long elapsedMs, int triggers) {
    }

    /**
     * in progress 를 자기 루프처럼 다시 넣으며 끝날 때까지 돌린다. penalize 대신 {@code pollMillis} 만큼 쉰다(= Penalty Duration).
     * {@code restartAfterFirst} 면 첫 실행 뒤 새 프로세서 인스턴스로 이어서 돌린다(NiFi 재시작 흉내).
     */
    protected Drive drive(final TestRunner first, final Map<String, String> attributes, final Duration max, final long pollMillis,
                          final java.util.function.Supplier<TestRunner> restartAfterFirst) throws Exception {
        final long start = System.currentTimeMillis();
        TestRunner runner = first;
        runner.enqueue("", attributes);
        runner.run();
        final List<String> states = new ArrayList<>();
        int triggers = 1;
        while (true) {
            final List<MockFlowFile> inProgress = runner.getFlowFilesForRelationship(CompactHive3Table.REL_IN_PROGRESS);
            if (inProgress.isEmpty()) {
                break;
            }
            final MockFlowFile flowFile = inProgress.get(0);
            assertTrue(flowFile.isPenalized());
            addState(states, flowFile.getAttribute(CompactHive3Table.ATTR_STATE));
            if (System.currentTimeMillis() - start > max.toMillis()) {
                fail("compaction 이 " + max + " 안에 끝나지 않았다. 상태: " + states + ", 속성: " + flowFile.getAttributes());
            }
            runner.clearTransferState();
            if (triggers == 1 && restartAfterFirst != null) {
                runner = restartAfterFirst.get();
            }
            Thread.sleep(pollMillis);
            runner.enqueue("", flowFile.getAttributes());
            runner.run();
            triggers++;
        }
        for (final org.apache.nifi.processor.Relationship relationship : List.of(CompactHive3Table.REL_SUCCESS, CompactHive3Table.REL_FAILURE,
                CompactHive3Table.REL_COUNT_MISMATCH)) {
            final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
            if (!flowFiles.isEmpty()) {
                assertEquals(1, flowFiles.size());
                addState(states, flowFiles.get(0).getAttribute(CompactHive3Table.ATTR_STATE));
                final Drive drive = new Drive(flowFiles.get(0), relationship, states, System.currentTimeMillis() - start, triggers);
                System.out.printf("[%s] %s -> %s in %d ms, %d triggers, states %s, attributes %s%n", getClass().getSimpleName(),
                        attributes, relationship.getName(), drive.elapsedMs(), triggers, states, flowFiles.get(0).getAttributes());
                return drive;
            }
        }
        throw new AssertionError("FlowFile 이 어디에도 없다");
    }

    private static void addState(final List<String> states, final String state) {
        if (state != null && (states.isEmpty() || !states.get(states.size() - 1).equals(state))) {
            states.add(state);
        }
    }

    protected Drive drive(final TestRunner runner, final Map<String, String> attributes) throws Exception {
        return drive(runner, attributes, Duration.ofMinutes(5), 2000, null);
    }

    protected static void assertRoutedTo(final Drive drive, final org.apache.nifi.processor.Relationship expected) {
        assertEquals(expected, drive.relationship(), () -> "속성: " + drive.result().getAttributes());
    }
}
