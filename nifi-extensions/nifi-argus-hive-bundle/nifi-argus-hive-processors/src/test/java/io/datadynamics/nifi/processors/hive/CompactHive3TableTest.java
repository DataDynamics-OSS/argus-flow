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

import io.datadynamics.nifi.processors.hive.FakeHive.Version;
import io.datadynamics.nifi.processors.hive.ScriptedHive3DBCPService.Result;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 가짜 Hive3DBCPService(JDBC 프록시)로 라우팅·속성·타임아웃·재시작 이어받기를 확인한다.
 */
class CompactHive3TableTest {

    private static final long T0 = 1_792_000_000_000L;

    private final MutableClock clock = new MutableClock(T0);
    private FakeHive hive;
    private ScriptedHive3DBCPService service;
    private CompactHive3Table processor;

    private TestRunner runner(final Version version) throws Exception {
        hive = new FakeHive(version);
        return newRunner();
    }

    /** 같은 FakeHive 에 새 프로세서 인스턴스(NiFi 재시작 흉내). */
    private TestRunner newRunner() throws Exception {
        processor = new CompactHive3Table();
        processor.setClock(clock);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        service = new ScriptedHive3DBCPService();
        service.setHandler(sql -> hive.handle(sql));
        runner.addControllerService("hive", service);
        runner.enableControllerService(service);
        runner.setProperty(CompactHive3Table.HIVE_DBCP_SERVICE, "hive");
        runner.setProperty(CompactHive3Table.DATABASE, "${db}");
        runner.setProperty(CompactHive3Table.TABLE, "${table}");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "${spec}");
        return runner;
    }

    private static Map<String, String> target(final String db, final String table, final String spec) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("db", db);
        attributes.put("table", table);
        if (spec != null) {
            attributes.put("spec", spec);
        }
        return attributes;
    }

    /** in progress 결과를 다시 큐에 넣고(자기 루프) 한 번 실행한다. */
    private MockFlowFile loopOnce(final TestRunner runner) {
        final List<MockFlowFile> inProgress = runner.getFlowFilesForRelationship(CompactHive3Table.REL_IN_PROGRESS);
        assertEquals(1, inProgress.size(), "in progress FlowFile 하나가 있어야 한다");
        final MockFlowFile previous = inProgress.get(0);
        assertTrue(previous.isPenalized(), "in progress 는 penalize 돼야 한다");
        runner.clearTransferState();
        runner.enqueue(previous.getContent(), previous.getAttributes());
        runner.run();
        return previous;
    }

    private static MockFlowFile single(final TestRunner runner, final org.apache.nifi.processor.Relationship relationship) {
        runner.assertAllFlowFilesTransferred(relationship, 1);
        return runner.getFlowFilesForRelationship(relationship).get(0);
    }

    // ---- ACID 수명 주기 ----

    @ParameterizedTest
    @EnumSource(Version.class)
    void acidMajorCompactionGoesThroughInProgressLoopToSuccess(final Version version) throws Exception {
        final TestRunner runner = runner(version);
        hive.acidTable("sales.events", "dt").rowCount = 3;
        hive.addCompaction("sales", "other", null, "MAJOR", "succeeded");

        runner.enqueue("payload", target("sales", "events", "dt='2026-10-01'"));
        runner.run();
        MockFlowFile flowFile = single(runner, CompactHive3Table.REL_IN_PROGRESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ID, "2");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "MAJOR");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_STATE, "initiated");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TABLE, "sales.events");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_PARTITION, "dt='2026-10-01'");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_BEFORE, "3");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_REQUESTED_AT, Long.toString(T0));
        flowFile.assertContentEquals("payload");
        assertTrue(service.executed().contains("ALTER TABLE `sales`.`events` PARTITION (`dt`='2026-10-01') COMPACT 'major'"));
        assertTrue(service.executed().contains("SET hive.compute.query.using.stats=false"));
        assertTrue(service.executed().contains("SELECT COUNT(*) FROM `sales`.`events` WHERE `dt` = '2026-10-01'"));
        assertEquals(1, runner.getProvenanceEvents().size());

        hive.compaction(2).state = "working";
        clock.advance(60_000);
        service.clearExecuted();
        loopOnce(runner);
        flowFile = single(runner, CompactHive3Table.REL_IN_PROGRESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_STATE, "working");
        // 상태 확인은 SHOW COMPACTIONS 만 실행한다
        assertEquals(List.of("SHOW COMPACTIONS"), service.executed());

        hive.compaction(2).state = "ready for cleaning";
        clock.advance(60_000);
        loopOnce(runner);
        flowFile = single(runner, CompactHive3Table.REL_SUCCESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_STATE, "ready for cleaning");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "3");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_DURATION_MS, "120000");
        flowFile.assertAttributeNotExists(CompactHive3Table.ATTR_ERROR);
        flowFile.assertContentEquals("payload");
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void completionStateSucceededWaitsForCleaner(final Version version) throws Exception {
        final TestRunner runner = runner(version);
        hive.acidTable("default.t").rowCount = 5;
        runner.setProperty(CompactHive3Table.COMPACTION_TYPE, "MINOR");
        runner.setProperty(CompactHive3Table.COMPLETION_STATE, "SUCCEEDED");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");

        runner.enqueue("", target("default", "t", null));
        runner.run();
        single(runner, CompactHive3Table.REL_IN_PROGRESS).assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "MINOR");
        assertTrue(service.executed().contains("ALTER TABLE `default`.`t` COMPACT 'minor'"));

        hive.compaction(1).state = "ready for cleaning";
        loopOnce(runner);
        single(runner, CompactHive3Table.REL_IN_PROGRESS).assertAttributeEquals(CompactHive3Table.ATTR_STATE, "ready for cleaning");

        hive.compaction(1).state = "succeeded";
        loopOnce(runner);
        single(runner, CompactHive3Table.REL_SUCCESS).assertAttributeEquals(CompactHive3Table.ATTR_STATE, "succeeded");
    }

    @Test
    void completedImmediatelyWhenAlreadyReadyAtRequestCheck() throws Exception {
        final TestRunner runner = runner(Version.HIVE4);
        hive.acidTable("default.t").rowCount = 1;
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.setProperty(CompactHive3Table.VALIDATE_ROW_COUNT, "false");
        hive.compactError = null;
        // 요청 직후 Worker 가 이미 끝낸 경우
        service.setHandler(sql -> {
            final Result result = hive.handle(sql);
            if (sql.contains("COMPACT '")) {
                hive.compactions.get(hive.compactions.size() - 1).state = "ready for cleaning";
            }
            return result;
        });
        runner.enqueue("", target("default", "t", null));
        runner.run();
        final MockFlowFile flowFile = single(runner, CompactHive3Table.REL_SUCCESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ID, "1");
        flowFile.assertAttributeNotExists(CompactHive3Table.ATTR_ROWCOUNT_BEFORE);
        assertFalse(service.executed().stream().anyMatch(s -> s.startsWith("SELECT COUNT")));
    }

    @Test
    void insertOnlyAcidTableIsCompacted() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.acidTable("default.io").properties.put("transactional_properties", "insert_only");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "io", null));
        runner.run();
        single(runner, CompactHive3Table.REL_IN_PROGRESS).assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "MAJOR");
    }

    // ---- 재시작 이어받기 ----

    @ParameterizedTest
    @EnumSource(Version.class)
    void resumesFromAttributesAfterRestartWithoutNewRequest(final Version version) throws Exception {
        runner(version);
        hive.acidTable("sales.events", "dt", "hour").rowCount = 10;
        hive.addCompaction("sales", "events", "dt=2026-10-01/hour=03", "MAJOR", "working");

        final TestRunner restarted = newRunner();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CompactHive3Table.ATTR_ID, "1");
        attributes.put(CompactHive3Table.ATTR_REQUESTED_AT, Long.toString(T0 - 1000));
        attributes.put(CompactHive3Table.ATTR_TABLE, "sales.events");
        attributes.put(CompactHive3Table.ATTR_PARTITION, "dt='2026-10-01',hour='03'");
        attributes.put(CompactHive3Table.ATTR_ROWCOUNT_BEFORE, "10");
        attributes.put(CompactHive3Table.ATTR_TYPE, "MAJOR");
        // 프로퍼티가 가리키는 대상이 달라도 속성의 대상을 따른다
        attributes.put("db", "nope");
        attributes.put("table", "nope");
        restarted.enqueue("", attributes);
        restarted.run();
        single(restarted, CompactHive3Table.REL_IN_PROGRESS);
        assertEquals(List.of("SHOW COMPACTIONS"), service.executed());

        hive.compaction(1).state = "ready for cleaning";
        service.clearExecuted();
        loopOnce(restarted);
        final MockFlowFile done = single(restarted, CompactHive3Table.REL_SUCCESS);
        done.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "10");
        done.assertAttributeEquals(CompactHive3Table.ATTR_DURATION_MS, "1000");
        assertTrue(service.executed().contains("SELECT COUNT(*) FROM `sales`.`events` WHERE `dt` = '2026-10-01' AND `hour` = '03'"));
        assertFalse(service.executed().stream().anyMatch(s -> s.contains("COMPACT '") || s.startsWith("DESCRIBE")));
    }

    @Test
    void missingCompactionInHistoryFails() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CompactHive3Table.ATTR_ID, "42");
        attributes.put(CompactHive3Table.ATTR_REQUESTED_AT, Long.toString(T0));
        attributes.put(CompactHive3Table.ATTR_TABLE, "default.t");
        runner.enqueue("", attributes);
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("42"));
    }

    @Test
    void invalidTrackingAttributesFail() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        runner.enqueue("", Map.of(CompactHive3Table.ATTR_ID, "x1"));
        runner.enqueue("", Map.of(CompactHive3Table.ATTR_ID, "1", CompactHive3Table.ATTR_TABLE, "default.t"));
        runner.run(2);
        runner.assertAllFlowFilesTransferred(CompactHive3Table.REL_FAILURE, 2);
    }

    // ---- 타임아웃·실패 상태 ----

    @Test
    void initiationTimeoutFailsWithServerConfigurationHint() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.acidTable("default.t");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.setProperty(CompactHive3Table.INITIATION_TIMEOUT, "5 min");
        runner.enqueue("", target("default", "t", null));
        runner.run();
        single(runner, CompactHive3Table.REL_IN_PROGRESS);

        clock.advance(4 * 60_000);
        loopOnce(runner);
        single(runner, CompactHive3Table.REL_IN_PROGRESS);

        clock.advance(2 * 60_000);
        loopOnce(runner);
        final MockFlowFile failed = single(runner, CompactHive3Table.REL_FAILURE);
        final String error = failed.getAttribute(CompactHive3Table.ATTR_ERROR);
        assertTrue(error.contains("hive.compactor.initiator.on") && error.contains("hive.compactor.worker.threads"), error);
        failed.assertAttributeEquals(CompactHive3Table.ATTR_ID, "1");
    }

    @Test
    void workingStateIsNotSubjectToInitiationTimeoutButToCompactionTimeout() throws Exception {
        final TestRunner runner = runner(Version.HIVE4);
        hive.acidTable("default.t");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.setProperty(CompactHive3Table.INITIATION_TIMEOUT, "1 min");
        runner.setProperty(CompactHive3Table.COMPACTION_TIMEOUT, "10 min");
        runner.enqueue("", target("default", "t", null));
        runner.run();
        hive.compaction(1).state = "working";

        clock.advance(5 * 60_000);
        loopOnce(runner);
        single(runner, CompactHive3Table.REL_IN_PROGRESS);

        clock.advance(6 * 60_000);
        loopOnce(runner);
        final String error = single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR);
        assertTrue(error.contains("Compaction Timeout") && error.contains("id 1") && error.contains("working"), error);
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void failedStatesRouteToFailure(final Version version) throws Exception {
        final TestRunner runner = runner(version);
        hive.acidTable("default.t");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "t", null));
        runner.run();
        final FakeHive.Compaction compaction = hive.compaction(1);
        compaction.state = version == Version.HIVE4 ? "refused" : "failed";
        compaction.errorMessage = "None of the compaction thresholds met, compaction request is refused!";
        loopOnce(runner);
        final String error = single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR);
        assertTrue(error.contains(compaction.state), error);
        if (version == Version.HIVE4) {
            assertTrue(error.contains("None of the compaction thresholds met"), error);
        }
    }

    @Test
    void unknownStateKeepsWaiting() throws Exception {
        final TestRunner runner = runner(Version.HIVE4);
        hive.acidTable("default.t");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "t", null));
        runner.run();
        hive.compaction(1).state = "cleaning in some future state";
        loopOnce(runner);
        single(runner, CompactHive3Table.REL_IN_PROGRESS);
    }

    // ---- 행 수 ----

    @Test
    void rowCountMismatchRoutesToCountMismatch() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.acidTable("default.t").rowCount = 100;
        hive.rowDeltaAfterCompaction = -1;
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "t", null));
        runner.run();
        hive.compaction(1).state = "succeeded";
        loopOnce(runner);
        final MockFlowFile flowFile = single(runner, CompactHive3Table.REL_COUNT_MISMATCH);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_BEFORE, "100");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "99");
        assertNotNull(flowFile.getAttribute(CompactHive3Table.ATTR_ERROR));
    }

    // ---- 중복 요청 ----

    @Test
    void hive3SilentlyIgnoredDuplicateRequestTracksExistingCompaction() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.acidTable("default.t", "dt");
        hive.addCompaction("default", "t", "dt=a", "MINOR", "working");
        runner.enqueue("", target("default", "t", "dt='a'"));
        runner.run();
        single(runner, CompactHive3Table.REL_IN_PROGRESS).assertAttributeEquals(CompactHive3Table.ATTR_ID, "1");
        assertEquals(1, hive.compactions.size());
    }

    @Test
    void hive4AlreadyScheduledErrorTracksExistingCompaction() throws Exception {
        final TestRunner runner = runner(Version.HIVE4);
        hive.acidTable("default.t", "dt");
        hive.addCompaction("default", "t", "dt=b", "MAJOR", "succeeded");
        hive.addCompaction("default", "t", "dt=a", "MAJOR", "initiated");
        runner.enqueue("", target("default", "t", "dt='a'"));
        runner.run();
        final MockFlowFile flowFile = single(runner, CompactHive3Table.REL_IN_PROGRESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ID, "2");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_STATE, "initiated");
    }

    @Test
    void compactErrorRoutesToFailure() throws Exception {
        final TestRunner runner = runner(Version.HIVE4);
        hive.acidTable("default.t", "dt");
        hive.compactError = Result.error("FAILED: SemanticException [Error 10006]: Partition not found {dt=nope}", 10006);
        runner.enqueue("", target("default", "t", "dt='nope'"));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("Partition not found"));
    }

    @Test
    void requestWithoutNewCompactionFails() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.acidTable("default.t");
        hive.dropRequests = true;
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "t", null));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("찾지 못했다"));
    }

    // ---- Non-ACID ----

    @Test
    void nonAcidOrcTableIsConcatenatedSynchronously() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.plainTable("default.o", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "dt").rowCount = 7;
        runner.enqueue("", target("default", "o", "dt='a'"));
        runner.run();
        final MockFlowFile flowFile = single(runner, CompactHive3Table.REL_SUCCESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "CONCATENATE");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_STATE, "succeeded");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_BEFORE, "7");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "7");
        flowFile.assertAttributeNotExists(CompactHive3Table.ATTR_ID);
        assertTrue(service.executed().contains("ALTER TABLE `default`.`o` PARTITION (`dt`='a') CONCATENATE"));
        assertFalse(service.executed().contains("SHOW COMPACTIONS"));
    }

    @Test
    void nonAcidOrcConcatenateRowCountMismatch() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.plainTable("default.o", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat").rowCount = 7;
        hive.rowDeltaAfterCompaction = 1;
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "o", null));
        runner.run();
        single(runner, CompactHive3Table.REL_COUNT_MISMATCH);
    }

    @Test
    void nonAcidOrcWithAcidOnlyFails() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.plainTable("default.o", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        runner.setProperty(CompactHive3Table.TABLE_TYPE_HANDLING, "ACID_ONLY");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "o", null));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("ACID_ONLY"));
        assertFalse(service.executed().stream().anyMatch(s -> s.startsWith("ALTER")));
    }

    @Test
    void nonAcidNonOrcTableFails() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.plainTable("default.txt", "org.apache.hadoop.mapred.TextInputFormat");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "txt", null));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("TextInputFormat"));
    }

    @Test
    void externalOrcTableFailsBeforeConcatenate() throws Exception {
        final TestRunner runner = runner(Version.HIVE4);
        hive.plainTable("default.ext", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat").tableType = "EXTERNAL_TABLE";
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "ext", null));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("managed"));
        assertFalse(service.executed().stream().anyMatch(s -> s.startsWith("ALTER")));
    }

    // ---- 식별자·파티션 검증 ----

    @Test
    void badIdentifiersFailWithoutRunningSql() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        runner.enqueue("", target("default", "t; DROP TABLE x", null));
        runner.enqueue("", target("de-fault", "t", null));
        runner.enqueue("", target("default", "t", "dt='a"));
        runner.enqueue("", target("default", "t", "d t='a'"));
        runner.run(4);
        runner.assertAllFlowFilesTransferred(CompactHive3Table.REL_FAILURE, 4);
        assertTrue(service.executed().isEmpty(), service.executed().toString());
    }

    @Test
    void partitionKeysMustMatchTablePartitionColumns() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.acidTable("default.p", "dt", "hour");
        hive.acidTable("default.np");
        runner.enqueue("", target("default", "p", "dt='a'"));
        runner.enqueue("", target("default", "p", "dt='a',minute='1'"));
        runner.enqueue("", target("default", "p", null));
        runner.enqueue("", target("default", "np", "dt='a'"));
        runner.run(4);
        runner.assertAllFlowFilesTransferred(CompactHive3Table.REL_FAILURE, 4);
        final List<MockFlowFile> failed = runner.getFlowFilesForRelationship(CompactHive3Table.REL_FAILURE);
        assertTrue(failed.get(0).getAttribute(CompactHive3Table.ATTR_ERROR).contains("[dt, hour]"));
        assertTrue(failed.get(2).getAttribute(CompactHive3Table.ATTR_ERROR).contains("파티션 테이블이므로"));
        assertTrue(failed.get(3).getAttribute(CompactHive3Table.ATTR_ERROR).contains("파티션 테이블이 아니므로"));
        assertFalse(service.executed().stream().anyMatch(s -> s.startsWith("ALTER")));
    }

    @Test
    void partitionKeyOrderAndEscapingAreHandled() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.acidTable("default.p", "dt", "name").rowCount = 2;
        runner.enqueue("", target("default", "p", "name='o\\'brien', DT='x'"));
        runner.run();
        final MockFlowFile flowFile = single(runner, CompactHive3Table.REL_IN_PROGRESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_PARTITION, "name='o\\'brien',dt='x'");
        assertTrue(service.executed().contains("ALTER TABLE `default`.`p` PARTITION (`name`='o\\'brien', `dt`='x') COMPACT 'major'"),
                service.executed().toString());
    }

    @Test
    void missingTableFails() throws Exception {
        final TestRunner runner = runner(Version.HIVE4);
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("default", "missing", null));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("테이블 정보를 읽지 못했다"));
    }

    @Test
    void errorAttributeFromPreviousFailureIsClearedOnSuccess() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        hive.plainTable("default.o", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        final Map<String, String> attributes = target("default", "o", null);
        attributes.put(CompactHive3Table.ATTR_ERROR, "old");
        runner.enqueue("", attributes);
        runner.run();
        single(runner, CompactHive3Table.REL_SUCCESS).assertAttributeNotExists(CompactHive3Table.ATTR_ERROR);
    }

    // ---- 설정·연결 ----

    @Test
    void invalidPartitionSpecPropertyIsInvalid() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "dt=2026");
        runner.assertNotValid();
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "dt='2026',hour='01'");
        runner.assertValid();
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "dt='${dt}'");
        runner.assertValid();
        runner.setProperty(CompactHive3Table.INITIATION_TIMEOUT, "soon");
        runner.assertNotValid();
    }

    @Test
    void connectionFailureReturnsFlowFileToQueue() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        service.setConnectionFailure(true);
        runner.enqueue("", Map.of(CompactHive3Table.ATTR_ID, "1"));
        runner.run();
        runner.assertTransferCount(CompactHive3Table.REL_FAILURE, 0);
        runner.assertTransferCount(CompactHive3Table.REL_IN_PROGRESS, 0);
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    /** 테스트용으로 바꿀 수 있는 시계. */
    static final class MutableClock extends Clock {
        private long millis;

        MutableClock(final long millis) {
            this.millis = millis;
        }

        void advance(final long delta) {
            millis += delta;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(final java.time.ZoneId zone) {
            return this;
        }

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }
}
