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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CompactHive3Table 의 REBALANCE·REWRITE (P2).
 */
class CompactHive3TableRewriteTest {

    private final CompactHive3TableTest.MutableClock clock = new CompactHive3TableTest.MutableClock(1_792_000_000_000L);
    private FakeHive hive;
    private ScriptedHive3DBCPService service;

    @TempDir
    Path tempDir;

    private TestRunner runner(final Version version, final String type) throws Exception {
        hive = new FakeHive(version);
        final CompactHive3Table processor = new CompactHive3Table();
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
        runner.setProperty(CompactHive3Table.COMPACTION_TYPE, type);
        return runner;
    }

    private static Map<String, String> target(final String table, final String spec) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("db", "default");
        attributes.put("table", table);
        if (spec != null) {
            attributes.put("spec", spec);
        }
        return attributes;
    }

    private FakeHive.Table acidTable(final String name) {
        final FakeHive.Table table = hive.acidTable("default." + name, "dt");
        table.columns.clear();
        table.columns.addAll(List.of("id", "name", "amount"));
        table.partitions.put("dt=a", "file:/nowhere/" + name + "/dt=a");
        table.rowCount = 10;
        return table;
    }

    private static MockFlowFile single(final TestRunner runner, final org.apache.nifi.processor.Relationship relationship) {
        runner.assertAllFlowFilesTransferred(relationship, 1);
        return runner.getFlowFilesForRelationship(relationship).get(0);
    }

    // ---- REWRITE ----

    @Test
    void rewriteWithFixedFileCountRunsInsertOverwriteAndRestoresSession() throws Exception {
        final TestRunner runner = runner(Version.HIVE3, "REWRITE");
        acidTable("t");
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "4");
        runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "ID");
        runner.enqueue("", target("t", "dt='a'"));
        runner.run();
        final MockFlowFile flowFile = single(runner, CompactHive3Table.REL_SUCCESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "REWRITE");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_STATE, "succeeded");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TARGET_FILE_COUNT, "4");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_BEFORE, "10");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "10");
        flowFile.assertAttributeNotExists(CompactHive3Table.ATTR_ID);
        flowFile.assertAttributeNotExists(CompactHive3Table.ATTR_PARTITION_SIZE_BYTES);

        final List<String> executed = service.executed();
        assertTrue(executed.contains("INSERT OVERWRITE TABLE `default`.`t` PARTITION (`dt`='a') SELECT `id`, `name`, `amount` FROM `default`.`t` "
                + "WHERE `dt` = 'a' DISTRIBUTE BY `id`"), executed.toString());
        assertFalse(executed.stream().anyMatch(s -> s.startsWith("SHOW COMPACTIONS")));
        // INSERT OVERWRITE 때 세션
        assertEquals("4", hive.rewriteReducers);
        assertEquals("false", hive.rewriteSession.get("hive.tez.auto.reducer.parallelism"));
        assertEquals("false", hive.rewriteSession.get("hive.merge.mapfiles"));
        assertEquals("false", hive.rewriteSession.get("hive.compute.query.using.stats"));
        // 끝나면 원래 값
        assertEquals(hive.sessionDefaults, hive.session);
        // 읽기 → 바꾸기 순서
        final int read = executed.indexOf("SET mapreduce.job.reduces");
        final int set = executed.indexOf("SET mapreduce.job.reduces=4");
        final int restore = executed.indexOf("SET mapreduce.job.reduces=-1");
        assertTrue(read >= 0 && read < set && set < restore, executed.toString());
        assertTrue(executed.indexOf("SET hive.merge.mapfiles=true") > executed.indexOf(executed.stream().filter(s -> s.startsWith("INSERT OVERWRITE")).findFirst().orElseThrow()));
    }

    @Test
    void rewriteFailureStillRestoresSession() throws Exception {
        final TestRunner runner = runner(Version.HIVE4, "REWRITE");
        acidTable("t");
        hive.rewriteError = Result.error("FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.tez.TezTask", 2);
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "2");
        runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "id");
        runner.enqueue("", target("t", "dt='a'"));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("TezTask"));
        assertEquals(hive.sessionDefaults, hive.session);
    }

    @Test
    void rewriteRejectsFunctionsUnknownAndPartitionColumnsAndBucketedTables() throws Exception {
        final TestRunner runner = runner(Version.HIVE3, "REWRITE");
        acidTable("t");
        hive.acidTable("default.b").numBuckets = 4;
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "2");
        runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "${keys}");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "${spec:replaceNull('')}");
        final String[][] cases = {{"t", "dt='a'", "rand()", "rand()"}, {"t", "dt='a'", "nope", "데이터 컬럼"}, {"t", "dt='a'", "dt", "데이터 컬럼"},
                {"b", null, "id", "버킷"}};
        for (final String[] c : cases) {
            final Map<String, String> attributes = target(c[0], c[1]);
            attributes.put("keys", c[2]);
            runner.enqueue("", attributes);
        }
        runner.run(cases.length);
        final List<MockFlowFile> failed = runner.getFlowFilesForRelationship(CompactHive3Table.REL_FAILURE);
        assertEquals(cases.length, failed.size());
        for (int i = 0; i < cases.length; i++) {
            final String error = failed.get(i).getAttribute(CompactHive3Table.ATTR_ERROR);
            assertTrue(error.contains(cases[i][3]), cases[i][2] + " -> " + error);
        }
        assertFalse(service.executed().stream().anyMatch(s -> s.startsWith("INSERT")));
    }

    @Test
    void rewriteOfNonAcidTextTableIsAllowedButNotWithAcidOnly() throws Exception {
        final TestRunner runner = runner(Version.HIVE3, "REWRITE");
        final FakeHive.Table text = hive.plainTable("default.txt", "org.apache.hadoop.mapred.TextInputFormat");
        text.tableType = "EXTERNAL_TABLE";
        text.rowCount = 3;
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "1");
        runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "id");
        runner.enqueue("", target("txt", null));
        runner.run();
        single(runner, CompactHive3Table.REL_SUCCESS).assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "REWRITE");

        runner.clearTransferState();
        runner.setProperty(CompactHive3Table.TABLE_TYPE_HANDLING, "ACID_ONLY");
        runner.enqueue("", target("txt", null));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("ACID_ONLY"));
    }

    @Test
    void fileCountFromTableStatistics() throws Exception {
        final TestRunner runner = runner(Version.HIVE3, "REWRITE");
        acidTable("t").totalSize = 1500;
        runner.setProperty(CompactHive3Table.TARGET_FILE_SIZE, "1 KB");
        runner.setProperty(CompactHive3Table.PARTITION_SIZE_SOURCE, "TABLE_STATISTICS");
        runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "id");
        runner.enqueue("", target("t", "dt='a'"));
        runner.run();
        final MockFlowFile flowFile = single(runner, CompactHive3Table.REL_SUCCESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_PARTITION_SIZE_BYTES, "1500");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TARGET_FILE_COUNT, "2");
        assertEquals("2", hive.rewriteReducers);
        assertTrue(service.executed().contains("DESCRIBE FORMATTED `default`.`t` PARTITION (`dt`='a')"));

        runner.clearTransferState();
        hive.tables.get("default.t").totalSize = -1;
        runner.enqueue("", target("t", "dt='a'"));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("totalSize"));
    }

    @Test
    void fileCountFromHdfsCountsCurrentAcidDirectories() throws Exception {
        final TestRunner runner = runner(Version.HIVE4, "REWRITE");
        final Path partition = Files.createDirectories(tempDir.resolve("t/dt=a"));
        Files.createDirectories(partition.resolve("base_0000002"));
        Files.write(partition.resolve("base_0000002/bucket_00000"), new byte[3000]);
        Files.createDirectories(partition.resolve("delta_0000001_0000001_0000"));
        Files.write(partition.resolve("delta_0000001_0000001_0000/bucket_00000_0"), new byte[5000]);
        Files.createDirectories(partition.resolve("delta_0000003_0000003_0000"));
        Files.write(partition.resolve("delta_0000003_0000003_0000/bucket_00000_0"), new byte[1000]);
        acidTable("t").partitions.put("dt=a", partition.toUri().toString());
        runner.setProperty(CompactHive3Table.TARGET_FILE_SIZE, "1 KB");
        runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "id");
        runner.enqueue("", target("t", "dt='a'"));
        runner.run();
        final MockFlowFile flowFile = single(runner, CompactHive3Table.REL_SUCCESS);
        // 옛 delta_1(base_2 에 합쳐짐)는 빼고 base 3000 + delta_3 1000
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_PARTITION_SIZE_BYTES, "4000");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TARGET_FILE_COUNT, "4");
    }

    @Test
    void rewritePropertiesAreValidatedOnlyForRewrite() throws Exception {
        final TestRunner runner = runner(Version.HIVE3, "REWRITE");
        runner.assertNotValid();
        runner.setProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS, "id");
        runner.assertValid();
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "0");
        runner.assertNotValid();
        runner.removeProperty(CompactHive3Table.TARGET_FILE_COUNT);
        runner.setProperty(CompactHive3Table.COMPACTION_TYPE, "MAJOR");
        runner.removeProperty(CompactHive3Table.DISTRIBUTE_BY_COLUMNS);
        runner.assertValid();
    }

    // ---- REBALANCE ----

    @Test
    void rebalanceOnHive4UsesInProgressLoop() throws Exception {
        final TestRunner runner = runner(Version.HIVE4, "REBALANCE");
        acidTable("t");
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "3");
        runner.enqueue("", target("t", "dt='a'"));
        runner.run();
        MockFlowFile flowFile = single(runner, CompactHive3Table.REL_IN_PROGRESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TYPE, "REBALANCE");
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_TARGET_FILE_COUNT, "3");
        assertTrue(service.executed().contains("SELECT version()"));
        assertTrue(service.executed().contains("ALTER TABLE `default`.`t` PARTITION (`dt`='a') COMPACT 'rebalance' CLUSTERED INTO 3 BUCKETS"));
        assertEquals("REBALANCE", hive.compaction(1).type);

        hive.compaction(1).state = "ready for cleaning";
        runner.clearTransferState();
        runner.enqueue("", flowFile.getAttributes());
        runner.run();
        flowFile = single(runner, CompactHive3Table.REL_SUCCESS);
        flowFile.assertAttributeEquals(CompactHive3Table.ATTR_ROWCOUNT_AFTER, "10");
    }

    @Test
    void rebalanceFailedStateMentionsQueryBasedCompaction() throws Exception {
        final TestRunner runner = runner(Version.HIVE4, "REBALANCE");
        acidTable("t");
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "2");
        runner.enqueue("", target("t", "dt='a'"));
        runner.run();
        final MockFlowFile inProgress = single(runner, CompactHive3Table.REL_IN_PROGRESS);
        hive.compaction(1).state = "failed";
        hive.compaction(1).errorMessage = "Rebalancing compaction is only supported in Tez, and via Query based compaction.";
        runner.clearTransferState();
        runner.enqueue("", inProgress.getAttributes());
        runner.run();
        final String error = single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR);
        assertTrue(error.contains("hive.compactor.crud.query.based") && error.contains("Query based"), error);
    }

    @Test
    void rebalanceRejectsHive3InsertOnlyAndBucketed() throws Exception {
        TestRunner runner = runner(Version.HIVE3, "REBALANCE");
        acidTable("t");
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "2");
        runner.enqueue("", target("t", "dt='a'"));
        runner.run();
        assertTrue(single(runner, CompactHive3Table.REL_FAILURE).getAttribute(CompactHive3Table.ATTR_ERROR).contains("Hive 4"));
        assertFalse(service.executed().stream().anyMatch(s -> s.contains("COMPACT")));

        runner = runner(Version.HIVE4, "REBALANCE");
        hive.acidTable("default.io").properties.put("transactional_properties", "insert_only");
        hive.acidTable("default.b").numBuckets = 2;
        hive.plainTable("default.o", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        runner.setProperty(CompactHive3Table.TARGET_FILE_COUNT, "2");
        runner.setProperty(CompactHive3Table.PARTITION_SPEC, "");
        runner.enqueue("", target("io", null));
        runner.enqueue("", target("b", null));
        runner.enqueue("", target("o", null));
        runner.run(3);
        final List<MockFlowFile> failed = runner.getFlowFilesForRelationship(CompactHive3Table.REL_FAILURE);
        assertEquals(3, failed.size());
        assertTrue(failed.get(0).getAttribute(CompactHive3Table.ATTR_ERROR).contains("INSERT_ONLY_ACID"));
        assertTrue(failed.get(1).getAttribute(CompactHive3Table.ATTR_ERROR).contains("버킷"));
        assertTrue(failed.get(2).getAttribute(CompactHive3Table.ATTR_ERROR).contains("NON_ACID_ORC"));
    }

    @Test
    void parsesHiveMajorVersion() {
        assertEquals(4, CompactHive3Table.majorVersion("4.0.1 r3af4517eb8cfd9407ad34ed78a0b48b57dfaa264"));
        assertEquals(3, CompactHive3Table.majorVersion("3.1.3 r4df4d75bf1e16fe0af75aad0b4179c34c07fc975"));
        assertEquals(3, CompactHive3Table.majorVersion("3.1.3000.7.1.9.0-387 rUnknown"));
        assertEquals(-1, CompactHive3Table.majorVersion("unknown"));
        assertEquals(-1, CompactHive3Table.majorVersion(null));
    }
}
