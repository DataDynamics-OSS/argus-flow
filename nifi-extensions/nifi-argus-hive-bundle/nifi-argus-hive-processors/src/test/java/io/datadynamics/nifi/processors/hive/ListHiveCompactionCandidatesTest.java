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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 가짜 Hive3DBCPService + @TempDir 의 실제 base·delta 디렉토리로 후보 탐색을 확인한다.
 */
class ListHiveCompactionCandidatesTest {

    private static final long NOW = 1_792_000_000_000L;
    private static final long OLD = NOW - 3 * 3600_000L;

    @TempDir
    Path warehouse;

    private FakeHive hive;
    private ScriptedHive3DBCPService service;

    private TestRunner runner(final Version version) throws Exception {
        hive = new FakeHive(version);
        final ListHiveCompactionCandidates processor = new ListHiveCompactionCandidates();
        processor.setClock(new CompactHive3TableTest.MutableClock(NOW));
        final TestRunner runner = TestRunners.newTestRunner(processor);
        service = new ScriptedHive3DBCPService();
        service.setHandler(sql -> hive.handle(sql));
        runner.addControllerService("hive", service);
        runner.enableControllerService(service);
        runner.setProperty(ListHiveCompactionCandidates.HIVE_DBCP_SERVICE, "hive");
        runner.setProperty(ListHiveCompactionCandidates.DATABASE, "sales");
        runner.setProperty(ListHiveCompactionCandidates.TABLE, "events");
        runner.setIncomingConnection(false);
        return runner;
    }

    private void file(final Path partition, final String relative, final int bytes) throws IOException {
        final Path path = partition.resolve(relative);
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[bytes]);
    }

    private static void age(final Path root, final long millis) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            for (final Path path : paths.collect(Collectors.toList())) {
                assertTrue(path.toFile().setLastModified(millis));
            }
        }
    }

    /** dt=a: base 없음 delta 3 → MAJOR, dt=b: 큰 base + 작은 delta 1 → 후보 아님, dt=c: base + 작은 delta 12 → MINOR,
     *  dt=d: 진행 중, dt=e: 방금 수정, dt=f: 옛 delta 만 남음(합쳐짐). */
    private FakeHive.Table acidLayout() throws IOException {
        final FakeHive.Table table = hive.acidTable("sales.events", "dt");
        final Path root = warehouse.resolve("events");
        for (int i = 1; i <= 3; i++) {
            file(root.resolve("dt=a"), String.format("delta_%07d_%07d_0000/bucket_00000", i, i), 10);
        }
        file(root.resolve("dt=b"), "base_0000005/bucket_00000", 10_000);
        file(root.resolve("dt=b"), "delta_0000006_0000006_0000/bucket_00000", 10);
        file(root.resolve("dt=c"), "base_0000005/bucket_00000", 10_000);
        for (int i = 6; i < 18; i++) {
            file(root.resolve("dt=c"), String.format("delta_%07d_%07d_0000/bucket_00000", i, i), 5);
        }
        file(root.resolve("dt=d"), "delta_0000001_0000001_0000/bucket_00000", 10);
        file(root.resolve("dt=e"), "delta_0000001_0000001_0000/bucket_00000", 10);
        file(root.resolve("dt=f"), "base_0000003/bucket_00000", 100);
        file(root.resolve("dt=f"), "delta_0000001_0000001_0000/bucket_00000", 100);
        file(root.resolve("dt=f"), "delta_0000002_0000003/bucket_00000", 100);
        age(root, OLD);
        age(root.resolve("dt=e"), NOW - 60_000);
        for (final String p : List.of("a", "b", "c", "d", "e", "f")) {
            table.partitions.put("dt=" + p, root.resolve("dt=" + p).toUri().toString());
        }
        hive.addCompaction("sales", "events", "dt=d", "MAJOR", "working");
        hive.addCompaction("sales", "events", "dt=b", "MAJOR", "succeeded");
        return table;
    }

    @Test
    void findsAcidCandidatesSortedByDeltaCountAndExcludesRunningAndFresh() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        acidLayout();
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHiveCompactionCandidates.REL_SUCCESS, 2);
        final List<MockFlowFile> candidates = runner.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS);
        final MockFlowFile c = candidates.get(0);
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_DATABASE, "sales");
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_TABLE, "events");
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, "dt='c'");
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_RECOMMENDED_TYPE, "MINOR");
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_DELTA_COUNT, "12");
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_DELETE_DELTA_COUNT, "0");
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_BASE_BYTES, "10000");
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_DELTA_BYTES, "60");
        c.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_FILE_COUNT, "13");
        assertTrue(c.getAttribute(ListHiveCompactionCandidates.ATTR_PARTITION_LOCATION).endsWith("events/dt=c"));
        c.assertContentEquals("");
        final MockFlowFile a = candidates.get(1);
        a.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, "dt='a'");
        a.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_RECOMMENDED_TYPE, "MAJOR");
        a.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_DELTA_COUNT, "3");
        a.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_BASE_BYTES, "0");
        assertTrue(service.executed().contains("SHOW PARTITIONS `sales`.`events`"));
        assertEquals(2, runner.getProvenanceEvents().size());
    }

    @Test
    void filterAgeAndMaximumCandidates() throws Exception {
        final TestRunner runner = runner(Version.HIVE4);
        acidLayout();
        runner.setProperty(ListHiveCompactionCandidates.MAXIMUM_CANDIDATES, "1");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHiveCompactionCandidates.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).get(0)
                .assertAttributeEquals(ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, "dt='c'");

        runner.clearTransferState();
        runner.setProperty(ListHiveCompactionCandidates.MAXIMUM_CANDIDATES, "100");
        runner.setProperty(ListHiveCompactionCandidates.PARTITION_FILTER, "dt='a'");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHiveCompactionCandidates.REL_SUCCESS, 1);
        assertTrue(service.executed().contains("SHOW PARTITIONS `sales`.`events` PARTITION (`dt`='a')"));

        runner.clearTransferState();
        runner.removeProperty(ListHiveCompactionCandidates.PARTITION_FILTER);
        runner.setProperty(ListHiveCompactionCandidates.MINIMUM_PARTITION_AGE, "30 sec");
        runner.run();
        // dt=e(1분 전 수정)도 후보: base 없음 + delta 1 → MAJOR
        assertEquals(3, runner.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).size());

        runner.clearTransferState();
        runner.setProperty(ListHiveCompactionCandidates.MINIMUM_PARTITION_AGE, "1 hour");
        runner.setProperty(ListHiveCompactionCandidates.DELTA_COUNT_THRESHOLD, "20");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHiveCompactionCandidates.REL_SUCCESS, 1);
    }

    @Test
    void inputFlowFileDrivesTargetAndGoesToOriginal() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        acidLayout();
        runner.setProperty(ListHiveCompactionCandidates.DATABASE, "${db}");
        runner.setProperty(ListHiveCompactionCandidates.TABLE, "${t}");
        runner.setIncomingConnection(true);
        runner.enqueue("trigger", Map.of("db", "sales", "t", "events", "run", "1"));
        runner.run();
        runner.assertTransferCount(ListHiveCompactionCandidates.REL_ORIGINAL, 1);
        runner.assertTransferCount(ListHiveCompactionCandidates.REL_SUCCESS, 2);
        final MockFlowFile child = runner.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).get(0);
        child.assertAttributeEquals("run", "1");
        child.assertContentEquals("");
    }

    @Test
    void failuresGoToFailureWithInputOrYieldWithout() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        acidLayout();
        runner.setProperty(ListHiveCompactionCandidates.TABLE, "${t}");
        runner.setIncomingConnection(true);
        runner.enqueue("", Map.of("t", "missing"));
        runner.enqueue("", Map.of("t", "bad-name"));
        runner.run(2);
        runner.assertAllFlowFilesTransferred(ListHiveCompactionCandidates.REL_FAILURE, 2);
        final List<MockFlowFile> failed = runner.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_FAILURE);
        assertTrue(failed.get(0).getAttribute(ListHiveCompactionCandidates.ATTR_ERROR).contains("Table not found"));
        assertTrue(failed.get(1).getAttribute(ListHiveCompactionCandidates.ATTR_ERROR).contains("[A-Za-z0-9_]+"));

        runner.clearTransferState();
        runner.enqueue("", Map.of("t", "events"));
        runner.setProperty(ListHiveCompactionCandidates.PARTITION_FILTER, "hour='01'");
        runner.run();
        assertTrue(runner.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_FAILURE).get(0)
                .getAttribute(ListHiveCompactionCandidates.ATTR_ERROR).contains("파티션 컬럼"));

        // 입력 없이 실패하면 아무것도 내보내지 않는다
        final TestRunner noInput = runner(Version.HIVE3);
        noInput.setProperty(ListHiveCompactionCandidates.TABLE, "missing");
        noInput.run();
        assertEquals(0, noInput.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).size()
                + noInput.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_FAILURE).size());
    }

    @Test
    void nonAcidAndNonPartitionedTables() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        final Path orc = warehouse.resolve("orc");
        final Path text = warehouse.resolve("txt");
        for (int i = 0; i < 12; i++) {
            file(orc, "dt=a/00000" + i + "_0", 100);
            file(text, String.format("%06d_0", i), 100);
        }
        file(orc, "dt=b/000000_0", 100);
        age(warehouse, OLD);
        final FakeHive.Table orcTable = hive.plainTable("sales.orc", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "dt");
        orcTable.partitions.put("dt=a", orc.resolve("dt=a").toUri().toString());
        orcTable.partitions.put("dt=b", orc.resolve("dt=b").toUri().toString());
        final FakeHive.Table textTable = hive.plainTable("sales.txt", "org.apache.hadoop.mapred.TextInputFormat");
        textTable.location = text.toUri().toString();
        textTable.tableType = "EXTERNAL_TABLE";

        runner.setProperty(ListHiveCompactionCandidates.TABLE, "orc");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHiveCompactionCandidates.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_RECOMMENDED_TYPE, "CONCATENATE");
        flowFile.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_FILE_COUNT, "12");
        flowFile.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, "dt='a'");
        // Non-ACID 는 SHOW COMPACTIONS 를 보지 않는다
        assertTrue(service.executed().stream().noneMatch(s -> s.equals("SHOW COMPACTIONS")));

        runner.clearTransferState();
        runner.setProperty(ListHiveCompactionCandidates.TABLE, "txt");
        runner.run();
        flowFile = runner.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_RECOMMENDED_TYPE, "REWRITE");
        flowFile.assertAttributeEquals(ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, "");
    }

    @Test
    void candidateAttributesFeedCompactHive3TableDirectly() throws Exception {
        final TestRunner lister = runner(Version.HIVE4);
        acidLayout();
        lister.setProperty(ListHiveCompactionCandidates.PARTITION_FILTER, "dt='a'");
        lister.run();
        final MockFlowFile candidate = lister.getFlowFilesForRelationship(ListHiveCompactionCandidates.REL_SUCCESS).get(0);

        final CompactHive3Table processor = new CompactHive3Table();
        final TestRunner compact = TestRunners.newTestRunner(processor);
        compact.addControllerService("hive", service);
        compact.enableControllerService(service);
        compact.setProperty(CompactHive3Table.HIVE_DBCP_SERVICE, "hive");
        compact.setProperty(CompactHive3Table.DATABASE, "${hive.compaction.database}");
        compact.setProperty(CompactHive3Table.TABLE, "${hive.compaction.table}");
        compact.setProperty(CompactHive3Table.PARTITION_SPEC, "${hive.compaction.partition.spec}");
        compact.enqueue("", candidate.getAttributes());
        compact.run();
        compact.assertAllFlowFilesTransferred(CompactHive3Table.REL_IN_PROGRESS, 1);
        final MockFlowFile tracked = compact.getFlowFilesForRelationship(CompactHive3Table.REL_IN_PROGRESS).get(0);
        tracked.assertAttributeEquals(CompactHive3Table.ATTR_TABLE, "sales.events");
        tracked.assertAttributeEquals(CompactHive3Table.ATTR_PARTITION, "dt='a'");
        assertTrue(service.executed().contains("ALTER TABLE `sales`.`events` PARTITION (`dt`='a') COMPACT 'major'"));
    }

    @Test
    void validatesProperties() throws Exception {
        final TestRunner runner = runner(Version.HIVE3);
        runner.assertValid();
        runner.setProperty(ListHiveCompactionCandidates.PARTITION_FILTER, "dt=2026");
        runner.assertNotValid();
        runner.setProperty(ListHiveCompactionCandidates.PARTITION_FILTER, "dt='${dt}'");
        runner.assertValid();
        runner.setProperty(ListHiveCompactionCandidates.DELTA_SIZE_RATIO_THRESHOLD, "x");
        runner.assertNotValid();
    }
}
