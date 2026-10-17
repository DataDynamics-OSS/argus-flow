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
package io.datadynamics.nifi.processors.listing.hdfs;

import io.datadynamics.nifi.processor.util.list.TriggeredListing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnJre(value = { JRE.JAVA_25 }, disabledReason = "java.security.auth.Subject.getSubject() is not supported")
class TestListHDFS {

    private ListHDFSWithMockedFileSystem processor;
    private TestRunner runner;

    @BeforeEach
    void setUp() {
        processor = new ListHDFSWithMockedFileSystem();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(ListHDFS.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");
        runner.setProperty(ListHDFS.DIRECTORY, "${hdfs.dir}");

        addFileStatus("/landing/a", "1.txt", false, 1_000L);
        addFileStatus("/landing/a", "2.txt", false, 2_000L);
        addFileStatus("/landing/a", "sub", true, 0L);
        addFileStatus("/landing/a/sub", "3.txt", false, 3_000L);
        addFileStatus("/landing/a", "4.txt._COPYING_", false, 4_000L);
        addFileStatus("/landing/b", "5.txt", false, 5_000L);
        addFileStatus("/secret", "6.txt", false, 6_000L);
    }

    private void enqueue(final String directory) {
        runner.enqueue(new byte[0], Map.of("hdfs.dir", directory, "request.id", directory));
    }

    private List<String> successFilenames() {
        return runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).stream().map(f -> f.getAttribute("filename")).sorted().toList();
    }

    @Test
    void listsDirectoryNamedByFlowFile() {
        enqueue("/landing/a");
        enqueue("/landing/b");

        runner.run(2);

        runner.assertTransferCount(TriggeredListing.REL_ORIGINAL, 2);
        assertEquals(List.of("1.txt", "2.txt", "3.txt", "5.txt"), successFilenames());
        final MockFlowFile sub = runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).stream()
                .filter(f -> "3.txt".equals(f.getAttribute("filename"))).findFirst().orElseThrow();
        sub.assertAttributeEquals("path", "/landing/a/sub");
        sub.assertAttributeEquals("request.id", "/landing/a");
        sub.assertAttributeEquals("hdfs.owner", "owner");
    }

    @Test
    void fileFilterIsEvaluatedAgainstFlowFile() {
        runner.setProperty(ListHDFS.FILE_FILTER, "${hdfs.filter}");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, "filter-mode-files-only");
        runner.enqueue(new byte[0], Map.of("hdfs.dir", "/landing/a", "hdfs.filter", "[12]\\.txt"));

        runner.run();

        assertEquals(List.of("1.txt", "2.txt"), successFilenames());
    }

    @Test
    void directoryOutsideAllowedBaseIsRejected() {
        runner.setProperty(ListHDFS.ALLOWED_BASE_DIRECTORY, "/landing");
        enqueue("/secret");
        enqueue("/landing/../secret");
        enqueue("/landingx");

        runner.run(3);

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 3);
        for (final MockFlowFile failed : runner.getFlowFilesForRelationship(TriggeredListing.REL_FAILURE)) {
            assertTrue(failed.getAttribute(TriggeredListing.ATTR_ERROR_MESSAGE).contains("outside of Allowed Base Directory"),
                    failed.getAttribute(TriggeredListing.ATTR_ERROR_MESSAGE));
        }
    }

    @Test
    void missingDirectoryRoutesFailure() {
        enqueue("/missing");

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
    }

    @Test
    void trackingTimestampsListsOnlyNewFiles() {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        enqueue("/landing/b");
        runner.run();
        assertEquals(List.of("5.txt"), successFilenames());

        addFileStatus("/landing/b", "7.txt", false, 7_000L);
        runner.clearTransferState();
        enqueue("/landing/b");
        runner.run();
        assertEquals(List.of("7.txt"), successFilenames());
    }

    @Test
    void recordWriterWritesOneChildPerInput() throws Exception {
        final MockRecordWriter writer = new MockRecordWriter("filename");
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(TriggeredListing.RECORD_WRITER, "writer");
        enqueue("/landing/a");

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst().assertAttributeEquals("record.count", "3");
    }

    private void addFileStatus(final String path, final String filename, final boolean isDirectory, final long modificationTime) {
        final Path fullPath = new Path("hdfs", "hdfscluster:8020", path);
        final Path filePath = new Path(fullPath, filename);
        final FileStatus fileStatus = new FileStatus(1L, isDirectory, 1, 1L, modificationTime, 0L, new FsPermission((short) 0x777), "owner", "group", filePath);
        processor.fileSystem.addFileStatus(fullPath, fileStatus);
    }

    private static class ListHDFSWithMockedFileSystem extends ListHDFS {
        private final MockFileSystem fileSystem = new MockFileSystem();

        @Override
        protected FileSystem getFileSystem() {
            return fileSystem;
        }

        @Override
        protected FileSystem getFileSystem(final Configuration config) {
            return fileSystem;
        }
    }
}
