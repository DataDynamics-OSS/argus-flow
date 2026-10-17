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
package io.datadynamics.nifi.processors.listing.file;

import io.datadynamics.nifi.processor.util.list.TriggeredListing;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestListFile {

    @TempDir
    Path base;

    private Path inbox;
    private TestRunner runner;

    @BeforeEach
    void setUp() throws IOException {
        inbox = Files.createDirectories(base.resolve("inbox"));
        write(inbox.resolve("a.csv"), 1_000_000L);
        write(Files.createDirectories(inbox.resolve("sub")).resolve("b.txt"), 2_000_000L);
        write(Files.createDirectories(base.resolve("other")).resolve("c.csv"), 3_000_000L);

        runner = TestRunners.newTestRunner(ListFile.class);
        runner.setProperty(ListFile.DIRECTORY, "${list.directory}");
        runner.setProperty(ListFile.ALLOWED_BASE_DIRECTORY, inbox.toString());
        runner.assertValid();
    }

    private static void write(final Path file, final long lastModified) throws IOException {
        Files.writeString(file, file.getFileName().toString());
        Files.setLastModifiedTime(file, FileTime.fromMillis(lastModified));
    }

    private void enqueue(final String directory) {
        runner.enqueue(new byte[0], Map.of("list.directory", directory, "request.id", "r1"));
    }

    @Test
    void listsDirectoryNamedByFlowFile() {
        enqueue(inbox.toString());

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 2);
        runner.assertTransferCount(TriggeredListing.REL_ORIGINAL, 1);
        final List<MockFlowFile> children = runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS);
        for (final MockFlowFile child : children) {
            child.assertAttributeEquals("request.id", "r1");
            child.assertAttributeExists("absolute.path");
            child.assertAttributeEquals(TriggeredListing.ATTR_TARGET, "Directory [" + inbox + "]");
        }
        assertEquals(List.of("a.csv", "b.txt"), children.stream().map(f -> f.getAttribute("filename")).sorted().toList());
        runner.getFlowFilesForRelationship(TriggeredListing.REL_ORIGINAL).getFirst().assertAttributeEquals(TriggeredListing.ATTR_COUNT, "2");
    }

    @Test
    void fileFilterIsEvaluatedAgainstFlowFile() {
        runner.setProperty(ListFile.FILE_FILTER, "${list.filter}");
        runner.enqueue(new byte[0], Map.of("list.directory", inbox.toString(), "list.filter", ".*\\.csv"));

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst().assertAttributeEquals("filename", "a.csv");
    }

    @Test
    void directoryOutsideAllowedBaseIsRejected() {
        enqueue(base.resolve("other").toString());
        enqueue(inbox.resolve("../other").toString());

        runner.run(2);

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 2);
        for (final MockFlowFile failed : runner.getFlowFilesForRelationship(TriggeredListing.REL_FAILURE)) {
            assertTrue(failed.getAttribute(TriggeredListing.ATTR_ERROR_MESSAGE).contains("outside of Allowed Base Directory"),
                    failed.getAttribute(TriggeredListing.ATTR_ERROR_MESSAGE));
        }
    }

    @Test
    void symbolicLinkEscapingAllowedBaseIsRejected() throws IOException {
        final Path link = inbox.resolve("escape");
        Files.createSymbolicLink(link, base.resolve("other"));
        enqueue(link.toString());

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
    }

    @Test
    void missingDirectoryRoutesFailure() {
        enqueue(inbox.resolve("missing").toString());

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
    }

    @Test
    void widenedFileFilterRelistsOlderFilesWithTrackingTimestamps() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        runner.setProperty(ListFile.FILE_FILTER, ".*\\.txt");
        enqueue(inbox.toString());
        runner.run();
        assertEquals(List.of("b.txt"), successNames());

        // a.csv(1,000,000)는 이미 기록된 최신 시각 b.txt(2,000,000)보다 오래됐다 — 같은 상태를 쓰면 영영 나오지 않는다
        runner.setProperty(ListFile.FILE_FILTER, ".*");
        runner.clearTransferState();
        enqueue(inbox.toString());
        runner.run();
        assertEquals(List.of("a.csv", "b.txt"), successNames());
    }

    @Test
    void perFlowFileFilterKeepsSeparateTrackingState() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        runner.setProperty(ListFile.FILE_FILTER, "${list.filter}");

        runner.enqueue(new byte[0], Map.of("list.directory", inbox.toString(), "list.filter", ".*\\.txt"));
        runner.run();
        assertEquals(List.of("b.txt"), successNames());

        runner.clearTransferState();
        runner.enqueue(new byte[0], Map.of("list.directory", inbox.toString(), "list.filter", ".*\\.csv"));
        runner.run();
        assertEquals(List.of("a.csv"), successNames());

        // 각 필터의 상태는 따로 유지된다 — 같은 요청을 반복하면 새 파일만
        write(inbox.resolve("e.txt"), 5_000_000L);
        runner.clearTransferState();
        runner.enqueue(new byte[0], Map.of("list.directory", inbox.toString(), "list.filter", ".*\\.txt"));
        runner.enqueue(new byte[0], Map.of("list.directory", inbox.toString(), "list.filter", ".*\\.csv"));
        runner.run(2);
        assertEquals(List.of("e.txt"), successNames());
    }

    private List<String> successNames() {
        return runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).stream().map(f -> f.getAttribute("filename")).sorted().toList();
    }

    @Test
    void trackingTimestampsListsOnlyNewFiles() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        enqueue(inbox.toString());
        runner.run();
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 2);

        write(inbox.resolve("d.csv"), 4_000_000L);
        runner.clearTransferState();
        enqueue(inbox.toString());
        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst().assertAttributeEquals("filename", "d.csv");
    }
}
