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
package io.datadynamics.nifi.processors.listing.transfer;

import io.datadynamics.nifi.processor.util.list.TriggeredListing;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestListSFTP {

    private SSHTestServer sshServer;
    private TestRunner runner;

    @BeforeEach
    void startServer(@TempDir final Path rootPath) throws Exception {
        Files.writeString(Files.createDirectories(rootPath.resolve("in")).resolve("x.txt"), "x");
        Files.writeString(rootPath.resolve("in").resolve("y.txt"), "y");
        Files.writeString(Files.createDirectories(rootPath.resolve("other")).resolve("z.txt"), "z");

        sshServer = new SSHTestServer(rootPath);
        sshServer.startServer();

        runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(SFTPTransfer.HOSTNAME, "${sftp.host}");
        runner.setProperty(SFTPTransfer.PORT, "${sftp.port}");
        runner.setProperty(SFTPTransfer.USERNAME, sshServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshServer.getPassword());
        runner.setProperty(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT, Boolean.FALSE.toString());
        runner.setProperty(ListSFTP.REMOTE_PATH, "${sftp.path}");
        runner.assertValid();
    }

    @AfterEach
    void stopServer() throws Exception {
        sshServer.stopServer();
    }

    private void enqueue(final String path, final Map<String, String> extra) {
        final Map<String, String> attributes = new HashMap<>(Map.of(
                "sftp.host", sshServer.getHost(),
                "sftp.port", Integer.toString(sshServer.getSSHPort()),
                "sftp.path", path));
        attributes.putAll(extra);
        runner.enqueue(new byte[0], attributes);
    }

    @Test
    void listsRemoteDirectoryNamedByFlowFile() {
        enqueue("/in", Map.of("request.id", "r1"));
        enqueue("/other", Map.of("request.id", "r2"));

        runner.run(2);

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 3);
        runner.assertTransferCount(TriggeredListing.REL_ORIGINAL, 2);
        final List<MockFlowFile> children = runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS);
        for (final MockFlowFile child : children) {
            child.assertAttributeEquals("sftp.remote.host", sshServer.getHost());
            child.assertAttributeEquals("sftp.listing.user", sshServer.getUsername());
        }
        assertEquals(List.of("x.txt", "y.txt"), children.stream().filter(f -> "r1".equals(f.getAttribute("request.id")))
                .map(f -> f.getAttribute("filename")).sorted().toList());
        assertEquals(List.of("z.txt"), children.stream().filter(f -> "r2".equals(f.getAttribute("request.id")))
                .map(f -> f.getAttribute("filename")).toList());
    }

    @Test
    void hostOutsideAllowedHostsIsRejectedBeforeConnecting() {
        runner.setProperty(ListSFTP.ALLOWED_HOSTS, "sftp\\.example\\.com");
        enqueue("/in", Map.of());

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
    }

    @Test
    void connectionFailureRoutesFailure() {
        runner.enqueue(new byte[0], Map.of("sftp.host", sshServer.getHost(), "sftp.port", "1", "sftp.path", "/in"));

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
    }

    @Test
    void recordWriterWritesOneChildPerInput() throws Exception {
        final MockRecordWriter writer = new MockRecordWriter("filename");
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(TriggeredListing.RECORD_WRITER, "writer");
        enqueue("/in", Map.of());

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst().assertAttributeEquals("record.count", "2");
    }
}
