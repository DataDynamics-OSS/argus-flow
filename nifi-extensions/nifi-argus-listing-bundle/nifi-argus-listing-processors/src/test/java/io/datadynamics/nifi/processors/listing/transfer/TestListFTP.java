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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;

import java.util.Map;

class TestListFTP {

    private static final String USERNAME = "nifi-ftp-user";
    private static final String PASSWORD = "Test test test chocolate";

    private final FakeFtpServer fakeFtpServer = new FakeFtpServer();
    private TestRunner runner;

    @BeforeEach
    void setUp() {
        fakeFtpServer.setServerControlPort(0);
        fakeFtpServer.addUserAccount(new UserAccount(USERNAME, PASSWORD, "/data"));
        final FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry("/data"));
        fileSystem.add(new DirectoryEntry("/data/in"));
        fileSystem.add(new FileEntry("/data/in/found.txt", "found"));
        fileSystem.add(new DirectoryEntry("/data/other"));
        fileSystem.add(new FileEntry("/data/other/elsewhere.txt", "elsewhere"));
        fakeFtpServer.setFileSystem(fileSystem);
        fakeFtpServer.start();

        runner = TestRunners.newTestRunner(ListFTP.class);
        runner.setProperty(ListFTP.HOSTNAME, "127.0.0.1");
        runner.setProperty(FTPTransfer.PORT, Integer.toString(fakeFtpServer.getServerControlPort()));
        runner.setProperty(ListFTP.USERNAME, USERNAME);
        runner.setProperty(FTPTransfer.PASSWORD, PASSWORD);
        runner.setProperty(ListFTP.REMOTE_PATH, "${ftp.path}");
        runner.assertValid();
    }

    @AfterEach
    void tearDown() {
        fakeFtpServer.stop();
    }

    @Test
    void listsRemoteDirectoryNamedByFlowFile() {
        runner.enqueue(new byte[0], Map.of("ftp.path", "/data/in"));

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        final MockFlowFile child = runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst();
        child.assertAttributeEquals("filename", "found.txt");
        child.assertAttributeEquals("ftp.remote.host", "127.0.0.1");
        runner.getFlowFilesForRelationship(TriggeredListing.REL_ORIGINAL).getFirst().assertAttributeEquals(TriggeredListing.ATTR_COUNT, "1");
    }
}
