/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) as a test fixture for nifi-argus-listing-bundle:
 *   nifi-extension-bundles/nifi-hadoop-bundle/nifi-hdfs-processors/src/test/java/org/apache/nifi/processors/hadoop/TestListHDFS.java (MockFileSystem)
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

class MockFileSystem extends FileSystem {
    private final Map<Path, Set<FileStatus>> fileStatuses = new HashMap<>();

    public void addFileStatus(final Path parent, final FileStatus child) {
        final Set<FileStatus> children = fileStatuses.computeIfAbsent(parent, k -> new HashSet<>());

        children.add(child);

        // if the child is directory and a key for it does not exist, create it with an empty set of children
        if (child.isDirectory() && !fileStatuses.containsKey(child.getPath())) {
            fileStatuses.put(child.getPath(), new HashSet<>());
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public long getDefaultBlockSize() {
        return 1024L;
    }

    @Override
    @SuppressWarnings("deprecation")
    public short getDefaultReplication() {
        return 1;
    }

    @Override
    public URI getUri() {
        return null;
    }

    @Override
    public FSDataInputStream open(final Path f, final int bufferSize) {
        return null;
    }

    @Override
    public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication,
                                     final long blockSize, final Progressable progress) {
        return null;
    }

    @Override
    public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) {
        return null;
    }

    @Override
    public boolean rename(final Path src, final Path dst) {
        return false;
    }

    @Override
    public boolean delete(final Path f, final boolean recursive) {
        return false;
    }

    @Override
    public FileStatus[] listStatus(final Path f) throws IOException {
        return fileStatuses.keySet().stream()
                // find the key in fileStatuses that matches the given Path f
                .filter(pathKey -> f.isAbsoluteAndSchemeAuthorityNull()
                        // f is an absolute path with no scheme and no authority, compare with the keys of fileStatuses without their scheme and authority
                        ? Path.getPathWithoutSchemeAndAuthority(pathKey).equals(Path.getPathWithoutSchemeAndAuthority(f)) :
                        // f is absolute, but contains a scheme or authority, compare directly to the keys of fileStatuses
                        // if f is not absolute, false will be returned;
                        f.isAbsolute() && pathKey.equals(f))
                // get the set of FileStatus objects for the filtered paths in the stream
                .map(fileStatuses::get)
                // return the first set of FileStatus objects in the stream; there should only be one, since fileStatuses is a Map
                .findFirst()

                // if no set of FileStatus objects was found, throw a FNFE
                .orElseThrow(() -> new FileNotFoundException(String.format("%s instance does not contain an key for %s", this.getClass().getSimpleName(), f))).toArray(new FileStatus[0]);
    }

    @Override
    public void setWorkingDirectory(final Path newDir) {

    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(new File(".").getAbsolutePath());
    }

    @Override
    public boolean mkdirs(final Path f, final FsPermission permission) {
        return false;
    }

    @Override
    public FileStatus getFileStatus(final Path path) {
        final Optional<FileStatus> fileStatus = fileStatuses.values().stream()
                .flatMap(Set::stream)
                .filter(fs -> fs.getPath().equals(path))
                .findFirst();
        if (fileStatus.isEmpty()) {
            throw new IllegalArgumentException("Could not find FileStatus");
        }
        return fileStatus.get();
    }
}
