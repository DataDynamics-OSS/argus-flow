/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle, design/listing-processors-port.md):
 *   nifi-extension-bundles/nifi-hadoop-bundle/nifi-hdfs-processors/src/main/java/org/apache/nifi/processors/hadoop/util/writer/FlowFileHadoopFileStatusWriter.java
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
package io.datadynamics.nifi.processors.listing.hdfs.util.writer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlowFileHadoopFileStatusWriter extends HadoopFileStatusWriter {
    public FlowFileHadoopFileStatusWriter(final ProcessSession session,
                                          final FlowFile parent,
                                          final List<FlowFile> children,
                                          final RecordSetWriterFactory writerFactory,
                                          final String hdfsPrefix,
                                          final ComponentLog logger) {
        super(session, parent, children, writerFactory, hdfsPrefix, logger);
    }

    @Override
    public void write(final List<FileStatus> statuses) {
        for (FileStatus status : statuses) {
            final Map<String, String> attributes = createAttributes(status);
            FlowFile flowFile = session.create(parent);
            children.add(flowFile);
            flowFile = session.putAllAttributes(flowFile, attributes);
            children.set(children.size() - 1, flowFile);
        }
    }

    private Map<String, String> createAttributes(final FileStatus status) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), status.getPath().getName());
        attributes.put(CoreAttributes.PATH.key(), getAbsolutePath(status.getPath().getParent()));
        attributes.put(hdfsPrefix + ".owner", status.getOwner());
        attributes.put(hdfsPrefix + ".group", status.getGroup());
        attributes.put(hdfsPrefix + ".lastModified", String.valueOf(status.getModificationTime()));
        attributes.put(hdfsPrefix + ".length", String.valueOf(status.getLen()));
        attributes.put(hdfsPrefix + ".replication", String.valueOf(status.getReplication()));
        attributes.put(hdfsPrefix + ".permissions", getPermissionsString(status.getPermission()));
        return attributes;
    }
}
