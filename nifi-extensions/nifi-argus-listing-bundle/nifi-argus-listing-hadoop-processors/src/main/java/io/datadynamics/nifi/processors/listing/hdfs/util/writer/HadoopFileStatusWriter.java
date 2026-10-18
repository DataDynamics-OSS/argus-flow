/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle):
 *   nifi-extension-bundles/nifi-hadoop-bundle/nifi-hdfs-processors/src/main/java/org/apache/nifi/processors/hadoop/util/writer/HadoopFileStatusWriter.java
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.IOException;
import java.util.List;

/**
 * Interface for common management of writing to records and to FlowFiles.
 *
 * <p>ARGUS: 업스트림은 FileStatusIterable 을 돌면서 이전 상태와 비교해 List 대상을 고르고(determineListable) 바로
 * success 로 전송했다. 트리거형에서는 대상 선별·추적을 TriggeredListingSupport 가 하므로, 여기서는 이미 선별된
 * FileStatus 를 입력 FlowFile 의 자식으로 쓰기만 하고 전송하지 않는다.</p>
 */
public abstract class HadoopFileStatusWriter {

    protected final ProcessSession session;
    protected final FlowFile parent;
    protected final List<FlowFile> children;
    protected final RecordSetWriterFactory writerFactory;
    protected final String hdfsPrefix;
    protected final ComponentLog logger;

    public HadoopFileStatusWriter(final ProcessSession session,
                                  final FlowFile parent,
                                  final List<FlowFile> children,
                                  final RecordSetWriterFactory writerFactory,
                                  final String hdfsPrefix,
                                  final ComponentLog logger) {
        this.session = session;
        this.parent = parent;
        this.children = children;
        this.writerFactory = writerFactory;
        this.hdfsPrefix = hdfsPrefix;
        this.logger = logger;
    }

    /**
     * 선별된 FileStatus 를 쓴다. 만든 자식 FlowFile 은 {@link #children} 에 최신 버전으로 넣는다.
     */
    public abstract void write(List<FileStatus> statuses) throws IOException, SchemaNotFoundException;

    String getAbsolutePath(final Path path) {
        final Path parent = path.getParent();
        final String prefix = (parent == null || parent.getName().equals("")) ? "" : getAbsolutePath(parent);
        return prefix + "/" + path.getName();
    }

    String getPerms(final FsAction action) {
        final StringBuilder sb = new StringBuilder();

        sb.append(action.implies(FsAction.READ) ? "r" : "-");
        sb.append(action.implies(FsAction.WRITE) ? "w" : "-");
        sb.append(action.implies(FsAction.EXECUTE) ? "x" : "-");

        return sb.toString();
    }

    String getPermissionsString(final FsPermission permission) {
        return String.format("%s%s%s", getPerms(permission.getUserAction()),
                getPerms(permission.getGroupAction()), getPerms(permission.getOtherAction()));
    }

    public static HadoopFileStatusWriter.Builder builder() {
        return new HadoopFileStatusWriter.Builder();
    }

    public static class Builder {
        private ProcessSession session;
        private FlowFile parent;
        private List<FlowFile> children;
        private RecordSetWriterFactory writerFactory;
        private ComponentLog logger;
        private String hdfsPrefix;

        public HadoopFileStatusWriter.Builder session(final ProcessSession session) {
            this.session = session;
            return this;
        }

        public HadoopFileStatusWriter.Builder parent(final FlowFile parent) {
            this.parent = parent;
            return this;
        }

        public HadoopFileStatusWriter.Builder children(final List<FlowFile> children) {
            this.children = children;
            return this;
        }

        public HadoopFileStatusWriter.Builder writerFactory(final RecordSetWriterFactory writerFactory) {
            this.writerFactory = writerFactory;
            return this;
        }

        public HadoopFileStatusWriter.Builder logger(final ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public HadoopFileStatusWriter.Builder hdfsPrefix(final String hdfsPrefix) {
            this.hdfsPrefix = hdfsPrefix;
            return this;
        }

        public HadoopFileStatusWriter build() {
            validateMandatoryField("session", session);
            validateMandatoryField("parent", parent);
            validateMandatoryField("children", children);

            if (writerFactory == null) {
                return new FlowFileHadoopFileStatusWriter(session, parent, children, writerFactory, hdfsPrefix, logger);
            } else {
                return new RecordHadoopFileStatusWriter(session, parent, children, writerFactory, hdfsPrefix, logger);
            }
        }

        private void validateMandatoryField(String variableName, Object variable) {
            if (variable == null) {
                throw new IllegalArgumentException(variableName + " is null but must be set");
            }
        }
    }
}
