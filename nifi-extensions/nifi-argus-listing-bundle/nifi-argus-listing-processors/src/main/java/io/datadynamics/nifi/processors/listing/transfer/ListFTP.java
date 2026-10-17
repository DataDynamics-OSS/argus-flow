/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle, design/listing-processors-port.md):
 *   nifi-extension-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/ListFTP.java
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

package io.datadynamics.nifi.processors.listing.transfer;

import io.datadynamics.nifi.processor.util.list.TriggeredListing;
import io.datadynamics.nifi.processors.listing.file.ListFile;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.list.ListedEntityTracker;

import java.util.Collection;
import java.util.List;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"argus", "triggered", "list", "ftp", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("입력 FlowFile 마다 FTP 서버의 디렉터리 하나를 List 한다. Hostname·Port·Username·Password·Remote Path 에 FlowFile 속성 Expression Language 를 쓸 수 있어, 업스트림이 List 할 대상을 정한다. 파일마다 filename·path 속성을 가진 FlowFile 을 입력 FlowFile 의 자식으로 내보내며, FetchFTP 와 함께 쓴다. Apache NiFi ListFTP 의 트리거형 포크다.")
@SeeAlso(classNames = {"org.apache.nifi.processors.standard.FetchFTP", "org.apache.nifi.processors.standard.GetFTP", "org.apache.nifi.processors.standard.PutFTP"})
@WritesAttributes({
    @WritesAttribute(attribute = "argus.list.target", description = "List 대상 설명 (original, success)"),
    @WritesAttribute(attribute = "argus.list.count", description = "내보낸 엔티티 수 (original)"),
    @WritesAttribute(attribute = "argus.list.error.message", description = "실패 사유 (failure)"),
    @WritesAttribute(attribute = "argus.list.error.type", description = "실패 예외 클래스 (failure)"),
    @WritesAttribute(attribute = "ftp.remote.host", description = "The hostname of the FTP Server"),
    @WritesAttribute(attribute = "ftp.remote.port", description = "The port that was connected to on the FTP Server"),
    @WritesAttribute(attribute = "ftp.listing.user", description = "The username of the user that performed the FTP Listing"),
    @WritesAttribute(attribute = ListFile.FILE_OWNER_ATTRIBUTE, description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_GROUP_ATTRIBUTE, description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_PERMISSIONS_ATTRIBUTE, description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_SIZE_ATTRIBUTE, description = "The number of bytes in the source file"),
    @WritesAttribute(attribute = ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description = "The timestamp of when the file in the filesystem was" +
            "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the FTP Server"),
    @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the FTP Server from which the file was pulled"),
})
@Stateful(scopes = {Scope.CLUSTER}, description = "'Tracking Timestamps' 전략을 쓰면 대상별로 내보낸 엔티티 중 최신 타임스탬프와 "
    + "그 시각의 식별자를 CLUSTER 상태에 저장한다. 다른 전략에서는 상태를 쓰지 않는다.")
public class ListFTP extends AbstractTriggeredListFileTransfer {

    private static final PropertyDescriptor PORT =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("21").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FILE_TRANSFER_LISTING_STRATEGY,
            HOSTNAME,
            PORT,
            USERNAME,
            FTPTransfer.PASSWORD,
            REMOTE_PATH,
            RECORD_WRITER,
            FTPTransfer.RECURSIVE_SEARCH,
            FTPTransfer.FOLLOW_SYMLINK,
            FTPTransfer.FILE_FILTER_REGEX,
            FTPTransfer.PATH_FILTER_REGEX,
            FTPTransfer.IGNORE_DOTTED_FILES,
            FTPTransfer.REMOTE_POLL_BATCH_SIZE,
            FTPTransfer.CONNECTION_TIMEOUT,
            FTPTransfer.DATA_TIMEOUT,
            FTPTransfer.CONNECTION_MODE,
            FTPTransfer.TRANSFER_MODE,
            FTPTransfer.PROXY_CONFIGURATION_SERVICE,
            FTPTransfer.BUFFER_SIZE,
            TriggeredListing.MAX_TRACKED_TARGETS,
            TriggeredListing.TRACKING_STATE_CACHE,
            TriggeredListing.TRACKING_TIME_WINDOW,
            TriggeredListing.INITIAL_LISTING_TARGET,
            TriggeredListing.MAX_LISTING_SIZE,
            ALLOWED_HOSTS,
            FTPTransfer.UTF8_ENCODING
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        FTPTransfer.migrateProxyProperties(config);
        config.renameProperty(FTPTransfer.OBSOLETE_UTF8_ENCODING, FTPTransfer.UTF8_ENCODING.getName());
        config.renameProperty(FTPTransfer.OLD_FOLLOW_SYMLINK_PROPERTY_NAME, FTPTransfer.FOLLOW_SYMLINK.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, ListedEntityTracker.TRACKING_STATE_CACHE.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, ListedEntityTracker.TRACKING_TIME_WINDOW.getName());
        config.renameProperty(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, ListedEntityTracker.INITIAL_LISTING_TARGET.getName());
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new FTPTransfer(context, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "ftp";
    }


    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
        FTPTransfer.validateProxySpec(validationContext, results);
    }
}
