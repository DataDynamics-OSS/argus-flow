/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle):
 *   nifi-extension-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/ListSFTP.java
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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.list.ListedEntityTracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"argus", "triggered", "list", "sftp", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("입력 FlowFile 마다 SFTP 서버의 디렉터리 하나를 List 한다. Hostname·Port·Username·Password·Remote Path 에 FlowFile 속성 Expression Language 를 쓸 수 있어, 업스트림이 List 할 대상을 정한다. 파일마다 filename·path 속성을 가진 FlowFile 을 입력 FlowFile 의 자식으로 내보내며, FetchSFTP 와 함께 쓴다. Apache NiFi ListSFTP 의 트리거형 포크다.")
@SeeAlso(classNames = {"org.apache.nifi.processors.standard.FetchSFTP", "org.apache.nifi.processors.standard.GetSFTP", "org.apache.nifi.processors.standard.PutSFTP"})
@WritesAttributes({
    @WritesAttribute(attribute = "argus.list.target", description = "List 대상 설명 (original, success)"),
    @WritesAttribute(attribute = "argus.list.count", description = "내보낸 엔티티 수 (original)"),
    @WritesAttribute(attribute = "argus.list.error.message", description = "실패 사유 (failure)"),
    @WritesAttribute(attribute = "argus.list.error.type", description = "실패 예외 클래스 (failure)"),
    @WritesAttribute(attribute = "sftp.remote.host", description = "The hostname of the SFTP Server"),
    @WritesAttribute(attribute = "sftp.remote.port", description = "The port that was connected to on the SFTP Server"),
    @WritesAttribute(attribute = "sftp.listing.user", description = "The username of the user that performed the SFTP Listing"),
    @WritesAttribute(attribute = ListFile.FILE_OWNER_ATTRIBUTE, description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_GROUP_ATTRIBUTE, description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_PERMISSIONS_ATTRIBUTE, description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_SIZE_ATTRIBUTE, description = "The number of bytes in the source file"),
    @WritesAttribute(attribute = ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description = "The timestamp of when the file in the filesystem was" +
                  "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the SFTP Server"),
    @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the SFTP Server from which the file was pulled"),
    @WritesAttribute(attribute = "mime.type", description = "The MIME Type that is provided by the configured Record Writer"),
})
@Stateful(scopes = {Scope.CLUSTER}, description = "'Tracking Timestamps' 전략을 쓰면 대상별로 내보낸 엔티티 중 최신 타임스탬프와 "
    + "그 시각의 식별자를 CLUSTER 상태에 저장한다. 다른 전략에서는 상태를 쓰지 않는다.")
public class ListSFTP extends AbstractTriggeredListFileTransfer {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FILE_TRANSFER_LISTING_STRATEGY,
            SFTPTransfer.HOSTNAME,
            SFTPTransfer.PORT,
            SFTPTransfer.USERNAME,
            SFTPTransfer.PASSWORD,
            SFTPTransfer.PRIVATE_KEY_PATH,
            SFTPTransfer.PRIVATE_KEY_PASSPHRASE,
            REMOTE_PATH,
            RECORD_WRITER,
            SFTPTransfer.RECURSIVE_SEARCH,
            SFTPTransfer.FOLLOW_SYMLINK,
            SFTPTransfer.FILE_FILTER_REGEX,
            SFTPTransfer.PATH_FILTER_REGEX,
            SFTPTransfer.IGNORE_DOTTED_FILES,
            SFTPTransfer.STRICT_HOST_KEY_CHECKING,
            SFTPTransfer.HOST_KEY_FILE,
            SFTPTransfer.CONNECTION_TIMEOUT,
            SFTPTransfer.DATA_TIMEOUT,
            SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT,
            SFTPTransfer.USE_COMPRESSION,
            SFTPTransfer.PROXY_CONFIGURATION_SERVICE,
            TriggeredListing.MAX_TRACKED_TARGETS,
            TriggeredListing.TRACKING_STATE_CACHE,
            TriggeredListing.TRACKING_TIME_WINDOW,
            TriggeredListing.INITIAL_LISTING_TARGET,
            TriggeredListing.MAX_LISTING_SIZE,
            ALLOWED_HOSTS,
            ListFile.MIN_AGE,
            ListFile.MAX_AGE,
            ListFile.MIN_SIZE,
            ListFile.MAX_SIZE,
            SFTPTransfer.ALGORITHM_CONFIGURATION,
            SFTPTransfer.CIPHERS_ALLOWED,
            SFTPTransfer.KEY_ALGORITHMS_ALLOWED,
            SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED,
            SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED
    );

    private volatile Predicate<FileInfo> fileFilter;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        FTPTransfer.migrateProxyProperties(config);
        config.removeProperty(FileTransfer.REMOTE_POLL_BATCH_SIZE.getName());
        SFTPTransfer.migrateAlgorithmProperties(config);
        config.renameProperty(SFTPTransfer.OLD_FOLLOW_SYMLINK_PROPERTY_NAME, SFTPTransfer.FOLLOW_SYMLINK.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, ListedEntityTracker.TRACKING_STATE_CACHE.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, ListedEntityTracker.TRACKING_TIME_WINDOW.getName());
        config.renameProperty(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, ListedEntityTracker.INITIAL_LISTING_TARGET.getName());
    }

    // ARGUS: SFTP 는 파일 나이·크기 필터도 있다.
    @Override
    protected Collection<PropertyDescriptor> getTrackingKeyProperties() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getTrackingKeyProperties());
        properties.addAll(List.of(ListFile.MIN_AGE, ListFile.MAX_AGE, ListFile.MIN_SIZE, ListFile.MAX_SIZE));
        return properties;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new SFTPTransfer(context, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "sftp";
    }


    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
        SFTPTransfer.validateProxySpec(validationContext, results);
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                            final boolean applyFilters) throws IOException {
        final List<FileInfo> listing = super.performListing(context, minTimestamp, listingMode, applyFilters);

        if (!applyFilters) {
            return listing;
        }

        final Predicate<FileInfo> filePredicate = listingMode == ListingMode.EXECUTION ? this.fileFilter : createFileFilter(context);
        return listing.stream()
                .filter(filePredicate)
                .collect(Collectors.toList());
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fileFilter = createFileFilter(context);
    }

    private Predicate<FileInfo> createFileFilter(final ProcessContext context) {
        final long minSize = context.getProperty(ListFile.MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxSize = context.getProperty(ListFile.MAX_SIZE).asDataSize(DataUnit.B);
        final long minAge = context.getProperty(ListFile.MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(ListFile.MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        return (attributes) -> {
            if (attributes.isDirectory()) {
                return true;
            }

            if (minSize > attributes.getSize()) {
                return false;
            }
            if (maxSize != null && maxSize < attributes.getSize()) {
                return false;
            }
            final long fileAge = System.currentTimeMillis() - attributes.getLastModifiedTime();
            if (minAge > fileAge) {
                return false;
            }
            if (maxAge != null && maxAge < fileAge) {
                return false;
            }

            return true;
        };
    }
}
