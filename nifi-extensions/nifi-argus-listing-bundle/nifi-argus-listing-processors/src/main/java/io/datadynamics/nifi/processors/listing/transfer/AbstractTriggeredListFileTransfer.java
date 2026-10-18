/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle):
 *   nifi-extension-bundles/nifi-extension-utils/nifi-file-transfer/src/main/java/org/apache/nifi/processor/util/file/transfer/ListFileTransfer.java
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

import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.GetFileTransfer;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import io.datadynamics.nifi.processor.util.list.AbstractTriggeredListProcessor;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class AbstractTriggeredListFileTransfer extends AbstractTriggeredListProcessor<FileInfo> {
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
        .name("Hostname")
        .description("The fully qualified hostname or IP address of the remote system")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) // ARGUS: FlowFile 속성으로 대상 지정
        .build();
    public static final PropertyDescriptor UNDEFAULTED_PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .description("The port to connect to on the remote host to fetch the data from")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) // ARGUS: FlowFile 속성으로 대상 지정
        .required(true)
        .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("Username")
        .description("Username")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) // ARGUS: FlowFile 속성으로 대상 지정
        .required(true)
        .build();
    public static final PropertyDescriptor REMOTE_PATH = new PropertyDescriptor.Builder()
        .name("Remote Path")
        .description("The path on the remote system from which to pull or push files")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) // ARGUS: FlowFile 속성으로 대상 지정
        .defaultValue(".")
        .build();
    // ARGUS: 'Time Window' 전략은 트리거 모델에 의미가 없어 없앴다.
    public static final PropertyDescriptor FILE_TRANSFER_LISTING_STRATEGY = LISTING_STRATEGY;
    // ARGUS: FlowFile 이 접속 호스트를 정하므로 허용 범위를 제한할 수 있게 한다.
    public static final PropertyDescriptor ALLOWED_HOSTS = new PropertyDescriptor.Builder()
        .name("Allowed Hosts")
        .description("접속을 허용할 호스트 이름의 정규식. 'Hostname' 을 평가한 값이 이 식과 일치하지 않으면 접속하지 않고 입력 FlowFile 을 failure 로 보낸다. "
            + "비워 두면 제한하지 않는다. Hostname 을 FlowFile 속성으로 받는 경우 설정을 권장한다.")
        .required(false)
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .build();

    @Override
    protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(GetFileTransfer.FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
        attributes.put(getProtocolName() + ".remote.host", context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue());
        attributes.put(getProtocolName() + ".remote.port", context.getProperty(UNDEFAULTED_PORT).evaluateAttributeExpressions().getValue());
        attributes.put(getProtocolName() + ".listing.user", context.getProperty(USERNAME).evaluateAttributeExpressions().getValue());
        attributes.put(GetFileTransfer.FILE_LAST_MODIFY_TIME_ATTRIBUTE, dateTimeFormatter.format(Instant.ofEpochMilli(fileInfo.getLastModifiedTime()).atZone(ZoneId.systemDefault())));
        attributes.put(GetFileTransfer.FILE_PERMISSIONS_ATTRIBUTE, fileInfo.getPermissions());
        attributes.put(GetFileTransfer.FILE_OWNER_ATTRIBUTE, fileInfo.getOwner());
        attributes.put(GetFileTransfer.FILE_GROUP_ATTRIBUTE, fileInfo.getGroup());
        attributes.put(GetFileTransfer.FILE_SIZE_ATTRIBUTE, Long.toString(fileInfo.getSize()));
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getFileName());
        final String fullPath = fileInfo.getFullPathFileName();
        if (fullPath != null) {
            final int index = fullPath.lastIndexOf("/");
            if (index > -1) {
                final String path = fullPath.substring(0, index);
                attributes.put(CoreAttributes.PATH.key(), path);
            }
        }
        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(REMOTE_PATH).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, 0L, ListingMode.CONFIGURATION_VERIFICATION, false).size();
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode) throws IOException {
        return performListing(context, minTimestamp, listingMode, true);
    }

    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                            final boolean applyFilters) throws IOException {
        // ARGUS: 허용 호스트 검사 — 접속 전에 막는다.
        final String allowedHosts = context.getProperty(ALLOWED_HOSTS).getValue();
        if (allowedHosts != null) {
            final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
            if (hostname == null || !Pattern.compile(allowedHosts).matcher(hostname).matches()) {
                throw new IOException("Hostname [%s] is not allowed by '%s'".formatted(hostname, ALLOWED_HOSTS.getName()));
            }
        }

        final FileTransfer transfer = getFileTransfer(context);
        final List<FileInfo> listing;
        try {
            listing = transfer.getListing(applyFilters);
        } finally {
            IOUtils.closeQuietly(transfer);
        }

        if (minTimestamp == null) {
            return listing;
        }

        final Iterator<FileInfo> itr = listing.iterator();
        while (itr.hasNext()) {
            final FileInfo next = itr.next();
            if (next.getLastModifiedTime() < minTimestamp) {
                itr.remove();
            }
        }

        return listing;
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("Remote Directory [%s] on [%s:%s]", getPath(context), context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(UNDEFAULTED_PORT).evaluateAttributeExpressions().getValue());
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return FileInfo.getRecordSchema();
    }


    // ARGUS: 업스트림 isListingResetNecessary() 는 Hostname·Remote Path 만 봤다. Port 도 대상이다.
    @Override
    protected Collection<PropertyDescriptor> getTargetProperties() {
        return List.of(HOSTNAME, UNDEFAULTED_PORT, REMOTE_PATH);
    }

    // ARGUS: 같은 경로라도 사용자·필터가 바뀌면 보이는 파일이 달라지므로 대상 키에 넣는다.
    @Override
    protected Collection<PropertyDescriptor> getTrackingKeyProperties() {
        return List.of(USERNAME, FileTransfer.RECURSIVE_SEARCH, FileTransfer.FOLLOW_SYMLINK, FileTransfer.FILE_FILTER_REGEX,
                FileTransfer.PATH_FILTER_REGEX, FileTransfer.IGNORE_DOTTED_FILES);
    }

    protected abstract FileTransfer getFileTransfer(final ProcessContext context);

    protected abstract String getProtocolName();
}
