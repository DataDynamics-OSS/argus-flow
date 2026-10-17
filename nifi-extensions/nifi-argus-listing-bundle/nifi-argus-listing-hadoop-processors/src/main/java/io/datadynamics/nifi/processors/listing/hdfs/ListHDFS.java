/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle, design/listing-processors-port.md):
 *   nifi-extension-bundles/nifi-hadoop-bundle/nifi-hdfs-processors/src/main/java/org/apache/nifi/processors/hadoop/ListHDFS.java
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

import io.datadynamics.nifi.processor.util.list.FlowFileBoundProcessContext;
import io.datadynamics.nifi.processor.util.list.ListingTarget;
import io.datadynamics.nifi.processor.util.list.TriggeredListing;
import io.datadynamics.nifi.processor.util.list.TriggeredListingSupport;
import io.datadynamics.nifi.processors.listing.hdfs.util.writer.HadoopFileStatusWriter;
import org.apache.nifi.processors.hadoop.AbstractHadoopProcessor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.ListableEntityWrapper;
import org.apache.nifi.expression.ExpressionLanguageScope;
import io.datadynamics.nifi.processors.listing.hdfs.util.FileStatusIterable;
import io.datadynamics.nifi.processors.listing.hdfs.util.FilterMode;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.datadynamics.nifi.processors.listing.hdfs.util.FilterMode.FILTER_DIRECTORIES_AND_FILES;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"argus", "triggered", "hadoop", "HCFS", "HDFS", "get", "list", "ingest", "source", "filesystem"})
@SeeAlso(classNames = {"org.apache.nifi.processors.hadoop.GetHDFS", "org.apache.nifi.processors.hadoop.FetchHDFS", "org.apache.nifi.processors.hadoop.PutHDFS"})
@CapabilityDescription("입력 FlowFile 마다 HDFS(또는 Hadoop 호환 파일시스템)의 디렉터리 하나를 List 한다. Directory·File Filter 에 FlowFile 속성 Expression Language 를 쓸 수 있어, 업스트림이 List 할 디렉터리를 정한다. 파일마다 filename·path 속성을 가진 FlowFile 을 입력 FlowFile 의 자식으로 내보내며, FetchHDFS 와 함께 쓴다. Apache NiFi ListHDFS 의 트리거형 포크다.")
@WritesAttributes({
    @WritesAttribute(attribute = "argus.list.target", description = "List 대상 설명 (original, success)"),
    @WritesAttribute(attribute = "argus.list.count", description = "내보낸 엔티티 수 (original)"),
    @WritesAttribute(attribute = "argus.list.error.message", description = "실패 사유 (failure)"),
    @WritesAttribute(attribute = "argus.list.error.type", description = "실패 예외 클래스 (failure)"),
        @WritesAttribute(attribute = "filename", description = "The name of the file that was read from HDFS."),
        @WritesAttribute(attribute = "path", description = "The path is set to the absolute path of the file's directory on HDFS. For example, if the Directory property is set to /tmp, "
                + "then files picked up from /tmp will have the path attribute set to \"./\". If the Recurse Subdirectories property is set to true and a file is picked up "
                + "from /tmp/abc/1/2/3, then the path attribute will be set to \"/tmp/abc/1/2/3\"."),
        @WritesAttribute(attribute = "hdfs.owner", description = "The user that owns the file in HDFS"),
        @WritesAttribute(attribute = "hdfs.group", description = "The group that owns the file in HDFS"),
        @WritesAttribute(attribute = "hdfs.lastModified", description = "The timestamp of when the file in HDFS was last modified, as milliseconds since midnight Jan 1, 1970 UTC"),
        @WritesAttribute(attribute = "hdfs.length", description = "The number of bytes in the file in HDFS"),
        @WritesAttribute(attribute = "hdfs.replication", description = "The number of HDFS replicas for hte file"),
        @WritesAttribute(attribute = "hdfs.permissions", description = "The permissions for the file in HDFS. This is formatted as 3 characters for the owner, "
                + "3 for the group, and 3 for other users. For example rw-rw-r--")
})
@Stateful(scopes = {Scope.CLUSTER}, description = "'Tracking Timestamps' 전략을 쓰면 대상별로 내보낸 엔티티 중 최신 타임스탬프와 "
    + "그 시각의 식별자를 CLUSTER 상태에 저장한다. 다른 전략에서는 상태를 쓰지 않는다.")
public class ListHDFS extends AbstractHadoopProcessor {

    private static final String NON_HIDDEN_FILES_REGEX = "[^\\.].*";
    private static final String HDFS_ATTRIBUTE_PREFIX = "hdfs";

    public static final PropertyDescriptor RECURSE_SUBDIRS = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether to list files from subdirectories of the HDFS directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = TriggeredListing.RECORD_WRITER;

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue(NON_HIDDEN_FILES_REGEX)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) // ARGUS: FlowFile 속성으로 필터 지정
            .build();

    // ARGUS: FlowFile 이 디렉터리를 정하므로, List 할 수 있는 범위를 설정으로 제한할 수 있게 한다.
    public static final PropertyDescriptor ALLOWED_BASE_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Allowed Base Directory")
            .description("List 를 허용할 최상위 디렉터리. 'Directory' 를 평가해 정규화한 경로가 이 디렉터리 아래가 아니면 List 하지 않고 "
                    + "입력 FlowFile 을 failure 로 보낸다. 비워 두면 제한하지 않는다. Directory 를 FlowFile 속성으로 받는 경우 설정을 권장한다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor FILE_FILTER_MODE = new PropertyDescriptor.Builder()
            .name("File Filter Mode")
            .description("Determines how the regular expression in  " + FILE_FILTER.getDisplayName() + " will be used when retrieving listings.")
            .required(true)
            .allowableValues(FilterMode.class)
            .defaultValue(FILTER_DIRECTORIES_AND_FILES.getValue())
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor MINIMUM_FILE_AGE = new PropertyDescriptor.Builder()
            .name("Minimum File Age")
            .description("The minimum age that a file must be in order to be pulled; any file younger than this "
                    + "amount of time (based on last modification date) will be ignored")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(0, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            .build();

    public static final PropertyDescriptor MAXIMUM_FILE_AGE = new PropertyDescriptor.Builder()
            .name("Maximum File Age")
            .description("The maximum age that a file must be in order to be pulled; any file older than this "
                    + "amount of time (based on last modification date) will be ignored. Minimum value is 100ms.")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            .build();

    public static final Relationship REL_SUCCESS = TriggeredListing.REL_SUCCESS;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                DIRECTORY,
                ALLOWED_BASE_DIRECTORY,
                TriggeredListing.LISTING_STRATEGY,
                TriggeredListing.MAX_TRACKED_TARGETS,
                TriggeredListing.TRACKING_STATE_CACHE,
                TriggeredListing.TRACKING_TIME_WINDOW,
                TriggeredListing.INITIAL_LISTING_TARGET,
                RECURSE_SUBDIRS,
                RECORD_WRITER,
                TriggeredListing.MAX_LISTING_SIZE,
                FILE_FILTER,
                FILE_FILTER_MODE,
                MINIMUM_FILE_AGE,
                MAXIMUM_FILE_AGE
            )
    ).toList();

    private static final Set<Relationship> RELATIONSHIPS = TriggeredListing.RELATIONSHIPS;

    // ARGUS: 업스트림은 Directory·File Filter 변경 시 상태를 지웠다. 결과에 영향을 주는 나머지 속성도 대상 키에 넣는다.
    private static final List<PropertyDescriptor> TRACKING_KEY_PROPERTIES = List.of(
            RECURSE_SUBDIRS,
            FILE_FILTER,
            FILE_FILTER_MODE,
            MINIMUM_FILE_AGE,
            MAXIMUM_FILE_AGE
    );

    private volatile boolean resetTracking = false;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {

        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(context));

        final Long minAgeProp = context.getProperty(MINIMUM_FILE_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAgeProp = context.getProperty(MAXIMUM_FILE_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long minimumAge = (minAgeProp == null) ? 0L : minAgeProp;
        final long maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;

        if (minimumAge > maximumAge) {
            problems.add(new ValidationResult.Builder().valid(false).subject("ListHDFS Configuration")
                    .explanation(MINIMUM_FILE_AGE.getDisplayName() + " cannot be greater than " + MAXIMUM_FILE_AGE.getDisplayName()).build());
        }
        return problems;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (isConfigurationRestored()
                && (TriggeredListing.LISTING_STRATEGY.equals(descriptor) || DIRECTORY.equals(descriptor) || TRACKING_KEY_PROPERTIES.contains(descriptor))) {
            resetTracking = true;
        }
    }

    // ARGUS: 대상·결과 설정이나 추적 전략이 바뀌면 다음 시작 시 추적 상태(와 캐시 항목)를 지운다.
    @OnScheduled
    public void resetTrackingIfNecessary(final ProcessContext context) throws IOException {
        if (resetTracking) {
            TriggeredListingSupport.resetTracking(context, getIdentifier(), getLogger());
            resetTracking = false;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // ARGUS: 입력 FlowFile 에 묶인 컨텍스트 — Directory·File Filter 를 FlowFile 속성으로 평가한다.
        final ProcessContext boundContext = FlowFileBoundProcessContext.bind(context, flowFile);
        final AtomicReference<Path> rootPath = new AtomicReference<>();
        TriggeredListingSupport.process(boundContext, session, flowFile, getIdentifier(), getLogger(),
                () -> {
                    rootPath.set(resolveRootPath(boundContext, flowFile));
                    return ListingTarget.of("HDFS Directory [%s]".formatted(rootPath.get()), boundContext, TRACKING_KEY_PROPERTIES);
                },
                minTimestamp -> listFileStatuses(boundContext, rootPath.get(), minTimestamp),
                (listingSession, parent, entities, children) -> HadoopFileStatusWriter.builder()
                        .session(listingSession)
                        .parent(parent)
                        .children(children)
                        .writerFactory(boundContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class))
                        .hdfsPrefix(getAttributePrefix())
                        .logger(getLogger())
                        .build()
                        .write(entities.stream().map(ListableEntityWrapper::getRawEntity).toList()));
    }

    private Path resolveRootPath(final ProcessContext context, final FlowFile flowFile) throws IOException {
        final String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
        if (directory == null || directory.isBlank()) {
            throw new IOException("Blank Directory is not valid: review configured expression for " + DIRECTORY.getName());
        }
        final Path rootPath = getNormalizedPath(context, DIRECTORY, flowFile);

        final String allowedBaseDirectory = context.getProperty(ALLOWED_BASE_DIRECTORY).evaluateAttributeExpressions().getValue();
        if (allowedBaseDirectory != null) {
            final String base = Path.getPathWithoutSchemeAndAuthority(getNormalizedPath(allowedBaseDirectory)).toString();
            final String root = Path.getPathWithoutSchemeAndAuthority(rootPath).toString();
            final String baseWithSeparator = base.endsWith("/") ? base : base + "/";
            if (!root.equals(base) && !root.startsWith(baseWithSeparator)) {
                throw new IOException("Directory [%s] is outside of %s [%s]".formatted(rootPath, ALLOWED_BASE_DIRECTORY.getName(), base));
            }
        }
        return rootPath;
    }

    /**
     * ARGUS: 업스트림 HadoopFileStatusWriter.determineListable() 의 필터(복사 중 파일, File Filter, 파일 나이)를 그대로 적용하고,
     * 이전 상태와의 비교는 TriggeredListingSupport 의 추적 전략에 맡긴다.
     */
    private List<ListableEntityWrapper<FileStatus>> listFileStatuses(final ProcessContext context, final Path rootPath, final long minTimestamp) {
        final FileSystem hdfs = getFileSystem();
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();
        final PathFilter pathFilter = createPathFilter(context);

        final Long minAgeProp = context.getProperty(MINIMUM_FILE_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long minimumAge = (minAgeProp == null) ? Long.MIN_VALUE : minAgeProp;
        final Long maxAgeProp = context.getProperty(MAXIMUM_FILE_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;
        final long currentTimeMillis = System.currentTimeMillis();

        final FileStatusIterable fileStatusIterable = new FileStatusIterable(rootPath, recursive, hdfs, getUserGroupInformation());
        final List<ListableEntityWrapper<FileStatus>> listed = new ArrayList<>();
        for (final FileStatus status : fileStatusIterable) {
            final boolean isCopyInProgress = status.getPath().getName().endsWith("_COPYING_");
            if (isCopyInProgress || !pathFilter.accept(status.getPath())) {
                continue;
            }
            final long fileAge = currentTimeMillis - status.getModificationTime();
            if (minimumAge > fileAge || fileAge > maximumAge || status.getModificationTime() < minTimestamp) {
                continue;
            }
            listed.add(new ListableEntityWrapper<>(
                    status,
                    fileStatus -> fileStatus.getPath().getName(),
                    fileStatus -> fileStatus.getPath().toString(),
                    FileStatus::getModificationTime,
                    FileStatus::getLen
            ));
        }

        getLogger().debug("Found a total of {} files in HDFS under {}, {} are listable", fileStatusIterable.getTotalFileCount(), rootPath, listed.size());
        return listed;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("record-writer", RECORD_WRITER.getName());
        config.renameProperty("file-filter-mode", FILE_FILTER_MODE.getName());
        config.renameProperty("minimum-file-age", MINIMUM_FILE_AGE.getName());
        config.renameProperty("maximum-file-age", MAXIMUM_FILE_AGE.getName());
    }

    private PathFilter createPathFilter(final ProcessContext context) {
        // ARGUS: File Filter 가 FlowFile 속성을 쓸 수 있으므로 업스트림처럼 스케줄 시점에 한 번 컴파일하지 않는다.
        final Pattern fileFilterRegexPattern = Pattern.compile(context.getProperty(FILE_FILTER).evaluateAttributeExpressions().getValue());
        final FilterMode filterMode = FilterMode.forName(context.getProperty(FILE_FILTER_MODE).getValue());
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();

        return switch (filterMode) {
            case FILTER_MODE_FILES_ONLY -> path -> fileFilterRegexPattern.matcher(path.getName()).matches();
            case FILTER_MODE_FULL_PATH -> path -> fileFilterRegexPattern.matcher(path.toString()).matches()
                    || fileFilterRegexPattern.matcher(Path.getPathWithoutSchemeAndAuthority(path).toString()).matches();
            // FILTER_DIRECTORIES_AND_FILES
            default -> path -> Stream.of(Path.getPathWithoutSchemeAndAuthority(path).toString().split("/"))
                    .skip(getPathSegmentsToSkip(recursive))
                    .allMatch(v -> fileFilterRegexPattern.matcher(v).matches());
        };
    }

    private int getPathSegmentsToSkip(final boolean recursive) {
        // We need to skip the first leading '/' of the path and if the traverse is recursive
        // the filter will be applied only to the subdirectories.
        return recursive ? 2 : 1;
    }

    protected String getAttributePrefix() {
        return HDFS_ATTRIBUTE_PREFIX;
    }
}
