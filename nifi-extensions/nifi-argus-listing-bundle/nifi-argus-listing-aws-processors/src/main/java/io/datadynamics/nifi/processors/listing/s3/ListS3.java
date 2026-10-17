/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle, design/listing-processors-port.md):
 *   nifi-extension-bundles/nifi-aws-bundle/nifi-aws-processors/src/main/java/org/apache/nifi/processors/aws/s3/ListS3.java
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
package io.datadynamics.nifi.processors.listing.s3;

import io.datadynamics.nifi.processor.util.list.FlowFileBoundProcessContext;
import io.datadynamics.nifi.processor.util.list.ListingTarget;
import io.datadynamics.nifi.processor.util.list.TriggeredListing;
import io.datadynamics.nifi.processor.util.list.TriggeredListingSupport;
import org.apache.nifi.processors.aws.s3.AbstractS3Processor;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.ListableEntityWrapper;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.FormatUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION_WITH_FF_EL;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.getRegion;
import static io.datadynamics.nifi.processors.listing.s3.util.S3Util.getRequestPayer;
import static io.datadynamics.nifi.processors.listing.s3.util.S3Util.sanitizeETag;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"argus", "triggered", "Amazon", "S3", "AWS", "list"})
@SeeAlso(classNames = {"org.apache.nifi.processors.aws.s3.FetchS3Object", "org.apache.nifi.processors.aws.s3.PutS3Object", "org.apache.nifi.processors.aws.s3.DeleteS3Object", "org.apache.nifi.processors.aws.s3.CopyS3Object", "org.apache.nifi.processors.aws.s3.GetS3ObjectMetadata", "org.apache.nifi.processors.aws.s3.GetS3ObjectTags", "org.apache.nifi.processors.aws.s3.TagS3Object"})
@CapabilityDescription("입력 FlowFile 마다 S3 버킷의 Prefix 하나를 List 한다. Bucket·Prefix·Delimiter·Custom Region 에 FlowFile 속성 Expression Language 를 쓸 수 있어, 업스트림이 List 할 대상을 정한다. 객체마다 FlowFile 을 입력 FlowFile 의 자식으로 내보내며, FetchS3Object 와 함께 쓴다. Apache NiFi ListS3 의 트리거형 포크다.")
@Stateful(scopes = {Scope.CLUSTER}, description = "'Tracking Timestamps' 전략을 쓰면 대상별로 내보낸 엔티티 중 최신 타임스탬프와 "
    + "그 시각의 식별자를 CLUSTER 상태에 저장한다. 다른 전략에서는 상태를 쓰지 않는다.")
@WritesAttributes({
    @WritesAttribute(attribute = "argus.list.target", description = "List 대상 설명 (original, success)"),
    @WritesAttribute(attribute = "argus.list.count", description = "내보낸 엔티티 수 (original)"),
    @WritesAttribute(attribute = "argus.list.error.message", description = "실패 사유 (failure)"),
    @WritesAttribute(attribute = "argus.list.error.type", description = "실패 예외 클래스 (failure)"),
        @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
        @WritesAttribute(attribute = "s3.region", description = "The region of the S3 bucket"),
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
        @WritesAttribute(attribute = "s3.etag", description = "The ETag that can be used to see if the file has changed"),
        @WritesAttribute(attribute = "s3.isLatest", description = "A boolean indicating if this is the latest version of the object"),
        @WritesAttribute(attribute = "s3.lastModified", description = "The last modified time in milliseconds since epoch in UTC time"),
        @WritesAttribute(attribute = "s3.length", description = "The size of the object in bytes"),
        @WritesAttribute(attribute = "s3.storeClass", description = "The storage class of the object"),
        @WritesAttribute(attribute = "s3.version", description = "The version of the object, if applicable"),
        @WritesAttribute(attribute = "s3.tag.___", description = "If 'Write Object Tags' is set to 'True', the tags associated to the S3 object that is being listed " +
                "will be written as part of the FlowFile attributes"),
        @WritesAttribute(attribute = "s3.user.metadata.___", description = "If 'Write User Metadata' is set to 'True', the user defined metadata associated to the S3 object that is being listed " +
                "will be written as part of the FlowFile attributes")})
public class ListS3 extends AbstractS3Processor implements VerifiableProcessor {

    // ARGUS: 추적 전략과 관련 속성은 트리거형 공통 정의(대상별 상태)를 쓴다.
    public static final PropertyDescriptor LISTING_STRATEGY = TriggeredListing.LISTING_STRATEGY;
    public static final PropertyDescriptor TRACKING_STATE_CACHE = TriggeredListing.TRACKING_STATE_CACHE;
    public static final PropertyDescriptor INITIAL_LISTING_TARGET = TriggeredListing.INITIAL_LISTING_TARGET;
    public static final PropertyDescriptor TRACKING_TIME_WINDOW = TriggeredListing.TRACKING_TIME_WINDOW;

    // ARGUS: FlowFile 이 버킷을 정하므로 허용 범위를 제한할 수 있게 한다.
    public static final PropertyDescriptor ALLOWED_BUCKETS = new Builder()
            .name("Allowed Buckets")
            .description("List 를 허용할 버킷 이름의 정규식. 'Bucket' 을 평가한 값이 이 식과 일치하지 않으면 List 하지 않고 입력 FlowFile 을 "
                    + "failure 로 보낸다. 비워 두면 제한하지 않는다. Bucket 을 FlowFile 속성으로 받는 경우 설정을 권장한다.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor DELIMITER = new Builder()
            .name("Delimiter")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) // ARGUS: FlowFile 속성으로 대상 지정
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The string used to delimit directories within the bucket. Please consult the AWS documentation " +
                    "for the correct use of this field.")
            .build();

    public static final PropertyDescriptor PREFIX = new Builder()
            .name("Prefix")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) // ARGUS: FlowFile 속성으로 대상 지정
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The prefix used to filter the object list. Do not begin with a forward slash '/'. In most cases, it should end with a forward slash '/'.")
            .build();

    public static final PropertyDescriptor USE_VERSIONS = new Builder()
            .name("Use Versions")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to use S3 versions, if applicable.  If false, only the latest version of each object will be returned.")
            .build();

    public static final PropertyDescriptor LIST_TYPE = new Builder()
            .name("List Type")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .allowableValues(
                    new AllowableValue("1", "List Objects V1"),
                    new AllowableValue("2", "List Objects V2"))
            .defaultValue("1")
            .description("Specifies whether to use the original List Objects or the newer List Objects Version 2 endpoint.")
            .build();

    public static final PropertyDescriptor MIN_AGE = new Builder()
            .name("Minimum Object Age")
            .description("The minimum age that an S3 object must be in order to be considered; any object younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor MAX_AGE = new Builder()
            .name("Maximum Object Age")
            .description("The maximum age that an S3 object can be in order to be considered; any object older than this amount of time (according to last modification date) will be ignored")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .addValidator(createMaxAgeValidator())
            .build();

    public static final PropertyDescriptor WRITE_OBJECT_TAGS = new Builder()
            .name("Write Object Tags")
            .description("If set to 'True', the tags associated with the S3 object will be written as FlowFile attributes")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor REQUESTER_PAYS = new Builder()
            .name("Requester Pays")
            .required(true)
            .description("If true, indicates that the requester consents to pay any charges associated with listing "
                    + "the S3 bucket.  This sets the 'x-amz-request-payer' header to 'requester'.  Note that this "
                    + "setting is not applicable when 'Use Versions' is 'true'.")
            .addValidator(createRequesterPaysValidator())
            .allowableValues(
                new AllowableValue("true", "true", "Indicates that the requester consents to pay any charges associated with listing the S3 bucket."),
                new AllowableValue("false", "false", "Does not consent to pay requester charges for listing the S3 bucket."))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor WRITE_USER_METADATA = new Builder()
            .name("Write User Metadata")
            .description("If set to 'True', the user defined metadata associated with the S3 object will be added to FlowFile attributes/records")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = TriggeredListing.RECORD_WRITER;

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        BUCKET_WITHOUT_DEFAULT_VALUE,
        REGION,
        CUSTOM_REGION_WITH_FF_EL,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        ALLOWED_BUCKETS,
        LISTING_STRATEGY,
        TriggeredListing.MAX_TRACKED_TARGETS,
        TRACKING_STATE_CACHE,
        TRACKING_TIME_WINDOW,
        INITIAL_LISTING_TARGET,
        RECORD_WRITER,
        TriggeredListing.MAX_LISTING_SIZE,
        MIN_AGE,
        MAX_AGE,
        WRITE_OBJECT_TAGS,
        WRITE_USER_METADATA,
        TIMEOUT,
        SSL_CONTEXT_SERVICE,
        ENDPOINT_OVERRIDE,
        PROXY_CONFIGURATION_SERVICE,
        DELIMITER,
        PREFIX,
        USE_VERSIONS,
        LIST_TYPE,
        REQUESTER_PAYS);

    public static final Set<Relationship> RELATIONSHIPS = TriggeredListing.RELATIONSHIPS;

    // ARGUS: 업스트림 TRACKING_RESET_PROPERTY_DESCRIPTORS 에 해당. 대상 속성은 대상 설명에, 결과 속성은 대상 키에 들어간다.
    private static final Set<PropertyDescriptor> TARGET_PROPERTIES = Set.of(
            BUCKET_WITHOUT_DEFAULT_VALUE,
            REGION,
            CUSTOM_REGION_WITH_FF_EL,
            PREFIX,
            DELIMITER
    );

    private static final List<PropertyDescriptor> TRACKING_KEY_PROPERTIES = List.of(
            USE_VERSIONS,
            MIN_AGE,
            MAX_AGE
    );

    private volatile boolean resetTracking = false;

    public static final String NULL_VERSION_ID = "null";

    private volatile Long minObjectAgeMilliseconds;
    private volatile Long maxObjectAgeMilliseconds;

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (isConfigurationRestored()
                && (LISTING_STRATEGY.equals(descriptor) || TARGET_PROPERTIES.contains(descriptor) || TRACKING_KEY_PROPERTIES.contains(descriptor))) {
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

    @OnScheduled
    public void initObjectAgeThresholds(ProcessContext context) {
        minObjectAgeMilliseconds = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        maxObjectAgeMilliseconds = context.getProperty(MAX_AGE) != null ? context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS) : null;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("listing-strategy", LISTING_STRATEGY.getName());
        config.renameProperty("delimiter", DELIMITER.getName());
        config.renameProperty("prefix", PREFIX.getName());
        config.renameProperty("use-versions", USE_VERSIONS.getName());
        config.renameProperty("list-type", LIST_TYPE.getName());
        config.renameProperty("min-age", MIN_AGE.getName());
        config.renameProperty("max-age", MAX_AGE.getName());
        config.renameProperty("write-s3-object-tags", WRITE_OBJECT_TAGS.getName());
        config.renameProperty("requester-pays", REQUESTER_PAYS.getName());
        config.renameProperty("write-s3-user-metadata", WRITE_USER_METADATA.getName());
        config.renameProperty("record-writer", RECORD_WRITER.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, TRACKING_STATE_CACHE.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, TRACKING_TIME_WINDOW.getName());
        config.renameProperty(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, INITIAL_LISTING_TARGET.getName());
    }

    private static Validator createRequesterPaysValidator() {
        return (subject, input, context) -> {
            boolean requesterPays = Boolean.parseBoolean(input);
            boolean useVersions = context.getProperty(USE_VERSIONS).asBoolean();
            boolean valid = !requesterPays || !useVersions;
            return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(valid)
                    .explanation(valid ? null : "'Requester Pays' cannot be used when listing object versions.")
                    .build();
        };
    }
    private static Validator createMaxAgeValidator() {
        return (subject, input, context) -> {
            Double  maxAge = input != null ? FormatUtils.getPreciseTimeDuration(input, TimeUnit.MILLISECONDS) : null;
            long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
            boolean valid = input != null && maxAge > minAge;
            return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(valid)
                    .explanation(valid ? null : "'Maximum Age' must be greater than 'Minimum Age' ")
                    .build();
        };
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // ARGUS: 입력 FlowFile 에 묶인 컨텍스트 — Bucket·Prefix·Delimiter·Region 을 FlowFile 속성으로 평가한다.
        final ProcessContext boundContext = FlowFileBoundProcessContext.bind(context, flowFile);
        final Map<String, String> attributes = flowFile.getAttributes();
        TriggeredListingSupport.process(boundContext, session, flowFile, getIdentifier(), getLogger(),
                () -> resolveTarget(boundContext, attributes),
                minTimestamp -> listObjectVersions(boundContext, attributes, minTimestamp),
                (listingSession, parent, entities, children) -> writeListing(boundContext, attributes, listingSession, parent, entities, children));
    }

    private ListingTarget resolveTarget(final ProcessContext context, final Map<String, String> attributes) throws IOException {
        final String bucket = context.getProperty(BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions().getValue();
        if (bucket == null || bucket.isBlank()) {
            throw new IOException("Blank Bucket is not valid: review configured expression for " + BUCKET_WITHOUT_DEFAULT_VALUE.getName());
        }
        final String allowedBuckets = context.getProperty(ALLOWED_BUCKETS).getValue();
        if (allowedBuckets != null && !Pattern.compile(allowedBuckets).matcher(bucket).matches()) {
            throw new IOException("Bucket [%s] is not allowed by '%s'".formatted(bucket, ALLOWED_BUCKETS.getName()));
        }

        final String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();
        final String delimiter = context.getProperty(DELIMITER).evaluateAttributeExpressions().getValue();
        return ListingTarget.of("S3 [s3://%s/%s] in [%s]%s".formatted(bucket, prefix == null ? "" : prefix, getRegion(context, attributes).id(),
                delimiter == null ? "" : " delimited by [" + delimiter + "]"), context, TRACKING_KEY_PROPERTIES);
    }

    private List<ListableEntityWrapper<ObjectVersion>> listObjectVersions(final ProcessContext context, final Map<String, String> attributes, final long minTimestamp) {
        final S3Client client = getClient(context, attributes);
        final S3BucketLister bucketLister = getS3BucketLister(context, client);
        final long currentTime = System.currentTimeMillis();

        final List<ListableEntityWrapper<ObjectVersion>> listed = new ArrayList<>();
        do {
            for (final ObjectVersion objectVersion : bucketLister.listVersions()) {
                if (objectVersion.lastModified().toEpochMilli() >= minTimestamp && includeObjectInListing(objectVersion, currentTime)) {
                    listed.add(new ListableEntityWrapper<>(
                            objectVersion,
                            ObjectVersion::key,
                            ListS3::getEntityIdentifier,
                            ov -> ov.lastModified().toEpochMilli(),
                            ObjectVersion::size
                    ));
                }
            }
            bucketLister.setNextMarker();
        } while (bucketLister.isTruncated());

        return listed;
    }

    private static String getEntityIdentifier(final ObjectVersion objectVersion) {
        final String versionId = objectVersion.versionId();
        return versionId == null || NULL_VERSION_ID.equals(versionId) ? objectVersion.key() : objectVersion.key() + "_" + versionId;
    }

    private void writeListing(final ProcessContext context, final Map<String, String> attributes, final ProcessSession session, final FlowFile parent,
                              final List<ListableEntityWrapper<ObjectVersion>> entities, final List<FlowFile> children) throws IOException, SchemaNotFoundException {
        if (entities.isEmpty()) {
            return;
        }

        final S3Client client = getClient(context, attributes);
        final String region = getRegion(context, attributes).id();
        final String bucket = context.getProperty(BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions().getValue();

        final S3ObjectWriter writer;
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        if (writerFactory == null) {
            writer = new AttributeObjectWriter(session, parent, children);
        } else {
            writer = new RecordObjectWriter(session, writerFactory, getLogger(), region, parent, children);
        }

        writer.beginListing();
        try {
            for (final ListableEntityWrapper<ObjectVersion> entity : entities) {
                final ObjectVersion objectVersion = entity.getRawEntity();
                final Map<String, String> tagging = getTagging(context, client, bucket, objectVersion);
                final Map<String, String> userMetadata = getUserMetadata(context, client, bucket, objectVersion);
                writer.addToListing(objectVersion, tagging, userMetadata, region, bucket);
            }
            writer.finishListing();
        } catch (final IOException | RuntimeException e) {
            writer.finishListingExceptionally(e);
            throw e;
        }
    }

    private Map<String, String> getTagging(ProcessContext context, S3Client client, String bucket, ObjectVersion objectVersion) {
        Map<String, String> tagging = null;
        if (context.getProperty(WRITE_OBJECT_TAGS).asBoolean()) {
            try {
                final GetObjectTaggingResponse taggingResponse = client.getObjectTagging(GetObjectTaggingRequest.builder()
                        .bucket(bucket)
                        .key(objectVersion.key())
                        .build());

                tagging = taggingResponse.tagSet().stream()
                        .collect(Collectors.toMap(Tag::key, Tag::value));
            } catch (final Exception e) {
                getLogger().warn("Failed to obtain Object Tags for S3 Object {} in bucket {}. Will list S3 Object without the object tags",
                        objectVersion.key(), bucket, e);
            }
        }
        return tagging;
    }

    private Map<String, String> getUserMetadata(ProcessContext context, S3Client client, String bucket, ObjectVersion objectVersion) {
        Map<String, String> userMetadata = null;
        if (context.getProperty(WRITE_USER_METADATA).asBoolean()) {
            try {
                final HeadObjectResponse headResponse = client.headObject(HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectVersion.key())
                        .build());

                userMetadata = headResponse.metadata();
            } catch (final Exception e) {
                getLogger().warn("Failed to obtain User Metadata for S3 Object {} in bucket {}. Will list S3 Object without the user metadata",
                        objectVersion.key(), bucket, e);
            }
        }
        return userMetadata;
    }

    private S3BucketLister getS3BucketLister(ProcessContext context, S3Client client) {
        final boolean requesterPays = context.getProperty(REQUESTER_PAYS).asBoolean();
        final boolean useVersions = context.getProperty(USE_VERSIONS).asBoolean();

        final String bucket = context.getProperty(BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions().getValue();
        final String delimiter = context.getProperty(DELIMITER).evaluateAttributeExpressions().getValue();
        final String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();

        final int listType = context.getProperty(LIST_TYPE).asInteger();

        final S3BucketLister bucketLister = useVersions
                ? new S3VersionBucketLister(client)
                : listType == 2
                ? new S3ObjectBucketListerVersion2(client)
                : new S3ObjectBucketLister(client);

        bucketLister.setBucketName(bucket);
        bucketLister.setRequesterPays(requesterPays);

        if (delimiter != null && !delimiter.isEmpty()) {
            bucketLister.setDelimiter(delimiter);
        }
        if (prefix != null && !prefix.isEmpty()) {
            bucketLister.setPrefix(prefix);
        }
        return bucketLister;
    }

    private interface S3BucketLister {
        void setBucketName(String bucketName);
        void setPrefix(String prefix);
        void setDelimiter(String delimiter);
        void setRequesterPays(boolean requesterPays);
        // Versions have a superset of the fields that Objects have, so we'll use
        // them as a common interface
        List<ObjectVersion> listVersions();
        void setNextMarker();
        boolean isTruncated();
    }

    public static class S3ObjectBucketLister implements S3BucketLister {
        private final S3Client client;
        private ListObjectsRequest.Builder listObjectsRequestBuilder;
        private ListObjectsResponse listObjectsResponse;

        public S3ObjectBucketLister(S3Client client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listObjectsRequestBuilder = ListObjectsRequest.builder().bucket(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listObjectsRequestBuilder.prefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listObjectsRequestBuilder.delimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            listObjectsRequestBuilder.requestPayer(getRequestPayer(requesterPays));
        }

        @Override
        public List<ObjectVersion> listVersions() {
            listObjectsResponse = client.listObjects(listObjectsRequestBuilder.build());
            return listObjectsResponse.contents().stream()
                    .map(ListS3::s3ObjectToObjectVersion)
                    .toList();
        }

        @Override
        public void setNextMarker() {
            final String nextMarker = listObjectsResponse.nextMarker();
            if (nextMarker != null) {
                listObjectsRequestBuilder.marker(nextMarker);
            } else {
                final List<S3Object> contents = listObjectsResponse.contents();
                if (!contents.isEmpty()) {
                    listObjectsRequestBuilder.marker(contents.get(contents.size() - 1).key());
                }
            }
        }

        @Override
        public boolean isTruncated() {
            return listObjectsResponse != null && listObjectsResponse.isTruncated();
        }
    }

    public static class S3ObjectBucketListerVersion2 implements S3BucketLister {
        private final S3Client client;
        private ListObjectsV2Request.Builder listObjectsRequestBuilder;
        private ListObjectsV2Response listObjectsResponse;

        public S3ObjectBucketListerVersion2(S3Client client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listObjectsRequestBuilder = ListObjectsV2Request.builder().bucket(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listObjectsRequestBuilder.prefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listObjectsRequestBuilder.delimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            listObjectsRequestBuilder.requestPayer(getRequestPayer(requesterPays));
        }

        @Override
        public List<ObjectVersion> listVersions() {
            listObjectsResponse = client.listObjectsV2(listObjectsRequestBuilder.build());
            return listObjectsResponse.contents().stream()
                    .map(ListS3::s3ObjectToObjectVersion)
                    .toList();
        }

        @Override
        public void setNextMarker() {
            listObjectsRequestBuilder.continuationToken(listObjectsResponse.nextContinuationToken());
        }

        @Override
        public boolean isTruncated() {
            return listObjectsResponse != null && listObjectsResponse.isTruncated();
        }
    }

    public static class S3VersionBucketLister implements S3BucketLister {
        private final S3Client client;
        private ListObjectVersionsRequest.Builder listVersionsRequestBuilder;
        private ListObjectVersionsResponse listVersionsResponse;

        public S3VersionBucketLister(S3Client client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listVersionsRequestBuilder = ListObjectVersionsRequest.builder().bucket(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listVersionsRequestBuilder.prefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listVersionsRequestBuilder.delimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            listVersionsRequestBuilder.requestPayer(getRequestPayer(requesterPays));
        }

        @Override
        public List<ObjectVersion> listVersions() {
            listVersionsResponse = client.listObjectVersions(listVersionsRequestBuilder.build());
            return listVersionsResponse.versions();
        }

        @Override
        public void setNextMarker() {
            listVersionsRequestBuilder.keyMarker(listVersionsResponse.nextKeyMarker());
            listVersionsRequestBuilder.versionIdMarker(listVersionsResponse.nextVersionIdMarker());
        }

        @Override
        public boolean isTruncated() {
            return listVersionsResponse != null && listVersionsResponse.isTruncated();
        }
    }

    interface S3ObjectWriter {
        void beginListing() throws IOException, SchemaNotFoundException;

        void addToListing(ObjectVersion objectVersion, Map<String, String> tagging, Map<String, String> userMetadata, String region, String bucket) throws IOException;

        void finishListing() throws IOException;

        void finishListingExceptionally(Exception cause);
    }

    static class RecordObjectWriter implements S3ObjectWriter {
        private static final RecordSchema RECORD_SCHEMA;

        private static final String KEY = "key";
        private static final String BUCKET = "bucket";
        private static final String OWNER = "owner";
        private static final String ETAG = "etag";
        private static final String LAST_MODIFIED = "lastModified";
        private static final String SIZE = "size";
        private static final String STORAGE_CLASS = "storageClass";
        private static final String IS_LATEST = "latest";
        private static final String VERSION_ID = "versionId";
        private static final String TAGS = "tags";
        private static final String USER_METADATA = "userMetadata";

        static {
            final List<RecordField> fields = new ArrayList<>();
            fields.add(new RecordField(KEY, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(BUCKET, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(OWNER, RecordFieldType.STRING.getDataType(), true));
            fields.add(new RecordField(ETAG, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(LAST_MODIFIED, RecordFieldType.TIMESTAMP.getDataType(), false));
            fields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
            fields.add(new RecordField(STORAGE_CLASS, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(IS_LATEST, RecordFieldType.BOOLEAN.getDataType(), false));
            fields.add(new RecordField(VERSION_ID, RecordFieldType.STRING.getDataType(), true));
            fields.add(new RecordField(TAGS, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), true));
            fields.add(new RecordField(USER_METADATA, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), true));

            RECORD_SCHEMA = new SimpleRecordSchema(fields);
        }

        private final ProcessSession session;
        private final RecordSetWriterFactory writerFactory;
        private final ComponentLog logger;
        private final String region;
        private final FlowFile parent;
        private final List<FlowFile> children;
        private RecordSetWriter recordWriter;
        private FlowFile flowFile;

        public RecordObjectWriter(final ProcessSession session, final RecordSetWriterFactory writerFactory, final ComponentLog logger, final String region,
                                  final FlowFile parent, final List<FlowFile> children) {
            this.session = session;
            this.writerFactory = writerFactory;
            this.logger = logger;
            this.region = region;
            this.parent = parent;
            this.children = children;
        }

        @Override
        public void beginListing() throws IOException, SchemaNotFoundException {
            flowFile = session.create(parent);

            final OutputStream out = session.write(flowFile);
            recordWriter = writerFactory.createWriter(logger, RECORD_SCHEMA, out, flowFile);
            recordWriter.beginRecordSet();
        }

        @Override
        public void addToListing(final ObjectVersion objectVersion, final Map<String, String> tagging, final Map<String, String> userMetadata, String region, String bucket) throws IOException {
            recordWriter.write(createRecordForListing(objectVersion, tagging, userMetadata, bucket));
        }

        @Override
        public void finishListing() throws IOException {
            final WriteResult writeResult = recordWriter.finishRecordSet();
            recordWriter.close();

            if (writeResult.getRecordCount() == 0) {
                session.remove(flowFile);
            } else {
                final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put("s3.region", region);
                flowFile = session.putAllAttributes(flowFile, attributes);

                children.add(flowFile);
            }
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
            try {
                recordWriter.close();
            } catch (IOException e) {
                logger.error("Failed to write listing as Records", e);
            }

            session.remove(flowFile);
        }

        private Record createRecordForListing(final ObjectVersion objectVersion, final Map<String, String> tagging, final Map<String, String> userMetadata, String bucket) {
            final Map<String, Object> values = new HashMap<>();
            values.put(KEY, objectVersion.key());
            values.put(BUCKET, bucket);

            if (objectVersion.owner() != null) { // We may not have permission to read the owner
                values.put(OWNER, objectVersion.owner().id());
            }

            values.put(ETAG, sanitizeETag(objectVersion.eTag()));
            values.put(LAST_MODIFIED, String.valueOf(objectVersion.lastModified().toEpochMilli()));
            values.put(SIZE, objectVersion.size());
            values.put(STORAGE_CLASS, objectVersion.storageClassAsString());
            values.put(IS_LATEST, objectVersion.isLatest());
            final String versionId = objectVersion.versionId();
            if (versionId != null && !versionId.equals(NULL_VERSION_ID)) {
                values.put(VERSION_ID, versionId);
            }

            if (tagging != null) {
                values.put(TAGS, tagging);
            }

            if (userMetadata != null) {
                values.put(USER_METADATA, userMetadata);
            }

            return new MapRecord(RECORD_SCHEMA, values);
        }
    }

    static class AttributeObjectWriter implements S3ObjectWriter {
        private final ProcessSession session;
        private final FlowFile parent;
        private final List<FlowFile> children;

        public AttributeObjectWriter(final ProcessSession session, final FlowFile parent, final List<FlowFile> children) {
            this.session = session;
            this.parent = parent;
            this.children = children;
        }

        @Override
        public void beginListing() {
        }

        @Override
        public void addToListing(final ObjectVersion objectVersion, final Map<String, String> tagging, final Map<String, String> userMetadata, final String region, String bucket) {
            // Create the attributes
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), objectVersion.key());
            attributes.put("s3.bucket", bucket);
            attributes.put("s3.region", region);

            if (objectVersion.owner() != null) { // We may not have permission to read the owner
                attributes.put("s3.owner", objectVersion.owner().id());
            }

            attributes.put("s3.etag", sanitizeETag(objectVersion.eTag()));
            attributes.put("s3.lastModified", String.valueOf(objectVersion.lastModified().toEpochMilli()));
            attributes.put("s3.length", String.valueOf(objectVersion.size()));
            attributes.put("s3.storeClass", objectVersion.storageClassAsString());
            attributes.put("s3.isLatest", String.valueOf(objectVersion.isLatest()));

            final String versionId = objectVersion.versionId();
            if (versionId != null && !versionId.equals(NULL_VERSION_ID)) {
                attributes.put("s3.version", versionId);
            }

            if (tagging != null) {
                tagging.forEach((key, value) -> attributes.put("s3.tag." + key, value));
            }

            if (userMetadata != null) {
                userMetadata.forEach((key, value) -> attributes.put("s3.user.metadata." + key, value));
            }

            // Create the flowfile
            FlowFile flowFile = session.create(parent);
            children.add(flowFile);
            flowFile = session.putAllAttributes(flowFile, attributes);
            children.set(children.size() - 1, flowFile);
        }

        @Override
        public void finishListing() {
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
        }

    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext processContext, final ComponentLog logger, final Map<String, String> attributes) {
        // ARGUS: 검증 화면에서 입력한 FlowFile 속성으로 대상 속성을 평가한다.
        final ProcessContext context = FlowFileBoundProcessContext.bind(processContext, attributes);
        final S3Client client = createClient(context, attributes);

        final List<ConfigVerificationResult> results = new ArrayList<>(super.verify(context, logger, attributes));
        final String bucketName = context.getProperty(BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions(attributes).getValue();

        if (bucketName == null || bucketName.isBlank()) {
            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Perform Listing")
                .outcome(Outcome.FAILED)
                .explanation("Bucket Name must be specified")
                .build());

            return results;
        }

        // Attempt to perform a listing of objects in the S3 bucket
        try {
            final S3BucketLister bucketLister = getS3BucketLister(context, client);
            int totalItems = 0;
            int totalMatchingItems = 0;
            do {
                final List<ObjectVersion> objectVersionList = bucketLister.listVersions();
                final long currentTime = System.currentTimeMillis();
                for (final ObjectVersion objectVersion : objectVersionList) {
                    totalItems++;
                    if (includeObjectInListing(objectVersion, currentTime)) {
                        totalMatchingItems++;
                    }
                }
                bucketLister.setNextMarker();
            } while (bucketLister.isTruncated());

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Perform Listing")
                .outcome(Outcome.SUCCESSFUL)
                .explanation("Successfully listed contents of bucket '" + bucketName + "', finding " + totalItems + " total object(s). "
                        + totalMatchingItems + " objects matched the filter.")
                .build());

            logger.info("Successfully verified configuration");
        } catch (final Exception e) {
            logger.warn("Failed to verify configuration. Could not list contents of bucket '{}'", bucketName, e);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Perform Listing")
                .outcome(Outcome.FAILED)
                .explanation("Failed to list contents of bucket '" + bucketName + "': " + e.getMessage())
                .build());
        }

        return results;
    }

    /**
     * Return whether to include the entity in the listing, based on the minimum and maximum object age (if configured).
     */
    private boolean includeObjectInListing(final ObjectVersion objectVersion, final long currentTimeMillis) {
        final long lastModifiedTime = objectVersion.lastModified().toEpochMilli();

        return (minObjectAgeMilliseconds == null || currentTimeMillis >= lastModifiedTime + minObjectAgeMilliseconds)
                && (maxObjectAgeMilliseconds == null || currentTimeMillis <= lastModifiedTime + maxObjectAgeMilliseconds);
    }

    private static ObjectVersion s3ObjectToObjectVersion(final S3Object s3Object) {
        return ObjectVersion.builder()
                .eTag(s3Object.eTag())
                .key(s3Object.key())
                .lastModified(s3Object.lastModified())
                .owner(s3Object.owner())
                .size(s3Object.size())
                .storageClass(s3Object.storageClassAsString())
                .isLatest(true)
                .build();
    }

}
