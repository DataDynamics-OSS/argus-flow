/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 2.10.0 (rel/nifi-2.10.0) for FlowFile-triggered listing
 * (nifi-argus-listing-bundle):
 *   nifi-extension-bundles/nifi-extension-utils/nifi-listed-entity/src/main/java/org/apache/nifi/processor/util/list/AbstractListProcessor.java
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

package io.datadynamics.nifi.processor.util.list;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * 입력 FlowFile 마다 대상 하나를 List 하는 프로세서의 베이스. 업스트림 {@code AbstractListProcessor} 의 포크다.
 * </p>
 * <p>
 * 업스트림은 타이머로 혼자 도는 Source 프로세서(INPUT_FORBIDDEN)로, 대상(경로·호스트 등)을 설정값으로만 정하고
 * 컴포넌트 상태 하나에 마지막 타임스탬프를 기억한다. 이 클래스는 다음이 다르다.
 * </p>
 * <ul>
 * <li>INPUT_REQUIRED — FlowFile 하나가 List 한 번이다. 대상 속성은 {@code FLOWFILE_ATTRIBUTES} 범위로 선언하고,
 *     하위 클래스에는 입력 FlowFile 에 묶인 컨텍스트({@link FlowFileBoundProcessContext})를 넘긴다.
 *     그래서 하위 클래스의 {@code evaluateAttributeExpressions()} 호출이 그대로 FlowFile 기준이 된다.</li>
 * <li>관계: success(결과, 입력 FlowFile 의 자식) / original(입력) / failure(실패한 입력).</li>
 * <li>추적은 대상별로 분리한다 ({@link TriggeredListingSupport}). 기본은 'No Tracking'.</li>
 * <li>{@code @TriggerSerially} 를 떼었다 — 추적 상태의 정합성은 상태 공급자 CAS 로 지킨다. 'Time Window' 전략,
 *     레거시 상태 마이그레이션, Primary Node 선출 처리, 타임스탬프 정밀도 지연(lag)은 트리거 모델에 의미가 없어 없앴다.</li>
 * </ul>
 * <p>
 * 하위 클래스가 구현할 것은 업스트림과 같다: {@link #performListing}, {@link #createAttributes}, {@link #getPath},
 * {@link #getRecordSchema}, {@link #countUnfilteredListing}, {@link #getListingContainerName}. 모두 입력 FlowFile 에
 * 묶인 컨텍스트를 받는다. {@link #getListingContainerName} 의 결과와 {@link #getTrackingKeyProperties} 의 값이 추적 상태를
 * 나누는 대상 키가 되므로, 같은 대상에는 같은 문자열을 돌려줘야 한다. 업스트림 isListingResetNecessary() 의 역할은
 * {@link #getTargetProperties} 와 {@link #getTrackingKeyProperties} 가 맡는다.
 * </p>
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Stateful(scopes = {Scope.CLUSTER}, description = "'Tracking Timestamps' 전략을 쓰면, 대상별로 내보낸 엔티티 중 최신 타임스탬프와 "
    + "그 시각의 식별자를 저장한다. 다른 전략에서는 상태를 쓰지 않는다.")
public abstract class AbstractTriggeredListProcessor<T extends ListableEntity> extends AbstractProcessor implements VerifiableProcessor {

    /**
     * Indicates the mode when performing a listing.
     */
    protected enum ListingMode {
        /**
         * Indicates the listing is being performed during normal processor execution.  May use configuration cached in the Processor object.
         */
        EXECUTION,
        /**
         * Indicates the listing is being performed during configuration verification.  Only use configuration provided in the ProcessContext argument, since the configuration may not
         * have been applied to the processor yet.
         */
        CONFIGURATION_VERIFICATION
    }

    private static final Long IGNORE_MIN_TIMESTAMP_VALUE = 0L;

    public static final Relationship REL_SUCCESS = TriggeredListing.REL_SUCCESS;
    public static final Relationship REL_ORIGINAL = TriggeredListing.REL_ORIGINAL;
    public static final Relationship REL_FAILURE = TriggeredListing.REL_FAILURE;

    public static final PropertyDescriptor LISTING_STRATEGY = TriggeredListing.LISTING_STRATEGY;
    public static final PropertyDescriptor RECORD_WRITER = TriggeredListing.RECORD_WRITER;

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.removeProperty("Distributed Cache Service");
        config.renameProperty("listing-strategy", LISTING_STRATEGY.getName());
        config.renameProperty("record-writer", RECORD_WRITER.getName());
    }

    private volatile boolean resetTracking = false;

    @Override
    public Set<Relationship> getRelationships() {
        return TriggeredListing.RELATIONSHIPS;
    }

    /**
     * ARGUS: 업스트림의 isListingResetNecessary() 에 해당. 대상·결과 설정이나 추적 전략이 바뀌면 다음 시작 시 추적 상태를 지운다.
     * (대상 키에 설정이 들어가 있어 바뀐 뒤의 List 는 이미 새 상태로 시작한다. 여기서는 더 이상 쓰이지 않는 상태와 캐시 항목을 치운다.)
     */
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isConfigurationRestored() && isTrackingResetNecessary(descriptor)) {
            resetTracking = true;
        }
    }

    protected boolean isTrackingResetNecessary(final PropertyDescriptor descriptor) {
        return LISTING_STRATEGY.equals(descriptor) || getTargetProperties().contains(descriptor) || getTrackingKeyProperties().contains(descriptor);
    }

    @OnScheduled
    public final void resetTrackingIfNecessary(final ProcessContext context) throws IOException {
        if (resetTracking) {
            TriggeredListingSupport.resetTracking(context, getIdentifier(), getLogger());
            resetTracking = false;
        }
    }

    /**
     * @return List 대상을 정하는 속성 (경로·호스트 등). 바뀌면 추적 상태를 지운다.
     */
    protected Collection<PropertyDescriptor> getTargetProperties() {
        return List.of();
    }

    /**
     * @return 대상 설명에는 없지만 결과에 영향을 주는 속성 (필터·재귀·나이·크기 등). 평가된 값이 대상 키에 들어가고, 바뀌면 추적 상태를 지운다.
     */
    protected Collection<PropertyDescriptor> getTrackingKeyProperties() {
        return List.of();
    }

    /**
     * In order to add custom validation at sub-classes, implement {@link #customValidate(ValidationContext, Collection)} method.
     */
    @Override
    protected final Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();
        customValidate(context, results);
        return results;
    }

    /**
     * Sub-classes can add custom validation by implementing this method.
     *
     * @param validationContext the validation context
     * @param validationResults add custom validation result to this collection
     */
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> validationResults) {

    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext processContext, final ComponentLog logger, final Map<String, String> attributes) {
        // ARGUS: 검증 화면에서 입력한 FlowFile 속성으로 대상 속성을 평가한다.
        final ProcessContext context = FlowFileBoundProcessContext.bind(processContext, attributes);

        final List<ConfigVerificationResult> results = new ArrayList<>();

        final String containerName = getListingContainerName(context);
        try {
            final Integer unfilteredListingCount = countUnfilteredListing(context);
            final int matchingCount = performListing(context, IGNORE_MIN_TIMESTAMP_VALUE, ListingMode.CONFIGURATION_VERIFICATION).size();

            final String countExplanation;
            if (unfilteredListingCount == null) {
                if (matchingCount == 0) {
                    countExplanation = "Found no objects matching the filter.";
                } else {
                    final String matchingCountText = matchingCount == 1 ? matchingCount + " object" : matchingCount + " objects";
                    countExplanation = String.format("Found %s matching the filter.", matchingCountText);
                }
            } else if (unfilteredListingCount == 0) {
                countExplanation = "Found no objects.";
            } else {
                final String unfilteredListingCountText = unfilteredListingCount == 1 ? unfilteredListingCount + " object" : unfilteredListingCount + " objects";
                final String unfilteredDemonstrativePronoun = unfilteredListingCount == 1 ? "that" : "those";
                final String matchingCountText = matchingCount == 1 ? matchingCount + " matches" : matchingCount + " match";
                countExplanation = String.format("Found %s.  Of %s, %s the filter.",
                        unfilteredListingCountText, unfilteredDemonstrativePronoun, matchingCountText);
            }

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Perform Listing")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation(String.format("Successfully listed contents of %s.  %s", containerName, countExplanation))
                    .build());

            logger.info("Successfully verified configuration");
        } catch (final IOException e) {
            logger.warn("Failed to verify configuration. Could not list contents of {}", containerName, e);

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Perform Listing")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to list contents of %s: %s", containerName, e.getMessage()))
                    .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessContext boundContext = FlowFileBoundProcessContext.bind(context, flowFile);
        TriggeredListingSupport.process(boundContext, session, flowFile, getIdentifier(), getLogger(),
                () -> ListingTarget.of(getListingContainerName(boundContext), boundContext, getTrackingKeyProperties()),
                minTimestamp -> performListing(boundContext, minTimestamp, ListingMode.EXECUTION),
                new StandardListingEmitter<T>(boundContext, getLogger(), getRecordSchema(), entity -> createAttributes(entity, boundContext)));
    }

    protected long getCurrentTime() {
        return System.currentTimeMillis();
    }

    /**
     * Creates a Map of attributes that should be applied to the FlowFile to represent this entity. The FlowFile is a child of the incoming FlowFile,
     * so it also inherits the attributes of the incoming FlowFile; the attributes returned here override them.
     *
     * @param entity  the entity represented by the FlowFile
     * @param context the ProcessContext bound to the incoming FlowFile
     * @return a Map of attributes for this entity
     */
    protected abstract Map<String, String> createAttributes(T entity, ProcessContext context);

    /**
     * Returns the path to perform a listing on.
     * Many resources can be comprised of a "path" (or a "container" or "bucket", etc.) as well as name or identifier that is unique only
     * within that path. This method is responsible for returning the path that is currently being polled for entities. If this concept
     * does not apply for the concrete implementation, it is recommended that the concrete implementation return "." or "/" for all invocations of this method.
     *
     * @param context the ProcessContext bound to the incoming FlowFile
     * @return the path that is to be used to perform the listing, or <code>null</code> if not applicable.
     */
    protected abstract String getPath(final ProcessContext context);

    /**
     * Performs a listing of the remote entities. Any entity that is returned by this method with a timestamp prior to the minTimestamp
     * will be filtered out by the tracking strategy. Therefore, it is not necessary that implementations perform this filtering but can be more efficient
     * if the filtering can be performed on the server side prior to retrieving the information.
     *
     * @param context      the ProcessContext bound to the incoming FlowFile
     * @param minTimestamp the minimum timestamp of entities that should be returned
     * @param listingMode  the listing mode, indicating whether the listing is being performed during configuration verification or normal processor execution
     * @return a Listing of entities that have a timestamp >= minTimestamp
     */
    protected abstract List<T> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode)
            throws IOException;

    /**
     * @return the RecordSchema that will be used for any Records that are produced by the Processor
     */
    protected abstract RecordSchema getRecordSchema();

    /**
     * Performs an unfiltered listing and returns the count, or null if this operation is not supported.
     *
     * @param context the ProcessContext to use in order to pull the appropriate entities
     * @return The number of unfiltered entities in the listing, or null if this processor does not support an unfiltered listing
     */
    protected abstract Integer countUnfilteredListing(final ProcessContext context)
            throws IOException;

    /**
     * Provides a human-readable name for the container being listed. It is also the identity of the listing target that separates tracking state,
     * so it must be stable for the same target.
     *
     * @param context The ProcessContext bound to the incoming FlowFile
     * @return The user-friendly name for the container
     */
    protected abstract String getListingContainerName(final ProcessContext context);
}
