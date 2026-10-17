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
package io.datadynamics.nifi.processor.util.list;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestAbstractTriggeredListProcessor {

    private InMemoryListProcessor processor;
    private TestRunner runner;

    @BeforeEach
    void setUp() {
        processor = new InMemoryListProcessor();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(InMemoryListProcessor.DIRECTORY, "${dir}");
    }

    private void enqueue(final String directory) {
        runner.enqueue(new byte[0], Map.of("dir", directory, "request.id", directory + "-req"));
    }

    @Test
    void noTrackingEmitsEveryEntityAsChildOfInputAndRoutesOriginal() {
        processor.put("a", new TestEntity("f1", 100L, 10L), new TestEntity("f2", 200L, 20L));
        enqueue("a");

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 2);
        runner.assertTransferCount(TriggeredListing.REL_ORIGINAL, 1);
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 0);
        for (final MockFlowFile child : runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS)) {
            child.assertAttributeEquals("request.id", "a-req");
            child.assertAttributeEquals(TriggeredListing.ATTR_TARGET, "Dir [a]");
            child.assertAttributeExists("filename");
        }
        final MockFlowFile original = runner.getFlowFilesForRelationship(TriggeredListing.REL_ORIGINAL).getFirst();
        original.assertAttributeEquals(TriggeredListing.ATTR_COUNT, "2");
        original.assertAttributeEquals(TriggeredListing.ATTR_TARGET, "Dir [a]");
    }

    @Test
    void noTrackingListsTheSameEntitiesAgain() {
        processor.put("a", new TestEntity("f1", 100L, 10L));
        enqueue("a");
        enqueue("a");

        runner.run(2);

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 2);
        runner.assertTransferCount(TriggeredListing.REL_ORIGINAL, 2);
    }

    @Test
    void emptyListingRoutesOriginalWithZeroCount() {
        enqueue("empty");

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_ORIGINAL).getFirst().assertAttributeEquals(TriggeredListing.ATTR_COUNT, "0");
    }

    @Test
    void trackingTimestampsListsOnlyNewEntitiesPerTarget() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        processor.put("a", new TestEntity("f1", 100L, 1L), new TestEntity("f2", 100L, 1L));

        enqueue("a");
        runner.run();
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 2);

        // 같은 최신 시각으로 늦게 들어온 f3 와 더 새로운 f4 만 나온다
        processor.put("a", new TestEntity("f1", 100L, 1L), new TestEntity("f2", 100L, 1L),
                new TestEntity("f3", 100L, 1L), new TestEntity("f4", 200L, 1L));
        runner.clearTransferState();
        enqueue("a");
        runner.run();
        assertEquals(List.of("f3", "f4"), successFilenames());

        runner.clearTransferState();
        enqueue("a");
        runner.run();
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_ORIGINAL).getFirst().assertAttributeEquals(TriggeredListing.ATTR_COUNT, "0");

        // 다른 대상은 상태가 따로다
        processor.put("b", new TestEntity("g1", 50L, 1L));
        runner.clearTransferState();
        enqueue("b");
        runner.run();
        assertEquals(List.of("g1"), successFilenames());

        final Map<String, String> state = runner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertEquals("200", state.get("t." + targetKey("Dir [a]") + ".ts"));
        assertEquals("50", state.get("t." + targetKey("Dir [b]") + ".ts"));
    }

    @Test
    void trackingTimestampsEvictsLeastRecentlyUpdatedTargets() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        runner.setProperty(TriggeredListing.MAX_TRACKED_TARGETS, "1");
        processor.put("a", new TestEntity("f1", 100L, 1L));
        processor.put("b", new TestEntity("g1", 100L, 1L));

        enqueue("a");
        runner.run();
        enqueue("b");
        runner.run();

        final Map<String, String> state = runner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertTrue(state.keySet().stream().noneMatch(key -> key.startsWith("t." + targetKey("Dir [a]"))), state.toString());
        assertTrue(state.containsKey("t." + targetKey("Dir [b]") + ".ts"), state.toString());
    }

    @Test
    void trackingEntitiesPicksNewChangedAndOldTimestampEntities() throws Exception {
        final MockMapCacheClient cache = new MockMapCacheClient();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_ENTITIES);
        runner.setProperty(TriggeredListing.TRACKING_STATE_CACHE, "cache");
        runner.setProperty(TriggeredListing.TRACKING_TIME_WINDOW, "100000 days");
        runner.setProperty(TriggeredListing.INITIAL_LISTING_TARGET, ListedEntityTracker.INITIAL_LISTING_TARGET_ALL);

        final long now = System.currentTimeMillis();
        processor.put("a", new TestEntity("f1", now - 1000, 10L), new TestEntity("f2", now - 1000, 20L));
        enqueue("a");
        runner.run();
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 2);
        assertTrue(cache.entries.containsKey(cacheKey("Dir [a]")));

        // f1 크기 변경, f3 는 기존보다 오래된 타임스탬프 — Tracking Timestamps 로는 못 잡는 경우
        processor.put("a", new TestEntity("f1", now - 1000, 11L), new TestEntity("f2", now - 1000, 20L), new TestEntity("f3", now - 5000, 30L));
        runner.clearTransferState();
        enqueue("a");
        runner.run();
        assertEquals(List.of("f1", "f3"), successFilenames());
    }

    @Test
    void listingFailureRoutesInputToFailureWithoutChildren() {
        enqueue(InMemoryListProcessor.FAILING_DIRECTORY);

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.assertTransferCount(TriggeredListing.REL_ORIGINAL, 0);
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(TriggeredListing.REL_FAILURE).getFirst();
        failed.assertAttributeEquals(TriggeredListing.ATTR_ERROR_TYPE, IOException.class.getName());
        failed.assertAttributeEquals(TriggeredListing.ATTR_ERROR_MESSAGE, "simulated listing failure");
        failed.assertAttributeEquals(TriggeredListing.ATTR_TARGET, "Dir [" + InMemoryListProcessor.FAILING_DIRECTORY + "]");
    }

    @Test
    void listingLargerThanMaximumRoutesFailure() {
        runner.setProperty(TriggeredListing.MAX_LISTING_SIZE, "1");
        processor.put("a", new TestEntity("f1", 100L, 1L), new TestEntity("f2", 100L, 1L));
        enqueue("a");

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
    }

    @Test
    void recordWriterWritesOneChildPerInput() throws Exception {
        final MockRecordWriter writer = new MockRecordWriter("name,timestamp");
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(TriggeredListing.RECORD_WRITER, "writer");
        processor.put("a", new TestEntity("f1", 100L, 1L), new TestEntity("f2", 200L, 1L));
        enqueue("a");

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        final MockFlowFile child = runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst();
        child.assertAttributeEquals("record.count", "2");
        child.assertAttributeEquals("request.id", "a-req");
    }

    @Test
    void abortedListingReturnsInputToQueue() {
        enqueue(InMemoryListProcessor.ABORTING_DIRECTORY);

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.assertTransferCount(TriggeredListing.REL_ORIGINAL, 0);
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 0);
        assertEquals(1, runner.getQueueSize().getObjectCount());
    }

    @Test
    void verifyEvaluatesTargetWithGivenAttributes() {
        processor.put("v", new TestEntity("f1", 100L, 1L));
        final var results = processor.verify(runner.getProcessContext(), runner.getLogger(), Map.of("dir", "v"));
        assertEquals(1, results.size());
        assertTrue(results.getFirst().getExplanation().contains("Dir [v]"), results.getFirst().getExplanation());
    }

    @Test
    void changingResultAffectingSettingListsTheTargetAgainAndClearsOldState() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        runner.setProperty(InMemoryListProcessor.NAME_FILTER, "a.*");
        processor.put("d", new TestEntity("a1", 100L, 1L), new TestEntity("b1", 50L, 1L));
        enqueue("d");
        runner.run();
        assertEquals(List.of("a1"), successFilenames());
        final String oldKey = TimestampTrackingStore.PREFIX + ListingTarget.of("Dir [d]", runner.getProcessContext(),
                List.of(InMemoryListProcessor.NAME_FILTER)).key();
        assertTrue(runner.getStateManager().getState(Scope.CLUSTER).toMap().containsKey(oldKey + ".ts"));

        // 필터를 넓히면 최신 타임스탬프(100)보다 오래된 b1 도 나와야 한다 — 같은 상태를 쓰면 영영 누락된다
        runner.setProperty(InMemoryListProcessor.NAME_FILTER, ".*");
        runner.clearTransferState();
        enqueue("d");
        runner.run();

        assertEquals(List.of("a1", "b1"), successFilenames());
        assertTrue(runner.getStateManager().getState(Scope.CLUSTER).toMap().keySet().stream().noneMatch(key -> key.startsWith(oldKey)),
                "old target state should be cleared on configuration change");
    }

    @Test
    void trackingEntitiesEvictsCacheEntriesOfLeastRecentlyUpdatedTargets() throws Exception {
        final MockMapCacheClient cache = enableEntityTracking();
        runner.setProperty(TriggeredListing.MAX_TRACKED_TARGETS, "1");
        final long now = System.currentTimeMillis();
        processor.put("a", new TestEntity("f1", now - 1000, 1L));
        processor.put("b", new TestEntity("g1", now - 1000, 1L));

        enqueue("a");
        runner.run();
        enqueue("b");
        runner.run();

        assertEquals(Set.of(cacheKey("Dir [b]")), cache.entries.keySet());
        assertEquals(Set.of(targetKey("Dir [b]")),
                EntityTrackingStore.registeredTargetKeys(runner.getStateManager().getState(Scope.CLUSTER)));
    }

    @Test
    void changingListingStrategyRemovesCachedEntitiesAndState() throws Exception {
        final MockMapCacheClient cache = enableEntityTracking();
        processor.put("a", new TestEntity("f1", System.currentTimeMillis() - 1000, 1L));
        enqueue("a");
        runner.run();
        assertEquals(1, cache.entries.size());

        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        enqueue("a");
        runner.run();

        assertTrue(cache.entries.isEmpty(), cache.entries.keySet().toString());
        assertTrue(EntityTrackingStore.registeredTargetKeys(runner.getStateManager().getState(Scope.CLUSTER)).isEmpty());
    }

    private MockMapCacheClient enableEntityTracking() throws Exception {
        final MockMapCacheClient cache = new MockMapCacheClient();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_ENTITIES);
        runner.setProperty(TriggeredListing.TRACKING_STATE_CACHE, "cache");
        runner.setProperty(TriggeredListing.TRACKING_TIME_WINDOW, "100000 days");
        return cache;
    }

    private String targetKey(final String description) {
        return ListingTarget.of(description, runner.getProcessContext(), List.of(InMemoryListProcessor.NAME_FILTER)).key();
    }

    private String cacheKey(final String description) {
        return EntityTrackingStore.CACHE_KEY_PREFIX + "::" + processor.getIdentifier() + "::" + targetKey(description);
    }

    private List<String> successFilenames() {
        final List<String> names = new ArrayList<>();
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS)) {
            names.add(flowFile.getAttribute("filename"));
        }
        names.sort(String::compareTo);
        return names;
    }

    record TestEntity(String name, long timestamp, long size) implements ListableEntity {
        static final RecordSchema SCHEMA = new SimpleRecordSchema(List.of(
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("timestamp", RecordFieldType.LONG.getDataType())));

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getIdentifier() {
            return name;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public long getSize() {
            return size;
        }

        @Override
        public Record toRecord() {
            return new MapRecord(SCHEMA, Map.of("name", name, "timestamp", timestamp));
        }
    }

    static class InMemoryListProcessor extends AbstractTriggeredListProcessor<TestEntity> {
        static final String FAILING_DIRECTORY = "failing";
        static final String ABORTING_DIRECTORY = "aborting";

        static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
                .name("Directory")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();

        static final PropertyDescriptor NAME_FILTER = new PropertyDescriptor.Builder()
                .name("Name Filter")
                .required(true)
                .defaultValue(".*")
                .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
                .build();

        private final Map<String, List<TestEntity>> directories = new ConcurrentHashMap<>();

        void put(final String directory, final TestEntity... entities) {
            directories.put(directory, List.of(entities));
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of(DIRECTORY, NAME_FILTER, TriggeredListing.LISTING_STRATEGY, TriggeredListing.MAX_TRACKED_TARGETS, TriggeredListing.TRACKING_STATE_CACHE,
                    TriggeredListing.TRACKING_TIME_WINDOW, TriggeredListing.INITIAL_LISTING_TARGET, TriggeredListing.RECORD_WRITER,
                    TriggeredListing.MAX_LISTING_SIZE);
        }

        @Override
        protected Map<String, String> createAttributes(final TestEntity entity, final ProcessContext context) {
            return Map.of("filename", entity.name(), "dir.listed", getPath(context));
        }

        @Override
        protected String getPath(final ProcessContext context) {
            return context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        }

        @Override
        protected List<TestEntity> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode) throws IOException {
            final String directory = getPath(context);
            if (FAILING_DIRECTORY.equals(directory)) {
                throw new IOException("simulated listing failure");
            }
            if (ABORTING_DIRECTORY.equals(directory)) {
                throw new ListingAbortedException("simulated stop");
            }
            final String nameFilter = context.getProperty(NAME_FILTER).getValue();
            return directories.getOrDefault(directory, List.of()).stream()
                    .filter(entity -> minTimestamp == null || entity.getTimestamp() >= minTimestamp)
                    .filter(entity -> entity.name().matches(nameFilter))
                    .toList();
        }

        @Override
        protected RecordSchema getRecordSchema() {
            return TestEntity.SCHEMA;
        }

        @Override
        protected Collection<PropertyDescriptor> getTargetProperties() {
            return List.of(DIRECTORY);
        }

        @Override
        protected Collection<PropertyDescriptor> getTrackingKeyProperties() {
            return List.of(NAME_FILTER);
        }

        @Override
        protected Integer countUnfilteredListing(final ProcessContext context) {
            return null;
        }

        @Override
        protected String getListingContainerName(final ProcessContext context) {
            return "Dir [" + getPath(context) + "]";
        }
    }
}
