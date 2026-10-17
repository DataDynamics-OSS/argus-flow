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

import io.datadynamics.nifi.processor.util.list.TestAbstractTriggeredListProcessor.InMemoryListProcessor;
import io.datadynamics.nifi.processor.util.list.TestAbstractTriggeredListProcessor.TestEntity;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.util.list.ListedEntity;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 추적 상태의 장애·동시성·초기화 경계 시나리오.
 */
class TestTriggeredListingState {

    private InMemoryListProcessor processor;
    private TestRunner runner;

    @BeforeEach
    void setUp() {
        processor = new InMemoryListProcessor();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(InMemoryListProcessor.DIRECTORY, "${dir}");
    }

    private void enqueue(final String directory) {
        runner.enqueue(new byte[0], Map.of("dir", directory));
    }

    private String targetKey(final String directory) {
        return ListingTarget.of("Dir [" + directory + "]", runner.getProcessContext(), List.of(InMemoryListProcessor.NAME_FILTER)).key();
    }

    private Map<String, String> clusterState() throws IOException {
        return runner.getStateManager().getState(Scope.CLUSTER).toMap();
    }

    private List<String> successNames() {
        return runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).stream().map(f -> f.getAttribute("filename")).sorted().toList();
    }

    // ---------------------------------------------------------------- Tracking Timestamps

    @Test
    void concurrentTasksDoNotLoseOtherTargetsState() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        final int targets = 40;
        for (int i = 0; i < targets; i++) {
            processor.put("d" + i, new TestEntity("f" + i, 1000L + i, 1L));
            enqueue("d" + i);
        }
        runner.setThreadCount(8);

        runner.run(targets);

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, targets);
        final Map<String, String> state = clusterState();
        for (int i = 0; i < targets; i++) {
            assertEquals(String.valueOf(1000L + i), state.get(TimestampTrackingStore.PREFIX + targetKey("d" + i) + TimestampTrackingStore.TIMESTAMP_SUFFIX),
                    "state of d" + i + " lost by concurrent replace");
        }
    }

    @Test
    void stateReadFailureRoutesInputToFailure() {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        processor.put("a", new TestEntity("f1", 100L, 1L));
        runner.getStateManager().setFailOnStateGet(Scope.CLUSTER, true);
        enqueue("a");

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
    }

    @Test
    void stateSaveFailureStillDeliversAndRelistsNextTime() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        processor.put("a", new TestEntity("f1", 100L, 1L));
        runner.getStateManager().setFailOnStateSet(Scope.CLUSTER, true);

        enqueue("a");
        runner.run();
        // 결과는 이미 커밋됐다 (at-least-once): 상태 저장 실패는 로그로만 남는다
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        runner.assertTransferCount(TriggeredListing.REL_ORIGINAL, 1);
        assertTrue(clusterState().isEmpty());

        runner.getStateManager().setFailOnStateSet(Scope.CLUSTER, false);
        runner.clearTransferState();
        enqueue("a");
        runner.run();
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
    }

    @Test
    void failedListingDoesNotAdvanceState() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        enqueue(InMemoryListProcessor.FAILING_DIRECTORY);

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
        assertTrue(clusterState().isEmpty());
    }

    @Test
    void listingOverMaximumSizeDoesNotAdvanceState() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        runner.setProperty(TriggeredListing.MAX_LISTING_SIZE, "1");
        processor.put("a", new TestEntity("f1", 100L, 1L), new TestEntity("f2", 200L, 1L));
        enqueue("a");
        runner.run();
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
        assertTrue(clusterState().isEmpty());

        // 상한을 올리면 전부 나온다 (Maximum Listing Size 는 결과 설정이 아니므로 상태 초기화 대상도 아니다)
        runner.setProperty(TriggeredListing.MAX_LISTING_SIZE, "10");
        runner.clearTransferState();
        enqueue("a");
        runner.run();
        assertEquals(List.of("f1", "f2"), successNames());
    }

    @Test
    void changingUnrelatedPropertyKeepsState() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        processor.put("a", new TestEntity("f1", 100L, 1L));
        enqueue("a");
        runner.run();
        final Map<String, String> before = clusterState();
        assertFalse(before.isEmpty());

        runner.setProperty(TriggeredListing.MAX_LISTING_SIZE, "5");
        runner.setProperty(TriggeredListing.MAX_TRACKED_TARGETS, "50");
        runner.clearTransferState();
        enqueue("a");
        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        assertEquals(before, clusterState());
    }

    @Test
    void changingTargetPropertyClearsState() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        processor.put("a", new TestEntity("f1", 100L, 1L));
        enqueue("a");
        runner.run();
        assertFalse(clusterState().isEmpty());

        runner.setProperty(InMemoryListProcessor.DIRECTORY, "${other.dir}");
        runner.clearTransferState();
        runner.run(1, true, true);

        assertTrue(clusterState().isEmpty());
    }

    @Test
    void perFlowFileFilterValuesKeepSeparateState() throws IOException {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        processor.put("a", new TestEntity("x1", 100L, 1L), new TestEntity("y1", 50L, 1L));
        runner.setProperty(InMemoryListProcessor.NAME_FILTER, "x.*");
        enqueue("a");
        runner.run();
        assertEquals(List.of("x1"), successNames());

        // 같은 디렉터리라도 필터가 다른 요청은 별도 대상이라, 더 오래된 y1 도 나온다
        runner.setProperty(InMemoryListProcessor.NAME_FILTER, "y.*");
        runner.clearTransferState();
        enqueue("a");
        runner.run();
        assertEquals(List.of("y1"), successNames());
    }

    // ---------------------------------------------------------------- Tracking Entities

    private MockMapCacheClient enableEntityTracking(final String window) throws Exception {
        final MockMapCacheClient cache = new MockMapCacheClient();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_ENTITIES);
        runner.setProperty(TriggeredListing.TRACKING_STATE_CACHE, "cache");
        runner.setProperty(TriggeredListing.TRACKING_TIME_WINDOW, window);
        return cache;
    }

    private Map<String, ListedEntity> cachedEntities(final MockMapCacheClient cache, final String directory) throws IOException {
        return new EntityTrackingStore(cache, processor.getIdentifier(), new ListingTarget("Dir [" + directory + "]", configuration())).fetch();
    }

    private String configuration() {
        return ListingTarget.of("x", runner.getProcessContext(), List.of(InMemoryListProcessor.NAME_FILTER)).configuration();
    }

    @Test
    void unchangedEntityListingWritesNothing() throws Exception {
        final MockMapCacheClient cache = enableEntityTracking("1 hour");
        processor.put("a", new TestEntity("f1", System.currentTimeMillis() - 1000, 1L));
        enqueue("a");
        runner.run();
        final StateMap stateAfterFirst = runner.getStateManager().getState(Scope.CLUSTER);
        final byte[] cachedAfterFirst = cache.entries.values().iterator().next();

        runner.clearTransferState();
        enqueue("a");
        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        assertEquals(stateAfterFirst.getStateVersion(), runner.getStateManager().getState(Scope.CLUSTER).getStateVersion(),
                "no component state write when nothing changed");
        assertTrue(cachedAfterFirst == cache.entries.values().iterator().next(), "no cache write when nothing changed");
    }

    @Test
    void entitiesOutsideTrackingWindowAreForgotten() throws Exception {
        final MockMapCacheClient cache = enableEntityTracking("1 hour");
        runner.setProperty(TriggeredListing.INITIAL_LISTING_TARGET, ListedEntityTracker.INITIAL_LISTING_TARGET_ALL);
        final long now = System.currentTimeMillis();
        processor.put("a", new TestEntity("old", now - TimeUnit.HOURS.toMillis(2), 1L), new TestEntity("new", now - 1000, 1L));

        enqueue("a");
        runner.run();
        // 최초 List 는 'All Available' 이라 창 밖의 old 도 나온다
        assertEquals(List.of("new", "old"), successNames());

        processor.put("a", new TestEntity("old", now - TimeUnit.HOURS.toMillis(2), 1L), new TestEntity("new", now - 1000, 1L),
                new TestEntity("newer", now - 500, 1L));
        runner.clearTransferState();
        enqueue("a");
        runner.run();

        assertEquals(List.of("newer"), successNames());
        assertEquals(Set.of("new", "newer"), cachedEntities(cache, "a").keySet(), "entity older than the window should be forgotten");
    }

    @Test
    void initialListingWithinWindowSkipsOldEntities() throws Exception {
        enableEntityTracking("1 hour");
        runner.setProperty(TriggeredListing.INITIAL_LISTING_TARGET, ListedEntityTracker.INITIAL_LISTING_TARGET_WINDOW);
        final long now = System.currentTimeMillis();
        processor.put("a", new TestEntity("old", now - TimeUnit.HOURS.toMillis(2), 1L), new TestEntity("new", now - 1000, 1L));
        enqueue("a");

        runner.run();

        assertEquals(List.of("new"), successNames());
    }

    @Test
    void resetWithRemovedCacheServiceStillClearsStateAndKeepsCacheEntries() throws Exception {
        final MockMapCacheClient cache = enableEntityTracking("1 hour");
        processor.put("a", new TestEntity("f1", System.currentTimeMillis() - 1000, 1L));
        enqueue("a");
        runner.run();
        assertEquals(1, cache.entries.size());

        // 전략을 바꾸면서 캐시 서비스도 흐름에서 지운 경우
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.NO_TRACKING);
        runner.removeProperty(TriggeredListing.TRACKING_STATE_CACHE);
        runner.disableControllerService(cache);
        runner.removeControllerService(cache);
        runner.clearTransferState();
        enqueue("a");
        runner.run();

        assertTrue(clusterState().isEmpty(), "state is cleared even if the cache is unavailable");
        assertEquals(1, cache.entries.size(), "cache entry is left when the cache service is gone");
        assertTrue(runner.getLogger().getWarnMessages().stream().anyMatch(m -> m.getMsg().contains("left in the cache")));
    }

    @Test
    void entityTrackingKeepsTargetsSeparate() throws Exception {
        final MockMapCacheClient cache = enableEntityTracking("1 hour");
        final long now = System.currentTimeMillis();
        processor.put("a", new TestEntity("same-name", now - 1000, 1L));
        processor.put("b", new TestEntity("same-name", now - 1000, 1L));

        enqueue("a");
        enqueue("b");
        runner.run(2);

        // 같은 식별자라도 대상이 다르면 각자 내보낸다
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 2);
        assertEquals(2, cache.entries.size());
        final Set<String> registered = EntityTrackingStore.registeredTargetKeys(runner.getStateManager().getState(Scope.CLUSTER));
        assertEquals(Set.of(targetKey("a"), targetKey("b")), registered);
    }

    @Test
    void noTrackingNeverTouchesState() throws IOException {
        processor.put("a", new TestEntity("f1", 100L, 1L));
        IntStream.range(0, 3).forEach(i -> enqueue("a"));

        runner.run(3);

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 3);
        assertTrue(clusterState().isEmpty());
        assertNull(runner.getStateManager().getState(Scope.CLUSTER).getStateVersion().orElse(null));
    }

    @SuppressWarnings("unused")
    private static List<String> names(final List<MockFlowFile> flowFiles) {
        return flowFiles.stream().map(f -> f.getAttribute("filename")).collect(Collectors.toCollection(ArrayList::new));
    }
}
