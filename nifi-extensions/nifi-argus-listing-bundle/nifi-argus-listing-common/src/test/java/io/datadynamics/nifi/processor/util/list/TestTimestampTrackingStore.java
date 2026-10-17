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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTimestampTrackingStore {

    private final ListingTarget targetA = new ListingTarget("A");
    private final ListingTarget targetB = new ListingTarget("B");

    @Test
    void concurrentWriteOfAnotherTargetIsPreserved() throws IOException {
        // 첫 replace 직전에 다른 노드가 대상 B 를 저장한 상황
        final ConflictingStateManager stateManager = new ConflictingStateManager(1, state ->
                state.put(TimestampTrackingStore.PREFIX + targetB.key() + TimestampTrackingStore.TIMESTAMP_SUFFIX, "500"));

        TimestampTrackingStore.save(stateManager, targetA, new TimestampTrackingState(100L, Set.of("a")), 10, 1L);

        assertEquals(2, stateManager.replaceAttempts.get());
        final StateMap state = stateManager.getState(Scope.CLUSTER);
        assertEquals(new TimestampTrackingState(100L, Set.of("a")), TimestampTrackingStore.read(state, targetA));
        assertEquals(500L, TimestampTrackingStore.read(state, targetB).timestamp());
    }

    @Test
    void concurrentWriteOfSameTargetIsMerged() throws IOException {
        // 다른 노드가 같은 대상 A 를 같은 시각의 다른 식별자로 먼저 저장한 상황
        final String prefix = TimestampTrackingStore.PREFIX + targetA.key();
        final ConflictingStateManager stateManager = new ConflictingStateManager(1, state -> {
            state.put(prefix + TimestampTrackingStore.TIMESTAMP_SUFFIX, "100");
            state.put(prefix + TimestampTrackingStore.IDENTIFIER_INFIX + "0", "other");
        });

        final TimestampTrackingState saved = TimestampTrackingStore.save(stateManager, targetA, new TimestampTrackingState(100L, Set.of("mine")), 10, 1L);

        assertEquals(new TimestampTrackingState(100L, Set.of("mine", "other")), saved);
        assertEquals(saved, TimestampTrackingStore.read(stateManager.getState(Scope.CLUSTER), targetA));
    }

    @Test
    void olderStateDoesNotOverwriteNewerConcurrentState() throws IOException {
        final String prefix = TimestampTrackingStore.PREFIX + targetA.key();
        final ConflictingStateManager stateManager = new ConflictingStateManager(1, state -> {
            state.put(prefix + TimestampTrackingStore.TIMESTAMP_SUFFIX, "900");
            state.put(prefix + TimestampTrackingStore.IDENTIFIER_INFIX + "0", "newer");
        });

        TimestampTrackingStore.save(stateManager, targetA, new TimestampTrackingState(100L, Set.of("older")), 10, 1L);

        assertEquals(new TimestampTrackingState(900L, Set.of("newer")), TimestampTrackingStore.read(stateManager.getState(Scope.CLUSTER), targetA));
    }

    @Test
    void giveUpAfterPersistentConflicts() {
        final ConflictingStateManager stateManager = new ConflictingStateManager(Integer.MAX_VALUE, state -> { });
        assertThrows(IOException.class, () -> TimestampTrackingStore.save(stateManager, targetA, new TimestampTrackingState(1L, Set.of()), 10, 1L));
    }

    @Test
    void readingUnknownTargetReturnsEmptyState() throws IOException {
        assertEquals(TimestampTrackingState.EMPTY, TimestampTrackingStore.read(new ConflictingStateManager(0, s -> { }).getState(Scope.CLUSTER), targetA));
    }

    @Test
    void entityRegistryEvictsLeastRecentlyRegisteredTarget() throws IOException {
        final ConflictingStateManager stateManager = new ConflictingStateManager(0, s -> { });
        assertTrue(EntityTrackingStore.register(stateManager, targetA, "cache-1", 1, 1L).isEmpty());
        assertEquals(List.of(targetA.key()), EntityTrackingStore.register(stateManager, targetB, "cache-1", 1, 2L));

        final StateMap state = stateManager.getState(Scope.CLUSTER);
        assertEquals(Set.of(targetB.key()), EntityTrackingStore.registeredTargetKeys(state));
        assertEquals("cache-1", EntityTrackingStore.registeredCacheServiceId(state));
    }

    /**
     * 처음 {@code conflicts} 번의 replace 직전에 다른 작성자가 상태를 바꾼 것처럼 동작하는 StateManager.
     */
    private static final class ConflictingStateManager implements StateManager {
        private final int conflicts;
        private final Consumer<Map<String, String>> concurrentWriter;
        private final AtomicInteger replaceAttempts = new AtomicInteger();
        private Map<String, String> state = new HashMap<>();
        private long version = 0;

        ConflictingStateManager(final int conflicts, final Consumer<Map<String, String>> concurrentWriter) {
            this.conflicts = conflicts;
            this.concurrentWriter = concurrentWriter;
        }

        @Override
        public synchronized void setState(final Map<String, String> newState, final Scope scope) {
            state = new HashMap<>(newState);
            version++;
        }

        @Override
        public synchronized StateMap getState(final Scope scope) {
            final Map<String, String> snapshot = Map.copyOf(state);
            final String snapshotVersion = String.valueOf(version);
            return new StateMap() {
                @Override
                public Optional<String> getStateVersion() {
                    return Optional.of(snapshotVersion);
                }

                @Override
                public String get(final String key) {
                    return snapshot.get(key);
                }

                @Override
                public Map<String, String> toMap() {
                    return snapshot;
                }
            };
        }

        @Override
        public synchronized boolean replace(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) {
            if (replaceAttempts.incrementAndGet() <= conflicts) {
                concurrentWriter.accept(state);
                version++;
            }
            if (!oldValue.getStateVersion().orElseThrow().equals(String.valueOf(version))) {
                return false;
            }
            setState(newValue, scope);
            return true;
        }

        @Override
        public synchronized void clear(final Scope scope) {
            setState(Map.of(), scope);
        }
    }
}
