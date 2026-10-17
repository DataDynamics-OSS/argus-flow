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

import org.apache.nifi.processor.util.list.ListableEntityWrapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTimestampTrackingState {

    private static ListableEntityWrapper<String> entity(final String id, final long timestamp) {
        return new ListableEntityWrapper<>(id, s -> s, s -> s, s -> timestamp, s -> 0L);
    }

    @Test
    void newEntitiesAreLaterOrUnseenAtLatestTimestamp() {
        final TimestampTrackingState state = new TimestampTrackingState(100L, Set.of("a"));
        assertTrue(state.isNew(101L, "a"));
        assertTrue(state.isNew(100L, "b"));
        assertFalse(state.isNew(100L, "a"));
        assertFalse(state.isNew(99L, "z"));
    }

    @Test
    void advanceKeepsIdentifiersOfLatestTimestampOnly() {
        final TimestampTrackingState state = new TimestampTrackingState(100L, Set.of("a"));
        assertEquals(new TimestampTrackingState(100L, Set.of("a", "b")), state.advance(List.of(entity("b", 100L), entity("c", 90L))));
        assertEquals(new TimestampTrackingState(200L, Set.of("d")), state.advance(List.of(entity("b", 100L), entity("d", 200L))));
        assertEquals(state, state.advance(List.of()));
    }

    @Test
    void mergeIsMonotonic() {
        final TimestampTrackingState older = new TimestampTrackingState(100L, Set.of("a"));
        final TimestampTrackingState newer = new TimestampTrackingState(200L, Set.of("b"));
        assertEquals(newer, older.merge(newer));
        assertEquals(newer, newer.merge(older));
        assertEquals(new TimestampTrackingState(100L, Set.of("a", "c")), older.merge(new TimestampTrackingState(100L, Set.of("c"))));
    }
}
