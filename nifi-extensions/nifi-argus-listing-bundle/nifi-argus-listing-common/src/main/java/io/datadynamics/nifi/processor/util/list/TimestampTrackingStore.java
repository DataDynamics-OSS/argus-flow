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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 'Tracking Timestamps' 상태를 컴포넌트 CLUSTER 상태에 대상별 키로 저장한다.
 *
 * <pre>
 *   t.&lt;targetKey&gt;.ts     최신 타임스탬프
 *   t.&lt;targetKey&gt;.seen   마지막 갱신 시각 (대상 수 상한 초과 시 오래된 대상부터 제거)
 *   t.&lt;targetKey&gt;.id.N   최신 타임스탬프의 식별자
 * </pre>
 *
 * <p>트리거 모드는 모든 노드가 동시에 실행되므로, 맵 전체를 덮어쓰는 {@code setState} 로는 다른 노드가 쓴
 * 대상 키가 유실된다. 그래서 {@link StateManager#replace} (상태 공급자 수준의 CAS) 로 저장하고, 충돌하면
 * 최신 상태를 다시 읽어 {@link TimestampTrackingState#merge 병합} 후 재시도한다.
 * ({@code ProcessSession#replaceState} 는 세션 안에서만 비교하고 커밋 시 {@code setState} 로 덮어써
 * 노드 간 원자성을 보장하지 않는다.)</p>
 */
public final class TimestampTrackingStore {

    static final String PREFIX = "t.";
    static final String TIMESTAMP_SUFFIX = ".ts";
    static final String SEEN_SUFFIX = TrackedTargets.SEEN_SUFFIX;
    static final String IDENTIFIER_INFIX = ".id.";

    private TimestampTrackingStore() {
    }

    public static TimestampTrackingState read(final StateMap stateMap, final ListingTarget target) {
        final Map<String, String> state = stateMap.toMap();
        final String prefix = PREFIX + target.key();
        final String timestamp = state.get(prefix + TIMESTAMP_SUFFIX);
        if (timestamp == null) {
            return TimestampTrackingState.EMPTY;
        }

        final Set<String> identifiers = new HashSet<>();
        final String identifierPrefix = prefix + IDENTIFIER_INFIX;
        for (final Map.Entry<String, String> entry : state.entrySet()) {
            if (entry.getKey().startsWith(identifierPrefix)) {
                identifiers.add(entry.getValue());
            }
        }
        return new TimestampTrackingState(Long.parseLong(timestamp), identifiers);
    }

    /**
     * 대상의 상태를 저장한다. 다른 태스크가 먼저 저장했으면 병합해 다시 시도한다.
     *
     * @param maxTrackedTargets 상태에 유지할 대상 수 상한
     * @return 실제로 저장된 (병합된) 상태
     */
    public static TimestampTrackingState save(final StateManager stateManager, final ListingTarget target,
                                              final TimestampTrackingState state, final int maxTrackedTargets,
                                              final long now) throws IOException {
        for (int attempt = 0; attempt < TrackedTargets.MAX_REPLACE_ATTEMPTS; attempt++) {
            final StateMap current = stateManager.getState(Scope.CLUSTER);
            final TimestampTrackingState merged = read(current, target).merge(state);

            final Map<String, String> updated = new HashMap<>(current.toMap());
            TrackedTargets.remove(updated, PREFIX, target.key());
            final String prefix = PREFIX + target.key();
            updated.put(prefix + TIMESTAMP_SUFFIX, String.valueOf(merged.timestamp()));
            updated.put(prefix + SEEN_SUFFIX, String.valueOf(now));
            int i = 0;
            for (final String identifier : merged.identifiers()) {
                updated.put(prefix + IDENTIFIER_INFIX + i++, identifier);
            }
            TrackedTargets.evictOldest(updated, PREFIX, maxTrackedTargets, target.key());

            if (stateManager.replace(current, updated, Scope.CLUSTER)) {
                return merged;
            }
            TrackedTargets.backoff(attempt);
        }
        throw new IOException("Failed to update tracking state for [" + target + "] after " + TrackedTargets.MAX_REPLACE_ATTEMPTS + " attempts due to concurrent updates");
    }
}
