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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 컴포넌트 상태 맵 안의 대상별 키 묶음({@code <prefix><targetKey>.*})을 다루는 공통 처리.
 * 각 대상은 {@code <prefix><targetKey>.seen} (마지막 갱신 시각) 키를 가진다.
 */
final class TrackedTargets {

    static final String SEEN_SUFFIX = ".seen";

    /**
     * 상태 공급자 CAS 재시도 횟수. 모든 노드·태스크가 같은 CLUSTER 상태 맵 하나를 갱신하므로 경합이 생긴다.
     * 재시도마다 {@link #backoff} 로 지터를 둬, 경합하는 태스크들이 같은 박자로 계속 충돌하지 않게 한다.
     */
    static final int MAX_REPLACE_ATTEMPTS = 50;

    private static final long MAX_BACKOFF_MILLIS = 100;

    private TrackedTargets() {
    }

    /**
     * CAS 가 실패한 뒤 다시 시도하기 전에 기다린다 — 지터가 있는 지수 백오프 (최대 {@value #MAX_BACKOFF_MILLIS}ms).
     */
    static void backoff(final int attempt) throws IOException {
        final long ceiling = Math.min(MAX_BACKOFF_MILLIS, 1L << Math.min(attempt, 10));
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(ceiling + 1));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while retrying tracking state update", e);
        }
    }

    static void remove(final Map<String, String> state, final String prefix, final String targetKey) {
        final String keyPrefix = prefix + targetKey + ".";
        state.keySet().removeIf(key -> key.startsWith(keyPrefix));
    }

    static Set<String> targetKeys(final Map<String, String> state, final String prefix) {
        return lastSeen(state, prefix).keySet();
    }

    /**
     * 대상 수가 {@code maxTargets} 를 넘으면 가장 오래 갱신되지 않은 대상부터 지운다.
     *
     * @return 지운 대상 키
     */
    static List<String> evictOldest(final Map<String, String> state, final String prefix, final int maxTargets, final String keepTargetKey) {
        final Map<String, Long> seen = lastSeen(state, prefix);
        final int excess = seen.size() - maxTargets;
        if (excess <= 0) {
            return List.of();
        }
        final List<String> oldest = seen.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(keepTargetKey))
                .sorted(Map.Entry.comparingByValue(Comparator.naturalOrder()))
                .limit(excess)
                .map(Map.Entry::getKey)
                .toList();
        oldest.forEach(targetKey -> remove(state, prefix, targetKey));
        return oldest;
    }

    private static Map<String, Long> lastSeen(final Map<String, String> state, final String prefix) {
        final Map<String, Long> seen = new HashMap<>();
        for (final Map.Entry<String, String> entry : state.entrySet()) {
            final String key = entry.getKey();
            if (key.startsWith(prefix) && key.endsWith(SEEN_SUFFIX)) {
                seen.put(key.substring(prefix.length(), key.length() - SEEN_SUFFIX.length()), Long.parseLong(entry.getValue()));
            }
        }
        return seen;
    }
}
