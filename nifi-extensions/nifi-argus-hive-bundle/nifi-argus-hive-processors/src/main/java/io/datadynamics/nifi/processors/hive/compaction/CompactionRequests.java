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
package io.datadynamics.nifi.processors.hive.compaction;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code ALTER TABLE ... COMPACT} 요청이 만든(또는 이어받은) compaction id 를 찾는다.
 * <p>
 * {@code ALTER TABLE ... COMPACT} 는 Hive 3.1·4.0 모두 결과 집합 없이 끝나며 id 를 돌려주지 않는다. 그래서 요청 직전
 * {@code SHOW COMPACTIONS} 에서 대상의 최대 id 를 기록하고, 요청 뒤 그보다 큰 id 를 찾는다. 같은 대상에 이미
 * {@code initiated}/{@code working} 요청이 있으면 Hive 는 새 요청을 만들지 않는다.
 * <ul>
 *     <li>Hive 3.1: 오류 없이 조용히 무시한다 → 대상의 진행 중 compaction 을 이어서 추적한다.</li>
 *     <li>Hive 4.0: SQLException(code 10432) "Compaction is already scheduled with state='initiated' and id=N" →
 *     메시지의 id 를 추적한다.</li>
 * </ul>
 */
public final class CompactionRequests {

    private static final Pattern ALREADY_SCHEDULED = Pattern.compile(
            "already\\s+(?:scheduled|enqueued|initiated|exists).*?\\bid\\s*[=:]?\\s*(\\d+)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private CompactionRequests() {
    }

    /**
     * 대상의 최대 compaction id. 없으면 0.
     */
    public static long maxId(final List<CompactionRecord> records, final String db, final String table, final PartitionSpec spec) {
        return records.stream().filter(r -> r.isFor(db, table, spec)).mapToLong(CompactionRecord::id).max().orElse(0L);
    }

    /**
     * 요청 뒤 목록에서 이 요청에 해당하는 compaction 을 고른다.
     * <ol>
     *     <li>대상의 id 가 {@code maxIdBefore} 보다 큰 것 중 가장 작은 것(요청 뒤 처음 생긴 것)</li>
     *     <li>없으면 대상의 {@code initiated}/{@code working} compaction 중 가장 큰 id(중복 요청으로 무시된 경우)</li>
     * </ol>
     */
    public static Optional<CompactionRecord> findRequested(final List<CompactionRecord> after, final long maxIdBefore,
                                                           final String db, final String table, final PartitionSpec spec) {
        final Optional<CompactionRecord> created = after.stream()
                .filter(r -> r.isFor(db, table, spec) && r.id() > maxIdBefore)
                .min(Comparator.comparingLong(CompactionRecord::id));
        if (created.isPresent()) {
            return created;
        }
        return after.stream()
                .filter(r -> r.isFor(db, table, spec))
                .filter(r -> {
                    final CompactionPhase phase = CompactionPhase.of(r.state());
                    return phase == CompactionPhase.INITIATED || phase == CompactionPhase.WORKING;
                })
                .max(Comparator.comparingLong(CompactionRecord::id));
    }

    /**
     * 같은 대상에 이미 요청이 있어 거부됐다는 오류 메시지(Hive 4)에서 기존 id 를 꺼낸다.
     */
    public static OptionalLong parseAlreadyScheduledId(final String message) {
        if (message == null) {
            return OptionalLong.empty();
        }
        final Matcher matcher = ALREADY_SCHEDULED.matcher(message);
        return matcher.find() ? OptionalLong.of(Long.parseLong(matcher.group(1))) : OptionalLong.empty();
    }

    /**
     * id 로 찾는다.
     */
    public static Optional<CompactionRecord> findById(final List<CompactionRecord> records, final long id) {
        return records.stream().filter(r -> r.id() == id).findFirst();
    }
}
