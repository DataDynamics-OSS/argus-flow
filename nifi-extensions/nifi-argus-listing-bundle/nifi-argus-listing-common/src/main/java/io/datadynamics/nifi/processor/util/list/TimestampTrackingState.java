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

import org.apache.nifi.processor.util.list.ListableEntity;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * 'Tracking Timestamps' 전략의 대상별 상태: 지금까지 내보낸 가장 최신 타임스탬프와, 그 시각을 가진 식별자 집합.
 *
 * <p>업스트림 세 프로세서의 상태 모델(AbstractListProcessor 의 {@code processed.timestamp + id.N},
 * ListS3 의 {@code ListingSnapshot}, ListHDFS 의 {@code FileStatusManager})이 모두 이 형태라 하나로 합쳤다.
 * 식별자 집합 덕분에 최신 시각과 같은 타임스탬프로 늦게 도착한 엔티티도 다음 List 에서 잡힌다.
 * 최신 시각보다 오래된 타임스탬프로 도착한 엔티티는 잡지 못한다 — 업스트림과 같은 한계이며,
 * 그런 경우는 'Tracking Entities' 를 쓴다.</p>
 *
 * @param timestamp   내보낸 엔티티 중 최신 타임스탬프 (없으면 0)
 * @param identifiers {@code timestamp} 와 같은 타임스탬프를 가진, 이미 내보낸 엔티티 식별자
 */
public record TimestampTrackingState(long timestamp, Set<String> identifiers) {

    public static final TimestampTrackingState EMPTY = new TimestampTrackingState(0L, Set.of());

    public TimestampTrackingState {
        identifiers = Set.copyOf(identifiers);
    }

    /**
     * @return 아직 내보내지 않은 엔티티인지
     */
    public boolean isNew(final long entityTimestamp, final String identifier) {
        return entityTimestamp > timestamp || (entityTimestamp == timestamp && !identifiers.contains(identifier));
    }

    /**
     * @param emitted 이번에 내보낸 엔티티
     * @return 내보낸 결과를 반영한 새 상태
     */
    public TimestampTrackingState advance(final Collection<? extends ListableEntity> emitted) {
        long latest = timestamp;
        for (final ListableEntity entity : emitted) {
            latest = Math.max(latest, entity.getTimestamp());
        }

        final Set<String> latestIdentifiers = new HashSet<>();
        if (latest == timestamp) {
            latestIdentifiers.addAll(identifiers);
        }
        for (final ListableEntity entity : emitted) {
            if (entity.getTimestamp() == latest) {
                latestIdentifiers.add(entity.getIdentifier());
            }
        }
        return new TimestampTrackingState(latest, latestIdentifiers);
    }

    /**
     * 동시에 같은 대상을 처리한 다른 태스크(다른 노드 포함)의 결과와 합친다. 순서와 무관한 단조 병합이다.
     */
    public TimestampTrackingState merge(final TimestampTrackingState other) {
        if (other.timestamp > timestamp) {
            return other;
        }
        if (other.timestamp < timestamp) {
            return this;
        }
        final Set<String> union = new HashSet<>(identifiers);
        union.addAll(other.identifiers);
        return new TimestampTrackingState(timestamp, union);
    }
}
