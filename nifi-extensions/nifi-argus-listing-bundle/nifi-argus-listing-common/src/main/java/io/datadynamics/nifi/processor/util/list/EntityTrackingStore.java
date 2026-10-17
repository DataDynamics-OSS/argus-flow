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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.processor.util.list.ListedEntity;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 'Tracking Entities' 상태를 DistributedMapCache 에 대상별 키로 저장한다.
 *
 * <p>업스트림 {@code ListedEntityTracker} 와 같은 판정 규칙(신규 / 더 새 타임스탬프 / 크기 변경)과 같은
 * 저장 형식(gzip JSON {@code Map<식별자, ListedEntity>})을 쓴다. 다만 그 클래스는 내부에서
 * {@code session.commitAsync()} 를 호출하고 부모 없이 FlowFile 을 만들어, 입력 FlowFile 을 같은 세션에서
 * 라우팅해야 하는 트리거 모델에 쓸 수 없어 판정·저장만 분리했다.</p>
 *
 * <p>캐시 키: {@code ArgusListedEntities::<processorId>::<targetKey>}</p>
 *
 * <p>캐시 서버는 키 목록 조회를 지원하지 않을 수 있으므로, 캐시에 쓴 대상을 컴포넌트 CLUSTER 상태에
 * {@code e.<targetKey>.seen} 으로 등록해 둔다. 대상 수가 'Maximum Tracked Targets' 를 넘으면 오래된 대상의 캐시 항목을,
 * 설정이 바뀌어 추적을 초기화할 때는 등록된 모든 대상의 캐시 항목을 지운다.</p>
 */
public final class EntityTrackingStore {

    static final String CACHE_KEY_PREFIX = "ArgusListedEntities";
    static final String REGISTRY_PREFIX = "e.";
    /** 캐시에 쓸 때 사용한 캐시 서비스 ID — 전략을 바꾼 뒤에는 캐시 속성을 읽을 수 없으므로(NIFI-14400) 초기화 때 이것으로 찾는다 */
    static final String CACHE_SERVICE_KEY = "e-cache-service";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Serializer<String> STRING_SERIALIZER = (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));

    private static final Serializer<Map<String, ListedEntity>> ENTITIES_SERIALIZER = (value, out) -> {
        final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(out);
        OBJECT_MAPPER.writeValue(gzipOutputStream, value);
        gzipOutputStream.finish();
    };

    private static final Deserializer<Map<String, ListedEntity>> ENTITIES_DESERIALIZER = bytes -> {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
            return OBJECT_MAPPER.readValue(in, new TypeReference<>() { });
        }
    };

    private final DistributedMapCacheClient client;
    private final String cacheKey;

    public EntityTrackingStore(final DistributedMapCacheClient client, final String processorId, final ListingTarget target) {
        this.client = client;
        this.cacheKey = cacheKey(processorId, target.key());
    }

    private static String cacheKey(final String processorId, final String targetKey) {
        return CACHE_KEY_PREFIX + "::" + processorId + "::" + targetKey;
    }

    /**
     * 대상을 등록(마지막 사용 시각 갱신)하고, 대상 수 상한을 넘은 오래된 대상을 등록에서 뺀다.
     *
     * @return 등록에서 빠진 대상 키 — 호출자가 {@link #remove} 로 캐시 항목을 지운다
     */
    public static List<String> register(final StateManager stateManager, final ListingTarget target, final String cacheServiceId,
                                        final int maxTrackedTargets, final long now) throws IOException {
        for (int attempt = 0; attempt < TrackedTargets.MAX_REPLACE_ATTEMPTS; attempt++) {
            final StateMap current = stateManager.getState(Scope.CLUSTER);
            final Map<String, String> updated = new HashMap<>(current.toMap());
            updated.put(REGISTRY_PREFIX + target.key() + TrackedTargets.SEEN_SUFFIX, String.valueOf(now));
            updated.put(CACHE_SERVICE_KEY, cacheServiceId);
            final List<String> evicted = TrackedTargets.evictOldest(updated, REGISTRY_PREFIX, maxTrackedTargets, target.key());
            if (stateManager.replace(current, updated, Scope.CLUSTER)) {
                return evicted;
            }
            TrackedTargets.backoff(attempt);
        }
        throw new IOException("Failed to register tracked target [" + target + "] after " + TrackedTargets.MAX_REPLACE_ATTEMPTS + " attempts due to concurrent updates");
    }

    /**
     * @return 등록된 대상의 캐시 항목이 들어 있는 캐시 서비스 ID, 없으면 {@code null}
     */
    public static String registeredCacheServiceId(final StateMap stateMap) {
        return stateMap.get(CACHE_SERVICE_KEY);
    }

    public static Set<String> registeredTargetKeys(final StateMap stateMap) {
        return TrackedTargets.targetKeys(stateMap.toMap(), REGISTRY_PREFIX);
    }

    public static void remove(final DistributedMapCacheClient client, final String processorId, final String targetKey) throws IOException {
        client.remove(cacheKey(processorId, targetKey), STRING_SERIALIZER);
    }

    public String getCacheKey() {
        return cacheKey;
    }

    /**
     * @return 이미 내보낸 엔티티, 이 대상을 처음 List 하는 경우 {@code null}
     */
    public Map<String, ListedEntity> fetch() throws IOException {
        final Map<String, ListedEntity> entities = client.get(cacheKey, STRING_SERIALIZER, ENTITIES_DESERIALIZER);
        return entities == null ? null : new HashMap<>(entities);
    }

    public void persist(final Map<String, ListedEntity> entities) throws IOException {
        client.put(cacheKey, entities, STRING_SERIALIZER, ENTITIES_SERIALIZER);
    }

    /**
     * @return 내보낼 엔티티 — 처음 보는 것, 타임스탬프가 더 새로운 것, 크기가 바뀐 것
     */
    public static <T extends ListableEntity> List<T> selectUpdated(final Collection<T> listed, final Map<String, ListedEntity> alreadyListed,
                                                                   final long minTimestampToList) {
        final List<T> updated = new ArrayList<>();
        for (final T entity : listed) {
            if (entity.getTimestamp() < minTimestampToList) {
                continue;
            }
            final ListedEntity previous = alreadyListed.get(entity.getIdentifier());
            if (previous == null || entity.getTimestamp() > previous.getTimestamp() || entity.getSize() != previous.getSize()) {
                updated.add(entity);
            }
        }
        return updated;
    }

    /**
     * 추적 창보다 오래된 엔티티를 지우고 이번에 내보낸 엔티티를 기록한다.
     */
    public static Map<String, ListedEntity> update(final Map<String, ListedEntity> alreadyListed, final Collection<? extends ListableEntity> emitted,
                                                   final long minTimestampToList) {
        final Map<String, ListedEntity> updated = new HashMap<>(alreadyListed);
        updated.values().removeIf(entity -> entity.getTimestamp() < minTimestampToList);
        for (final ListableEntity entity : emitted) {
            updated.put(entity.getIdentifier(), new ListedEntity(entity.getTimestamp(), entity.getSize()));
        }
        return updated;
    }
}
