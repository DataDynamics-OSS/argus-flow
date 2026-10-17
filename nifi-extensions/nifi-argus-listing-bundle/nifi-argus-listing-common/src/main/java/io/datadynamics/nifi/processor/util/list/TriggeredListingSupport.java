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
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.processor.util.list.ListedEntity;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.schema.access.SchemaNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.datadynamics.nifi.processor.util.list.TriggeredListing.ATTR_COUNT;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.ATTR_ERROR_MESSAGE;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.ATTR_ERROR_TYPE;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.ATTR_TARGET;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.BY_ENTITIES;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.BY_TIMESTAMPS;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.INITIAL_LISTING_TARGET;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.LISTING_STRATEGY;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.MAX_LISTING_SIZE;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.MAX_TRACKED_TARGETS;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.REL_FAILURE;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.REL_ORIGINAL;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.REL_SUCCESS;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.TRACKING_STATE_CACHE;
import static io.datadynamics.nifi.processor.util.list.TriggeredListing.TRACKING_TIME_WINDOW;

/**
 * 입력 FlowFile 하나에 대한 트리거형 List 의 공통 흐름.
 *
 * <pre>
 *   대상 해석 → (추적 상태 읽기) → List → 새 엔티티 선별 → 건수 상한 검사
 *     → 자식 FlowFile 출력(success) → 입력 FlowFile(original) → 커밋 → (커밋 성공 후 추적 상태 저장)
 *   실패 시: 만든 자식 FlowFile 제거, 입력 FlowFile 에 오류 속성 → penalize → failure
 * </pre>
 *
 * <p>상태는 커밋이 성공한 뒤에 저장한다 (at-least-once). 상태 저장이 실패하거나 같은 대상을 동시에
 * 처리하면 다음 List 에서 같은 엔티티가 다시 나올 수 있지만, 놓치지는 않는다 — 업스트림 List 와 같은 선택이다.</p>
 *
 * <p>ListFile/SFTP/FTP 는 {@link AbstractTriggeredListProcessor} 를 통해, 다른 베이스 클래스를 상속해야 하는
 * ListS3(AbstractS3Processor)·ListHDFS(AbstractHadoopProcessor)는 이 클래스를 직접 호출한다.</p>
 */
public final class TriggeredListingSupport {

    /** List 대상을 정한다. 필수 값 누락·허용 범위 밖이면 예외 → failure */
    @FunctionalInterface
    public interface TargetResolver {
        ListingTarget resolve() throws IOException;
    }

    /** 대상을 List 한다. {@code minTimestamp} 보다 오래된 엔티티는 돌려주지 않아도 된다(서버 측 필터링 용도). */
    @FunctionalInterface
    public interface EntityLister<T extends ListableEntity> {
        List<T> list(long minTimestamp) throws IOException;
    }

    /**
     * 선별된 엔티티를 FlowFile 로 쓴다. 자식 FlowFile 은 {@code session.create(parent)} 로 만들고,
     * 만들 때마다 최신 버전을 {@code children} 에 넣는다 (실패 시 제거 대상). 전송(transfer)은 하지 않는다.
     */
    @FunctionalInterface
    public interface ListingEmitter<T extends ListableEntity> {
        void emit(ProcessSession session, FlowFile parent, List<T> entities, List<FlowFile> children) throws IOException, SchemaNotFoundException;
    }

    @FunctionalInterface
    private interface StatePersister {
        void persist() throws IOException;
    }

    private record Selection<T>(List<T> entities, StatePersister persister) {
    }

    private TriggeredListingSupport() {
    }

    public static <T extends ListableEntity> void process(final ProcessContext context, final ProcessSession session, final FlowFile input,
                                                          final String processorId, final ComponentLog logger,
                                                          final TargetResolver targetResolver, final EntityLister<T> lister,
                                                          final ListingEmitter<T> emitter) {
        final List<FlowFile> children = new ArrayList<>();
        ListingTarget target = null;
        try {
            target = targetResolver.resolve();
            final Selection<T> selection = select(context, processorId, target, lister);

            final int maxListingSize = context.getProperty(MAX_LISTING_SIZE).asInteger();
            if (selection.entities().size() > maxListingSize) {
                throw new IOException("Listing of [%s] found %d entities, exceeding Maximum Listing Size %d"
                        .formatted(target, selection.entities().size(), maxListingSize));
            }

            emitter.emit(session, input, selection.entities(), children);
            for (final FlowFile child : children) {
                session.transfer(session.putAttribute(child, ATTR_TARGET, target.description()), REL_SUCCESS);
            }
            children.clear();

            final FlowFile original = session.putAllAttributes(input, Map.of(
                    ATTR_TARGET, target.description(),
                    ATTR_COUNT, String.valueOf(selection.entities().size())));
            session.transfer(original, REL_ORIGINAL);
            logger.debug("Listed {} entities from {} for {}", selection.entities().size(), target, input);

            final StatePersister persister = selection.persister();
            final ListingTarget listedTarget = target;
            session.commitAsync(() -> {
                if (persister != null) {
                    try {
                        persister.persist();
                    } catch (final IOException e) {
                        logger.warn("Failed to save tracking state for {}; the same entities may be listed again", listedTarget, e);
                    }
                }
            });
        } catch (final ListingAbortedException e) {
            logger.info("Listing of {} aborted: {}; returning {} to the queue", target, e.getMessage(), input);
            session.rollback();
        } catch (final Exception e) {
            if (!children.isEmpty()) {
                session.remove(children);
            }
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(ATTR_ERROR_MESSAGE, String.valueOf(e.getMessage()));
            attributes.put(ATTR_ERROR_TYPE, e.getClass().getName());
            if (target != null) {
                attributes.put(ATTR_TARGET, target.description());
            }
            final FlowFile failed = session.penalize(session.putAllAttributes(input, attributes));
            session.transfer(failed, REL_FAILURE);
            logger.error("Failed to list {} for {}", target == null ? "target" : target, input, e);
        }
    }

    /**
     * 추적 상태를 모두 지운다 — 대상·필터·전략 설정이 바뀌었을 때 (업스트림의 속성 변경 시 상태 초기화와 같은 시점).
     * 'Tracking Entities' 로 캐시에 쓴 대상도 등록 정보로 찾아 지운다. 캐시 서비스가 설정돼 있지 않으면 캐시 항목은 남는다.
     */
    public static void resetTracking(final ProcessContext context, final String processorId, final ComponentLog logger) throws IOException {
        final StateManager stateManager = context.getStateManager();
        final StateMap state = stateManager.getState(Scope.CLUSTER);
        final Set<String> cachedTargets = EntityTrackingStore.registeredTargetKeys(state);
        if (!cachedTargets.isEmpty()) {
            // 전략을 바꾼 뒤에는 캐시 속성을 읽을 수 없으므로(NIFI-14400) 기록해 둔 서비스 ID 로 찾는다
            final String cacheServiceId = EntityTrackingStore.registeredCacheServiceId(state);
            final ControllerService service = cacheServiceId == null ? null : context.getControllerServiceLookup().getControllerService(cacheServiceId);
            if (service instanceof DistributedMapCacheClient client && context.getControllerServiceLookup().isControllerServiceEnabled(cacheServiceId)) {
                for (final String targetKey : cachedTargets) {
                    EntityTrackingStore.remove(client, processorId, targetKey);
                }
            } else {
                logger.warn("Tracking configuration changed but cache service [{}] is not available; {} cached target listings were left in the cache",
                        cacheServiceId, cachedTargets.size());
            }
        }
        stateManager.clear(Scope.CLUSTER);
        logger.info("Tracking configuration changed; cleared tracking state ({} cached targets)", cachedTargets.size());
    }

    private static <T extends ListableEntity> Selection<T> select(final ProcessContext context, final String processorId,
                                                                  final ListingTarget target, final EntityLister<T> lister) throws IOException {
        final String strategy = context.getProperty(LISTING_STRATEGY).getValue();

        if (BY_TIMESTAMPS.getValue().equals(strategy)) {
            final StateManager stateManager = context.getStateManager();
            final TimestampTrackingState previous = TimestampTrackingStore.read(stateManager.getState(Scope.CLUSTER), target);
            final List<T> selected = lister.list(previous.timestamp()).stream()
                    .filter(entity -> previous.isNew(entity.getTimestamp(), entity.getIdentifier()))
                    .toList();
            if (selected.isEmpty()) {
                return new Selection<>(selected, null);
            }
            final int maxTrackedTargets = context.getProperty(MAX_TRACKED_TARGETS).asInteger();
            final TimestampTrackingState next = previous.advance(selected);
            return new Selection<>(selected, () -> TimestampTrackingStore.save(stateManager, target, next, maxTrackedTargets, System.currentTimeMillis()));
        }

        if (BY_ENTITIES.getValue().equals(strategy)) {
            final DistributedMapCacheClient client = context.getProperty(TRACKING_STATE_CACHE).asControllerService(DistributedMapCacheClient.class);
            final EntityTrackingStore store = new EntityTrackingStore(client, processorId, target);
            final Map<String, ListedEntity> fetched = store.fetch();
            final boolean initialListing = fetched == null;
            final Map<String, ListedEntity> previous = initialListing ? Map.of() : fetched;

            final long windowMillis = context.getProperty(TRACKING_TIME_WINDOW).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
            final boolean listAll = ListedEntityTracker.INITIAL_LISTING_TARGET_ALL.getValue().equals(context.getProperty(INITIAL_LISTING_TARGET).getValue());
            final long minTimestampToList = initialListing && listAll ? -1 : System.currentTimeMillis() - windowMillis;

            final List<T> selected = EntityTrackingStore.selectUpdated(lister.list(minTimestampToList), previous, minTimestampToList);
            final Map<String, ListedEntity> updated = EntityTrackingStore.update(previous, selected, minTimestampToList);
            final boolean changed = initialListing || !updated.equals(previous);
            final StateManager stateManager = context.getStateManager();
            final String cacheServiceId = context.getProperty(TRACKING_STATE_CACHE).getValue();
            final int maxTrackedTargets = context.getProperty(MAX_TRACKED_TARGETS).asInteger();
            if (!changed) {
                return new Selection<>(selected, null);
            }
            return new Selection<>(selected, () -> {
                store.persist(updated);
                // 대상 등록(갱신 시각) — 상한을 넘은 오래된 대상의 캐시 항목은 지운다.
                // 변경이 있을 때만 등록해 FlowFile 마다 상태 공급자에 쓰지 않는다.
                for (final String evicted : EntityTrackingStore.register(stateManager, target, cacheServiceId, maxTrackedTargets, System.currentTimeMillis())) {
                    EntityTrackingStore.remove(client, processorId, evicted);
                }
            });
        }

        return new Selection<>(lister.list(0L), null);
    }
}
