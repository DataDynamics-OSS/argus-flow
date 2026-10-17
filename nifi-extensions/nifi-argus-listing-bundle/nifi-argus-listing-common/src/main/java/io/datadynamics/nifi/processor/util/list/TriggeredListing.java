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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.util.Set;

/**
 * 트리거형 List 프로세서 5종이 공유하는 관계·속성·FlowFile 속성 정의.
 */
public final class TriggeredListing {

    private TriggeredListing() {
    }

    // ---------------------------------------------------------------- 관계

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("List 결과. Record Writer 가 없으면 엔티티마다 FlowFile 하나, 있으면 전체 결과를 담은 FlowFile 하나. "
                    + "입력 FlowFile 의 속성을 상속한다.")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("List 를 마친 입력 FlowFile. 결과가 0건이어도 이리로 간다.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("List 대상을 해석하지 못했거나(필수 값 누락, 허용 범위 밖), 접속·List 에 실패했거나, "
                    + "결과가 'Maximum Listing Size' 를 넘은 입력 FlowFile.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_ORIGINAL, REL_FAILURE);

    // ---------------------------------------------------------------- FlowFile 속성

    /** List 대상 설명 — original 과 success FlowFile 에 기록 */
    public static final String ATTR_TARGET = "argus.list.target";
    /** 내보낸 엔티티 수 — original FlowFile 에 기록 */
    public static final String ATTR_COUNT = "argus.list.count";
    /** 실패 사유 — failure FlowFile 에 기록 */
    public static final String ATTR_ERROR_MESSAGE = "argus.list.error.message";
    /** 실패 예외 클래스 — failure FlowFile 에 기록 */
    public static final String ATTR_ERROR_TYPE = "argus.list.error.type";

    // ---------------------------------------------------------------- 추적 전략

    public static final AllowableValue NO_TRACKING = new AllowableValue("none", "No Tracking",
            "추적하지 않는다. 입력 FlowFile 이 올 때마다 대상의 현재 목록 전체를 내보낸다. '요청받은 경로를 지금 List' 하는 대부분의 경우에 쓴다.");

    public static final AllowableValue BY_TIMESTAMPS = new AllowableValue("timestamps", "Tracking Timestamps",
            "대상별로 내보낸 엔티티 중 최신 타임스탬프와 그 시각의 식별자를 기억해, 같은 대상을 다시 요청받으면 새 엔티티만 내보낸다. "
                    + "상태는 컴포넌트 CLUSTER 상태에 대상별 키로 저장한다. 최신 시각보다 오래된 타임스탬프로 나중에 들어온 엔티티는 잡지 못한다.");

    public static final AllowableValue BY_ENTITIES = new AllowableValue("entities", "Tracking Entities",
            "대상별로 'Entity Tracking Time Window' 안에 내보낸 모든 엔티티를 기억해, 새 엔티티와 타임스탬프·크기가 바뀐 엔티티를 내보낸다. "
                    + "오래된 타임스탬프로 들어온 엔티티도 잡는다. DistributedMapCache 서비스가 필요하다.");

    // ---------------------------------------------------------------- 속성

    public static final PropertyDescriptor LISTING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Listing Strategy")
            .description("같은 대상을 반복해서 요청받았을 때 무엇을 내보낼지 정한다. 추적 상태는 대상(경로·버킷·호스트 등)과 결과에 영향을 주는 "
                    + "설정(필터·재귀·나이·크기 등)의 조합별로 분리된다. 이 전략이나 대상·필터 설정을 바꾸면 다음 시작 시 추적 상태를 모두 지운다.")
            .required(true)
            .allowableValues(NO_TRACKING, BY_TIMESTAMPS, BY_ENTITIES)
            .defaultValue(NO_TRACKING.getValue())
            .build();

    public static final PropertyDescriptor MAX_TRACKED_TARGETS = new PropertyDescriptor.Builder()
            .name("Maximum Tracked Targets")
            .description("추적 상태를 유지할 대상(경로·버킷·호스트 등) 수 상한. 넘으면 가장 오래 쓰이지 않은 대상의 상태부터 지운다 "
                    + "('Tracking Entities' 는 그 대상의 캐시 항목도 지운다). 날짜별 경로처럼 대상이 계속 바뀌는 흐름에서 상태가 무한히 쌓이지 않게 한다. "
                    + "'Tracking Timestamps' 상태는 상태 공급자(ZooKeeper, Kubernetes ConfigMap 등)의 크기 제한(보통 약 1MiB)을 받으므로, "
                    + "대상 수가 많으면 'Tracking Entities' 를 쓴다.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .dependsOn(LISTING_STRATEGY, BY_TIMESTAMPS, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor TRACKING_STATE_CACHE = new PropertyDescriptor.Builder()
            .name(ListedEntityTracker.TRACKING_STATE_CACHE.getName())
            .description("'Tracking Entities' 의 이미 내보낸 엔티티 목록을 저장할 캐시. "
                    + "캐시 키는 'ArgusListedEntities::{프로세서 ID}::{대상 키}' 이고 값은 gzip 된 JSON 이다.")
            .identifiesControllerService(DistributedMapCacheClient.class)
            .required(true)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new PropertyDescriptor.Builder()
            .name(ListedEntityTracker.TRACKING_TIME_WINDOW.getName())
            .description("'Tracking Entities' 가 이미 내보낸 엔티티를 기억하는 기간. 이 기간 안의 타임스탬프를 가진 엔티티가 비교 대상이고, "
                    + "이보다 오래된 기록은 지운다.")
            .required(true)
            .defaultValue(ListedEntityTracker.TRACKING_TIME_WINDOW.getDefaultValue())
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor INITIAL_LISTING_TARGET = new PropertyDescriptor.Builder()
            .name(ListedEntityTracker.INITIAL_LISTING_TARGET.getName())
            .description("어떤 대상을 처음 List 할 때 무엇을 내보낼지 정한다.")
            .required(true)
            .allowableValues(ListedEntityTracker.INITIAL_LISTING_TARGET_ALL, ListedEntityTracker.INITIAL_LISTING_TARGET_WINDOW)
            .defaultValue(ListedEntityTracker.INITIAL_LISTING_TARGET_ALL.getValue())
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("List 결과를 쓸 Record Writer. 지정하지 않으면 엔티티마다 속성만 가진 FlowFile 을 하나씩 만들고, "
                    + "지정하면 입력 FlowFile 하나당 전체 결과를 레코드로 담은 FlowFile 하나를 만든다. 대상 하나의 엔티티가 많으면 지정을 권장한다.")
            .required(false)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    public static final PropertyDescriptor MAX_LISTING_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Listing Size")
            .description("입력 FlowFile 하나로 내보낼 수 있는 엔티티 수 상한. 넘으면 아무것도 내보내지 않고 입력 FlowFile 을 failure 로 보낸다. "
                    + "잘못된 경로(예: 루트 디렉터리) 요청이 FlowFile 폭증으로 이어지는 것을 막는다.")
            .required(true)
            .defaultValue("100000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
}
