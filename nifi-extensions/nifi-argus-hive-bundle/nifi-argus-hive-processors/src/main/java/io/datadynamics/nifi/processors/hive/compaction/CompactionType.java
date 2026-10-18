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

import org.apache.nifi.components.DescribedValue;

/**
 * 요청할 compaction 종류(`CompactHive3Table` 의 `Compaction Type`).
 */
public enum CompactionType implements DescribedValue {
    MAJOR("MAJOR", "base 와 모든 delta/delete_delta 를 새 base 하나로 다시 쓴다. 조회 성능 효과가 가장 크지만 비용도 크다."),
    MINOR("MINOR", "delta 들을 delta 하나(와 delete_delta 하나)로 합친다. base 는 그대로 둔다."),
    REBALANCE("REBALANCE", "Hive 4 이상, 버킷 없는 Full ACID 테이블 전용. COMPACT 'rebalance' CLUSTERED INTO N BUCKETS 로 "
            + "파일(버킷) N 개의 새 base 를 만든다(비동기, MAJOR 와 같은 루프). Hive 서버에 query-based compaction"
            + "(hive.compactor.crud.query.based=true, Tez)이 켜져 있어야 한다."),
    REWRITE("REWRITE", "INSERT OVERWRITE ... SELECT ... DISTRIBUTE BY <키> 를 리듀서 N 개로 실행해 파일 N 개로 다시 쓴다(동기). "
            + "ACID·Non-ACID(external 포함) 모두 가능, 버킷 테이블 불가. 파티션에 배타 lock 을 잡고 끝날 때까지 NiFi 스레드를 쓴다.");

    private final String displayName;
    private final String description;

    CompactionType(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
