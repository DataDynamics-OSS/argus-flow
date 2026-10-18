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
 * ACID 테이블에 요청할 compaction 종류(`CompactHive3Table` 의 `Compaction Type`).
 */
public enum CompactionType implements DescribedValue {
    MAJOR("MAJOR", "base 와 모든 delta/delete_delta 를 새 base 하나로 다시 쓴다. 조회 성능 효과가 가장 크지만 비용도 크다."),
    MINOR("MINOR", "delta 들을 delta 하나(와 delete_delta 하나)로 합친다. base 는 그대로 둔다.");

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
