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
 * 테이블 종류에 따른 처리 범위(`Table Type Handling`).
 */
public enum TableTypeHandling implements DescribedValue {
    ACID_ONLY("ACID만", "트랜잭션(ACID) 테이블만 처리한다. Non-ACID 테이블이면 failure 로 보낸다."),
    ACID_AND_NON_ACID_ORC("ACID + Non-ACID ORC", "ACID 테이블은 COMPACT, Non-ACID ORC managed 테이블은 CONCATENATE 로 처리한다. 그 밖의 Non-ACID 테이블은 failure.");

    private final String displayName;
    private final String description;

    TableTypeHandling(final String displayName, final String description) {
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
