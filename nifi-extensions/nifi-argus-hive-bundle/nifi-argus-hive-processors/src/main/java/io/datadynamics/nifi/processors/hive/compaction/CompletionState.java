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
 * compaction 을 완료로 볼 상태(`Completion State`).
 */
public enum CompletionState implements DescribedValue {
    READY_FOR_CLEANING("ready for cleaning", "새 base/delta 가 커밋된 상태(ready for cleaning)부터 완료로 본다. 조회는 이미 새 파일을 쓰고, 옛 디렉토리는 Cleaner 가 나중에 지운다."),
    SUCCEEDED("succeeded", "Cleaner 가 옛 디렉토리까지 지운 상태(succeeded)를 기다린다. Cleaner 는 Metastore 에서 돌고 열린 트랜잭션이 끝나야 진행되므로 더 오래 걸릴 수 있다.");

    private final String displayName;
    private final String description;

    CompletionState(final String displayName, final String description) {
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
