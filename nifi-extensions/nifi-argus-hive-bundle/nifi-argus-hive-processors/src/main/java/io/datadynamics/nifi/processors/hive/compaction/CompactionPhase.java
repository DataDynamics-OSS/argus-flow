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

import java.util.Locale;

/**
 * {@code SHOW COMPACTIONS} 상태 문자열을 처리 단계로 나눈다.
 * <p>
 * Hive 3.1 상태: {@code initiated, working, ready for cleaning, succeeded, failed, attempted}.
 * Hive 4.0 은 여기에 {@code did not initiate, refused}(예: "None of the compaction thresholds met") 등을 더한다.
 */
public enum CompactionPhase {
    /** 큐에 들어가 Worker 를 기다리는 중. */
    INITIATED,
    /** Worker 가 실행 중. */
    WORKING,
    /** 새 base/delta 가 커밋됐고 Cleaner 를 기다리는 중. */
    READY_FOR_CLEANING,
    /** Cleaner 까지 끝남. */
    SUCCEEDED,
    /** 실패·거부·미시작. */
    FAILED,
    /** 알 수 없는 상태. 다음 버전의 중간 상태일 수 있으므로 계속 기다린다(Compaction Timeout 으로 끝난다). */
    UNKNOWN;

    public static CompactionPhase of(final String state) {
        if (state == null) {
            return UNKNOWN;
        }
        final String normalized = state.trim().toLowerCase(Locale.ROOT).replace('_', ' ');
        return switch (normalized) {
            case "initiated" -> INITIATED;
            case "working" -> WORKING;
            case "ready for cleaning" -> READY_FOR_CLEANING;
            case "succeeded" -> SUCCEEDED;
            case "failed", "attempted", "did not initiate", "refused", "aborted" -> FAILED;
            default -> UNKNOWN;
        };
    }

    /**
     * 완료 기준에 따라 이 단계가 완료인지 본다.
     */
    public boolean isComplete(final CompletionState completionState) {
        return this == SUCCEEDED || (this == READY_FOR_CLEANING && completionState == CompletionState.READY_FOR_CLEANING);
    }
}
