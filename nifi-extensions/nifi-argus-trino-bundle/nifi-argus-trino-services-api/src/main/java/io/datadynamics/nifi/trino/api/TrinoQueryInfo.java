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
package io.datadynamics.nifi.trino.api;

/**
 * Trino 쿼리의 식별자와 실행 통계.
 *
 * @param queryId         Trino query id (예: {@code 20261018_094637_00008_rq293})
 * @param state           쿼리 상태(예: {@code FINISHED}, {@code FAILED})
 * @param processedRows   처리한 입력 행 수
 * @param processedBytes  처리한 입력 바이트 수
 * @param cpuTimeMillis   누적 CPU 시간(ms)
 * @param wallTimeMillis  누적 wall 시간(ms)
 * @param queuedTimeMillis 대기열 시간(ms)
 */
public record TrinoQueryInfo(String queryId, String state, long processedRows, long processedBytes,
                             long cpuTimeMillis, long wallTimeMillis, long queuedTimeMillis) {
}
