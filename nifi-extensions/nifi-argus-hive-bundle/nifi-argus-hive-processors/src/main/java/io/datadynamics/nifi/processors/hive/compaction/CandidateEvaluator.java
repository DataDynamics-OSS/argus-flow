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

import java.util.Optional;

/**
 * 파티션 디렉토리 수치로 compaction 후보인지와 권장 유형을 정한다.
 * <ul>
 *     <li>ACID — Hive Initiator 와 같은 순서: delta 크기 / base 크기 &gt; {@code deltaSizeRatioThreshold}
 *     ({@code hive.compactor.delta.pct.threshold}, base 가 없으면 delta 가 하나라도 있으면) → MAJOR,
 *     아니면 delta+delete_delta 디렉토리 수 &gt; {@code deltaCountThreshold}({@code hive.compactor.delta.num.threshold}) → MINOR.</li>
 *     <li>Non-ACID — 파일 수 &gt; {@code smallFileCountThreshold} 이고 평균 파일 크기 &lt; {@code smallFileBytes} →
 *     managed ORC 면 CONCATENATE, 그 밖에는 REWRITE.</li>
 * </ul>
 */
public final class CandidateEvaluator {

    /** 권장 유형(출력 속성 {@code hive.compaction.recommended.type} 값). */
    public enum Recommendation { MAJOR, MINOR, CONCATENATE, REWRITE }

    /**
     * 임계값.
     */
    public record Thresholds(int deltaCountThreshold, double deltaSizeRatioThreshold, int smallFileCountThreshold, long smallFileBytes) {
    }

    private CandidateEvaluator() {
    }

    /**
     * @param stats      디렉토리 수치
     * @param managedOrc Non-ACID 일 때 managed ORC 테이블인지(CONCATENATE 가능)
     */
    public static Optional<Recommendation> evaluate(final PartitionDirectoryScanner.Stats stats, final boolean managedOrc,
                                                    final Thresholds thresholds) {
        if (stats.acid()) {
            final int deltas = stats.totalDeltaCount();
            if (deltas == 0) {
                return Optional.empty();
            }
            if (stats.baseName() == null || stats.baseBytes() == 0) {
                return Optional.of(Recommendation.MAJOR);
            }
            if ((double) stats.deltaBytes() / (double) stats.baseBytes() > thresholds.deltaSizeRatioThreshold()) {
                return Optional.of(Recommendation.MAJOR);
            }
            if (deltas > thresholds.deltaCountThreshold()) {
                return Optional.of(Recommendation.MINOR);
            }
            return Optional.empty();
        }
        if (stats.fileCount() > thresholds.smallFileCountThreshold() && stats.averageFileBytes() < thresholds.smallFileBytes()) {
            return Optional.of(managedOrc ? Recommendation.CONCATENATE : Recommendation.REWRITE);
        }
        return Optional.empty();
    }
}
