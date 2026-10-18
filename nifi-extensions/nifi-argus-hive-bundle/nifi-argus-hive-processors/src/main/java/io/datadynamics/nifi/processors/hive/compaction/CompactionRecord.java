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
 * {@code SHOW COMPACTIONS} 의 한 행. Hive 가 값 없음을 {@code ---} 로 보여 주는 칸은 {@code null} 이다.
 *
 * @param id           compaction id
 * @param database     DB 이름
 * @param table        테이블 이름
 * @param partition    파티션 이름({@code k=v/k2=v2}), 비파티션이면 {@code null}
 * @param type         MAJOR/MINOR 등
 * @param state        상태 문자열(예: {@code initiated}, {@code ready for cleaning})
 * @param worker       worker id (Hive 3.1 은 host-thread, Hive 4 는 thread id)
 * @param workerHost   worker host (Hive 4)
 * @param enqueueTime  enqueue 시각 epoch ms 문자열 (Hive 4)
 * @param startTime    시작 시각 epoch ms 문자열
 * @param duration     소요 시간 ms 문자열
 * @param hadoopJobId  Hadoop/Tez job id
 * @param errorMessage 오류 메시지 (Hive 4)
 */
public record CompactionRecord(long id, String database, String table, String partition, String type, String state,
                               String worker, String workerHost, String enqueueTime, String startTime, String duration,
                               String hadoopJobId, String errorMessage) {

    /**
     * 같은 DB·테이블·파티션의 compaction 인지(대소문자 무시) 본다.
     */
    public boolean isFor(final String db, final String tableName, final PartitionSpec spec) {
        return database != null && table != null && database.equalsIgnoreCase(db) && table.equalsIgnoreCase(tableName)
                && spec.matchesPartitionName(partition);
    }

    /**
     * 소문자·trim 한 상태.
     */
    public String normalizedState() {
        return state == null ? "" : state.trim().toLowerCase(Locale.ROOT);
    }
}
