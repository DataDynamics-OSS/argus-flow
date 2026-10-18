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
 * 출력 파일 수를 크기로 정할 때 파티션 크기를 얻는 곳(`Partition Size Source`).
 */
public enum PartitionSizeSource implements DescribedValue {
    HDFS("HDFS", "파티션 Location 을 Hadoop FileSystem 으로 직접 센다(권장). ACID 테이블은 현재 base·delta 만 센다(아직 지워지지 않은 옛 디렉토리 제외). "
            + "Hive 설정 리소스(core-site.xml·hdfs-site.xml 포함)와 보안 클러스터면 Kerberos 사용자 서비스가 필요하다."),
    TABLE_STATISTICS("테이블 통계", "DESCRIBE FORMATTED 의 totalSize 통계를 쓴다. 통계가 오래됐거나(ANALYZE 이후 적재) 없을 수 있다.");

    private final String displayName;
    private final String description;

    PartitionSizeSource(final String displayName, final String description) {
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
