/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/UnmatchedColumnBehavior.java
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.iceberg;

import org.apache.nifi.components.DescribedValue;

/**
 * 레코드(문서)에 존재하지 않는 테이블 컬럼, 즉 매칭되지 않은 컬럼을 만났을 때
 * 프로세서가 취할 동작을 정의하는 열거형.
 */
public enum UnmatchedColumnBehavior implements DescribedValue {
    IGNORE_UNMATCHED_COLUMN("Ignore Unmatched Columns",
            "데이터베이스의 컬럼 중 문서에 해당 필드가 없는 경우, 필수가 아닌 것으로 간주합니다. 별도의 알림은 기록되지 않습니다."),

    WARNING_UNMATCHED_COLUMN("Warn on Unmatched Columns",
            "데이터베이스의 컬럼 중 문서에 해당 필드가 없는 경우, 필수가 아닌 것으로 간주합니다. 경고 로그가 기록됩니다."),

    FAIL_UNMATCHED_COLUMN("Fail on Unmatched Columns",
            "데이터베이스의 컬럼 중 문서에 해당 필드가 없는 경우 플로우가 실패합니다. 오류 로그가 기록됩니다.");


    private final String displayName;
    private final String description;

    UnmatchedColumnBehavior(final String displayName, final String description) {
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
