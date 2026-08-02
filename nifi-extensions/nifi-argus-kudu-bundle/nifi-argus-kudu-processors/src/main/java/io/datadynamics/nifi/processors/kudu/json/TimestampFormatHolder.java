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
package io.datadynamics.nifi.processors.kudu.json;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link TimestampFormats}에 담긴 컬럼별 Timestamp Format 목록을 컬럼명으로 빠르게 조회할 수 있도록
 * Map 구조로 변환하여 보관하는 홀더 클래스입니다.
 * PutKudu에서 레코드의 각 필드를 Kudu 컬럼에 매핑할 때, 컬럼별로 지정된 파싱 패턴을 찾기 위해 사용합니다.
 */
public class TimestampFormatHolder {

    /**
     * 컬럼명을 키로 하는 Timestamp Format 맵
     */
    Map<String, TimestampFormat> columns;

    /**
     * 리스트 형태의 TimestampFormats를 컬럼명 기준의 Map으로 변환하여 저장합니다.
     */
    public TimestampFormatHolder(TimestampFormats formats) {
        this.columns = new HashMap<>();
        List<TimestampFormat> formats1 = formats.formats;
        for (TimestampFormat timestampFormat : formats1) {
            this.columns.put(timestampFormat.columnName, timestampFormat);
        }
    }

    /**
     * 지정한 컬럼명에 대해 설정된 Timestamp 유형을 반환합니다. 설정이 없으면 null을 반환합니다.
     */
    public TimestampType getType(String columnName) {
        if (!columns.containsKey(columnName)) {
            return null;
        }
        return columns.get(columnName).getType();
    }

    /**
     * 지정한 컬럼명에 대해 설정된 Timestamp 파싱 패턴을 반환합니다. 설정이 없으면 null을 반환합니다.
     */
    public String getPattern(String columnName) {
        if (!columns.containsKey(columnName)) {
            return null;
        }
        return columns.get(columnName).getTimestampFormat();
    }

}
