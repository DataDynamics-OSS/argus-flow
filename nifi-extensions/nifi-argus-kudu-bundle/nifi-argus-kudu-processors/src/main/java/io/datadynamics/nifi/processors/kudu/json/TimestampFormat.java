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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * PutKudu의 'Timestamp 컬럼의 Timestamp Format(JSON 형식)' 속성에 지정한 JSON의 개별 컬럼 항목을 표현하는 모델 클래스입니다.
 * 컬럼명, 해당 컬럼에 적용할 Timestamp 파싱 패턴, Timestamp 유형(TimestampType)을 담고 있습니다.
 */
public class TimestampFormat implements Serializable {

    /**
     * Timestamp Format을 적용할 대상 Kudu 컬럼명
     */
    @JsonProperty("column-name")
    String columnName;

    /**
     * 해당 컬럼값을 파싱할때 사용할 Timestamp 패턴 (예: yyyy-MM-dd HH:mm:ss)
     */
    @JsonProperty("timestamp-pattern")
    String timestampFormat;

    /**
     * Timestamp의 정밀도 유형 (DATE, TIMESTAMP, TIMESTAMP_MILLIS, TIMESTAMP_MICROS)
     */
    @JsonProperty("type")
    TimestampType type;

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public TimestampType getType() {
        return type;
    }

    public void setType(TimestampType type) {
        this.type = type;
    }

}
