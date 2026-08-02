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

import java.io.Serializable;
import java.util.List;

/**
 * PutKudu의 'Timestamp 컬럼의 Timestamp Format(JSON 형식)' 속성값을 Jackson으로 역직렬화하기 위한 루트 모델 클래스입니다.
 * 아래와 같은 JSON 구조의 최상위 {@code formats} 배열을 담습니다.
 * <pre>
 * {
 *   "formats": [
 *     { "column-name": "COL_TIMESTAMP", "timestamp-pattern": "yyyy-MM-dd HH:mm:ss", "type": "TIMESTAMP_MILLIS" }
 *   ]
 * }
 * </pre>
 */
public class TimestampFormats implements Serializable {

    /**
     * 컬럼별 Timestamp Format 목록
     */
    List<TimestampFormat> formats;

    public List<TimestampFormat> getFormats() {
        return formats;
    }

    public void setFormats(List<TimestampFormat> formats) {
        this.formats = formats;
    }

}
