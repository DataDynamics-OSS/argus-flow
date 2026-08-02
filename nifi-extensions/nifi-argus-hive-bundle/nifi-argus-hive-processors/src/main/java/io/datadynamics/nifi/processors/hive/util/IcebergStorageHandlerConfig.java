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
package io.datadynamics.nifi.processors.hive.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Iceberg 테이블 형식으로 Hive 테이블을 생성할 때 필요한 {@link StorageHandlerConfig} 구현체.
 * CREATE TABLE 문의 STORED BY 절에 사용할 스토리지 핸들러 식별자("ICEBERG")와,
 * Hive 엔진에서 Iceberg 테이블을 인식/조회할 수 있도록 하는 TBLPROPERTIES를 함께 제공한다.
 */
public class IcebergStorageHandlerConfig implements StorageHandlerConfig {
    static final String ICEBERG_STORAGE_HANDLER_CLASSNAME = "ICEBERG";

    private final Map<String, String> tablePropertiesMap = new HashMap<>();

    public IcebergStorageHandlerConfig() {
        // Hive 엔진(HiveServer2 등)에서 Iceberg 테이블을 조회할 수 있도록 활성화하는 속성.
        // 이 속성이 없으면 Hive에서 Iceberg 테이블을 인식하지 못할 수 있다.
        this.tablePropertiesMap.put("engine.hive.enabled", "true");
    }

    public String getStorageHandlerClassName() {
        return "ICEBERG";
    }

    // 외부에서 내부 상태를 변경하지 못하도록 수정 불가능한 맵으로 감싸 반환한다.
    public Map<String, String> getTablePropertiesMap() {
        return Collections.unmodifiableMap(this.tablePropertiesMap);
    }
}
