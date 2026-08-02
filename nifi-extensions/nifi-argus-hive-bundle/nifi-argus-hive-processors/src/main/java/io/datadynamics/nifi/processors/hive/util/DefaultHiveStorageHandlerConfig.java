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
import java.util.Map;

/**
 * 스토리지 핸들러를 사용하지 않는 일반 Hive 테이블을 위한 기본(no-op) {@link StorageHandlerConfig} 구현체.
 * STORED BY 절과 추가 TBLPROPERTIES가 필요 없는 경우 이 구현을 사용한다.
 */
public class DefaultHiveStorageHandlerConfig implements StorageHandlerConfig {

    // 특정 스토리지 핸들러를 사용하지 않으므로 클래스명이 존재하지 않는다.
    public String getStorageHandlerClassName() {
        return null;
    }

    // 추가로 설정할 테이블 속성이 없으므로 빈 맵을 반환한다.
    public Map<String, String> getTablePropertiesMap() {
        return Collections.emptyMap();
    }

}
