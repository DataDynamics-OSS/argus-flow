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


import java.util.Map;

/**
 * Hive 테이블 생성 시 사용할 스토리지 핸들러(예: Iceberg)에 대한 설정 정보를 제공하는 인터페이스.
 * 스토리지 핸들러 종류에 따라 CREATE TABLE 문에 필요한 클래스명과 테이블 속성(TBLPROPERTIES)이 달라지므로
 * 이를 구현체별로 추상화하여 프로세서 코드가 특정 스토리지 핸들러에 종속되지 않도록 한다.
 */
public interface StorageHandlerConfig {

    /**
     * CREATE TABLE 문의 STORED BY 절에 사용할 스토리지 핸들러의 클래스명(또는 식별자)을 반환한다.
     * 스토리지 핸들러를 사용하지 않는 기본 구현의 경우 null을 반환할 수 있다.
     */
    String getStorageHandlerClassName();

    /**
     * CREATE TABLE 문의 TBLPROPERTIES 절에 추가할 테이블 속성 목록을 반환한다.
     */
    Map<String, String> getTablePropertiesMap();

}
