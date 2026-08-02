/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-services-api/src/main/java/org/apache/nifi/services/iceberg/IcebergCatalogService.java
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software

 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.services.iceberg;

import org.apache.nifi.controller.ControllerService;

import java.util.List;
import java.util.Map;

/**
 * Iceberg 카탈로그 서비스에 대한 기본적인 커넥터를 제공한다.
 * 이 인터페이스를 구현하는 컨트롤러 서비스는 특정 카탈로그 종류(Hive, Hadoop 등)에
 * 연결하는 데 필요한 정보를 프로세서에 제공한다.
 */
public interface IcebergCatalogService extends ControllerService {

    // 이 서비스가 사용하는 카탈로그의 종류(HIVE, HADOOP 등)를 반환한다.
    IcebergCatalogType getCatalogType();

    // 카탈로그 연결에 필요한 Hadoop 프로퍼티 이름과 값의 매핑을 반환한다.
    Map<IcebergCatalogProperty, String> getCatalogProperties();

    // 카탈로그 설정에 사용되는 Hadoop 설정 파일들의 경로 목록을 반환한다.
    List<String> getConfigFilePaths();
}
