/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/test/java/org/apache/nifi/processors/iceberg/catalog/TestHadoopCatalogService.java
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
package org.apache.nifi.processors.iceberg.catalog;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.services.iceberg.IcebergCatalogProperty;
import org.apache.nifi.services.iceberg.IcebergCatalogService;
import org.apache.nifi.services.iceberg.IcebergCatalogType;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.createTempDirectory;

/**
 * 테스트 전용 Hadoop 카탈로그 서비스 구현체.
 * 실제 Hadoop 카탈로그 대신 임시 디렉터리를 웨어하우스 위치로 사용하여,
 * PutIceberg 등의 프로세서 테스트에서 실제 Hadoop 환경 없이도 카탈로그 서비스를 주입할 수 있도록 한다.
 */
public class TestHadoopCatalogService extends AbstractControllerService implements IcebergCatalogService {

    // 카탈로그 속성(웨어하우스 위치 등)을 보관하는 맵
    private final Map<IcebergCatalogProperty, String> catalogProperties = new HashMap<>();

    // 생성 시 임시 디렉터리를 만들어 웨어하우스 위치로 등록한다.
    public TestHadoopCatalogService() throws IOException {
        File warehouseLocation = createTempDirectory("metastore").toFile();
        catalogProperties.put(IcebergCatalogProperty.WAREHOUSE_LOCATION, warehouseLocation.getAbsolutePath());
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.HADOOP;
    }

    @Override
    public Map<IcebergCatalogProperty, String> getCatalogProperties() {
        return catalogProperties;
    }

    @Override
    public List<String> getConfigFilePaths() {
        // Hadoop 카탈로그 테스트 구현체는 별도의 설정 파일 경로가 필요 없으므로 null 반환
        return null;
    }
}
