/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/test/java/org/apache/nifi/processors/iceberg/catalog/TestHiveCatalogService.java
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.METASTORE_URI;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.WAREHOUSE_LOCATION;

/**
 * 테스트 전용 Hive 카탈로그 서비스 구현체.
 * 실제 Hive 메타스토어에 연결하지 않고, 테스트에서 필요한 카탈로그 속성(metastore URI, 웨어하우스 위치 등)과
 * 설정 파일 경로(core-site.xml 등)를 자유롭게 주입할 수 있도록 Builder 패턴을 제공한다.
 * Kerberos 보안 설정 검증 등 PutIceberg 프로세서의 커스텀 검증 테스트에서 사용된다.
 */
public class TestHiveCatalogService extends AbstractControllerService implements IcebergCatalogService {

    private final List<String> configFilePaths;
    private final Map<IcebergCatalogProperty, String> catalogProperties;

    public TestHiveCatalogService(Map<IcebergCatalogProperty, String> catalogProperties, List<String> configFilePaths) {
        this.catalogProperties = catalogProperties;
        this.configFilePaths = configFilePaths;
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.HIVE;
    }

    @Override
    public Map<IcebergCatalogProperty, String> getCatalogProperties() {
        return catalogProperties;
    }

    @Override
    public List<String> getConfigFilePaths() {
        return configFilePaths;
    }

    // TestHiveCatalogService 인스턴스를 손쉽게 구성하기 위한 빌더.
    // 필요한 속성만 선택적으로 설정한 뒤 build()를 호출하면 카탈로그 속성 맵이 채워진 서비스가 생성된다.
    public static class Builder {
        private String metastoreUri;
        private String warehouseLocation;
        private List<String> configFilePaths;

        public Builder withMetastoreUri(String metastoreUri) {
            this.metastoreUri = metastoreUri;
            return this;
        }

        public Builder withWarehouseLocation(String warehouseLocation) {
            this.warehouseLocation = warehouseLocation;
            return this;
        }

        public Builder withConfigFilePaths(List<String> configFilePaths) {
            this.configFilePaths = configFilePaths;
            return this;
        }

        // 지금까지 설정된 값들을 바탕으로 카탈로그 속성 맵을 조립하고 TestHiveCatalogService를 생성한다.
        public TestHiveCatalogService build() {
            Map<IcebergCatalogProperty, String> properties = new HashMap<>();

            if (metastoreUri != null) {
                properties.put(METASTORE_URI, metastoreUri);
            }

            if (warehouseLocation != null) {
                properties.put(WAREHOUSE_LOCATION, warehouseLocation);
            }

            return new TestHiveCatalogService(properties, configFilePaths);
        }
    }
}
