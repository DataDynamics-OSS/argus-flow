/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/catalog/IcebergCatalogFactory.java
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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.nifi.services.iceberg.IcebergCatalogProperty;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.iceberg.IcebergUtils.getConfigurationFromFiles;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.METASTORE_URI;
import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.WAREHOUSE_LOCATION;

/**
 * {@link IcebergCatalogService} 컨트롤러 서비스에 설정된 카탈로그 타입과 속성을 바탕으로
 * 실제 Iceberg {@link Catalog} 구현체(HiveCatalog, HadoopCatalog 등)를 생성해 주는 팩토리 클래스.
 * 프로세서는 이 클래스를 통해 카탈로그 구현 세부 사항을 신경 쓰지 않고 Catalog 인스턴스를 얻을 수 있다.
 */
public class IcebergCatalogFactory {

    private final IcebergCatalogService catalogService;

    public IcebergCatalogFactory(IcebergCatalogService catalogService) {
        this.catalogService = catalogService;
    }

    /**
     * 컨트롤러 서비스에 설정된 카탈로그 타입에 맞는 Catalog 인스턴스를 생성한다.
     *
     * @return 초기화가 완료된 Catalog 인스턴스
     * @throws IllegalArgumentException 지원하지 않는 카탈로그 타입인 경우
     */
    public Catalog create() {
        switch (catalogService.getCatalogType()) {
            case HIVE:
                return initHiveCatalog(catalogService);
            case HADOOP:
                return initHadoopCatalog(catalogService);
            default:
                throw new IllegalArgumentException("Unknown catalog type: " + catalogService.getCatalogType());
        }
    }

    /**
     * Hive 메타스토어를 사용하는 {@link HiveCatalog}를 초기화한다.
     * 설정 파일 경로(core-site.xml, hive-site.xml 등)가 지정되어 있으면 Hadoop Configuration에 반영하고,
     * 메타스토어 URI와 웨어하우스 위치를 카탈로그 속성으로 설정한다.
     */
    private Catalog initHiveCatalog(IcebergCatalogService catalogService) {
        HiveCatalog catalog = new HiveCatalog();

        if (catalogService.getConfigFilePaths() != null) {
            final Configuration configuration = getConfigurationFromFiles(catalogService.getConfigFilePaths());
            catalog.setConf(configuration);
        }

        final Map<IcebergCatalogProperty, String> catalogProperties = catalogService.getCatalogProperties();
        final Map <String, String> properties = new HashMap<>();

        if (catalogProperties.containsKey(METASTORE_URI)) {
            properties.put(CatalogProperties.URI, catalogProperties.get(METASTORE_URI));
        }

        if (catalogProperties.containsKey(WAREHOUSE_LOCATION)) {
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, catalogProperties.get(WAREHOUSE_LOCATION));
        }

        catalog.initialize("hive-catalog", properties);
        return catalog;
    }

    /**
     * 파일시스템(HDFS 등) 기반의 {@link HadoopCatalog}를 초기화한다.
     * 설정 파일 경로가 지정되어 있으면 해당 설정을 사용하고, 없으면 기본 Configuration을 사용한다.
     */
    private Catalog initHadoopCatalog(IcebergCatalogService catalogService) {
        final Map<IcebergCatalogProperty, String> catalogProperties = catalogService.getCatalogProperties();
        final String warehousePath = catalogProperties.get(WAREHOUSE_LOCATION);

        if (catalogService.getConfigFilePaths() != null) {
            return new HadoopCatalog(getConfigurationFromFiles(catalogService.getConfigFilePaths()), warehousePath);
        } else {
            return new HadoopCatalog(new Configuration(), warehousePath);
        }
    }
}
