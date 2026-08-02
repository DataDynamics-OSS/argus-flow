/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-services-api/src/main/java/org/apache/nifi/services/iceberg/IcebergCatalogProperty.java
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

/**
 * Iceberg 카탈로그 구현체가 사용하는 Hadoop 설정 프로퍼티를 나타내는 열거형이다.
 * 각 상수는 실제 Hadoop 설정 파일(core-site.xml, hive-site.xml 등)에서 사용되는
 * 프로퍼티 이름(hadoopPropertyName)과 매핑된다.
 */
public enum IcebergCatalogProperty {

    // Hive 메타스토어의 접속 URI를 지정하는 Hadoop 프로퍼티
    METASTORE_URI("hive.metastore.uris"),
    // 웨어하우스(테이블 데이터 저장 위치)의 기본 경로를 지정하는 Hadoop 프로퍼티
    WAREHOUSE_LOCATION("hive.metastore.warehouse.dir");

    private final String hadoopPropertyName;

    IcebergCatalogProperty(String hadoopPropertyName) {
        this.hadoopPropertyName = hadoopPropertyName;
    }

    public String getHadoopPropertyName() {
        return hadoopPropertyName;
    }

}
