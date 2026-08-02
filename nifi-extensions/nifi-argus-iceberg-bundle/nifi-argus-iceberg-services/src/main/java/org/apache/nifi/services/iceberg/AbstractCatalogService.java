/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-services/src/main/java/org/apache/nifi/services/iceberg/AbstractCatalogService.java
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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.w3c.dom.Document;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 카탈로그 서비스 구현체들이 공통으로 사용하는 프로퍼티와 메서드를 담고 있는 추상 클래스이다.
 * HiveCatalogService, HadoopCatalogService 등 구체적인 카탈로그 서비스는 이 클래스를
 * 상속하여 공통 로직(Hadoop 설정 파일 파싱, 파일 경로 목록 생성 등)을 재사용한다.
 */
public abstract class AbstractCatalogService extends AbstractControllerService implements IcebergCatalogService {

    // 카탈로그 연결에 필요한 Hadoop 프로퍼티(예: 메타스토어 URI, 웨어하우스 경로) 맵
    protected Map<IcebergCatalogProperty, String> catalogProperties = new HashMap<>();

    // Hadoop 설정 파일들의 경로 목록 (core-site.xml, hive-site.xml 등)
    protected List<String> configFilePaths;

    static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hadoop-config-resources")
            .displayName("Hadoop 설정 리소스")
            .description("Hadoop 설정(core-site.xml 등)을 포함하는 파일 또는 쉼표로 구분된 파일 목록. 지정하지 않으면 기본 설정이 사용된다.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    /**
     * 주어진 경로 목록에 있는 각 Hadoop 설정 파일을 읽어 XML Document 객체로 파싱한다.
     * 파일 읽기나 파싱 중 오류가 발생하면 ProcessException을 발생시켜 처리를 중단시킨다.
     */
    protected List<Document> parseConfigFilePaths(String configFilePaths) {
        List<Document> documentList = new ArrayList<>();
        for (final String configFile : createFilePathList(configFilePaths)) {
            File file = new File(configFile.trim());
            try (final InputStream fis = new FileInputStream(file);
                 final InputStream in = new BufferedInputStream(fis)) {
                final StandardDocumentProvider documentProvider = new StandardDocumentProvider();
                documentList.add(documentProvider.parse(in));
            } catch (IOException e) {
                throw new ProcessException("Failed to load config files", e);
            }
        }
        return documentList;
    }

    /**
     * 쉼표로 구분된 파일 경로 문자열을 개별 경로들로 분리하여 트리밍한 뒤 리스트로 반환한다.
     * 입력이 null이거나 공백뿐이면 빈 리스트를 반환한다.
     */
    protected List<String> createFilePathList(String configFilePaths) {
        List<String> filePathList = new ArrayList<>();
        if (configFilePaths != null && !configFilePaths.trim().isEmpty()) {
            for (final String configFile : configFilePaths.split(",")) {
                filePathList.add(configFile.trim());
            }
        }
        return filePathList;
    }

    @Override
    public Map<IcebergCatalogProperty, String> getCatalogProperties() {
        return catalogProperties;
    }

    @Override
    public List<String> getConfigFilePaths() {
        return configFilePaths;
    }
}
