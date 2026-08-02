/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-services/src/main/java/org/apache/nifi/services/iceberg/HiveCatalogService.java
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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Tags({"iceberg", "catalog", "service", "metastore", "hive"})
@CapabilityDescription("Iceberg 테이블을 추적하기 위해 Hive 메타스토어에 연결하는 카탈로그 서비스.")
public class HiveCatalogService extends AbstractCatalogService {

    static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("hive-metastore-uri")
            .displayName("Hive 메타스토어 URI")
            .description("Hive 메타스토어의 URI 위치(들). 이는 Hive 서버의 위치가 아니라는 점에 유의한다. Hive 메타스토어의 기본 포트는 9043이다.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .build();

    static final PropertyDescriptor WAREHOUSE_LOCATION = new PropertyDescriptor.Builder()
            .name("warehouse-location")
            .displayName("기본 웨어하우스 위치")
            .description("웨어하우스의 기본 데이터베이스 위치. 이 필드는 'hive.metastore.warehouse.dir' 설정 프로퍼티를 설정하거나 재정의한다.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            METASTORE_URI,
            WAREHOUSE_LOCATION,
            HADOOP_CONFIGURATION_RESOURCES
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    /**
     * 메타스토어 URI와 웨어하우스 위치가 프로퍼티로 직접 지정되지 않은 경우,
     * Hadoop 설정 파일에서 해당 값을 찾을 수 있는지 검사하여 둘 중 하나도
     * 확인되지 않으면 검증 오류를 추가한다.
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {

        final List<ValidationResult> problems = new ArrayList<>();
        boolean configMetastoreUriPresent = false;
        boolean configWarehouseLocationPresent = false;

        final String propertyMetastoreUri = validationContext.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue();
        final String propertyWarehouseLocation = validationContext.getProperty(WAREHOUSE_LOCATION).evaluateAttributeExpressions().getValue();

        // Load the configurations for validation only if any config resource is provided and if either the metastore URI or the warehouse location property is missing
        if (validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet() && (propertyMetastoreUri == null || propertyWarehouseLocation == null)) {
            final String configFiles = validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            final List<Document> documents = parseConfigFilePaths(configFiles);

            for (Document document : documents) {
                final NodeList nameNodeList = document.getElementsByTagName("name");

                for (int i = 0; i < nameNodeList.getLength(); i++) {
                    final String nodeValue = nameNodeList.item(i).getFirstChild().getNodeValue();

                    if (nodeValue.equals(IcebergCatalogProperty.METASTORE_URI.getHadoopPropertyName())) {
                        configMetastoreUriPresent = true;
                    }

                    if (nodeValue.equals(IcebergCatalogProperty.WAREHOUSE_LOCATION.getHadoopPropertyName())) {
                        configWarehouseLocationPresent = true;
                    }

                    if (configMetastoreUriPresent && configWarehouseLocationPresent) {
                        break;
                    }
                }
            }
        }

        if (!configMetastoreUriPresent && propertyMetastoreUri == null) {
            problems.add(new ValidationResult.Builder()
                    .subject("Hive Metastore URI")
                    .valid(false)
                    .explanation("cannot find hive metastore uri, please provide it in the 'Hive Metastore URI' property" +
                            " or provide a configuration file which contains 'hive.metastore.uris' value.")
                    .build());
        }

        if (!configWarehouseLocationPresent && propertyWarehouseLocation == null) {
            problems.add(new ValidationResult.Builder()
                    .subject("Default Warehouse Location")
                    .valid(false)
                    .explanation("cannot find default warehouse location, please provide it in the 'Default Warehouse Location' property" +
                            " or provide a configuration file which contains 'hive.metastore.warehouse.dir' value.")
                    .build());
        }

        return problems;
    }

    // 컨트롤러 서비스가 활성화될 때 설정된 프로퍼티들(메타스토어 URI, 웨어하우스 위치,
    // Hadoop 설정 파일 경로)을 읽어들여 카탈로그 프로퍼티 맵을 채운다.
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        if (context.getProperty(METASTORE_URI).isSet()) {
            catalogProperties.put(IcebergCatalogProperty.METASTORE_URI, context.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(WAREHOUSE_LOCATION).isSet()) {
            catalogProperties.put(IcebergCatalogProperty.WAREHOUSE_LOCATION, context.getProperty(WAREHOUSE_LOCATION).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet()) {
            configFilePaths = createFilePathList(context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue());
        }
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.HIVE;
    }
}
