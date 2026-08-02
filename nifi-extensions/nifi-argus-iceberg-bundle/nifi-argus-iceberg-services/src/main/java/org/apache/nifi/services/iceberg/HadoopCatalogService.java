/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-services/src/main/java/org/apache/nifi/services/iceberg/HadoopCatalogService.java
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
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.services.iceberg.IcebergCatalogProperty.WAREHOUSE_LOCATION;

@Tags({"iceberg", "catalog", "service", "hadoop", "hdfs"})
@CapabilityDescription("HDFS 또는 원자적 rename을 지원하는 유사한 파일 시스템을 사용할 수 있는 카탈로그 서비스.")
public class HadoopCatalogService extends AbstractCatalogService {

    static final PropertyDescriptor WAREHOUSE_PATH = new PropertyDescriptor.Builder()
            .name("warehouse-path")
            .displayName("웨어하우스 경로")
            .description("웨어하우스 위치의 경로.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            WAREHOUSE_PATH,
            HADOOP_CONFIGURATION_RESOURCES
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    // 컨트롤러 서비스가 활성화될 때 Hadoop 설정 파일 경로와 웨어하우스 위치 프로퍼티를
    // 읽어들여 카탈로그 프로퍼티 맵을 채운다.
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        if (context.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet()) {
            configFilePaths = createFilePathList(context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue());
        }

        catalogProperties.put(WAREHOUSE_LOCATION, context.getProperty(WAREHOUSE_PATH).evaluateAttributeExpressions().getValue());
    }

    @Override
    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.HADOOP;
    }

}
