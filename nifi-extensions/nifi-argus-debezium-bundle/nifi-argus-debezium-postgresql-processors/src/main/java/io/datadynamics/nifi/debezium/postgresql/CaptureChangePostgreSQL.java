/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.debezium.postgresql;

import io.datadynamics.nifi.debezium.AbstractDebeziumCDCProcessor;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Properties;

@PrimaryNodeOnly
@TriggerSerially
@Stateful(scopes = Scope.LOCAL, description = "오프셋은 Storage Directory에 파일로 보존된다. PostgreSQL은 스키마 이력을 사용하지 않는다.")
@Tags({"cdc", "debezium", "postgresql", "postgres", "change data capture", "sql", "source"})
@CapabilityDescription("Debezium Embedded Engine으로 PostgreSQL 논리 복제(logical decoding)에서 행 수준 변경 이벤트를 캡처하여 JSON FlowFile로 방출한다.")
@WritesAttributes({
        @WritesAttribute(attribute = "cdc.key", description = "변경 이벤트 키의 JSON 직렬화."),
        @WritesAttribute(attribute = "cdc.destination", description = "이벤트의 논리 대상(일반적으로 topicPrefix.schema.table)."),
        @WritesAttribute(attribute = "cdc.connector", description = "이벤트를 생성한 Debezium 커넥터 클래스.")
})
public class CaptureChangePostgreSQL extends AbstractDebeziumCDCProcessor {

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("database-name")
            .displayName("데이터베이스 이름")
            .description("연결할 PostgreSQL 데이터베이스 이름. Debezium database.dbname으로 매핑.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor PLUGIN_NAME = new PropertyDescriptor.Builder()
            .name("plugin-name")
            .displayName("디코딩 플러그인")
            .description("논리 디코딩 출력 플러그인. Debezium plugin.name으로 매핑.")
            .required(true)
            .allowableValues("pgoutput", "decoderbufs")
            .defaultValue("pgoutput")
            .build();

    static final PropertyDescriptor SLOT_NAME = new PropertyDescriptor.Builder()
            .name("slot-name")
            .displayName("복제 슬롯 이름")
            .description("이 커넥터가 사용할 복제 슬롯 이름. Debezium slot.name으로 매핑. 커넥터마다 고유해야 한다.")
            .required(true)
            .defaultValue("debezium")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor PUBLICATION_NAME = new PropertyDescriptor.Builder()
            .name("publication-name")
            .displayName("Publication 이름")
            .description("pgoutput 플러그인이 사용할 publication 이름. Debezium publication.name으로 매핑.")
            .required(false)
            .defaultValue("dbz_publication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    @Override
    protected String getConnectorClass() {
        return "io.debezium.connector.postgresql.PostgresConnector";
    }

    @Override
    protected boolean requiresSchemaHistory() {
        return false;
    }

    @Override
    protected List<PropertyDescriptor> getConnectorPropertyDescriptors() {
        return List.of(DATABASE_NAME, PLUGIN_NAME, SLOT_NAME, PUBLICATION_NAME);
    }

    @Override
    protected void addConnectorConfiguration(final ProcessContext context, final Properties props) {
        props.setProperty("database.dbname", context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue());
        props.setProperty("plugin.name", context.getProperty(PLUGIN_NAME).getValue());
        props.setProperty("slot.name", context.getProperty(SLOT_NAME).evaluateAttributeExpressions().getValue());
        if (context.getProperty(PUBLICATION_NAME).isSet()) {
            props.setProperty("publication.name", context.getProperty(PUBLICATION_NAME).evaluateAttributeExpressions().getValue());
        }
    }
}
