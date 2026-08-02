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
package io.datadynamics.nifi.debezium.oracle;

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
@Stateful(scopes = Scope.LOCAL, description = "오프셋과 스키마 이력은 Storage Directory에 파일로 보존된다.")
@Tags({"cdc", "debezium", "oracle", "logminer", "change data capture", "sql", "source"})
@CapabilityDescription("Debezium Embedded Engine으로 Oracle LogMiner에서 행 수준 변경 이벤트를 캡처하여 JSON FlowFile로 방출한다.")
@WritesAttributes({
        @WritesAttribute(attribute = "cdc.key", description = "변경 이벤트 키의 JSON 직렬화."),
        @WritesAttribute(attribute = "cdc.destination", description = "이벤트의 논리 대상(일반적으로 topicPrefix.schema.table)."),
        @WritesAttribute(attribute = "cdc.connector", description = "이벤트를 생성한 Debezium 커넥터 클래스.")
})
public class CaptureChangeOracle extends AbstractDebeziumCDCProcessor {

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("database-name")
            .displayName("데이터베이스 이름")
            .description("연결할 Oracle 데이터베이스/서비스 이름. Debezium database.dbname으로 매핑.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor PDB_NAME = new PropertyDescriptor.Builder()
            .name("pdb-name")
            .displayName("PDB(Pluggable Database) 이름")
            .description("멀티테넌트(CDB/PDB) 구성에서 PDB 이름. Debezium database.pdb.name으로 매핑. 비 CDB 환경이면 비워둔다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor SCHEMA_INCLUDE_LIST = new PropertyDescriptor.Builder()
            .name("schema-include-list")
            .displayName("스키마 포함 목록")
            .description("캡처할 스키마 정규식 목록(쉼표 구분). Debezium schema.include.list로 매핑.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    @Override
    protected String getConnectorClass() {
        return "io.debezium.connector.oracle.OracleConnector";
    }

    @Override
    protected List<PropertyDescriptor> getConnectorPropertyDescriptors() {
        return List.of(DATABASE_NAME, PDB_NAME, SCHEMA_INCLUDE_LIST);
    }

    @Override
    protected void addConnectorConfiguration(final ProcessContext context, final Properties props) {
        props.setProperty("database.dbname", context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue());
        if (context.getProperty(PDB_NAME).isSet()) {
            props.setProperty("database.pdb.name", context.getProperty(PDB_NAME).evaluateAttributeExpressions().getValue());
        }
        if (context.getProperty(SCHEMA_INCLUDE_LIST).isSet()) {
            props.setProperty("schema.include.list", context.getProperty(SCHEMA_INCLUDE_LIST).evaluateAttributeExpressions().getValue());
        }
    }
}
