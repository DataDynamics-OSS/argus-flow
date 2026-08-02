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
package io.datadynamics.nifi.debezium.mysql;

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
@Tags({"cdc", "debezium", "mysql", "change data capture", "sql", "source"})
@CapabilityDescription("Debezium Embedded Engine으로 MySQL binlog에서 행 수준 변경 이벤트를 캡처하여 JSON FlowFile로 방출한다.")
@WritesAttributes({
        @WritesAttribute(attribute = "cdc.key", description = "변경 이벤트 키의 JSON 직렬화."),
        @WritesAttribute(attribute = "cdc.destination", description = "이벤트의 논리 대상(일반적으로 topicPrefix.database.table)."),
        @WritesAttribute(attribute = "cdc.connector", description = "이벤트를 생성한 Debezium 커넥터 클래스.")
})
public class CaptureChangeMySQL extends AbstractDebeziumCDCProcessor {

    static final PropertyDescriptor DATABASE_SERVER_ID = new PropertyDescriptor.Builder()
            .name("database-server-id")
            .displayName("서버 ID")
            .description("이 커넥터를 MySQL 복제 클라이언트로 식별하는 고유 숫자 ID. 클러스터의 다른 replica와 겹치면 안 된다. Debezium database.server.id로 매핑.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor DATABASE_INCLUDE_LIST = new PropertyDescriptor.Builder()
            .name("database-include-list")
            .displayName("데이터베이스 포함 목록")
            .description("캡처할 데이터베이스 정규식 목록(쉼표 구분). Debezium database.include.list로 매핑. 비우면 전체 대상.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    @Override
    protected String getConnectorClass() {
        return "io.debezium.connector.mysql.MySqlConnector";
    }

    @Override
    protected List<PropertyDescriptor> getConnectorPropertyDescriptors() {
        return List.of(DATABASE_SERVER_ID, DATABASE_INCLUDE_LIST);
    }

    @Override
    protected void addConnectorConfiguration(final ProcessContext context, final Properties props) {
        props.setProperty("database.server.id", context.getProperty(DATABASE_SERVER_ID).evaluateAttributeExpressions().getValue());
        if (context.getProperty(DATABASE_INCLUDE_LIST).isSet()) {
            props.setProperty("database.include.list", context.getProperty(DATABASE_INCLUDE_LIST).evaluateAttributeExpressions().getValue());
        }
    }
}
