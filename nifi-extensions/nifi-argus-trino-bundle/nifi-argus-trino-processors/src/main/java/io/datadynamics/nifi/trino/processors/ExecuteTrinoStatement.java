/*
 * Copyright 2026 Data Dynamics Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.trino.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.EXECUTION_MODE;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.MAX_BACKGROUND_QUERIES;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.QUERY_TIMEOUT;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.TRINO_CONNECTION_SERVICE;

@Tags({"trino", "sql", "ddl", "dml", "elt", "ctas", "merge", "insert", "lakehouse"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SeeAlso({QueryTrinoRecord.class, MaintainTrinoTable.class})
@CapabilityDescription("Trino 에서 DDL/DML/ELT 문장 한 개(CREATE TABLE AS, INSERT INTO ... SELECT, MERGE, DELETE, CALL 등)를 실행한다. "
        + "SQL 은 SQL Statement 프로퍼티나 입력 FlowFile 콘텐츠(UTF-8)로 받는다. Trino JDBC 는 한 번에 한 문장만 실행하므로 끝의 ';' 하나만 허용하고 "
        + "따옴표·주석 밖에 ';' 가 더 있으면 failure 로 보낸다. 결과 집합을 돌려주는 문장(SHOW 등)은 결과를 버리고 행 수만 trino.result.row.count 에 기록한다. "
        + "입력 없이 스케줄 실행도 할 수 있다. 오래 걸리는 문장은 Execution Mode=BACKGROUND 로 NiFi 스레드 점유를 피한다.")
@WritesAttributes({
        @WritesAttribute(attribute = TrinoAttributes.QUERY_ID, description = "Trino query id."),
        @WritesAttribute(attribute = TrinoAttributes.QUERY_STATE, description = "쿼리 최종 상태."),
        @WritesAttribute(attribute = TrinoAttributes.UPDATE_COUNT, description = "업데이트 수(문장이 돌려줄 때만)."),
        @WritesAttribute(attribute = TrinoAttributes.RESULT_ROW_COUNT, description = "결과 집합을 돌려주는 문장에서 버린 결과 행 수."),
        @WritesAttribute(attribute = TrinoAttributes.PROCESSED_ROWS, description = "Trino 가 처리한 입력 행 수."),
        @WritesAttribute(attribute = TrinoAttributes.PROCESSED_BYTES, description = "Trino 가 처리한 입력 바이트 수."),
        @WritesAttribute(attribute = TrinoAttributes.CPU_TIME_MS, description = "누적 CPU 시간(ms)."),
        @WritesAttribute(attribute = TrinoAttributes.WALL_TIME_MS, description = "누적 wall 시간(ms)."),
        @WritesAttribute(attribute = TrinoAttributes.QUEUED_TIME_MS, description = "대기열 시간(ms)."),
        @WritesAttribute(attribute = TrinoAttributes.ERROR_MESSAGE, description = "failure 로 보낼 때 오류 메시지."),
        @WritesAttribute(attribute = TrinoAttributes.ERROR_CODE, description = "failure 로 보낼 때 Trino 오류 코드.")
})
public class ExecuteTrinoStatement extends AbstractTrinoStatementProcessor {

    static final PropertyDescriptor SQL_STATEMENT = new PropertyDescriptor.Builder()
            .name("SQL Statement")
            .displayName("SQL 문장")
            .description("실행할 문장 한 개. 비우면 입력 FlowFile 콘텐츠(UTF-8)를 문장으로 쓴다. 끝의 ';' 하나는 제거한다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            TRINO_CONNECTION_SERVICE, SQL_STATEMENT, QUERY_TIMEOUT, EXECUTION_MODE, MAX_BACKGROUND_QUERIES);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected void checkSchedulable(final ProcessContext context) {
        if (!context.getProperty(SQL_STATEMENT).isSet() && !context.hasIncomingConnection()) {
            throw new ProcessException("SQL Statement 가 비어 있고 입력 연결도 없다. 문장을 지정하거나 문장을 담은 FlowFile 을 연결해야 한다");
        }
    }

    @Override
    protected StatementRequest createRequest(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        final String sql;
        if (context.getProperty(SQL_STATEMENT).isSet()) {
            sql = context.getProperty(SQL_STATEMENT).evaluateAttributeExpressions(flowFile).getValue();
        } else if (flowFile != null) {
            try (InputStream in = session.read(flowFile)) {
                sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            } catch (final IOException e) {
                throw new ProcessException("입력 FlowFile 콘텐츠를 읽을 수 없다", e);
            }
        } else {
            throw new IllegalArgumentException("SQL Statement 가 비어 있고 입력 FlowFile 도 없다");
        }
        return new StatementRequest(SqlStatements.requireSingleStatement(sql), Map.of());
    }
}
