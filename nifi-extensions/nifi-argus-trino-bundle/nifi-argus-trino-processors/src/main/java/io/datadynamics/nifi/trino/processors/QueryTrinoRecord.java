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

import io.datadynamics.nifi.trino.api.TrinoConnectionService;
import io.datadynamics.nifi.trino.api.TrinoQueryInfo;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.QUERY_TIMEOUT;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.TRINO_CONNECTION_SERVICE;

@Tags({"trino", "sql", "query", "select", "record", "lakehouse", "iceberg", "delta"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Trino 에서 SELECT 를 실행하고 결과를 Record Writer 로 스트리밍한다. 쿼리는 SQL Query 프로퍼티나 입력 FlowFile 콘텐츠(UTF-8)로 받는다. "
        + "결과 FlowFile 에 Trino query id·실행 통계를 기록해 Trino UI 와 연결할 수 있고, 프로세서를 중지하면 실행 중인 쿼리를 취소한다(USER_CANCELED). "
        + "결과가 0건이어도 스키마만 있는 FlowFile 1개를 낸다(record.count=0). 쓰기 도중 실패하면 부분 결과를 내보내지 않는다.")
@WritesAttributes({
        @WritesAttribute(attribute = TrinoAttributes.QUERY_ID, description = "Trino query id."),
        @WritesAttribute(attribute = TrinoAttributes.QUERY_STATE, description = "쿼리 최종 상태(예: FINISHED)."),
        @WritesAttribute(attribute = TrinoAttributes.PROCESSED_ROWS, description = "Trino 가 처리한 입력 행 수."),
        @WritesAttribute(attribute = TrinoAttributes.PROCESSED_BYTES, description = "Trino 가 처리한 입력 바이트 수."),
        @WritesAttribute(attribute = TrinoAttributes.CPU_TIME_MS, description = "누적 CPU 시간(ms)."),
        @WritesAttribute(attribute = TrinoAttributes.WALL_TIME_MS, description = "누적 wall 시간(ms)."),
        @WritesAttribute(attribute = TrinoAttributes.QUEUED_TIME_MS, description = "대기열 시간(ms)."),
        @WritesAttribute(attribute = TrinoAttributes.RECORD_COUNT, description = "이 FlowFile 의 레코드 수."),
        @WritesAttribute(attribute = "mime.type", description = "Record Writer 가 정한 MIME 타입."),
        @WritesAttribute(attribute = "fragment.identifier", description = "Max Rows Per FlowFile > 0 일 때 같은 쿼리 결과 FlowFile 의 공통 ID."),
        @WritesAttribute(attribute = "fragment.index", description = "Max Rows Per FlowFile > 0 일 때 0부터 시작하는 순번."),
        @WritesAttribute(attribute = "fragment.count", description = "Max Rows Per FlowFile > 0 일 때 결과 FlowFile 총수."),
        @WritesAttribute(attribute = TrinoAttributes.ERROR_MESSAGE, description = "failure 로 보낼 때 오류 메시지."),
        @WritesAttribute(attribute = TrinoAttributes.ERROR_CODE, description = "failure 로 보낼 때 Trino 오류 코드(SQLException.getErrorCode).")
})
public class QueryTrinoRecord extends AbstractProcessor {

    static final PropertyDescriptor SQL_QUERY = new PropertyDescriptor.Builder()
            .name("SQL Query")
            .displayName("SQL 쿼리")
            .description("실행할 SELECT 문 한 개. 비우면 입력 FlowFile 콘텐츠(UTF-8)를 쿼리로 쓴다. 끝의 ';' 하나는 제거한다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("결과 레코드를 쓸 Record Writer.")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    static final PropertyDescriptor MAX_ROWS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("Max Rows Per FlowFile")
            .displayName("FlowFile 당 최대 행 수")
            .description("결과를 이 행 수마다 FlowFile 로 나눈다. 0 이면 전부 한 FlowFile. 0 보다 크면 fragment.* 속성을 붙인다.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .displayName("Fetch Size")
            .description("Statement.setFetchSize 힌트. 0 이면 드라이버 기본값.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("쿼리 결과 FlowFile.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("쿼리 실패 시 입력 FlowFile(trino.error.* 속성 포함). 입력이 없으면 로그 후 yield 한다.")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("쿼리가 성공한 입력 FlowFile.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            TRINO_CONNECTION_SERVICE, SQL_QUERY, RECORD_WRITER, MAX_ROWS_PER_FLOWFILE, FETCH_SIZE, QUERY_TIMEOUT);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL);

    private static final int DEFAULT_PRECISION = 10;
    private static final int DEFAULT_SCALE = 0;

    private final RunningStatements running = new RunningStatements();
    private volatile boolean stopping;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        stopping = false;
        if (!context.getProperty(SQL_QUERY).isSet() && !context.hasIncomingConnection()) {
            throw new ProcessException("SQL Query 가 비어 있고 입력 연결도 없다. 쿼리를 지정하거나 쿼리를 담은 FlowFile 을 연결해야 한다");
        }
    }

    @OnUnscheduled
    public void onUnscheduled() {
        stopping = true;
        running.cancelAll(getLogger());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = null;
        if (context.hasIncomingConnection()) {
            input = session.get();
            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final String sql;
        try {
            sql = SqlStatements.requireSingleStatement(readSql(context, session, input));
        } catch (final IllegalArgumentException e) {
            handleFailure(context, session, input, e, null);
            return;
        }

        final TrinoConnectionService service = context.getProperty(TRINO_CONNECTION_SERVICE).asControllerService(TrinoConnectionService.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final int maxRows = context.getProperty(MAX_ROWS_PER_FLOWFILE).asInteger();
        final int fetchSize = context.getProperty(FETCH_SIZE).asInteger();
        final int timeoutSeconds = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final Map<String, String> inputAttributes = input == null ? Map.of() : input.getAttributes();

        final long start = System.nanoTime();
        final List<FlowFile> outputs = new ArrayList<>();
        Statement statement = null;
        TrinoQueryInfo info = null;
        try (Connection connection = service.getConnection(inputAttributes)) {
            statement = connection.createStatement();
            running.add(statement);
            if (stopping) {
                throw new SQLException("프로세서가 중지 중이라 쿼리를 시작하지 않았다");
            }
            if (fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
            if (timeoutSeconds > 0) {
                statement.setQueryTimeout(timeoutSeconds);
            }
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                writeResults(session, input, resultSet, writerFactory, inputAttributes, maxRows, outputs);
                info = service.getQueryInfo(resultSet);
            }
            if (info == null) {
                info = service.getQueryInfo(statement);
            }
        } catch (final SQLException | ProcessException | IOException e) {
            session.remove(outputs);
            final TrinoQueryInfo failedInfo = statement == null ? null : service.getQueryInfo(statement);
            closeQuietly(statement);
            running.remove(statement);
            if (stopping) {
                getLogger().info("프로세서 중지로 쿼리가 취소되어 세션을 되돌린다: {}", e.getMessage());
                session.rollback();
                return;
            }
            handleFailure(context, session, input, e, failedInfo);
            return;
        }
        closeQuietly(statement);
        running.remove(statement);

        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        final String transitUri = TrinoAttributes.transitUri(service.getJdbcUrl(), info);
        final String fragmentId = UUID.randomUUID().toString();
        final List<FlowFile> finished = new ArrayList<>(outputs.size());
        for (int i = 0; i < outputs.size(); i++) {
            final Map<String, String> attributes = new HashMap<>();
            TrinoAttributes.putQueryInfo(attributes, info);
            if (maxRows > 0) {
                attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
                attributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), Integer.toString(i));
                attributes.put(FragmentAttributes.FRAGMENT_COUNT.key(), Integer.toString(outputs.size()));
            }
            final FlowFile output = session.putAllAttributes(outputs.get(i), attributes);
            if (input == null) {
                session.getProvenanceReporter().receive(output, transitUri, elapsedMillis);
            } else {
                session.getProvenanceReporter().fetch(output, transitUri, elapsedMillis);
            }
            finished.add(output);
        }
        session.transfer(finished, REL_SUCCESS);
        if (input != null) {
            session.transfer(input, REL_ORIGINAL);
        }
        getLogger().debug("Trino 쿼리 {} 완료: FlowFile {}개, {}ms", info == null ? "?" : info.queryId(), finished.size(), elapsedMillis);
    }

    private void writeResults(final ProcessSession session, final FlowFile input, final ResultSet resultSet,
                              final RecordSetWriterFactory writerFactory, final Map<String, String> inputAttributes,
                              final int maxRows, final List<FlowFile> outputs) throws SQLException, IOException {
        final ResultSetRecordSet recordSet = new ResultSetRecordSet(resultSet, null, DEFAULT_PRECISION, DEFAULT_SCALE, true);
        final RecordSchema writeSchema;
        try {
            writeSchema = writerFactory.getSchema(inputAttributes, recordSet.getSchema());
        } catch (final Exception e) {
            throw new ProcessException("Record Writer 스키마를 정할 수 없다: " + e.getMessage(), e);
        }

        final Record[] pending = {recordSet.next()};
        do {
            FlowFile output = input == null ? session.create() : session.create(input);
            outputs.add(output);
            final WriteResult[] result = new WriteResult[1];
            final String[] mimeType = new String[1];
            output = session.write(output, out -> {
                try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, inputAttributes)) {
                    writer.beginRecordSet();
                    int written = 0;
                    while (pending[0] != null && (maxRows == 0 || written < maxRows)) {
                        writer.write(pending[0]);
                        written++;
                        pending[0] = recordSet.next();
                    }
                    result[0] = writer.finishRecordSet();
                    mimeType[0] = writer.getMimeType();
                } catch (final IOException e) {
                    throw e;
                } catch (final Exception e) {
                    throw new IOException(e.getMessage(), e);
                }
            });
            final Map<String, String> attributes = new HashMap<>(result[0].getAttributes());
            attributes.put(TrinoAttributes.RECORD_COUNT, Integer.toString(result[0].getRecordCount()));
            if (mimeType[0] != null) {
                attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType[0]);
            }
            output = session.putAllAttributes(output, attributes);
            outputs.set(outputs.size() - 1, output);
        } while (pending[0] != null);
    }

    private String readSql(final ProcessContext context, final ProcessSession session, final FlowFile input) {
        if (context.getProperty(SQL_QUERY).isSet()) {
            return context.getProperty(SQL_QUERY).evaluateAttributeExpressions(input).getValue();
        }
        if (input == null) {
            throw new IllegalArgumentException("SQL Query 가 비어 있고 입력 FlowFile 도 없다");
        }
        try (InputStream in = session.read(input)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new ProcessException("입력 FlowFile 콘텐츠를 읽을 수 없다", e);
        }
    }

    private void handleFailure(final ProcessContext context, final ProcessSession session, final FlowFile input,
                               final Exception error, final TrinoQueryInfo info) {
        if (input == null) {
            getLogger().error("Trino 쿼리 실패", error);
            context.yield();
            return;
        }
        final Map<String, String> attributes = new HashMap<>();
        TrinoAttributes.putError(attributes, error, info);
        getLogger().error("Trino 쿼리 실패: {}", input, error);
        FlowFile failed = session.putAllAttributes(input, attributes);
        failed = session.penalize(failed);
        session.transfer(failed, REL_FAILURE);
    }

    private static void closeQuietly(final Statement statement) {
        if (statement == null) {
            return;
        }
        try {
            statement.close();
        } catch (final SQLException ignored) {
            // 이미 닫힘
        }
    }
}
