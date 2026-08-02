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
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/AbstractExecuteSQL.java
 */
package io.datadynamics.nifi.processors.db.executesql;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * SQL SELECT 쿼리(또는 하위 클래스가 구성한 쿼리)를 실행하여 그 결과 집합을 FlowFile로 만들어내는
 * 프로세서들의 공통 추상 클래스.
 * <p>
 * 주요 책임:
 * - DBCP 커넥션 풀에서 연결을 획득하고 SQL Pre/Post-Query, 본 쿼리를 순서대로 실행한다.
 * - 쿼리 결과(ResultSet)를 하위 클래스가 제공하는 {@link SqlWriter}를 통해 원하는 포맷(Avro, CSV 등)으로
 *   직렬화하여 FlowFile 콘텐츠로 기록한다.
 * - 최대 행 수(Max Rows Per Flow File) 설정에 따라 결과를 여러 FlowFile로 분할(fragment)하고,
 *   출력 배치 크기(Output Batch Size)에 따라 세션 커밋 시점을 제어한다.
 * - 실행 통계(행 수, 실행 시간, fetch 시간 등)를 FlowFile 속성으로 기록하고, 실패 시 원본 FlowFile을
 *   페널티와 함께 실패(failure) 관계로 라우팅한다.
 * <p>
 * 실제 SQL 실행 결과를 어떤 포맷으로 직렬화할지는 하위 클래스가 {@link #configureSqlWriter}를 구현하여 결정한다.
 */
public abstract class AbstractExecuteSQL extends AbstractProcessor {

    // 처리된 결과 로우 수를 담는 FlowFile 속성 키
    public static final String RESULT_ROW_COUNT = "executesql.row.count";
    // 쿼리 실행 + 결과 fetch에 소요된 총 시간(ms)을 담는 FlowFile 속성 키
    public static final String RESULT_QUERY_DURATION = "executesql.query.duration";
    // 쿼리 실행 자체에 소요된 시간(ms)을 담는 FlowFile 속성 키
    public static final String RESULT_QUERY_EXECUTION_TIME = "executesql.query.executiontime";
    // 결과 집합을 fetch하는 데 소요된 시간(ms)을 담는 FlowFile 속성 키
    public static final String RESULT_QUERY_FETCH_TIME = "executesql.query.fetchtime";
    // 하나의 쿼리 실행이 여러 결과 집합을 반환할 때, 몇 번째 결과 집합인지를 담는 FlowFile 속성 키
    public static final String RESULTSET_INDEX = "executesql.resultset.index";
    // 쿼리 실행 실패 시 오류 메시지를 담는 FlowFile 속성 키
    public static final String RESULT_ERROR_MESSAGE = "executesql.error.message";
    // 입력 FlowFile의 UUID를 결과 FlowFile에 전달하기 위한 속성 키
    public static final String INPUT_FLOWFILE_UUID = "input.flowfile.uuid";

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("SQL 쿼리 결과 집합으로부터 FlowFile을 성공적으로 생성했습니다.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL 쿼리 실행에 실패했습니다. 입력 FlowFile은 페널티가 부여된 후 이 관계로 라우팅됩니다.")
            .build();
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .displayName("데이터베이스 커넥션 풀링 서비스")
            .description("데이터베이스에 대한 연결을 획득하는 데 사용되는 컨트롤러 서비스.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    public static final PropertyDescriptor SQL_PRE_QUERY = new PropertyDescriptor.Builder()
            .name("sql-pre-query")
            .displayName("SQL 사전 쿼리")
            .description("본 SQL 쿼리가 실행되기 전에 실행할, 세미콜론으로 구분된 쿼리 목록입니다. "
                    + "예를 들어 본 쿼리 실행 전에 세션 속성을 설정하는 용도로 사용할 수 있습니다. "
                    + "쿼리문 자체에 세미콜론을 포함해야 한다면 백슬래시로 이스케이프하여('\\;') 사용할 수 있습니다. "
                    + "오류가 없는 한 이 쿼리들의 결과/출력은 표시되지 않습니다.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL select query")
            .displayName("SQL SELECT 쿼리")
            .description("실행할 SQL SELECT 쿼리입니다. 이 쿼리는 비어 있거나, 상수 값이거나, Expression Language를 사용하여 "
                    + "속성으로부터 구성될 수 있습니다. 이 프로퍼티가 지정되어 있으면 입력 FlowFile의 콘텐츠와 무관하게 이 값이 "
                    + "사용됩니다. 이 프로퍼티가 비어 있으면 입력 FlowFile의 콘텐츠가 프로세서가 데이터베이스에 전달할 유효한 SQL "
                    + "SELECT 쿼리를 담고 있어야 합니다. 단, FlowFile 콘텐츠에 대해서는 Expression Language가 평가되지 않는다는 "
                    + "점에 유의하십시오.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor SQL_POST_QUERY = new PropertyDescriptor.Builder()
            .name("sql-post-query")
            .displayName("SQL 사후 쿼리")
            .description("본 SQL 쿼리가 실행된 후에 실행할, 세미콜론으로 구분된 쿼리 목록입니다. "
                    + "예를 들어 본 쿼리 실행 후에 세션 속성을 설정하는 용도로 사용할 수 있습니다. "
                    + "쿼리문 자체에 세미콜론을 포함해야 한다면 백슬래시로 이스케이프하여('\\;') 사용할 수 있습니다. "
                    + "오류가 없는 한 이 쿼리들의 결과/출력은 표시되지 않습니다.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .displayName("최대 대기 시간")
            .description("실행 중인 SQL SELECT 쿼리에 허용되는 최대 시간입니다. "
                    + "0이면 제한이 없음을 의미합니다. 1초 미만의 값은 0으로 취급됩니다.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("esql-max-rows")
            .displayName("FlowFile당 최대 행 수")
            .description("하나의 FlowFile에 포함될 최대 결과 행 수입니다. 이 값을 설정하면 매우 큰 결과 집합을 여러 FlowFile로 "
                    + "나눌 수 있습니다. 0으로 지정하면 모든 행이 하나의 FlowFile로 반환됩니다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("esql-output-batch-size")
            .displayName("출력 배치 크기")
            .description("프로세스 세션을 커밋하기 전에 큐에 쌓아 둘 출력 FlowFile의 개수입니다. 0으로 설정하면 모든 결과 행이 "
                    + "처리되고 출력 FlowFile이 다운스트림 관계로 전달될 준비가 된 시점에 세션이 커밋됩니다. 결과 집합이 클 경우 "
                    + "프로세서 실행이 끝나는 시점에 대량의 FlowFile이 한꺼번에 전달될 수 있습니다. 이 프로퍼티를 설정하면 지정한 "
                    + "개수만큼의 FlowFile이 전달 준비가 될 때마다 세션이 커밋되어 해당 FlowFile들이 다운스트림 관계로 전달됩니다. "
                    + "참고: 이 프로퍼티가 설정되면 FlowFile에 fragment.count 속성이 설정되지 않습니다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("esql-fetch-size")
            .displayName("Fetch 크기")
            .description("한 번에 결과 집합에서 가져올(fetch) 결과 행 수입니다. 이는 데이터베이스 드라이버에 대한 힌트일 뿐이며 "
                    + "정확히 지켜지지 않을 수 있습니다. 0으로 지정하면 이 힌트는 무시됩니다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("esql-auto-commit")
            .displayName("Auto Commit 설정")
            .description("DB 연결의 auto commit(자동 커밋) 기능을 활성화하거나 비활성화합니다. 기본값은 'true'입니다. "
                    + "이 프로세서는 데이터를 읽는 용도로 사용되므로 대부분의 JDBC 드라이버에서는 기본값을 사용해도 별다른 영향이 "
                    + "없습니다. 다만 PostgreSQL 드라이버와 같은 일부 JDBC 드라이버의 경우, 한 번에 가져오는 결과 행 수를 "
                    + "제한하려면 auto commit 기능을 비활성화해야 합니다. "
                    + "auto commit이 활성화되어 있으면 PostgreSQL 드라이버는 결과 집합 전체를 한 번에 메모리로 로드합니다. "
                    + "이는 대용량 데이터를 조회하는 쿼리를 실행할 때 많은 메모리를 사용하게 되는 원인이 될 수 있습니다. "
                    + "PostgreSQL 드라이버의 이러한 동작에 대한 자세한 내용은 https://jdbc.postgresql.org//documentation/head/query.html 을 참고하십시오. ")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    protected Set<Relationship> relationships;
    protected List<PropertyDescriptor> propDescriptors;

    protected DBCPService dbcpService;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    /**
     * 프로세서가 스케줄될 때(실행 시작 시) 한 번 호출되는 초기화 메서드.
     * SQL Select Query 프로퍼티가 설정되어 있지 않은데 입력 커넥션도 없다면, 쿼리를 어디서도 얻을 수 없는
     * 잘못된 구성이므로 즉시 실패시킨다(입력 FlowFile 콘텐츠로부터 쿼리를 읽어올 방법이 없기 때문).
     * 이후 실제 쿼리 실행에 사용할 DBCPService(커넥션 풀)를 미리 얻어 캐시해 둔다.
     */
    @OnScheduled
    public void setup(ProcessContext context) {
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(SQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

    }

    /**
     * 프로세서의 핵심 실행 로직.
     * <p>
     * 처리 흐름 개요:
     * 1) 입력 커넥션이 있으면 FlowFile을 가져온다(self-loop만 있는 경우는 FlowFile 없이도 계속 진행).
     * 2) Pre-Query, 본 SELECT 쿼리, Post-Query에 사용할 값들을 프로퍼티/FlowFile 콘텐츠에서 구성한다.
     * 3) DBCP 커넥션을 획득하고 PreparedStatement를 준비, fetchSize/queryTimeout을 적용한 뒤 Pre-Query를 실행한다.
     * 4) 동적(dynamic) 프로퍼티와 FlowFile 속성을 SQL 파라미터로 바인딩하고 본 쿼리를 실행한다.
     * 5) 쿼리가 여러 결과 집합을 반환할 수 있으므로 getMoreResults()로 순회하면서, 결과 집합마다
     *    SqlWriter로 직렬화하여 새 FlowFile을 만들고 maxRowsPerFlowFile 단위로 분할(fragment)한다.
     * 6) outputBatchSize에 도달하면 세션을 커밋하여 대량의 FlowFile이 한꺼번에 몰리지 않도록 한다.
     * 7) Post-Query 실행 후 auto-commit이 꺼져 있으면 명시적으로 commit()한다.
     * 8) 예외 발생 시 원본 FlowFile을 실패(failure) 관계로 라우팅하고, 생성 중이던 결과 FlowFile은 정리한다.
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();
        final int queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();

        List<String> preQueries = getQueries(context.getProperty(SQL_PRE_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
        List<String> postQueries = getQueries(context.getProperty(SQL_POST_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());

        SqlWriter sqlWriter = configureSqlWriter(session, context, fileToProcess);

        String selectQuery;
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, Charset.defaultCharset())));
            selectQuery = queryContents.toString();
        }

        int resultCount = 0;
        try (final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes())) {
            con.setAutoCommit(context.getProperty(AUTO_COMMIT).asBoolean());
            try (final PreparedStatement st = con.prepareStatement(selectQuery)) {
                if (fetchSize != null && fetchSize > 0) {
                    try {
                        st.setFetchSize(fetchSize);
                    } catch (SQLException se) {
                        // Not all drivers support this, just log the error (at debug level) and move on
                        logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                    }
                }
                st.setQueryTimeout(queryTimeout); // timeout in seconds

                // Execute pre-query, throw exception and cleanup Flow Files if fail
                Pair<String, SQLException> failure = executeConfigStatements(con, preQueries);
                if (failure != null) {
                    // In case of failure, assigning config query to "selectQuery" to follow current error handling
                    selectQuery = failure.getLeft();
                    throw failure.getRight();
                }

                final Map<String, SensitiveValueWrapper> sqlParameters = context.getProperties()
                        .entrySet()
                        .stream()
                        .filter(e -> e.getKey().isDynamic())
                        .collect(Collectors.toMap(e -> e.getKey().getName(), e -> new SensitiveValueWrapper(e.getValue(), e.getKey().isSensitive())));

                if (fileToProcess != null) {
                    for (Map.Entry<String, String> entry : fileToProcess.getAttributes().entrySet()) {
                        sqlParameters.put(entry.getKey(), new SensitiveValueWrapper(entry.getValue(), false));
                    }
                }

                if (!sqlParameters.isEmpty()) {
                    JdbcCommon.setSensitiveParameters(st, sqlParameters);
                }

                logger.debug("Executing query {}", selectQuery);

                int fragmentIndex = 0;
                final String fragmentId = UUID.randomUUID().toString();

                final StopWatch executionTime = new StopWatch(true);

                boolean hasResults = st.execute();

                long executionTimeElapsed = executionTime.getElapsed(TimeUnit.MILLISECONDS);

                boolean hasUpdateCount = st.getUpdateCount() != -1;

                Map<String, String> inputFileAttrMap = fileToProcess == null ? null : fileToProcess.getAttributes();
                String inputFileUUID = fileToProcess == null ? null : fileToProcess.getAttribute(CoreAttributes.UUID.key());
                while (hasResults || hasUpdateCount) {
                    //getMoreResults() and execute() return false to indicate that the result of the statement is just a number and not a ResultSet
                    if (hasResults) {
                        final AtomicLong nrOfRows = new AtomicLong(0L);

                        try {
                            final ResultSet resultSet = st.getResultSet();
                            do {
                                final StopWatch fetchTime = new StopWatch(true);

                                FlowFile resultSetFF;
                                if (fileToProcess == null) {
                                    resultSetFF = session.create();
                                } else {
                                    resultSetFF = session.create(fileToProcess);
                                }

                                if (inputFileAttrMap != null) {
                                    resultSetFF = session.putAllAttributes(resultSetFF, inputFileAttrMap);
                                }


                                try {
                                    resultSetFF = session.write(resultSetFF, out -> {
                                        try {
                                            nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), null));
                                        } catch (Exception e) {
                                            throw (e instanceof ProcessException) ? (ProcessException) e : new ProcessException(e);
                                        }
                                    });

                                    long fetchTimeElapsed = fetchTime.getElapsed(TimeUnit.MILLISECONDS);

                                    // set attributes
                                    final Map<String, String> attributesToAdd = new HashMap<>();
                                    attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                                    attributesToAdd.put(RESULT_QUERY_DURATION, String.valueOf(executionTimeElapsed + fetchTimeElapsed));
                                    attributesToAdd.put(RESULT_QUERY_EXECUTION_TIME, String.valueOf(executionTimeElapsed));
                                    attributesToAdd.put(RESULT_QUERY_FETCH_TIME, String.valueOf(fetchTimeElapsed));
                                    attributesToAdd.put(RESULTSET_INDEX, String.valueOf(resultCount));
                                    if (inputFileUUID != null) {
                                        attributesToAdd.put(INPUT_FLOWFILE_UUID, inputFileUUID);
                                    }
                                    attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
                                    resultSetFF = session.putAllAttributes(resultSetFF, attributesToAdd);
                                    sqlWriter.updateCounters(session);

                                    // if fragmented ResultSet, determine if we should keep this fragment; set fragment attributes
                                    if (maxRowsPerFlowFile > 0) {
                                        // if row count is zero and this is not the first fragment, drop it instead of committing it.
                                        if (nrOfRows.get() == 0 && fragmentIndex > 0) {
                                            session.remove(resultSetFF);
                                            break;
                                        }

                                        resultSetFF = session.putAttribute(resultSetFF, FRAGMENT_ID, fragmentId);
                                        resultSetFF = session.putAttribute(resultSetFF, FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                                    }

                                    logger.info("{} contains {} records; transferring to 'success'", resultSetFF, nrOfRows.get());

                                    // Report a FETCH event if there was an incoming flow file, or a RECEIVE event otherwise
                                    if (context.hasIncomingConnection()) {
                                        session.getProvenanceReporter().fetch(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                                    } else {
                                        session.getProvenanceReporter().receive(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                                    }
                                    resultSetFlowFiles.add(resultSetFF);

                                    // If we've reached the batch size, send out the flow files
                                    if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                                        session.transfer(resultSetFlowFiles, REL_SUCCESS);
                                        // Need to remove the original input file if it exists
                                        if (fileToProcess != null) {
                                            session.remove(fileToProcess);
                                            fileToProcess = null;
                                        }

                                        session.commitAsync();
                                        resultSetFlowFiles.clear();
                                    }

                                    fragmentIndex++;
                                } catch (Exception e) {
                                    // Remove any result set flow file(s) and propagate the exception
                                    session.remove(resultSetFF);
                                    session.remove(resultSetFlowFiles);
                                    if (e instanceof ProcessException) {
                                        throw (ProcessException) e;
                                    } else {
                                        throw new ProcessException(e);
                                    }
                                }
                            } while (maxRowsPerFlowFile > 0 && nrOfRows.get() == maxRowsPerFlowFile);

                            // If we are splitting results but not outputting batches, set count on all FlowFiles
                            if (outputBatchSize == 0 && maxRowsPerFlowFile > 0) {
                                for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                                    resultSetFlowFiles.set(i,
                                            session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                                }
                            }
                        } catch (final SQLException e) {
                            throw new ProcessException(e);
                        }

                        resultCount++;
                    }

                    // are there anymore result sets?
                    try {
                        hasResults = st.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
                        hasUpdateCount = st.getUpdateCount() != -1;
                    } catch (SQLException ex) {
                        hasResults = false;
                        hasUpdateCount = false;
                    }
                }

                // Execute post-query, throw exception and cleanup Flow Files if fail
                failure = executeConfigStatements(con, postQueries);
                if (failure != null) {
                    selectQuery = failure.getLeft();
                    resultSetFlowFiles.forEach(session::remove);
                    throw failure.getRight();
                }

                // If the auto commit is set to false, commit() is called for consistency
                if (!con.getAutoCommit()) {
                    con.commit();
                }

                // Transfer any remaining files to SUCCESS
                session.transfer(resultSetFlowFiles, REL_SUCCESS);
                resultSetFlowFiles.clear();

                //If we had at least one result then it's OK to drop the original file, but if we had no results then
                //  pass the original flow file down the line to trigger downstream processors
                if (fileToProcess != null) {
                    if (resultCount > 0) {
                        session.remove(fileToProcess);
                    } else {
                        fileToProcess = session.write(fileToProcess, out -> sqlWriter.writeEmptyResultSet(out, getLogger()));
                        fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, "0");
                        fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), sqlWriter.getMimeType());
                        session.transfer(fileToProcess, REL_SUCCESS);
                    }
                } else if (resultCount == 0) {
                    //If we had no inbound FlowFile, no exceptions, and the SQL generated no result sets (Insert/Update/Delete statements only)
                    // Then generate an empty Output FlowFile
                    FlowFile resultSetFF = session.create();

                    resultSetFF = session.write(resultSetFF, out -> sqlWriter.writeEmptyResultSet(out, getLogger()));
                    resultSetFF = session.putAttribute(resultSetFF, RESULT_ROW_COUNT, "0");
                    resultSetFF = session.putAttribute(resultSetFF, CoreAttributes.MIME_TYPE.key(), sqlWriter.getMimeType());
                    session.transfer(resultSetFF, REL_SUCCESS);
                }
            }
        } catch (final ProcessException | SQLException e) {
            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute SQL select query [{}]. No FlowFile to route to failure", selectQuery, e);
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute SQL select query [{}] for {} routing to failure", selectQuery, fileToProcess, e);
                    fileToProcess = session.penalize(fileToProcess);
                } else {
                    logger.error("Unable to execute SQL select query [{}] routing to failure", selectQuery, e);
                    context.yield();
                }
                session.putAttribute(fileToProcess, RESULT_ERROR_MESSAGE, e.getMessage());
                session.transfer(fileToProcess, REL_FAILURE);
            }
        }
    }

    /*
     * Executes given queries using pre-defined connection.
     * Returns null on success, or a query string if failed.
     *
     * 주어진 연결(Connection)을 사용하여 설정용 쿼리 목록(Pre-Query 또는 Post-Query)을 순서대로 실행한다.
     * 모두 성공하면 null을 반환하고, 실행 도중 하나라도 실패하면 실패한 쿼리 문자열과 SQLException을
     * 쌍(Pair)으로 묶어 반환한다. 실패 즉시 나머지 쿼리는 실행하지 않고 중단한다.
     */
    protected Pair<String, SQLException> executeConfigStatements(final Connection con, final List<String> configQueries) {
        if (configQueries == null || configQueries.isEmpty()) {
            return null;
        }

        for (String confSQL : configQueries) {
            try (final Statement st = con.createStatement()) {
                st.execute(confSQL);
            } catch (SQLException e) {
                return Pair.of(confSQL, e);
            }
        }
        return null;
    }

    /*
     * Extract list of queries from config property
     *
     * SQL Pre-Query / Post-Query 프로퍼티 값(세미콜론으로 구분된 문자열)을 개별 쿼리 목록으로 분리한다.
     * 정규식 "(?<!\\);" 는 백슬래시로 이스케이프되지 않은 세미콜론만 구분자로 사용하도록 하여,
     * 쿼리문 자체에 세미콜론이 포함된 경우(예: "\\;") 이를 구분자로 오인하지 않게 한다.
     * 분리 후에는 이스케이프된 "\\;"를 실제 ";"로 되돌리고, 빈 쿼리는 목록에서 제외한다.
     */
    protected List<String> getQueries(final String value) {
        if (value == null || value.length() == 0 || value.trim().length() == 0) {
            return null;
        }
        final List<String> queries = new LinkedList<>();
        for (String query : value.split("(?<!\\\\);")) {
            query = query.replaceAll("\\\\;", ";");
            if (query.trim().length() > 0) {
                queries.add(query.trim());
            }
        }
        return queries;
    }

    // 결과 집합을 어떤 포맷(Avro, CSV 등)으로 직렬화할지는 하위 클래스(구체적인 프로세서 구현체)가 결정한다.
    // 예: ExecuteSQL은 Avro로, ExecuteSQLRecord는 지정된 Record Writer로 직렬화하는 SqlWriter를 반환할 수 있다.
    protected abstract SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess);
}