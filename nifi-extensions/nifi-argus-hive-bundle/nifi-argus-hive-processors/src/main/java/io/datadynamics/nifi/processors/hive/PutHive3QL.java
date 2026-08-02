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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/processors/hive/PutHive3QL.java
 */
package io.datadynamics.nifi.processors.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import io.datadynamics.nifi.services.api.hive.VendorHiveDBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.*;
import org.apache.nifi.processor.util.pattern.ExceptionHandler.OnError;
import org.apache.nifi.processor.util.pattern.PartialFunctions.FetchFlowFiles;
import org.apache.nifi.processor.util.pattern.PartialFunctions.InitConnection;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

// SelectHive3QL과 짝을 이루는 프로세서: HiveQL DML/DDL(UPDATE, INSERT 등)을 실행하여 데이터를 Hive에 적재(Put)한다.
@SeeAlso(SelectHive3QL.class)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"cloudera", "sql", "hive", "put", "database", "update", "insert"})
@CapabilityDescription("HiveQL DDL/DML 명령(UPDATE, INSERT 등)을 실행한다. 입력으로 들어온 FlowFile의 내용은 실행할 HiveQL 명령이어야 한다. "
        + "HiveQL 명령에서는 파라미터를 이스케이프하기 위해 ?를 사용할 수 있다. 이 경우 사용할 파라미터는 hiveql.args.N.type, hiveql.args.N.value "
        + "명명 규칙을 따르는 FlowFile 속성으로 존재해야 하며, 여기서 N은 양의 정수이다. hiveql.args.N.type은 JDBC Type을 나타내는 숫자여야 한다. "
        + "FlowFile의 내용은 UTF-8 형식이어야 한다.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "hiveql.args.N.type", description = "입력된 FlowFile은 파라미터화된 HiveQL 구문으로 간주된다. 각 파라미터의 타입은 "
                + "해당 파라미터의 JDBC Type을 나타내는 정수로 지정한다."),
        @ReadsAttribute(attribute = "hiveql.args.N.value", description = "입력된 FlowFile은 파라미터화된 HiveQL 구문으로 간주된다. 파라미터의 값은 "
                + "hiveql.args.1.value, hiveql.args.2.value, hiveql.args.3.value와 같은 형식으로 지정한다. hiveql.args.1.value 파라미터의 타입은 hiveql.args.1.type 속성으로 지정한다.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "query.input.tables", description = "'success' 관계로 라우팅되는 FlowFile에 기록되는 속성으로, "
                + "입력 테이블 이름(있는 경우)을 'databaseName.tableName' 형식으로 콤마로 구분하여 담는다."),
        @WritesAttribute(attribute = "query.output.tables", description = "'success' 관계로 라우팅되는 FlowFile에 기록되는 속성으로, "
                + "대상(출력) 테이블 이름을 'databaseName.tableName' 형식으로 담는다.")
})
public class PutHive3QL extends AbstractHive3QLProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("hive-batch-size")
            .displayName("배치 크기")
            .description("한 번의 트랜잭션으로 데이터베이스에 적재할 FlowFile의 개수(권장값)")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor STATEMENT_DELIMITER = new PropertyDescriptor.Builder()
            .name("statement-delimiter")
            .displayName("구문 구분자")
            .description("여러 개의 SQL 구문으로 이루어진 스크립트에서 각 구문을 구분하는 데 사용하는 구분자")
            .required(true)
            .defaultValue(";")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("데이터베이스가 성공적으로 갱신된 후 FlowFile이 라우팅되는 관계")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("데이터베이스를 갱신할 수 없었지만 다시 시도하면 성공할 가능성이 있는 경우 FlowFile이 라우팅되는 관계")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("데이터베이스를 갱신할 수 없고 재시도해도 실패할 것으로 판단되는 경우(예: 잘못된 쿼리나 무결성 제약 조건 위반) "
                    + "FlowFile이 라우팅되는 관계")
            .build();


    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * PropertyDescriptor 목록과 Relationship 집합을 클래스 로딩 시 한 번만 생성하여 불변(immutable) 컬렉션으로 보관한다.
     * 매 onTrigger 호출마다 새로 생성하지 않도록 하기 위함이다.
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HIVE_DBCP_SERVICE);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(QUERY_TIMEOUT);
        _propertyDescriptors.add(CHARSET);
        _propertyDescriptors.add(STATEMENT_DELIMITER);
        _propertyDescriptors.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    // Put 패턴(초기화 -> FlowFile 조회 -> 적재)을 조립하는 실행 객체. constructProcess()에서 각 단계를 조합해 생성한다.
    private Put<FunctionContext, Connection> process;
    // SQLException 등을 NiFi의 ErrorTypes로 매핑하여 성공/재시도/실패 라우팅을 결정하는 핸들러
    private ExceptionHandler<FunctionContext> exceptionHandler;
    // DBCP 컨트롤러 서비스로부터 커넥션을 얻고, 로깅/프로버넌스에 사용할 커넥션 URL을 FunctionContext에 저장한다.
    private final InitConnection<FunctionContext, Connection> initConnection = (context, session, fc, ffs) -> {
        final VendorHiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(VendorHiveDBCPService.class);
        final Connection connection = dbcpService.getConnection();
        fc.connectionUrl = dbcpService.getConnectionURL();
        return connection;
    };
    // Batch Size 프로퍼티에 설정된 개수만큼 세션에서 FlowFile을 가져온다.
    private final FetchFlowFiles<FunctionContext> fetchFlowFiles = (context, session, functionContext, result) -> {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        return session.get(batchSize);
    };
    // 실제로 각 FlowFile의 HiveQL을 실행하는 핵심 로직. FlowFile 내용을 Statement Delimiter 기준으로 여러 구문으로 분리한 뒤 순차 실행한다.
    private final Put.PutFlowFile<FunctionContext, Connection> putFlowFile = (context, session, fc, conn, flowFile, result) -> {
        final String script = getHiveQL(session, flowFile, fc.charset);
        // 이스케이프되지 않은(백슬래시가 앞에 없는) 구분자 기준으로만 분리하기 위한 부정 후방탐색(negative lookbehind) 정규식
        String regex = "(?<!\\\\)" + Pattern.quote(fc.statementDelimiter);

        String[] hiveQLs = script.split(regex);

        final Set<TableName> tableNames = new HashSet<>();
        exceptionHandler.execute(fc, flowFile, input -> {
            int loc = 1;
            for (String hiveQLStr : hiveQLs) {
                getLogger().debug("HiveQL: {}", hiveQLStr);

                final String hiveQL = hiveQLStr.trim();
                if (!StringUtils.isEmpty(hiveQL)) {
                    try (final PreparedStatement stmt = conn.prepareStatement(hiveQL)) {

                        // Get ParameterMetadata
                        // Hive JDBC Doesn't support this yet:
                        // ParameterMetaData pmd = stmt.getParameterMetaData();
                        // int paramCount = pmd.getParameterCount();
                        int paramCount = StringUtils.countMatches(hiveQL, "?");

                        if (paramCount > 0) {
                            loc = setParameters(loc, stmt, paramCount, flowFile.getAttributes());
                        }

                        // Parse hiveQL and extract input/output tables
                        try {
                            tableNames.addAll(findTableNames(hiveQL));
                        } catch (Exception e) {
                            // If failed to parse the query, just log a warning message, but continue.
                            getLogger().warn("Failed to parse hiveQL: {} due to {}", hiveQL, e, e);
                        }

                        stmt.setQueryTimeout(context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(flowFile).asInteger());

                        // Execute the statement
                        stmt.execute();
                        fc.proceed();
                    }
                }
            }

            // Emit a Provenance SEND event
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fc.startNanos);

            final FlowFile updatedFlowFile = session.putAllAttributes(flowFile, toQueryTableAttributes(tableNames));
            session.getProvenanceReporter().send(updatedFlowFile, fc.connectionUrl, transmissionMillis, true);
            result.routeTo(flowFile, REL_SUCCESS);

        }, onFlowFileError(context, session, result));

    };

    // 프로세서가 스케줄될 때(스레드가 트리거되기 전) 예외 처리 규칙과 Put 실행 파이프라인을 구성한다.
    @OnScheduled
    public void constructProcess() {
        exceptionHandler = new ExceptionHandler<>();
        // 발생한 예외를 Hive의 벤더 에러 코드 범위에 따라 NiFi의 ErrorTypes(InvalidInput/TemporalFailure 등)로 매핑한다.
        exceptionHandler.mapException(e -> {
            if (e instanceof SQLNonTransientException) {
                return ErrorTypes.InvalidInput;
            } else if (e instanceof SQLException) {
                // SQLException의 벤더 코드를 참고하여 판단한다 -- 에러 코드별 상세 내용은 Hive의 ErrorMsg 클래스를 참조
                int errorCode = ((SQLException) e).getErrorCode();
                getLogger().debug("Error occurred during Hive operation, Hive returned error code {}", errorCode);
                if (errorCode >= 10000 && errorCode < 20000) {
                    return ErrorTypes.InvalidInput;
                } else if (errorCode >= 20000 && errorCode < 30000) {
                    return ErrorTypes.InvalidInput;
                } else if (errorCode >= 30000 && errorCode < 40000) {
                    return ErrorTypes.TemporalInputFailure;
                } else if (errorCode >= 40000 && errorCode < 50000) {
                    // 일부 파싱 에러를 포함한 알 수 없는 에러이지만, ProcessException을 유발하는 UnknownFailure 대신
                    // InvalidInput 에러 타입으로 처리하여 failure로 라우팅한다.
                    return ErrorTypes.InvalidInput;
                } else {
                    // 알 수 없는 에러는 기본적으로 TemporalFailure로 처리한다(원래 구현 방식 유지).
                    // 이렇게 하면 사용자의 Rollback On Failure 설정에 따라 failure로 라우팅되거나 롤백될 수 있다.
                    return ErrorTypes.TemporalFailure;
                }
            } else {
                return ErrorTypes.UnknownFailure;
            }
        });
        exceptionHandler.adjustError(RollbackOnFailure.createAdjustError(getLogger()));

        process = new Put<>();
        process.setLogger(getLogger());
        process.initConnection(initConnection);
        process.fetchFlowFiles(fetchFlowFiles);
        process.putFlowFile(putFlowFile);
        process.adjustRoute(RollbackOnFailure.createAdjustRoute(REL_FAILURE, REL_RETRY));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    // 개별 FlowFile 처리 중 발생한 오류를 로깅하고, RollbackOnFailure 설정에 따라 최종 라우팅 대상을 결정한다.
    private OnError<FunctionContext, FlowFile> onFlowFileError(final ProcessContext context, final ProcessSession session, final RoutingResult result) {
        OnError<FunctionContext, FlowFile> onFlowFileError = ExceptionHandler.createOnError(context, session, result, REL_FAILURE, REL_RETRY);
        onFlowFileError = onFlowFileError.andThen((c, i, r, e) -> {
            switch (r.destination()) {
                case Failure:
                    getLogger().error("Failed to update Hive for {} due to {}; routing to failure", i, e, e);
                    break;
                case Retry:
                    getLogger().error("Failed to update Hive for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry", i, e, e);
                    break;
                case Self:
                    getLogger().error("Failed to update Hive for {} due to {};", i, e, e);
                    break;
            }
        });
        return RollbackOnFailure.createOnError(onFlowFileError);
    }

    // 프로세서 실행 진입점. FunctionContext를 구성한 뒤 RollbackOnFailure 유틸리티를 통해 Put 파이프라인을 실행한다.
    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final Boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        final String statementDelimiter = context.getProperty(STATEMENT_DELIMITER).getValue();
        final FunctionContext functionContext = new FunctionContext(rollbackOnFailure, charset, statementDelimiter);
        RollbackOnFailure.onTrigger(context, sessionFactory, functionContext, getLogger(), session -> process.onTrigger(context, session, functionContext));
    }

    // 한 번의 onTrigger 호출 동안 공유되는 실행 컨텍스트. 문자셋, 구문 구분자, 처리 시작 시각(프로버넌스용), 커넥션 URL을 보관한다.
    private class FunctionContext extends RollbackOnFailure {
        final Charset charset;
        final String statementDelimiter;
        final long startNanos = System.nanoTime();

        String connectionUrl;


        private FunctionContext(boolean rollbackOnFailure, Charset charset, String statementDelimiter) {
            super(rollbackOnFailure, false);
            this.charset = charset;
            this.statementDelimiter = statementDelimiter;
        }
    }
}