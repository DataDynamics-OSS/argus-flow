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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/processors/hive/SelectHive3QL.java
 */
package io.datadynamics.nifi.processors.hive;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import io.datadynamics.nifi.services.api.hive.VendorHiveDBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.PartialFunctions;
import org.apache.nifi.util.StopWatch;
import io.datadynamics.nifi.processors.hive.util.CsvOutputOptions;
import io.datadynamics.nifi.processors.hive.util.HiveJdbcCommon;

import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.datadynamics.nifi.processors.hive.util.HiveJdbcCommon.*;

/**
 * HiveQL SELECT 쿼리를 실행하여 결과 집합을 Avro 또는 CSV FlowFile로 변환하는 프로세서.
 * 유입 FlowFile이 있으면 그 내용(또는 속성)을 쿼리 소스로 사용할 수 있고, 없으면
 * "HiveQL Select Query" 프로퍼티에 지정된 정적 쿼리를 사용한다. 결과가 매우 큰 경우
 * "Max Rows Per Flow File"로 여러 FlowFile로 분할하여 OOM을 방지할 수 있다.
 */
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"cloudera", "hive", "sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("제공된 HiveQL SELECT 쿼리를 Hive 데이터베이스 연결에 대해 실행한다. 쿼리 결과는 Avro 또는 CSV 형식으로 변환된다."
        + " 스트리밍 방식을 사용하므로 임의로 큰 결과 집합도 지원한다. 이 프로세서는 표준 스케줄링 방식을 이용해 "
        + "타이머 또는 cron 표현식으로 실행되도록 스케줄링할 수 있으며, 유입되는 FlowFile에 의해 트리거될 수도 있다. "
        + "유입 FlowFile에 의해 트리거되는 경우 SELECT 쿼리를 평가할 때 해당 FlowFile의 속성을 사용할 수 있다. "
        + "FlowFile 속성 'selecthiveql.row.count'는 선택된 행(row)의 개수를 나타낸다.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "출력 FlowFile의 MIME 타입을 설정한다. Avro인 경우 application/avro-binary, CSV인 경우 text/csv로 설정된다."),
        @WritesAttribute(attribute = "filename", description = "선택된 출력 형식에 따라 filename 속성에 .avro 또는 .csv 확장자를 추가한다."),
        @WritesAttribute(attribute = "selecthiveql.row.count", description = "쿼리에 의해 선택/반환된 행(row)의 개수를 나타낸다."),
        @WritesAttribute(attribute = "selecthiveql.query.duration", description = "쿼리 실행 시간과 결과 fetch 시간을 합산한 값(밀리초). "
                + "'Max Rows Per Flow File'이 설정된 경우, 이 값은 전체 결과 집합이 아니라 해당 FlowFile에 포함된 행에 대한 fetch 시간만을 반영한다."),
        @WritesAttribute(attribute = "selecthiveql.query.executiontime", description = "쿼리 실행 시간(밀리초). "
                + "이 값은 'Max Rows Per Flow File' 설정과 무관하게 쿼리 실행 시간 자체를 반영한다."),
        @WritesAttribute(attribute = "selecthiveql.query.fetchtime", description = "결과 집합 fetch 시간(밀리초). "
                + "'Max Rows Per Flow File'이 설정된 경우, 이 값은 전체 결과 집합이 아니라 해당 FlowFile에 포함된 행에 대한 fetch 시간만을 반영한다."),
        @WritesAttribute(attribute = "fragment.identifier", description = "'Max Rows Per Flow File'이 설정된 경우 동일한 쿼리 결과 집합에서 나온 모든 FlowFile은 "
                + "동일한 fragment.identifier 속성 값을 가진다. 이를 이용해 결과들을 서로 연관지을 수 있다."),
        @WritesAttribute(attribute = "fragment.count", description = "'Max Rows Per Flow File'이 설정된 경우 이는 하나의 ResultSet으로부터 "
                + "생성된 FlowFile의 총 개수이다. fragment.identifier 속성과 함께 사용하여 "
                + "동일한 유입 ResultSet에 속한 FlowFile이 몇 개였는지 파악할 수 있다."),
        @WritesAttribute(attribute = "fragment.index", description = "'Max Rows Per Flow File'이 설정된 경우 동일한 결과 집합 FlowFile로부터 파생된 "
                + "출력 FlowFile 목록 내에서 이 FlowFile의 위치를 나타낸다. fragment.identifier 속성과 함께 사용하여 "
                + "어떤 FlowFile들이 동일한 쿼리 결과 집합에서 유래했는지, 그리고 어떤 순서로 "
                + "FlowFile이 생성되었는지 파악할 수 있다."),
        @WritesAttribute(attribute = "query.input.tables", description = "쉼표로 구분된 'databaseName.tableName' 형식의 입력 테이블명을 포함한다.")
})
public class SelectHive3QL extends AbstractHive3QLProcessor {

    public static final String RESULT_QUERY_DURATION = "selecthiveql.query.duration";
    public static final String RESULT_QUERY_EXECUTION_TIME = "selecthiveql.query.executiontime";
    public static final String RESULT_QUERY_FETCH_TIME = "selecthiveql.query.fetchtime";
    public static final PropertyDescriptor HIVEQL_PRE_QUERY = new PropertyDescriptor.Builder()
            .name("hive-pre-query")
            .displayName("HiveQL 사전 쿼리")
            .description("실행할 HiveQL 사전 쿼리(pre-query). 세미콜론으로 구분된 쿼리 목록. "
                    + "예: 'set tez.queue.name=queue1; set hive.exec.orc.split.strategy=ETL; set hive.exec.reducers.bytes.per.reducer=1073741824'. "
                    + "이 쿼리들이 성공적으로 실행되면 결과/출력은 표시되지 않는다는 점에 유의한다.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor HIVEQL_POST_QUERY = new PropertyDescriptor.Builder()
            .name("hive-post-query")
            .displayName("HiveQL 사후 쿼리")
            .description("실행할 HiveQL 사후 쿼리(post-query). 세미콜론으로 구분된 쿼리 목록. "
                    + "이 쿼리들이 성공적으로 실행되면 결과/출력은 표시되지 않는다는 점에 유의한다.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor USE_AVRO_LOGICAL_TYPES = new PropertyDescriptor.Builder()
            .name("use-logical-types")
            .displayName("Avro Logical Type 사용")
            .description("DECIMAL, DATE, TIMESTAMP 컬럼에 대해 Avro Logical Type을 사용할지 여부. "
                    + "비활성화하면 문자열로 기록된다. "
                    + "활성화하면 Logical Type이 사용되어 각 논리 타입의 하위(underlying) 타입으로 기록된다. 구체적으로, "
                    + "DECIMAL은 logical 'decimal'로서 정밀도(precision)와 스케일(scale) 메타데이터가 포함된 bytes로 기록되고, "
                    + "DATE는 logical 'date'로서 유닉스 에포크(1970-01-01) 이후 경과 일수를 나타내는 int로 기록되며, "
                    + "TIMESTAMP는 logical 'timestamp-millis'로서 유닉스 에포크 이후 경과 밀리초를 나타내는 long으로 기록된다. "
                    + "기록된 Avro 레코드를 읽는 리더가 이러한 Logical Type을 인식하는 경우, 리더 구현에 따라 더 많은 컨텍스트와 함께 이 값들을 역직렬화할 수 있다.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    static final String RESULT_ROW_COUNT = "selecthiveql.row.count";
    // 관계(Relationship) 정의
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("HiveQL 쿼리 결과 집합으로부터 FlowFile을 성공적으로 생성함.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("HiveQL 쿼리 실행에 실패함. 유입 FlowFile은 페널티가 부여된 후 이 관계로 라우팅된다.")
            .build();
    static final PropertyDescriptor HIVEQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("hive-query")
            .displayName("HiveQL SELECT 쿼리")
            .description("실행할 HiveQL SELECT 쿼리. 설정되지 않은 경우 쿼리는 유입 FlowFile의 콘텐츠에 포함되어 있는 것으로 간주한다.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("hive-fetch-size")
            .displayName("Fetch 크기")
            .description("결과 집합으로부터 한 번에 fetch할 결과 행(row)의 개수. 이는 드라이버에 대한 힌트이며 "
                    + "정확히 지켜지지 않을 수 있다. 값이 0으로 지정되면 이 힌트는 무시된다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("hive-max-rows")
            .displayName("FlowFile당 최대 행 수")
            .description("단일 FlowFile에 포함될 결과 행(row)의 최대 개수. " +
                    "이를 통해 매우 큰 결과 집합을 여러 FlowFile로 나눌 수 있다. 값이 0으로 지정되면 모든 행이 단일 FlowFile로 반환된다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor MAX_FRAGMENTS = new PropertyDescriptor.Builder()
            .name("hive-max-frags")
            .displayName("최대 조각(Fragment) 수")
            .description("생성할 조각(fragment)의 최대 개수. 값이 0으로 지정되면 모든 조각이 반환된다. " +
                    "이는 이 프로세서가 거대한 테이블을 수집할 때 OutOfMemoryError가 발생하는 것을 방지한다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor HIVEQL_CSV_HEADER = new PropertyDescriptor.Builder()
            .name("csv-header")
            .displayName("CSV 헤더")
            .description("출력에 헤더를 포함할지 여부")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor HIVEQL_CSV_ALT_HEADER = new PropertyDescriptor.Builder()
            .name("csv-alt-header")
            .displayName("대체 CSV 헤더")
            .description("쉼표로 구분된 헤더 필드 목록")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor HIVEQL_CSV_DELIMITER = new PropertyDescriptor.Builder()
            .name("csv-delimiter")
            .displayName("CSV 구분자")
            .description("필드를 구분하는 데 사용되는 CSV 구분자")
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor HIVEQL_CSV_QUOTE = new PropertyDescriptor.Builder()
            .name("csv-quote")
            .displayName("CSV 인용부호")
            .description("CSV 필드에 강제로 따옴표(quote)를 적용할지 여부. 이 설정은 CSV Escape 설정과 충돌할 수 있음에 유의한다.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor HIVEQL_CSV_ESCAPE = new PropertyDescriptor.Builder()
            .name("csv-escape")
            .displayName("CSV 이스케이프")
            .description("출력 시 CSV 문자열을 이스케이프할지 여부. 이 설정은 CSV Quote 설정과 충돌할 수 있음에 유의한다.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor HIVEQL_OUTPUT_FORMAT = new PropertyDescriptor.Builder()
            .name("hive-output-format")
            .displayName("출력 형식")
            .description("Hive로부터 가져온 레코드를 어떻게 표현할지 지정한다 (예: Avro, CSV)")
            .required(true)
            .allowableValues(AVRO, CSV)
            .defaultValue(AVRO)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * 프로퍼티 디스크립터 목록이 단 한 번만 생성되도록 보장한다.
     * 관계(Relationship) Set도 함께 생성한다.
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HIVE_DBCP_SERVICE);
        _propertyDescriptors.add(HIVEQL_PRE_QUERY);
        _propertyDescriptors.add(HIVEQL_SELECT_QUERY);
        _propertyDescriptors.add(HIVEQL_POST_QUERY);
        _propertyDescriptors.add(FETCH_SIZE);
        _propertyDescriptors.add(QUERY_TIMEOUT);
        _propertyDescriptors.add(MAX_ROWS_PER_FLOW_FILE);
        _propertyDescriptors.add(MAX_FRAGMENTS);
        _propertyDescriptors.add(HIVEQL_OUTPUT_FORMAT);
        _propertyDescriptors.add(NORMALIZE_NAMES_FOR_AVRO);
        _propertyDescriptors.add(USE_AVRO_LOGICAL_TYPES);
        _propertyDescriptors.add(HIVEQL_CSV_HEADER);
        _propertyDescriptors.add(HIVEQL_CSV_ALT_HEADER);
        _propertyDescriptors.add(HIVEQL_CSV_DELIMITER);
        _propertyDescriptors.add(HIVEQL_CSV_QUOTE);
        _propertyDescriptors.add(HIVEQL_CSV_ESCAPE);
        _propertyDescriptors.add(CHARSET);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * 프로세서 스케줄링 시점에 설정을 검증한다.
     * SELECT 쿼리가 프로퍼티로 지정되지 않았다면 반드시 유입 커넥션이 있어야 한다(쿼리를 FlowFile 콘텐츠에서 읽어와야 하므로).
     * 둘 다 없으면 초기화를 실패시킨다.
     */
    @OnScheduled
    public void setup(ProcessContext context) {
        // 쿼리가 설정되지 않았다면 유입 FlowFile이 반드시 필요하다. 그렇지 않으면 초기화를 실패시킨다.
        if (!context.getProperty(HIVEQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        // PartialFunctions.onTrigger는 세션 롤백/커밋을 감싸는 표준 래퍼로,
        // 예외 발생 시 세션이 안전하게 롤백되도록 보장한다.
        PartialFunctions.onTrigger(context, sessionFactory, getLogger(), session -> onTrigger(context, session));
    }

    private void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = (context.hasIncomingConnection() ? session.get() : null);
        FlowFile flowfile = null;

        // 유입 FlowFile이 없고 모든 유입 커넥션이 자기 자신으로의 셀프 루프(self-loop)뿐이라면 계속 진행해도 된다.
        // 그러나 유입 FlowFile이 없는 상태에서 다른 프로세서로부터의 커넥션이 존재한다면,
        // FlowFile이 있을 때만 실행해야 한다는 것을 의미하므로 즉시 종료한다.
        if (context.hasIncomingConnection()) {
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ComponentLog logger = getLogger();
        final VendorHiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(VendorHiveDBCPService.class);
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());

        List<String> preQueries = getQueries(context.getProperty(HIVEQL_PRE_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
        List<String> postQueries = getQueries(context.getProperty(HIVEQL_POST_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());

        // flowbased: 쿼리를 FlowFile에서 읽어와야 하는지 여부. true이면 PreparedStatement를 사용해
        // FlowFile 콘텐츠의 쿼리를 실행하고, false이면 정적으로 설정된 SELECT 쿼리를 사용한다.
        final boolean flowbased = !(context.getProperty(HIVEQL_SELECT_QUERY).isSet());

        // 실행할 SQL 쿼리 문자열을 확보한다.
        String hqlStatement;

        if (context.getProperty(HIVEQL_SELECT_QUERY).isSet()) {
            hqlStatement = context.getProperty(HIVEQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // 쿼리가 설정되지 않은 경우 유입 FlowFile이 반드시 있어야 하며, 그 콘텐츠는 유효한 SQL SELECT 쿼리여야 한다.
            // 유입 커넥션이 없다면 스케줄링 시점에 프로세서가 실패하므로 onTrigger가 호출되지 않는다.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, charset)));
            hqlStatement = queryContents.toString();
        }


        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer maxFragments = context.getProperty(MAX_FRAGMENTS).isSet()
                ? context.getProperty(MAX_FRAGMENTS).evaluateAttributeExpressions(fileToProcess).asInteger()
                : 0;
        final String outputFormat = context.getProperty(HIVEQL_OUTPUT_FORMAT).getValue();
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final StopWatch stopWatch = new StopWatch(true);
        final boolean header = context.getProperty(HIVEQL_CSV_HEADER).asBoolean();
        final String altHeader = context.getProperty(HIVEQL_CSV_ALT_HEADER).evaluateAttributeExpressions(fileToProcess).getValue();
        final String delimiter = context.getProperty(HIVEQL_CSV_DELIMITER).evaluateAttributeExpressions(fileToProcess).getValue();
        final boolean quote = context.getProperty(HIVEQL_CSV_QUOTE).asBoolean();
        final boolean escape = context.getProperty(HIVEQL_CSV_ESCAPE).asBoolean();
        final boolean useLogicalTypes = context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean();
        final String fragmentIdentifier = UUID.randomUUID().toString();

        try (final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
             final Statement st = (flowbased ? con.prepareStatement(hqlStatement) : con.createStatement())
        ) {
            Pair<String, SQLException> failure = executeConfigStatements(con, preQueries);
            if (failure != null) {
                // 실패 시 기존 에러 처리 흐름을 그대로 따르도록 실패한 설정 쿼리를 "hqlStatement"에 대입한다
                hqlStatement = failure.getLeft();
                flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                fileToProcess = null;
                throw failure.getRight();
            }
            st.setQueryTimeout(context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asInteger());

            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // 모든 드라이버가 이 기능을 지원하지는 않으므로, 에러를 debug 레벨로만 기록하고 계속 진행한다
                    logger.debug("Cannot set fetch size to {} due to {}", fetchSize, se.getLocalizedMessage(), se);
                }
            }

            final List<FlowFile> resultSetFlowFiles = new ArrayList<>();
            try {
                logger.debug("Executing query {}", hqlStatement);
                if (flowbased) {
                    // Hive JDBC는 아직 이 방식을 지원하지 않는다:
                    // ParameterMetaData pmd = ((PreparedStatement)st).getParameterMetaData();
                    // int paramCount = pmd.getParameterCount();

                    // SQL 내 파라미터 개수를 확인하는 대체 방법.
                    int paramCount = StringUtils.countMatches(hqlStatement, "?");

                    if (paramCount > 0) {
                        setParameters(1, (PreparedStatement) st, paramCount, fileToProcess.getAttributes());
                    }
                }

                final StopWatch executionTime = new StopWatch(true);
                final ResultSet resultSet;

                try {
                    resultSet = (flowbased ? ((PreparedStatement) st).executeQuery() : st.executeQuery(hqlStatement));
                } catch (SQLException se) {
                    // 쿼리 실행 중 오류가 발생하면 FlowFile을 failure로 라우팅해야 하므로 여기서 확실히 생성해 둔다
                    flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                    fileToProcess = null;
                    throw se;
                }
                long executionTimeElapsed = executionTime.getElapsed(TimeUnit.MILLISECONDS);

                int fragmentIndex = 0;
                String baseFilename = (fileToProcess != null) ? fileToProcess.getAttribute(CoreAttributes.FILENAME.key()) : null;
                while (true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);
                    final StopWatch fetchTime = new StopWatch(true);

                    flowfile = (fileToProcess == null) ? session.create() : session.create(fileToProcess);
                    if (baseFilename == null) {
                        baseFilename = flowfile.getAttribute(CoreAttributes.FILENAME.key());
                    }
                    try {
                        flowfile = session.write(flowfile, out -> {
                            try {
                                if (AVRO.equals(outputFormat)) {
                                    nrOfRows.set(HiveJdbcCommon.convertToAvroStream(resultSet, out, maxRowsPerFlowFile, convertNamesForAvro, useLogicalTypes));
                                } else if (CSV.equals(outputFormat)) {
                                    CsvOutputOptions options = new CsvOutputOptions(header, altHeader, delimiter, quote, escape, maxRowsPerFlowFile);
                                    nrOfRows.set(HiveJdbcCommon.convertToCsvStream(resultSet, out, options));
                                } else {
                                    nrOfRows.set(0L);
                                    throw new ProcessException("Unsupported output format: " + outputFormat);
                                }
                            } catch (final SQLException | RuntimeException e) {
                                throw new ProcessException("Error during database query or conversion of records.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // 재발생(rethrow)시키기 전에 결과 목록에 flowfile을 추가해 두어야 바깥 catch에서 세션으로부터 제거될 수 있다
                        resultSetFlowFiles.add(flowfile);
                        throw e;
                    }
                    long fetchTimeElapsed = fetchTime.getElapsed(TimeUnit.MILLISECONDS);

                    if (nrOfRows.get() > 0 || resultSetFlowFiles.isEmpty()) {
                        final Map<String, String> attributes = new HashMap<>();
                        // 선택된 행 개수를 속성으로 설정
                        attributes.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));

                        try {
                            // 쿼리를 파싱하여 입력/출력 테이블명을 속성으로 설정
                            attributes.putAll(toQueryTableAttributes(findTableNames(hqlStatement)));
                        } catch (Exception e) {
                            // 쿼리 파싱에 실패하면 경고 로그만 남기고 계속 진행한다.
                            getLogger().warn("Failed to parse query: {} due to {}", hqlStatement, e, e);
                        }

                        // 출력 문서의 MIME 타입을 설정하고 파일명에 확장자를 추가
                        if (AVRO.equals(outputFormat)) {
                            attributes.put(CoreAttributes.MIME_TYPE.key(), MIME_TYPE_AVRO_BINARY);
                            attributes.put(CoreAttributes.FILENAME.key(), baseFilename + "." + fragmentIndex + ".avro");
                        } else if (CSV.equals(outputFormat)) {
                            attributes.put(CoreAttributes.MIME_TYPE.key(), CSV_MIME_TYPE);
                            attributes.put(CoreAttributes.FILENAME.key(), baseFilename + "." + fragmentIndex + ".csv");
                        }

                        if (maxRowsPerFlowFile > 0) {
                            attributes.put("fragment.identifier", fragmentIdentifier);
                            attributes.put("fragment.index", String.valueOf(fragmentIndex));
                        }

                        attributes.put(RESULT_QUERY_DURATION, String.valueOf(executionTimeElapsed + fetchTimeElapsed));
                        attributes.put(RESULT_QUERY_EXECUTION_TIME, String.valueOf(executionTimeElapsed));
                        attributes.put(RESULT_QUERY_FETCH_TIME, String.valueOf(fetchTimeElapsed));

                        flowfile = session.putAllAttributes(flowfile, attributes);

                        logger.info("{} contains {} " + outputFormat + " records; transferring to 'success'",
                                flowfile, nrOfRows.get());

                        if (context.hasIncomingConnection()) {
                            // FlowFile이 유입 커넥션으로부터 온 경우, Fetch provenance 이벤트를 발생시킨다
                            session.getProvenanceReporter().fetch(flowfile, dbcpService.getConnectionURL(),
                                    "Retrieved " + nrOfRows.get() + " rows", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        } else {
                            // Hive로부터 받은 행들로 FlowFile을 새로 생성한 경우, Receive provenance 이벤트를 발생시킨다
                            session.getProvenanceReporter().receive(flowfile, dbcpService.getConnectionURL(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        }
                        resultSetFlowFiles.add(flowfile);
                    } else {
                        // 반환된 행이 없는 경우(이미 첫 FlowFile이 전송되었다면) 처리가 끝난 것이므로 이 flowfile은 제거하고 계속 진행한다
                        session.remove(flowfile);
                        if (resultSetFlowFiles != null && resultSetFlowFiles.size() > 0) {
                            flowfile = resultSetFlowFiles.get(resultSetFlowFiles.size() - 1);
                        }
                        break;
                    }

                    fragmentIndex++;
                    if (maxFragments > 0 && fragmentIndex >= maxFragments) {
                        break;
                    }
                }

                for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                    // 모든 FlowFile에 fragment.count 속성을 설정
                    if (maxRowsPerFlowFile > 0) {
                        resultSetFlowFiles.set(i,
                                session.putAttribute(resultSetFlowFiles.get(i), "fragment.count", Integer.toString(fragmentIndex)));
                    }
                }

            } catch (final SQLException e) {
                throw e;
            }

            failure = executeConfigStatements(con, postQueries);
            if (failure != null) {
                hqlStatement = failure.getLeft();
                if (resultSetFlowFiles != null) {
                    resultSetFlowFiles.forEach(ff -> session.remove(ff));
                }
                flowfile = (fileToProcess == null) ? session.create() : fileToProcess;
                fileToProcess = null;
                throw failure.getRight();
            }

            session.transfer(resultSetFlowFiles, REL_SUCCESS);
            if (fileToProcess != null) {
                session.remove(fileToProcess);
            }

        } catch (final ProcessException | SQLException e) {
            logger.error("Issue processing SQL {} due to {}.", hqlStatement, e);
            if (flowfile == null) {
                // 커넥션, Statement 등을 설정하는 도중에 예외가 발생한 경우 이런 상황이 생길 수 있다.
                logger.error("Unable to execute HiveQL select query {} due to {}. No FlowFile to route to failure",
                        hqlStatement, e);
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute HiveQL select query {} for {} due to {}; routing to failure",
                            hqlStatement, flowfile, e);
                    flowfile = session.penalize(flowfile);
                } else {
                    logger.error("Unable to execute HiveQL select query {} due to {}; routing to failure",
                            hqlStatement, e);
                    context.yield();
                }
                session.transfer(flowfile, REL_FAILURE);
            }
        }
    }

    /*
     * 주어진 커넥션을 사용하여 지정된 쿼리들(사전/사후 쿼리)을 실행한다.
     * 모두 성공하면 null을 반환하고, 실패하면 실패한 쿼리 문자열과 예외를 담은 Pair를 반환한다.
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

    /**
     * 세미콜론으로 구분된 쿼리 문자열을 개별 쿼리 목록으로 분리한다. 각 쿼리의 앞뒤 공백은 제거되며
     * 빈 문자열인 쿼리는 목록에서 제외된다.
     */
    protected List<String> getQueries(final String value) {
        if (value == null || value.length() == 0 || value.trim().length() == 0) {
            return null;
        }
        final List<String> queries = new LinkedList<>();
        for (String query : value.split(";")) {
            if (query.trim().length() > 0) {
                queries.add(query.trim());
            }
        }
        return queries;
    }
}
