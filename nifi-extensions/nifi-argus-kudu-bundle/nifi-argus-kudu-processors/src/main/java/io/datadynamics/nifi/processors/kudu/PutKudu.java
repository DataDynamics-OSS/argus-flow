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
 *   nifi-nar-bundles/nifi-kudu-bundle/nifi-kudu-processors/src/main/java/org/apache/nifi/processors/kudu/PutKudu.java
 */
package io.datadynamics.nifi.processors.kudu;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datadynamics.nifi.processors.kudu.json.TimestampFormatHolder;
import io.datadynamics.nifi.processors.kudu.json.TimestampFormats;
import io.datadynamics.nifi.processors.kudu.json.TimestampType;
import io.datadynamics.nifi.processors.kudu.validator.JsonValidator;
import io.datadynamics.nifi.processors.kudu.validator.TimestampValidator;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StringUtils;

import javax.security.auth.login.LoginException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

/**
 * FlowFile로 유입된 레코드를 Kudu 테이블에 저장(Insert/Upsert/Update/Delete 등)하는 프로세서입니다.
 * RecordReader로 레코드를 읽어 Kudu의 Schema에 맞게 PartialRow를 구성한 뒤 KuduSession을 통해 배치로 반영하며,
 * Schema Drift 처리, Timestamp 컬럼 포맷 변환, Kerberos 인증, 실패 처리 전략(라우팅/롤백) 등을 지원합니다.
 */
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@SupportsBatching
@RequiresInstanceClassLoading // Because of calls to UserGroupInformation.setConfiguration
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"timestamp", "custom", "put", "database", "kudu", "record", "gmt"})
@CapabilityDescription("지정한 Record Reader를 사용하여 Incoming FlowFile에서 레코드를 읽고 해당 레코드를 Kudu의 테이블에 기록합니다. " +
        "Kudu 테이블의 스키마는 Record Reader의 스키마를 활용합니다. " +
        "입력 FlowFile에서 레코드를 읽거나 Kudu에 레코드를 쓰는 동안 오류가 발생하면 FlowFile이 failure로 라우팅됩니다.")
@WritesAttributes({
        @WritesAttribute(attribute = "PutKudu Total Rows", description = "Kudu에 기록한 총 레코드 수"),
        @WritesAttribute(attribute = "PutKudu Error Rows", description = "Kudu에 기록시 에러가 발생한 레코드 수")
})
public class PutKudu extends AbstractKuduProcessor {

    ///////////////////////////////////////////////
    // Utility
    ///////////////////////////////////////////////

    /**
     * JSON을 파싱하기 위한 Jackson Object Mapper
     */
    protected static final ObjectMapper mapper = new ObjectMapper();

    ///////////////////////////////////////////////
    // Counter
    ///////////////////////////////////////////////

    /**
     * NiFi Web UI Summary에 표시하기 위한 키값
     */
    public static final String RECORD_COUNT_ATTR = "record.count";

    ///////////////////////////////////////////////
    // Relationship
    ///////////////////////////////////////////////

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile은 Kudu에 데이터가 성공적으로 저장한 후 이 관계로 라우팅됩니다.")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile을 Kudu에 저장할 수 없는 경우 이 관계로 라우팅됩니다.")
            .build();

    ///////////////////////////////////////////////
    // Validator
    ///////////////////////////////////////////////

    /**
     * JSON 형식이 유효한지 검사하는 Validator
     */
    protected static final Validator JsonValidator = new JsonValidator();

    /**
     * Timestamp Pattern이 유효한지 검사하는 Validator
     */
    protected static final Validator timestampValidator = new TimestampValidator();

    /**
     * Kudu Operation Type이 유효한지 확인하는 Validator
     */
    protected static final Validator OperationTypeValidator = new Validator() {
        @Override
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language 필요").valid(true).build();
            }

            boolean valid;
            try {
                OperationType.valueOf(value.toUpperCase());
                valid = true;
            } catch (IllegalArgumentException ex) {
                valid = false;
            }

            final String explanation = valid ? null : "다음의 값중 하나가 되어야 합니다: " + Arrays.stream(OperationType.values()).map(Enum::toString).collect(Collectors.joining(", "));
            return new ValidationResult.Builder().subject(subject).input(value).valid(valid).explanation(explanation).build();
        }
    };

    ///////////////////////////////////////////////
    // Allowable Value
    ///////////////////////////////////////////////

    static final AllowableValue FAILURE_STRATEGY_ROUTE = new AllowableValue("route-to-failure",
            "실패로 처리",
            "Kudu Insert Operation을 실패한 레코드를 포함하는 FlowFile은 'failure'로 라우팅합니다.");
    static final AllowableValue FAILURE_STRATEGY_ROLLBACK = new AllowableValue("rollback",
            "세션 롤백",
            "레코드를 Insert할 수 없는 경우 NiFi Session의 모든 FlowFile을 Input Queue로 롤백 처리합니다.\n" +
                    "Kudu로 Insert할 수 없는 경우 문제가 해결될떄 까지 후속 데이터도 Kudu로 Insert하지 않습니다.\n" +
                    "FlowFile의 처리 순서가 매우 중요한 경우 이것을 사용할 수 있습니다.");

    ///////////////////////////////////////////////
    // Property
    ///////////////////////////////////////////////

    public static final PropertyDescriptor RECORD_READER = new Builder()
            .name("record-reader")
            .displayName("레코드 리더")
            .description("FlowFile의 레코드를 읽기 위한 Record Reader 서비스를 지정하십시오.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor TABLE_NAME = new Builder()
            .name("kudu-table-name")
            .displayName("데이터를 저장할 Kudu 테이블명")
            .description("Kudu 테이블에 데이터를 저장하기 위해서 테이블명을 지정하십시오.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    protected static final PropertyDescriptor ADD_HOUR = new Builder()
            .name("add-hour-to-timestamp-column")
            .displayName("Timestamp 컬럼에 시간 추가")
            .description("Timestamp 컬럼에서 GMT, KST 등의 시간 조정을 위해서 추가할 시간을 입력합니다.\n" +
                    "Kudu는 GMT를 기본 시간으로 사용하므로 GMT로 데이터를 사용할 수 없을때 추가할 시간을 입력합니다.\n" +
                    "그러면 Timestamp 컬럼에 대해서 지정한 시간만큼 +하여 저장합니다.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    protected static final PropertyDescriptor LOWERCASE_FIELD_NAMES = new Builder()
            .name("use-lower-case")
            .displayName("소문자 필드명 사용")
            .description("Kudu 테이블 컬럼의 인덱스를 찾을 때 컬럼명을 소문자로 변환하여 처리합니다.")
            .defaultValue("false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    protected static final PropertyDescriptor HANDLE_SCHEMA_DRIFT = new Builder()
            .name("handle-schema-drift")
            .displayName("컬럼 누락시 컬럼 추가 처리")
            .description("true로 설정하면 대상 Kudu 테이블에 없는 이름을 가진 필드(스키마와 테이블의 데이터의 컬럼이 다른 경우)가 발견되면\n" +
                    "Kudu 테이블에 새로운 컬럼을 추가하도록 변경합니다.")
            .defaultValue("false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    protected static final PropertyDescriptor INSERT_OPERATION = new Builder()
            .name("insert-operation")
            .displayName("Kudu Operation 유형")
            .description("Kudu Operation의 유형을 지정하십시오.\n" +
                    "사용 가능한 값: " +
                    Arrays.stream(OperationType.values()).map(Enum::toString).collect(Collectors.joining(", ")) +
                    ".\n" +
                    "INSERT_IGNORE는 동일 Primary Key를 가진 데이터를 INSERT하는 경우 에러를 발생시키지 않고 무시합니다.\n" +
                    "Kudu Operation 추적 기능을 true로 설정했을때 JVM Heap 소비를 줄여줄 수 있습니다.")
            .defaultValue(OperationType.INSERT.toString())
            .addValidator(OperationTypeValidator)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    protected static final PropertyDescriptor FLUSH_MODE = new Builder()
            .name("flush-mode")
            .displayName("Flush 모드")
            .description("Kudu 세션에 Flush 모드를 설정합니다.\n" +
                    "AUTO_FLUSH_SYNC: Kudu Operation을 완료하거나 또는 예외를 발생시킵니다.\n" +
                    "AUTO_FLUSH_BACKGROUND: Kudu Operation을 버퍼에 추가합니다. 메모리에 추가하므로 빠르게 처리되나, 버퍼가 꽉 찰때까지 대기해야할 수 있습니다.\n" +
                    "MANUAL_FLUSH: Kudu Operation을 버퍼에 추가하거나 또는 버퍼가 꽉 찼을때 KuduException을 발생시킵니다.")
            .required(true)
            .defaultValue(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND.toString())
            .allowableValues(SessionConfiguration.FlushMode.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor MAX_ROW_COUNT_PER_BATCH = new Builder()
            .name("max-row-count-per-batch")
            .displayName("1개의 배치당 최대 ROW 수")
            .description("단일 Kudu 클라이언트가 1회 배치 처리에서 처리할 최대 레코드 수는 1~100000입니다.\n" +
                    "1000개로 지정한 경우 1000개 마다 Kudu Session을 flush 처리합니다.\n" +
                    "메모리 크기 및 행당 데이터 크기에 따라 적절한 일괄 처리 크기를 설정합니다.\n" +
                    "최고의 성능을 위한 최상의 숫자를 찾으려면 이 숫자를 점차적으로 늘리면서 테스트하여 결정하도록 합니다.")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.createLongValidator(1, 100000, true))
            .build();
    protected static final PropertyDescriptor IGNORE_NULL = new Builder()
            .name("ignore-null")
            .displayName("NULL 무시")
            .description("Kudu Put 작업시 NULL 무시. true로 설정하면 non-null만 업데이트합니다.")
            .defaultValue("false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor FAILURE_STRATEGY = new Builder()
            .name("failure-strategy")
            .displayName("실패 처리 방법")
            .description("배치에서 하나 이상의 레코드를 Kudu로 전송할 수 없는 경우 실패 처리 방법을 지정합니다.")
            .required(true)
            .allowableValues(FAILURE_STRATEGY_ROUTE, FAILURE_STRATEGY_ROLLBACK)
            .defaultValue(FAILURE_STRATEGY_ROUTE.getValue())
            .build();
    static final PropertyDescriptor CUSTOM_COLUMN_TIMESTAMP_PATTERNS = new Builder()
            .name("timestamp-pattern-per-column")
            .displayName("Timestamp 컬럼의 Timestamp Format(JSON 형식)")
            .description("각각의 Timestamp 컬럼별로 Timestamp Format을 별도로 지정할 수 있습니다.\n" +
                    "필요하지 않다면 빈값으로 설정하십시오.\n" +
                    "Timestamp Format은 microseconds까지만 지원합니다. 형식:\n" +
                    "{\n" +
                    "  \"formats\": [\n" +
                    "    {\n" +
                    "      \"column-name\": \"COL_TIMESTAMP\",\n" +
                    "      \"timestamp-pattern\": \"yyyy-MM-dd HH:mm:ss\",\n" +
                    "      \"type\": \"TIMESTAMP_MILLIS\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}\n" +
                    "지원하는 Type: " + Arrays.stream(TimestampType.values()).map(Enum::toString).collect(Collectors.joining(", ")))
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor DEFAULT_TIMESTAMP_PATTERN = new Builder()
            .name("default-timestamp-pattern")
            .displayName("Timestamp 컬럼의 기본 날짜 패턴")
            .description("'Timestamp 컬럼의 Timestamp Format(JSON 형식)'으로 컬럼별 파싱 패턴을 지정하지 않으면\n" +
                    "Timestamp 컬럼에 이 파싱 패턴을 일괄 적용합니다.")
            .required(true)
            .defaultValue("yyyy-MM-dd HH:mm:ss.SSS")
            .addValidator(timestampValidator)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor ROW_LOGGING_COUNT = new Builder()
            .name("row-logging-count")
            .displayName("ROW 처리 로깅시 ROW 건수")
            .description("ROW를 처리할때 특정 건수마다 로그를 출력합니다. 0으로 설정하는 경우 출력하지 않습니다.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ENVIRONMENT)
            .build();
    static final PropertyDescriptor TRACK_KUDU_OPERATION_PER_ROW = new Builder()
            .name("track-kudu-operation-per-row")
            .displayName("ROW에 대한 Kudu Operation을 추적")
            .description("FlowFile의 각각의 ROW에 대해서 Kudu Operation을 추적합니다.\n" +
                    "이 옵션을 true로 활성화 하면 PutKudu의 기본동작이나\n" +
                    "JVM Heap을 과도하게 사용하여 큰 데이터를 처리할때 NiFi 장애가 발생할 수 있습니다.\n" +
                    "ROW 단위로 에러 추적이 필요하지 않은 경우 false로 설정합니다.")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ENVIRONMENT)
            .build();

    ///////////////////////////////////////////////////
    // onScheduled시 설정하는 값
    // 이 값들은 FlowFile Attribute에서 resolve할 수 없다.
    ///////////////////////////////////////////////////

    /**
     * 1회 배치당 최대 처리하는 Max Row Count
     */
    private volatile int maxRowCountPerBatch = 100;

    /**
     * Timestamp 컬럼에 대해서 추가할 시간
     */
    private volatile int addHour = 0;

    /**
     * JVM Heap 부족을 방지하기 위해서 Kudu Operation을 추적할지 여부
     */
    private volatile boolean isTrackKuduOperationPerRow = false;

    /**
     * Kudu Flush Mode
     */
    private volatile SessionConfiguration.FlushMode flushMode;

    /**
     * FlowFile 처리 실패 방법
     */
    private volatile String failureStrategy;

    /**
     * Kudu가 Insert Ignore를 지원하는지 여부.
     * Kudu의 Insert Ignore는 구버전의 Kudu에서는 지원하지 않는다.
     */
    private volatile boolean supportsInsertIgnoreOp;

    ///////////////////////////////////////////////
    // Method
    ///////////////////////////////////////////////

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(TABLE_NAME);
        properties.add(FAILURE_STRATEGY);
        properties.add(LOWERCASE_FIELD_NAMES);
        properties.add(HANDLE_SCHEMA_DRIFT);
        properties.add(RECORD_READER);
        properties.add(INSERT_OPERATION);
        properties.add(FLUSH_MODE);
        properties.add(MAX_ROW_COUNT_PER_BATCH);
        properties.add(IGNORE_NULL);
        properties.add(TRACK_KUDU_OPERATION_PER_ROW);
        properties.add(WORKER_COUNT);
        properties.add(ADD_HOUR);
        properties.add(KUDU_OPERATION_TIMEOUT_MS);
        properties.add(KUDU_SASL_PROTOCOL_NAME);
        properties.add(KERBEROS_USER_SERVICE);
        properties.add(CUSTOM_COLUMN_TIMESTAMP_PATTERNS);
        properties.add(DEFAULT_TIMESTAMP_PATTERN);
        properties.add(ROW_LOGGING_COUNT);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws LoginException {
        // 아래의 값들은 FlowFile의 Attribute에서 Expression을 통해 Resolve할 수 없다.
        // onScheduled은 FlowFile이 없기 때문이다.
        maxRowCountPerBatch = context.getProperty(MAX_ROW_COUNT_PER_BATCH).asInteger();
        isTrackKuduOperationPerRow = context.getProperty(TRACK_KUDU_OPERATION_PER_ROW).evaluateAttributeExpressions().asBoolean();
        addHour = context.getProperty(ADD_HOUR).asInteger();
        flushMode = SessionConfiguration.FlushMode.valueOf(context.getProperty(FLUSH_MODE).getValue().toUpperCase());

        createKerberosUserAndOrKuduClient(context);

        // Kudu가 Insert Ignore를 지원하는지 확인합니다. 구버전의 Kudu는 Insert Ignore를 지원하지 않습니다.
        supportsInsertIgnoreOp = supportsIgnoreOperations();
        getLogger().info("Kudu Cluster의 Insert Ignore 지원 여부: {}", supportsInsertIgnoreOp ? "지원" : "미지원");

        failureStrategy = context.getProperty(FAILURE_STRATEGY).getValue();
    }

    private boolean isRollbackOnFailure() {
        return FAILURE_STRATEGY_ROLLBACK.getValue().equalsIgnoreCase(failureStrategy);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // 원본 코드에서는 N개의 FlowFile을 처리하도록 구현되어 있으나 Concurrent Tasks로 제어하면 되며,
        // 처리의 복잡도를 줄이기 위해서 1개의 FlowFile만 처리한다.
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Timestamp 컬럼을 위해서 Timestamp Format(Pattern)을 지정한 JSON을 로딩한다.
        // 이 파라마터는 FlowFile Attribute를 지원한다.
        String customTimestampPatterns = context.getProperty(CUSTOM_COLUMN_TIMESTAMP_PATTERNS).evaluateAttributeExpressions(flowFile).getValue();
        String defaultTimestampPatterns = context.getProperty(DEFAULT_TIMESTAMP_PATTERN).evaluateAttributeExpressions(flowFile).getValue();
        getLogger().info("지정한 Timestamp 패턴(포맷) JSON : \n{}", customTimestampPatterns);
        getLogger().info("기본 Timestamp 패턴(포맷) : {}", defaultTimestampPatterns);

        TimestampFormatHolder holder = null;
        if (!StringUtils.isEmpty(customTimestampPatterns)) {
            try {
                TimestampFormats timestampFormats = mapper.readValue(customTimestampPatterns, TimestampFormats.class);
                holder = new TimestampFormatHolder(timestampFormats);
            } catch (Exception e) {
                getLogger().warn("Timestamp Pattern JSON을 파싱할 수 없습니다. JSON : \n{}", customTimestampPatterns, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        }

        final TimestampFormatHolder finalHolder = holder;

        final KerberosUser user = getKerberosUser();
        if (user == null) {
            executeOnKuduClient(kuduClient -> processFlowFiles(context, session, flowFile, kuduClient, finalHolder, defaultTimestampPatterns));
            return;
        }

        final PrivilegedExceptionAction<Void> privilegedAction = () -> {
            executeOnKuduClient(kuduClient -> processFlowFiles(context, session, flowFile, kuduClient, finalHolder, defaultTimestampPatterns));
            return null;
        };

        final KerberosAction<Void> action = new KerberosAction<>(user, privilegedAction, getLogger());
        action.execute();
    }

    private void processFlowFiles(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final KuduClient kuduClient, TimestampFormatHolder holder, String defaultTimestampPatterns) {
        final Map<FlowFile, Integer> processedRecords = new HashMap<>();
        final Map<FlowFile, Object> flowFileFailures = new HashMap<>();

        /*
         * 각 Kudu Operation을 FlowFile과 매핑합니다.
         * FlowFile의 ROW가 많은 경우 이 Map은 JVM Heap을 과도하게 소비합니다..
         */
        final Map<Operation, FlowFile> operationFlowFileMap = new HashMap<>();
        final List<RowError> pendingRowErrors = new ArrayList<>();

        final KuduSession kuduSession = createKuduSession(kuduClient);
        try {
            processRecords(flowFile,
                    processedRecords,
                    flowFileFailures,
                    operationFlowFileMap,
                    pendingRowErrors,
                    session,
                    context,
                    kuduClient,
                    kuduSession,
                    holder,
                    defaultTimestampPatterns);
        } finally {
            try {
                flushKuduSession(kuduSession, true, pendingRowErrors);
            } catch (final KuduException | RuntimeException e) {
                getLogger().error("KuduSession.close() 실패", e);
            }
        }

        if (isRollbackOnFailure() && (!pendingRowErrors.isEmpty() || !flowFileFailures.isEmpty())) {
            logFailures(pendingRowErrors, operationFlowFileMap);
            session.rollback();
            context.yield();
        } else {
            transferFlowFiles(flowFile, processedRecords, flowFileFailures, operationFlowFileMap, pendingRowErrors, session);
        }
    }

    private void processRecords(final FlowFile flowFile,
                                final Map<FlowFile, Integer> processedRecords,
                                final Map<FlowFile, Object> flowFileFailures,
                                final Map<Operation, FlowFile> operationFlowFileMap,
                                final List<RowError> pendingRowErrors,
                                final ProcessSession session,
                                final ProcessContext context,
                                final KuduClient kuduClient,
                                final KuduSession kuduSession,
                                final TimestampFormatHolder holder,
                                final String defaultTimestampPatterns) {

        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        int rowLoggingCount = context.getProperty(ROW_LOGGING_COUNT).evaluateAttributeExpressions().asInteger();

        int bufferedRecords = 0;
        OperationType prevOperationType = OperationType.INSERT;
        try (final InputStream in = session.read(flowFile);
             final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger())) {

            final String tableName = getEvaluatedProperty(TABLE_NAME, context, flowFile);
            final boolean ignoreNull = Boolean.parseBoolean(getEvaluatedProperty(IGNORE_NULL, context, flowFile));
            final boolean lowercaseFields = Boolean.parseBoolean(getEvaluatedProperty(LOWERCASE_FIELD_NAMES, context, flowFile));
            final boolean handleSchemaDrift = Boolean.parseBoolean(getEvaluatedProperty(HANDLE_SCHEMA_DRIFT, context, flowFile));

            final OperationType staticOperationType = OperationType.valueOf(getEvaluatedProperty(INSERT_OPERATION, context, flowFile).toUpperCase());
            final Function<Record, OperationType> operationTypeFunction = record -> staticOperationType;

            // RecordReader로 1개의 ROW를 로딩하여 반환합니다. RecordSet은 Schema와 Row의 묶음입니다.
            final RecordSet recordSet = recordReader.createRecordSet();

            // Kudu 테이블을 오픈합니다.
            KuduTable kuduTable = kuduClient.openTable(tableName);

            // Schema Drift를 처리하기 첫번째 레코드로 Kudu 테이블을 평가합니다.
            Record record = recordSet.next();

            // handleSchemaDrift가 true면, 누락된 컬럼을 확인하여 Kudu 테이블에 컬럼을 추가합니다.
            if (handleSchemaDrift) {
                final boolean driftDetected = handleSchemaDrift(kuduTable, kuduClient, flowFile, record, lowercaseFields);

                if (driftDetected) {
                    // 새로운 Schema를 위해서 테이블을 오픈합니다.
                    kuduTable = kuduClient.openTable(tableName);
                }
            }

            long startTime = System.currentTimeMillis();
            int rowCount = 0;
            recordReaderLoop:
            while (record != null) {
                final OperationType operationType = operationTypeFunction.apply(record);

                final List<Record> dataRecords = Collections.singletonList(record); // Data Record Path는 무시함

                for (final Record dataRecord : dataRecords) {
                    // supportIgnoreOps가 false, INSERT_IGNORE로 설정한 경우 Kudu 세션은 ROW 오류를 무시하도록 합니다.
                    // 구버전의 Kudu는 Insert Ignore를 지원하지 않습니다.
                    // 따라서 구버전의 Kudu에서 INSERT_IGNORE를 사용하면 IgnoreAllDuplicatRows를 활성화 합니다.
                    if (!supportsInsertIgnoreOp && prevOperationType != operationType && (prevOperationType == OperationType.INSERT_IGNORE || operationType == OperationType.INSERT_IGNORE)) {
                        // 버퍼를 비우고 IgnoreAllDuplicatRows를 설정합니다.
                        flushKuduSession(kuduSession, false, pendingRowErrors);
                        kuduSession.setIgnoreAllDuplicateRows(operationType == OperationType.INSERT_IGNORE);
                    }
                    prevOperationType = operationType;

                    final List<String> fieldNames = dataRecord.getSchema().getFieldNames();

                    // Kudu Operation을 생성한다.
                    Operation operation = createKuduOperation(operationType, dataRecord, fieldNames, ignoreNull, lowercaseFields, kuduTable, holder, defaultTimestampPatterns);

                    // FlowFile에 속한 ROW를 처리한 Kudu Operation을 매핑한다.
                    // 이렇게 하면 버퍼를 비운후에 FlowFile이 실패했다는 것을 알아낼 수 있습니다.
                    if (isTrackKuduOperationPerRow) operationFlowFileMap.put(operation, flowFile);
                    rowCount++;

                    // "MANUAL_FLUSH is enabled but the buffer is too big" 오류를 피하기 위해서 Kudu Session의 버퍼를 flush합니다.
                    // MANUAL_FLUSH이고 처리한 ROW가 설정한 배치 크기에 도달하면 세션을 flush합니다.
                    if (bufferedRecords == maxRowCountPerBatch && flushMode == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
                        bufferedRecords = 0;
                        flushKuduSession(kuduSession, false, pendingRowErrors);
                    }

                    // Flush 모드가 AUTO_FLUSH_SYNC인 경우에만 OperationResponse을 반환합니다.
                    OperationResponse response = kuduSession.apply(operation);
                    if (response != null && response.hasRowError()) {
                        // 첫 에러 발생시 레코드 처리를 중지합니다.
                        // Kudu는 이전 Operation에 대해서 롤백 기능을 지원하지 않습니다.
                        flowFileFailures.put(flowFile, response.getRowError());
                        getLogger().warn("FlowFile {}을 처리하던 도중 에러가 발생했습니다. 처리를 중지합니다. 에러: ", flowFile, response.getRowError());
                        break recordReaderLoop;
                    }

                    bufferedRecords++;

                    // FlowFile 별로 처리한 Record를 기록합니다.
                    processedRecords.merge(flowFile, 1, Integer::sum);

                    if (rowLoggingCount > 0 && (rowCount % rowLoggingCount == 0)) {
                        getLogger().info("{}", String.format("FlowFile %s은 현재 %s개 ROW를 처리하고 있습니다. 처리 시간: %s", flowFile, rowCount, formatHumanReadableTime(System.currentTimeMillis() - startTime)));
                    }
                }

                record = recordSet.next();
            }

            if (rowLoggingCount > 0) {
                getLogger().info("{}", String.format("FlowFile %s은 현재 %s개 ROW를 처리하였습니다. 완료 시간: %s", flowFile, rowCount, formatHumanReadableTime(System.currentTimeMillis() - startTime)));
            }
        } catch (Exception ex) {
            getLogger().error("FlowFile {}을 Kudu에 저장할 수 없습니다.", new Object[]{flowFile}, ex);
            flowFileFailures.put(flowFile, ex);
        }
    }

    private boolean handleSchemaDrift(final KuduTable kuduTable, final KuduClient kuduClient, final FlowFile flowFile, final Record record, final boolean lowercaseFields) {
        if (record == null) {
            getLogger().debug("FlowFile {}에는 Schema Drift를 처리할 레코드가 없습니다.", flowFile);
            return false;
        }

        final String tableName = kuduTable.getName();
        final Schema schema = kuduTable.getSchema();

        final List<RecordField> recordFields = record.getSchema().getFields(); // Data Record Path는 지원하지 않음

        final List<RecordField> missing = recordFields.stream()
                .filter(field -> !schema.hasColumn(lowercaseFields ? field.getFieldName().toLowerCase() : field.getFieldName()))
                .collect(Collectors.toList());

        if (missing.isEmpty()) {
            getLogger().debug("FlowFile {}에서 탐지한 Schema Drift가 없습니다.", flowFile);
            return false;
        }

        getLogger().info("Schema Drift를 처리하기 위해서 테이블 '{}'에 {}개의 컬럼을 추가합니다.", tableName, missing.size());

        // 존재하지 않는 컬럼이 다수 존재한다면 실패 처리를 회피하기 위해서동시에 각 컬럼을 추가한다.
        // 동시에 처리하는 쓰레드 또는 애플리케이션이 Schema Drift 처리를 시도하는 것을 고려하여 생성함.
        for (final RecordField field : missing) {
            try {
                final String columnName = lowercaseFields ? field.getFieldName().toLowerCase() : field.getFieldName();
                kuduClient.alterTable(tableName, getAddNullableColumnStatement(columnName, field.getDataType()));
            } catch (final KuduException e) {
                // 동시에 쓰레드 또는 애플리케이션에서 Schema Drift를 처리하므로 이미 컬럼이 존재한다면 에러가 발생하고 이 경우 무시한다.
                if (e.getStatus().isAlreadyPresent()) {
                    getLogger().info("Schema Drift를 처리하고 있는 중 테이블 '{}'에 이미 컬럼이 존재하는 상태입니다.", tableName);
                } else {
                    throw new ProcessException(e);
                }
            }
        }

        return true;
    }

    private void transferFlowFiles(final FlowFile flowFile,
                                   final Map<FlowFile, Integer> processedRecords,
                                   final Map<FlowFile, Object> flowFileFailures,
                                   final Map<Operation, FlowFile> operationFlowFileMap,
                                   final List<RowError> pendingRowErrors,
                                   final ProcessSession session) {

        getLogger().info("{}", String.format("Operation File Map : %s, FlowFile Failure : %s, Pending Row Errors : %s", operationFlowFileMap.size(), flowFileFailures.size(), pendingRowErrors.size()));

        // 각 FlowFile 마다 RowError를 확인한다. RowError가 많다면 Heap을 많이 소진할 수 있다.
        // isTrackKuduOperationPerRow이 true면 operationFlowFileMap은 비어 있으므로 이것을 처리하는데 시간은 오래 걸리지 않는다.
        List<RowError> rowErrors = null;
        if (isTrackKuduOperationPerRow) {
            Map<FlowFile, List<RowError>> flowFileRowErrors = pendingRowErrors.stream()
                    .filter(e -> operationFlowFileMap.get(e.getOperation()) != null)
                    .collect(
                            // 에러가 발생한 Row의 Kudu Operation이 있는 FlowFile을 그룹핑한다.
                            Collectors.groupingBy(e -> operationFlowFileMap.get(e.getOperation()))
                    );
            rowErrors = flowFileRowErrors.get(flowFile);
        } else {
            rowErrors = pendingRowErrors;
        }

        long totalCount = 0L;
        final int count = processedRecords.getOrDefault(flowFile, 0);
        totalCount += count;

        if (rowErrors != null && !rowErrors.isEmpty()) {
            rowErrors.forEach(rowError -> getLogger().error("Kudu에 저장할 수 없습니다. 에러: {}", rowError.toString()));
            FlowFile f = session.putAttribute(flowFile, RECORD_COUNT_ATTR, Integer.toString(count - rowErrors.size()));
            totalCount -= rowErrors.size(); // 카운터에 에러 ROW를 포함시키지 않는다.
            session.adjustCounter("PutKudu Error Rows", rowErrors.size(), false);
            session.transfer(f, REL_FAILURE);
        } else {
            FlowFile f = session.putAttribute(flowFile, RECORD_COUNT_ATTR, String.valueOf(count));

            if (flowFileFailures.containsKey(flowFile)) {
                getLogger().error("FlowFile '{}'을 Kudu에 저장할 수 없습니다.", flowFileFailures.get(flowFile));
                session.transfer(f, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().send(flowFile, "FlowFile을 Kudu에 성공적으로 저장했습니다.");
            }
        }

        session.adjustCounter("PutKudu Total Rows", totalCount, false);
    }

    private void logFailures(final List<RowError> pendingRowErrors, final Map<Operation, FlowFile> operationFlowFileMap) {
        final Map<FlowFile, List<RowError>> flowFileRowErrors = pendingRowErrors.stream().collect(
                Collectors.groupingBy(e -> operationFlowFileMap.get(e.getOperation())));

        for (final Map.Entry<FlowFile, List<RowError>> entry : flowFileRowErrors.entrySet()) {
            final FlowFile flowFile = entry.getKey();
            final List<RowError> errors = entry.getValue();

            getLogger().error("Kudu에 FlowFile {}을 기록할 수 없습니다. 에러: {}", flowFile, errors);
        }
    }

    private String getEvaluatedProperty(PropertyDescriptor property, ProcessContext context, FlowFile flowFile) {
        PropertyValue evaluatedProperty = context.getProperty(property).evaluateAttributeExpressions(flowFile);
        if (property.isRequired() && evaluatedProperty == null) {
            throw new ProcessException(String.format("속성 `%s`은 필수로 지정해야 합니다. NULL값은 해석할 수 없습니다.", property.getDisplayName()));
        }
        return evaluatedProperty.getValue();
    }

    /**
     * Kudu 세션을 생성한다.
     */
    protected KuduSession createKuduSession(final KuduClient client) {
        final KuduSession kuduSession = client.newSession();
        kuduSession.setMutationBufferSpace(maxRowCountPerBatch);
        kuduSession.setFlushMode(flushMode);
        return kuduSession;
    }

    /**
     * 지정한 Kudu Operation Type에 맞는 Kudu Operation을 생성한다.
     */
    protected Operation createKuduOperation(OperationType operationType,
                                            Record record,
                                            List<String> fieldNames,
                                            boolean ignoreNull,
                                            boolean lowercaseFields,
                                            KuduTable kuduTable,
                                            TimestampFormatHolder holder,
                                            String defaultTimestampPatterns) {
        Operation operation;
        switch (operationType) {
            case INSERT:
                operation = kuduTable.newInsert();
                break;
            case INSERT_IGNORE:
                // Kudu Cluster가 Insert Ignore를 지원하지 않으면 Insert를 사용한다.
                if (!supportsInsertIgnoreOp) {
                    operation = kuduTable.newInsert();
                } else {
                    operation = kuduTable.newInsertIgnore();
                }
                break;
            case UPSERT:
                operation = kuduTable.newUpsert();
                break;
            case UPDATE:
                operation = kuduTable.newUpdate();
                break;
            case UPDATE_IGNORE:
                operation = kuduTable.newUpdateIgnore();
                break;
            case DELETE:
                operation = kuduTable.newDelete();
                break;
            case DELETE_IGNORE:
                operation = kuduTable.newDeleteIgnore();
                break;
            default:
                throw new IllegalArgumentException(String.format("Kudu Operation 유형 '%s'을 Kudu에서 지원하지 않습니다.", operationType));
        }
        buildPartialRow(kuduTable.getSchema(), operation.getRow(), record, fieldNames, ignoreNull, lowercaseFields, addHour, holder, defaultTimestampPatterns);
        return operation;
    }

    public static String formatHumanReadableTime(long diffLongTime) {
        StringBuffer buf = new StringBuffer();
        long hours = diffLongTime / (60 * 60 * 1000);
        long rem = (diffLongTime % (60 * 60 * 1000));
        long minutes = rem / (60 * 1000);
        rem = rem % (60 * 1000);
        long seconds = rem / 1000;

        if (hours != 0) {
            buf.append(hours);
            buf.append("시간 ");
        }
        if (minutes != 0) {
            buf.append(minutes);
            buf.append("분 ");
        }
        // 차이가 없다면 0을 반환한다.
        buf.append(seconds);
        buf.append("초");
        return buf.toString();
    }
}
