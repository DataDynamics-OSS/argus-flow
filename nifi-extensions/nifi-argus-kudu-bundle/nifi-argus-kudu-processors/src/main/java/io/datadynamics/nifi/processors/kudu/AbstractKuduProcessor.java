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
 *   nifi-nar-bundles/nifi-kudu-bundle/nifi-kudu-processors/src/main/java/org/apache/nifi/processors/kudu/AbstractKuduProcessor.java
 */
package io.datadynamics.nifi.processors.kudu;

import io.datadynamics.nifi.processors.kudu.converter.FieldConverter;
import io.datadynamics.nifi.processors.kudu.converter.ObjectTimestampFieldConverter;
import io.datadynamics.nifi.processors.kudu.json.TimestampFormatHolder;
import io.datadynamics.nifi.processors.kudu.util.DataTypeUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Kudu 클라이언트 생성/종료, Kerberos 인증, NiFi Record를 Kudu의 PartialRow로 변환하는 등
 * Kudu 관련 프로세서들이 공통으로 사용하는 기능을 제공하는 추상 클래스입니다.
 * PutKudu 등 실제 프로세서는 이 클래스를 상속하여 공통 Property, 클라이언트 관리, Row 변환 로직을 재사용합니다.
 */
public abstract class AbstractKuduProcessor extends AbstractProcessor {

    static final PropertyDescriptor KUDU_MASTERS = new Builder()
            .name("kudu-masters")
            // 버그 수정: 원본은 여기서 .name("Kudu Master 주소")를 다시 호출하여 실효 프로퍼티 식별자가
            // "kudu-masters"가 아닌 "Kudu Master 주소"로 덮어써졌다(빌더는 마지막 .name() 우선). displayName으로
            // 교정해 식별자를 upstream/다른 프로퍼티와 동일한 kebab-case "kudu-masters"로 복원한다.
            .displayName("Kudu 마스터")
            .description("접속할 Kudu 마스터 주소 목록(쉼표 구분). 예) kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos 사용자 서비스")
            .description("Kerberos 인증시 필요한 Kerberos User Controller Service를 지정하십시오.")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();
    static final PropertyDescriptor KUDU_OPERATION_TIMEOUT_MS = new Builder()
            .name("kudu-operations-timeout-ms")
            .displayName("Kudu 작업 타임아웃")
            .description("Kudu Operation의 기본 타임아웃(세션 및 스캐너에 사용)입니다.")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    static final PropertyDescriptor KUDU_SASL_PROTOCOL_NAME = new Builder()
            .name("kudu-sasl-protocol-name")
            .displayName("Kudu SASL 프로토콜명")
            .description("Kerberos를 통해 인증시 사용할 프로토콜명. Service Principal 명과 반드시 일치해야 합니다.")
            .required(false)
            .defaultValue("kudu")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    /**
     * 기본 Kudu Client의 Worker Thread는 Core의 개수만큼 설정이 되므로 baremetal에서는 이 값을 낮춰서 설정해야 한다.
     */
    private static final int DEFAULT_WORKER_COUNT = 4;
    static final PropertyDescriptor WORKER_COUNT = new Builder()
            .name("worker-count")
            .displayName("Kudu Client Worker 쓰레드 개수")
            .description(String.format("Kudu 클라이언트 읽기 및 쓰기 작업을 처리하는 최대 작업자 스레드 수입니다.\n" +
                    "PutKudu의 경우 기본값으로 현재 Processor의 개수 * 2만큼 설정합니다.\n" +
                    "예를 들어 24Core * 2EA로 구성된 서버라면 vCore로 계산하여 총 96으로 기본값이 설정됩니다.\n" +
                    "현재 사용가능한 프로세스의 개수: %s\n" +
                    "NiFi Flow에 Kudu Client를 사용하는 Processor(예; PutKudu)를 많이 사용하고 이 설정값을 높게 설정한 경우\n" +
                    "NiFi JVM이 과도한 쓰레드를 사용하게 됨으로써 성능 저하가 발생할 수 있습니다.\n" +
                    "일반적으로 NiFi Cluster의 노드 개수만큼 설정하는 것을 권장합니다.", Runtime.getRuntime().availableProcessors() * 2))
            .required(true)
            .defaultValue(Integer.toString(DEFAULT_WORKER_COUNT))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    private static final FieldConverter<Object, Timestamp> TIMESTAMP_FIELD_CONVERTER = new ObjectTimestampFieldConverter();
    private static final String MICROSECOND_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss[.SSSSSS]";
    private final ReadWriteLock kuduClientReadWriteLock = new ReentrantReadWriteLock();
    private final Lock kuduClientReadLock = kuduClientReadWriteLock.readLock();
    private final Lock kuduClientWriteLock = kuduClientReadWriteLock.writeLock();
    Logger logger = LoggerFactory.getLogger(AbstractProcessor.class);
    private volatile KuduClient kuduClient;

    private volatile KerberosUser kerberosUser;

    protected KerberosUser getKerberosUser() {
        return this.kerberosUser;
    }

    protected boolean supportsIgnoreOperations() {
        try {
            return kuduClient.supportsIgnoreOperations();
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * NiFi 2.x에서는 KerberosCredentialsService 및 Kerberos Principal/Password 방식이 제거되어
     * KerberosUserService 기반으로만 Kerberos 인증을 수행합니다.
     */
    protected void createKerberosUserAndOrKuduClient(ProcessContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        if (kerberosUserService != null) {
            kerberosUser = kerberosUserService.createKerberosUser();
            kerberosUser.login();
        }
        createKuduClient(context);
    }

    protected void createKuduClient(ProcessContext context) {
        kuduClientWriteLock.lock();
        try {
            if (this.kuduClient != null) {
                try {
                    this.kuduClient.close();
                } catch (KuduException e) {
                    getLogger().error("Kudu 클라이언트를 종료할 수 없습니다.");
                }
            }

            if (kerberosUser != null) {
                final KerberosAction<KuduClient> kerberosAction = new KerberosAction<>(kerberosUser, () -> buildClient(context), getLogger());
                this.kuduClient = kerberosAction.execute();
            } else {
                this.kuduClient = buildClient(context);
            }
        } finally {
            kuduClientWriteLock.unlock();
        }
    }

    protected KuduClient buildClient(final ProcessContext context) {
        final String masters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
        final int operationTimeout = context.getProperty(KUDU_OPERATION_TIMEOUT_MS).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final String saslProtocolName = context.getProperty(KUDU_SASL_PROTOCOL_NAME).evaluateAttributeExpressions().getValue();
        final int workerCount = context.getProperty(WORKER_COUNT).asInteger();

        // Max Pool Size로 Executors.newCachedThreadPool()에 따라 Executor를 생성합니다.
        final int corePoolSize = 0;
        final long threadKeepAliveTime = 60;
        final Executor nioExecutor = new ThreadPoolExecutor(corePoolSize, workerCount, threadKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<>(), new ClientThreadFactory(getIdentifier()));

        return new KuduClient.KuduClientBuilder(masters).defaultOperationTimeoutMs(operationTimeout).saslProtocolName(saslProtocolName).workerCount(workerCount).nioExecutor(nioExecutor).build();
    }

    protected void executeOnKuduClient(Consumer<KuduClient> actionOnKuduClient) {
        kuduClientReadLock.lock();
        try {
            actionOnKuduClient.accept(kuduClient);
        } finally {
            kuduClientReadLock.unlock();
        }
    }

    protected void flushKuduSession(final KuduSession kuduSession, boolean close, final List<RowError> rowErrors) throws KuduException {
        final List<OperationResponse> responses = close ? kuduSession.close() : kuduSession.flush();

        if (kuduSession.getFlushMode() == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
            rowErrors.addAll(Arrays.asList(kuduSession.getPendingErrors().getRowErrors()));
        } else {
            responses.stream().filter(OperationResponse::hasRowError).map(OperationResponse::getRowError).forEach(rowErrors::add);
        }
    }

    @OnStopped
    public void shutdown() throws Exception {
        try {
            if (this.kuduClient != null) {
                getLogger().debug("Kudu 클라이언트를 종료하는 중입니다.");
                this.kuduClient.close();
                this.kuduClient = null;
            }
        } finally {
            if (kerberosUser != null) {
                kerberosUser.logout();
                kerberosUser = null;
            }
        }
    }

    protected void buildPartialRow(Schema schema,
                                   PartialRow row,
                                   Record record,
                                   List<String> fieldNames,
                                   boolean ignoreNull,
                                   boolean lowercaseFields,
                                   int addHour,
                                   TimestampFormatHolder holder,
                                   String defaultTimestampPatterns) {

        for (String recordFieldName : fieldNames) {
            String colName = recordFieldName;
            if (lowercaseFields) {
                colName = colName.toLowerCase();
            }

            if (!schema.hasColumn(colName)) {
                continue;
            }

            final int columnIndex = schema.getColumnIndex(colName);
            final ColumnSchema colSchema = schema.getColumnByIndex(columnIndex);
            final Type colType = colSchema.getType();

            if (record.getValue(recordFieldName) == null) {
                if (schema.getColumnByIndex(columnIndex).isKey()) {
                    throw new IllegalArgumentException(String.format("Primary Key 컬럼 '%s'은 NULL로 설정할 수 없습니다.", colName));
                } else if (!schema.getColumnByIndex(columnIndex).isNullable()) {
                    throw new IllegalArgumentException(String.format("컬럼 '%s'를 NULL로 설정할 수 없습니다.", colName));
                }

                if (!ignoreNull) {
                    row.setNull(colName);
                }
            } else {
                Object value = record.getValue(recordFieldName);
                final Optional<DataType> fieldDataType = record.getSchema().getDataType(recordFieldName);

                // JSON 형식의 컬럼별 Timestamp Format을 지정하지 않으면 기본 포맷을 사용하거나 Avro의 Timestamp Format을 사용한다.
                String defaultPattern = defaultTimestampPatterns == null ? fieldDataType.map(DataType::getFormat).orElse(null) : defaultTimestampPatterns;
                String finalTimestampPattern = (holder == null ? defaultPattern : (StringUtils.isEmpty(holder.getPattern(recordFieldName)) ? defaultPattern : holder.getPattern(recordFieldName)));
                if (getLogger().isDebugEnabled()) getLogger().debug("{}", String.format("Record Field Name: {} ==> {}, Date Format: {}", recordFieldName, colType, finalTimestampPattern));

                switch (colType) {
                    case BOOL:
                        row.addBoolean(columnIndex, DataTypeUtils.toBoolean(value, recordFieldName));
                        break;
                    case INT8:
                        row.addByte(columnIndex, DataTypeUtils.toByte(value, recordFieldName));
                        break;
                    case INT16:
                        row.addShort(columnIndex, DataTypeUtils.toShort(value, recordFieldName));
                        break;
                    case INT32:
                        row.addInt(columnIndex, DataTypeUtils.toInteger(value, recordFieldName));
                        break;
                    case INT64:
                        row.addLong(columnIndex, DataTypeUtils.toLong(value, recordFieldName));
                        break;
                    case UNIXTIME_MICROS:
                        Optional<String> optionalPattern = Optional.of(finalTimestampPattern);
                        if (getLogger().isDebugEnabled()) getLogger().debug("{}", String.format("[Timestamp] Record Field Name: %s ==> %s, Date Format: %s, Add Hour: %s", recordFieldName, colType, finalTimestampPattern, addHour));
                        final Timestamp timestamp = TIMESTAMP_FIELD_CONVERTER.convertField(value, optionalPattern, recordFieldName, addHour, finalTimestampPattern);
                        row.addTimestamp(columnIndex, timestamp);
                        break;
                    case STRING:
                        row.addString(columnIndex, DataTypeUtils.toString(value, finalTimestampPattern));
                        break;
                    case BINARY:
                        row.addBinary(columnIndex, DataTypeUtils.toString(value, finalTimestampPattern).getBytes());
                        break;
                    case FLOAT:
                        row.addFloat(columnIndex, DataTypeUtils.toFloat(value, recordFieldName));
                        break;
                    case DOUBLE:
                        row.addDouble(columnIndex, DataTypeUtils.toDouble(value, recordFieldName));
                        break;
                    case DECIMAL:
                        row.addDecimal(columnIndex, new BigDecimal(DataTypeUtils.toString(value, finalTimestampPattern)));
                        break;
                    case VARCHAR:
                        row.addVarchar(columnIndex, DataTypeUtils.toString(value, finalTimestampPattern));
                        break;
                    case DATE:
                        final String dateFormat = finalTimestampPattern == null ? RecordFieldType.DATE.getDefaultFormat() : finalTimestampPattern;
                        row.addDate(columnIndex, getDate(value, recordFieldName, addHour, dateFormat));
                        break;
                    default:
                        throw new IllegalStateException(String.format("'%s'은 알 수 없는 컬럼형", colType));
                }
            }
        }
    }

    /**
     * Timestamp 패턴을 반환하거나, 선택적으로 레코드 필드의 유형에Ekfktj 마이크로초 타임스탬프 패턴을 반환합니다.
     *
     * @param optionalDataType Data Type
     * @return Date Format Pattern
     */
    private Optional<String> getTimestampPattern(final Optional<DataType> optionalDataType) {
        String pattern = null;
        if (optionalDataType.isPresent()) {
            final DataType dataType = optionalDataType.get();
            if (RecordFieldType.TIMESTAMP == dataType.getFieldType()) {
                pattern = MICROSECOND_TIMESTAMP_PATTERN;
            } else {
                pattern = dataType.getFormat();
            }
        }
        return Optional.ofNullable(pattern);
    }

    /**
     * 입력 값이 문자열인 경우 선택적 구문 분석을 사용하여 레코드 필드 값에서 java.sql.Date 가져옵니다.
     *
     * @param value           Record Field Value
     * @param recordFieldName Record Field Name
     * @param addHour         Hour to Add
     * @param dateFormat      Date Format Pattern
     * @return 날짜 객체 또는 값이 null인 경우 null
     */
    private Date getDate(final Object value, final String recordFieldName, int addHour, final String dateFormat) {
        final LocalDate localDate = DataTypeUtils.toLocalDate(value, () -> {
            return DataTypeUtils.getDateTimeFormatter(dateFormat, ZoneId.systemDefault());
        }, recordFieldName);
        return Date.valueOf(localDate);
    }

    /**
     * NiFi DataType을 Kudu Type으로 변환합니다.
     */
    private Type toKuduType(DataType nifiType) {
        switch (nifiType.getFieldType()) {
            case BOOLEAN:
                return Type.BOOL;
            case BYTE:
                return Type.INT8;
            case SHORT:
                return Type.INT16;
            case INT:
                return Type.INT32;
            case LONG:
                return Type.INT64;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case DECIMAL:
                return Type.DECIMAL;
            case TIMESTAMP:
                return Type.UNIXTIME_MICROS;
            case CHAR:
            case STRING:
                return Type.STRING;
            case DATE:
                return Type.DATE;
            default:
                throw new IllegalArgumentException(String.format("'%s'은 지원하지 않는 자료형", nifiType));
        }
    }

    private ColumnTypeAttributes getKuduTypeAttributes(final DataType nifiType) {
        if (nifiType.getFieldType().equals(RecordFieldType.DECIMAL)) {
            final DecimalDataType decimalDataType = (DecimalDataType) nifiType;
            return new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(decimalDataType.getPrecision()).scale(decimalDataType.getScale()).build();
        } else {
            return null;
        }
    }

    /**
     * NiFi 필드 선언을 기반으로 새 열로 테이블을 확장하는 alter 문을 생성합니다.
     * 참고: BigDecimal 소수 자릿수 및 정밀도 처리를 다루지 않으므로 단순히 {@link AlterTableOptions#addNullableColumn(String, Type)}을 호출하는 것만으로는 충분하지 않습니다.
     *
     * @param columnName 새로운 컬럼명
     * @param nifiType   필드 타입
     * @return 새로운 필드명을 포함하는 Alter Table 구문
     */
    protected AlterTableOptions getAddNullableColumnStatement(final String columnName, final DataType nifiType) {
        final AlterTableOptions alterTable = new AlterTableOptions();
        alterTable.addColumn(new ColumnSchema.ColumnSchemaBuilder(columnName, toKuduType(nifiType)).nullable(true).defaultValue(null).typeAttributes(getKuduTypeAttributes(nifiType)).build());
        return alterTable;
    }

    private static class ClientThreadFactory implements ThreadFactory {
        private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();

        private final AtomicInteger threadCount = new AtomicInteger();

        private final String identifier;

        private ClientThreadFactory(final String identifier) {
            this.identifier = identifier;
        }

        /**
         * 지정한 이름으로 Daemon Thread를 생성합니다.
         *
         * @param runnable Runnable
         * @return Created Thread
         */
        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = defaultThreadFactory.newThread(runnable);
            thread.setDaemon(true);
            thread.setName(getName());
            return thread;
        }

        private String getName() {
            return String.format("PutKudu[%s]-client-%d", identifier, threadCount.getAndIncrement());
        }
    }
}
