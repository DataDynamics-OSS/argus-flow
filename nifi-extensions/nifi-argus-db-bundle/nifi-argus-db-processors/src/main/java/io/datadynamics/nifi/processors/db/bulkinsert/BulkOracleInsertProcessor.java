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
package io.datadynamics.nifi.processors.db.bulkinsert;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT;


/**
 * Oracle DB에 FlowFile의 Content를 포함하는 데이터를 Bulk Insert를 수행하는 Processor.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"dd", "custom", "sql", "select", "jdbc", "query", "database"})
public class BulkOracleInsertProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Bulk Insert SQL을 성공적으로 실행함")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL 실행 에러. 입력으로 들어온 FlowFile은 라우팅되거나 페널티를 부여하게 됨")
            .build();
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .displayName("데이터베이스 커넥션 풀링 서비스")
            .description("데이터베이스 연결에 필요한 커넥션 풀링 컨트롤러 서비스")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("데이터베이스 접근는 성공하였으나 INSERT 등에서 실패시 재시도를 위해서 라우팅함.")
            .build();
    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("put-db-record-record-reader")
            .displayName("레코드 리더")
            .description("입력 FlowFile에서 Record를 읽기 위한 Record Reader Service")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor BULK_INSERT_ROWS = new PropertyDescriptor.Builder()
            .name("bulk-insert-rows")
            .displayName("벌크 삽입 로우 수")
            .description("벌크로 INSERT할 ROW 수")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ENVIRONMENT)
            .defaultValue("100")
            .required(true)
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("put-db-record-query-timeout")
            .displayName("최대 대기 시간")
            .description("SQL문 실행시 허용하는 최대 시간(초). 0 설정시 무한대를 의미하며 1보다 작게 설정하는 경우 0과 동일하게 간주합니다.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ENVIRONMENT)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-schema-name")
            .displayName("스키마명")
            .description("데이터를 넣을 테이블의 Schema명")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-table-name")
            .displayName("테이블명")
            .description("데이터를 넣을 테이블명. 스키마명을 지정하지 않으면 테이블명만 사용합니다.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor AVRO_SCHEMA = new PropertyDescriptor.Builder()
            .name("avro-schema")
            .displayName("Avro 스키마 JSON")
            .description("Record를 파싱하기 위한 Avro Schema JSON. NiFi의 Record Reader의 Schema의 경우 Logical Data Type을 완전하게 포함하고 있지 않으므로(예; Deciaml의 precision, scale) 이를 확인하기 위해서 추가적으로 Avro Schema를 사용자가 입력한다.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Bulk Insert 실패시 에러 메시지를 담아 REL_FAILURE/REL_RETRY로 전달되는 FlowFile 속성명
    static final String BULK_INSERT_ERROR = "bulk.oracle.insert.error";

    // SQL에 직접 들어가는 식별자(스키마명/테이블명/컬럼명)로 허용하는 형식.
    // Oracle의 비인용 식별자 규칙(영문자로 시작, 영숫자/_/$/#) 또는 큰따옴표로 감싼 인용 식별자(내부에 큰따옴표/NUL 불가)만 허용한다.
    private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[A-Za-z][A-Za-z0-9_$#]*");
    private static final Pattern QUOTED_IDENTIFIER = Pattern.compile("\"[^\"\u0000]+\"");

    // 이 프로세서가 지원하는 관계(Relationship) 집합 (getRelationships()에서 매 호출마다 재구성)
    protected Set<Relationship> relationships;
    // 이 프로세서가 지원하는 PropertyDescriptor 목록 (getSupportedPropertyDescriptors()에서 매 호출마다 재구성)
    protected List<PropertyDescriptor> propDescriptors;
    /**
     * DBCP Connection Pooling Service
     */
    protected DBCPService dbcpService;

    @Override
    public Set<Relationship> getRelationships() {
        relationships = new HashSet<>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_RETRY);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        propDescriptors = new ArrayList<>();
        propDescriptors.add(DBCP_SERVICE);
        propDescriptors.add(RECORD_READER_FACTORY);
        propDescriptors.add(BULK_INSERT_ROWS);
        propDescriptors.add(QUERY_TIMEOUT);
        propDescriptors.add(SCHEMA_NAME);
        propDescriptors.add(TABLE_NAME);
        propDescriptors.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        propDescriptors.add(AVRO_SCHEMA);
        return propDescriptors;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        /////////////////////////////////////////
        // FlowFile 확인
        /////////////////////////////////////////

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        /////////////////////////////////////////
        // INSERT SQL 생성 및 JDBC 처리
        /////////////////////////////////////////

        Optional<Connection> connectionHolder = Optional.empty();

        boolean originalAutoCommit = false;
        try {
            final Connection connection = dbcpService.getConnection(flowFile.getAttributes());
            connectionHolder = Optional.of(connection);

            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            bulkInsert(context, session, flowFile, connection);
            connection.commit();

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, getJdbcUrl(connection));
        } catch (final Exception e) {
            // 트랜잭션을 지원하므로 SQLTransientException 예외가 발생하는 경우 Retry를 시도한다.
            final Relationship relationship;
            if (isTransient(e)) {
                relationship = REL_RETRY;
                flowFile = session.penalize(flowFile);
            } else {
                relationship = REL_FAILURE;
            }

            getLogger().error("FlowFile '{}'의 Bulk Insert 실패. {}으로 라우팅.", flowFile, relationship, e);

            // Rollback할 것인가? 실패 처리할 것인가?
            final boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
            if (rollbackOnFailure) {
                session.rollback();
                // 롤백된 FlowFile은 즉시 다시 처리되므로 yield하여 실패 직후 무한 재시도 루프를 막는다.
                context.yield();
            } else {
                flowFile = session.putAttribute(flowFile, BULK_INSERT_ERROR, getErrorMessage(e));
                session.transfer(flowFile, relationship);
            }

            connectionHolder.ifPresent(connection -> {
                try {
                    connection.rollback();
                } catch (final Exception rollbackException) {
                    getLogger().error("JDBC Transaction Rollback 실패", rollbackException);
                }
            });
        } finally {
            if (originalAutoCommit) {
                connectionHolder.ifPresent(connection -> {
                    try {
                        connection.setAutoCommit(true);
                    } catch (final Exception autoCommitException) {
                        getLogger().warn("Auto Commit 설정 실패", autoCommitException);
                    }
                });
            }

            connectionHolder.ifPresent(connection -> {
                try {
                    connection.close();
                } catch (final Exception closeException) {
                    getLogger().warn("Database Connection 닫기 실패", closeException);
                }
            });
        }
    }

    /**
     * 예외가 재시도 가능한(SQLTransientException) 오류인지 확인한다.
     * 예외 자신과 cause 체인, SQLException의 next exception 체인(BatchUpdateException 포함)을 모두 확인한다.
     *
     * @param e 발생한 예외
     * @return SQLTransientException이 포함되어 있으면 true
     */
    private boolean isTransient(final Throwable e) {
        final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        final Deque<Throwable> pending = new ArrayDeque<>();
        pending.add(e);
        while (!pending.isEmpty()) {
            final Throwable current = pending.poll();
            if (current == null || !visited.add(current)) {
                continue;
            }
            if (current instanceof SQLTransientException) {
                return true;
            }
            if (current.getCause() != null) {
                pending.add(current.getCause());
            }
            if (current instanceof SQLException && ((SQLException) current).getNextException() != null) {
                pending.add(((SQLException) current).getNextException());
            }
        }
        return false;
    }

    /**
     * FlowFile 속성에 기록할 에러 메시지를 반환한다. 예외 메시지가 없으면 예외 클래스명을 사용한다.
     *
     * @param e 발생한 예외
     * @return 에러 메시지 (null이 아님)
     */
    private String getErrorMessage(final Throwable e) {
        final String message = e.getMessage();
        return message != null ? message : e.getClass().getName();
    }

    /**
     * Oracle Bulk Insert를 실행한다.
     *
     * @param context    Process Context
     * @param session    Process Session
     * @param flowFile   실제 데이터를 포함하는 (예; Avro) Flow File
     * @param connection JDBC Connection
     * @throws Exception Bulk Insert 및 FlowFile을 처리할 수 없는 경우
     */
    private void bulkInsert(ProcessContext context, ProcessSession session, FlowFile flowFile, Connection connection) throws Exception {
        // FlowFile의 Content에서 데이터를 로딩하기 위해서 Record Reader를 확인하고 Bulk Insert를 진행함
        // Record Reader도 자원(버퍼, 스키마 접근 등)을 가지므로 InputStream과 함께 닫는다.
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        try (final InputStream in = session.read(flowFile);
             final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger())) {
            _bulkInsert(context, flowFile, connection, recordReader);
        }
    }

    /**
     * Oracle Bulk Insert를 실행한다.
     *
     * @param context      Process Context
     * @param flowFile     실제 데이터를 포함하는 (예; Avro) Flow File
     * @param connection   JDBC Connection
     * @param recordReader FlowFile의 Content에서 데이터를 로딩하기 위한 Record Reader
     * @throws MalformedRecordException Record Reader가 스키마를 로딩할 수 없는 경우
     * @throws IOException              Record Reader가 파일을 로딩할 수 없는 경우
     * @throws SQLException             Bulk Insert를 실행할 수 없는 경우
     */
    private void _bulkInsert(final ProcessContext context, final FlowFile flowFile, final Connection connection, final RecordReader recordReader)
            throws MalformedRecordException, IOException, SQLException {

        /////////////////////////////////////////
        // SQL 생성 및 처리에 필요한 옵션
        /////////////////////////////////////////

        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final Integer maxBulkRowCount = context.getProperty(BULK_INSERT_ROWS).evaluateAttributeExpressions().asInteger();

        /////////////////////////////////////////
        // 사용자가 입력한 Avro Schema를 파싱한다.
        /////////////////////////////////////////

        // AVRO_SCHEMA는 FLOWFILE_ATTRIBUTES 범위이므로 FlowFile 속성으로 평가한다.
        final String avroSchemaJson = context.getProperty(AVRO_SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
        Schema schema = new Schema.Parser().parse(avroSchemaJson);

        ///////////////////////////////////////////////////////////////////////////////////
        // FlowFile Content의 데이터를 처리하는 Record Reader가 해당 데이터의 Schema를 확인한다.
        ///////////////////////////////////////////////////////////////////////////////////

        final RecordSchema recordSchema = recordReader.getSchema();

        /////////////////////////////////////////
        // SQL을 실행한다.
        /////////////////////////////////////////

        // SQL에 직접 들어가는 식별자는 바인딩할 수 없으므로 실행 전에 검증한다. (SQL Injection 방지)
        final String qualifiedTableName = getTableName(schemaName, tableName);
        final String columns = getColumns(recordSchema);

        // Statement.setQueryTimeout()은 초 단위이므로 초로 변환한다.
        final int timeoutSeconds = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();

        // Max Bulk Row Count 만큼 채운 배치는 SQL이 동일하므로 PreparedStatement를 재사용하고, 남은 데이터는 별도로 처리한다.
        PreparedStatement fullBatchStatement = null;
        try {
            Record currentRecord;
            List<Record> records = new ArrayList<>();
            while ((currentRecord = recordReader.nextRecord()) != null) {
                records.add(currentRecord);
                if (records.size() >= maxBulkRowCount) { // 한번에 처리하려는 ROW Count 만큼만 처리
                    if (fullBatchStatement == null) {
                        final String sql = generateInsertQuery(qualifiedTableName, columns, recordSchema, records.size());
                        fullBatchStatement = prepareStatement(connection, sql, timeoutSeconds);
                    }
                    executeInsert(fullBatchStatement, recordSchema, records, schema);
                    records.clear();
                }
            }

            // Max Bulk Row Count에 도달하지 않은 남은 데이터를 처리
            if (!records.isEmpty()) {
                final String sql = generateInsertQuery(qualifiedTableName, columns, recordSchema, records.size());
                // SQL에는 값이 아닌 바인딩 파라미터(?)만 들어 있지만 행 수만큼 길어지므로 DEBUG로 남긴다.
                this.getLogger().debug("Bulk Insert SQL : \n{}", sql);
                try (final PreparedStatement statement = prepareStatement(connection, sql, timeoutSeconds)) {
                    executeInsert(statement, recordSchema, records, schema);
                }
                records.clear();
            }
        } finally {
            if (fullBatchStatement != null) {
                fullBatchStatement.close();
            }
        }
    }

    /**
     * PreparedStatement를 생성하고 Query Timeout을 설정한다.
     *
     * @param connection    JDBC Connection
     * @param sql           바인딩 파라미터(?)를 포함하는 SQL
     * @param timeoutSeconds Query Timeout(초)
     * @return PreparedStatement
     * @throws SQLException PreparedStatement를 생성할 수 없는 경우
     */
    private PreparedStatement prepareStatement(final Connection connection, final String sql, final int timeoutSeconds) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout(timeoutSeconds); // 초단위 타임아웃
        } catch (SQLException se) {
            // 만약에 JDBC Driver가 Query Timeout을 지우너하지 않으면 무한으로 가정한다. 이 경우 0만 허용한다.
            if (timeoutSeconds > 0) {
                statement.close();
                throw se;
            }
        }
        return statement;
    }

    /**
     * N개 ROW의 값을 PreparedStatement에 바인딩하고 실행한다.
     *
     * @param statement    INSERT ALL PreparedStatement
     * @param recordSchema 레코드 스키마
     * @param records      N개 ROW
     * @param schema       Avro Schema
     * @throws SQLException 바인딩 또는 실행할 수 없는 경우
     */
    private void executeInsert(final PreparedStatement statement, final RecordSchema recordSchema, final List<Record> records, final Schema schema) throws SQLException {
        statement.clearParameters();
        final List<RecordField> fields = recordSchema.getFields();
        int index = 1;
        for (Record record : records) {
            for (RecordField field : fields) {
                final Object value = getValue(field, record, schema);
                if (value == null) {
                    statement.setNull(index, getSqlType(field, schema)); // ORACLE NULL 처리
                } else if (value instanceof Timestamp) {
                    statement.setTimestamp(index, (Timestamp) value);
                } else if (value instanceof Time) {
                    statement.setTime(index, (Time) value);
                } else if (value instanceof java.sql.Date) {
                    statement.setDate(index, (java.sql.Date) value);
                } else if (value instanceof byte[]) {
                    statement.setBytes(index, (byte[]) value); // RAW/BLOB 등 바이너리 컬럼
                } else {
                    statement.setObject(index, value);
                }
                index++;
            }
        }
        statement.execute();
    }

    /**
     * Oracle INSERT SQL 문을 생성한다. 값은 모두 바인딩 파라미터(?)로 구성한다.
     *
     * @param qualifiedTableName 검증된 "스키마명.테이블명"
     * @param columns            검증된 컬럼명 목록
     * @param recordSchema       레코드 스키마
     * @param rowCount           ROW 수
     * @return SQL
     */
    private String generateInsertQuery(String qualifiedTableName, String columns, RecordSchema recordSchema, int rowCount) {
        String sql = "INSERT ALL" + "\n" +
                _generateInsertQuery(qualifiedTableName, columns, recordSchema, rowCount) + "\n" +
                "SELECT 1 FROM dual";
        return sql;
    }

    /**
     * Insert All의 Sub Insert를 생성한다.
     *
     * @param qualifiedTableName 검증된 "스키마명.테이블명"
     * @param columns            검증된 컬럼명 목록
     * @param recordSchema       레코드 스키마
     * @param rowCount           ROW 수
     * @return SQL
     */
    private String _generateInsertQuery(String qualifiedTableName, String columns, RecordSchema recordSchema, int rowCount) {
        final String insert = getInsertSQL(qualifiedTableName, columns, recordSchema);
        List<String> inserts = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            inserts.add(insert);
        }
        return String.join("\n", inserts);
    }

    /**
     * Insert All의 Sub Insert를 생성한다.
     *
     * @param qualifiedTableName 검증된 "스키마명.테이블명"
     * @param columns            검증된 컬럼명 목록
     * @param recordSchema       레코드 스키마
     * @return SQL
     */
    private String getInsertSQL(String qualifiedTableName, String columns, RecordSchema recordSchema) {
        return String.format("INTO %s ( %s ) VALUES ( %s )", qualifiedTableName, columns, getPlaceholders(recordSchema));
    }

    /**
     * 테이블명을 구성한다.
     *
     * @param schemaName 스키마명
     * @param tableName  테이블명
     * @return "스키마명.테이블명" (스키마명이 없으면 "테이블명")
     * @throws IllegalArgumentException 스키마명 또는 테이블명이 유효한 식별자가 아닌 경우
     */
    private String getTableName(String schemaName, String tableName) {
        final String table = validateIdentifier("테이블명", tableName);
        if (schemaName == null || schemaName.isEmpty()) {
            return table;
        }
        return String.format("%s.%s", validateIdentifier("스키마명", schemaName), table);
    }

    /**
     * SQL에 직접 들어가는 식별자를 검증한다. 비인용 식별자({@code [A-Za-z][A-Za-z0-9_$#]*}) 또는
     * 큰따옴표로 감싼 인용 식별자만 허용한다.
     *
     * @param kind       식별자 종류 (에러 메시지용)
     * @param identifier 식별자
     * @return 검증된 식별자
     * @throws IllegalArgumentException 유효한 식별자가 아닌 경우
     */
    private String validateIdentifier(String kind, String identifier) {
        if (identifier == null || !(UNQUOTED_IDENTIFIER.matcher(identifier).matches() || QUOTED_IDENTIFIER.matcher(identifier).matches())) {
            throw new IllegalArgumentException(String.format("유효하지 않은 %s '%s'. 영문자로 시작하는 영숫자/_/$/# 또는 큰따옴표로 감싼 식별자만 허용합니다.", kind, identifier));
        }
        return identifier;
    }

    /**
     * INSERT SQL을 구성하기 위한 컬럼명 목록을 구성한다.
     *
     * @param recordSchema 스키마
     * @return 컬럼명 목록
     */
    private String getColumns(RecordSchema recordSchema) {
        List<RecordField> fields = recordSchema.getFields();
        List<String> columns = new ArrayList<>();
        for (RecordField field : fields) {
            columns.add(validateIdentifier("컬럼명", field.getFieldName()));
        }
        return String.join(", ", columns);
    }

    /**
     * INSERT SQL을 구성하기 위해서 VALUES로 구성할 바인딩 파라미터(?) 목록을 구성한다.
     *
     * @param recordSchema 스키마
     * @return 바인딩 파라미터 목록
     */
    private String getPlaceholders(RecordSchema recordSchema) {
        return String.join(", ", Collections.nCopies(recordSchema.getFieldCount(), "?"));
    }

    /**
     * 해당 컬럼의 값을 반환한다. 이 메소드는 NiFi Record를 구성하는 Field의 Data Type을 기반으로 동작한다.
     * Decimal인 경우 NiFi Record Schema에는 precision, scale이 정의되어 있지 않아서
     * Avro Schema를 통해서 Decimal의 precision, scale을 확인해야 한다.
     * Decimal은 Byte Array로 Avro File에 저장되어 있지만 이를 변환하려면 Big Decimal로 변환해야 하며
     * 이때 Decimal의 precision, scale이 필요하다. 이러한 이유로 이 프로세서에서는 Avro Schema를 지정하도록 되어 있다.
     *
     * 값은 PreparedStatement에 바인딩할 Java 객체로 반환하며 NULL은 그대로 null을 반환한다(SQL NULL로 바인딩).
     *
     * @param field  컬럼
     * @param schema Avro Schema
     * @param record 1개 ROW
     * @return 바인딩할 값 (NULL인 경우 null)
     */
    private Object getValue(RecordField field, Record record, Schema schema) {
        Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
        final Object rawValue = record.getValue(field.getFieldName());
        if (rawValue == null) {
            return null;
        }
        switch (field.getDataType().getFieldType()) {
            case BYTE:
                // NiFi의 BYTE는 단일 Byte 값이다. 일부 Reader는 Avro bytes를 ByteBuffer/byte[]로 넘기므로 바이트 배열로 처리한다.
                if (isByteArrayValue(rawValue)) {
                    return getBytesValue(field, rawValue, schema, decimalConversion);
                }
                return DataTypeUtils.toByte(rawValue, field.getFieldName());
            case ARRAY:
                // Avro bytes는 NiFi에서 ARRAY of BYTE(Object[] of Byte)로 표현된다.
                if (isByteArrayType(field)) {
                    return getBytesValue(field, rawValue, schema, decimalConversion);
                }
                return String.valueOf(rawValue);
            case DATE:
            case TIME:
            case TIMESTAMP:
                // 문자열로 넘기면 DB 세션의 NLS_DATE_FORMAT에 의존하므로 java.sql.Date/Time/Timestamp로 변환하여 바인딩한다.
                return DataTypeUtils.convertType(rawValue, field.getDataType(), field.getFieldName());
            case BIGINT:
                // BigInteger는 JDBC 표준 매핑이 없으므로 BigDecimal로 바인딩한다.
                return rawValue instanceof java.math.BigInteger ? new BigDecimal((java.math.BigInteger) rawValue) : rawValue;
            case DECIMAL:
            case FLOAT:
            case SHORT:
            case LONG:
            case INT:
            case DOUBLE:
            case BOOLEAN:
                return rawValue;
            case CHAR:
            case STRING:
            default:
                return String.valueOf(rawValue);
        }
    }

    /**
     * 바이트 배열 값(ByteBuffer, byte[], Byte의 Object[])인지 확인한다.
     *
     * @param value 값
     * @return 바이트 배열 값이면 true
     */
    private boolean isByteArrayValue(Object value) {
        return value instanceof ByteBuffer || value instanceof byte[] || value instanceof Object[];
    }

    /**
     * 필드가 ARRAY of BYTE(Avro bytes)인지 확인한다.
     *
     * @param field 컬럼
     * @return ARRAY of BYTE이면 true
     */
    private boolean isByteArrayType(RecordField field) {
        return field.getDataType() instanceof ArrayDataType
                && ((ArrayDataType) field.getDataType()).getElementType().getFieldType() == RecordFieldType.BYTE;
    }

    /**
     * 바이트 배열 값을 바인딩할 값으로 변환한다. Avro Schema에서 Decimal Logical Type이면 BigDecimal로,
     * 아니면 byte[]로 반환한다(setBytes로 바인딩).
     *
     * @param field             컬럼
     * @param rawValue          ByteBuffer, byte[] 또는 Byte의 Object[]
     * @param schema            Avro Schema
     * @param decimalConversion Decimal 변환기
     * @return BigDecimal 또는 byte[]
     */
    private Object getBytesValue(RecordField field, Object rawValue, Schema schema, Conversions.DecimalConversion decimalConversion) {
        final byte[] bytes = toBytes(field, rawValue);
        final LogicalType logicalType = getDecimalLogicalType(getAvroField(field, schema));
        if (logicalType != null) {
            return decimalConversion.fromBytes(ByteBuffer.wrap(bytes), schema, logicalType);
        }
        return bytes;
    }

    /**
     * ByteBuffer, byte[], Byte의 Object[]를 byte[]로 변환한다.
     * ByteBuffer는 backing array 전체가 아니라 position~limit 구간만 사용한다(array offset 고려).
     *
     * @param field    컬럼 (에러 메시지용)
     * @param rawValue 값
     * @return byte[]
     * @throws IllegalArgumentException 바이트로 변환할 수 없는 요소가 있는 경우
     */
    private byte[] toBytes(RecordField field, Object rawValue) {
        if (rawValue instanceof byte[]) {
            return (byte[]) rawValue;
        }
        if (rawValue instanceof ByteBuffer) {
            final ByteBuffer buffer = ((ByteBuffer) rawValue).duplicate();
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        if (rawValue instanceof Object[]) {
            final Object[] array = (Object[]) rawValue;
            final byte[] bytes = new byte[array.length];
            for (int i = 0; i < array.length; i++) {
                if (!(array[i] instanceof Number)) {
                    throw new IllegalArgumentException(String.format("컬럼 '%s'의 바이트 배열에 바이트가 아닌 요소(%s)가 있습니다.", field.getFieldName(), array[i]));
                }
                bytes[i] = ((Number) array[i]).byteValue();
            }
            return bytes;
        }
        throw new IllegalArgumentException(String.format("컬럼 '%s'의 값(%s)을 바이트 배열로 변환할 수 없습니다.", field.getFieldName(), rawValue.getClass().getName()));
    }

    /**
     * 바이트 배열 컬럼의 Decimal 여부를 판단하기 위해 Avro Schema에서 필드를 찾는다.
     *
     * @param field  컬럼
     * @param schema Avro Schema
     * @return Avro Schema 필드
     * @throws IllegalArgumentException Avro Schema에 해당 필드가 없는 경우
     */
    private Schema.Field getAvroField(RecordField field, Schema schema) {
        final Schema.Field f = schema.getField(field.getFieldName());
        if (f == null) {
            throw new IllegalArgumentException(String.format("Avro 스키마에 컬럼 '%s'이(가) 없습니다. 바이트 컬럼의 Decimal 여부를 확인하려면 Avro 스키마에 해당 필드를 정의해야 합니다.", field.getFieldName()));
        }
        return f;
    }

    /**
     * NULL 값을 바인딩하기 위한 JDBC SQL Type을 반환한다.
     *
     * @param field  컬럼
     * @param schema Avro Schema
     * @return {@link Types}
     */
    private int getSqlType(RecordField field, Schema schema) {
        switch (field.getDataType().getFieldType()) {
            case BYTE:
                final Schema.Field f = schema.getField(field.getFieldName());
                return f != null && getDecimalLogicalType(f) != null ? Types.DECIMAL : Types.TINYINT;
            case ARRAY:
                if (isByteArrayType(field)) {
                    return getDecimalLogicalType(getAvroField(field, schema)) != null ? Types.DECIMAL : Types.VARBINARY;
                }
                return Types.VARCHAR;
            case DATE:
                return Types.DATE;
            case TIME:
                return Types.TIME;
            case TIMESTAMP:
                return Types.TIMESTAMP;
            case BIGINT:
            case DECIMAL:
                return Types.DECIMAL;
            case FLOAT:
                return Types.REAL;
            case DOUBLE:
                return Types.DOUBLE;
            case SHORT:
                return Types.SMALLINT;
            case LONG:
                return Types.BIGINT;
            case INT:
                return Types.INTEGER;
            case BOOLEAN:
                return Types.BOOLEAN;
            case CHAR:
            case STRING:
            default:
                return Types.VARCHAR;
        }
    }

    /**
     * 지정한 컬럼이 Decimal Type인 경우 Decimal Type을 처리한다.
     * Avro Schema에서 Null을 포함하는 경우 Null이 아닌 Decimal Logical Type에 대해서 반환한다.
     *
     * @param field 컬럼을 구성하는 필드
     * @return Decimal Logical Type (Decimal이 아는 경우 NULL)
     */
    private LogicalType getDecimalLogicalType(Schema.Field field) {
        Schema s = field.schema();
        if (s.getLogicalType() != null && "decimal".equals(s.getLogicalType().getName())) {
            return s.getLogicalType();
        } else {
            // getTypes()는 UNION이 아니면 예외를 던지므로 UNION인 경우에만 확인한다.
            if (s.getType() == Schema.Type.UNION) {
                List<Schema> types = s.getTypes();
                for (Schema type : types) {
                    if (type.getLogicalType() != null && "decimal".equals(type.getLogicalType().getName())) {
                        return type.getLogicalType();
                    }
                }
            }
        }
        return null;
    }

    /**
     * JDBC Database Metadata에서 JDBC URL을 반환한다.
     *
     * @param connection JDBC Connection
     * @return JDBC URL
     */
    private String getJdbcUrl(final Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData != null) {
                return databaseMetaData.getURL();
            }
        } catch (final Exception e) {
            getLogger().warn("JDBC Driver를 통해서 URL 확인 실패.", e);
        }

        return "DBCPService";
    }
}