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
import org.apache.avro.LogicalTypes;
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
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT;


/**
 * Oracle DB에 FlowFile의 Content를 포함하는 데이터를 Bulk Insert를 수행하는 Processor.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
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
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
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
            .description("데이터를 넣을 테이블명")
            .required(true)
            .dependsOn(SCHEMA_NAME)
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
            final Throwable toAnalyze = (e instanceof BatchUpdateException) ? e.getCause() : e;
            if (toAnalyze instanceof SQLTransientException) {
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
            } else {
                flowFile = session.putAttribute(flowFile, BULK_INSERT_ERROR, e.getMessage());
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
        try (final InputStream in = session.read(flowFile)) {
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
            final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
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

        final String avroSchemaJson = context.getProperty(AVRO_SCHEMA).evaluateAttributeExpressions().getValue();
        Schema schema = new Schema.Parser().parse(avroSchemaJson);

        ///////////////////////////////////////////////////////////////////////////////////
        // FlowFile Content의 데이터를 처리하는 Record Reader가 해당 데이터의 Schema를 확인한다.
        ///////////////////////////////////////////////////////////////////////////////////

        final RecordSchema recordSchema = recordReader.getSchema();

        /////////////////////////////////////////
        // SQL을 실행한다.
        /////////////////////////////////////////

        try (final Statement statement = connection.createStatement()) {
            final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            try {
                statement.setQueryTimeout(timeoutMillis); // 초단위 타임아웃
            } catch (SQLException se) {
                // 만약에 JDBC Driver가 Query Timeout을 지우너하지 않으면 무한으로 가정한다. 이 경우 0만 허용한다.
                if (timeoutMillis > 0) {
                    throw se;
                }
            }

            int count = 0;
            Record currentRecord;
            List<Record> records = new ArrayList<>();
            while ((currentRecord = recordReader.nextRecord()) != null) {
                if (count >= maxBulkRowCount) { // 한번에 처리하려는 ROW Count 만큼만 처리
                    String sql = generateInsertQuery(schemaName, tableName, recordSchema, records, schema);
                    statement.execute(sql);
                    records.clear();
                } else {
                    // Max Bulk Row Count에 도달하지 않으면 무조건 데이터를 추가
                    records.add(currentRecord);
                    count++;
                }
            }

            // Max Bulk Row Count에 도달하지 않은 남은 데이터를 처리
            if (count > 0) {
                String sql = generateInsertQuery(schemaName, tableName, recordSchema, records, schema);
                this.getLogger().info("Bulk Insert SQl : \n{}", sql);
                statement.execute(sql);
                records.clear();
            }
        }
    }

    /**
     * Oracle INSERT SQL 문을 생성한다.
     *
     * @param schemaName   스키마명
     * @param tableName    테이블명
     * @param recordSchema 레코드 스키마
     * @param records      데이터
     * @param schema       Avro Schema
     * @return SQL
     */
    private String generateInsertQuery(String schemaName, String tableName, RecordSchema recordSchema, List<Record> records, Schema schema) {
        String sql = "INSERT ALL" + "\n" +
                _generateInsertQuery(schemaName, tableName, recordSchema, records, schema) + "\n" +
                "SELECT 1 FROM dual;";
        return sql;
    }

    /**
     * Insert All의 Sub Insert를 생성한다.
     *
     * @param schemaName   스키마명
     * @param tableName    테이블명
     * @param recordSchema 레코드 스키마
     * @param records      N개 ROW
     * @param schema       Avro Schema
     * @return SQL
     */
    private String _generateInsertQuery(String schemaName, String tableName, RecordSchema recordSchema, List<Record> records, Schema schema) {
        List<String> inserts = new ArrayList<>();
        for (Record record : records) {
            inserts.add(getInsertSQL(schemaName, tableName, recordSchema, record, schema));
        }
        return String.join("\n", inserts);
    }

    /**
     * Insert All의 Sub Insert를 생성한다.
     *
     * @param schemaName   스키마명
     * @param tableName    테이블명
     * @param recordSchema 레코드 스키마
     * @param record       1개 ROW
     * @param schema       Avro Schema
     * @return SQL
     */
    private String getInsertSQL(String schemaName, String tableName, RecordSchema recordSchema, Record record, Schema schema) {
        return String.format("INTO %s ( %s ) VALUES ( %s )", getTableName(schemaName, tableName), getColumns(recordSchema), getValues(recordSchema, record, schema));
    }

    /**
     * 테이블명을 구성한다.
     *
     * @param schemaName 스키마명
     * @param tableName  테이블명
     * @return "스키마명.테이블명"
     */
    private String getTableName(String schemaName, String tableName) {
        return String.format("%s.%s", schemaName, tableName);
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
            columns.add(field.getFieldName());
        }
        return String.join(", ", columns);
    }

    /**
     * INSERT SQL을 구성하기 위해서 VALUES로 구성할 값 목록을 구성한다.
     *
     * @param recordSchema 스키마
     * @param record       1개 ROW
     * @param schema       Avro Schema
     * @return 값 목록
     */
    private String getValues(RecordSchema recordSchema, Record record, Schema schema) {
        List<String> values = new ArrayList<>();
        List<RecordField> fields = recordSchema.getFields();
        for (RecordField field : fields) {
            String value = getValue(field, record, schema);
            values.add("null".equals(value) ? "'NULL'" : value); // ORACLE NULL 처리
        }
        return String.join(", ", values);
    }

    /**
     * 해당 컬럼의 값을 반환한다. 이 메소드는 NiFi Record를 구성하는 Field의 Data Type을 기반으로 동작한다.
     * Decimal인 경우 NiFi Record Schema에는 precision, scale이 정의되어 있지 않아서
     * Avro Schema를 통해서 Decimal의 precision, scale을 확인해야 한다.
     * Decimal은 Byte Array로 Avro File에 저장되어 있지만 이를 변환하려면 Big Decimal로 변환해야 하며
     * 이때 Decimal의 precision, scale이 필요하다. 이러한 이유로 이 프로세서에서는 Avro Schema를 지정하도록 되어 있다.
     *
     * @param field  컬럼
     * @param schema Avro Schema
     * @param record 1개 ROW
     * @return 값
     */
    private String getValue(RecordField field, Record record, Schema schema) {
        Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
        switch (field.getDataType().getFieldType()) {
            case BYTE:
                ByteBuffer value = (ByteBuffer) record.getValue(field.getFieldName());
                Schema.Field f = schema.getField(field.getFieldName());
                LogicalType logicalType = getDecimalLogicalType(f);
                if (logicalType != null) {
                    LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                    BigDecimal bigDecimal = decimalConversion.fromBytes(value, schema, decimalType);
                    return bigDecimal.toString();
                } else {
                    return new String(value.array()); // TODO BYTE인 경우 요건에 맞춰서 처리해야 한다.
                }
            case DATE:
            case TIME:
            case TIMESTAMP:
                return String.format("'%s'", String.valueOf(record.getValue(field.getFieldName())));
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case SHORT:
            case LONG:
            case INT:
            case DOUBLE:
            case BOOLEAN:
                return String.valueOf(record.getValue(field.getFieldName()));
            case CHAR:
            case STRING:
            default:
                return String.format("'%s'", record.getValue(field.getFieldName()));
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
            if (s.getTypes() != null && s.getTypes().size() > 0) {
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