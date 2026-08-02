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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/PutDatabaseRecord.java
 */
package io.datadynamics.nifi.processors.db.putdatabaserecord;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.nifi.expression.ExpressionLanguageScope.*;

/**
 * FlowFile로부터 하나 이상의 레코드를 읽어 관계형 데이터베이스에 INSERT/UPDATE/DELETE/UPSERT/INSERT_IGNORE 또는
 * 임의의 SQL 문으로 반영하는 프로세서.
 * <p>
 * 처리 흐름 개요:
 * 1) Record Reader 컨트롤러 서비스로 FlowFile 본문을 파싱하여 레코드 스키마를 얻는다.
 * 2) Statement Type 프로퍼티(또는 속성/RecordPath)로 결정된 SQL 문 종류에 따라 레코드 스키마와
 *    대상 테이블의 실제 컬럼 메타데이터(TableSchema)를 비교하여 SQL을 동적으로 생성한다.
 * 3) 생성된 PreparedStatement에 레코드 값을 바인딩하고 배치(batch)로 묶어 실행한 뒤 트랜잭션을 커밋한다.
 * 4) 오류 발생 시 재시도 가능 여부(SQLTransientException 여부)에 따라 retry 또는 failure 관계로 라우팅하고,
 *    필요 시 JDBC 트랜잭션을 롤백한다.
 * <p>
 * Statement Type이 UPDATE인 경우, 입력 레코드는 기본 키(또는 사용자가 지정한 Update Keys)의 값을
 * 변경해서는 안 된다. 이를 위반하면 UPDATE 문이 아무 행도 갱신하지 못하거나, 의도치 않게 다른
 * 기존 레코드를 덮어써 데이터가 손상될 수 있다.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"dd", "custom", "sql", "record", "jdbc", "put", "database", "update", "insert", "delete"})
@CapabilityDescription("PutDatabaseRecord 프로세서는 지정된 RecordReader를 사용하여 입력 FlowFile로부터 (여러 개일 수 있는) 레코드를 읽어들입니다. "
        + "이 레코드들은 SQL 문으로 변환되어 하나의 트랜잭션으로 실행됩니다. 오류가 발생하면 FlowFile은 failure 또는 retry로 라우팅되며, "
        + "레코드가 성공적으로 전송되면 "
        + "입력 FlowFile은 "
        + "success로 라우팅됩니다. 프로세서가 실행하는 SQL 문의 종류는 Statement Type 프로퍼티로 지정하며, INSERT/UPDATE/DELETE와 같이 고정된 값이나, "
        + "FlowFile 속성으로부터 Statement Type을 가져오도록 하는 'Use statement.type Attribute'를 사용할 수 있습니다. 중요: Statement Type이 UPDATE인 경우, "
        + "입력 레코드는 기본 키(또는 사용자 지정 Update Keys)의 값을 변경해서는 안 됩니다. 이러한 레코드가 발견되면 데이터베이스로 전송되는 UPDATE 문이 아무 것도 "
        + "수행하지 않거나(새로운 기본 키 값을 가진 기존 레코드가 없는 경우), 의도치 않게 기존 데이터를 손상시킬 수 있습니다(기본 키의 새로운 값이 이미 존재하는 "
        + "레코드를 변경하게 되는 경우).")
@ReadsAttribute(attribute = PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, description = "Statement Type 프로퍼티에서 'Use statement.type Attribute'가 선택된 경우, "
        + "이 속성 값을 사용하여 생성 및 실행할 SQL 문의 종류(INSERT, UPDATE, DELETE, SQL 등)를 결정합니다.")
@WritesAttribute(attribute = PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR, description = "처리 중 오류가 발생하면 FlowFile은 failure 또는 retry로 라우팅되며, "
        + "이 속성에는 오류의 원인이 기록됩니다.")
public class PutDatabaseRecord extends AbstractProcessor {

    public static final String UPDATE_TYPE = "UPDATE";
    public static final String INSERT_TYPE = "INSERT";
    public static final String DELETE_TYPE = "DELETE";
    public static final String UPSERT_TYPE = "UPSERT";
    public static final String INSERT_IGNORE_TYPE = "INSERT_IGNORE";
    public static final String SQL_TYPE = "SQL";   // Not an allowable value in the Statement Type property, must be set by attribute
    public static final String USE_ATTR_TYPE = "Use statement.type Attribute";
    public static final String USE_RECORD_PATH = "Use Record Path";
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("SQL 질의 결과 집합으로부터 FlowFile을 성공적으로 생성했습니다.")
            .build();
    protected static final Map<String, DatabaseAdapter> dbAdapters;
    static final String STATEMENT_TYPE_ATTRIBUTE = "statement.type";
    static final String PUT_DATABASE_RECORD_ERROR = "putdatabaserecord.error";
    static final AllowableValue IGNORE_UNMATCHED_FIELD = new AllowableValue("Ignore Unmatched Fields", "Ignore Unmatched Fields",
            "문서 내에서 데이터베이스 컬럼에 매핑되지 않는 필드는 무시합니다");
    static final AllowableValue FAIL_UNMATCHED_FIELD = new AllowableValue("Fail on Unmatched Fields", "Fail on Unmatched Fields",
            "문서에 데이터베이스 컬럼으로 매핑할 수 없는 필드가 있으면 FlowFile은 failure 관계로 라우팅됩니다");
    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore Unmatched Columns",
            "Ignore Unmatched Columns",
            "문서에 대응하는 필드가 없는 데이터베이스 컬럼은 필수가 아닌 것으로 간주합니다. 별도의 알림은 기록되지 않습니다");
    static final AllowableValue WARNING_UNMATCHED_COLUMN = new AllowableValue("Warn on Unmatched Columns",
            "Warn on Unmatched Columns",
            "문서에 대응하는 필드가 없는 데이터베이스 컬럼은 필수가 아닌 것으로 간주합니다. 경고가 로그로 기록됩니다");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail on Unmatched Columns",
            "Fail on Unmatched Columns",
            "문서에 대응하는 필드가 없는 데이터베이스 컬럼이 하나라도 있으면 플로우가 실패합니다. 오류가 로그로 기록됩니다");
    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("데이터베이스를 갱신할 수 없었지만 동일한 작업을 재시도하면 성공할 가능성이 있는 경우 FlowFile이 이 관계로 라우팅됩니다")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("데이터베이스를 갱신할 수 없고 재시도해도 실패할 것으로 예상되는 경우(예: 잘못된 질의나 무결성 제약 조건 위반) "
                    + "FlowFile이 이 관계로 라우팅됩니다")
            .build();
    // Properties
    static final PropertyDescriptor RECORD_READER_FACTORY = new Builder()
            .name("put-db-record-record-reader")
            .displayName("레코드 리더")
            .description("입력 데이터를 파싱하고 데이터의 스키마를 결정하는 데 사용할 컨트롤러 서비스를 지정합니다.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE = new Builder()
            .name("put-db-record-statement-type")
            .displayName("SQL 문 유형(Statement Type)")
            .description("생성할 SQL 문의 종류를 지정합니다. "
                    + "각 연산의 동작 방식에 대한 설명은 데이터베이스 문서를 참고하십시오. "
                    + "일부 데이터베이스 종류는 특정 Statement Type을 지원하지 않을 수 있음에 유의하십시오. "
                    + "'Use statement.type Attribute'를 선택하면 값은 FlowFile의 statement.type 속성에서 "
                    + "가져옵니다. 'SQL' Statement Type을 사용할 수 있는 것은 'Use statement.type Attribute' 옵션뿐입니다. 'SQL'을 지정한 경우, "
                    + "'Field Containing SQL' 프로퍼티로 지정한 필드의 값은 대상 데이터베이스에서 유효한 SQL 문이어야 하며, 그대로 실행됩니다.")
            .required(true)
            .allowableValues(UPDATE_TYPE, INSERT_TYPE, UPSERT_TYPE, INSERT_IGNORE_TYPE, DELETE_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE_RECORD_PATH = new Builder()
            .name("Statement Type Record Path")
            .displayName("SQL 문 유형 Record Path")
            .description("Statement Type을 결정하기 위해 각 레코드에 대해 평가할 RecordPath를 지정합니다. RecordPath의 평가 결과는 INSERT, UPDATE, UPSERT, DELETE 중 하나와 일치해야 합니다.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(NONE)
            .dependsOn(STATEMENT_TYPE, USE_RECORD_PATH)
            .build();
    static final PropertyDescriptor UPDATE_KEYS = new Builder()
            .name("put-db-record-update-keys")
            .displayName("Update Keys(갱신 기준 컬럼)")
            .description("UPDATE 문에서 데이터베이스의 한 행을 고유하게 식별하는 컬럼 이름의 콤마(,) 구분 목록입니다. "
                    + "Statement Type이 UPDATE이고 이 프로퍼티를 설정하지 않으면 테이블의 기본 키(Primary Key)가 사용됩니다. "
                    + "이 경우 기본 키가 존재하지 않고 Unmatched Column Behaviour가 FAIL로 설정되어 있으면 SQL 변환이 실패합니다. "
                    + "Statement Type이 INSERT인 경우 이 프로퍼티는 무시됩니다"
            )
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, UPDATE_TYPE, UPSERT_TYPE, SQL_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();
    static final PropertyDescriptor FIELD_CONTAINING_SQL = new Builder()
            .name("put-db-record-field-containing-sql")
            .displayName("SQL이 포함된 필드")
            .description("(statement.type 속성으로 설정된) Statement Type이 'SQL'인 경우, 이 필드는 레코드 내에서 실행할 SQL 문을 담고 있는 필드가 무엇인지 나타냅니다. "
                    + "해당 필드의 값은 단일 SQL 문이어야 합니다. Statement Type이 'SQL'이 아니면 이 필드는 무시됩니다.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();
    static final PropertyDescriptor ALLOW_MULTIPLE_STATEMENTS = new Builder()
            .name("put-db-record-allow-multiple-statements")
            .displayName("여러 SQL 문 허용")
            .description("(statement.type 속성으로 설정된) Statement Type이 'SQL'인 경우, 이 필드는 필드 값을 세미콜론(;)으로 분리하여 각 문을 개별적으로 "
                    + "실행할지 여부를 나타냅니다. 어느 한 문이라도 오류를 일으키면 전체 문 집합이 롤백됩니다. Statement Type이 'SQL'이 아니면 이 필드는 무시됩니다.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(STATEMENT_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();
    static final PropertyDescriptor DATA_RECORD_PATH = new Builder()
            .name("Data Record Path")
            .displayName("데이터 Record Path")
            .description("지정된 경우, 이 프로퍼티는 입력되는 각 레코드에 대해 평가할 RecordPath를 나타내며, 이 RecordPath를 평가한 결과 레코드가 " +
                    "전체 입력 레코드 대신 데이터베이스로 전송됩니다. 지정하지 않으면 입력 레코드 전체가 데이터베이스에 반영됩니다.")
            .required(false)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(NONE)
            .build();
    static final PropertyDescriptor DBCP_SERVICE = new Builder()
            .name("put-db-record-dcbp-service")
            .displayName("데이터베이스 커넥션 풀 서비스")
            .description("레코드를 전송하기 위해 데이터베이스 연결을 획득하는 데 사용되는 컨트롤러 서비스입니다.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    static final PropertyDescriptor CATALOG_NAME = new Builder()
            .name("put-db-record-catalog-name")
            .displayName("Catalog 이름")
            .description("SQL 문이 대상으로 하는 카탈로그의 이름입니다. 갱신 대상 데이터베이스에는 적용되지 않을 수 있으며, 이 경우 필드를 비워 두십시오. "
                    + "이 프로퍼티를 설정하고 데이터베이스가 대소문자를 구분하는 경우, 카탈로그 이름은 데이터베이스의 카탈로그 이름과 정확히 일치해야 합니다.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SCHEMA_NAME = new Builder()
            .name("put-db-record-schema-name")
            .displayName("Schema 이름")
            .description("테이블이 속한 스키마의 이름입니다. 갱신 대상 데이터베이스에는 적용되지 않을 수 있으며, 이 경우 필드를 비워 두십시오. "
                    + "이 프로퍼티를 설정하고 데이터베이스가 대소문자를 구분하는 경우, 스키마 이름은 데이터베이스의 스키마 이름과 정확히 일치해야 합니다.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE_NAME = new Builder()
            .name("put-db-record-table-name")
            .displayName("Table 이름")
            .description("SQL 문이 대상으로 하는 테이블의 이름입니다. 데이터베이스가 대소문자를 구분하는 경우, 테이블 이름은 데이터베이스의 테이블 이름과 정확히 일치해야 합니다.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new Builder()
            .name("put-db-record-translate-field-names")
            .displayName("필드 이름 변환")
            .description("true로 설정하면 프로세서는 필드 이름을 지정된 테이블의 적절한 컬럼 이름으로 변환하려고 시도합니다. "
                    + "false로 설정하면 필드 이름이 컬럼 이름과 정확히 일치해야 하며, 그렇지 않으면 해당 컬럼은 갱신되지 않습니다")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new Builder()
            .name("put-db-record-unmatched-field-behavior")
            .displayName("매핑되지 않은 필드 처리 방식")
            .description("입력 레코드에 데이터베이스 테이블의 어떤 컬럼에도 매핑되지 않는 필드가 있는 경우, 이 프로퍼티는 해당 상황을 어떻게 처리할지 지정합니다")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();
    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new Builder()
            .name("put-db-record-unmatched-column-behavior")
            .displayName("매핑되지 않은 컬럼 처리 방식")
            .description("입력 레코드에 데이터베이스 테이블의 모든 컬럼에 대응하는 필드 매핑이 없는 경우, 이 프로퍼티는 해당 상황을 어떻게 처리할지 지정합니다")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();
    static final PropertyDescriptor QUOTE_IDENTIFIERS = new Builder()
            .name("put-db-record-quoted-identifiers")
            .displayName("컬럼 식별자 인용(Quote)")
            .description("이 옵션을 활성화하면 모든 컬럼 이름을 따옴표로 감싸므로, 테이블의 컬럼 이름으로 예약어를 사용할 수 있게 됩니다.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTE_TABLE_IDENTIFIER = new Builder()
            .name("put-db-record-quoted-table-identifiers")
            .displayName("테이블 식별자 인용(Quote)")
            .description("이 옵션을 활성화하면 테이블 이름이 따옴표로 감싸져, 테이블 이름에 특수 문자를 사용할 수 있게 됩니다.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new Builder()
            .name("put-db-record-query-timeout")
            .displayName("최대 대기 시간")
            .description("실행 중인 SQL 문에 허용되는 최대 시간이며 "
                    + ", 0이면 제한이 없음을 의미합니다. 1초 미만의 시간은 0으로 취급됩니다.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ENVIRONMENT)
            .build();

    static final PropertyDescriptor TABLE_SCHEMA_CACHE_SIZE = new Builder()
            .name("table-schema-cache-size")
            .displayName("Table Schema 캐시 크기")
            .description("캐시할 테이블 스키마의 최대 개수를 지정합니다")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new Builder()
            .name("put-db-record-max-batch-size")
            .displayName("최대 배치 크기")
            .description("각 배치에 포함할 최대 SQL 문 개수를 지정합니다. 0이면 배치 크기에 제한이 없으며, "
                    + "이 경우 문의 개수가 많으면 메모리 사용량 문제가 발생할 수 있습니다.")
            .defaultValue("1000")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor DB_TYPE;
    protected static Set<Relationship> relationships;
    protected static List<PropertyDescriptor> propDescriptors;

    static {
        dbAdapters = new HashMap<>();
        ArrayList<AllowableValue> dbAdapterValues = new ArrayList<>();

        ServiceLoader<DatabaseAdapter> dbAdapterLoader = ServiceLoader.load(DatabaseAdapter.class);
        dbAdapterLoader.forEach(databaseAdapter -> {
            dbAdapters.put(databaseAdapter.getName(), databaseAdapter);
            dbAdapterValues.add(new AllowableValue(databaseAdapter.getName(), databaseAdapter.getName(), databaseAdapter.getDescription()));
        });

        DB_TYPE = new Builder()
                .name("db-type")
                .displayName("데이터베이스 종류")
                .description("데이터베이스별 코드를 생성하는 데 사용되는 데이터베이스의 종류/유형입니다. 대부분의 경우 Generic 유형으로 "
                        + "충분하지만, 일부 데이터베이스(예: Oracle)는 별도의 SQL 절이 필요합니다. ")
                .allowableValues(dbAdapterValues.toArray(new AllowableValue[0]))
                .defaultValue("Generic")
                .required(false)
                .build();

        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        r.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(RECORD_READER_FACTORY);
        pds.add(DB_TYPE);
        pds.add(STATEMENT_TYPE);
        pds.add(STATEMENT_TYPE_RECORD_PATH);
        pds.add(DATA_RECORD_PATH);
        pds.add(DBCP_SERVICE);
        pds.add(CATALOG_NAME);
        pds.add(SCHEMA_NAME);
        pds.add(TABLE_NAME);
        pds.add(TRANSLATE_FIELD_NAMES);
        pds.add(UNMATCHED_FIELD_BEHAVIOR);
        pds.add(UNMATCHED_COLUMN_BEHAVIOR);
        pds.add(UPDATE_KEYS);
        pds.add(FIELD_CONTAINING_SQL);
        pds.add(ALLOW_MULTIPLE_STATEMENTS);
        pds.add(QUOTE_IDENTIFIERS);
        pds.add(QUOTE_TABLE_IDENTIFIER);
        pds.add(QUERY_TIMEOUT);
        pds.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        pds.add(TABLE_SCHEMA_CACHE_SIZE);
        pds.add(MAX_BATCH_SIZE);

        propDescriptors = Collections.unmodifiableList(pds);
    }

    // (catalog, schema, table) 단위로 조회한 TableSchema(실제 컬럼 메타데이터)를 캐시하여
    // 매 FlowFile 처리마다 JDBC DatabaseMetaData를 다시 조회하지 않도록 한다.
    private Cache<SchemaKey, TableSchema> schemaCache;
    // onScheduled 시점에 DB_TYPE 프로퍼티 값으로 결정되는, 데이터베이스별 SQL 방언 어댑터(UPSERT 구문 등 처리)
    private DatabaseAdapter databaseAdapter;
    // Statement Type이 'Use Record Path'일 때, 레코드별로 실제 Statement Type(INSERT/UPDATE/...)을
    // 결정하기 위해 RecordPath를 평가하는 함수. Use Record Path가 아니면 null
    private volatile Function<Record, String> recordPathOperationType;
    // Data Record Path 프로퍼티가 지정된 경우, 입력 레코드에서 실제로 DB에 반영할 하위 레코드를
    // 추출하기 위해 미리 컴파일해 둔 RecordPath. 지정되지 않으면 null(레코드 전체를 사용)
    private volatile RecordPath dataRecordPath;


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    // TODO 이 프로세서는 동적 프로퍼티를 사용하지 않으므로 다음 주요 버전에서 제거할 것
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    /**
     * 선택된 Database Type(어댑터)이 요청된 Statement Type(UPSERT/INSERT_IGNORE)을 실제로 지원하는지
     * 검증한다. 어댑터가 해당 기능을 지원하지 않으면(예: 일부 데이터베이스는 UPSERT 문법이 없음)
     * 프로세서를 유효하지 않은 상태로 표시하여 스케줄링 전에 사용자가 인지하도록 한다.
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        DatabaseAdapter databaseAdapter = dbAdapters.get(validationContext.getProperty(DB_TYPE).getValue());
        String statementType = validationContext.getProperty(STATEMENT_TYPE).getValue();
        if ((UPSERT_TYPE.equals(statementType) && !databaseAdapter.supportsUpsert())
                || (INSERT_IGNORE_TYPE.equals(statementType) && !databaseAdapter.supportsInsertIgnore())) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(STATEMENT_TYPE.getDisplayName())
                    .valid(false)
                    .explanation(databaseAdapter.getName() + " does not support " + statementType)
                    .build()
            );
        }

        return validationResults;
    }

    /**
     * 프로세서가 스케줄링될 때 한 번 호출되어, 매 FlowFile 처리마다 반복하면 비용이 큰 초기화를 수행한다.
     * - Database Type 프로퍼티 값에 해당하는 DatabaseAdapter를 조회하여 필드에 저장
     * - 프로퍼티로 지정된 크기만큼의 TableSchema 캐시(schemaCache)를 새로 생성
     * - Statement Type이 'Use Record Path'이면 RecordPath를 컴파일하여 레코드별 Statement Type 결정 함수 준비
     * - Data Record Path가 지정되어 있으면 이를 컴파일해 둔다
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        databaseAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());

        final int tableSchemaCacheSize = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
        schemaCache = Caffeine.newBuilder()
                .maximumSize(tableSchemaCacheSize)
                .build();

        if (USE_RECORD_PATH.equals(context.getProperty(STATEMENT_TYPE).getValue())) {
            final String statementTypeRecordPathValue = context.getProperty(STATEMENT_TYPE_RECORD_PATH).getValue();
            final RecordPath recordPath = RecordPath.compile(statementTypeRecordPathValue);
            recordPathOperationType = new RecordPathStatementType(recordPath);
        } else {
            recordPathOperationType = null;
        }

        final String dataRecordPathValue = context.getProperty(DATA_RECORD_PATH).getValue();
        dataRecordPath = dataRecordPathValue == null ? null : RecordPath.compile(dataRecordPathValue);
    }

    /**
     * 프로세서의 핵심 실행 메서드. 큐에서 FlowFile을 하나 꺼내 JDBC 커넥션을 획득한 뒤,
     * 자동 커밋을 끄고 putToDatabase()로 실제 SQL 생성/실행을 위임한다.
     * 성공하면 트랜잭션을 커밋하고 success로 라우팅하며, 실패하면 예외의 종류(SQLTransientException 여부)에 따라
     * retry(일시적 오류, 재시도 가능) 또는 failure(재시도해도 실패할 오류)로 라우팅하고 JDBC 트랜잭션을 롤백한다.
     * RollbackOnFailure 프로퍼티가 활성화되어 있으면 세션 자체도 롤백하여 FlowFile을 큐에 남겨둔다.
     * finally 블록에서는 커넥션의 auto-commit 설정을 원래 상태로 되돌리고 커넥션을 반드시 닫는다.
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        Optional<Connection> connectionHolder = Optional.empty();

        boolean originalAutoCommit = false;
        try {
            final Connection connection = dbcpService.getConnection(flowFile.getAttributes());
            connectionHolder = Optional.of(connection);

            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            putToDatabase(context, session, flowFile, connection);
            connection.commit();

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, getJdbcUrl(connection));
        } catch (final Exception e) {
            // 예외가 발생하면, 동일한 요청을 다시 시도했을 때 성공할 가능성이 있는 경우 'retry'로 라우팅한다.
            // 그렇지 않으면 'failure'로 라우팅한다. SQLTransientException은 재시도가 성공할 수 있음을 나타내는 특정 타입이다.
            final Relationship relationship;
            final Throwable toAnalyze = (e instanceof BatchUpdateException) ? e.getCause() : e;
            if (toAnalyze instanceof SQLTransientException) {
                relationship = REL_RETRY;
                flowFile = session.penalize(flowFile);
            } else {
                relationship = REL_FAILURE;
            }

            getLogger().error("Failed to put Records to database for {}. Routing to {}.", flowFile, relationship, e);

            final boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
            if (rollbackOnFailure) {
                session.rollback();
            } else {
                flowFile = session.putAttribute(flowFile, PUT_DATABASE_RECORD_ERROR, e.getMessage());
                session.transfer(flowFile, relationship);
            }

            connectionHolder.ifPresent(connection -> {
                try {
                    connection.rollback();
                } catch (final Exception rollbackException) {
                    getLogger().error("Failed to rollback JDBC transaction", rollbackException);
                }
            });
        } finally {
            if (originalAutoCommit) {
                connectionHolder.ifPresent(connection -> {
                    try {
                        connection.setAutoCommit(true);
                    } catch (final Exception autoCommitException) {
                        getLogger().warn("Failed to set auto-commit back to true on connection", autoCommitException);
                    }
                });
            }

            connectionHolder.ifPresent(connection -> {
                try {
                    connection.close();
                } catch (final Exception closeException) {
                    getLogger().warn("Failed to close database connection", closeException);
                }
            });
        }
    }


    /**
     * Statement Type이 'SQL'인 경우 사용되는 실행 경로. 레코드를 SQL로 변환하는 대신, 레코드의 특정 필드에
     * 이미 담겨 있는 SQL 문 문자열을 그대로(as-is) 실행한다. Allow Multiple SQL Statements가 true이면
     * 세미콜론(이스케이프되지 않은 ';') 기준으로 문을 분리하여 각각 실행한다.
     */
    private void executeSQL(final ProcessContext context, final FlowFile flowFile, final Connection connection, final RecordReader recordReader)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final RecordSchema recordSchema = recordReader.getSchema();

        // SQL 문이 담긴 필드가 어느 것인지 찾는다
        final String sqlField = context.getProperty(FIELD_CONTAINING_SQL).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(sqlField)) {
            throw new IllegalArgumentException(format("SQL specified as Statement Type but no Field Containing SQL was found, FlowFile %s", flowFile));
        }

        boolean schemaHasSqlField = recordSchema.getFields().stream().anyMatch((field) -> sqlField.equals(field.getFieldName()));
        if (!schemaHasSqlField) {
            throw new IllegalArgumentException(format("Record schema does not contain Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
        }

        try (final Statement statement = connection.createStatement()) {
            final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            try {
                statement.setQueryTimeout(timeoutMillis); // timeout in seconds
            } catch (SQLException se) {
                // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                if (timeoutMillis > 0) {
                    throw se;
                }
            }

            Record currentRecord;
            while ((currentRecord = recordReader.nextRecord()) != null) {
                final String sql = currentRecord.getAsString(sqlField);
                if (sql == null || StringUtils.isEmpty(sql)) {
                    throw new MalformedRecordException(format("Record had no (or null) value for Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
                }

                // Execute the statement(s) as-is
                if (context.getProperty(ALLOW_MULTIPLE_STATEMENTS).asBoolean()) {
                    final String regex = "(?<!\\\\);";
                    final String[] sqlStatements = (sql).split(regex);
                    for (String sqlStatement : sqlStatements) {
                        statement.execute(sqlStatement);
                    }
                } else {
                    statement.execute(sql);
                }
            }
        }
    }


    /**
     * INSERT/UPDATE/DELETE/UPSERT/INSERT_IGNORE(또는 Use Record Path로 레코드별로 결정된 종류)의 DML을
     * 실제로 생성하고 실행하는 핵심 메서드.
     * <p>
     * 처리 절차:
     * 1) catalog/schema/table 이름과 Statement Type을 확인하고, schemaCache에서 TableSchema(실제 DB 컬럼 메타데이터)를 얻는다
     *    (캐시에 없으면 JDBC DatabaseMetaData를 조회하여 채운다).
     * 2) Statement Type별로 SQL 문자열을 최초 1회 생성(generateInsert/Update/Delete/Upsert/InsertIgnore)하고
     *    PreparedStatement로 준비하여 statementType별 캐시(preparedSql)에 저장해 재사용한다.
     * 3) 레코드마다 필드 값을 대상 컬럼의 SQL 타입에 맞게 변환(DataTypeUtils)한 뒤 PreparedStatement에 바인딩하고
     *    addBatch()로 배치에 추가한다. Statement Type이 도중에 바뀌거나 배치 크기가 Max Batch Size에 도달하면
     *    그 시점까지의 배치를 executeBatch()로 실행한다.
     * 4) 모든 레코드 처리가 끝나면 남은 배치를 마저 실행하고, finally에서 모든 PreparedStatement를 닫는다.
     */
    private void executeDML(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                            final Connection con, final RecordReader recordReader, final String explicitStatementType, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final ComponentLog log = getLogger();

        final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String configuredStatementType = context.getProperty(STATEMENT_TYPE).getValue();
        final String updateKeys = switch (configuredStatementType) {
            case UPDATE_TYPE, UPSERT_TYPE, SQL_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH ->
                    context.getProperty(UPDATE_KEYS).evaluateAttributeExpressions(flowFile).getValue();
            default -> null;
        };
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
        final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        // 테이블 이름이 설정되어 있는지 확인한다. 생성될 SQL 문과 TableSchema 캐시 키 모두에 필요하다
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException(format("Cannot process %s because Table Name is null or empty", flowFile));
        }

        final SchemaKey schemaKey = new SchemaKey(catalog, schemaName, tableName);
        final TableSchema tableSchema;
        try {
            tableSchema = schemaCache.get(schemaKey, key -> {
                try {
                    final TableSchema schema = TableSchema.from(con, catalog, schemaName, tableName, settings.translateFieldNames, updateKeys, log);
                    getLogger().debug("Fetched Table Schema {} for table name {}", schema, tableName);
                    return schema;
                } catch (SQLException e) {
                    // Wrap this in a runtime exception, it is unwrapped in the outer try
                    throw new ProcessException(e);
                }
            });
            if (tableSchema == null) {
                throw new IllegalArgumentException("No table schema specified!");
            }
        } catch (ProcessException pe) {
            // Unwrap the SQLException if one occurred
            if (pe.getCause() instanceof SQLException) {
                throw (SQLException) pe.getCause();
            } else {
                throw pe;
            }
        }

        // 카탈로그/스키마를 포함한 완전한(fully qualified) 테이블 이름을 구성한다
        final String fqTableName = generateTableName(settings, catalog, schemaName, tableName, tableSchema);

        final Map<String, PreparedSqlAndColumns> preparedSql = new HashMap<>();
        int currentBatchSize = 0;
        int batchIndex = 0;
        Record outerRecord;
        PreparedStatement lastPreparedStatement = null;

        try {
            while ((outerRecord = recordReader.nextRecord()) != null) {
                final String statementType;
                if (USE_RECORD_PATH.equalsIgnoreCase(explicitStatementType)) {
                    statementType = recordPathOperationType.apply(outerRecord);
                } else {
                    statementType = explicitStatementType;
                }

                final List<Record> dataRecords = getDataRecords(outerRecord);
                for (final Record currentRecord : dataRecords) {
                    PreparedSqlAndColumns preparedSqlAndColumns = preparedSql.get(statementType);
                    if (preparedSqlAndColumns == null) {
                        final RecordSchema recordSchema = currentRecord.getSchema();

                        final SqlAndIncludedColumns sqlHolder;
                        if (INSERT_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateInsert(recordSchema, fqTableName, tableSchema, settings);
                        } else if (UPDATE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateUpdate(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateDelete(recordSchema, fqTableName, tableSchema, settings);
                        } else if (UPSERT_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateUpsert(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else if (INSERT_IGNORE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateInsertIgnore(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else {
                            throw new IllegalArgumentException(format("Statement Type %s is not valid, FlowFile %s", statementType, flowFile));
                        }

                        // 생성된 SQL을 디버그 로그로 남긴다
                        log.debug("Generated SQL: {}", sqlHolder.getSql());
                        // PreparedStatement를 생성한다
                        final PreparedStatement preparedStatement = con.prepareStatement(sqlHolder.getSql());

                        try {
                            preparedStatement.setQueryTimeout(timeoutMillis); // timeout in seconds
                        } catch (final SQLException se) {
                            // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                            if (timeoutMillis > 0) {
                                throw se;
                            }
                        }

                        preparedSqlAndColumns = new PreparedSqlAndColumns(sqlHolder, preparedStatement);
                        preparedSql.put(statementType, preparedSqlAndColumns);
                    }

                    final PreparedStatement ps = preparedSqlAndColumns.getPreparedStatement();
                    final List<Integer> fieldIndexes = preparedSqlAndColumns.getSqlAndIncludedColumns().getFieldIndexes();
                    final String sql = preparedSqlAndColumns.getSqlAndIncludedColumns().getSql();

                    if (currentBatchSize > 0 && ps != lastPreparedStatement && lastPreparedStatement != null) {
                        batchIndex++;
                        log.debug("Executing query {} because Statement Type changed between Records for {}; fieldIndexes: {}; batch index: {}; batch size: {}",
                                sql, flowFile, fieldIndexes, batchIndex, currentBatchSize);
                        lastPreparedStatement.executeBatch();

                        session.adjustCounter("Batches Executed", 1, false);
                        currentBatchSize = 0;
                    }
                    lastPreparedStatement = ps;

                    final Object[] values = currentRecord.getValues();
                    final List<DataType> dataTypes = currentRecord.getSchema().getDataTypes();
                    final RecordSchema recordSchema = currentRecord.getSchema();
                    final Map<String, ColumnDescription> columns = tableSchema.getColumns();

                    int deleteIndex = 0;
                    for (int i = 0; i < fieldIndexes.size(); i++) {
                        final int currentFieldIndex = fieldIndexes.get(i);
                        Object currentValue = values[currentFieldIndex];
                        final DataType dataType = dataTypes.get(currentFieldIndex);
                        final int fieldSqlType = DataTypeUtils.getSQLTypeValue(dataType);
                        final String fieldName = recordSchema.getField(currentFieldIndex).getFieldName();
                        String columnName = ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames);
                        int sqlType;

                        final ColumnDescription column = columns.get(columnName);
                        // fieldIndexes는 테이블 컬럼과 매칭된 필드에 대응해야 하므로 여기서 'column'이 null이면 안 되지만, 만약을 대비해 처리한다
                        if (column == null) {
                            if (!settings.ignoreUnmappedFields) {
                                throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", columns.keySet()));
                            } else {
                                sqlType = fieldSqlType;
                            }
                        } else {
                            sqlType = column.getDataType();
                            // SQLServer는 DatabaseMetaData에서 sql_variant에 대해 -150을 반환하지만, 실제로 sql_variant 파라미터를 설정할 때 서버는 -156을 기대한다
                            if (sqlType == -150) {
                                sqlType = -156;
                            }
                        }

                        // 필요한 경우 필드의 데이터 타입을 컬럼의 데이터 타입으로 변환한다
                        if (fieldSqlType != sqlType) {
                            try {
                                DataType targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(sqlType);
                                // sqlType이 지원되지 않는 타입이면 대신 fieldSqlType으로 대체한다
                                if (targetDataType == null) {
                                    targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(fieldSqlType);
                                }
                                if (targetDataType != null) {
                                    if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY) {
                                        if (currentValue instanceof Object[]) {
                                            // Object[Byte] 배열을 byte[]로 변환한다
                                            Object[] src = (Object[]) currentValue;
                                            if (src.length > 0) {
                                                if (!(src[0] instanceof Byte)) {
                                                    throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY/VARBINARY/LONGVARBINARY");
                                                }
                                            }
                                            byte[] dest = new byte[src.length];
                                            for (int j = 0; j < src.length; j++) {
                                                dest[j] = (Byte) src[j];
                                            }
                                            currentValue = dest;
                                        } else if (currentValue instanceof String) {
                                            currentValue = ((String) currentValue).getBytes(StandardCharsets.UTF_8);
                                        } else if (currentValue != null && !(currentValue instanceof byte[])) {
                                            throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY/VARBINARY/LONGVARBINARY");
                                        }
                                    } else {
                                        currentValue = DataTypeUtils.convertType(
                                                currentValue,
                                                targetDataType,
                                                fieldName);
                                    }
                                }
                            } catch (IllegalTypeConversionException itce) {
                                // 필드와 컬럼의 타입이 일치하지 않거나 값을 컬럼 데이터 타입으로 변환할 수 없는 경우,
                                // 원래의 객체와 필드 데이터 타입으로 다시 시도한다
                                sqlType = DataTypeUtils.getSQLTypeValue(dataType);
                            }
                        }

                        // DELETE 타입이고 컬럼이 nullable이면, null 체크(자세한 내용은 generateDelete 참고)를 위해 값을 두 번 바인딩한다
                        if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                            setParameter(ps, ++deleteIndex, currentValue, fieldSqlType, sqlType);
                            if (column != null && column.isNullable()) {
                                setParameter(ps, ++deleteIndex, currentValue, fieldSqlType, sqlType);
                            }
                        } else if (UPSERT_TYPE.equalsIgnoreCase(statementType)) {
                            final int timesToAddObjects = databaseAdapter.getTimesToAddColumnObjectsForUpsert();
                            for (int j = 0; j < timesToAddObjects; j++) {
                                setParameter(ps, i + (fieldIndexes.size() * j) + 1, currentValue, fieldSqlType, sqlType);
                            }
                        } else {
                            setParameter(ps, i + 1, currentValue, fieldSqlType, sqlType);
                        }
                    }

                    ps.addBatch();
                    session.adjustCounter(statementType + " updates performed", 1, false);
                    if (++currentBatchSize == maxBatchSize) {
                        batchIndex++;
                        log.debug("Executing query {} because batch reached max size for {}; fieldIndexes: {}; batch index: {}; batch size: {}",
                                sql, flowFile, fieldIndexes, batchIndex, currentBatchSize);
                        session.adjustCounter("Batches Executed", 1, false);
                        ps.executeBatch();
                        currentBatchSize = 0;
                    }
                }
            }

            if (currentBatchSize > 0) {
                lastPreparedStatement.executeBatch();
                session.adjustCounter("Batches Executed", 1, false);
            }
        } finally {
            for (final PreparedSqlAndColumns preparedSqlAndColumns : preparedSql.values()) {
                preparedSqlAndColumns.getPreparedStatement().close();
            }
        }
    }

    /**
     * PreparedStatement의 지정된 인덱스 위치에 값을 바인딩한다. 대상 컬럼의 SQL 타입(BLOB/CLOB/VARBINARY 등)에 따라
     * 바인딩 방식이 달라지므로 타입별로 분기하여 처리하며, 그 외의 일반 타입은 setObject()로 처리한다.
     * OTHER 타입 + VARCHAR 조합은 PostgreSQL enum 등 특수 케이스를 위해 우선 시도 후 실패하면 일반 방식으로 재시도한다.
     */
    private void setParameter(PreparedStatement ps, int index, Object value, int fieldSqlType, int sqlType) throws IOException {
        if (sqlType == Types.BLOB) {
            // Byte[] 또는 String(byte[]로 변환된 모든 것)을 BLOB으로 변환한다
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement", e);
                        }
                    } else {
                        throw new IOException("Expected BLOB to be of type byte[] but is instead " + value.getClass().getName());
                    }
                }
                byte[] byteArray = (byte[]) value;
                try (InputStream inputStream = new ByteArrayInputStream(byteArray)) {
                    ps.setBlob(index, inputStream);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data " + value, e.getCause());
                }
            } else {
                try (InputStream inputStream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8))) {
                    ps.setBlob(index, inputStream);
                } catch (IOException | SQLException e) {
                    throw new IOException("Unable to parse binary data " + value, e.getCause());
                }
            }
        } else if (sqlType == Types.CLOB) {
            if (value == null) {
                try {
                    ps.setNull(index, Types.CLOB);
                } catch (SQLException e) {
                    throw new IOException("Unable to setNull() on prepared statement", e);
                }
            } else {
                try {
                    Clob clob = ps.getConnection().createClob();
                    clob.setString(1, value.toString());
                    ps.setClob(index, clob);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse data as CLOB/String " + value, e.getCause());
                }
            }
        } else if (sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY) {
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement", e);
                        }
                    } else {
                        throw new IOException("Expected VARBINARY/LONGVARBINARY to be of type byte[] but is instead " + value.getClass().getName());
                    }
                }
                byte[] byteArray = (byte[]) value;
                try {
                    ps.setBytes(index, byteArray);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data with size" + byteArray.length, e.getCause());
                }
            } else {
                byte[] byteArray = new byte[0];
                try {
                    byteArray = value.toString().getBytes(StandardCharsets.UTF_8);
                    ps.setBytes(index, byteArray);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data with size" + byteArray.length, e.getCause());
                }
            }
        } else {
            try {
                // 지정된 필드 타입이 OTHER이고 SQL 타입이 VARCHAR인 경우, 문자열 리터럴로의 변환은 문제없이 되었지만
                // 파라미터를 설정할 때는 OTHER 타입으로 먼저 시도한다. 오류가 발생하면
                // sqlType을 사용하는 일반적인 방식으로 재시도한다
                // 이는 PostgreSQL enum 등의 경우에 도움이 된다
                if (fieldSqlType == Types.OTHER && sqlType == Types.VARCHAR) {
                    try {
                        ps.setObject(index, value, fieldSqlType);
                    } catch (SQLException e) {
                        // 기본 setObject 파라미터 방식으로 대체한다
                        ps.setObject(index, value, sqlType);
                    }
                } else {
                    ps.setObject(index, value, sqlType);
                }
            } catch (SQLException e) {
                throw new IOException("Unable to setObject() with value " + value + " at index " + index + " of type " + sqlType, e);
            }
        }
    }

    /**
     * Data Record Path 프로퍼티가 지정된 경우, 입력 레코드(outerRecord)에서 RecordPath를 평가하여
     * 실제로 데이터베이스에 반영할 하위 레코드(들)를 추출한다. 지정되지 않았다면 입력 레코드 자체를 그대로 반환한다.
     * RecordPath 평가 결과가 RECORD 타입이 아니거나 결과가 비어 있으면 예외를 던진다.
     */
    private List<Record> getDataRecords(final Record outerRecord) {
        if (dataRecordPath == null) {
            return Collections.singletonList(outerRecord);
        }

        final RecordPathResult result = dataRecordPath.evaluate(outerRecord);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(Collectors.toList());
        if (fieldValues.isEmpty()) {
            throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record yielded no results.");
        }

        for (final FieldValue fieldValue : fieldValues) {
            final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();
            if (fieldType != RecordFieldType.RECORD) {
                throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record expected to return one or more Records but encountered field of type" +
                        " " + fieldType);
            }
        }

        final List<Record> dataRecords = new ArrayList<>(fieldValues.size());
        for (final FieldValue fieldValue : fieldValues) {
            dataRecords.add((Record) fieldValue.getValue());
        }

        return dataRecords;
    }

    private String getJdbcUrl(final Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData != null) {
                return databaseMetaData.getURL();
            }
        } catch (final Exception e) {
            getLogger().warn("Could not determine JDBC URL based on the Driver Connection.", e);
        }

        return "DBCPService";
    }

    /**
     * FlowFile에 적용할 Statement Type을 결정한다. Statement Type 프로퍼티가 'Use statement.type Attribute'로
     * 설정되어 있으면 FlowFile의 statement.type 속성 값을 사용하고, 그렇지 않으면 프로퍼티 값을 그대로 사용한다.
     * 이후 validateStatementType()으로 값의 유효성을 검증한다.
     */
    private String getStatementType(final ProcessContext context, final FlowFile flowFile) {
        // 필요한 경우 속성으로부터 statement type을 가져온다
        final String statementTypeProperty = context.getProperty(STATEMENT_TYPE).getValue();
        String statementType = statementTypeProperty;
        if (USE_ATTR_TYPE.equals(statementTypeProperty)) {
            statementType = flowFile.getAttribute(STATEMENT_TYPE_ATTRIBUTE);
        }

        return validateStatementType(statementType, flowFile);
    }

    private String validateStatementType(final String statementType, final FlowFile flowFile) {
        if (statementType == null || statementType.trim().isEmpty()) {
            throw new ProcessException("No Statement Type specified for " + flowFile);
        }

        if (INSERT_TYPE.equalsIgnoreCase(statementType) || UPDATE_TYPE.equalsIgnoreCase(statementType) || DELETE_TYPE.equalsIgnoreCase(statementType)
                || UPSERT_TYPE.equalsIgnoreCase(statementType) || SQL_TYPE.equalsIgnoreCase(statementType) || USE_RECORD_PATH.equalsIgnoreCase(statementType)
                || INSERT_IGNORE_TYPE.equalsIgnoreCase(statementType)) {

            return statementType;
        }

        throw new ProcessException("Invalid Statement Type <" + statementType + "> for " + flowFile);
    }

    /**
     * FlowFile 본문을 Record Reader로 파싱하여 얻은 RecordReader를 이용해, Statement Type이 'SQL'이면
     * 원본 SQL을 그대로 실행(executeSQL)하고, 그 외의 경우는 DMLSettings(현재 프로퍼티 설정 스냅샷)를 만들어
     * SQL을 동적으로 생성/실행(executeDML)한다.
     */
    private void putToDatabase(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Connection connection) throws Exception {
        final String statementType = getStatementType(context, flowFile);

        try (final InputStream in = session.read(flowFile)) {
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
            final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());

            if (SQL_TYPE.equalsIgnoreCase(statementType)) {
                executeSQL(context, flowFile, connection, recordReader);
            } else {
                final DMLSettings settings = new DMLSettings(context);
                executeDML(context, session, flowFile, connection, recordReader, statementType, settings);
            }
        }
    }

    /**
     * catalog.schema.table 형태로 완전한(fully qualified) 테이블 이름을 조합한다.
     * catalog/schemaName이 null이면 해당 구간은 생략하며, Quote Table Identifiers 설정이 켜져 있으면
     * 각 구간을 데이터베이스 방언의 식별자 인용 문자(quoted identifier)로 감싼다.
     */
    String generateTableName(final DMLSettings settings, final String catalog, final String schemaName, final String tableName, final TableSchema tableSchema) {
        final StringBuilder tableNameBuilder = new StringBuilder();
        if (catalog != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(catalog)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(catalog);
            }

            tableNameBuilder.append(".");
        }

        if (schemaName != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(schemaName)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(schemaName);
            }

            tableNameBuilder.append(".");
        }

        if (settings.quoteTableName) {
            tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            tableNameBuilder.append(tableName);
        }

        return tableNameBuilder.toString();
    }

    private Set<String> getNormalizedColumnNames(final RecordSchema schema, final boolean translateFieldNames) {
        final Set<String> normalizedFieldNames = new HashSet<>();
        if (schema != null) {
            schema.getFieldNames().forEach((fieldName) -> normalizedFieldNames.add(ColumnDescription.normalizeColumnName(fieldName, translateFieldNames)));
        }
        return normalizedFieldNames;
    }

    /**
     * 레코드 스키마와 실제 테이블 컬럼 메타데이터(tableSchema)를 비교하여 "INSERT INTO table (col1, col2, ...)
     * VALUES (?, ?, ...)" 형태의 PreparedStatement용 SQL을 생성한다. 테이블 컬럼에 매핑되지 않는 필드는
     * Unmatched Field Behavior 설정에 따라 무시하거나 예외를 던진다. 반환되는 SqlAndIncludedColumns의
     * fieldIndexes는 SQL의 각 '?' 위치가 레코드의 몇 번째 필드에 대응하는지를 나타낸다.
     */
    SqlAndIncludedColumns generateInsert(final RecordSchema recordSchema, final String tableName, final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, SQLException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(tableName);
        sqlBuilder.append(" (");

        // 레코드의 모든 필드를 순회하며 컬럼 이름을 추가하여 SQL 문을 구성한다
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(", ");
                    }

                    if (settings.escapeColumnNames) {
                        sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                .append(desc.getColumnName())
                                .append(tableSchema.getQuotedIdentifierString());
                    } else {
                        sqlBuilder.append(desc.getColumnName());
                    }
                    includedColumns.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }

            // 값 자리에 들어갈 '?' 플레이스홀더들을 추가하여 SQL 문을 완성한다.
            sqlBuilder.append(") VALUES (");
            sqlBuilder.append(StringUtils.repeat("?", ",", includedColumns.size()));
            sqlBuilder.append(")");

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    /**
     * UPSERT(존재하면 UPDATE, 없으면 INSERT) SQL을 생성한다. 실제 SQL 문법은 데이터베이스마다 크게 다르므로
     * (예: MySQL의 ON DUPLICATE KEY UPDATE, PostgreSQL의 ON CONFLICT 등) 컬럼 목록과 키 컬럼 목록만 구성한 뒤
     * databaseAdapter.getUpsertStatement()에 위임하여 방언에 맞는 SQL을 완성한다.
     */
    SqlAndIncludedColumns generateUpsert(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                         final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, SQLException, MalformedRecordException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames);

        List<String> usedColumnNames = new ArrayList<>();
        List<Integer> usedColumnIndices = new ArrayList<>();

        List<String> fieldNames = recordSchema.getFieldNames();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (settings.escapeColumnNames) {
                        usedColumnNames.add(tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString());
                    } else {
                        usedColumnNames.add(desc.getColumnName());
                    }
                    usedColumnIndices.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }
        }

        final Set<String> literalKeyColumnNames = new HashSet<>(keyColumnNames.size());
        for (String literalKeyColumnName : keyColumnNames) {
            if (settings.escapeColumnNames) {
                literalKeyColumnNames.add(tableSchema.getQuotedIdentifierString() + literalKeyColumnName + tableSchema.getQuotedIdentifierString());
            } else {
                literalKeyColumnNames.add(literalKeyColumnName);
            }
        }
        String sql = databaseAdapter.getUpsertStatement(tableName, usedColumnNames, literalKeyColumnNames);

        return new SqlAndIncludedColumns(sql, usedColumnIndices);
    }

    /**
     * INSERT_IGNORE(키 충돌 시 오류 없이 무시하고 넘어가는 INSERT) SQL을 생성한다. generateUpsert와 마찬가지로
     * 컬럼/키 컬럼 목록만 구성하고 실제 방언별 문법은 databaseAdapter.getInsertIgnoreStatement()에 위임한다.
     */
    SqlAndIncludedColumns generateInsertIgnore(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                               final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, SQLException, MalformedRecordException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames);

        List<String> usedColumnNames = new ArrayList<>();
        List<Integer> usedColumnIndices = new ArrayList<>();

        List<String> fieldNames = recordSchema.getFieldNames();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (settings.escapeColumnNames) {
                        usedColumnNames.add(tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString());
                    } else {
                        usedColumnNames.add(desc.getColumnName());
                    }
                    usedColumnIndices.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }
        }

        final Set<String> literalKeyColumnNames = new HashSet<>(keyColumnNames.size());
        for (String literalKeyColumnName : keyColumnNames) {
            if (settings.escapeColumnNames) {
                literalKeyColumnNames.add(tableSchema.getQuotedIdentifierString() + literalKeyColumnName + tableSchema.getQuotedIdentifierString());
            } else {
                literalKeyColumnNames.add(literalKeyColumnName);
            }
        }

        String sql = databaseAdapter.getInsertIgnoreStatement(tableName, usedColumnNames, literalKeyColumnNames);

        return new SqlAndIncludedColumns(sql, usedColumnIndices);
    }

    /**
     * "UPDATE table SET col1 = ?, col2 = ? WHERE keyCol1 = ? AND keyCol2 = ?" 형태의 SQL을 생성한다.
     * Update Keys 프로퍼티(없으면 테이블의 기본 키)에 해당하는 컬럼은 SET 절에서 제외하고 WHERE 절의 조건으로만
     * 사용한다. includedColumns의 순서가 곧 PreparedStatement에 값을 바인딩할 순서(SET 절 다음 WHERE 절)이므로
     * 이 순서를 executeDML에서 그대로 사용한다.
     */
    SqlAndIncludedColumns generateUpdate(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                         final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, SQLException {

        final Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        final Set<String> normalizedKeyColumnNames = normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames);

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ");
        sqlBuilder.append(tableName);

        // 레코드의 모든 필드를 순회하며 컬럼 이름을 추가하여 SQL 문을 구성한다
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            sqlBuilder.append(" SET ");

            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final String normalizedColName = ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames);
                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null) {
                    if (!settings.ignoreUnmappedFields) {
                        throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                    } else {
                        // User is ignoring unmapped fields, but log at debug level just in case
                        getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                        continue;
                    }
                }

                // 이 컬럼이 Update Key인지 확인한다. Update Key라면 지금은 건너뛰고
                // SET 절을 모두 완성한 뒤 다시 처리한다
                if (!normalizedKeyColumnNames.contains(normalizedColName)) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(", ");
                    }

                    if (settings.escapeColumnNames) {
                        sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                .append(desc.getColumnName())
                                .append(tableSchema.getQuotedIdentifierString());
                    } else {
                        sqlBuilder.append(desc.getColumnName());
                    }

                    sqlBuilder.append(" = ?");
                    includedColumns.add(i);
                }
            }

            AtomicInteger whereFieldCount = new AtomicInteger(0);
            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();
                boolean firstUpdateKey = true;

                final String normalizedColName = ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames);
                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc != null) {

                    // 이 컬럼이 Update Key인지 확인한다. Update Key라면 WHERE 절에 추가한다
                    if (normalizedKeyColumnNames.contains(normalizedColName)) {

                        if (whereFieldCount.getAndIncrement() > 0) {
                            sqlBuilder.append(" AND ");
                        } else if (firstUpdateKey) {
                            // Update Key 값을 기준으로 WHERE 절을 설정한다
                            sqlBuilder.append(" WHERE ");
                            firstUpdateKey = false;
                        }

                        if (settings.escapeColumnNames) {
                            sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                    .append(desc.getColumnName())
                                    .append(tableSchema.getQuotedIdentifierString());
                        } else {
                            sqlBuilder.append(desc.getColumnName());
                        }
                        sqlBuilder.append(" = ?");
                        includedColumns.add(i);
                    }
                }
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    /**
     * "DELETE FROM table WHERE (col1 = ? OR (col1 is null AND ? is null)) AND ..." 형태의 SQL을 생성한다.
     * PreparedStatement는 값이 null인지 미리 알 수 없으므로, nullable 컬럼에 대해서는 NIFI-3742에서 도입된
     * null-안전(null-safe) 패턴(column = ? OR (column is null AND ? is null))을 사용해 컬럼당 파라미터를
     * 두 번 바인딩할 수 있도록 조건절을 구성한다. 이로 인해 setParameter 호출 시 해당 컬럼이 nullable이면
     * 같은 값을 두 번 세팅해야 한다(executeDML의 deleteIndex 처리 참고).
     */
    SqlAndIncludedColumns generateDelete(final RecordSchema recordSchema, final String tableName, final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, SQLDataException {

        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);
        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = ColumnDescription.normalizeColumnName(requiredColName, settings.translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM ");
        sqlBuilder.append(tableName);

        // 레코드의 모든 필드를 순회하며 컬럼 이름을 추가하여 SQL 문을 구성한다
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            sqlBuilder.append(" WHERE ");
            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {

                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(ColumnDescription.normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(" AND ");
                    }

                    String columnName;
                    if (settings.escapeColumnNames) {
                        columnName = tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString();
                    } else {
                        columnName = desc.getColumnName();
                    }
                    // PreparedStatement를 사용하기 때문에 값이 null인지 미리 알 수 없으므로 WHERE 절을 null-안전하게 구성해야 한다.
                    // 값이 null이면 필터는 "column = null"이 아니라 "column IS null"이어야 한다. 값이 null인지 알 수 없으므로
                    // 다음과 같은 구조(NIFI-3742에서 유래)를 사용한다:
                    //   (column = ? OR (column is null AND ? is null))
                    sqlBuilder.append("(");
                    sqlBuilder.append(columnName);
                    sqlBuilder.append(" = ?");

                    // 컬럼이 nullable인 경우에만 null 체크가 필요하다. 그렇지 않다면 애초에 해당 행이 존재하지 않을 것이다
                    if (desc.isNullable()) {
                        sqlBuilder.append(" OR (");
                        sqlBuilder.append(columnName);
                        sqlBuilder.append(" is null AND ? is null))");
                    } else {
                        sqlBuilder.append(")");
                    }
                    includedColumns.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }
        }

        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    /**
     * 테이블에서 필수(Required, 즉 NOT NULL이고 기본값이 없는)로 분류된 컬럼들에 대해 레코드 스키마에
     * 대응하는 필드가 존재하는지 검사한다. 누락된 경우 Unmatched Column Behavior 설정에 따라
     * 예외를 던지거나(FAIL) 경고 로그만 남긴다(WARN).
     */
    private void checkValuesForRequiredColumns(RecordSchema recordSchema, TableSchema tableSchema, DMLSettings settings) {
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);

        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = ColumnDescription.normalizeColumnName(requiredColName, settings.translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new IllegalArgumentException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }
    }

    /**
     * UPDATE/UPSERT/INSERT_IGNORE의 WHERE 절(또는 충돌 판정) 기준이 될 키 컬럼 이름 집합을 결정한다.
     * Update Keys 프로퍼티가 지정되어 있으면 이를 콤마로 분리하여 사용하고, 지정되어 있지 않으면
     * 테이블의 기본 키(Primary Key) 컬럼을 사용한다. 둘 다 없으면(키 컬럼을 하나도 결정할 수 없으면) 예외를 던진다.
     */
    private Set<String> getUpdateKeyColumnNames(String tableName, String updateKeys, TableSchema tableSchema) throws SQLIntegrityConstraintViolationException {
        final Set<String> updateKeyColumnNames;

        if (updateKeys == null) {
            updateKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        } else {
            updateKeyColumnNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeyColumnNames.add(updateKey.trim());
            }
        }

        if (updateKeyColumnNames.isEmpty()) {
            throw new SQLIntegrityConstraintViolationException("Table '" + tableName + "' not found or does not have a Primary Key and no Update Keys were specified");
        }

        return updateKeyColumnNames;
    }

    /**
     * 키 컬럼 이름들을 정규화(대소문자/네이밍 규칙 변환)하고, 레코드에 각 키 컬럼에 대응하는 필드 값이
     * 실제로 존재하는지 검사한다. 키 컬럼 값이 누락되면 WHERE 절 또는 충돌 판정 조건이 불완전해지므로
     * Unmatched Column Behavior 설정에 따라 예외를 던지거나 경고를 남긴다.
     */
    private Set<String> normalizeKeyColumnNamesAndCheckForValues(RecordSchema recordSchema, String updateKeys, DMLSettings settings, Set<String> updateKeyColumnNames)
            throws MalformedRecordException {
        // 정규화된 모든 Update Key 이름의 집합을 만들고, 각 Update Key 필드에 대응하는 값이
        // 레코드에 실제로 존재하는지 확인한다.
        final Set<String> normalizedRecordFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);

        final Set<String> normalizedKeyColumnNames = new HashSet<>();
        for (final String updateKeyColumnName : updateKeyColumnNames) {
            String normalizedKeyColumnName = ColumnDescription.normalizeColumnName(updateKeyColumnName, settings.translateFieldNames);

            if (!normalizedRecordFieldNames.contains(normalizedKeyColumnName)) {
                String missingColMessage = "Record does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + updateKeyColumnName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
            normalizedKeyColumnNames.add(normalizedKeyColumnName);
        }

        return normalizedKeyColumnNames;
    }

    /**
     * schemaCache의 키로 사용되는 (catalog, schema, table) 조합. 동일한 3중값이면 동일한 TableSchema를
     * 재사용할 수 있도록 hashCode/equals를 구현한다.
     */
    static class SchemaKey {
        private final String catalog;
        private final String schemaName;
        private final String tableName;

        public SchemaKey(final String catalog, final String schemaName, final String tableName) {
            this.catalog = catalog;
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            int result = catalog != null ? catalog.hashCode() : 0;
            result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
            result = 31 * result + tableName.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SchemaKey schemaKey = (SchemaKey) o;

            if (catalog != null ? !catalog.equals(schemaKey.catalog) : schemaKey.catalog != null) return false;
            if (schemaName != null ? !schemaName.equals(schemaKey.schemaName) : schemaKey.schemaName != null)
                return false;
            return tableName.equals(schemaKey.tableName);
        }
    }

    /**
     * 생성된 SQL 문자열과, 어떤 컬럼들이 갱신 대상인지(PreparedStatement에 어떤 레코드 필드 값을 설정해야 하는지)를
     * 함께 담아두는 보관용 클래스. getIncludedColumns가 null이면 모든 컬럼/필드가 포함됨을 의미한다.
     */
    static class SqlAndIncludedColumns {
        private final String sql;
        private final List<Integer> fieldIndexes;

        /**
         * 생성자
         *
         * @param sql          PreparedStatement용 SQL 문(파라미터는 ?로 표기됨)
         * @param fieldIndexes 레코드 인덱스의 목록. 목록의 인덱스 위치가 SQL PreparedStatement 내에서 해당 레코드 필드가 위치하는 순서를 나타낸다
         */
        public SqlAndIncludedColumns(final String sql, final List<Integer> fieldIndexes) {
            this.sql = sql;
            this.fieldIndexes = fieldIndexes;
        }

        public String getSql() {
            return sql;
        }

        public List<Integer> getFieldIndexes() {
            return fieldIndexes;
        }
    }

    /**
     * 생성된 SQL/포함 컬럼 정보(SqlAndIncludedColumns)와 이를 기반으로 실제로 준비된 PreparedStatement를
     * 함께 보관하여, 동일한 Statement Type의 후속 레코드에서 재사용(배치 추가)할 수 있도록 한다.
     */
    static class PreparedSqlAndColumns {
        private final SqlAndIncludedColumns sqlAndIncludedColumns;
        private final PreparedStatement preparedStatement;

        public PreparedSqlAndColumns(final SqlAndIncludedColumns sqlAndIncludedColumns, final PreparedStatement preparedStatement) {
            this.sqlAndIncludedColumns = sqlAndIncludedColumns;
            this.preparedStatement = preparedStatement;
        }

        public SqlAndIncludedColumns getSqlAndIncludedColumns() {
            return sqlAndIncludedColumns;
        }

        public PreparedStatement getPreparedStatement() {
            return preparedStatement;
        }
    }

    /**
     * Statement Type이 'Use Record Path'일 때 사용되는 함수 구현체. 각 레코드에 대해 컴파일된 RecordPath를
     * 평가하여 단일 값(INSERT/UPDATE/DELETE/UPSERT 중 하나)을 얻어낸다. 평가 결과가 없거나 여러 개이거나
     * 허용되지 않는 값이면 ProcessException을 던진다.
     */
    private static class RecordPathStatementType implements Function<Record, String> {
        private final RecordPath recordPath;

        public RecordPathStatementType(final RecordPath recordPath) {
            this.recordPath = recordPath;
        }

        @Override
        public String apply(final Record record) {
            final RecordPathResult recordPathResult = recordPath.evaluate(record);
            final List<FieldValue> resultList = recordPathResult.getSelectedFields().distinct().collect(Collectors.toList());
            if (resultList.isEmpty()) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record but got no results");
            }

            if (resultList.size() > 1) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record and received multiple distinct results (" + resultList + ")");
            }

            final String resultValue = String.valueOf(resultList.get(0).getValue()).toUpperCase();
            switch (resultValue) {
                case INSERT_TYPE:
                case UPDATE_TYPE:
                case DELETE_TYPE:
                case UPSERT_TYPE:
                    return resultValue;
            }

            throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record to determine Statement Type but found invalid value: " + resultValue);
        }
    }

    /**
     * onTrigger 1회 실행 동안 사용할 DML 관련 프로퍼티 값들을 한 번만 읽어 불변 스냅샷으로 보관하는 클래스.
     * 매 레코드/필드 처리마다 ProcessContext.getProperty()를 반복 호출하지 않도록 하기 위한 목적이다.
     */
    static class DMLSettings {
        private final boolean translateFieldNames;
        private final boolean ignoreUnmappedFields;

        // Unmatched Column Behavior가 실패(fail)인지 경고(warning)인지 여부
        private final boolean failUnmappedColumns;
        private final boolean warningUnmappedColumns;

        // 컬럼 이름을 인용 부호로 감쌀지 여부
        private final boolean escapeColumnNames;

        // 테이블 이름을 인용 부호로 감쌀지 여부
        private final boolean quoteTableName;

        DMLSettings(ProcessContext context) {
            translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
            ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());

            failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());
            warningUnmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

            escapeColumnNames = context.getProperty(QUOTE_IDENTIFIERS).asBoolean();
            quoteTableName = context.getProperty(QUOTE_TABLE_IDENTIFIER).asBoolean();
        }
    }

}