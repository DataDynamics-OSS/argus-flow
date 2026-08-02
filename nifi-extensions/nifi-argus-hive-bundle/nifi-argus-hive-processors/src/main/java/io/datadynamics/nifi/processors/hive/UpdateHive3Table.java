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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/processors/hive/UpdateHive3Table.java
 */
package io.datadynamics.nifi.processors.hive;

import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import io.datadynamics.nifi.services.api.hive.VendorHiveDBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.DiscontinuedException;
import org.apache.nifi.processors.hadoop.exception.RecordReaderFactoryException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Hive JDBC 커넥션을 통해 대상 테이블의 현재 메타데이터(컬럼, 파티션, 위치 등)를 조회하고,
 * 입력 레코드의 스키마와 비교하여 테이블을 생성하거나(Create Table Strategy가 'Create If Not Exists'인 경우)
 * 부족한 컬럼/파티션을 ALTER TABLE로 추가하는 프로세서.
 * <p>
 * 클래스 로더 격리를 위해 {@code @RequiresInstanceClassLoading}이 지정되어 있는데, 이는 Hive JDBC 드라이버가
 * 인스턴스별로 독립된 클래스로더에서 로드되어야 여러 인스턴스가 서로 다른 Hive 버전/설정을 사용할 때 충돌하지 않기 때문이다.
 * <p>
 * "Update Field Names" 옵션이 true인 경우, 입력 레코드 필드명과 실제 Hive 컬럼명의 대소문자가 다를 수 있으므로
 * Record Reader로 읽은 레코드를 Record Writer로 다시 써서 필드명을 Hive 컬럼명과 정확히 일치시킨다
 * (Avro 등 일부 포맷은 필드명이 정확히 일치해야 하기 때문).
 */
@Tags({"cloudera", "hive", "metadata", "jdbc", "database", "table"})
@CapabilityDescription("이 프로세서는 Hive JDBC 커넥션과 입력 레코드를 사용하여, 들어오는 레코드를 수용하는 데 필요한 Hive 3.0+ 테이블 변경 사항을 생성한다.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "hive.table.management.strategy", description = "'Table Management Strategy' 속성이 이 속성값을 사용하도록 설정된 경우에 읽어들이는 속성이다. "
                + "이 속성의 값은 'Table Management Strategy' 속성이 허용하는 옵션 중 하나와 대소문자를 무시하고 일치해야 한다.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "output.table", description = "'success' 및 'failure' 관계로 라우팅되는 FlowFile에 기록되며, "
                + "대상 테이블 이름을 담고 있다."),
        @WritesAttribute(attribute = "output.path", description = "'success' 및 'failure' 관계로 라우팅되는 FlowFile에 기록되며, "
                + "테이블이 위치한 파일시스템 상의 경로(테이블이 파티션되어 있다면 파티션 위치)를 담고 있다."),
        @WritesAttribute(attribute = "mime.type", description = "Record Writer가 지정되어 있고 Update Field Names가 'true'인 경우에만, "
                + "Record Writer가 지정한 MIME 타입으로 mime.type 속성을 설정한다."),
        @WritesAttribute(attribute = "record.count", description = "Record Writer가 지정되어 있고 Update Field Names가 'true'인 경우에만, FlowFile에 포함된 레코드 개수를 설정한다.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@RequiresInstanceClassLoading
public class UpdateHive3Table extends AbstractProcessor {

    // 이 프로세서가 사용하는 관계(Relationship) 정의
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("레코드가 Hive로 성공적으로 반영된 후 라우팅되는 FlowFile을 포함하는 관계.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("레코드를 Hive로 반영하지 못한 경우 라우팅되는 FlowFile을 포함하는 관계.")
            .build();
    static final String TEXTFILE = "TEXTFILE";
    static final String SEQUENCEFILE = "SEQUENCEFILE";
    static final String ORC = "ORC";
    static final String PARQUET = "PARQUET";
    static final String AVRO = "AVRO";
    static final String RCFILE = "RCFILE";
    // 테이블 생성 시 사용할 수 있는 저장 포맷(STORED AS 절)에 대한 선택지들
    static final AllowableValue TEXTFILE_STORAGE = new AllowableValue(TEXTFILE, TEXTFILE, "일반 텍스트 파일로 저장한다. hive.default.fileformat 설정 파라미터가 "
            + "다른 값으로 설정되어 있지 않은 한, TEXTFILE이 기본 파일 포맷이다.");
    static final AllowableValue SEQUENCEFILE_STORAGE = new AllowableValue(SEQUENCEFILE, SEQUENCEFILE, "압축된 Sequence File로 저장한다.");
    static final AllowableValue ORC_STORAGE = new AllowableValue(ORC, ORC, "ORC 파일 포맷으로 저장한다. ACID 트랜잭션과 비용 기반 옵티마이저(CBO)를 지원하며, "
            + "컬럼 수준 메타데이터를 저장한다.");
    static final AllowableValue PARQUET_STORAGE = new AllowableValue(PARQUET, PARQUET, "Parquet 컬럼형 저장 포맷인 Parquet 형식으로 저장한다.");
    static final AllowableValue AVRO_STORAGE = new AllowableValue(AVRO, AVRO, "Avro 포맷으로 저장한다.");
    static final AllowableValue RCFILE_STORAGE = new AllowableValue(RCFILE, RCFILE, "Record Columnar File 포맷으로 저장한다.");
    static final AllowableValue CREATE_IF_NOT_EXISTS = new AllowableValue("Create If Not Exists", "Create If Not Exists",
            "테이블이 아직 존재하지 않는 경우 주어진 스키마로 테이블을 생성한다");
    static final AllowableValue FAIL_IF_NOT_EXISTS = new AllowableValue("Fail If Not Exists", "Fail If Not Exists",
            "대상 테이블이 아직 존재하지 않는 경우 에러를 기록하고 FlowFile을 failure로 라우팅한다");
    static final String TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE = "hive.table.management.strategy";
    static final AllowableValue MANAGED_TABLE = new AllowableValue("Managed", "Managed",
            "이 프로세서가 생성하는 모든 테이블은 관리형(Managed) 테이블이 된다(자세한 내용은 Hive 문서 참조).");
    static final AllowableValue EXTERNAL_TABLE = new AllowableValue("External", "External",
            "이 프로세서가 생성하는 모든 테이블은 `External Table Location` 속성값 위치에 있는 외부(External) 테이블이 된다.");
    static final AllowableValue ATTRIBUTE_DRIVEN_TABLE = new AllowableValue("Use '" + TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE + "' Attribute",
            "Use '" + TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE + "' Attribute",
            "'" + TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE + "' FlowFile 속성을 검사하여 테이블 관리 전략을 결정한다. 이 속성의 값은 "
                    + "다른 허용값(Managed, External 등) 중 하나와 대소문자를 무시하고 일치해야 한다.");
    static final String ATTR_OUTPUT_TABLE = "output.table";
    static final String ATTR_OUTPUT_PATH = "output.path";
    // 프로세서 속성(PropertyDescriptor) 정의
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("레코드 리더")
            .description("입력 FlowFile을 읽기 위한 서비스. 이 Reader는 레코드의 스키마를 판단하는 데에만 사용되며, 실제 레코드 자체는 처리되지 않는다.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor HIVE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("hive3-dbcp-service")
            .displayName("Hive 데이터베이스 연결 풀링 서비스")
            .description("Hive 데이터베이스에 대한 커넥션을 얻기 위해 사용하는 Hive 컨트롤러 서비스")
            .required(true)
            .identifiesControllerService(VendorHiveDBCPService.class)
            .build();
    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("hive3-table-name")
            .displayName("테이블명")
            .description("업데이트할 데이터베이스 테이블의 이름. 테이블이 존재하지 않는 경우, Create Table 속성값에 따라 테이블을 생성하거나 "
                    + "에러가 발생한다.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor CREATE_TABLE = new PropertyDescriptor.Builder()
            .name("hive3-create-table")
            .displayName("테이블 생성 전략")
            .description("대상 테이블이 존재하지 않을 때 어떻게 처리할지(생성할지, 실패시킬지 등)를 지정한다.")
            .required(true)
            .addValidator(Validator.VALID)
            .allowableValues(CREATE_IF_NOT_EXISTS, FAIL_IF_NOT_EXISTS)
            .defaultValue(FAIL_IF_NOT_EXISTS.getValue())
            .build();
    static final PropertyDescriptor TABLE_MANAGEMENT_STRATEGY = new PropertyDescriptor.Builder()
            .name("hive3-create-table-management")
            .displayName("테이블 관리 전략")
            .description("(테이블이 생성될 경우) 해당 테이블이 관리형(Managed) 테이블인지 외부(External) 테이블인지를 지정한다. External을 지정하는 경우, "
                    + "'External Table Location' 속성을 반드시 지정해야 한다. '" + TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE + "' 값을 선택한 경우에도 'External Table Location'을 "
                    + "지정해야 하지만, Expression Language를 포함하거나 빈 문자열로 둘 수 있으며, 속성값이 'Managed'로 평가될 경우에는 무시된다.")
            .required(true)
            .addValidator(Validator.VALID)
            .allowableValues(MANAGED_TABLE, EXTERNAL_TABLE, ATTRIBUTE_DRIVEN_TABLE)
            .defaultValue(MANAGED_TABLE.getValue())
            .dependsOn(CREATE_TABLE, CREATE_IF_NOT_EXISTS)
            .build();
    static final PropertyDescriptor EXTERNAL_TABLE_LOCATION = new PropertyDescriptor.Builder()
            .name("hive3-external-table-location")
            .displayName("외부 테이블 위치")
            .description("(외부 테이블을 생성할 경우) 테이블 데이터를 저장할 파일 경로(예: HDFS 경로)를 지정한다.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .dependsOn(TABLE_MANAGEMENT_STRATEGY, EXTERNAL_TABLE, ATTRIBUTE_DRIVEN_TABLE)
            .build();

    static final PropertyDescriptor TABLE_STORAGE_FORMAT = new PropertyDescriptor.Builder()
            .name("hive3-storage-format")
            .displayName("테이블 생성 저장 포맷")
            .description("테이블이 생성될 경우, 지정된 저장 포맷이 사용된다.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(TEXTFILE_STORAGE, SEQUENCEFILE_STORAGE, ORC_STORAGE, PARQUET_STORAGE, AVRO_STORAGE, RCFILE_STORAGE)
            .defaultValue(TEXTFILE)
            .dependsOn(CREATE_TABLE, CREATE_IF_NOT_EXISTS)
            .build();
    static final PropertyDescriptor UPDATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("hive3-update-field-names")
            .displayName("필드명 업데이트")
            .description("출력 스키마의 필드명을 지정된 테이블의 정확한 컬럼명으로 맞출지 여부를 나타낸다. "
                    + "입력 레코드의 필드명이 테이블 컬럼명과 대소문자 표기가 다를 수 있는 경우에 사용해야 한다. 예를 들어 출력 FlowFile(및 대상 테이블 저장 포맷)이 "
                    + "Avro 포맷인 경우, Hive/Impala는 필드명이 컬럼명과 정확히 일치할 것을 요구하므로 이 속성을 true로 설정해야 한다.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("hive3-record-writer")
            .displayName("레코드 라이터")
            .description("결과를 FlowFile에 기록할 때 사용할 컨트롤러 서비스를 지정한다. Record Writer는 추론된 스키마 동작을 흉내내기 위해 Inherit Schema를 "
                    + "사용해야 한다. 즉 Writer에 별도의 명시적 스키마를 정의할 필요가 없으며, 컬럼 타입으로부터 스키마를 추론하는 것과 동일한 로직으로 스키마가 공급된다. "
                    + "Create Table Strategy가 'Create If Not Exists'로 설정된 경우, 생성된 테이블 위치에 데이터가 올바르게 저장되려면 Record Writer의 출력 포맷이 "
                    + "Record Reader의 포맷과 일치해야 한다. 이 속성은 'Update Field Names'가 true로 설정되어 있고 필드명이 컬럼명과 정확히 일치하지 않는 경우에만 "
                    + "사용된다는 점에 유의한다. 필드명 업데이트가 필요하지 않거나('Update Field Names'가 false인 경우) Record Writer는 사용되지 않으며, 대신 입력 FlowFile이 "
                    + "수정 없이 그대로 success 또는 failure로 라우팅된다.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .dependsOn(UPDATE_FIELD_NAMES, "true")
            .required(true)
            .build();
    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("hive3-query-timeout")
            .displayName("쿼리 타임아웃")
            .description("드라이버가 쿼리 실행을 기다리는 시간(초)을 설정한다. "
                    + "값이 0이면 타임아웃이 없음을 의미한다. 참고: 0이 아닌 값은 드라이버에 따라 지원되지 않을 수 있다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    static final PropertyDescriptor PARTITION_CLAUSE = new PropertyDescriptor.Builder()
            .name("hive3-partition-clause")
            .displayName("파티션 절")
            .description("대상 테이블의 파티션 컬럼에 해당하는 속성명 목록과 (선택적인) 데이터 타입을 콤마로 구분하여 지정한다. 간단히 말해, 테이블이 이미 "
                    + "파티션되어 있거나 파티션과 함께 생성될 예정이라면, 각 파티션명은 FlowFile의 속성이어야 하며 이 속성값에 나열되어야 한다. 이는 들어오는 모든 레코드가 "
                    + "동일한 파티션에 속하며 파티션 컬럼이 레코드의 필드가 아님을 전제로 한다. 이 필드를 지정하는 예를 들면, 상류에 PartitionRecord가 있고 "
                    + "'name'(string 타입)과 'age'(integer 타입) 두 개의 파티션 컬럼을 사용하는 경우, 이 속성을 'name string, age int'로 설정할 수 있다. 데이터 타입은 "
                    + "선택 사항이며, 파티션을 생성할 때 지정하지 않으면 기본적으로 string 타입이 된다. string이 아닌 기본(primitive) 타입의 경우, 기존 파티션 컬럼에 대한 "
                    + "데이터 타입을 지정해 두면 파티션 값을 해석하는 데 도움이 된다. 테이블이 이미 존재하는 경우에는 데이터 타입을 지정할 필요가 없으며 "
                    + "(지정하더라도 그 경우에는 무시된다). 테이블이 파티션되어 있다면 이 속성은 반드시 설정해야 하며, 테이블의 각 파티션 컬럼에 대응하는 속성이 있어야 한다. "
                    + "해당 속성들의 값이 파티션 값으로 사용되며, 그 결과 생성되는 output.path 속성값은 파일시스템 상의 파티션 위치를 나타낸다 "
                    + "(하류의 PutHDFS 같은 프로세서에서 사용하기 위함).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    // 초기화(init) 시점에 채워지는, 이 프로세서가 지원하는 속성 목록과 관계 집합(불변 컬렉션으로 노출)
    private List<PropertyDescriptor> propertyDescriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(RECORD_READER);
        props.add(HIVE_DBCP_SERVICE);
        props.add(TABLE_NAME);
        props.add(PARTITION_CLAUSE);
        props.add(CREATE_TABLE);
        props.add(TABLE_MANAGEMENT_STRATEGY);
        props.add(EXTERNAL_TABLE_LOCATION);
        props.add(TABLE_STORAGE_FORMAT);
        props.add(UPDATE_FIELD_NAMES);
        props.add(RECORD_WRITER_FACTORY);
        props.add(QUERY_TIMEOUT);

        propertyDescriptors = Collections.unmodifiableList(props);

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
     * 프로세서 설정 화면에서 속성값을 저장하기 전에 호출되는 커스텀 유효성 검증.
     * 개별 PropertyDescriptor의 validator만으로는 표현할 수 없는, 속성 간 상호 의존 규칙(예: A가 true면 B가 반드시 설정돼야 함)을 검사한다.
     * 단, Table Management Strategy가 '속성 기반(ATTRIBUTE_DRIVEN_TABLE)'인 경우에는 실제 값이 FlowFile 속성에서 런타임에 결정되므로
     * 여기서는 External Table Location 필수 여부를 미리 검증할 수 없어 건너뛴다(onTrigger에서 런타임에 재검증한다).
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));
        final boolean recordWriterFactorySet = validationContext.getProperty(RECORD_WRITER_FACTORY).isSet();
        final boolean createIfNotExists = validationContext.getProperty(CREATE_TABLE).getValue().equals(CREATE_IF_NOT_EXISTS.getValue());
        final boolean updateFieldNames = validationContext.getProperty(UPDATE_FIELD_NAMES).asBoolean();

        if (!recordWriterFactorySet && updateFieldNames) {
            validationResults.add(new ValidationResult.Builder().subject(RECORD_WRITER_FACTORY.getDisplayName())
                    .explanation("Record Writer must be set if 'Update Field Names' is true").valid(false).build());
        }
        final String tableManagementStrategy = validationContext.getProperty(TABLE_MANAGEMENT_STRATEGY).getValue();
        final boolean managedTable;
        if (!ATTRIBUTE_DRIVEN_TABLE.getValue().equals(tableManagementStrategy)) {
            managedTable = MANAGED_TABLE.getValue().equals(tableManagementStrategy);
            // 외부 테이블 설정의 유효성을 보장한다
            if (createIfNotExists && !managedTable && !validationContext.getProperty(EXTERNAL_TABLE_LOCATION).isSet()) {
                validationResults.add(new ValidationResult.Builder().subject(EXTERNAL_TABLE_LOCATION.getDisplayName())
                        .explanation("External Table Location must be set when Table Management Strategy is set to External").valid(false).build());
            }
        }
        return validationResults;
    }

    /**
     * FlowFile 1건을 처리하는 메인 로직. 처리 흐름은 다음과 같다.
     * 1) Record Reader로 입력 FlowFile의 스키마만 파악한다(레코드 본문은 아직 읽지 않음).
     * 2) Hive 커넥션을 얻어 {@link #checkAndUpdateTableSchema}를 호출, 테이블을 생성하거나 부족한 컬럼/파티션을 추가한다.
     * 3) 필드명 업데이트가 필요하다고 판단되면(반환된 OutputMetadataHolder가 null이 아니면),
     *    Record Reader/Writer를 이용해 FlowFile 본문을 다시 써서 필드명을 Hive 컬럼명과 맞춘다.
     * 4) 처리 결과에 따라 success 또는 failure 관계로 FlowFile을 전달한다.
     * <p>
     * IOException/SQLException은 재시도가 의미 없는 것으로 간주하여 failure로 라우팅하고,
     * DiscontinuedException은 세션 커밋 중단 등 처리를 계속할 수 없는 상황이므로 FlowFile을 큐에 그대로 남겨둔다(Relationship.SELF).
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory recordWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String partitionClauseString = context.getProperty(PARTITION_CLAUSE).evaluateAttributeExpressions(flowFile).getValue();
        List<String> partitionClauseElements = null;
        if (!StringUtils.isEmpty(partitionClauseString)) {
            partitionClauseElements = Arrays.stream(partitionClauseString.split(",")).filter(Objects::nonNull).map(String::trim).collect(Collectors.toList());
        }

        final ComponentLog log = getLogger();

        try {
            final RecordReader reader;

            try (final InputStream in = session.read(flowFile)) {
                // RecordReader 생성에 실패하면 failure로 라우팅해야 하므로, 일반적으로 retry로 처리되는
                // 다른 IOException들과는 별도로 이 예외를 구분해서 처리해야 한다
                try {
                    reader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
                } catch (Exception e) {
                    throw new RecordReaderFactoryException("Unable to create RecordReader", e);
                }
            } catch (RecordReaderFactoryException rrfe) {
                log.error(
                        "Failed to create {} for {} - routing to failure",
                        RecordReader.class.getSimpleName(), flowFile,
                        rrfe
                );
                // 위에서 예외를 감싸고 있으므로(wrapping) cause는 항상 존재해야 하지만,
                // 메시지가 없을 수도 있다. 이 경우 예외를 던진 클래스 이름을 로깅하여
                // 그런 상황을 처리한다.
                Throwable c = rrfe.getCause();
                if (c != null) {
                    session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
                } else {
                    session.putAttribute(flowFile, "record.error.message", rrfe.getClass().getCanonicalName() + " Thrown");
                }
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final RecordSchema recordSchema = reader.getSchema();

            final boolean createIfNotExists = context.getProperty(CREATE_TABLE).getValue().equals(CREATE_IF_NOT_EXISTS.getValue());
            final boolean updateFieldNames = context.getProperty(UPDATE_FIELD_NAMES).asBoolean();
            if (recordWriterFactory == null && updateFieldNames) {
                throw new ProcessException("Record Writer must be set if 'Update Field Names' is true");
            }
            final String tableManagementStrategy = context.getProperty(TABLE_MANAGEMENT_STRATEGY).getValue();
            final boolean managedTable;
            if (ATTRIBUTE_DRIVEN_TABLE.getValue().equals(tableManagementStrategy)) {
                String tableManagementStrategyAttribute = flowFile.getAttribute(TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE);
                if (MANAGED_TABLE.getValue().equalsIgnoreCase(tableManagementStrategyAttribute)) {
                    managedTable = true;
                } else if (EXTERNAL_TABLE.getValue().equalsIgnoreCase(tableManagementStrategyAttribute)) {
                    managedTable = false;
                } else {
                    log.error("The '{}' attribute either does not exist or has invalid value: {}. Must be one of (ignoring case): Managed, External. "
                                    + "Routing flowfile to failure",
                            TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE, tableManagementStrategyAttribute);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            } else {
                managedTable = MANAGED_TABLE.getValue().equals(tableManagementStrategy);
            }

            // 외부 테이블 설정의 유효성을 보장한다(customValidate에서는 속성 기반 전략의 경우 검증할 수 없었으므로 여기서 재확인)
            if (createIfNotExists && !managedTable && !context.getProperty(EXTERNAL_TABLE_LOCATION).isSet()) {
                throw new IOException("External Table Location must be set when Table Management Strategy is set to External");
            }
            final String externalTableLocation = managedTable ? null : context.getProperty(EXTERNAL_TABLE_LOCATION).evaluateAttributeExpressions(flowFile).getValue();
            if (!managedTable && StringUtils.isEmpty(externalTableLocation)) {
                log.error("External Table Location has invalid value: {}. Routing flowfile to failure", externalTableLocation);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final String storageFormat = context.getProperty(TABLE_STORAGE_FORMAT).getValue();
            final VendorHiveDBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(VendorHiveDBCPService.class);
            try (final Connection connection = dbcpService.getConnection()) {

                final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
                OutputMetadataHolder outputMetadataHolder = checkAndUpdateTableSchema(attributes, connection, recordSchema, tableName, partitionClauseElements,
                        createIfNotExists, externalTableLocation, storageFormat, updateFieldNames);
                if (outputMetadataHolder != null) {
                    // 출력 스키마가 변경되었다(즉 필드명이 업데이트되었다)는 뜻이므로, 이에 맞춰 FlowFile 본문을 다시 쓴다
                    try {
                        final FlowFile inputFlowFile = flowFile;
                        flowFile = session.write(flowFile, (in, out) -> {

                            // RecordReader 생성에 실패하면 failure로 라우팅해야 하므로, 일반적으로 retry로 처리되는
                            // 다른 IOException들과는 별도로 이 예외를 구분해서 처리해야 한다
                            final RecordReader recordReader;
                            final RecordSetWriter recordSetWriter;
                            try {
                                recordReader = recordReaderFactory.createRecordReader(inputFlowFile, in, getLogger());
                                recordSetWriter = recordWriterFactory.createWriter(getLogger(), outputMetadataHolder.getOutputSchema(), out, attributes);
                            } catch (Exception e) {
                                if (e instanceof IOException) {
                                    throw (IOException) e;
                                }
                                throw new IOException(new RecordReaderFactoryException("Unable to create RecordReader", e));
                            }

                            WriteResult writeResult = updateRecords(recordSchema, outputMetadataHolder, recordReader, recordSetWriter);
                            recordSetWriter.flush();
                            recordSetWriter.close();
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), recordSetWriter.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                        });
                    } catch (final Exception e) {
                        getLogger().error("Failed to process {}; will route to failure", flowFile, e);
                        // 위에서 예외를 감싸고 있으므로(wrapping) cause는 항상 존재해야 하지만,
                        // 메시지가 없을 수도 있다. 이 경우 예외를 던진 클래스 이름을 로깅하여
                        // 그런 상황을 처리한다.
                        Throwable c = e.getCause();
                        if (c != null) {
                            session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
                        } else {
                            session.putAttribute(flowFile, "record.error.message", e.getClass().getCanonicalName() + " Thrown");
                        }
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }

                }
                attributes.put(ATTR_OUTPUT_TABLE, tableName);
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().invokeRemoteProcess(flowFile, dbcpService.getConnectionURL());
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (IOException | SQLException e) {
            flowFile = session.putAttribute(flowFile, ATTR_OUTPUT_TABLE, tableName);
            log.error("Exception while processing {} - routing to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        } catch (DiscontinuedException e) {
            // 입력 FlowFile의 처리가 중단되었다. 큐에서 제거하지 않고 그대로 남겨둔다.
            getLogger().warn("Discontinued processing for {} due to {}", flowFile, e, e);
            session.transfer(flowFile, Relationship.SELF);
        } catch (Throwable t) {
            throw (t instanceof ProcessException) ? (ProcessException) t : new ProcessException(t);
        }
    }

    /**
     * 대상 테이블의 DDL을 조회/갱신하는 핵심 로직.
     * <ul>
     *     <li>테이블이 없고 createIfNotExists가 true이면 입력 레코드 스키마와 파티션 절로부터 CREATE TABLE 문을 생성해 실행한다.</li>
     *     <li>테이블이 이미 존재하면 {@code DESC FORMATTED} 결과를 파싱하여 기존 컬럼/파티션 컬럼/테이블 위치를 알아낸 뒤,
     *         입력 스키마에는 있지만 테이블에는 없는 컬럼을 ALTER TABLE ADD COLUMNS로 추가한다.</li>
     *     <li>파티션 절이 지정된 경우, FlowFile 속성값을 이용해 ALTER TABLE ADD IF NOT EXISTS PARTITION 문으로 파티션을 추가하고,
     *         출력 경로(output.path)를 해당 파티션의 실제 파일시스템 경로로 갱신한다.</li>
     *     <li>updateFieldNames가 true이면, 입력 레코드 필드명과 Hive 컬럼명을 대소문자 무시 비교로 매핑하여
     *         실제로 이름이 달라 다시 써야 하는 경우에만 매핑 정보를 담은 {@link OutputMetadataHolder}를 반환한다(불필요하면 null).</li>
     * </ul>
     * JDBC 커넥션 위에서 여러 DDL 문을 순차 실행하는 상태 기반(stateful) 로직이라 동시에 두 FlowFile이 같은 프로세서 인스턴스에서
     * 이 메서드를 함께 실행하면 DDL이 뒤섞일 위험이 있으므로 synchronized로 보호한다.
     * <p>
     * DESC FORMATTED의 출력 포맷(헤더/빈 줄/구분선)은 Hive 버전에 따라 미묘하게 달라질 수 있어, 이 메서드의 파싱 로직은
     * 각 구간(컬럼 목록, 파티션 정보, 위치 정보)의 헤더 문자열을 직접 찾아가며 한 줄씩 순회하는 방식으로 구현되어 있다.
     */
    private synchronized OutputMetadataHolder checkAndUpdateTableSchema(Map<String, String> attributes, final Connection conn, final RecordSchema schema,
                                                                        final String tableName, List<String> partitionClause, final boolean createIfNotExists,
                                                                        final String externalTableLocation, final String storageFormat, final boolean updateFieldNames) throws IOException {
        // 현재 테이블 메타데이터를 읽어 Reader의 스키마와 비교하고,
        // 스키마에는 있지만 테이블에는 없는 컬럼을 추가한다
        try (Statement s = conn.createStatement()) {
            // 테이블 존재 여부를 판단한다
            ResultSet tables = s.executeQuery("SHOW TABLES");
            List<String> tableNames = new ArrayList<>();
            String hiveTableName;
            while (tables.next() && StringUtils.isNotEmpty(hiveTableName = tables.getString(1))) {
                tableNames.add(hiveTableName);
            }

            List<String> columnsToAdd = new ArrayList<>();
            String outputPath;
            boolean tableCreated = false;
            if (!tableNames.contains(tableName) && createIfNotExists) {
                StringBuilder createTableStatement = new StringBuilder();
                for (RecordField recordField : schema.getFields()) {
                    String recordFieldName = recordField.getFieldName();
                    // 테이블에 존재하지 않는 필드이므로 추가한다
                    columnsToAdd.add("`" + recordFieldName + "` " + NiFiOrcUtils.getHiveTypeFromFieldType(recordField.getDataType(), true));
                    getLogger().debug("Adding column " + recordFieldName + " to table " + tableName);
                }

                // 파티션 절을 처리한다
                if (partitionClause == null) {
                    partitionClause = Collections.emptyList();
                }
                List<String> validatedPartitionClause = new ArrayList<>(partitionClause.size());
                for (String partition : partitionClause) {
                    String[] partitionInfo = partition.split(" ");
                    if (partitionInfo.length != 2) {
                        validatedPartitionClause.add("`" + partitionInfo[0] + "` string");
                    } else {
                        validatedPartitionClause.add("`" + partitionInfo[0] + "` " + partitionInfo[1]);
                    }
                }

                createTableStatement.append("CREATE ")
                        .append(externalTableLocation == null ? "" : "EXTERNAL ")
                        .append("TABLE IF NOT EXISTS `")
                        .append(tableName)
                        .append("` (")
                        .append(String.join(", ", columnsToAdd))
                        .append(") ")
                        .append(validatedPartitionClause.isEmpty() ? "" : "PARTITIONED BY (" + String.join(", ", validatedPartitionClause) + ") ")
                        .append("STORED AS ")
                        .append(storageFormat)
                        .append(externalTableLocation == null ? "" : " LOCATION '" + externalTableLocation + "'");

                String createTableSql = createTableStatement.toString();

                if (StringUtils.isNotEmpty(createTableSql)) {
                    // 테이블 생성을 실행한다
                    getLogger().info("Executing Hive DDL: " + createTableSql);
                    s.execute(createTableSql);
                }

                tableCreated = true;
            }

            // 테이블 정보(컬럼, 파티션, 위치 등)를 처리한다
            List<String> hiveColumns = new ArrayList<>();

            String describeTable = "DESC FORMATTED `" + tableName + "`";
            ResultSet tableInfo = s.executeQuery(describeTable);
            // 결과는 col_name, data_type, comment 3개 컬럼으로 구성된다. 첫 행이 헤더인지 확인하여 헤더면 건너뛰고, 아니면 컬럼명으로 추가한다
            tableInfo.next();
            String columnName = tableInfo.getString(1);
            if (StringUtils.isNotEmpty(columnName) && !columnName.startsWith("#")) {
                hiveColumns.add(columnName);
            }
            // 방금 읽은 값이 헤더였다면, 뒤이어 빈 줄이 있는지 확인하여 건너뛰고, 그렇지 않으면 컬럼명으로 추가한다
            if (columnName.startsWith("#")) {
                tableInfo.next();
                columnName = tableInfo.getString(1);
                if (StringUtils.isNotEmpty(columnName)) {
                    hiveColumns.add(columnName);
                }
            }

            // 모든 컬럼명을 수집한다
            while (tableInfo.next() && StringUtils.isNotEmpty(columnName = tableInfo.getString(1))) {
                hiveColumns.add(columnName);
            }

            // 모든 파티션 컬럼을 수집한다
            boolean moreRows = true;
            boolean headerFound = false;
            while (moreRows && !headerFound) {
                String line = tableInfo.getString(1);
                if ("# Partition Information".equals(line)) {
                    headerFound = true;
                } else if ("# Detailed Table Information".equals(line)) {
                    // 파티션이 없는 테이블이므로 headerFound를 false로 둔 채 루프를 빠져나간다
                    break;
                }
                moreRows = tableInfo.next();
            }

            List<String> partitionColumns = new ArrayList<>();
            List<String> partitionColumnsEqualsValueList = new ArrayList<>();
            List<String> partitionColumnsLocationList = new ArrayList<>();
            if (headerFound) {
                // 테이블이 파티션되어 있다면, 각 파티션 컬럼에 대해 partition=value 형태의 문자열을 구성한다
                String partitionColumnName;
                columnName = tableInfo.getString(1);
                if (StringUtils.isNotEmpty(columnName) && !columnName.startsWith("#")) {
                    partitionColumns.add(columnName);
                }
                // 방금 읽은 값이 헤더였다면, 뒤이어 빈 줄이 있는지 확인하여 건너뛰고, 그렇지 않으면 컬럼명으로 추가한다
                if (columnName.startsWith("#")) {
                    tableInfo.next();
                    columnName = tableInfo.getString(1);
                    if (StringUtils.isNotEmpty(columnName)) {
                        partitionColumns.add(columnName);
                    }
                }
                while (tableInfo.next() && StringUtils.isNotEmpty(partitionColumnName = tableInfo.getString(1))) {
                    partitionColumns.add(partitionColumnName);
                }

                final int partitionColumnsSize = partitionColumns.size();
                final int partitionClauseSize = (partitionClause == null) ? 0 : partitionClause.size();
                if (partitionClauseSize != partitionColumnsSize) {
                    throw new IOException("Found " + partitionColumnsSize + " partition columns but " + partitionClauseSize + " partition values were supplied");
                }

                for (int i = 0; i < partitionClauseSize; i++) {
                    String partitionName = partitionClause.get(i).split(" ")[0];
                    String partitionValue = attributes.get(partitionName);
                    if (StringUtils.isEmpty(partitionValue)) {
                        throw new IOException("No value found for partition value attribute '" + partitionName + "'");
                    }
                    if (!partitionColumns.contains(partitionName)) {
                        throw new IOException("Cannot add partition '" + partitionName + "' to existing table");
                    }
                    partitionColumnsEqualsValueList.add("`" + partitionName + "`='" + partitionValue + "'");
                    // 출력 경로용으로 따옴표 없는 버전도 추가해 둔다
                    partitionColumnsLocationList.add(partitionName + "=" + partitionValue);
                }
            }

            // 테이블 위치를 가져온다
            moreRows = true;
            headerFound = false;
            while (moreRows && !headerFound) {
                String line = tableInfo.getString(1);
                if (line.startsWith("Location:")) {
                    headerFound = true;
                    continue; // 두 번째 컬럼 값을 가져와야 하므로 여기서는 next()를 호출하지 않는다
                }
                moreRows = tableInfo.next();
            }
            String tableLocation = tableInfo.getString(2);

            String alterTableSql;
            // 테이블이 새로 생성된 것이 아니라면, 그에 맞게 ALTER한다
            if (!tableCreated) {
                StringBuilder alterTableStatement = new StringBuilder();
                // 새 컬럼을 처리한다
                for (RecordField recordField : schema.getFields()) {
                    String recordFieldName = recordField.getFieldName().toLowerCase();
                    if (!hiveColumns.contains(recordFieldName) && !partitionColumns.contains(recordFieldName)) {
                        // 테이블에 존재하지 않는(그리고 파티션 컬럼도 아닌) 필드이므로 추가한다
                        columnsToAdd.add("`" + recordFieldName + "` " + NiFiOrcUtils.getHiveTypeFromFieldType(recordField.getDataType(), true));
                        hiveColumns.add(recordFieldName);
                        getLogger().info("Adding column " + recordFieldName + " to table " + tableName);
                    }
                }

                if (!columnsToAdd.isEmpty()) {
                    alterTableStatement.append("ALTER TABLE `")
                            .append(tableName)
                            .append("` ADD COLUMNS (")
                            .append(String.join(", ", columnsToAdd))
                            .append(")");

                    alterTableSql = alterTableStatement.toString();
                    if (StringUtils.isNotEmpty(alterTableSql)) {
                        // 테이블 업데이트를 실행한다
                        getLogger().info("Executing Hive DDL: " + alterTableSql);
                        s.execute(alterTableSql);
                    }
                }
            }

            outputPath = tableLocation;

            // 새 파티션 값을 처리한다
            if (!partitionColumnsEqualsValueList.isEmpty()) {
                alterTableSql = "ALTER TABLE `" +
                        tableName +
                        "` ADD IF NOT EXISTS PARTITION (" +
                        String.join(", ", partitionColumnsEqualsValueList) +
                        ")";
                if (StringUtils.isNotEmpty(alterTableSql)) {
                    // 테이블 업데이트를 실행한다
                    getLogger().info("Executing Hive DDL: " + alterTableSql);
                    s.execute(alterTableSql);
                }
                // 파티션 값들의 HDFS 위치를 나타내는 속성값을 추가한다
                outputPath = tableLocation + "/" + String.join("/", partitionColumnsLocationList);
            }

            // 필드명을 업데이트하는 경우 새 RecordSchema를 반환하고, 그렇지 않으면 null을 반환한다
            OutputMetadataHolder outputMetadataHolder;
            if (updateFieldNames) {
                List<RecordField> inputRecordFields = schema.getFields();
                List<RecordField> outputRecordFields = new ArrayList<>();
                Map<String, String> fieldMap = new HashMap<>();
                boolean needsUpdating = false;

                for (RecordField inputRecordField : inputRecordFields) {
                    final String inputRecordFieldName = inputRecordField.getFieldName();
                    boolean found = false;
                    for (String hiveColumnName : hiveColumns) {
                        if (inputRecordFieldName.equalsIgnoreCase(hiveColumnName)) {
                            // 필드명이 컬럼명과 정확히 일치하지 않으면 플래그를 설정한다. 이 전체 플래그로
                            // 레코드를 다시 써야 하는지(true) 아닌지(false)를 판단한다
                            if (!inputRecordFieldName.equals(hiveColumnName)) {
                                needsUpdating = true;
                            }
                            fieldMap.put(inputRecordFieldName, hiveColumnName);
                            outputRecordFields.add(new RecordField(hiveColumnName, inputRecordField.getDataType(), inputRecordField.getDefaultValue(), inputRecordField.isNullable()));
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // 입력 필드가 Hive 테이블 컬럼이 아니었다면, 스키마에 원래 이름 그대로 다시 추가한다
                        fieldMap.put(inputRecordFieldName, inputRecordFieldName);
                    }
                }
                outputMetadataHolder = needsUpdating ? new OutputMetadataHolder(new SimpleRecordSchema(outputRecordFields), fieldMap)
                        : null;
            } else {
                outputMetadataHolder = null;
            }
            attributes.put(ATTR_OUTPUT_PATH, outputPath);
            return outputMetadataHolder;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * checkAndUpdateTableSchema에서 계산한 필드명 매핑({@link OutputMetadataHolder#getFieldMap()})을 이용해
     * Reader가 읽은 모든 레코드를 순회하면서, 입력 필드명 기준의 값을 Hive 컬럼명 기준의 새 레코드로 옮겨 담아 Writer로 출력한다.
     * 값 자체는 변경하지 않고 필드명만 교체하는 단순 재매핑(remap) 작업이다.
     */
    private synchronized WriteResult updateRecords(final RecordSchema inputRecordSchema, final OutputMetadataHolder outputMetadataHolder,
                                                   final RecordReader reader, final RecordSetWriter writer) throws IOException {
        try {
            writer.beginRecordSet();
            Record inputRecord;
            while ((inputRecord = reader.nextRecord()) != null) {
                List<RecordField> inputRecordFields = inputRecordSchema.getFields();
                Map<String, Object> outputRecordFields = new HashMap<>(inputRecordFields.size());
                // 입력 필드명의 값을 출력 필드명으로 복사한다
                for (Map.Entry<String, String> mapping : outputMetadataHolder.getFieldMap().entrySet()) {
                    outputRecordFields.put(mapping.getValue(), inputRecord.getValue(mapping.getKey()));
                }
                Record outputRecord = new MapRecord(outputMetadataHolder.getOutputSchema(), outputRecordFields);
                writer.write(outputRecord);
            }
            return writer.finishRecordSet();

        } catch (MalformedRecordException mre) {
            throw new IOException("Error reading records: " + mre.getMessage(), mre);
        }
    }

    /**
     * checkAndUpdateTableSchema의 계산 결과(필드명 업데이트가 필요한 경우)를 onTrigger로 전달하기 위한 단순 보관 객체.
     * outputSchema는 Hive 컬럼명으로 필드명이 교체된 새 RecordSchema이고,
     * fieldMap은 "입력 필드명 -> 출력 필드명" 매핑으로, updateRecords에서 레코드를 재작성할 때 사용된다.
     */
    private static class OutputMetadataHolder {
        private final RecordSchema outputSchema;
        private final Map<String, String> fieldMap;

        public OutputMetadataHolder(RecordSchema outputSchema, Map<String, String> fieldMap) {
            this.outputSchema = outputSchema;
            this.fieldMap = fieldMap;
        }

        public RecordSchema getOutputSchema() {
            return outputSchema;
        }

        public Map<String, String> getFieldMap() {
            return fieldMap;
        }
    }
}
