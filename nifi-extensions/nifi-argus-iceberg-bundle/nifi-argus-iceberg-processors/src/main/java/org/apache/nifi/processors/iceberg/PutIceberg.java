/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-processors/src/main/java/org/apache/nifi/processors/iceberg/PutIceberg.java
 */
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
package org.apache.nifi.processors.iceberg;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.Tasks;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import org.apache.nifi.processors.iceberg.converter.IcebergRecordConverter;
import org.apache.nifi.processors.iceberg.writer.IcebergTaskWriterFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.nifi.processors.iceberg.IcebergUtils.getConfigurationFromFiles;
import static org.apache.nifi.processors.iceberg.IcebergUtils.getDynamicProperties;

/**
 * Record Reader로 파싱한 레코드를 Iceberg 테이블에 적재하는 프로세서.
 * 카탈로그 서비스를 통해 테이블을 로드하고, IcebergRecordConverter로 레코드를 변환한 뒤
 * TaskWriter로 데이터 파일을 생성하고, 재시도(backoff) 로직과 함께 커밋을 수행한다.
 * Kerberos 인증이 필요한 경우 상위 클래스인 AbstractIcebergProcessor가 이를 처리한다.
 */
@Tags({"iceberg", "put", "table", "store", "record", "parse", "orc", "parquet", "avro"})
@CapabilityDescription("이 프로세서는 Iceberg API를 사용하여 레코드를 파싱하고 Iceberg 테이블에 적재합니다. " +
        "들어오는 데이터 세트는 Record Reader 컨트롤러 서비스로 파싱된 후, 설정된 카탈로그 서비스와 제공된 테이블 정보를 이용하여 Iceberg 테이블에 적재됩니다. " +
        "대상 Iceberg 테이블은 이미 존재해야 하며 들어오는 레코드와 스키마가 일치해야 합니다. " +
        "즉 Record Reader의 스키마는 Iceberg 스키마의 모든 필드를 포함해야 하며, Iceberg 스키마에 존재하지 않는 추가 필드는 무시됩니다. " +
        "'small file problem'을 방지하려면 앞단에 MergeRecord 프로세서를 배치하는 것을 권장합니다.")
@DynamicProperty(
        name = "스냅샷 요약(summary)에 추가할 사용자 지정 키. 값은 반드시 'snapshot-property.' 접두사로 시작해야 합니다.",
        value = "스냅샷 요약에 추가할 사용자 지정 값.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "스냅샷 요약에 사용자 지정 키와 그에 대응하는 값을 항목으로 추가합니다. 키 형식은 'snapshot-property.custom-key' 이어야 합니다.")
@WritesAttributes({
        @WritesAttribute(attribute = "iceberg.record.count", description = "FlowFile에 포함된 레코드 수.")
})
public class PutIceberg extends AbstractIcebergProcessor {

    public static final String ICEBERG_RECORD_COUNT = "iceberg.record.count";
    public static final String ICEBERG_SNAPSHOT_SUMMARY_PREFIX = "snapshot-property.";
    public static final String ICEBERG_SNAPSHOT_SUMMARY_FLOWFILE_UUID = "nifi-flowfile-uuid";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("레코드 리더")
            .description("들어오는 데이터를 파싱하고 데이터의 스키마를 결정하는 데 사용할 컨트롤러 서비스를 지정합니다.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor CATALOG_NAMESPACE = new PropertyDescriptor.Builder()
            .name("catalog-namespace")
            .displayName("카탈로그 네임스페이스")
            .description("카탈로그의 네임스페이스.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("테이블 이름")
            .description("기록할 대상 Iceberg 테이블의 이름.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("unmatched-column-behavior")
            .displayName("일치하지 않는 컬럼 처리 방식")
            .description("들어오는 레코드에 데이터베이스 테이블의 모든 컬럼에 대응하는 필드 매핑이 없는 경우, 이 속성은 그 상황을 어떻게 처리할지 지정합니다.")
            .allowableValues(UnmatchedColumnBehavior.class)
            .defaultValue(UnmatchedColumnBehavior.FAIL_UNMATCHED_COLUMN.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor FILE_FORMAT = new PropertyDescriptor.Builder()
            .name("file-format")
            .displayName("파일 형식")
            .description("Iceberg 데이터 파일을 기록할 때 사용할 파일 형식." +
                    " 설정하지 않으면 'write.format.default' 테이블 속성이 사용되며, 기본값은 parquet입니다.")
            .allowableValues(
                    new AllowableValue("AVRO"),
                    new AllowableValue("PARQUET"),
                    new AllowableValue("ORC"))
            .build();

    static final PropertyDescriptor MAXIMUM_FILE_SIZE = new PropertyDescriptor.Builder()
            .name("maximum-file-size")
            .displayName("최대 파일 크기")
            .description("파일이 가질 수 있는 최대 크기이며, 이 크기를 초과하면 남은 데이터로 새 파일이 생성됩니다." +
                    " 설정하지 않으면 'write.target-file-size-bytes' 테이블 속성이 사용되며, 기본값은 512 MB입니다.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    static final PropertyDescriptor NUMBER_OF_COMMIT_RETRIES = new PropertyDescriptor.Builder()
            .name("number-of-commit-retries")
            .displayName("커밋 재시도 횟수")
            .description("커밋을 실패로 처리하기 전에 재시도할 횟수.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("10")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor MINIMUM_COMMIT_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("minimum-commit-wait-time")
            .displayName("최소 커밋 대기 시간")
            .description("커밋을 재시도하기 전에 대기할 최소 시간.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("100 ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAXIMUM_COMMIT_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("maximum-commit-wait-time")
            .displayName("최대 커밋 대기 시간")
            .description("커밋을 재시도하기 전에 대기할 최대 시간.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("2 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAXIMUM_COMMIT_DURATION = new PropertyDescriptor.Builder()
            .name("maximum-commit-duration")
            .displayName("최대 커밋 지속 시간")
            .description("커밋 재시도 전체에 대한 타임아웃 기간.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("데이터 적재가 성공적으로 완료된 후 FlowFile은 이 관계로 전달됩니다.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            RECORD_READER,
            CATALOG,
            CATALOG_NAMESPACE,
            TABLE_NAME,
            UNMATCHED_COLUMN_BEHAVIOR,
            FILE_FORMAT,
            MAXIMUM_FILE_SIZE,
            KERBEROS_USER_SERVICE,
            NUMBER_OF_COMMIT_RETRIES,
            MINIMUM_COMMIT_WAIT_TIME,
            MAXIMUM_COMMIT_WAIT_TIME,
            MAXIMUM_COMMIT_DURATION
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * 동적 속성을 정의한다. 키가 반드시 {@code snapshot-property.} 접두사로 시작해야 유효한 것으로 처리하며,
     * 접두사를 제거한 나머지 부분이 스냅샷 요약(summary)에 기록될 커스텀 키가 된다.
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator((subject, input, context) -> {
                    ValidationResult.Builder builder = new ValidationResult.Builder().subject(subject).input(input);
                    if (subject.startsWith(ICEBERG_SNAPSHOT_SUMMARY_PREFIX)) {
                        builder.valid(true);
                    } else {
                        builder.valid(false).explanation("Dynamic property key must begin with '" + ICEBERG_SNAPSHOT_SUMMARY_PREFIX + "'");
                    }
                    return builder.build();
                })
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    /**
     * 카탈로그 서비스가 활성화되어 있을 때, Hadoop 설정의 Kerberos 보안 활성화 여부와
     * Kerberos User Service 설정 여부가 서로 모순되지 않는지 검증한다.
     * 즉, Kerberos 보안이 켜져 있는데 Kerberos User Service가 없거나,
     * 반대로 보안이 꺼져 있는데 Kerberos User Service가 설정된 경우 검증 오류를 추가한다.
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>();
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        boolean catalogServiceEnabled = context.getControllerServiceLookup().isControllerServiceEnabled(catalogService);

        if (catalogServiceEnabled) {
            final boolean kerberosUserServiceIsSet = context.getProperty(KERBEROS_USER_SERVICE).isSet();
            final boolean securityEnabled = SecurityUtil.isSecurityEnabled(getConfigurationFromFiles(catalogService.getConfigFilePaths()));

            if (securityEnabled && !kerberosUserServiceIsSet) {
                problems.add(new ValidationResult.Builder()
                        .subject(KERBEROS_USER_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("'hadoop.security.authentication' is set to 'kerberos' in the hadoop configuration files but no KerberosUserService is configured.")
                        .build());
            }

            if (!securityEnabled && kerberosUserServiceIsSet) {
                problems.add(new ValidationResult.Builder()
                        .subject(KERBEROS_USER_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("KerberosUserService is configured but 'hadoop.security.authentication' is not set to 'kerberos' in the hadoop configuration files.")
                        .build());
            }
        }

        return problems;
    }

    /**
     * 실제 FlowFile 처리 로직. 카탈로그에서 테이블을 로드한 뒤, Record Reader로 레코드를 읽어
     * IcebergRecordConverter로 변환하고 TaskWriter를 통해 데이터 파일을 작성한다.
     * 쓰기가 완료되면 생성된 데이터 파일들을 테이블에 커밋(append)하고, 실패 시에는
     * 커밋되지 않은 데이터 파일을 정리(abort)한 뒤 FlowFile을 실패 관계로 전달한다.
     */
    @Override
    public void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        final long startNanos = System.nanoTime();
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String fileFormat = context.getProperty(FILE_FORMAT).getValue();
        final String maximumFileSize = context.getProperty(MAXIMUM_FILE_SIZE).evaluateAttributeExpressions(flowFile).getValue();

        Table table;

        try {
            table = loadTable(context, flowFile);
        } catch (Exception e) {
            if (!handleAuthErrors(e, session, context)) {
                getLogger().error("Failed to load table from catalog", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
            }
            return;
        }

        TaskWriter<org.apache.iceberg.data.Record> taskWriter = null;
        int recordCount = 0;

        try (final InputStream in = session.read(flowFile); final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
            final FileFormat format = getFileFormat(table.properties(), fileFormat);
            final IcebergTaskWriterFactory taskWriterFactory = new IcebergTaskWriterFactory(table, flowFile.getId(), format, maximumFileSize);
            taskWriter = taskWriterFactory.create();
            final UnmatchedColumnBehavior unmatchedColumnBehavior =
                    UnmatchedColumnBehavior.valueOf(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

            final IcebergRecordConverter recordConverter = new IcebergRecordConverter(table.schema(), reader.getSchema(), format, unmatchedColumnBehavior, getLogger());

            Record record;
            while ((record = reader.nextRecord()) != null) {
                taskWriter.write(recordConverter.convert(record));
                recordCount++;
            }

            final WriteResult result = taskWriter.complete();
            appendDataFiles(context, flowFile, table, result);
        } catch (Exception e) {
            if (!handleAuthErrors(e, session, context)) {
                getLogger().error("Exception occurred while writing Iceberg records", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
            }

            try {
                if (taskWriter != null) {
                    abort(taskWriter.dataFiles(), table);
                }
            } catch (Exception ex) {
                getLogger().warn("Failed to abort uncommitted data files", ex);
            }
            return;
        }

        flowFile = session.putAttribute(flowFile, ICEBERG_RECORD_COUNT, String.valueOf(recordCount));
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, table.location(), transferMillis);
        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * 프로퍼티 컨텍스트에 제공된 값을 이용하여 카탈로그 서비스로부터 테이블을 로드한다.
     *
     * @param context {@link Catalog}와 {@link Table}에 필요한 사용자 제공 정보를 담고 있다
     * @return 로드된 테이블
     */
    private Table loadTable(final PropertyContext context, final FlowFile flowFile) {
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        final String catalogNamespace = context.getProperty(CATALOG_NAMESPACE).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        final IcebergCatalogFactory catalogFactory = new IcebergCatalogFactory(catalogService);
        final Catalog catalog = catalogFactory.create();

        final Namespace namespace = Namespace.of(catalogNamespace);
        final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

        return catalog.loadTable(tableIdentifier);
    }

    /**
     * 아직 커밋되지 않은 데이터 파일들을 주어진 {@link Table}에 추가(append)한다.
     * 커밋 재시도 횟수, 최소/최대 대기 시간, 전체 타임아웃 기간을 이용해 지수 백오프(exponential backoff) 방식으로
     * {@link CommitFailedException} 발생 시에만 재시도한다.
     *
     * @param context 프로세서 컨텍스트
     * @param table   추가 대상 테이블
     * @param result  {@link TaskWriter}가 생성한 데이터 파일들
     */
    void appendDataFiles(ProcessContext context, FlowFile flowFile, Table table, WriteResult result) {
        final int numberOfCommitRetries = context.getProperty(NUMBER_OF_COMMIT_RETRIES).evaluateAttributeExpressions(flowFile).asInteger();
        final long minimumCommitWaitTime = context.getProperty(MINIMUM_COMMIT_WAIT_TIME).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maximumCommitWaitTime = context.getProperty(MAXIMUM_COMMIT_WAIT_TIME).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maximumCommitDuration = context.getProperty(MAXIMUM_COMMIT_DURATION).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);

        final AppendFiles appender = table.newAppend();
        Arrays.stream(result.dataFiles()).forEach(appender::appendFile);

        addSnapshotSummaryProperties(context, appender, flowFile);

        Tasks.foreach(appender)
                .exponentialBackoff(minimumCommitWaitTime, maximumCommitWaitTime, maximumCommitDuration, 2.0)
                .retry(numberOfCommitRetries)
                .onlyRetryOn(CommitFailedException.class)
                .run(PendingUpdate::commit);
    }

    /**
     * FlowFile의 uuid와 동적 속성으로 제공된 추가 항목들을 스냅샷 요약(summary)에 추가한다.
     *
     * @param context  프로세서 컨텍스트
     * @param appender 스냅샷 요약을 설정할 테이블 appender
     * @param flowFile uuid를 가져올 FlowFile
     */
    private void addSnapshotSummaryProperties(ProcessContext context, AppendFiles appender, FlowFile flowFile) {
        appender.set(ICEBERG_SNAPSHOT_SUMMARY_FLOWFILE_UUID, flowFile.getAttribute(CoreAttributes.UUID.key()));

        for (Map.Entry<String, String> dynamicProperty : getDynamicProperties(context, flowFile).entrySet()) {
            String key = dynamicProperty.getKey().substring(ICEBERG_SNAPSHOT_SUMMARY_PREFIX.length());
            appender.set(key, dynamicProperty.getValue());
        }
    }

    /**
     * 요청된 값과 테이블 설정을 기반으로 실제 사용할 쓰기 파일 형식을 결정한다.
     * 프로세서에 값이 지정되지 않은 경우 테이블 속성의 기본 파일 형식을 사용한다.
     *
     * @param tableProperties 테이블 속성
     * @param fileFormat      프로세서에서 요청한 파일 형식
     * @return 사용할 파일 형식
     */
    private FileFormat getFileFormat(Map<String, String> tableProperties, String fileFormat) {
        final String fileFormatName = fileFormat != null ? fileFormat : tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(fileFormatName.toUpperCase(Locale.ENGLISH));
    }

    /**
     * 테이블에 아직 커밋되지 않은, 이미 작성이 완료된 데이터 파일들을 삭제한다.
     * 쓰기 또는 커밋 과정에서 오류가 발생했을 때 남은 파일을 정리하기 위해 사용된다.
     *
     * @param dataFiles 태스크 라이터가 생성한 파일들
     * @param table     대상 테이블
     */
    void abort(DataFile[] dataFiles, Table table) {
        Tasks.foreach(dataFiles)
                .retry(3)
                .run(file -> table.io().deleteFile(file.path().toString()));
    }
}
