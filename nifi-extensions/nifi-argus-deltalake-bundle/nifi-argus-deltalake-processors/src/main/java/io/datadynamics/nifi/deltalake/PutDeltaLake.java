/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.deltalake;

import io.datadynamics.nifi.deltalake.convert.CloseableIterators;
import io.datadynamics.nifi.deltalake.convert.DeltaTypeMapper;
import io.datadynamics.nifi.deltalake.convert.RecordColumnarBatch;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Tags({"delta", "deltalake", "delta lake", "put", "record", "sink", "lakehouse", "parquet"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Configured Record Reader로 FlowFile을 읽어 경로 기반 Delta Lake 테이블에 append한다. "
        + "Delta Kernel(Java)로 Parquet 데이터 파일을 쓰고 _delta_log에 커밋하며, 테이블이 없으면 레코드 스키마로 생성한다. "
        + "MVP: 원시 타입만 지원하고 파일시스템은 file://·hdfs:// 기본 지원(S3 등은 §10 참조).")
@WritesAttributes({
        @WritesAttribute(attribute = "deltalake.table.path", description = "쓰기 대상 Delta 테이블 경로."),
        @WritesAttribute(attribute = "deltalake.version", description = "이 append로 커밋된 테이블 버전."),
        @WritesAttribute(attribute = "record.count", description = "테이블에 기록된 레코드 수.")
})
@DynamicProperty(name = "Hadoop Configuration 속성명(예: fs.s3a.access.key)",
        value = "Hadoop Configuration 값",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
        description = "동적 프로퍼티는 DefaultEngine이 사용하는 Hadoop Configuration에 그대로 설정된다.")
public class PutDeltaLake extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("입력 FlowFile을 레코드로 파싱할 Record Reader 서비스.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor TABLE_PATH = new PropertyDescriptor.Builder()
            .name("table-path")
            .displayName("테이블 경로")
            .description("Delta 테이블의 스토리지 경로(예: file:///data/tbl, hdfs://ns/warehouse/tbl). "
                    + "_delta_log가 이 경로 하위에 관리된다.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor PARTITION_COLUMNS = new PropertyDescriptor.Builder()
            .name("partition-columns")
            .displayName("파티션 컬럼")
            .description("테이블 최초 생성 시 사용할 파티션 컬럼 목록(쉼표 구분). 기존 테이블에는 무시되고 "
                    + "테이블 메타데이터의 파티션 정의를 따른다.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor HADOOP_CONFIG_RESOURCES = new PropertyDescriptor.Builder()
            .name("hadoop-config-resources")
            .displayName("Hadoop 설정 리소스")
            .description("core-site.xml, hdfs-site.xml 등 Hadoop 설정 파일 경로 목록(쉼표 구분). "
                    + "HDFS 접속·자격증명 등을 여기서 로드한다.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Delta 테이블에 성공적으로 커밋된 FlowFile.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("파싱 또는 쓰기 실패로 커밋되지 못한 FlowFile.")
            .build();

    private static final String ENGINE_INFO = "Argus-Flow-PutDeltaLake";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(RECORD_READER, TABLE_PATH, PARTITION_COLUMNS, HADOOP_CONFIG_RESOURCES);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .description("Hadoop Configuration 속성 '" + propertyDescriptorName + "'로 설정된다.")
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String tablePath = context.getProperty(TABLE_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final List<String> requestedPartitionColumns = parseList(
                context.getProperty(PARTITION_COLUMNS).evaluateAttributeExpressions().getValue());

        try {
            final Configuration hadoopConf = buildHadoopConfiguration(context);
            final Engine engine = DefaultEngine.create(hadoopConf);
            final Table table = Table.forPath(engine, tablePath);

            // 1) 레코드 적재
            final List<Record> records = new ArrayList<>();
            final RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final RecordSchema recordSchema;
            try (InputStream in = session.read(flowFile);
                 RecordReader reader = factory.createRecordReader(flowFile, in, getLogger())) {
                recordSchema = reader.getSchema();
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    records.add(record);
                }
            }

            // 2) 트랜잭션 구성 (없으면 생성)
            TransactionBuilder builder;
            try {
                final Snapshot snapshot = table.getLatestSnapshot(engine);
                builder = table.createTransactionBuilder(engine, ENGINE_INFO, Operation.WRITE);
                // 기존 테이블: 스키마/파티션은 테이블 정의를 따른다.
                getLogger().debug("기존 Delta 테이블에 append: {} (v{})", tablePath, snapshot.getVersion());
            } catch (final TableNotFoundException notFound) {
                final StructType newSchema = DeltaTypeMapper.toDeltaSchema(recordSchema);
                TransactionBuilder create = table.createTransactionBuilder(engine, ENGINE_INFO, Operation.CREATE_TABLE)
                        .withSchema(engine, newSchema);
                if (!requestedPartitionColumns.isEmpty()) {
                    create = create.withPartitionColumns(engine, requestedPartitionColumns);
                }
                builder = create;
                getLogger().info("Delta 테이블 신규 생성: {} (파티션={})", tablePath, requestedPartitionColumns);
            }

            final Transaction txn = builder.build(engine);
            final Row txnState = txn.getTransactionState(engine);
            final StructType writeSchema = txn.getSchema(engine);
            final List<String> partitionColumns = txn.getPartitionColumns(engine);

            // 3) 파티션 값별로 그룹핑 후 데이터 파일 기록 → append 액션 수집
            final List<Row> appendActions = new ArrayList<>();
            if (!records.isEmpty()) {
                final Map<String, List<Record>> groups = new LinkedHashMap<>();
                final Map<String, Map<String, Literal>> groupPartitionValues = new LinkedHashMap<>();
                groupByPartition(records, partitionColumns, writeSchema, groups, groupPartitionValues);

                for (final Map.Entry<String, List<Record>> group : groups.entrySet()) {
                    final Map<String, Literal> partitionValues = groupPartitionValues.get(group.getKey());
                    writeGroup(engine, txnState, writeSchema, group.getValue(), partitionValues, appendActions);
                }
            }

            // 4) 커밋
            final long version;
            try (CloseableIterable<Row> actions =
                         CloseableIterable.inMemoryIterable(CloseableIterators.fromList(appendActions))) {
                final TransactionCommitResult result = txn.commit(engine, actions);
                version = result.getVersion();
            }

            FlowFile out = session.putAttribute(flowFile, "deltalake.table.path", tablePath);
            out = session.putAttribute(out, "deltalake.version", Long.toString(version));
            out = session.putAttribute(out, "record.count", Integer.toString(records.size()));
            session.getProvenanceReporter().send(out, tablePath);
            session.transfer(out, REL_SUCCESS);
            getLogger().info("Delta 테이블에 {} 레코드 커밋 완료: {} (v{})", records.size(), tablePath, version);

        } catch (final Exception e) {
            getLogger().error("Delta 테이블 쓰기 실패: {}", tablePath, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }

    /** 각 레코드를 파티션 컬럼 값 기준으로 그룹핑한다(unpartitioned면 단일 그룹). */
    private void groupByPartition(final List<Record> records,
                                  final List<String> partitionColumns,
                                  final StructType writeSchema,
                                  final Map<String, List<Record>> groups,
                                  final Map<String, Map<String, Literal>> groupPartitionValues) {
        for (final Record record : records) {
            final Map<String, Literal> partitionValues = new LinkedHashMap<>();
            final StringBuilder key = new StringBuilder();
            for (final String column : partitionColumns) {
                final DataType type = writeSchema.get(column).getDataType();
                final Object coerced = DeltaTypeMapper.coerce(type, record.getValue(column));
                partitionValues.put(column, DeltaTypeMapper.toLiteral(type, coerced));
                key.append(column).append('=').append(coerced == null ? " " : coerced).append('/');
            }
            final String groupKey = key.toString();
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(record);
            groupPartitionValues.putIfAbsent(groupKey, partitionValues);
        }
    }

    /** 한 파티션 그룹을 Parquet로 기록하고 append 액션들을 누적한다. */
    private void writeGroup(final Engine engine,
                            final Row txnState,
                            final StructType writeSchema,
                            final List<Record> groupRecords,
                            final Map<String, Literal> partitionValues,
                            final List<Row> appendActions) throws Exception {
        final int rowCount = groupRecords.size();
        final List<StructField> fields = writeSchema.fields();
        final List<Object[]> columns = new ArrayList<>(fields.size());
        for (final StructField field : fields) {
            final Object[] columnValues = new Object[rowCount];
            final DataType type = field.getDataType();
            final String name = field.getName();
            for (int row = 0; row < rowCount; row++) {
                columnValues[row] = DeltaTypeMapper.coerce(type, groupRecords.get(row).getValue(name));
            }
            columns.add(columnValues);
        }

        final ColumnarBatch batch = RecordColumnarBatch.of(writeSchema, columns, rowCount);
        final CloseableIterator<FilteredColumnarBatch> logicalData =
                CloseableIterators.singleton(new FilteredColumnarBatch(batch, Optional.empty()));

        final CloseableIterator<FilteredColumnarBatch> physicalData =
                Transaction.transformLogicalData(engine, txnState, logicalData, partitionValues);
        final DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);

        final CloseableIterator<DataFileStatus> dataFiles = engine.getParquetHandler()
                .writeParquetFiles(writeContext.getTargetDirectory(), physicalData, writeContext.getStatisticsColumns());

        try (CloseableIterator<Row> actions =
                     Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext)) {
            while (actions.hasNext()) {
                appendActions.add(actions.next());
            }
        }
    }

    private Configuration buildHadoopConfiguration(final ProcessContext context) {
        final Configuration configuration = new Configuration();
        if (context.getProperty(HADOOP_CONFIG_RESOURCES).isSet()) {
            for (final String resource : parseList(context.getProperty(HADOOP_CONFIG_RESOURCES)
                    .evaluateAttributeExpressions().getValue())) {
                configuration.addResource(new Path(resource));
            }
        }
        context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .forEach(pd -> configuration.set(pd.getName(),
                        context.getProperty(pd).evaluateAttributeExpressions().getValue()));
        return configuration;
    }

    private static List<String> parseList(final String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        final List<String> values = new ArrayList<>();
        for (final String token : Arrays.asList(csv.split(","))) {
            final String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return values;
    }
}
