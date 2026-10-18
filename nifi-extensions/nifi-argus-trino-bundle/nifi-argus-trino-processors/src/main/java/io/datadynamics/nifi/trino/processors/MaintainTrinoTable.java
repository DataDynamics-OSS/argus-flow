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
package io.datadynamics.nifi.trino.processors;

import io.datadynamics.nifi.trino.processors.MaintenanceSql.Operation;
import io.datadynamics.nifi.trino.processors.MaintenanceSql.SyncMode;
import io.datadynamics.nifi.trino.processors.MaintenanceSql.TableFormat;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.EXECUTION_MODE;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.MAX_BACKGROUND_QUERIES;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.QUERY_TIMEOUT;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.TRINO_CONNECTION_SERVICE;

@Tags({"trino", "iceberg", "delta", "hive", "lakehouse", "maintenance", "optimize", "compaction", "vacuum", "snapshot"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SeeAlso({ExecuteTrinoStatement.class})
@CapabilityDescription("Trino 로 Iceberg/Delta Lake/Hive 테이블 유지보수(OPTIMIZE, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, VACUUM, "
        + "SYNC_PARTITION_METADATA, ANALYZE)를 실행한다. 입력 FlowFile 이 있으면 그 속성으로 대상 테이블을 정하고(Catalog/Schema/Table 에 EL), "
        + "없으면 스케줄마다 실행한다(Cron 스케줄 권장). 같은 테이블의 동시 유지보수를 피하려면 Concurrent Tasks=1 을 권장한다. "
        + "Iceberg expire_snapshots·remove_orphan_files, Delta vacuum 은 카탈로그의 최소 보존 기간(기본 7d)보다 짧은 Retention Threshold 를 거부하며, "
        + "이 오류는 그대로 failure 로 보고한다. OPTIMIZE 는 수십 분 걸릴 수 있으므로 Execution Mode=BACKGROUND 를 고려한다.")
@WritesAttributes({
        @WritesAttribute(attribute = TrinoAttributes.QUERY_ID, description = "Trino query id."),
        @WritesAttribute(attribute = TrinoAttributes.MAINTENANCE_SQL, description = "실행한 유지보수 SQL."),
        @WritesAttribute(attribute = TrinoAttributes.MAINTENANCE_OPERATION, description = "실행한 작업(예: OPTIMIZE)."),
        @WritesAttribute(attribute = TrinoAttributes.WALL_TIME_MS, description = "누적 wall 시간(ms)."),
        @WritesAttribute(attribute = TrinoAttributes.UPDATE_COUNT, description = "업데이트 수(문장이 돌려줄 때만. 예: ANALYZE)."),
        @WritesAttribute(attribute = TrinoAttributes.RESULT_ROW_COUNT, description = "결과 행 수(지표 행을 돌려주는 작업. 예: Iceberg optimize)."),
        @WritesAttribute(attribute = TrinoAttributes.ERROR_MESSAGE, description = "failure 로 보낼 때 오류 메시지."),
        @WritesAttribute(attribute = TrinoAttributes.ERROR_CODE, description = "failure 로 보낼 때 Trino 오류 코드.")
})
public class MaintainTrinoTable extends AbstractTrinoStatementProcessor {

    static final PropertyDescriptor TABLE_FORMAT = new PropertyDescriptor.Builder()
            .name("Table Format")
            .displayName("테이블 포맷")
            .description("대상 테이블의 포맷(카탈로그의 커넥터).")
            .required(true)
            .allowableValues(TableFormat.class)
            .build();

    static final PropertyDescriptor OPERATION = new PropertyDescriptor.Builder()
            .name("Operation")
            .displayName("작업")
            .description("실행할 유지보수 작업. 포맷과 맞지 않는 조합(예: HIVE + VACUUM)은 유효하지 않다.")
            .required(true)
            .allowableValues(Operation.class)
            .build();

    static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder()
            .name("Catalog")
            .displayName("카탈로그")
            .description("대상 Trino 카탈로그.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("Schema")
            .displayName("스키마")
            .description("대상 스키마.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table")
            .displayName("테이블")
            .description("대상 테이블.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor FILE_SIZE_THRESHOLD = new PropertyDescriptor.Builder()
            .name("File Size Threshold")
            .displayName("파일 크기 임계값")
            .description("OPTIMIZE 에서 이보다 작은 파일을 병합 대상으로 삼는다. Trino data size 형식(예: 128MB, 1GB).")
            .required(true)
            .defaultValue("128MB")
            .addValidator(StandardValidators.createRegexMatchingValidator(MaintenanceSql.DATA_SIZE))
            .dependsOn(OPERATION, Operation.OPTIMIZE)
            .build();

    static final PropertyDescriptor PARTITION_FILTER = new PropertyDescriptor.Builder()
            .name("Partition Filter")
            .displayName("파티션 필터")
            .description("OPTIMIZE 의 WHERE 절(선택, 예: event_date >= DATE '2026-10-01'). SQL 조각이 그대로 붙으므로 신뢰할 수 있는 값만 넣어야 한다"
                    + "(FlowFile 속성 EL 은 지원하지 않는다). 파티션 컬럼 조건만 쓸 수 있다.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(OPERATION, Operation.OPTIMIZE)
            .build();

    static final PropertyDescriptor RETENTION_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Retention Threshold")
            .displayName("보존 기간")
            .description("EXPIRE_SNAPSHOTS·REMOVE_ORPHAN_FILES·VACUUM 의 보존 기간. Trino duration 형식(예: 7d, 168h). "
                    + "카탈로그 최소 보존 기간(기본 7d)보다 짧으면 Trino 가 거부한다.")
            .required(true)
            .defaultValue("7d")
            .addValidator(StandardValidators.createRegexMatchingValidator(MaintenanceSql.DURATION))
            .dependsOn(OPERATION, Operation.EXPIRE_SNAPSHOTS, Operation.REMOVE_ORPHAN_FILES, Operation.VACUUM)
            .build();

    static final PropertyDescriptor SYNC_MODE = new PropertyDescriptor.Builder()
            .name("Sync Mode")
            .displayName("동기화 모드")
            .description("SYNC_PARTITION_METADATA 의 모드.")
            .required(true)
            .allowableValues(SyncMode.class)
            .defaultValue(SyncMode.ADD)
            .dependsOn(OPERATION, Operation.SYNC_PARTITION_METADATA)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            TRINO_CONNECTION_SERVICE, TABLE_FORMAT, OPERATION, CATALOG, SCHEMA, TABLE, FILE_SIZE_THRESHOLD, PARTITION_FILTER,
            RETENTION_THRESHOLD, SYNC_MODE, QUERY_TIMEOUT, EXECUTION_MODE, MAX_BACKGROUND_QUERIES);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        final TableFormat format = context.getProperty(TABLE_FORMAT).asAllowableValue(TableFormat.class);
        final Operation operation = context.getProperty(OPERATION).asAllowableValue(Operation.class);
        if (format != null && operation != null && !operation.supports(format)) {
            results.add(new ValidationResult.Builder()
                    .subject(OPERATION.getDisplayName())
                    .valid(false)
                    .explanation(format.getValue() + " 테이블은 " + operation.getValue() + " 를 지원하지 않는다")
                    .build());
        }
        return results;
    }

    @Override
    protected StatementRequest createRequest(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        final TableFormat format = context.getProperty(TABLE_FORMAT).asAllowableValue(TableFormat.class);
        final Operation operation = context.getProperty(OPERATION).asAllowableValue(Operation.class);
        // 의존 조건이 맞는 프로퍼티만 읽는다
        final boolean optimize = operation == Operation.OPTIMIZE;
        final boolean retention = operation == Operation.EXPIRE_SNAPSHOTS || operation == Operation.REMOVE_ORPHAN_FILES
                || operation == Operation.VACUUM;
        final boolean sync = operation == Operation.SYNC_PARTITION_METADATA;
        final String sql = MaintenanceSql.build(format, operation,
                context.getProperty(CATALOG).evaluateAttributeExpressions(flowFile).getValue(),
                context.getProperty(SCHEMA).evaluateAttributeExpressions(flowFile).getValue(),
                context.getProperty(TABLE).evaluateAttributeExpressions(flowFile).getValue(),
                optimize ? context.getProperty(FILE_SIZE_THRESHOLD).getValue() : null,
                optimize ? context.getProperty(PARTITION_FILTER).getValue() : null,
                retention ? context.getProperty(RETENTION_THRESHOLD).getValue() : null,
                sync ? context.getProperty(SYNC_MODE).asAllowableValue(SyncMode.class) : null);
        return new StatementRequest(sql, Map.of(
                TrinoAttributes.MAINTENANCE_SQL, sql,
                TrinoAttributes.MAINTENANCE_OPERATION, operation.getValue()));
    }
}
