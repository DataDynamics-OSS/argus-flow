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
package io.datadynamics.nifi.processors.hive;

import io.datadynamics.nifi.processors.hive.compaction.CompactionPhase;
import io.datadynamics.nifi.processors.hive.compaction.CompactionRecord;
import io.datadynamics.nifi.processors.hive.compaction.CompactionRequests;
import io.datadynamics.nifi.processors.hive.compaction.CompactionSql;
import io.datadynamics.nifi.processors.hive.compaction.CompactionType;
import io.datadynamics.nifi.processors.hive.compaction.CompletionState;
import io.datadynamics.nifi.processors.hive.compaction.HiveIdentifiers;
import io.datadynamics.nifi.processors.hive.compaction.HiveTableInfo;
import io.datadynamics.nifi.processors.hive.compaction.PartitionSpec;
import io.datadynamics.nifi.processors.hive.compaction.ShowCompactionsParser;
import io.datadynamics.nifi.processors.hive.compaction.TableTypeHandling;
import io.datadynamics.nifi.services.api.hive.Hive3DBCPService;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * HiveServer2 로 Hive 테이블(파티션) compaction 을 요청하고, 끝날 때까지 FlowFile 속성으로 상태를 추적한다.
 * <p>
 * {@link AbstractHive3QLProcessor} 가 아니라 {@link AbstractProcessor} 를 상속한다. 그 기반 클래스는 FlowFile 내용의
 * HiveQL 을 읽고 {@code hiveql.args.N.*} 파라미터를 바인딩하며 hive-exec 파서로 입출력 테이블을 뽑는 프로세서용이다.
 * 이 프로세서는 프로퍼티로 SQL 을 직접 만들고, 한 번에 FlowFile 하나를 처리한 뒤 진행 중이면 {@code in progress} 로
 * 돌려보내는 상태 없는 루프라서 그 기능이 필요 없다. 같은 이름·의미의 {@code hive3-dbcp-service}, {@code hive3-query-timeout}
 * 프로퍼티는 그 클래스의 정의를 그대로 재사용한다.
 * <p>
 * 진행 상황(compaction id, 요청 시각, 사전 행 수)을 FlowFile 속성에 두므로 NiFi 스레드를 붙잡지 않고, NiFi 를 재시작해도
 * 이어서 추적한다. 폴링 간격은 프로세서의 Penalty Duration 이다.
 */
@Tags({"hive", "acid", "compaction", "compact", "concatenate", "orc", "maintenance", "small files", "delta"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SeeAlso({PutHive3QL.class, PutHive3Streaming.class})
@CapabilityDescription("HiveServer2 로 Hive 테이블 또는 파티션의 compaction 을 실행한다. ACID 테이블(full ACID, insert-only)은 "
        + "ALTER TABLE ... COMPACT 'major'|'minor' 를 비동기로 요청하고(AND WAIT 를 쓰지 않는다) SHOW COMPACTIONS 로 상태를 추적하며, "
        + "Non-ACID ORC managed 테이블은 ALTER TABLE ... CONCATENATE 로 ORC stripe 를 병합한다(동기). "
        + "진행 중이면 FlowFile 을 penalize 해서 'in progress' 로 보내므로, 'in progress' 를 반드시 이 프로세서 자신에게 다시 연결해야 한다(자기 루프). "
        + "폴링 간격은 프로세서 Settings 의 Penalty Duration 이다(예: 1 min). 진행 상황은 FlowFile 속성(hive.compaction.*)에 있어 "
        + "NiFi 를 재시작해도 이어서 추적한다. 선택적으로 compaction 전후 COUNT(*)(hive.compute.query.using.stats=false)를 비교해 "
        + "다르면 'count mismatch' 로 보낸다. 운영 주의: 적재가 끝난 파티션(보통 D-1 이전)만 대상으로 한다(CONCATENATE 는 배타 lock 을 잡는다). "
        + "compaction 완료 뒤에도 Cleaner 가 옛 디렉토리를 지우기 전까지는 파일이 남는다(ready for cleaning). "
        + "자동 compaction(Initiator)과 겹칠 수 있으므로 수동으로만 관리할 테이블에는 TBLPROPERTIES no_auto_compaction=true 를 권장한다. "
        + "요청이 'initiated' 에 머무르면 Hive 서버의 hive.compactor.initiator.on·hive.compactor.worker.threads 설정을 확인한다.")
@ReadsAttributes({
        @ReadsAttribute(attribute = CompactHive3Table.ATTR_ID, description = "있으면 새로 요청하지 않고 이 id 의 상태만 확인한다(in progress 루프, 재시작 이어받기)."),
        @ReadsAttribute(attribute = CompactHive3Table.ATTR_REQUESTED_AT, description = "타임아웃 계산의 기준 시각."),
        @ReadsAttribute(attribute = CompactHive3Table.ATTR_TABLE, description = "상태 확인 뒤 사후 행 수를 셀 대상 테이블."),
        @ReadsAttribute(attribute = CompactHive3Table.ATTR_PARTITION, description = "상태 확인 뒤 사후 행 수를 셀 대상 파티션."),
        @ReadsAttribute(attribute = CompactHive3Table.ATTR_ROWCOUNT_BEFORE, description = "사후 행 수와 비교할 사전 행 수.")
})
@WritesAttributes({
        @WritesAttribute(attribute = CompactHive3Table.ATTR_ID, description = "Hive compaction id (ACID 테이블만)."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_TYPE, description = "MAJOR, MINOR 또는 CONCATENATE."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_STATE, description = "마지막으로 확인한 SHOW COMPACTIONS 상태(예: initiated, working, ready for cleaning, succeeded). "
                + "CONCATENATE 는 succeeded."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_REQUESTED_AT, description = "요청 시각(epoch ms)."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_DURATION_MS, description = "요청부터 완료를 확인할 때까지 걸린 시간(ms). 폴링 간격만큼 오차가 있다."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_ROWCOUNT_BEFORE, description = "사전 행 수 (Validate Row Count=true)."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_ROWCOUNT_AFTER, description = "사후 행 수 (Validate Row Count=true)."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_TABLE, description = "대상 테이블(db.table)."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_PARTITION, description = "대상 파티션(정규화한 Partition Spec, 예: dt='2026-10-01'). 테이블 전체면 빈 값."),
        @WritesAttribute(attribute = CompactHive3Table.ATTR_ERROR, description = "failure·count mismatch 로 보낼 때 이유.")
})
public class CompactHive3Table extends AbstractProcessor {

    static final String ATTR_ID = "hive.compaction.id";
    static final String ATTR_TYPE = "hive.compaction.type";
    static final String ATTR_STATE = "hive.compaction.state";
    static final String ATTR_REQUESTED_AT = "hive.compaction.requested.at";
    static final String ATTR_DURATION_MS = "hive.compaction.duration.ms";
    static final String ATTR_ROWCOUNT_BEFORE = "hive.compaction.rowcount.before";
    static final String ATTR_ROWCOUNT_AFTER = "hive.compaction.rowcount.after";
    static final String ATTR_TABLE = "hive.compaction.table";
    static final String ATTR_PARTITION = "hive.compaction.partition";
    static final String ATTR_ERROR = "hive.compaction.error";

    static final String TYPE_CONCATENATE = "CONCATENATE";

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("compaction 이 완료됐고 사전·사후 행 수가 같다(또는 행 수 검증을 끈 경우).")
            .build();
    static final Relationship REL_IN_PROGRESS = new Relationship.Builder()
            .name("in progress")
            .description("Hive 에서 compaction 이 진행 중이다. FlowFile 은 penalize 된다(=폴링 간격). 반드시 이 프로세서 자신에게 다시 연결해야 한다.")
            .build();
    static final Relationship REL_COUNT_MISMATCH = new Relationship.Builder()
            .name("count mismatch")
            .description("compaction 은 완료됐지만 사전·사후 행 수가 다르다. 알림으로 연결할 것을 권장한다.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("테이블 판별, 요청, 상태 확인, 타임아웃, 행 수 검증 중 실패. 이유는 hive.compaction.error 속성에 있다.")
            .build();

    static final PropertyDescriptor HIVE_DBCP_SERVICE = AbstractHive3QLProcessor.HIVE_DBCP_SERVICE;

    static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("Database")
            .displayName("데이터베이스")
            .description("대상 Hive 데이터베이스. 영문자·숫자·밑줄만 쓸 수 있다.")
            .required(true)
            .defaultValue("default")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table")
            .displayName("테이블")
            .description("대상 Hive 테이블. 영문자·숫자·밑줄만 쓸 수 있다.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor PARTITION_SPEC = new PropertyDescriptor.Builder()
            .name("Partition Spec")
            .displayName("파티션 지정")
            .description("대상 파티션. 형식: dt='2026-10-01',hour='03' (값은 작은따옴표로 감싸고, 값 안의 작은따옴표는 \\' 로 쓴다). "
                    + "파티션 테이블이면 모든 파티션 키를 지정해야 하고, 키가 테이블의 파티션 컬럼과 다르면 failure 로 보낸다. "
                    + "비파티션 테이블이면 비워 둔다.")
            .required(false)
            .addValidator(new PartitionSpecValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor COMPACTION_TYPE = new PropertyDescriptor.Builder()
            .name("Compaction Type")
            .displayName("Compaction 종류")
            .description("ACID 테이블에 요청할 compaction 종류. Non-ACID ORC 테이블(CONCATENATE)에는 적용하지 않는다.")
            .required(true)
            .allowableValues(CompactionType.class)
            .defaultValue(CompactionType.MAJOR)
            .build();

    static final PropertyDescriptor TABLE_TYPE_HANDLING = new PropertyDescriptor.Builder()
            .name("Table Type Handling")
            .displayName("테이블 종류 처리")
            .description("Non-ACID 테이블을 어떻게 다룰지 정한다.")
            .required(true)
            .allowableValues(TableTypeHandling.class)
            .defaultValue(TableTypeHandling.ACID_AND_NON_ACID_ORC)
            .build();

    static final PropertyDescriptor VALIDATE_ROW_COUNT = new PropertyDescriptor.Builder()
            .name("Validate Row Count")
            .displayName("행 수 검증")
            .description("compaction 전후로 SELECT COUNT(*) (hive.compute.query.using.stats=false)를 실행해 비교한다. "
                    + "다르면 'count mismatch' 로 보낸다. 대상 파티션을 한 번씩 더 읽으므로 큰 파티션에서는 시간이 걸린다.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor COMPLETION_STATE = new PropertyDescriptor.Builder()
            .name("Completion State")
            .displayName("완료 기준 상태")
            .description("ACID compaction 을 완료로 볼 SHOW COMPACTIONS 상태.")
            .required(true)
            .allowableValues(CompletionState.class)
            .defaultValue(CompletionState.READY_FOR_CLEANING)
            .build();

    static final PropertyDescriptor INITIATION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Initiation Timeout")
            .displayName("시작 대기 타임아웃")
            .description("요청이 'initiated'(Worker 대기) 상태로 머무를 수 있는 최대 시간(요청 시각 기준). 넘으면 failure 로 보내며, "
                    + "Hive 서버의 hive.compactor.initiator.on·hive.compactor.worker.threads 확인을 안내한다.")
            .required(true)
            .defaultValue("30 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor COMPACTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Compaction Timeout")
            .displayName("Compaction 타임아웃")
            .description("요청부터 완료까지 최대 시간. 넘으면 failure 로 보낸다. 요청은 Hive 에 남아 계속 진행될 수 있다(id 는 hive.compaction.id).")
            .required(true)
            .defaultValue("6 hours")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = AbstractHive3QLProcessor.QUERY_TIMEOUT;

    private static final List<PropertyDescriptor> PROPERTIES = List.of(HIVE_DBCP_SERVICE, DATABASE, TABLE, PARTITION_SPEC,
            COMPACTION_TYPE, TABLE_TYPE_HANDLING, VALIDATE_ROW_COUNT, COMPLETION_STATE, INITIATION_TIMEOUT, COMPACTION_TIMEOUT,
            QUERY_TIMEOUT);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_IN_PROGRESS, REL_COUNT_MISMATCH, REL_FAILURE);

    private volatile Clock clock = Clock.systemUTC();

    /**
     * 테스트용 시계 교체.
     */
    void setClock(final Clock clock) {
        this.clock = clock;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final Hive3DBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(Hive3DBCPService.class);
        final Connection connection;
        try {
            connection = dbcpService.getConnection(flowFile.getAttributes());
        } catch (final ProcessException e) {
            // 연결 풀 문제는 FlowFile 탓이 아니므로 큐에 되돌리고 잠시 쉰다(진행 중 추적 속성은 그대로 남는다)
            getLogger().error("Hive 커넥션을 얻지 못해 {} 를 큐로 되돌린다", flowFile, e);
            session.rollback(true);
            context.yield();
            return;
        }
        final Job job = new Job(context, session, flowFile, dbcpService);
        try (connection) {
            final String existingId = flowFile.getAttribute(ATTR_ID);
            if (existingId != null && !existingId.isBlank()) {
                job.checkStatus(connection, existingId.trim());
            } else {
                job.start(connection);
            }
        } catch (final JobFailure e) {
            job.fail(e.getMessage(), e.getCause());
        } catch (final SQLException e) {
            job.fail("Hive 쿼리 실패: " + e.getMessage(), e);
        } catch (final RuntimeException e) {
            job.fail("처리 중 오류: " + e, e);
        }
    }

    /**
     * failure 로 보낼 이유.
     */
    private static final class JobFailure extends Exception {
        JobFailure(final String message) {
            super(message);
        }

        JobFailure(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * compaction 대상.
     */
    private record Target(String database, String table, PartitionSpec spec) {
        String qualified() {
            return database + "." + table;
        }
    }

    /**
     * FlowFile 하나의 처리.
     */
    private final class Job {
        private final ProcessContext context;
        private final ProcessSession session;
        private final Hive3DBCPService dbcpService;
        private final Map<String, String> attributes = new HashMap<>();
        private final int queryTimeout;
        private final boolean validateRowCount;
        private FlowFile flowFile;

        Job(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Hive3DBCPService dbcpService) {
            this.context = context;
            this.session = session;
            this.flowFile = flowFile;
            this.dbcpService = dbcpService;
            final Integer timeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(flowFile).asInteger();
            this.queryTimeout = timeout == null ? 0 : timeout;
            this.validateRowCount = context.getProperty(VALIDATE_ROW_COUNT).asBoolean();
        }

        // ---- ①~③ 새 요청 ----

        void start(final Connection connection) throws JobFailure, SQLException {
            final Target target = targetFromProperties();
            attributes.put(ATTR_TABLE, target.qualified());
            attributes.put(ATTR_PARTITION, target.spec().toCanonicalString());

            // ① 판별
            final HiveTableInfo info = describe(connection, target);
            checkPartition(target, info);
            final String type = resolveType(info);
            attributes.put(ATTR_TYPE, type);

            // ② 사전 행 수
            if (validateRowCount) {
                attributes.put(ATTR_ROWCOUNT_BEFORE, Long.toString(countRows(connection, target)));
            }

            // ③ 요청
            final long requestedAt = clock.millis();
            attributes.put(ATTR_REQUESTED_AT, Long.toString(requestedAt));
            if (TYPE_CONCATENATE.equals(type)) {
                final String sql = CompactionSql.concatenate(target.database(), target.table(), target.spec());
                execute(connection, sql);
                session.getProvenanceReporter().invokeRemoteProcess(flowFile, connectionUrl(), sql);
                attributes.put(ATTR_STATE, "succeeded");
                complete(connection, target, requestedAt);
                return;
            }

            final CompactionType compactionType = CompactionType.valueOf(type);
            final long maxIdBefore = CompactionRequests.maxId(showCompactions(connection), target.database(), target.table(), target.spec());
            final String sql = CompactionSql.compact(target.database(), target.table(), target.spec(), compactionType);
            OptionalLong existingId = OptionalLong.empty();
            try {
                execute(connection, sql);
            } catch (final SQLException e) {
                existingId = CompactionRequests.parseAlreadyScheduledId(e.getMessage());
                if (existingId.isEmpty()) {
                    throw new JobFailure("compaction 요청 실패(" + sql + "): " + e.getMessage(), e);
                }
                getLogger().info("{} 에 이미 compaction(id={})이 예약돼 있어 그 요청을 추적한다: {}", target.qualified(), existingId.getAsLong(), e.getMessage());
            }
            session.getProvenanceReporter().invokeRemoteProcess(flowFile, connectionUrl(), sql);

            final List<CompactionRecord> after = showCompactions(connection);
            final Optional<CompactionRecord> requested = existingId.isPresent()
                    ? CompactionRequests.findById(after, existingId.getAsLong())
                    : CompactionRequests.findRequested(after, maxIdBefore, target.database(), target.table(), target.spec());
            if (requested.isEmpty()) {
                throw new JobFailure("compaction 요청(" + sql + ") 뒤 SHOW COMPACTIONS 에서 " + target.qualified()
                        + (target.spec().isEmpty() ? "" : " [" + target.spec() + "]") + " 의 새 compaction 을 찾지 못했다"
                        + (existingId.isPresent() ? "(id=" + existingId.getAsLong() + ")" : "(요청 전 최대 id=" + maxIdBefore + ")"));
            }
            attributes.put(ATTR_ID, Long.toString(requested.get().id()));
            evaluate(connection, target, requested.get(), requestedAt);
        }

        // ④ 상태 확인 (in progress 루프, 재시작 이어받기)

        void checkStatus(final Connection connection, final String idText) throws JobFailure, SQLException {
            final long id;
            try {
                id = Long.parseLong(idText);
            } catch (final NumberFormatException e) {
                throw new JobFailure(ATTR_ID + " 속성 값 '" + idText + "' 이(가) 숫자가 아니다");
            }
            final long requestedAt = longAttribute(ATTR_REQUESTED_AT);
            final Target target = targetFromAttributes();
            final Optional<CompactionRecord> record = CompactionRequests.findById(showCompactions(connection), id);
            if (record.isEmpty()) {
                throw new JobFailure("SHOW COMPACTIONS 에서 compaction id " + id + " 를 찾지 못했다. 이력 보존 설정"
                        + "(hive.compactor.history.retention.*)에 따라 이미 지워졌을 수 있다");
            }
            evaluate(connection, target, record.get(), requestedAt);
        }

        private void evaluate(final Connection connection, final Target target, final CompactionRecord record, final long requestedAt)
                throws JobFailure, SQLException {
            attributes.put(ATTR_STATE, record.state());
            final CompactionPhase phase = CompactionPhase.of(record.state());
            final CompletionState completionState = context.getProperty(COMPLETION_STATE).asAllowableValue(CompletionState.class);
            if (phase.isComplete(completionState)) {
                complete(connection, target, requestedAt);
                return;
            }
            if (phase == CompactionPhase.FAILED) {
                throw new JobFailure("compaction id " + record.id() + " 이(가) '" + record.state() + "' 상태로 끝났다"
                        + (record.errorMessage() == null ? "" : ": " + record.errorMessage())
                        + ". Hive Metastore·HiveServer2 로그(compactor Worker)를 확인한다");
            }
            final long elapsed = clock.millis() - requestedAt;
            final long initiationTimeout = context.getProperty(INITIATION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
            if (phase == CompactionPhase.INITIATED && elapsed > initiationTimeout) {
                throw new JobFailure("compaction id " + record.id() + " 이(가) " + elapsed + " ms 동안 'initiated' 에 머물렀다"
                        + "(Initiation Timeout " + initiationTimeout + " ms). Worker 가 요청을 가져가지 않는다. Hive 서버(Metastore/HiveServer2)의 "
                        + "hive.compactor.initiator.on=true, hive.compactor.worker.threads>0 설정과 Worker 큐 적체를 확인한다. "
                        + "요청은 Hive 에 남아 있다");
            }
            final long compactionTimeout = context.getProperty(COMPACTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
            if (elapsed > compactionTimeout) {
                throw new JobFailure("compaction id " + record.id() + " 이(가) " + elapsed + " ms 안에 끝나지 않았다(Compaction Timeout "
                        + compactionTimeout + " ms, 마지막 상태 '" + record.state() + "'). 요청은 Hive 에 남아 계속 진행될 수 있다");
            }
            if (phase == CompactionPhase.UNKNOWN) {
                getLogger().warn("{} compaction id {} 의 상태 '{}' 를 알 수 없어 계속 기다린다", flowFile, record.id(), record.state());
            }
            transfer(REL_IN_PROGRESS, true);
        }

        // ⑤ 사후 행 수

        private void complete(final Connection connection, final Target target, final long requestedAt) throws JobFailure, SQLException {
            attributes.put(ATTR_DURATION_MS, Long.toString(Math.max(0L, clock.millis() - requestedAt)));
            if (!validateRowCount) {
                transfer(REL_SUCCESS, false);
                return;
            }
            final long after = countRows(connection, target);
            attributes.put(ATTR_ROWCOUNT_AFTER, Long.toString(after));
            final String beforeText = attributes.getOrDefault(ATTR_ROWCOUNT_BEFORE, flowFile.getAttribute(ATTR_ROWCOUNT_BEFORE));
            if (beforeText == null || beforeText.isBlank()) {
                getLogger().warn("{} 에 사전 행 수({})가 없어 사후 행 수만 기록한다", flowFile, ATTR_ROWCOUNT_BEFORE);
                transfer(REL_SUCCESS, false);
                return;
            }
            final long before;
            try {
                before = Long.parseLong(beforeText.trim());
            } catch (final NumberFormatException e) {
                throw new JobFailure(ATTR_ROWCOUNT_BEFORE + " 속성 값 '" + beforeText + "' 이(가) 숫자가 아니다");
            }
            if (before == after) {
                transfer(REL_SUCCESS, false);
            } else {
                attributes.put(ATTR_ERROR, "compaction 전후 행 수가 다르다: before=" + before + ", after=" + after);
                getLogger().warn("{} {} compaction 전후 행 수가 다르다: before={}, after={}", flowFile, target.qualified(), before, after);
                transfer(REL_COUNT_MISMATCH, false);
            }
        }

        // ---- 판별 ----

        private Target targetFromProperties() throws JobFailure {
            final String database = trim(context.getProperty(DATABASE).evaluateAttributeExpressions(flowFile).getValue());
            final String table = trim(context.getProperty(TABLE).evaluateAttributeExpressions(flowFile).getValue());
            final String specText = context.getProperty(PARTITION_SPEC).evaluateAttributeExpressions(flowFile).getValue();
            try {
                HiveIdentifiers.require("데이터베이스", database);
                HiveIdentifiers.require("테이블", table);
                return new Target(database, table, PartitionSpec.parse(specText));
            } catch (final IllegalArgumentException e) {
                throw new JobFailure(e.getMessage());
            }
        }

        private Target targetFromAttributes() throws JobFailure {
            final String qualified = flowFile.getAttribute(ATTR_TABLE);
            final int dot = qualified == null ? -1 : qualified.indexOf('.');
            if (dot <= 0) {
                throw new JobFailure(ATTR_TABLE + " 속성(db.table)이 없거나 올바르지 않다: " + qualified);
            }
            try {
                final String database = HiveIdentifiers.require("데이터베이스", qualified.substring(0, dot));
                final String table = HiveIdentifiers.require("테이블", qualified.substring(dot + 1));
                return new Target(database, table, PartitionSpec.parse(flowFile.getAttribute(ATTR_PARTITION)));
            } catch (final IllegalArgumentException e) {
                throw new JobFailure(ATTR_TABLE + "/" + ATTR_PARTITION + " 속성이 올바르지 않다: " + e.getMessage());
            }
        }

        private HiveTableInfo describe(final Connection connection, final Target target) throws JobFailure {
            try {
                final List<String[]> properties = query(connection, CompactionSql.showTblProperties(target.database(), target.table()));
                final List<String[]> describe = query(connection, CompactionSql.describeFormatted(target.database(), target.table()));
                return HiveTableInfo.parse(properties, describe);
            } catch (final SQLException e) {
                throw new JobFailure(target.qualified() + " 테이블 정보를 읽지 못했다: " + e.getMessage(), e);
            }
        }

        private void checkPartition(final Target target, final HiveTableInfo info) throws JobFailure {
            if (info.isPartitioned()) {
                if (target.spec().isEmpty()) {
                    throw new JobFailure(target.qualified() + " 은(는) 파티션 테이블이므로 Partition Spec 에 모든 파티션 키("
                            + String.join(", ", info.partitionColumns()) + ")를 지정해야 한다");
                }
                if (!target.spec().hasSameKeys(info.partitionColumns())) {
                    throw new JobFailure("Partition Spec 의 키 " + target.spec().values().keySet() + " 가 " + target.qualified()
                            + " 의 파티션 컬럼 " + info.partitionColumns() + " 과 다르다");
                }
            } else if (!target.spec().isEmpty()) {
                throw new JobFailure(target.qualified() + " 은(는) 파티션 테이블이 아니므로 Partition Spec 을 비워야 한다");
            }
        }

        private String resolveType(final HiveTableInfo info) throws JobFailure {
            final TableTypeHandling handling = context.getProperty(TABLE_TYPE_HANDLING).asAllowableValue(TableTypeHandling.class);
            final String table = attributes.get(ATTR_TABLE);
            switch (info.kind()) {
                case FULL_ACID:
                case INSERT_ONLY_ACID:
                    return context.getProperty(COMPACTION_TYPE).asAllowableValue(CompactionType.class).name();
                case NON_ACID_ORC:
                    if (handling == TableTypeHandling.ACID_ONLY) {
                        throw new JobFailure(table + " 은(는) 트랜잭션(ACID) 테이블이 아니다(Table Type Handling=ACID_ONLY)");
                    }
                    if (!info.isManaged()) {
                        throw new JobFailure(table + " 은(는) Non-ACID ORC " + info.tableType() + " 이다. CONCATENATE 는 managed 테이블에서만 된다"
                                + "(Hive 4 는 Non-ACID managed 테이블을 EXTERNAL 로 바꿔 만들 수 있다)");
                    }
                    return TYPE_CONCATENATE;
                default:
                    if (handling == TableTypeHandling.ACID_ONLY) {
                        throw new JobFailure(table + " 은(는) 트랜잭션(ACID) 테이블이 아니다(Table Type Handling=ACID_ONLY)");
                    }
                    throw new JobFailure(table + " 은(는) Non-ACID 테이블이고 ORC 가 아니어서(InputFormat=" + info.inputFormat()
                            + ") compaction·CONCATENATE 를 지원하지 않는다");
            }
        }

        // ---- JDBC ----

        private long countRows(final Connection connection, final Target target) throws JobFailure {
            final String sql = CompactionSql.countRows(target.database(), target.table(), target.spec());
            try {
                execute(connection, CompactionSql.DISABLE_STATS_ANSWER);
                final List<String[]> rows = query(connection, sql);
                if (rows.isEmpty() || rows.get(0).length == 0 || rows.get(0)[0] == null) {
                    throw new JobFailure("행 수 쿼리(" + sql + ")가 결과를 돌려주지 않았다");
                }
                return Long.parseLong(rows.get(0)[0].trim());
            } catch (final SQLException | NumberFormatException e) {
                throw new JobFailure("행 수를 세지 못했다(" + sql + "): " + e.getMessage(), e);
            }
        }

        private List<CompactionRecord> showCompactions(final Connection connection) throws JobFailure {
            try (Statement statement = statement(connection);
                 ResultSet resultSet = statement.executeQuery(CompactionSql.SHOW_COMPACTIONS)) {
                return ShowCompactionsParser.parse(resultSet);
            } catch (final SQLException | IllegalStateException e) {
                throw new JobFailure("SHOW COMPACTIONS 실패: " + e.getMessage(), e);
            }
        }

        private void execute(final Connection connection, final String sql) throws SQLException {
            getLogger().debug("HiveQL 실행: {}", sql);
            try (Statement statement = statement(connection)) {
                if (statement.execute(sql)) {
                    // Hive 3.1.3·4.0.1 의 ALTER TABLE ... COMPACT/CONCATENATE 는 결과 집합이 없다. 다른 배포판 대비로 소비만 한다.
                    try (ResultSet ignored = statement.getResultSet()) {
                        getLogger().debug("{} 이(가) 결과 집합을 돌려줬다", sql);
                    }
                }
            }
        }

        private List<String[]> query(final Connection connection, final String sql) throws SQLException {
            getLogger().debug("HiveQL 조회: {}", sql);
            try (Statement statement = statement(connection); ResultSet resultSet = statement.executeQuery(sql)) {
                final int columns = resultSet.getMetaData().getColumnCount();
                final List<String[]> rows = new ArrayList<>();
                while (resultSet.next()) {
                    final String[] row = new String[columns];
                    for (int i = 0; i < columns; i++) {
                        row[i] = resultSet.getString(i + 1);
                    }
                    rows.add(row);
                }
                return rows;
            }
        }

        private Statement statement(final Connection connection) throws SQLException {
            final Statement statement = connection.createStatement();
            if (queryTimeout > 0) {
                statement.setQueryTimeout(queryTimeout);
            }
            return statement;
        }

        private String connectionUrl() {
            try {
                return dbcpService.getConnectionURL();
            } catch (final RuntimeException e) {
                return "hive2://unknown";
            }
        }

        // ---- 라우팅 ----

        private long longAttribute(final String name) throws JobFailure {
            final String value = flowFile.getAttribute(name);
            try {
                return Long.parseLong(value == null ? "" : value.trim());
            } catch (final NumberFormatException e) {
                throw new JobFailure(name + " 속성 값 '" + value + "' 이(가) 없거나 숫자가 아니다");
            }
        }

        private void transfer(final Relationship relationship, final boolean penalize) {
            if (relationship != REL_COUNT_MISMATCH && flowFile.getAttribute(ATTR_ERROR) != null) {
                // failure 를 다시 이 프로세서로 돌린 경우 이전 오류를 지운다
                flowFile = session.removeAttribute(flowFile, ATTR_ERROR);
            }
            flowFile = session.putAllAttributes(flowFile, attributes);
            if (penalize) {
                flowFile = session.penalize(flowFile);
            }
            session.transfer(flowFile, relationship);
        }

        void fail(final String message, final Throwable cause) {
            attributes.put(ATTR_ERROR, message);
            flowFile = session.putAllAttributes(flowFile, attributes);
            if (cause != null) {
                getLogger().error("{} compaction 실패: {}", flowFile, message, cause);
            } else {
                getLogger().error("{} compaction 실패: {}", flowFile, message);
            }
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private static String trim(final String value) {
        return value == null ? null : value.trim();
    }

    /**
     * Partition Spec 형식 검증. EL 이 있으면 실행 시점에 검증한다.
     */
    private static final class PartitionSpecValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final org.apache.nifi.components.ValidationContext context) {
            if (input == null || input.isBlank() || context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true)
                        .explanation("비었거나 Expression Language 를 포함함").build();
            }
            try {
                PartitionSpec.parse(input);
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            } catch (final IllegalArgumentException e) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(e.getMessage()).build();
            }
        }
    }
}
