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

import io.datadynamics.nifi.processors.hive.compaction.CandidateEvaluator;
import io.datadynamics.nifi.processors.hive.compaction.CandidateEvaluator.Recommendation;
import io.datadynamics.nifi.processors.hive.compaction.CompactionPhase;
import io.datadynamics.nifi.processors.hive.compaction.CompactionRecord;
import io.datadynamics.nifi.processors.hive.compaction.CompactionSql;
import io.datadynamics.nifi.processors.hive.compaction.HiveIdentifiers;
import io.datadynamics.nifi.processors.hive.compaction.HiveTableInfo;
import io.datadynamics.nifi.processors.hive.compaction.PartitionDirectoryScanner;
import io.datadynamics.nifi.processors.hive.compaction.PartitionSpec;
import io.datadynamics.nifi.processors.hive.compaction.ShowCompactionsParser;
import io.datadynamics.nifi.processors.hive.util.ValidationResources;
import io.datadynamics.nifi.services.api.hive.Hive3DBCPService;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 한 테이블의 파티션 디렉토리를 훑어 compaction 이 필요한 파티션마다 FlowFile 을 하나씩 낸다.
 * 결과는 {@link CompactHive3Table} 에 바로 연결하도록 {@code hive.compaction.database}·{@code hive.compaction.table}·
 * {@code hive.compaction.partition.spec} 속성을 쓴다.
 */
@Tags({"hive", "acid", "compaction", "orc", "maintenance", "small files", "delta", "list"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SeeAlso({CompactHive3Table.class})
@CapabilityDescription("Hive 테이블의 파티션(SHOW PARTITIONS, 선택 Partition Filter)마다 Location 디렉토리를 Hadoop FileSystem 으로 세어 "
        + "compaction 이 필요한 파티션을 찾는다. ACID 테이블은 Hive Initiator 와 같은 기준(현재 delta 크기 / base 크기 > Delta Size Ratio Threshold "
        + "→ MAJOR, base 가 없으면 delta 가 있으면 MAJOR, 그렇지 않고 delta+delete_delta 디렉토리 수 > Delta Count Threshold → MINOR)으로, "
        + "Non-ACID 테이블은 파일 수 > Small File Count Threshold 이고 평균 파일 크기 < Small File Size 면 후보로 본다"
        + "(managed ORC 는 CONCATENATE, 그 밖에는 REWRITE 권장). 이미 합쳐졌지만 Cleaner 가 아직 지우지 않은 옛 base·delta 는 세지 않는다. "
        + "마지막 수정이 Minimum Partition Age 보다 최근인 파티션(적재 중일 수 있음)과 SHOW COMPACTIONS 에서 initiated·working 인 파티션은 뺀다. "
        + "후보마다 빈 FlowFile 을 success 로 보내며(delta 수 내림차순, 최대 Maximum Candidates 개), 속성을 CompactHive3Table 의 "
        + "Database=${hive.compaction.database}, Table=${hive.compaction.table}, Partition Spec=${hive.compaction.partition.spec} 로 쓴다. "
        + "입력이 있으면 그 속성으로 대상을 정하고 입력은 original 로 보낸다. 입력이 없으면 스케줄(Cron 권장)마다 실행한다. "
        + "파티션마다 DESCRIBE FORMATTED 와 디렉토리 목록을 읽으므로 파티션이 많으면 Partition Filter 로 범위를 줄인다.")
@WritesAttributes({
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_DATABASE, description = "대상 데이터베이스."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_TABLE, description = "대상 테이블(데이터베이스 제외). "
                + "CompactHive3Table 은 처리하면서 이 속성을 db.table 로 덮어쓴다."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_PARTITION_SPEC, description = "Partition Spec 형식의 파티션(예: dt='2026-10-01'). 비파티션 테이블이면 빈 값."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_RECOMMENDED_TYPE, description = "MAJOR, MINOR, CONCATENATE 또는 REWRITE."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_DELTA_COUNT, description = "현재 delta 디렉토리 수(ACID)."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_DELETE_DELTA_COUNT, description = "현재 delete_delta 디렉토리 수(ACID)."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_BASE_BYTES, description = "현재 base 크기(byte, ACID)."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_DELTA_BYTES, description = "현재 delta + delete_delta 크기(byte, ACID)."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_FILE_COUNT, description = "현재 데이터 파일 수(숨김·메타 파일 제외)."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_PARTITION_LOCATION, description = "파티션 Location."),
        @WritesAttribute(attribute = ListHiveCompactionCandidates.ATTR_ERROR, description = "failure 로 보낼 때 이유.")
})
public class ListHiveCompactionCandidates extends AbstractProcessor {

    static final String ATTR_DATABASE = "hive.compaction.database";
    static final String ATTR_TABLE = "hive.compaction.table";
    static final String ATTR_PARTITION_SPEC = "hive.compaction.partition.spec";
    static final String ATTR_RECOMMENDED_TYPE = "hive.compaction.recommended.type";
    static final String ATTR_DELTA_COUNT = "hive.compaction.delta.count";
    static final String ATTR_DELETE_DELTA_COUNT = "hive.compaction.delete.delta.count";
    static final String ATTR_BASE_BYTES = "hive.compaction.base.bytes";
    static final String ATTR_DELTA_BYTES = "hive.compaction.delta.bytes";
    static final String ATTR_FILE_COUNT = "hive.compaction.file.count";
    static final String ATTR_PARTITION_LOCATION = "hive.compaction.partition.location";
    static final String ATTR_ERROR = "hive.compaction.error";

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("compaction 후보 파티션마다 빈 FlowFile 하나.")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("탐색을 마친 입력 FlowFile.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("테이블 조회·HDFS 접근에 실패한 입력 FlowFile. 이유는 hive.compaction.error 속성에 있다.")
            .build();

    static final PropertyDescriptor HIVE_DBCP_SERVICE = AbstractHive3QLProcessor.HIVE_DBCP_SERVICE;

    static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(PutHive3Streaming.HIVE_CONFIGURATION_RESOURCES)
            .description("파티션 디렉토리를 읽을 Hadoop 설정(core-site.xml, hdfs-site.xml, hive-site.xml) 파일 목록(쉼표 구분). "
                    + "비우면 클래스패스의 설정이나 기본값(로컬 파일시스템)을 쓴다. Kerberos 를 쓰려면 이 설정에 hadoop.security.authentication=kerberos 가 있어야 한다.")
            .build();

    static final PropertyDescriptor KERBEROS_USER_SERVICE = PutHive3Streaming.KERBEROS_USER_SERVICE;

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

    static final PropertyDescriptor PARTITION_FILTER = new PropertyDescriptor.Builder()
            .name("Partition Filter")
            .displayName("파티션 필터")
            .description("SHOW PARTITIONS ... PARTITION (...) 에 쓰는 부분 파티션 지정(예: dt='2026-10-01' 이면 그 날짜의 모든 hour 파티션). "
                    + "Partition Spec 과 같은 형식이며 일부 키만 줄 수 있다. 비우면 모든 파티션.")
            .required(false)
            .addValidator(new PartitionFilterValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor DELTA_COUNT_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Delta Count Threshold")
            .displayName("delta 수 임계값")
            .description("ACID 파티션의 현재 delta + delete_delta 디렉토리 수가 이 값보다 크면 MINOR 후보(Hive hive.compactor.delta.num.threshold 와 같은 뜻).")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor DELTA_SIZE_RATIO_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Delta Size Ratio Threshold")
            .displayName("delta 크기 비율 임계값")
            .description("ACID 파티션의 현재 delta 크기 / base 크기가 이 값보다 크면 MAJOR 후보(Hive hive.compactor.delta.pct.threshold 와 같은 뜻). "
                    + "base 가 없으면 delta 가 하나라도 있으면 MAJOR.")
            .required(true)
            .defaultValue("0.1")
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .build();

    static final PropertyDescriptor SMALL_FILE_COUNT_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Small File Count Threshold")
            .displayName("작은 파일 수 임계값")
            .description("Non-ACID 파티션의 파일 수가 이 값보다 크고 평균 파일 크기가 Small File Size 보다 작으면 후보.")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor SMALL_FILE_SIZE = new PropertyDescriptor.Builder()
            .name("Small File Size")
            .displayName("작은 파일 크기")
            .description("Non-ACID 파티션 후보 판정의 평균 파일 크기 기준.")
            .required(true)
            .defaultValue("64 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    static final PropertyDescriptor MINIMUM_PARTITION_AGE = new PropertyDescriptor.Builder()
            .name("Minimum Partition Age")
            .displayName("최소 파티션 경과 시간")
            .description("파티션 디렉토리(와 그 아래 현재 파일)의 마지막 수정이 이 시간보다 최근이면 적재 중일 수 있으므로 뺀다.")
            .required(true)
            .defaultValue("1 hour")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAXIMUM_CANDIDATES = new PropertyDescriptor.Builder()
            .name("Maximum Candidates")
            .displayName("최대 후보 수")
            .description("한 번에 내보낼 후보 FlowFile 의 최대 개수. delta 수(Non-ACID 는 파일 수) 내림차순으로 자른다.")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = AbstractHive3QLProcessor.QUERY_TIMEOUT;

    private static final List<PropertyDescriptor> PROPERTIES = List.of(HIVE_DBCP_SERVICE, HIVE_CONFIGURATION_RESOURCES, KERBEROS_USER_SERVICE,
            DATABASE, TABLE, PARTITION_FILTER, DELTA_COUNT_THRESHOLD, DELTA_SIZE_RATIO_THRESHOLD, SMALL_FILE_COUNT_THRESHOLD, SMALL_FILE_SIZE,
            MINIMUM_PARTITION_AGE, MAXIMUM_CANDIDATES, QUERY_TIMEOUT);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_ORIGINAL, REL_FAILURE);

    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();
    private volatile HiveFileSystemAccess fileSystemAccess;
    private volatile Clock clock = Clock.systemUTC();

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
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        if (validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).isSet()) {
            final KerberosUserService kerberosUserService = validationContext.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            final String configFiles = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            results.addAll(new HiveConfigurator().validate(configFiles, kerberosUserService, validationResourceHolder, getLogger()));
        }
        return results;
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        fileSystemAccess = HiveFileSystemAccess.open(context.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue(),
                context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class), getLogger());
    }

    @OnStopped
    public void shutdown() {
        validationResourceHolder.set(null);
        final HiveFileSystemAccess access = fileSystemAccess;
        fileSystemAccess = null;
        if (access != null) {
            access.close();
        }
    }

    /** 후보 하나. */
    private record Candidate(PartitionSpec spec, PartitionDirectoryScanner.Stats stats, Recommendation recommendation) {
        long sortKey() {
            return stats.acid() ? stats.totalDeltaCount() : stats.fileCount();
        }
    }

    /** 실패 이유. */
    private static final class ListingFailure extends Exception {
        ListingFailure(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = null;
        if (context.hasIncomingConnection()) {
            input = session.get();
            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }
        final String database = trim(context.getProperty(DATABASE).evaluateAttributeExpressions(input).getValue());
        final String table = trim(context.getProperty(TABLE).evaluateAttributeExpressions(input).getValue());
        final List<Candidate> candidates;
        try {
            candidates = findCandidates(context, input, database, table);
        } catch (final ListingFailure e) {
            getLogger().error("{}.{} compaction 후보 탐색 실패: {}", database, table, e.getMessage(), e.getCause());
            if (input != null) {
                input = session.putAttribute(input, ATTR_ERROR, e.getMessage());
                session.transfer(input, REL_FAILURE);
            } else {
                context.yield();
            }
            return;
        }

        final int maximum = context.getProperty(MAXIMUM_CANDIDATES).asInteger();
        final List<Candidate> sorted = new ArrayList<>(candidates);
        sorted.sort(Comparator.comparingLong(Candidate::sortKey).reversed()
                .thenComparing(c -> c.spec().toCanonicalString()));
        if (sorted.size() > maximum) {
            getLogger().info("{}.{} 후보 {}개 중 Maximum Candidates={}개만 내보낸다", database, table, sorted.size(), maximum);
        }
        for (final Candidate candidate : sorted.subList(0, Math.min(maximum, sorted.size()))) {
            FlowFile output = input == null ? session.create() : session.create(input);
            output = session.putAllAttributes(output, attributes(database, table, candidate));
            if (input == null) {
                session.getProvenanceReporter().create(output, "compaction 후보 " + candidate.stats().location());
            }
            session.transfer(output, REL_SUCCESS);
        }
        if (input != null) {
            session.transfer(input, REL_ORIGINAL);
        }
    }

    private static Map<String, String> attributes(final String database, final String table, final Candidate candidate) {
        final PartitionDirectoryScanner.Stats stats = candidate.stats();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_DATABASE, database);
        attributes.put(ATTR_TABLE, table);
        attributes.put(ATTR_PARTITION_SPEC, candidate.spec().toCanonicalString());
        attributes.put(ATTR_RECOMMENDED_TYPE, candidate.recommendation().name());
        attributes.put(ATTR_DELTA_COUNT, Integer.toString(stats.deltaCount()));
        attributes.put(ATTR_DELETE_DELTA_COUNT, Integer.toString(stats.deleteDeltaCount()));
        attributes.put(ATTR_BASE_BYTES, Long.toString(stats.baseBytes()));
        attributes.put(ATTR_DELTA_BYTES, Long.toString(stats.deltaBytes()));
        attributes.put(ATTR_FILE_COUNT, Long.toString(stats.fileCount()));
        attributes.put(ATTR_PARTITION_LOCATION, stats.location());
        return attributes;
    }

    private List<Candidate> findCandidates(final ProcessContext context, final FlowFile input, final String database, final String table)
            throws ListingFailure {
        final PartitionSpec filter;
        try {
            HiveIdentifiers.require("데이터베이스", database);
            HiveIdentifiers.require("테이블", table);
            filter = PartitionSpec.parse(context.getProperty(PARTITION_FILTER).evaluateAttributeExpressions(input).getValue());
        } catch (final IllegalArgumentException e) {
            throw new ListingFailure(e.getMessage(), null);
        }
        final HiveFileSystemAccess access = fileSystemAccess;
        if (access == null) {
            throw new ListingFailure("Hadoop FileSystem 접근이 준비되지 않았다", null);
        }
        final Integer timeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(input).asInteger();
        final int queryTimeout = timeout == null ? 0 : timeout;
        final CandidateEvaluator.Thresholds thresholds = new CandidateEvaluator.Thresholds(
                context.getProperty(DELTA_COUNT_THRESHOLD).asInteger(),
                context.getProperty(DELTA_SIZE_RATIO_THRESHOLD).asDouble(),
                context.getProperty(SMALL_FILE_COUNT_THRESHOLD).asInteger(),
                context.getProperty(SMALL_FILE_SIZE).asDataSize(DataUnit.B).longValue());
        final long minimumAge = context.getProperty(MINIMUM_PARTITION_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long now = clock.millis();
        final String qualified = database + "." + table;

        final Hive3DBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(Hive3DBCPService.class);
        final Map<String, String> connectionAttributes = input == null ? Map.of() : input.getAttributes();
        try (Connection connection = dbcpService.getConnection(connectionAttributes)) {
            final HiveTableInfo info = HiveTableInfo.parse(query(connection, CompactionSql.showTblProperties(database, table), queryTimeout),
                    query(connection, CompactionSql.describeFormatted(database, table), queryTimeout));
            final boolean acid = info.isAcid();
            final boolean managedOrc = !acid && info.isOrc() && info.isManaged();

            final List<PartitionSpec> partitions = new ArrayList<>();
            if (info.isPartitioned()) {
                if (!filter.isSubsetOf(info.partitionColumns())) {
                    throw new ListingFailure("Partition Filter 의 키 " + filter.values().keySet() + " 가 " + qualified + " 의 파티션 컬럼 "
                            + info.partitionColumns() + " 에 없다", null);
                }
                for (final String[] row : query(connection, CompactionSql.showPartitions(database, table, filter), queryTimeout)) {
                    if (row.length > 0 && row[0] != null && !row[0].isBlank()) {
                        partitions.add(PartitionSpec.fromPartitionName(row[0]));
                    }
                }
            } else {
                if (!filter.isEmpty()) {
                    throw new ListingFailure(qualified + " 은(는) 파티션 테이블이 아니므로 Partition Filter 를 비워야 한다", null);
                }
                partitions.add(PartitionSpec.empty());
            }

            // 이미 진행 중인 compaction (Hive 4 는 중복 요청을 거부하고 Hive 3.1 은 조용히 무시한다)
            final List<CompactionRecord> running = new ArrayList<>();
            if (acid) {
                try (Statement statement = statement(connection, queryTimeout);
                     ResultSet resultSet = statement.executeQuery(CompactionSql.SHOW_COMPACTIONS)) {
                    for (final CompactionRecord record : ShowCompactionsParser.parse(resultSet)) {
                        final CompactionPhase phase = CompactionPhase.of(record.state());
                        if (phase == CompactionPhase.INITIATED || phase == CompactionPhase.WORKING) {
                            running.add(record);
                        }
                    }
                }
            }

            final List<Candidate> candidates = new ArrayList<>();
            for (final PartitionSpec spec : partitions) {
                if (running.stream().anyMatch(r -> r.isFor(database, table, spec))) {
                    getLogger().debug("{} [{}] 는 compaction 이 진행 중이라 뺀다", qualified, spec);
                    continue;
                }
                final String location = spec.isEmpty() ? info.location()
                        : HiveTableInfo.parseDescribe(query(connection, CompactionSql.describeFormatted(database, table, spec), queryTimeout), Map.of())
                        .location();
                if (location == null || location.isBlank()) {
                    throw new ListingFailure(qualified + " [" + spec + "] 의 Location 을 DESCRIBE FORMATTED 에서 찾지 못했다", null);
                }
                final PartitionDirectoryScanner.Stats stats;
                try {
                    stats = access.doAs(location, (fs, path) -> fs.exists(path) ? PartitionDirectoryScanner.scan(fs, path, acid) : null);
                } catch (final IOException e) {
                    throw new ListingFailure(qualified + " [" + spec + "] 디렉토리 " + location + " 를 읽지 못했다: " + e.getMessage(), e);
                }
                if (stats == null) {
                    getLogger().debug("{} [{}] 디렉토리 {} 가 없어 건너뛴다", qualified, spec, location);
                    continue;
                }
                if (now - stats.lastModified() < minimumAge) {
                    getLogger().debug("{} [{}] 는 {} ms 전에 바뀌어 뺀다(Minimum Partition Age)", qualified, spec, now - stats.lastModified());
                    continue;
                }
                final Optional<Recommendation> recommendation = CandidateEvaluator.evaluate(stats, managedOrc, thresholds);
                recommendation.ifPresent(r -> candidates.add(new Candidate(spec, stats, r)));
            }
            return candidates;
        } catch (final SQLException e) {
            throw new ListingFailure(qualified + " 조회 실패: " + e.getMessage(), e);
        } catch (final IllegalArgumentException | IllegalStateException e) {
            throw new ListingFailure(qualified + " 결과 해석 실패: " + e.getMessage(), e);
        } catch (final ProcessException e) {
            throw new ListingFailure("Hive 커넥션을 얻지 못했다: " + e.getMessage(), e);
        }
    }

    private static Statement statement(final Connection connection, final int queryTimeout) throws SQLException {
        final Statement statement = connection.createStatement();
        if (queryTimeout > 0) {
            statement.setQueryTimeout(queryTimeout);
        }
        return statement;
    }

    private static List<String[]> query(final Connection connection, final String sql, final int queryTimeout) throws SQLException {
        try (Statement statement = statement(connection, queryTimeout); ResultSet resultSet = statement.executeQuery(sql)) {
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

    private static String trim(final String value) {
        return value == null ? null : value.trim();
    }

    /**
     * Partition Filter 형식 검증. EL 이 있으면 실행 시점에 검증한다.
     */
    private static final class PartitionFilterValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (input == null || input.isBlank() || context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
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
