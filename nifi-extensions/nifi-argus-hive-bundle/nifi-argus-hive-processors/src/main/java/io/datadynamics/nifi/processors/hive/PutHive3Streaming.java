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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/processors/hive/PutHive3Streaming.java
 */
package io.datadynamics.nifi.processors.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.streaming.*;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.DiscontinuedException;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.hadoop.exception.RecordReaderFactoryException;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.StringUtils;
import io.datadynamics.nifi.processors.hive.util.AuthenticationFailedException;
import io.datadynamics.nifi.processors.hive.util.HiveOptions;
import io.datadynamics.nifi.processors.hive.util.ValidationResources;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.datadynamics.nifi.processors.hive.AbstractHive3QLProcessor.ATTR_OUTPUT_TABLES;

/**
 * Record Reader로 읽은 레코드를 Hive Streaming API를 통해 Apache Hive 3.x 테이블에 스트리밍 방식으로 적재하는 프로세서.
 * 트랜잭션 단위(Records per Transaction)와 배치 단위(Transactions per Batch)로 커밋을 제어하여
 * 대량의 레코드도 안정적으로 처리할 수 있으며, 실패 시 일부 성공한 레코드는 success로,
 * 나머지 미처리 원본 FlowFile은 retry/failure로 나뉘어 라우팅될 수 있다(완전한 롤백은 지원하지 않음).
 * Kerberos 인증 환경에서는 KerberosUserService를 통해 인증을 수행한다.
 */
@Tags({"cloudera", "hive", "streaming", "put", "database", "store"})
@CapabilityDescription("이 프로세서는 Hive Streaming을 사용하여 FlowFile의 레코드를 Apache Hive 3.0+ 테이블로 전송한다. 'Static Partition Values'가 설정되지 않은 경우, "
        + "파티션 값은 각 레코드의 '마지막' 필드들인 것으로 간주된다. 예를 들어 테이블이 컬럼 A로 파티셔닝되어 있다면 "
        + "각 레코드의 마지막 필드가 컬럼 A에 해당해야 한다. 'Static Partition Values'가 설정된 경우, 해당 값들이 파티션 값으로 사용되며 "
        + "파티션 컬럼에 대응하는 레코드 필드는 무시된다.")
@WritesAttributes({
        @WritesAttribute(attribute = "hivestreaming.record.count", description = "'success' 및 'failure' 관계로 라우팅되는 FlowFile에 기록되는 속성으로, "
                + "유입 FlowFile에 포함된 레코드 개수를 나타낸다. 하나의 FlowFile에 속한 모든 레코드는 단일 트랜잭션으로 커밋된다."),
        @WritesAttribute(attribute = "query.output.tables", description = "'success' 및 'failure' 관계로 라우팅되는 FlowFile에 기록되는 속성으로, "
                + "'databaseName.tableName' 형식의 대상 테이블명을 나타낸다.")
})
@RequiresInstanceClassLoading
public class PutHive3Streaming extends AbstractProcessor {
    // 속성(Attribute) 키
    public static final String HIVE_STREAMING_RECORD_COUNT_ATTR = "hivestreaming.record.count";
    // 관계(Relationship) 정의
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("레코드가 Hive로 성공적으로 전송된 후 Avro 레코드를 포함한 FlowFile이 이 관계로 라우팅된다.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("레코드를 Hive로 전송하지 못한 경우 Avro 레코드를 포함한 FlowFile이 이 관계로 라우팅된다.")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("레코드를 Hive로 전송할 수 없는 경우 유입 FlowFile이 이 관계로 라우팅된다. "
                    + "일부 레코드는 이미 성공적으로 처리되었을 수 있으며, 이 경우 해당 레코드는 Avro FlowFile 형태로 success 관계로 라우팅된다. "
                    + "retry, success, failure 세 관계를 조합하면 얼마나 많은 레코드가 성공/실패했는지 알 수 있다. "
                    + "완전한 롤백이 불가능하므로 이를 이용해 재시도 기능을 제공할 수 있다.")
            .build();
    // 프로퍼티(Property) 정의
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("레코드 리더")
            .description("유입 FlowFile로부터 레코드를 읽어들이는 데 사용할 서비스.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("hive3-stream-metastore-uri")
            .displayName("Hive 메타스토어 URI")
            .description("Hive 메타스토어의 URI 위치(들). 쉼표로 구분된 Hive 메타스토어 URI 목록이며, Hive Server의 위치가 아님에 유의한다. "
                    + "Hive 메타스토어의 기본 포트는 9043이다. 이 필드를 설정하지 않으면 제공된 설정 리소스 중 'hive.metastore.uris' 프로퍼티가 사용되며, "
                    + "그마저도 제공되지 않으면 기본 hive-site.xml의 기본값(일반적으로 thrift://localhost:9083)이 사용된다.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .build();
    static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hive3-config-resources")
            .displayName("Hive 설정 리소스")
            .description("Hive 설정(예: hive-site.xml)이 담긴 파일 또는 쉼표로 구분된 파일 목록. 이를 설정하지 않으면 Hadoop이 "
                    + "클래스패스에서 'hive-site.xml' 파일을 찾거나 기본 설정으로 대체한다. Kerberos 등을 이용한 인증을 활성화하려면 "
                    + "설정 파일에 해당 프로퍼티가 설정되어 있어야 함에 유의한다. 또한 Max Concurrent Tasks가 1보다 큰 값으로 설정된 경우, "
                    + "동시성 문제를 방지하기 위해 'hcatalog.hive.client.cache.disabled' 프로퍼티가 강제로 'true'로 설정됨에 유의한다. "
                    + "자세한 내용은 Hive 문서를 참조한다.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
            .name("hive3-stream-database-name")
            .displayName("데이터베이스 이름")
            .description("데이터를 적재할 데이터베이스의 이름.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("hive3-stream-table-name")
            .displayName("테이블 이름")
            .description("데이터를 적재할 데이터베이스 테이블의 이름.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor STATIC_PARTITION_VALUES = new PropertyDescriptor.Builder()
            .name("hive3-stream-part-vals")
            .displayName("정적 파티션 값")
            .description("대상 테이블의 파티션 컬럼에 대한 값을 쉼표로 구분하여 지정한다. 유입되는 모든 레코드가 파티션 컬럼에 대해 동일한 값을 "
                    + "가진다면, 이 값을 여기에 입력하여 성능 향상을 얻을 수 있다. 이 프로퍼티를 지정하는 경우 대개 Expression Language를 함께 "
                    + "사용하게 되는데, 예를 들어 상류에 PartitionRecord가 있고 'name'과 'age' 두 파티션을 사용한다면 이 프로퍼티를 "
                    + "${name},${age}로 설정할 수 있다. 이 프로퍼티가 설정되면 해당 값들이 파티션 값으로 사용되며, "
                    + "파티션 컬럼에 대응하는 레코드 필드는 무시된다. 이 프로퍼티가 설정되지 않으면 파티션 값은 각 레코드의 마지막 필드들인 것으로 간주된다.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor RECORDS_PER_TXN = new PropertyDescriptor.Builder()
            .name("hive3-stream-records-per-transaction")
            .displayName("트랜잭션당 레코드 수")
            .description("트랜잭션을 커밋하기 전에 처리할 레코드 개수. 0으로 설정하면 모든 레코드가 단일 트랜잭션으로 기록된다.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();
    static final PropertyDescriptor TXNS_PER_BATCH = new PropertyDescriptor.Builder()
            .name("hive3-stream-transactions-per-batch")
            .displayName("배치당 트랜잭션 수")
            .description("프로세서 태스크가 필요로 하는 트랜잭션 개수에 대해 Hive Streaming에 제공하는 힌트. Records per Transaction(0이 아닌 경우)과 "
                    + "Transactions per Batch의 곱은 FlowFile(들)에 포함된 예상 최대 레코드 개수보다 커야 하며, 이렇게 해야 "
                    + "트랜잭션 배치 실패 시 전체 롤백이 보장된다.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();
    static final PropertyDescriptor CALL_TIMEOUT = new PropertyDescriptor.Builder()
            .name("hive3-stream-call-timeout")
            .displayName("호출 타임아웃")
            .description("Hive Streaming 작업이 완료되기까지 허용되는 시간(초). 값이 0이면 프로세서는 작업이 끝날 때까지 무한정 대기한다. "
                    + "이 프로퍼티는 Expression Language를 지원하지만 유입 FlowFile 속성에 대해서는 평가되지 않음에 유의한다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    static final PropertyDescriptor DISABLE_STREAMING_OPTIMIZATIONS = new PropertyDescriptor.Builder()
            .name("hive3-stream-disable-optimizations")
            .displayName("스트리밍 최적화 비활성화")
            .description("스트리밍 최적화를 비활성화할지 여부. 스트리밍 최적화를 비활성화하면 성능과 메모리 사용량에 상당한 영향을 미친다.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    static final PropertyDescriptor ROLLBACK_ON_FAILURE = RollbackOnFailure.createRollbackOnFailureProperty(
            "참고: 동일한 입력 FlowFile로부터 파생된 Hive Streaming 트랜잭션 중 일부가 이미 커밋된 상태에서 오류가 발생한 경우," +
                    " (즉, FlowFile에 포함된 레코드 수가 'Records per Transaction'보다 많고 2번째 이후 트랜잭션에서 실패가 발생한 경우)" +
                    " 성공한 레코드는 'success' 관계로 전달되고 원본 입력 FlowFile은 유입 큐에 그대로 남는다." +
                    " 동일한 FlowFile이 다시 처리될 때 이미 성공한 레코드에 대해 중복 레코드가 생성될 수 있다.");
    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos 사용자 서비스")
            .description("Kerberos 인증에 사용할 Kerberos User 컨트롤러 서비스를 지정한다")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();
    private static final String CLIENT_CACHE_DISABLED_PROPERTY = "hcatalog.hive.client.cache.disabled";
    final protected AtomicReference<KerberosUser> kerberosUserReference = new AtomicReference<>();
    // 검증 시 동일한 설정을 반복해서 다시 로드하지 않도록 캐시된 Configuration 정보를 보관한다
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();
    protected volatile HiveConfigurator hiveConfigurator = new HiveConfigurator();
    protected volatile UserGroupInformation ugi;
    protected volatile HiveConf hiveConfig;
    protected volatile int callTimeout;
    protected ExecutorService callTimeoutPool;
    protected volatile boolean rollbackOnFailure;
    private List<PropertyDescriptor> propertyDescriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(RECORD_READER);
        props.add(METASTORE_URI);
        props.add(HIVE_CONFIGURATION_RESOURCES);
        props.add(DB_NAME);
        props.add(TABLE_NAME);
        props.add(STATIC_PARTITION_VALUES);
        props.add(RECORDS_PER_TXN);
        props.add(TXNS_PER_BATCH);
        props.add(CALL_TIMEOUT);
        props.add(DISABLE_STREAMING_OPTIMIZATIONS);
        props.add(ROLLBACK_ON_FAILURE);
        props.add(KERBEROS_USER_SERVICE);

        propertyDescriptors = Collections.unmodifiableList(props);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
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
     * Hive Configuration Resources 프로퍼티가 지정된 경우, 설정 파일 로드와 Kerberos 설정의 일관성을 검증한다.
     * 검증 결과는 캐시(validationResourceHolder)를 이용해 동일한 설정 파일을 반복해서 다시 읽지 않도록 한다.
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        boolean confFileProvided = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).isSet();

        final List<ValidationResult> problems = new ArrayList<>();

        if (confFileProvided) {
            final KerberosUserService kerberosUserService = validationContext.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            final String configFiles = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            problems.addAll(hiveConfigurator.validate(configFiles, kerberosUserService, validationResourceHolder, getLogger()));
        }
        return problems;
    }

    /**
     * 프로세서 스케줄링 시점에 Hive 설정을 로드하고 필요 시 Kerberos 인증을 수행한다.
     * 이 메서드에서 얻은 UserGroupInformation(ugi)은 onTrigger에서 doAs(...)로 감싸는 데 사용되어
     * 이후의 모든 Hive 작업이 인증된 사용자 컨텍스트에서 실행되도록 보장한다.
     */
    @OnScheduled
    public void setup(final ProcessContext context) throws IOException {
        ComponentLog log = getLogger();
        rollbackOnFailure = context.getProperty(ROLLBACK_ON_FAILURE).asBoolean();

        final String configFiles = context.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        hiveConfig = hiveConfigurator.getConfigurationFromFiles(configFiles);

        // 동시 실행 태스크가 2개 이상이면 'hcatalog.hive.client.cache.disabled'를 true로 강제 설정하여 동시성 문제를 방지한다
        if (context.getMaxConcurrentTasks() > 1) {
            hiveConfig.setBoolean(CLIENT_CACHE_DISABLED_PROPERTY, true);
        }

        // 동적(dynamic) 프로퍼티들을 Hive 설정에 추가로 반영한다
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                hiveConfig.set(descriptor.getName(), entry.getValue());
            }
        }

        hiveConfigurator.preload(hiveConfig);

        if (SecurityUtil.isSecurityEnabled(hiveConfig)) {
            final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            if (kerberosUserService == null) {
                throw new ProcessException("Kerberos authentication is required by the specified configuration, Kerberos User Service must be configured");
            }
            final KerberosUser kerberosUser = kerberosUserService.createKerberosUser();
            kerberosUserReference.set(kerberosUser);
            log.info("Hive Security Enabled, logging in as principal {}", kerberosUser.getPrincipal());

            try {
                ugi = hiveConfigurator.authenticate(hiveConfig, kerberosUserReference.get());
            } catch (AuthenticationFailedException ae) {
                log.error(ae.getMessage(), ae);
                throw new ProcessException(ae);
            }

            log.info("Successfully logged in as principal " + kerberosUser.getPrincipal());
        } else {
            ugi = SecurityUtil.loginSimple(hiveConfig);
            kerberosUserReference.set(null);
        }

        callTimeout = context.getProperty(CALL_TIMEOUT).evaluateAttributeExpressions().asInteger() * 1000; // milliseconds
        String timeoutName = "put-hive3-streaming-%d";
        this.callTimeoutPool = Executors.newFixedThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat(timeoutName).build());
    }

    /**
     * 유입 FlowFile을 Record Reader로 읽어 레코드 단위로 Hive Streaming Connection에 기록한다.
     * Kerberos 보안이 활성화된 경우 UGI(UserGroupInformation).doAs(...)로 전체 처리를 감싸서
     * 인증된 사용자 컨텍스트에서 Hive Metastore/HDFS 접근이 이루어지도록 한다.
     * Records per Transaction 단위로 트랜잭션을 나누어 커밋하며, 예외 종류에 따라
     * success/failure/retry/SELF 관계 중 하나로 FlowFile을 라우팅한다.
     */
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        getUgi().doAs((PrivilegedAction<Void>) () -> {
            FlowFile flowFile = session.get();
            if (flowFile == null) {
                return null;
            }

            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final String dbName = context.getProperty(DB_NAME).evaluateAttributeExpressions(flowFile).getValue();
            final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();

            final ComponentLog log = getLogger();
            String metastoreURIs = null;
            if (context.getProperty(METASTORE_URI).isSet()) {
                metastoreURIs = context.getProperty(METASTORE_URI).evaluateAttributeExpressions(flowFile).getValue();
                if (StringUtils.isEmpty(metastoreURIs)) {
                    // 이 시점에는 비어 있으면 안 되는 값이므로, 에러를 기록하고 FlowFile에 페널티를 부여한 후 반환한다
                    log.error("The '" + METASTORE_URI.getDisplayName() + "' property evaluated to null or empty, penalizing flow file, routing to failure");
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                }
            }

            final String staticPartitionValuesString = context.getProperty(STATIC_PARTITION_VALUES).evaluateAttributeExpressions(flowFile).getValue();
            final boolean disableStreamingOptimizations = context.getProperty(DISABLE_STREAMING_OPTIMIZATIONS).asBoolean();

            // 사용자가 설정한 경우 설정 내의 Hive Metastore URI를 재정의(override)한다
            if (metastoreURIs != null) {
                hiveConfig.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreURIs);
            }

            final int recordsPerTransaction = context.getProperty(RECORDS_PER_TXN).evaluateAttributeExpressions(flowFile).asInteger();
            final int transactionsPerBatch = context.getProperty(TXNS_PER_BATCH).evaluateAttributeExpressions(flowFile).asInteger();

            HiveOptions o = new HiveOptions(metastoreURIs, dbName, tableName)
                    .withHiveConf(hiveConfig)
                    .withCallTimeout(callTimeout)
                    .withStreamingOptimizations(!disableStreamingOptimizations)
                    .withTransactionBatchSize(transactionsPerBatch);

            if (!StringUtils.isEmpty(staticPartitionValuesString)) {
                List<String> staticPartitionValues = Arrays.stream(staticPartitionValuesString.split(",")).filter(Objects::nonNull).map(String::trim).collect(Collectors.toList());
                o = o.withStaticPartitionValues(staticPartitionValues);
            }

            if (SecurityUtil.isSecurityEnabled(hiveConfig)) {
                final KerberosUser kerberosUser = kerberosUserReference.get();
                if (kerberosUser != null) {
                    o = o.withKerberosPrincipal(kerberosUser.getPrincipal());
                }
            }

            final HiveOptions options = o;

            // 원래의 클래스 로더를 보존해 두고, Hive Metastore가 사용할 수 있도록 이 클래스의 클래스 로더로 명시적으로 전환한다
            ClassLoader originalClassloader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

            StreamingConnection hiveStreamingConnection = null;

            try {
                final RecordReader reader;

                try (final InputStream in = session.read(flowFile)) {
                    // RecordReader 생성에 실패하면 failure로 라우팅해야 하므로, 일반적으로 retry로
                    // 라우팅되는 다른 IOException들과는 별도로 처리해야 한다
                    try {
                        reader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
                    } catch (Exception e) {
                        throw new RecordReaderFactoryException("Unable to create RecordReader", e);
                    }

                    hiveStreamingConnection = makeStreamingConnection(options, reader, recordsPerTransaction);

                    // 레코드를 Hive Streaming으로 기록한 후 커밋하고 닫는다
                    boolean exitLoop = false;
                    while (!exitLoop) {
                        hiveStreamingConnection.beginTransaction();
                        // HiveRecordWriter는 트랜잭션당 레코드 개수를 추적하며, 한도에 도달하면
                        // 해당 트랜잭션의 기록을 완료한다. 이후 다음 반복을 위해 다시 초기화된다.
                        try {
                            hiveStreamingConnection.write(in);
                        } catch (RecordsEOFException reofe) {
                            exitLoop = true;
                        }
                        hiveStreamingConnection.commitTransaction();
                    }
                    in.close();

                    Map<String, String> updateAttributes = new HashMap<>();
                    updateAttributes.put(HIVE_STREAMING_RECORD_COUNT_ATTR, Long.toString(hiveStreamingConnection.getConnectionStats().getRecordsWritten()));
                    updateAttributes.put(ATTR_OUTPUT_TABLES, options.getQualifiedTableName());
                    flowFile = session.putAllAttributes(flowFile, updateAttributes);
                    session.getProvenanceReporter().send(flowFile, hiveStreamingConnection.getMetastoreUri());
                } catch (TransactionError te) {
                    if (rollbackOnFailure) {
                        throw new ProcessException(te.getLocalizedMessage(), te);
                    } else {
                        throw new ShouldRetryException(te.getLocalizedMessage(), te);
                    }
                } catch (RecordReaderFactoryException rrfe) {
                    if (rollbackOnFailure) {
                        throw new ProcessException(rrfe);
                    } else {
                        log.error(
                                "Failed to create {} for {} - routing to failure",
                                RecordReader.class.getSimpleName(), flowFile,
                                rrfe
                        );
                        session.transfer(flowFile, REL_FAILURE);
                        return null;
                    }
                }
                session.transfer(flowFile, REL_SUCCESS);
            } catch (InvalidTable | SerializationError | StreamingIOFailure | IOException e) {
                if (rollbackOnFailure) {
                    if (hiveStreamingConnection != null) {
                        abortConnection(hiveStreamingConnection);
                    }
                    throw new ProcessException(e.getLocalizedMessage(), e);
                } else {
                    Map<String, String> updateAttributes = new HashMap<>();
                    final String recordCountAttribute = (hiveStreamingConnection != null) ? Long.toString(hiveStreamingConnection.getConnectionStats().getRecordsWritten()) : "0";
                    updateAttributes.put(HIVE_STREAMING_RECORD_COUNT_ATTR, recordCountAttribute);
                    updateAttributes.put(ATTR_OUTPUT_TABLES, options.getQualifiedTableName());
                    flowFile = session.putAllAttributes(flowFile, updateAttributes);
                    log.error(
                            "Exception while processing {} - routing to failure",
                            flowFile,
                            e
                    );
                    session.transfer(flowFile, REL_FAILURE);
                }
            } catch (DiscontinuedException e) {
                // 입력 FlowFile 처리가 중단되었다. 입력 큐에 그대로 남겨둔다.
                getLogger().warn("Discontinued processing for {} due to {}", flowFile, e, e);
                session.transfer(flowFile, Relationship.SELF);
            } catch (ConnectionError ce) {
                // 메타스토어에 연결할 수 없는 경우 프로세서를 yield 시킨다
                context.yield();
                throw new ProcessException("A connection to metastore cannot be established", ce);
            } catch (ShouldRetryException e) {
                // 이 예외는 이미 다른 에러를 재분류한 결과이므로 단순히 FlowFile을 retry로 전달하면 된다. 다만 트랜잭션은 abort해야 한다.
                getLogger().error(e.getLocalizedMessage(), e);
                if (hiveStreamingConnection != null) {
                    abortConnection(hiveStreamingConnection);
                }
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_RETRY);
            } catch (StreamingException se) {
                // 그 외의 모든 예외를 처리한다. 대부분 레코드 단위의 예외이다 (Hive가 위에서 잡은 예외의 하위 클래스를 던지기 때문)
                Throwable cause = se.getCause();
                if (cause == null) cause = se;
                // 유입 데이터 자체의 문제로 인한 실패이므로, 설정에 따라 rollback하거나 페널티 부여 후 failure로 라우팅한다 (어느 경우든 트랜잭션은 abort한다)
                if (rollbackOnFailure) {
                    if (hiveStreamingConnection != null) {
                        abortConnection(hiveStreamingConnection);
                    }
                    throw new ProcessException(cause.getLocalizedMessage(), cause);
                } else {
                    flowFile = session.penalize(flowFile);
                    Map<String, String> updateAttributes = new HashMap<>();
                    final String recordCountAttribute = (hiveStreamingConnection != null) ? Long.toString(hiveStreamingConnection.getConnectionStats().getRecordsWritten()) : "0";
                    updateAttributes.put(HIVE_STREAMING_RECORD_COUNT_ATTR, recordCountAttribute);
                    updateAttributes.put(ATTR_OUTPUT_TABLES, options.getQualifiedTableName());
                    flowFile = session.putAllAttributes(flowFile, updateAttributes);
                    log.error(
                            "Exception while trying to stream {} to hive - routing to failure",
                            flowFile,
                            se
                    );
                    session.transfer(flowFile, REL_FAILURE);
                }

            } catch (Throwable t) {
                if (hiveStreamingConnection != null) {
                    abortConnection(hiveStreamingConnection);
                }
                throw (t instanceof ProcessException) ? (ProcessException) t : new ProcessException(t);
            } finally {
                closeConnection(hiveStreamingConnection);
                // 원래의 클래스 로더를 복원한다. 필수는 아닐 수 있으나, 프로세서 태스크가 클래스 로더를 변경했으므로 원상 복구하는 것이 바람직한 습관이다
                Thread.currentThread().setContextClassLoader(originalClassloader);
            }
            return null;
        });
    }

    /**
     * HiveStreamingConnection을 생성하고 연결까지 완료한다.
     * RecordReader를 HiveRecordWriter로 감싸 Hive Streaming API가 레코드를 기록할 수 있도록 연결한다.
     */
    StreamingConnection makeStreamingConnection(HiveOptions options, RecordReader reader, int recordsPerTransaction) throws StreamingException {
        return HiveStreamingConnection.newBuilder()
                .withDatabase(options.getDatabaseName())
                .withTable(options.getTableName())
                .withStaticPartitionValues(options.getStaticPartitionValues())
                .withHiveConf(options.getHiveConf())
                .withRecordWriter(new HiveRecordWriter(reader, getLogger(), recordsPerTransaction))
                .withTransactionBatchSize(options.getTransactionBatchSize())
                .withAgentInfo("NiFi " + this.getClass().getSimpleName() + " [" + this.getIdentifier()
                        + "] thread " + Thread.currentThread().getId() + "[" + Thread.currentThread().getName() + "]")
                .connect();
    }

    /**
     * 프로세서가 중지될 때 스레드 풀, UGI, Kerberos 사용자 정보 등 스케줄링 중에 확보한 자원을 정리한다.
     */
    @OnStopped
    public void cleanup() {
        validationResourceHolder.set(null); // 리소스 재검증을 트리거한다

        ComponentLog log = getLogger();

        if (callTimeoutPool != null) {
            callTimeoutPool.shutdown();
            try {
                while (!callTimeoutPool.isTerminated()) {
                    callTimeoutPool.awaitTermination(callTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (Throwable t) {
                log.warn("shutdown interrupted on " + callTimeoutPool, t);
            }
            callTimeoutPool = null;
        }

        ugi = null;
        kerberosUserReference.set(null);
    }

    private void abortAndCloseConnection(StreamingConnection connection) {
        try {
            abortConnection(connection);
            closeConnection(connection);
        } catch (Exception ie) {
            getLogger().warn("unable to close hive connections. ", ie);
        }
    }

    /**
     * 커넥션에서 진행 중인 트랜잭션을 중단(abort)한다
     */
    private void abortConnection(StreamingConnection connection) {
        if (connection != null) {
            try {
                connection.abortTransaction();
            } catch (Exception e) {
                getLogger().error("Failed to abort Hive Streaming transaction " + connection + " due to exception ", e);
            }
        }
    }

    /**
     * 스트리밍 커넥션을 닫는다
     */
    private void closeConnection(StreamingConnection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                getLogger().error("Failed to close Hive Streaming connection " + connection + " due to exception ", e);
            }
        }
    }

    /**
     * 현재 UserGroupInformation(UGI)을 반환한다. 연관된 KerberosUser가 있다면 TGT(Ticket Granting Ticket)의
     * 만료 임박 여부를 확인하여 필요 시 자동으로 재로그인(relogin)함으로써, 장시간 실행되는 프로세서에서도
     * Kerberos 인증이 끊기지 않도록 한다.
     */
    UserGroupInformation getUgi() {
        getLogger().trace("getting UGI instance");
        if (kerberosUserReference.get() != null) {
            // 이 UGI에 연관된 KerberosUser가 있다면, TGT를 확인하고 만료가 임박한 경우 재로그인한다
            KerberosUser kerberosUser = kerberosUserReference.get();
            getLogger().debug("kerberosUser is " + kerberosUser);
            try {
                getLogger().debug("checking TGT on kerberosUser [{}]", kerberosUser);
                kerberosUser.checkTGTAndRelogin();
            } catch (final KerberosLoginException e) {
                throw new ProcessException("Unable to relogin with kerberos credentials for " + kerberosUser.getPrincipal(), e);
            }
        } else {
            getLogger().debug("kerberosUser was null, will not refresh TGT with KerberosUser");
        }
        return ugi;
    }

    /**
     * TransactionError를 rollbackOnFailure가 비활성화된 상태에서 재분류하기 위한 내부 시그널용 예외.
     * onTrigger의 catch 블록에서 이 예외를 감지하여 FlowFile을 REL_RETRY로 라우팅한다.
     */
    private static class ShouldRetryException extends RuntimeException {
        private ShouldRetryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

