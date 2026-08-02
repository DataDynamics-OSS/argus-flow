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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/processors/hive/TriggerHiveMetaStoreEvent.java
 */
package io.datadynamics.nifi.processors.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import io.datadynamics.nifi.processors.hive.util.AuthenticationFailedException;
import io.datadynamics.nifi.processors.hive.util.ValidationResources;
import org.apache.thrift.TException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HiveMetaStore에 직접 이벤트를 발생(fire)시켜 메타스토어 알림(notification)을 생성하는 프로세서.
 * 파일 시스템에 데이터가 적재(insert)되거나 삭제(delete)되었을 때, 실제 파일 이동과는 별개로
 * Hive 메타스토어에 삽입/파티션 추가/파티션 삭제 이벤트를 알려 다운스트림 리스너(예: 캡처/CDC 도구)가
 * 이를 인지할 수 있도록 한다. 파티션 테이블의 경우 파티션 존재 여부에 따라 INSERT 또는 ADD_PARTITION
 * 이벤트로 분기 처리한다.
 */
@Tags({"cloudera", "hive", "metastore", "notification", "insert", "delete", "partition", "event"})
@CapabilityDescription("이 프로세서는 HiveMetaStore에서 다양한 유형의 이벤트를 발생시켜 알림을 생성할 수 있다. " +
        "실행할 메타스토어 작업은 입력으로 들어온 path 및 event type 속성으로 결정된다. " +
        "지원되는 이벤트 타입 값은 데이터 삽입의 경우 'put', 데이터 삭제의 경우 'delete'이다. " +
        "이러한 알림을 생성하려면 메타스토어 설정에서 알림 기능이 활성화되어 있어야 한다. 예를 들어 'hive.metastore.transactional.event.listeners' " +
        "속성에 'org.apache.hive.hcatalog.listener.DbNotificationListener'와 같은 적절한 리스너가 설정되어 있어야 한다.")
@WritesAttributes({
        @WritesAttribute(attribute = "metastore.notification.event", description = "발생시킨 알림의 이벤트 타입.")
})
public class TriggerHiveMetaStoreEvent extends AbstractProcessor {

    public static final String METASTORE_NOTIFICATION_EVENT = "metastore.notification.event";
    static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("hive-metastore-uri")
            .displayName("Hive 메타스토어 URI")
            .description("Hive 메타스토어의 URI 위치. 쉼표로 구분된 Hive 메타스토어 URI 목록이며, Hive Server의 위치가 아님에 유의한다. "
                    + "Hive 메타스토어의 기본 포트는 9043이다. 이 필드를 설정하지 않으면 제공된 설정 리소스의 'hive.metastore.uris' 속성이 "
                    + "사용되며, 그마저도 없으면 기본 hive-site.xml의 기본값(보통 thrift://localhost:9083)이 사용된다.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .build();
    static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hive-config-resources")
            .displayName("Hive 설정 리소스")
            .description("Hive 설정(예: hive-site.xml)을 포함하는 파일 또는 쉼표로 구분된 파일 목록. 이를 설정하지 않으면 Hadoop이 "
                    + "클래스패스에서 'hive-site.xml' 파일을 찾거나 기본 설정으로 대체한다. Kerberos 인증 등을 활성화하려면 "
                    + "설정 파일에 적절한 속성이 설정되어 있어야 함에 유의한다. 자세한 내용은 Hive 문서를 참고한다.")
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    static final PropertyDescriptor EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("event-type")
            .displayName("이벤트 유형")
            .description("이벤트의 타입. 데이터 삽입의 경우 'put', 데이터 삭제의 경우 'delete' 값을 사용할 수 있다.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${event.type}")
            .build();
    static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
            .name("path")
            .displayName("경로")
            .description("파일 시스템에 위치한 파일 또는 폴더의 경로.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${path}")
            .build();
    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("catalog-name")
            .displayName("카탈로그명")
            .description("카탈로그의 이름.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("database-name")
            .displayName("데이터베이스명")
            .description("데이터베이스의 이름.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("테이블명")
            .description("테이블의 이름.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos 사용자 서비스")
            .description("Kerberos 인증에 사용할 Kerberos User 컨트롤러 서비스를 지정한다.")
            .identifiesControllerService(KerberosUserService.class)
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("데이터 적재가 성공한 후 FlowFile이 이 관계로 라우팅된다.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("데이터 적재가 실패했고 잘못된 데이터나 스키마 등 재시도해도 다시 실패할 것으로 판단되는 경우 FlowFile이 이 관계로 라우팅된다.")
            .build();
    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));
    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            METASTORE_URI,
            HIVE_CONFIGURATION_RESOURCES,
            EVENT_TYPE,
            PATH,
            CATALOG_NAME,
            DATABASE_NAME,
            TABLE_NAME,
            KERBEROS_USER_SERVICE
    ));
    private final HiveConfigurator hiveConfigurator = new HiveConfigurator();
    private final AtomicReference<KerberosUser> kerberosUserReference = new AtomicReference<>();
    // 검증 시마다 동일한 설정을 반복해서 로드하지 않도록 캐시된 Configuration 정보를 보관하는 홀더
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();
    private Configuration hiveConfig;
    private volatile UserGroupInformation ugi;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    // Hive 설정 파일에서 Kerberos 보안이 활성화되어 있는데도 Kerberos User Service가 구성되지 않은 경우를
    // 검증 단계에서 미리 잡아내어, 프로세서 실행 중 인증 실패로 이어지지 않도록 한다.
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>();
        final boolean confFileProvided = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).isSet();

        if (confFileProvided) {
            final boolean kerberosUserServiceIsSet = validationContext.getProperty(KERBEROS_USER_SERVICE).isSet();
            final String configFiles = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            final Configuration config = hiveConfigurator.getConfigurationForValidation(validationResourceHolder, configFiles, getLogger());

            final boolean securityEnabled = SecurityUtil.isSecurityEnabled(config);
            if (securityEnabled && !kerberosUserServiceIsSet) {
                problems.add(new ValidationResult.Builder()
                        .subject(KERBEROS_USER_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("설정 파일에 보안 인증 방식이 'kerberos'로 지정되어 있지만 KerberosUserService가 구성되어 있지 않다.")
                        .build());
            }
        }

        return problems;
    }

    // 프로세서가 스케줄링될 때(실행 시작 시) 한 번 호출되어 Hive 설정을 로드하고,
    // METASTORE_URI 프로퍼티가 지정된 경우 이를 Thrift URI 설정에 덮어쓰며,
    // Kerberos User Service가 연결되어 있으면 인증을 수행해 UGI(UserGroupInformation)를 준비한다.
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final String configFiles = context.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        hiveConfig = hiveConfigurator.getConfigurationFromFiles(configFiles);

        if (context.getProperty(METASTORE_URI).isSet()) {
            hiveConfig.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), context.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue());
        }

        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosUserService != null) {
            kerberosUserReference.set(kerberosUserService.createKerberosUser());
            try {
                ugi = hiveConfigurator.authenticate(hiveConfig, kerberosUserReference.get());
            } catch (AuthenticationFailedException e) {
                getLogger().error(e.getMessage(), e);
                throw new ProcessException(e);
            }
        }
    }

    // 큐에서 FlowFile을 꺼내 실제 처리(doOnTrigger)를 수행한다.
    // Kerberos User가 설정되어 있으면 UGI.doAs를 통해 인증된 컨텍스트 안에서 처리하고,
    // 그렇지 않으면 별도 인증 없이 바로 처리한다.
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final KerberosUser kerberosUser = kerberosUserReference.get();
        if (kerberosUser == null) {
            doOnTrigger(context, session, flowFile);
        } else {
            try {
                getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
                    doOnTrigger(context, session, flowFile);
                    return null;
                });

            } catch (Exception e) {
                getLogger().error("Privileged action failed with kerberos user " + kerberosUser, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    // 실제 메타스토어 이벤트 처리 로직.
    // 대상 테이블을 조회하여 파티션 테이블인지 확인한 뒤, event-type 속성 값('put'/'delete')에 따라
    // 삽입(파티션 여부에 따라 INSERT 또는 ADD_PARTITION) 또는 파티션 삭제(DROP_PARTITION) 이벤트를 발생시킨다.
    // 처리 결과에 따라 FlowFile을 REL_SUCCESS 또는 REL_FAILURE로 라우팅한다.
    public void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        String catalogName;
        if (context.getProperty(CATALOG_NAME).isSet()) {
            catalogName = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions().getValue();
        } else {
            catalogName = MetaStoreUtils.getDefaultCatalog(hiveConfig);
        }

        final String eventType = context.getProperty(EVENT_TYPE).evaluateAttributeExpressions(flowFile).getValue();
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String path = context.getProperty(PATH).evaluateAttributeExpressions(flowFile).getValue();

        try (final HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConfig)) {
            final Table table = metaStoreClient.getTable(catalogName, databaseName, tableName);
            final boolean isPartitioned = !table.getPartitionKeys().isEmpty();

            switch (eventType.toLowerCase().trim()) {
                case "put":
                    if (isPartitioned) {
                        EventMessage.EventType eventMessageType = handlePartitionedInsert(metaStoreClient, catalogName, databaseName, tableName, path);
                        flowFile = session.putAttribute(flowFile, METASTORE_NOTIFICATION_EVENT, eventMessageType.toString());
                    } else {
                        handleFileInsert(metaStoreClient, catalogName, databaseName, tableName, path, null);
                        flowFile = session.putAttribute(flowFile, METASTORE_NOTIFICATION_EVENT, EventMessage.EventType.INSERT.toString());
                    }
                    break;
                case "delete":
                    if (isPartitioned) {
                        handleDropPartition(metaStoreClient, catalogName, databaseName, tableName, path);
                        flowFile = session.putAttribute(flowFile, METASTORE_NOTIFICATION_EVENT, EventMessage.EventType.DROP_PARTITION.toString());
                    } else {
                        getLogger().warn("The target table '{}' is not partitioned. No metastore action was executed.", tableName);
                    }
                    break;
                default:
                    getLogger().error("Unknown event type '{}'", eventType);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
            }
        } catch (Exception e) {
            getLogger().error("Error occurred while metastore event processing", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * 파티션 테이블에 대한 삽입 이벤트를 처리한다. 파티션이 이미 존재하면 메타스토어에 삽입 이벤트를 실행하고,
     * 존재하지 않으면 테이블에 새 파티션을 추가한다.
     *
     * @param catalogName  카탈로그 이름
     * @param databaseName 데이터베이스 이름
     * @param tableName    테이블 이름
     * @param path         파일 또는 디렉토리의 경로
     * @return 실행된 이벤트의 타입
     * @throws TException 메타스토어 작업이 실패한 경우 발생
     */
    private EventMessage.EventType handlePartitionedInsert(HiveMetaStoreClient metaStoreClient, String catalogName,
                                                           String databaseName, String tableName, String path) throws TException {
        final List<String> partitionValues = getPartitionValuesFromPath(path);

        try {
            // Check if the partition already exists for the file to be inserted
            metaStoreClient.getPartition(databaseName, tableName, partitionValues);
            getLogger().debug("Creating file insert for partition with values {}", partitionValues);
            handleFileInsert(metaStoreClient, catalogName, databaseName, tableName, path, partitionValues);
            return EventMessage.EventType.INSERT;
        } catch (Exception e) {
            if (e instanceof NoSuchObjectException) {
                getLogger().debug("Partition with values {} does not exists. Trying to append new partition", partitionValues, e);
                try {
                    metaStoreClient.appendPartition(catalogName, databaseName, tableName, partitionValues);
                    return EventMessage.EventType.ADD_PARTITION;
                } catch (TException ex) {
                    throw new TException("Failed to append partition with values " + partitionValues, ex);
                }
            }
            throw new TException("Error occurred during partitioned file insertion with values " + partitionValues, e);
        }
    }

    /**
     * 파일 또는 디렉토리 경로에서 파티션 값을 파싱한다.
     * 예: hdfs://localhost:9000/user/hive/warehouse/table/a=5/b=10/file -> 파티션 값은 [5,10]
     *
     * @param path 파일 또는 디렉토리의 경로
     * @return 파티션 값 목록
     */
    private List<String> getPartitionValuesFromPath(String path) {
        final String[] pathParts = path.split("/");
        List<String> partitionValues = new ArrayList<>();
        for (String pathPart : pathParts) {
            if (pathPart.contains("=")) {
                final String[] partitionParts = pathPart.split("=");
                partitionValues.add(partitionParts[1]);
            }
        }
        getLogger().debug("The following partition values were processed from path '{}': {}", path, partitionValues);
        return partitionValues;
    }

    /**
     * 파일 삽입 이벤트를 구성하여 메타스토어로 전송한다.
     *
     * @param catalogName     카탈로그 이름
     * @param databaseName    데이터베이스 이름
     * @param tableName       테이블 이름
     * @param path            파일 또는 디렉토리의 경로
     * @param partitionValues 파티션 값 목록
     * @throws IOException 체크섬 생성 중 오류가 발생한 경우
     * @throws TException  메타스토어 작업이 실패한 경우 발생
     */
    private void handleFileInsert(HiveMetaStoreClient metaStoreClient, String catalogName, String databaseName,
                                  String tableName, String path, List<String> partitionValues) throws IOException, TException {
        final InsertEventRequestData insertEventRequestData = new InsertEventRequestData();
        insertEventRequestData.setReplace(false);
        insertEventRequestData.addToFilesAdded(path);
        insertEventRequestData.addToFilesAddedChecksum(checksumFor(path));

        final FireEventRequestData fireEventRequestData = new FireEventRequestData();
        fireEventRequestData.setInsertData(insertEventRequestData);

        final FireEventRequest fireEventRequest = new FireEventRequest(true, fireEventRequestData);
        fireEventRequest.setCatName(catalogName);
        fireEventRequest.setDbName(databaseName);
        fireEventRequest.setTableName(tableName);
        if (partitionValues != null) {
            fireEventRequest.setPartitionVals(partitionValues);
        }

        metaStoreClient.fireListenerEvent(fireEventRequest);
    }

    /**
     * 주어진 경로에서 파싱한 파티션 값으로 메타스토어에 파티션 삭제(drop) 이벤트를 발생시킨다.
     *
     * @param catalogName  카탈로그 이름
     * @param databaseName 데이터베이스 이름
     * @param tableName    테이블 이름
     * @param path         파일 또는 디렉토리의 경로
     * @throws TException 메타스토어 작업이 실패한 경우 발생
     */
    private void handleDropPartition(HiveMetaStoreClient metaStoreClient, String catalogName, String databaseName,
                                     String tableName, String path) throws TException {
        final List<String> partitionValues = getPartitionValuesFromPath(path);
        try {
            metaStoreClient.dropPartition(catalogName, databaseName, tableName, partitionValues, true);
        } catch (TException e) {
            if (e instanceof NoSuchObjectException) {
                getLogger().error("Failed to drop partition. Partition with values {} does not exists.", partitionValues, e);
            }
            throw new TException(e);
        }
    }

    /**
     * FileSystem이 지원하는 경우, 주어진 경로의 파일에 대한 체크섬을 생성한다.
     *
     * @param filePath 파일 경로
     * @return 파일의 체크섬.
     * @throws IOException FileSystem을 가져오는 중 문제가 발생한 경우
     */
    private String checksumFor(String filePath) throws IOException {
        final Path path = new Path(filePath);
        final FileSystem fileSystem = path.getFileSystem(hiveConfig);
        final FileChecksum checksum = fileSystem.getFileChecksum(path);
        if (checksum != null) {
            return StringUtils.byteToHexString(checksum.getBytes(), 0, checksum.getLength());
        }
        return "";
    }

    /**
     * 사용자 TGT(Ticket Granting Ticket)가 만료되었는지 확인하고, 필요한 경우 재로그인을 수행한다.
     *
     * @return ugi
     */
    private UserGroupInformation getUgi() {
        KerberosUser kerberosUser = kerberosUserReference.get();
        try {
            kerberosUser.checkTGTAndRelogin();
        } catch (KerberosLoginException e) {
            throw new ProcessException("Unable to re-login with kerberos credentials for " + kerberosUser.getPrincipal(), e);
        }
        return ugi;
    }
}
