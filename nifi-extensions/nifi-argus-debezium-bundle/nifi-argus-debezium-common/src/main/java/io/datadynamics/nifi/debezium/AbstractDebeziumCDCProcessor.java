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
package io.datadynamics.nifi.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Debezium Embedded Engine을 감싸는 CDC 소스 프로세서의 공통 베이스.
 *
 * <p>커넥터별 하위 클래스는 {@link #getConnectorClass()}와 커넥터 고유 설정만 제공하고,
 * 엔진 수명주기 관리·변경 이벤트 큐잉·FlowFile 방출은 이 클래스가 담당한다.</p>
 *
 * <p>전달 보장: 오프셋은 {@code offset.flush.interval.ms} 주기로 파일에 flush되므로,
 * 재기동 시 마지막 flush 이후 이벤트가 재전송될 수 있다(at-least-once). 하위 컨슈머는
 * 이벤트의 LSN/GTID로 중복을 제거해야 한다.</p>
 */
public abstract class AbstractDebeziumCDCProcessor extends AbstractProcessor {

    public static final PropertyDescriptor TOPIC_PREFIX = new PropertyDescriptor.Builder()
            .name("topic-prefix")
            .displayName("토픽 접두사")
            .description("이 CDC 소스의 논리 이름. Debezium의 topic.prefix로 매핑되며, 오프셋/스키마 이력 네임스페이스를 결정한다. 커넥터마다 고유해야 한다.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DATABASE_HOSTNAME = new PropertyDescriptor.Builder()
            .name("database-hostname")
            .displayName("데이터베이스 호스트명")
            .description("소스 데이터베이스 호스트명 또는 IP.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DATABASE_PORT = new PropertyDescriptor.Builder()
            .name("database-port")
            .displayName("데이터베이스 포트")
            .description("소스 데이터베이스 포트.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DATABASE_USER = new PropertyDescriptor.Builder()
            .name("database-user")
            .displayName("데이터베이스 사용자")
            .description("CDC 권한을 가진 데이터베이스 사용자.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DATABASE_PASSWORD = new PropertyDescriptor.Builder()
            .name("database-password")
            .displayName("데이터베이스 비밀번호")
            .description("데이터베이스 사용자 비밀번호.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor TABLE_INCLUDE_LIST = new PropertyDescriptor.Builder()
            .name("table-include-list")
            .displayName("테이블 포함 목록")
            .description("캡처할 테이블의 정규식 목록(쉼표 구분). Debezium table.include.list로 매핑. 비우면 전체 대상.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SNAPSHOT_MODE = new PropertyDescriptor.Builder()
            .name("snapshot-mode")
            .displayName("스냅샷 모드")
            .description("초기 스냅샷 전략. Debezium snapshot.mode로 매핑(예: initial, no_data, when_needed, never). 커넥터별 지원 값이 다를 수 있다.")
            .required(false)
            .defaultValue("initial")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor OFFSET_STORAGE_DIR = new PropertyDescriptor.Builder()
            .name("offset-storage-dir")
            .displayName("저장 디렉토리")
            .description("오프셋과 스키마 이력 파일을 저장할 로컬 디렉토리. 프로세서(그리고 클러스터의 해당 노드)마다 고유해야 하며 재기동 간 보존되어야 한다.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor OFFSET_FLUSH_INTERVAL = new PropertyDescriptor.Builder()
            .name("offset-flush-interval")
            .displayName("오프셋 Flush 주기")
            .description("오프셋을 저장소에 flush하는 주기. Debezium offset.flush.interval.ms로 매핑.")
            .required(true)
            .defaultValue("60 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("max-batch-size")
            .displayName("최대 배치 크기")
            .description("onTrigger 1회당 큐에서 방출할 최대 변경 이벤트 수.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("캡처된 변경 이벤트가 이 관계로 전송된다. FlowFile 본문은 Debezium이 직렬화한 JSON 변경 이벤트다.")
            .build();

    private static final int QUEUE_CAPACITY = 10_000;

    private volatile DebeziumEngine<ChangeEvent<String, String>> engine;
    private volatile ExecutorService executor;
    private final LinkedBlockingQueue<ChangeEvent<String, String>> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicReference<Throwable> engineFailure = new AtomicReference<>();

    /** 커넥터 구현체의 완전한 클래스명(예: io.debezium.connector.mysql.MySqlConnector). */
    protected abstract String getConnectorClass();

    /** 커넥터 고유 프로퍼티 디스크립터. */
    protected abstract List<PropertyDescriptor> getConnectorPropertyDescriptors();

    /** 컨텍스트로부터 커넥터 고유 Debezium 설정 키를 채운다. */
    protected abstract void addConnectorConfiguration(ProcessContext context, Properties props);

    /** 스키마 이력 저장소 사용 여부. RDBMS 커넥터는 대부분 true, PostgreSQL은 false. */
    protected boolean requiresSchemaHistory() {
        return true;
    }

    protected List<PropertyDescriptor> getCommonPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(TOPIC_PREFIX);
        descriptors.add(DATABASE_HOSTNAME);
        descriptors.add(DATABASE_PORT);
        descriptors.add(DATABASE_USER);
        descriptors.add(DATABASE_PASSWORD);
        descriptors.add(TABLE_INCLUDE_LIST);
        descriptors.add(SNAPSHOT_MODE);
        descriptors.add(OFFSET_STORAGE_DIR);
        descriptors.add(OFFSET_FLUSH_INTERVAL);
        descriptors.add(MAX_BATCH_SIZE);
        return descriptors;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(getCommonPropertyDescriptors());
        descriptors.addAll(getConnectorPropertyDescriptors());
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    /** 임의의 Debezium 설정 키를 동적 프로퍼티로 넘길 수 있게 한다. */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .description("Debezium 설정 키 '" + propertyDescriptorName + "'로 그대로 전달된다.")
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .build();
    }

    public synchronized void startEngine(final ProcessContext context) {
        if (engine != null) {
            return;
        }
        queue.clear();
        engineFailure.set(null);

        final Properties props = buildEngineProperties(context);

        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(this::enqueue)
                .using((success, message, error) -> {
                    if (!success && error != null) {
                        engineFailure.set(error);
                        getLogger().error("Debezium 엔진이 오류로 종료됨: {}", message, error);
                    }
                })
                .build();

        executor = Executors.newSingleThreadExecutor(r -> {
            final Thread t = new Thread(r);
            t.setName("debezium-" + context.getProperty(TOPIC_PREFIX).evaluateAttributeExpressions().getValue());
            t.setDaemon(true);
            return t;
        });
        executor.submit(engine);
        getLogger().info("Debezium 엔진 시작: connector={}", getConnectorClass());
    }

    private void enqueue(final ChangeEvent<String, String> event) {
        if (event == null || event.value() == null) {
            // tombstone 등 value가 null인 이벤트는 건너뛴다.
            return;
        }
        try {
            queue.put(event);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected Properties buildEngineProperties(final ProcessContext context) {
        final Properties props = new Properties();
        final String topicPrefix = context.getProperty(TOPIC_PREFIX).evaluateAttributeExpressions().getValue();

        props.setProperty("name", topicPrefix);
        props.setProperty("connector.class", getConnectorClass());
        props.setProperty("topic.prefix", topicPrefix);

        props.setProperty("database.hostname", context.getProperty(DATABASE_HOSTNAME).evaluateAttributeExpressions().getValue());
        props.setProperty("database.port", context.getProperty(DATABASE_PORT).evaluateAttributeExpressions().getValue());
        props.setProperty("database.user", context.getProperty(DATABASE_USER).evaluateAttributeExpressions().getValue());
        props.setProperty("database.password", context.getProperty(DATABASE_PASSWORD).evaluateAttributeExpressions().getValue());

        if (context.getProperty(TABLE_INCLUDE_LIST).isSet()) {
            props.setProperty("table.include.list", context.getProperty(TABLE_INCLUDE_LIST).evaluateAttributeExpressions().getValue());
        }
        if (context.getProperty(SNAPSHOT_MODE).isSet()) {
            props.setProperty("snapshot.mode", context.getProperty(SNAPSHOT_MODE).getValue());
        }

        final File storageDir = new File(context.getProperty(OFFSET_STORAGE_DIR).evaluateAttributeExpressions().getValue());
        if (!storageDir.exists() && !storageDir.mkdirs()) {
            throw new ProcessException("저장 디렉토리를 생성할 수 없습니다: " + storageDir);
        }
        final long flushMillis = context.getProperty(OFFSET_FLUSH_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", new File(storageDir, "offsets.dat").getAbsolutePath());
        props.setProperty("offset.flush.interval.ms", Long.toString(flushMillis));

        if (requiresSchemaHistory()) {
            props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
            props.setProperty("schema.history.internal.file.filename", new File(storageDir, "schema-history.dat").getAbsolutePath());
        }

        // 커넥터 고유 설정을 채운 뒤, 동적 프로퍼티로 최종 오버라이드를 허용한다.
        addConnectorConfiguration(context, props);
        context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .forEach(pd -> props.setProperty(pd.getName(),
                        context.getProperty(pd).evaluateAttributeExpressions().getValue()));

        return props;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        if (engine == null) {
            startEngine(context);
        }
        final Throwable failure = engineFailure.get();
        if (failure != null) {
            throw new ProcessException("Debezium 엔진이 실패한 상태입니다. 프로세서를 재시작하세요.", failure);
        }

        final int maxBatch = context.getProperty(MAX_BATCH_SIZE).asInteger();
        int emitted = 0;
        ChangeEvent<String, String> event;
        while (emitted < maxBatch && (event = queue.poll()) != null) {
            FlowFile flowFile = session.create();
            final byte[] payload = event.value().getBytes(StandardCharsets.UTF_8);
            try (final OutputStream out = session.write(flowFile)) {
                out.write(payload);
            } catch (final Exception e) {
                session.remove(flowFile);
                throw new ProcessException("변경 이벤트를 FlowFile에 기록하지 못했습니다.", e);
            }
            flowFile = session.putAllAttributes(flowFile, buildAttributes(event));
            session.getProvenanceReporter().receive(flowFile, buildTransitUri(context, event));
            session.transfer(flowFile, REL_SUCCESS);
            emitted++;
        }

        if (emitted == 0) {
            context.yield();
        }
    }

    protected java.util.Map<String, String> buildAttributes(final ChangeEvent<String, String> event) {
        final java.util.Map<String, String> attrs = new java.util.HashMap<>();
        attrs.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        if (event.key() != null) {
            attrs.put("cdc.key", event.key());
        }
        if (event.destination() != null) {
            attrs.put("cdc.destination", event.destination());
        }
        if (event.partition() != null) {
            attrs.put("cdc.partition", String.valueOf(event.partition()));
        }
        attrs.put("cdc.connector", getConnectorClass());
        return attrs;
    }

    private String buildTransitUri(final ProcessContext context, final ChangeEvent<String, String> event) {
        final String host = context.getProperty(DATABASE_HOSTNAME).evaluateAttributeExpressions().getValue();
        final String port = context.getProperty(DATABASE_PORT).evaluateAttributeExpressions().getValue();
        final String dest = event.destination() == null ? "" : event.destination();
        return "cdc://" + host + ":" + port + "/" + dest;
    }

    @OnStopped
    public synchronized void stopEngine() {
        if (engine != null) {
            try {
                engine.close();
            } catch (final Exception e) {
                getLogger().warn("Debezium 엔진 종료 중 오류", e);
            }
            engine = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    getLogger().warn("Debezium 엔진 스레드가 30초 내에 종료되지 않았습니다.");
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
        queue.clear();
    }
}
