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

import io.datadynamics.nifi.trino.api.TrinoConnectionService;
import io.datadynamics.nifi.trino.api.TrinoQueryInfo;
import io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.ExecutionMode;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.EXECUTION_MODE;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.MAX_BACKGROUND_QUERIES;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.QUERY_TIMEOUT;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.REL_FAILURE;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.REL_SUCCESS;
import static io.datadynamics.nifi.trino.processors.TrinoProcessorProperties.TRINO_CONNECTION_SERVICE;

/**
 * 문장 하나를 실행하고 결과(업데이트 수·통계)를 속성으로 남기는 프로세서의 공통 기반.
 * <p>
 * SYNCHRONOUS 는 트리거 스레드에서 실행한다. BACKGROUND 는 트리거 스레드가 FlowFile 을 꺼내 세션을 연 채
 * 프로세서 전용 executor 에 넘기고 즉시 돌아오며, 작업 스레드가 쿼리를 끝까지 실행(드라이버가 결과를 계속 polling)한 뒤
 * 같은 세션에서 전송·커밋한다. Trino 는 클라이언트 polling 이 끊기면 query.client.timeout 뒤 쿼리를 버리므로
 * query id 만 저장하고 연결을 놓는 비동기 방식은 쓰지 않는다.
 */
abstract class AbstractTrinoStatementProcessor extends AbstractSessionFactoryProcessor {

    /**
     * 실행할 문장과 결과 FlowFile 에 더할 속성.
     */
    protected record StatementRequest(String sql, Map<String, String> attributes) {
    }

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
    private static final long STOP_WAIT_SECONDS = 30;

    private final RunningStatements running = new RunningStatements();
    private final AtomicInteger activeBackground = new AtomicInteger();

    private volatile boolean stopping;
    private volatile ExecutionMode executionMode = ExecutionMode.SYNCHRONOUS;
    private volatile ExecutorService executor;
    private volatile Semaphore permits;
    private volatile int maxBackground;

    /**
     * FlowFile(없을 수 있음)로부터 실행할 문장을 만든다.
     *
     * @throws IllegalArgumentException 입력이 잘못된 경우(failure 로 보낸다)
     */
    protected abstract StatementRequest createRequest(ProcessContext context, ProcessSession session, FlowFile flowFile);

    /**
     * 스케줄 시 추가 검사. 실행할 수 없으면 {@link ProcessException}.
     */
    protected void checkSchedulable(final ProcessContext context) {
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        checkSchedulable(context);
        stopping = false;
        executionMode = context.getProperty(EXECUTION_MODE).asAllowableValue(ExecutionMode.class);
        if (executionMode == ExecutionMode.BACKGROUND) {
            maxBackground = context.getProperty(MAX_BACKGROUND_QUERIES).asInteger();
            permits = new Semaphore(maxBackground);
            executor = Executors.newFixedThreadPool(maxBackground, threadFactory());
        }
    }

    @OnUnscheduled
    public void onUnscheduled() {
        stopping = true;
        running.cancelAll(getLogger());
    }

    @OnStopped
    public void onStopped() {
        stopping = true;
        final ExecutorService current = executor;
        executor = null;
        if (current == null) {
            return;
        }
        running.cancelAll(getLogger());
        current.shutdown();
        try {
            if (!current.awaitTermination(STOP_WAIT_SECONDS, TimeUnit.SECONDS)) {
                getLogger().warn("백그라운드 Trino 문장이 {}초 안에 끝나지 않아 강제 종료한다", STOP_WAIT_SECONDS);
                current.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            current.shutdownNow();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final boolean background = executionMode == ExecutionMode.BACKGROUND;
        final Semaphore currentPermits = permits;
        if (background && !currentPermits.tryAcquire()) {
            getLogger().debug("백그라운드 Trino 문장이 가득 찼다({}/{}). yield", activeBackground.get(), maxBackground);
            context.yield();
            return;
        }

        boolean handedOff = false;
        final ProcessSession session = sessionFactory.createSession();
        try {
            FlowFile flowFile = null;
            if (context.hasIncomingConnection()) {
                flowFile = session.get();
                if (flowFile == null && context.hasNonLoopConnection()) {
                    session.commitAsync();
                    return;
                }
            }

            final StatementRequest request;
            try {
                request = createRequest(context, session, flowFile);
            } catch (final IllegalArgumentException e) {
                routeFailure(context, session, flowFile, e, null, Map.of());
                session.commitAsync();
                return;
            }

            if (!background) {
                execute(context, session, flowFile, request);
                return;
            }

            final FlowFile input = flowFile;
            final ExecutorService currentExecutor = executor;
            final int active = activeBackground.incrementAndGet();
            try {
                if (currentExecutor == null) {
                    throw new RejectedExecutionException("executor 종료됨");
                }
                currentExecutor.execute(() -> runInBackground(context, session, input, request, currentPermits));
                handedOff = true;
                getLogger().debug("백그라운드 Trino 문장 시작: 진행 중 {}/{}", active, maxBackground);
            } catch (final RejectedExecutionException e) {
                activeBackground.decrementAndGet();
                getLogger().debug("백그라운드 executor 가 종료 중이라 FlowFile 을 되돌린다");
                session.rollback();
            }
        } catch (final Throwable t) {
            session.rollback(true);
            throw t;
        } finally {
            if (background && !handedOff) {
                currentPermits.release();
            }
        }
    }

    private void runInBackground(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                                 final StatementRequest request, final Semaphore taskPermits) {
        try {
            execute(context, session, flowFile, request);
        } catch (final Throwable t) {
            getLogger().error("백그라운드 Trino 문장 처리 중 예기치 않은 오류. 세션을 되돌린다", t);
            try {
                session.rollback(true);
            } catch (final Throwable rollbackError) {
                getLogger().warn("세션 롤백 실패", rollbackError);
            }
        } finally {
            taskPermits.release();
            getLogger().debug("백그라운드 Trino 문장 종료: 진행 중 {}/{}", activeBackground.decrementAndGet(), maxBackground);
        }
    }

    /**
     * 문장을 실행하고 결과를 같은 세션에서 전송·커밋한다.
     */
    private void execute(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                         final StatementRequest request) {
        final TrinoConnectionService service = context.getProperty(TRINO_CONNECTION_SERVICE).asControllerService(TrinoConnectionService.class);
        final int timeoutSeconds = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final Map<String, String> inputAttributes = flowFile == null ? Map.of() : flowFile.getAttributes();
        final long start = System.nanoTime();

        Statement statement = null;
        try (Connection connection = service.getConnection(inputAttributes)) {
            statement = connection.createStatement();
            running.add(statement);
            if (stopping) {
                throw new SQLException("프로세서가 중지 중이라 문장을 시작하지 않았다");
            }
            if (timeoutSeconds > 0) {
                statement.setQueryTimeout(timeoutSeconds);
            }

            long resultRows = -1;
            long updateCount = -1;
            TrinoQueryInfo info = null;
            if (statement.execute(request.sql())) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    resultRows = 0;
                    while (resultSet.next()) {
                        resultRows++;
                    }
                    info = service.getQueryInfo(resultSet);
                }
            } else {
                updateCount = statement.getLargeUpdateCount();
            }
            if (info == null) {
                info = service.getQueryInfo(statement);
            }

            final Map<String, String> attributes = new HashMap<>(request.attributes());
            TrinoAttributes.putQueryInfo(attributes, info);
            if (updateCount >= 0) {
                attributes.put(TrinoAttributes.UPDATE_COUNT, Long.toString(updateCount));
            }
            if (resultRows >= 0) {
                attributes.put(TrinoAttributes.RESULT_ROW_COUNT, Long.toString(resultRows));
            }
            FlowFile output = flowFile == null ? session.create() : flowFile;
            output = session.putAllAttributes(output, attributes);
            final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            session.getProvenanceReporter().invokeRemoteProcess(output, TrinoAttributes.transitUri(service.getJdbcUrl(), info),
                    "Trino 문장 실행 " + elapsedMillis + "ms");
            session.transfer(output, REL_SUCCESS);
            closeQuietly(statement);
            running.remove(statement);
            session.commitAsync();
            getLogger().debug("Trino 문장 {} 완료({}ms)", info == null ? "?" : info.queryId(), elapsedMillis);
        } catch (final SQLException | ProcessException e) {
            final TrinoQueryInfo failedInfo = statement == null ? null : service.getQueryInfo(statement);
            closeQuietly(statement);
            running.remove(statement);
            if (stopping) {
                getLogger().info("프로세서 중지로 Trino 문장이 취소되어 FlowFile 을 큐로 되돌린다: {}", e.getMessage());
                session.rollback();
                return;
            }
            routeFailure(context, session, flowFile, e, failedInfo, request.attributes());
            session.commitAsync();
        }
    }

    private void routeFailure(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                              final Exception error, final TrinoQueryInfo info, final Map<String, String> requestAttributes) {
        if (flowFile == null) {
            getLogger().error("Trino 문장 실행 실패", error);
            context.yield();
            return;
        }
        final Map<String, String> attributes = new HashMap<>(requestAttributes);
        TrinoAttributes.putError(attributes, error, info);
        getLogger().error("Trino 문장 실행 실패: {}", flowFile, error);
        FlowFile failed = session.putAllAttributes(flowFile, attributes);
        failed = session.penalize(failed);
        session.transfer(failed, REL_FAILURE);
    }

    private ThreadFactory threadFactory() {
        final AtomicInteger counter = new AtomicInteger();
        final String prefix = getClass().getSimpleName() + "[" + getIdentifier() + "]-background-";
        return runnable -> {
            final Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    int getActiveBackgroundCount() {
        return activeBackground.get();
    }

    private static void closeQuietly(final Statement statement) {
        if (statement == null) {
            return;
        }
        try {
            statement.close();
        } catch (final SQLException ignored) {
            // 이미 닫힘
        }
    }
}
