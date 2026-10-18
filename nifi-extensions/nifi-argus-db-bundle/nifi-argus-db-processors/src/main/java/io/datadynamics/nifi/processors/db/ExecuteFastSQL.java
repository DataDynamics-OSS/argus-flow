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
package io.datadynamics.nifi.processors.db;

import de.siegmar.fastcsv.writer.*;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.Relationship.Builder;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import io.datadynamics.nifi.processors.db.executesql.JdbcCommon;
import org.apache.nifi.util.StopWatch;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 여러 스레드를 사용해 SQL 쿼리 결과 집합을 병렬로 가져와 CSV 형식의 FlowFile로 변환하는 프로세서.
 * 표준 ExecuteSQL과 달리 결과 집합을 다중 스레드로 나눠서 가져오기(fetch) 때문에 대용량 결과를
 * 더 빠르게 처리할 수 있다. 결과는 Avro가 아닌 CSV로 기록된다.
 */
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("SQL SELECT 쿼리를 실행하고 결과 집합을 여러 스레드로 나눠 가져와 CSV 형식의 FlowFile로 출력한다. "
        + "BINARY/VARBINARY/BLOB 등 바이너리 컬럼 값은 Base64(RFC 4648, 줄바꿈 없음) 문자열로, CLOB/NCLOB 값은 문자열 내용으로 기록한다.")
@Tags({"sql", "select", "jdbc", "query", "database"})
@WritesAttributes({@WritesAttribute(attribute = "executesql.query.fetch.time", description = "결과 집합(Result Set)을 가져오는 데 걸린 시간(밀리초). "),
        @WritesAttribute(attribute = "executesql.query.total.time", description = "결과 집합을 가져오는 시간과 행을 처리하는 시간을 합산한 총 소요 시간(밀리초)."),
        @WritesAttribute(attribute = "executesql.fetch.count", description = "출력 FlowFile에 포함된 행의 개수. 이 값은 Fetch Size 속성 값 이하이다."),
        @WritesAttribute(attribute = "mime.type", description = "mime.type 속성을 'text/csv'로 설정한다.")
})
public class ExecuteFastSQL extends AbstractProcessor {

    private static final String CSV_MIME_TYPE = "text/csv";
    private static final int NUM_THREADS = 10;
    // Fetch Size가 0(기본값) 이하일 때 FlowFile 하나에 담을 행 수
    private static final int DEFAULT_BATCH_SIZE = 1000;

    public static final Relationship REL_SUCCESS = (new Builder()).name("success").description("SQL 쿼리 결과 집합으로부터 FlowFile을 성공적으로 생성함.").build();
    public static final Relationship REL_FAILURE = (new Builder()).name("failure").description("SQL 쿼리 실행에 실패함. 입력 FlowFile은 페널티가 부과된 후 이 관계로 라우팅됨").build();

    public static final PropertyDescriptor DBCP_SERVICE = (new org.apache.nifi.components.PropertyDescriptor.Builder())
            .name("Database Connection Pooling Service")
            .displayName("데이터베이스 커넥션 풀링 서비스")
            .description("데이터베이스 연결을 획득하는 데 사용되는 컨트롤러 서비스")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor FETCH_SIZE;
    public static final PropertyDescriptor FETCH_THREADS;
    public static final PropertyDescriptor QUERY_TIMEOUT;
    public static final PropertyDescriptor SQL_SELECT_QUERY;

    private final List<PropertyDescriptor> propDescriptors;

    private final Set<Relationship> relationships;

    protected DBCPService dbcpService;

    /**
     * 이 프로세서가 지원하는 PropertyDescriptor 목록과 Relationship 집합을 초기화한다.
     */
    public ExecuteFastSQL() {
        List<PropertyDescriptor> pds = new ArrayList();
        pds.add(DBCP_SERVICE);
        pds.add(SQL_SELECT_QUERY);
        pds.add(QUERY_TIMEOUT);
        pds.add(FETCH_SIZE);
        pds.add(FETCH_THREADS);
        this.propDescriptors = Collections.unmodifiableList(pds);
        this.relationships = new HashSet(2);
        this.relationships.add(REL_SUCCESS);
        this.relationships.add(REL_FAILURE);
    }

    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.propDescriptors;
    }

    // SQL SELECT 쿼리도 입력 연결도 없을 때의 오류 메시지
    static final String NO_QUERY_NO_INPUT_MESSAGE = "SQL SELECT 쿼리를 지정하거나, 쿼리를 콘텐츠로 담은 FlowFile을 받을 입력 연결이 있어야 합니다.";

    /**
     * SQL SELECT 쿼리가 비어 있으면 입력 FlowFile의 콘텐츠를 쿼리로 사용하므로 입력 연결이 반드시 필요하다.
     * NiFi의 ValidationContext는 연결 정보를 제공하지 않아 customValidate로는 판단할 수 없으므로
     * (Apache ExecuteSQL과 같이) 스케줄 시점에 확인하여 프로세서 시작을 거부한다.
     */
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        if (!context.getProperty(SQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            throw new ProcessException(NO_QUERY_NO_INPUT_MESSAGE);
        }
    }

    /**
     * SQL SELECT 쿼리를 실행하고, 다수의 FetchThread를 이용해 결과 집합을 병렬로 읽어
     * CSV FlowFile로 분할 출력한다. 입력 FlowFile이 있으면 그 속성값을 커넥션 획득 및
     * 쿼리 파라미터 바인딩에 사용하고, SQL_SELECT_QUERY 속성이 비어 있으면 입력 FlowFile의
     * 콘텐츠 자체를 쿼리문으로 사용한다.
     * <p>
     * ProcessSession은 thread-safe 하지 않으므로 세션 조작(create/write/transfer/remove)은 모두
     * onTrigger 스레드에서만 수행한다. FetchThread는 행을 읽어 CSV 바이트 배열로 직렬화한 배치만
     * 큐로 넘긴다. 성공 시 입력 FlowFile은 제거되고(결과 FlowFile은 입력의 자식으로 생성됨),
     * 실패 시 이미 만든 결과 FlowFile을 모두 제거한 뒤 입력 FlowFile에 페널티를 부과하여
     * REL_FAILURE로 보낸다. 입력 FlowFile이 없으면 ProcessException을 던져 세션을 롤백한다.
     */
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        ComponentLog logger = this.getLogger();
        int queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
        Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        Integer fetchThreads = context.getProperty(FETCH_THREADS).evaluateAttributeExpressions(fileToProcess).asInteger();
        String selectQuery;
        if (!context.getProperty(SQL_SELECT_QUERY).isSet() && fileToProcess == null) {
            // 쿼리를 읽을 입력 FlowFile이 없다(입력이 자기 자신으로의 loop 연결뿐인 경우 등). session.read(null)을 막는다.
            throw new ProcessException(NO_QUERY_NO_INPUT_MESSAGE);
        }
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, (in) -> {
                // 플랫폼 기본 charset에 의존하지 않도록 쿼리 콘텐츠는 UTF-8로 읽는다.
                queryContents.append(IOUtils.toString(in, StandardCharsets.UTF_8));
            });
            selectQuery = queryContents.toString();
        }

        // Fetch Size가 0 이하이면 드라이버 힌트는 설정하지 않고, FlowFile당 행 수는 기본 배치 크기를 사용한다.
        int batchSize = (fetchSize == null || fetchSize <= 0) ? DEFAULT_BATCH_SIZE : fetchSize;
        int numFetchThreads;
        if (fetchThreads == null || fetchThreads <= 0) {
            logger.warn("The evaluated value of Fetch Threads is not valid, using {}", new Object[]{NUM_THREADS});
            numFetchThreads = NUM_THREADS;
        } else {
            numFetchThreads = fetchThreads;
        }

        this.dbcpService = (DBCPService) context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        // 지금까지 생성한 결과 FlowFile. 모든 배치가 성공한 뒤에만 REL_SUCCESS로 전송하고, 실패 시 일괄 제거한다.
        final List<FlowFile> outputFlowFiles = new ArrayList<>();
        try (Connection con = this.dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
             PreparedStatement st = con.prepareStatement(selectQuery)) {
            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException var45) {
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, var45.getLocalizedMessage()}, var45);
                }
            }

            st.setQueryTimeout(queryTimeout);
            if (fileToProcess != null) {
                JdbcCommon.setParameters(st, fileToProcess.getAttributes());
            }

            logger.debug("Executing query {}", new Object[]{selectQuery});
            st.execute();
            ResultSet resultSet = st.getResultSet();
            if (resultSet == null) {
                // SELECT가 아닌 문장(UPDATE 등)은 결과 집합이 없다. 조용히 성공 처리하지 않고 오류로 보고한다.
                throw new ProcessException("The query did not return a result set (not a SELECT statement): " + selectQuery);
            }

            this.fetchAndTransfer(session, st, resultSet, batchSize, numFetchThreads, fileToProcess, context.hasIncomingConnection(), outputFlowFiles);
        } catch (InterruptedException var49) {
            // 프로세서 중지 등으로 인터럽트된 경우: 세션을 롤백시켜 입력 FlowFile을 큐로 되돌린다.
            Thread.currentThread().interrupt();
            throw new ProcessException("Interrupted while fetching the result set", var49);
        } catch (Exception var50) {
            Throwable cause = var50 instanceof ExecutionException && var50.getCause() != null ? var50.getCause() : var50;
            if (!outputFlowFiles.isEmpty()) {
                session.remove(outputFlowFiles);
            }

            if (fileToProcess == null) {
                if (cause instanceof ProcessException) {
                    throw (ProcessException) cause;
                }
                throw new ProcessException(cause);
            }

            logger.error("Unable to execute SQL select query {} for {} due to {}; routing to failure", new Object[]{selectQuery, fileToProcess, cause.toString()}, cause);
            fileToProcess = session.penalize(fileToProcess);
            session.transfer(fileToProcess, REL_FAILURE);
            return;
        }

        session.transfer(outputFlowFiles, REL_SUCCESS);

        // 결과 FlowFile은 입력 FlowFile의 자식으로 생성되었으므로 입력 FlowFile은 제거한다(ExecuteSQL과 동일).
        if (fileToProcess != null) {
            session.remove(fileToProcess);
        }
    }

    /**
     * numFetchThreads개의 FetchThread를 실행하여 공유 ResultSet을 배치 단위로 읽게 하고,
     * 워커가 넘겨준 CSV 배치를 onTrigger 스레드에서 FlowFile로 만들어 outputFlowFiles에 추가한다
     * (전송은 호출자가 모든 배치 성공 후 수행).
     * 워커 예외는 Future.get()을 통해 ExecutionException으로 전파된다. 반환 전에 모든 워커가
     * 종료되었음을 보장하므로 호출자가 Statement/Connection을 닫아도 안전하다.
     */
    private void fetchAndTransfer(ProcessSession session, Statement statement, ResultSet resultSet, int batchSize, int numFetchThreads, FlowFile inputFlowFile,
                                  boolean hasIncomingConnection, List<FlowFile> outputFlowFiles) throws Exception {
        // 큐 용량을 제한하여 FlowFile 기록이 느릴 때 메모리에 쌓이는 배치 수를 억제한다(back-pressure).
        BlockingQueue<CsvBatch> batchQueue = new ArrayBlockingQueue<>(numFetchThreads);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        ExecutorService executorService = Executors.newFixedThreadPool(numFetchThreads, (r) -> {
            Thread t = new Thread(r, "ExecuteFastSQL-fetch-" + this.getIdentifier());
            t.setDaemon(true);
            return t;
        });

        try {
            List<Future<Void>> futures = new ArrayList<>(numFetchThreads);
            for (int i = 0; i < numFetchThreads; ++i) {
                futures.add(executorService.submit(new ExecuteFastSQL.FetchThread(resultSet, batchSize, batchQueue, cancelled)));
            }

            while (true) {
                CsvBatch batch = batchQueue.poll(100L, TimeUnit.MILLISECONDS);
                if (batch != null) {
                    outputFlowFiles.add(this.createBatchFlowFile(session, batch, inputFlowFile, hasIncomingConnection));
                    continue;
                }

                if (allDone(futures)) {
                    // 모든 워커가 종료된 뒤에는 더 이상 배치가 추가되지 않으므로 남은 배치만 비우고 끝낸다.
                    while ((batch = batchQueue.poll()) != null) {
                        outputFlowFiles.add(this.createBatchFlowFile(session, batch, inputFlowFile, hasIncomingConnection));
                    }
                    return;
                }
            }
        } finally {
            cancelled.set(true);
            executorService.shutdownNow();
            if (!executorService.awaitTermination(30L, TimeUnit.SECONDS)) {
                // 워커가 드라이버 호출(ResultSet.next() 등)에서 블록되어 인터럽트에 반응하지 않는 경우:
                // Statement를 취소하여 블록을 풀고 잠시 더 기다린다. 호출자가 Statement/Connection을 닫는 것은 그 이후이다.
                this.getLogger().warn("Fetch threads did not terminate within 30 seconds; cancelling the statement");
                try {
                    statement.cancel();
                } catch (SQLException | UnsupportedOperationException cancelException) {
                    this.getLogger().debug("Cannot cancel the statement", cancelException);
                }
                if (!executorService.awaitTermination(10L, TimeUnit.SECONDS)) {
                    this.getLogger().warn("Fetch threads did not terminate after cancelling the statement");
                }
            }
        }
    }

    /**
     * 모든 워커가 종료되었는지 확인한다. 종료된 워커는 Future.get()으로 결과를 확인하여
     * 워커에서 발생한 예외를 즉시 ExecutionException으로 전파한다.
     */
    private static boolean allDone(List<Future<Void>> futures) throws InterruptedException, ExecutionException {
        boolean done = true;
        for (Future<Void> future : futures) {
            if (future.isDone()) {
                future.get();
            } else {
                done = false;
            }
        }
        return done;
    }

    /**
     * onTrigger 스레드에서 CSV 배치 하나를 FlowFile로 기록하고 프로비넌스/속성을 설정한다.
     */
    private FlowFile createBatchFlowFile(ProcessSession session, CsvBatch batch, FlowFile inputFlowFile, boolean hasIncomingConnection) {
        FlowFile outputFlowFile = inputFlowFile != null ? session.create(inputFlowFile) : session.create();
        outputFlowFile = session.write(outputFlowFile, (out) -> out.write(batch.content));

        if (hasIncomingConnection) {
            session.getProvenanceReporter().fetch(outputFlowFile, "Retrieved " + batch.numberOfRows + " rows");
        } else {
            session.getProvenanceReporter().receive(outputFlowFile, "Retrieved " + batch.numberOfRows + " rows");
        }

        Map<String, String> attributes = new HashMap();
        attributes.put("mime.type", CSV_MIME_TYPE);
        attributes.put("executesql.query.fetch.time", batch.fetchTime);
        attributes.put("executesql.query.total.time", batch.totalTime);
        attributes.put("executesql.fetch.count", String.valueOf(batch.numberOfRows));
        return session.putAllAttributes(outputFlowFile, attributes);
    }

    static {
        FETCH_SIZE = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Fetch Size").displayName("Fetch 크기").description("한 번에 결과 집합에서 가져올 행의 개수이자 출력 FlowFile 하나에 담을 최대 행 수. 드라이버 fetch size 힌트로도 전달되며, 힌트는 정확히 지켜지지 않을 수 있다. 값이 0이면 드라이버 힌트는 설정하지 않고 FlowFile당 기본 " + DEFAULT_BATCH_SIZE + "행을 사용한다.").defaultValue("0").required(true).addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
        FETCH_THREADS = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Fetch Threads").displayName("Fetch 스레드 수").description("결과 집합에서 동시에 데이터를 가져오는 데 사용할 스레드의 개수.").defaultValue(String.valueOf(10)).required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
        QUERY_TIMEOUT = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Max Wait Time").displayName("최대 대기 시간").description("실행 중인 SQL SELECT 쿼리에 허용되는 최대 시간. 0이면 제한이 없음을 의미한다. 1초 미만의 시간은 0으로 취급된다.").defaultValue("0 seconds").required(true).addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).sensitive(false).build();
        SQL_SELECT_QUERY = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("SQL select query").displayName("SQL SELECT 쿼리").description("실행할 SQL SELECT 쿼리. 이 쿼리는 비어 있을 수도 있고, 상수 값이거나 Expression Language를 사용하여 속성으로부터 구성될 수도 있다. 이 속성이 지정되면 입력 FlowFile의 콘텐츠와 무관하게 이 값이 사용된다. 이 속성이 비어 있으면 입력 FlowFile의 콘텐츠에 프로세서가 데이터베이스로 전송할 유효한 SQL SELECT 쿼리가 담겨 있어야 한다. FlowFile 콘텐츠는 UTF-8로 읽으며, Expression Language가 평가되지 않는다는 점에 유의한다.").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    }

    /**
     * FetchThread가 직렬화한 CSV 배치. onTrigger 스레드가 이 배치로 FlowFile을 만든다.
     */
    private static final class CsvBatch {
        private final byte[] content;
        private final int numberOfRows;
        private final String fetchTime;
        private final String totalTime;

        private CsvBatch(byte[] content, int numberOfRows, String fetchTime, String totalTime) {
            this.content = content;
            this.numberOfRows = numberOfRows;
            this.fetchTime = fetchTime;
            this.totalTime = totalTime;
        }
    }

    /**
     * 별도의 스레드 풀에서 실행되며, 공유되는 ResultSet으로부터 지정된 배치 크기만큼 행을
     * 읽어 CSV 바이트 배열로 직렬화한 뒤 배치 큐로 넘기는 작업 단위.
     * ResultSet은 여러 스레드가 동시에 접근하므로 행 읽기 구간을 synchronized로 보호하며,
     * ProcessSession에는 접근하지 않는다(세션 조작은 onTrigger 스레드 전담).
     * 더 이상 읽을 행이 없으면(0건) 종료한다. 예외는 Future를 통해 onTrigger로 전파된다.
     */
    private static class FetchThread implements Callable<Void> {
        private final ResultSet resultSet;
        private final int fetchSize;
        private final BlockingQueue<CsvBatch> batchQueue;
        private final AtomicBoolean cancelled;

        public FetchThread(ResultSet rs, int fetchSize, BlockingQueue<CsvBatch> batchQueue, AtomicBoolean cancelled) {
            this.resultSet = rs;
            this.fetchSize = fetchSize;
            this.batchQueue = batchQueue;
            this.cancelled = cancelled;
        }

        /**
         * ResultSet이 소진될 때까지 반복하여, 매 회 배치 크기만큼의 행을 CSV로 직렬화해 큐에 넣는다.
         * fetchTime은 행을 가져오는 시간(ResultSet 잠금 대기 포함)만, total은 CSV 직렬화까지
         * 포함한 배치 처리 시간을 측정한다.
         */
        public Void call() throws Exception {
            int columnCount = this.resultSet.getMetaData().getColumnCount();

            while (!this.cancelled.get()) {
                StopWatch fetchTime = new StopWatch();
                StopWatch total = new StopWatch();
                total.start();
                List<String[]> rows = new ArrayList<>(Math.min(this.fetchSize, 10000));

                fetchTime.start();
                synchronized (this.resultSet) {
                    // 크기 검사를 먼저 해야 배치 경계에서 next()로 전진한 행이 버려지지 않는다.
                    while (rows.size() < this.fetchSize && this.resultSet.next()) {
                        String[] rowStrings = new String[columnCount];

                        for (int i = 0; i < columnCount; ++i) {
                            rowStrings[i] = toCsvValue(this.resultSet.getObject(i + 1));
                        }

                        rows.add(rowStrings);
                    }
                }
                fetchTime.stop();

                if (rows.isEmpty()) {
                    return null;
                }

                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (CsvWriter csvWriter = CsvWriter.builder().build(new OutputStreamWriter(bytes, StandardCharsets.UTF_8))) {
                    for (String[] row : rows) {
                        csvWriter.writeRecord(row);
                    }
                }
                total.stop();

                this.batchQueue.put(new CsvBatch(bytes.toByteArray(), rows.size(), fetchTime.getDuration(), total.getDuration()));
            }
            return null;
        }

        /**
         * 컬럼 값을 CSV 필드 문자열로 변환한다. 바이너리(BINARY/VARBINARY/BLOB 등 byte[]와 Blob)는 Base64로,
         * CLOB/NCLOB은 문자열 내용으로 기록한다(LOB은 읽은 뒤 free). 그 밖의 타입은 toString()을 사용한다.
         * ResultSet의 현재 행이 유효한 동안(synchronized 구간 안에서) 호출해야 한다.
         */
        private static String toCsvValue(Object object) throws SQLException {
            if (object == null) {
                return null;
            }
            if (object instanceof byte[]) {
                return Base64.getEncoder().encodeToString((byte[]) object);
            }
            if (object instanceof Blob) {
                final Blob blob = (Blob) object;
                try {
                    return Base64.getEncoder().encodeToString(blob.getBytes(1L, Math.toIntExact(blob.length())));
                } finally {
                    freeQuietly(blob::free);
                }
            }
            if (object instanceof Clob) {
                final Clob clob = (Clob) object;
                try {
                    return clob.getSubString(1L, Math.toIntExact(clob.length()));
                } finally {
                    freeQuietly(clob::free);
                }
            }
            return object.toString();
        }

        /**
         * LOB 자원을 해제한다. 드라이버가 free()를 지원하지 않거나 실패해도 값 변환은 계속한다.
         */
        private static void freeQuietly(LobFreer freer) {
            try {
                freer.free();
            } catch (SQLException | UnsupportedOperationException ignored) {
                // free()는 자원 해제 힌트이므로 실패해도 무시한다.
            }
        }

        @FunctionalInterface
        private interface LobFreer {
            void free() throws SQLException;
        }
    }
}
