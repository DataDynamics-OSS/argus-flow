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
import org.apache.nifi.annotation.documentation.Tags;
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

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

/**
 * 여러 스레드를 사용해 SQL 쿼리 결과 집합을 병렬로 가져와 CSV 형식의 FlowFile로 변환하는 프로세서.
 * 표준 ExecuteSQL과 달리 결과 집합을 다중 스레드로 나눠서 가져오기(fetch) 때문에 대용량 결과를
 * 더 빠르게 처리할 수 있다. 결과는 Avro가 아닌 CSV로 기록된다.
 */
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "jdbc", "query", "database"})
@WritesAttributes({@WritesAttribute(attribute = "executesql.query.fetch.time", description = "결과 집합(Result Set)을 가져오는 데 걸린 시간(밀리초). "),
        @WritesAttribute(attribute = "executesql.query.total.time", description = "결과 집합을 가져오는 시간과 행을 처리하는 시간을 합산한 총 소요 시간(밀리초)."),
        @WritesAttribute(attribute = "executesql.fetch.count", description = "출력 FlowFile에 포함된 행의 개수. 이 값은 Fetch Size 속성 값 이하이다."),
        @WritesAttribute(attribute = "mime.type", description = "mime.type 속성을 'text/csv'로 설정한다.")
})
public class ExecuteFastSQL extends AbstractProcessor {

    private static final String CSV_MIME_TYPE = "text/csv";
    private static final int NUM_THREADS = 10;

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

    /**
     * SQL SELECT 쿼리를 실행하고, 다수의 FetchThread를 이용해 결과 집합을 병렬로 읽어
     * CSV FlowFile로 분할 출력한다. 입력 FlowFile이 있으면 그 속성값을 커넥션 획득 및
     * 쿼리 파라미터 바인딩에 사용하고, SQL_SELECT_QUERY 속성이 비어 있으면 입력 FlowFile의
     * 콘텐츠 자체를 쿼리문으로 사용한다.
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
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, (in) -> {
                queryContents.append(IOUtils.toString(in, Charset.defaultCharset()));
            });
            selectQuery = queryContents.toString();
        }

        this.dbcpService = (DBCPService) context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        try {
            Connection con = this.dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
            Throwable var10 = null;

            try {
                PreparedStatement st = con.prepareStatement(selectQuery);
                Throwable var12 = null;

                try {
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
                    int numFetchThreads;
                    if (fetchThreads == null) {
                        logger.warn("The evaluated value of Fetch Threads is not valid, using {}", new Object[]{10});
                        numFetchThreads = 10;
                    } else {
                        numFetchThreads = fetchThreads;
                    }

                    BlockingQueue<Runnable> threadQ = new ArrayBlockingQueue(numFetchThreads);
                    ExecutorService executorService = new ThreadPoolExecutor(numFetchThreads, numFetchThreads, 1L, TimeUnit.SECONDS, threadQ, new CallerRunsPolicy());

                    for (int i = 0; i < fetchThreads; ++i) {
                        executorService.execute(new ExecuteFastSQL.FetchThread(session, resultSet, fetchSize, fileToProcess, context.hasIncomingConnection(), this.getLogger()));
                    }

                    executorService.shutdown();
                    executorService.awaitTermination(1L, TimeUnit.DAYS);
                } catch (Throwable var46) {
                    var12 = var46;
                    throw var46;
                } finally {
                    if (st != null) {
                        if (var12 != null) {
                            try {
                                st.close();
                            } catch (Throwable var44) {
                                var12.addSuppressed(var44);
                            }
                        } else {
                            st.close();
                        }
                    }

                }
            } catch (Throwable var48) {
                var10 = var48;
                throw var48;
            } finally {
                if (con != null) {
                    if (var10 != null) {
                        try {
                            con.close();
                        } catch (Throwable var43) {
                            var10.addSuppressed(var43);
                        }
                    } else {
                        con.close();
                    }
                }

            }
        } catch (SQLException var50) {
            throw new ProcessException(var50);
        } catch (Exception var51) {
            if (fileToProcess != null) {
                session.transfer(fileToProcess, REL_FAILURE);
            }

            if (var51 instanceof ProcessException) {
                throw (ProcessException) var51;
            } else {
                throw new ProcessException(var51);
            }
        }
    }

    static {
        FETCH_SIZE = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Fetch Size").displayName("Fetch 크기").description("한 번에 결과 집합에서 가져올 행의 개수. 이는 데이터베이스 드라이버에 대한 힌트일 뿐이며 정확히 지켜지지 않을 수 있다. 값이 0으로 지정되면 이 힌트는 무시된다.").defaultValue("0").required(true).addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
        FETCH_THREADS = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Fetch Threads").displayName("Fetch 스레드 수").description("결과 집합에서 동시에 데이터를 가져오는 데 사용할 스레드의 개수.").defaultValue(String.valueOf(10)).required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
        QUERY_TIMEOUT = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("Max Wait Time").displayName("최대 대기 시간").description("실행 중인 SQL SELECT 쿼리에 허용되는 최대 시간. 0이면 제한이 없음을 의미한다. 1초 미만의 시간은 0으로 취급된다.").defaultValue("0 seconds").required(true).addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).sensitive(false).build();
        SQL_SELECT_QUERY = (new org.apache.nifi.components.PropertyDescriptor.Builder()).name("SQL select query").displayName("SQL SELECT 쿼리").description("실행할 SQL SELECT 쿼리. 이 쿼리는 비어 있을 수도 있고, 상수 값이거나 Expression Language를 사용하여 속성으로부터 구성될 수도 있다. 이 속성이 지정되면 입력 FlowFile의 콘텐츠와 무관하게 이 값이 사용된다. 이 속성이 비어 있으면 입력 FlowFile의 콘텐츠에 프로세서가 데이터베이스로 전송할 유효한 SQL SELECT 쿼리가 담겨 있어야 한다. FlowFile 콘텐츠에 대해서는 Expression Language가 평가되지 않는다는 점에 유의한다.").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    }

    /**
     * 별도의 스레드 풀에서 실행되며, 공유되는 ResultSet으로부터 지정된 Fetch Size만큼 행을
     * 읽어 CSV로 직렬화한 뒤 새 FlowFile로 만들어 REL_SUCCESS로 전송하는 작업 단위.
     * ResultSet은 여러 스레드가 동시에 접근하므로 next() 호출 구간을 synchronized로 보호한다.
     * 더 이상 읽을 행이 없으면(0건) 빈 FlowFile을 제거하고 종료한다.
     */
    private static class FetchThread implements Runnable {
        private final ComponentLog logger;
        private final ProcessSession session;
        private final ResultSet resultSet;
        private final int fetchSize;
        private final boolean hasIncomingConnection;
        private final FlowFile inputFlowFile;

        public FetchThread(ProcessSession session, ResultSet rs, Integer fetchSize, FlowFile inputFlowFile, boolean hasIncomingConnection, ComponentLog log) {
            this.session = session;
            this.resultSet = rs;
            this.fetchSize = fetchSize == null ? 1000 : fetchSize;
            this.inputFlowFile = inputFlowFile;
            this.hasIncomingConnection = hasIncomingConnection;
            this.logger = log;
        }

        /**
         * ResultSet이 소진될 때까지 반복하여, 매 회 Fetch Size만큼의 행을 CSV로 기록한
         * FlowFile을 만들어 전송한다. fetchTime은 순수하게 행을 가져오는 시간만, total은
         * FlowFile 생성/기록까지 포함한 전체 처리 시간을 측정한다.
         */
        public void run() {
            StopWatch fetchTime = new StopWatch();
            StopWatch total = new StopWatch();

            while (true) {
                total.start();
                int numberOfRows = 0;

                try {
                    ResultSetMetaData resultSetMetaData = this.resultSet.getMetaData();
                    int columnCount = resultSetMetaData.getColumnCount();
                    FlowFile outputFlowFile = this.inputFlowFile != null ? this.session.create(this.inputFlowFile) : this.session.create();
                    OutputStream flowFileOutputStream = this.session.write(outputFlowFile);
                    Throwable var8 = null;

                    try {
                        CsvWriter csvWriter = CsvWriter.builder().build(new OutputStreamWriter(flowFileOutputStream));
                        Throwable var10 = null;

                        try {
                            fetchTime.start();
                            synchronized (this.resultSet) {
                                while (this.resultSet.next() && numberOfRows < this.fetchSize) {
                                    fetchTime.stop();
                                    String[] rowStrings = new String[columnCount];

                                    for (int i = 0; i < columnCount; ++i) {
                                        Object object = this.resultSet.getObject(i + 1);
                                        rowStrings[i] = object == null ? null : object.toString();
                                    }

                                    csvWriter.writeRecord(rowStrings);
                                    ++numberOfRows;
                                    fetchTime.start();
                                }
                            }

                            fetchTime.stop();
                        } catch (Throwable var42) {
                            var10 = var42;
                            throw var42;
                        } finally {
                            if (csvWriter != null) {
                                if (var10 != null) {
                                    try {
                                        csvWriter.close();
                                    } catch (Throwable var40) {
                                        var10.addSuppressed(var40);
                                    }
                                } else {
                                    csvWriter.close();
                                }
                            }

                        }
                    } catch (Throwable var44) {
                        var8 = var44;
                        throw var44;
                    } finally {
                        if (flowFileOutputStream != null) {
                            if (var8 != null) {
                                try {
                                    flowFileOutputStream.close();
                                } catch (Throwable var39) {
                                    var8.addSuppressed(var39);
                                }
                            } else {
                                flowFileOutputStream.close();
                            }
                        }

                    }

                    total.stop();
                    if (numberOfRows == 0) {
                        this.session.remove(outputFlowFile);
                        return;
                    }

                    if (this.hasIncomingConnection) {
                        this.session.getProvenanceReporter().fetch(outputFlowFile, "Retrieved " + numberOfRows + " rows");
                    } else {
                        this.session.getProvenanceReporter().receive(outputFlowFile, "Retrieved " + numberOfRows + " rows");
                    }

                    Map<String, String> attributes = new HashMap();
                    attributes.put("mime.type", "text/csv");
                    attributes.put("executesql.query.fetch.time", fetchTime.getDuration());
                    attributes.put("executesql.query.total.time", total.getDuration());
                    attributes.put("executesql.fetch.count", String.valueOf(numberOfRows));
                    this.session.putAllAttributes(outputFlowFile, attributes);
                    this.session.transfer(outputFlowFile, ExecuteFastSQL.REL_SUCCESS);
                } catch (SQLException | IOException var46) {
                    throw new RuntimeException(var46);
                }
            }
        }
    }
}
