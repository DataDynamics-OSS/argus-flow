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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/sql/SqlWriter.java
 */
package io.datadynamics.nifi.processors.db.executesql;


import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;

import static io.datadynamics.nifi.processors.db.executesql.JdbcCommon.ResultSetRowCallback;


/**
 * SqlWriter 인터페이스는 ExecuteSQL, ExecuteSQLRecord, QueryDatabaseTable, QueryDatabaseTableRecord와 같은
 * 프로세서들이 SQL 결과 집합을 각자에게 적합한 방식으로 FlowFile에 기록할 수 있도록 표준화된 방법을 제공한다.
 * 예를 들어 ExecuteSQL은 결과 집합을 Avro로 기록하지만, ExecuteSQLRecord는 Record API를 사용하여
 * 선택된 RecordSetWriter가 지정하는 방식으로 결과 집합을 기록한다.
 */
public interface SqlWriter {

    /**
     * 주어진 결과 집합을 주어진 출력 스트림에 기록하며, 각 행이 처리될 때마다 콜백을 적용할 수 있다.
     *
     * @param resultSet    기록할 ResultSet
     * @param outputStream 결과 집합을 기록할 OutputStream
     * @param logger       기록 과정에서 메시지를 로깅하는 데 사용할 수 있는 공용 로거
     * @param callback     ResultSet의 각 행이 처리될 때 호출될 수 있는 MaxValueResultSetRowCollector
     * @return 출력 스트림에 기록된 행의 개수
     * @throws Exception 결과 집합을 출력 스트림에 기록하는 도중 오류가 발생한 경우
     */
    long writeResultSet(ResultSet resultSet, OutputStream outputStream, ComponentLog logger, ResultSetRowCallback callback) throws Exception;

    /**
     * 출력되는 flow file(들)에 추가할 속성 키/값 쌍의 맵을 반환한다. 기본 구현은 빈 맵을 반환한다.
     *
     * @return 속성 키/값 쌍의 맵
     */
    default Map<String, String> getAttributesToAdd() {
        return Collections.emptyMap();
    }

    /**
     * 결과 집합을 처리한 결과로 세션 카운터를 갱신한다. 기본 구현은 아무 동작도 하지 않으며, 카운터는 갱신되지 않는다.
     *
     * @param session 카운터를 갱신할 대상 세션
     */
    default void updateCounters(ProcessSession session) {
    }

    /**
     * 출력 스트림에 빈 결과 집합을 기록한다. 경우에 따라 ResultSet에 유효한 행이 하나도 없을 수 있는데,
     * 이런 경우 행을 가져오려고 시도하면 오류가 발생하거나 예기치 않게 동작할 수 있다. 이 메서드는 구현체가
     * 행이 없는 결과 집합에 대해 적절한 출력을 기록하도록 지시한다.
     *
     * @param outputStream 빈 결과 집합을 기록할 OutputStream
     * @param logger       기록 과정에서 메시지를 로깅하는 데 사용할 수 있는 공용 로거
     * @throws IOException 빈 결과 집합을 기록하는 도중 오류가 발생한 경우
     */
    void writeEmptyResultSet(OutputStream outputStream, ComponentLog logger) throws IOException;

    /**
     * 출력 형식의 MIME 타입을 반환한다. 이는 FlowFile 속성이나 형식별 처리가 필요한 경우에 사용될 수 있다.
     *
     * @return 출력 형식의 MIME 타입 문자열.
     */
    String getMimeType();
}