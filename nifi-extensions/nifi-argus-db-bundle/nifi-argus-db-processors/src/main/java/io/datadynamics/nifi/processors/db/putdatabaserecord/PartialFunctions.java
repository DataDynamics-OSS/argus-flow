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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-put-pattern/src/main/java/org/apache/nifi/processor/util/pattern/PartialFunctions.java
 */
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;

/**
 * 여러 프로세스 패턴(process pattern)에서 공통으로 재사용할 수 있는 부분 함수(partial function)들을 모아둔 클래스.
 * Put 계열 프로세서(예: PutDatabaseRecord)의 onTrigger 로직 중 반복되는 조각(FlowFile 가져오기, 세션 커밋/롤백,
 * 결과 라우팅 등)을 함수형 인터페이스로 추출하여 재사용성을 높인다.
 */
public class PartialFunctions {

    /**
     * 세션에서 FlowFile을 한 건만 가져오는 기본 FetchFlowFiles 구현.
     */
    public static <FCT> FetchFlowFiles<FCT> fetchSingleFlowFile() {
        return (context, session, functionContext, result) -> session.get(1);
    }

    /**
     * RoutingResult에 누적된 라우팅 정보를 그대로 session.transfer()를 통해 실제로 전송하는 기본 TransferFlowFiles 구현.
     */
    public static <FCT> TransferFlowFiles<FCT> transferRoutedFlowFiles() {
        return (context, session, functionContext, result)
                -> result.getRoutedFlowFiles().forEach(((relationship, routedFlowFiles)
                -> session.transfer(routedFlowFiles, relationship)));
    }

    /**
     * <p>{@link org.apache.nifi.processor.AbstractProcessor#onTrigger(ProcessContext, ProcessSession)}가 하는 일과 동일하다.</p>
     * <p>ProcessSessionFactory로부터 세션을 생성하고 지정한 onTrigger 함수를 실행한 뒤, 성공적으로 끝나면 세션을 커밋한다.</p>
     * <p>onTrigger 실행 중 Exception이 발생하면 세션은 롤백되며, 처리 중이던 FlowFile들은 페널티를 받는다.</p>
     */
    public static void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger, OnTrigger onTrigger) throws ProcessException {
        onTrigger(context, sessionFactory, logger, onTrigger, (session, t) -> session.rollback(true));
    }

    /**
     * 롤백 시 동작(penalize 여부 등)을 커스터마이즈할 수 있는 버전의 onTrigger.
     * RollbackOnFailure가 활성화된 경우처럼, 롤백 시 FlowFile을 penalize 하지 않아야 하는 경우에 사용된다.
     */
    public static void onTrigger(
            ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger, OnTrigger onTrigger,
            RollbackSession rollbackSession) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger.execute(session);
            session.commitAsync();
        } catch (final Throwable t) {
            logger.error("{} failed to process due to {}; rolling back session", new Object[]{onTrigger, t});
            rollbackSession.rollback(session, t);
            throw t;
        }
    }

    /**
     * 가져온 FlowFile들을 바탕으로 (JDBC Connection 등) 실제 처리에 필요한 연결/자원을 초기화하는 함수.
     */
    @FunctionalInterface
    public interface InitConnection<FC, C> {
        C apply(ProcessContext context, ProcessSession session, FC functionContext, List<FlowFile> flowFiles) throws ProcessException;
    }

    /**
     * 이번 onTrigger 실행에서 처리할 FlowFile 목록을 세션으로부터 가져오는 함수.
     */
    @FunctionalInterface
    public interface FetchFlowFiles<FC> {
        List<FlowFile> apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;
    }

    /**
     * 연결(connection)을 사용한 실제 처리가 정상적으로 완료된 뒤 호출되는 함수.
     */
    @FunctionalInterface
    public interface OnCompleted<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection) throws ProcessException;
    }

    /**
     * 연결(connection)을 사용한 처리 중 예외가 발생했을 때 호출되는 함수.
     */
    @FunctionalInterface
    public interface OnFailed<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection, Exception e) throws ProcessException;
    }

    /**
     * 처리 성공/실패 여부와 관계없이 마지막에 연결(connection) 등의 자원을 정리하는 함수.
     */
    @FunctionalInterface
    public interface Cleanup<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection) throws ProcessException;
    }

    /**
     * 하나의 처리 단위로 묶인 FlowFile들의 그룹을 나타낸다. (예: 같은 테이블/스키마로 묶인 FlowFile 배치)
     */
    @FunctionalInterface
    public interface FlowFileGroup {
        List<FlowFile> getFlowFiles();
    }

    /**
     * 처리 로직이 만든 RoutingResult를 문맥에 따라 추가로 보정하는 함수.
     * RollbackOnFailure처럼 ExceptionHandler를 거치지 않고 직접 라우팅된 결과에 대해서도
     * 안전망 역할로 롤백 여부를 재검토할 때 사용된다.
     */
    @FunctionalInterface
    public interface AdjustRoute<FC> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;
    }

    /**
     * RoutingResult에 누적된 라우팅 결과를 실제로 세션에 반영(transfer)하는 함수.
     */
    @FunctionalInterface
    public interface TransferFlowFiles<FC> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;

        /**
         * 이 TransferFlowFiles를 수행한 뒤 이어서 다른 TransferFlowFiles도 수행하도록 체이닝한다.
         */
        default TransferFlowFiles<FC> andThen(TransferFlowFiles<FC> after) {
            return (context, session, functionContext, result) -> {
                apply(context, session, functionContext, result);
                after.apply(context, session, functionContext, result);
            };
        }
    }

    /**
     * 하나의 ProcessSession을 대상으로 실제 onTrigger 로직을 실행하는 함수.
     */
    @FunctionalInterface
    public interface OnTrigger {
        void execute(ProcessSession session) throws ProcessException;
    }


    /**
     * onTrigger 도중 예외가 발생했을 때 세션을 롤백하는 방식을 지정하는 함수.
     * (예: FlowFile을 penalize 할지 여부를 문맥에 따라 다르게 처리)
     */
    @FunctionalInterface
    public interface RollbackSession {
        void rollback(ProcessSession session, Throwable t);
    }

    /**
     * RoutingResult를 보고 이번 처리가 "실패로 취급되어야 하는지"를 판단하는 함수.
     */
    @FunctionalInterface
    public interface AdjustFailed {
        boolean apply(ProcessContext context, RoutingResult result);
    }
}