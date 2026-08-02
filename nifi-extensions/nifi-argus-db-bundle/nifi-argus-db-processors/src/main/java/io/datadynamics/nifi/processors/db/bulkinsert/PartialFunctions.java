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
package io.datadynamics.nifi.processors.db.bulkinsert;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;

/**
 * 여러 처리 패턴(process pattern)에서 공통으로 재사용할 수 있는 다양한 부분 함수(partial function)들을 모아둔 클래스.
 * Put과 같은 처리 패턴이 FlowFile 조회, 세션 커밋/롤백, 결과 라우팅 등의 표준 동작을
 * 직접 구현하지 않고 이 클래스가 제공하는 함수를 조합해서 사용할 수 있게 한다.
 */
public class PartialFunctions {

    // 입력 큐에서 FlowFile을 1개만 가져오는 기본 FetchFlowFiles 구현.
    public static <FCT> FetchFlowFiles<FCT> fetchSingleFlowFile() {
        return (context, session, functionContext, result) -> session.get(1);
    }

    // RoutingResult에 기록된 관계별 FlowFile 목록을 그대로 세션에 transfer하는 기본 TransferFlowFiles 구현.
    public static <FCT> TransferFlowFiles<FCT> transferRoutedFlowFiles() {
        return (context, session, functionContext, result)
                -> result.getRoutedFlowFiles().forEach(((relationship, routedFlowFiles)
                -> session.transfer(routedFlowFiles, relationship)));
    }

    /**
     * <p>이 메서드는 {@link org.apache.nifi.processor.AbstractProcessor#onTrigger(ProcessContext, ProcessSession)}가 하는 일과 동일하다.</p>
     * <p>ProcessSessionFactory로부터 세션을 생성하고, 지정된 onTrigger 함수를 실행한 뒤,
     * 정상적으로 끝나면 세션을 커밋한다.</p>
     * <p>onTrigger 실행 중 예외가 발생하면 세션은 롤백되며, 처리 중이던 FlowFile들에는 페널티가 부여된다.</p>
     */
    public static void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger, OnTrigger onTrigger) throws ProcessException {
        onTrigger(context, sessionFactory, logger, onTrigger, (session, t) -> session.rollback(true));
    }

    // rollbackSession 전략을 커스터마이징할 수 있는 onTrigger 오버로드.
    // (예: RollbackOnFailure는 페널티 부여 여부를 컨텍스트에 따라 다르게 적용하기 위해 이 오버로드를 사용한다.)
    public static void onTrigger(
            ProcessContext context, ProcessSessionFactory sessionFactory, ComponentLog logger, OnTrigger onTrigger,
            RollbackSession rollbackSession) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger.execute(session);
            session.commitAsync();
        } catch (final Throwable t) {
            // 처리 도중 예외가 발생하면 로그를 남기고 지정된 전략으로 세션을 롤백한 뒤 예외를 그대로 재전파한다.
            logger.error("{} failed to process due to {}; rolling back session", new Object[]{onTrigger, t});
            rollbackSession.rollback(session, t);
            throw t;
        }
    }

    // 처리에 필요한 커넥션(예: JDBC Connection)을 초기화하는 함수.
    @FunctionalInterface
    public interface InitConnection<FC, C> {
        C apply(ProcessContext context, ProcessSession session, FC functionContext, List<FlowFile> flowFiles) throws ProcessException;
    }

    // 입력 큐에서 처리할 FlowFile들을 가져오는 함수.
    @FunctionalInterface
    public interface FetchFlowFiles<FC> {
        List<FlowFile> apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;
    }

    // 정상 처리가 완료된 후 후속 작업(커밋 등)을 수행하는 함수.
    @FunctionalInterface
    public interface OnCompleted<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection) throws ProcessException;
    }

    // 처리 실패 시 후속 작업(예: 커넥션 롤백)을 수행하는 함수.
    @FunctionalInterface
    public interface OnFailed<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection, Exception e) throws ProcessException;
    }

    // 성공/실패 여부와 관계없이 항상 수행되어야 하는 정리(cleanup) 작업을 수행하는 함수. (예: 커넥션 닫기)
    @FunctionalInterface
    public interface Cleanup<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection) throws ProcessException;
    }

    // 여러 FlowFile을 하나의 단위로 묶어 처리할 때 사용하는 그룹 인터페이스. (예: ExceptionHandler의 group 처리)
    @FunctionalInterface
    public interface FlowFileGroup {
        List<FlowFile> getFlowFiles();
    }

    // 처리 완료 후 라우팅 결과(RoutingResult)를 컨텍스트에 맞게 보정하는 함수. (예: RollbackOnFailure의 안전망 역할)
    @FunctionalInterface
    public interface AdjustRoute<FC> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;
    }

    // RoutingResult에 기록된 FlowFile들을 실제로 세션에 transfer하는 함수.
    @FunctionalInterface
    public interface TransferFlowFiles<FC> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, RoutingResult result) throws ProcessException;

        // 이 TransferFlowFiles를 먼저 실행한 뒤, 이어서 after를 실행하는 함수로 합성한다.
        default TransferFlowFiles<FC> andThen(TransferFlowFiles<FC> after) {
            return (context, session, functionContext, result) -> {
                apply(context, session, functionContext, result);
                after.apply(context, session, functionContext, result);
            };
        }
    }

    // onTrigger()의 실제 처리 로직을 표현하는 함수형 인터페이스.
    @FunctionalInterface
    public interface OnTrigger {
        void execute(ProcessSession session) throws ProcessException;
    }


    // 예외 발생시 세션을 어떻게 롤백할지 결정하는 함수. (페널티 부여 여부 등을 컨텍스트에 따라 다르게 적용 가능)
    @FunctionalInterface
    public interface RollbackSession {
        void rollback(ProcessSession session, Throwable t);
    }

    // 라우팅 결과에 실패가 포함되어 있는지 등을 판단하여 후속 처리를 조정하는 함수.
    @FunctionalInterface
    public interface AdjustFailed {
        boolean apply(ProcessContext context, RoutingResult result);
    }
}