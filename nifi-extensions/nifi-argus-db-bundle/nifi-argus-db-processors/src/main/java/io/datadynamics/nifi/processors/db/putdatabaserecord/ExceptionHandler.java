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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-put-pattern/src/main/java/org/apache/nifi/processor/util/pattern/ExceptionHandler.java
 */
package io.datadynamics.nifi.processors.db.putdatabaserecord;


import io.datadynamics.nifi.processors.db.putdatabaserecord.ErrorTypes.Result;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * <p>ExceptionHandler는 재사용 가능한 부분 함수(partial function)들을 조합하여 구조화된 예외 처리 로직을 제공한다.
 *
 * <p>
 * ExceptionHandler를 사용함으로써 얻는 이점:
 * <li>오류 처리 코드를 외부화하여, 본래의 정상 처리 흐름(expected path)에만 집중한 깔끔한 코드를 작성할 수 있다.</li>
 * <li>구체적인 Exception들을 {@link ErrorTypes}로 분류하여, 오류 유형별로 일관된 처리를 적용할 수 있다.</li>
 * <li>{@link RollbackOnFailure}처럼 문맥(context)을 인지하는 오류 처리가 가능하다.</li>
 * </p>
 */
public class ExceptionHandler<C> {

    /**
     * 발생한 Exception을 ErrorTypes 중 하나로 단순 분류하는 함수.
     */
    private Function<Exception, ErrorTypes> mapException;
    /**
     * 분류된 오류 유형을 문맥(context)에 맞게 조정하는 함수.
     * (예: RollbackOnFailure 설정에 따라 Failure를 ProcessException으로 승격시키는 등)
     */
    private BiFunction<C, ErrorTypes, Result> adjustError;
    /**
     * 최종 결정된 오류 유형에 따라 입력에 대해 실제 액션(라우팅 등)을 수행하는 함수.
     */
    private OnError<C, ?> onError;

    /**
     * Result에 담긴 페널티 종류에 따라 FlowFile을 penalize 하거나 프로세서를 yield 시킨다.
     */
    private static FlowFile penalize(final ProcessContext context, final ProcessSession session,
                                     final FlowFile flowFile, final ErrorTypes.Penalty penalty) {
        switch (penalty) {
            case Penalize:
                return session.penalize(flowFile);
            case Yield:
                context.yield();
        }
        return flowFile;
    }

    /**
     * {@link Result}의 destination과 penalty 값에 따라 입력을 라우팅하는 {@link OnError} 함수 인스턴스를 생성한다.
     *
     * @param context       프로세서를 yield 시키기 위해 사용하는 프로세스 컨텍스트
     * @param session       FlowFile을 penalize 하기 위해 사용하는 프로세스 세션
     * @param routingResult 입력 FlowFile이 이 {@link RoutingResult}를 통해 목적지 관계로 라우팅된다
     * @param relFailure    프로세서의 failure 관계를 지정
     * @param relRetry      프로세서의 retry 관계를 지정
     * @return 조합된 함수
     */
    public static <C> OnError<C, FlowFile> createOnError(
            final ProcessContext context, final ProcessSession session, final RoutingResult routingResult,
            final Relationship relFailure, final Relationship relRetry) {

        return (fc, input, result, e) -> {
            final PartialFunctions.FlowFileGroup flowFileGroup = () -> Collections.singletonList(input);
            createOnGroupError(context, session, routingResult, relFailure, relRetry).apply(fc, flowFileGroup, result, e);
        };
    }

    /**
     * {@link #createOnError(ProcessContext, ProcessSession, RoutingResult, Relationship, Relationship)}와 동일하지만
     * 단일 FlowFile이 아닌 FlowFileGroup(여러 FlowFile 묶음) 단위로 처리한다.
     *
     * @param context       프로세서를 yield 시키기 위해 사용하는 프로세스 컨텍스트
     * @param session       FlowFile들을 penalize 하기 위해 사용하는 프로세스 세션
     * @param routingResult 입력 FlowFile들이 이 {@link RoutingResult}를 통해 목적지 관계로 라우팅된다
     * @param relFailure    프로세서의 failure 관계를 지정
     * @param relRetry      프로세서의 retry 관계를 지정
     * @return 조합된 함수
     */
    public static <C, I extends PartialFunctions.FlowFileGroup> OnError<C, I> createOnGroupError(
            final ProcessContext context, final ProcessSession session, final RoutingResult routingResult,
            final Relationship relFailure, final Relationship relRetry) {
        return (c, g, r, e) -> {
            final Relationship routeTo;
            switch (r.destination()) {
                case Failure:
                    routeTo = relFailure;
                    break;
                case Retry:
                    routeTo = relRetry;
                    break;
                case Self:
                    routeTo = Relationship.SELF;
                    break;
                default:
                    // ProcessException 목적지(또는 알 수 없는 목적지)인 경우, 세션을 롤백시키기 위해 예외를 다시 던진다.
                    if (e instanceof ProcessException) {
                        throw (ProcessException) e;
                    } else {
                        Object inputs = null;
                        if (g != null) {
                            final List<FlowFile> flowFiles = g.getFlowFiles();
                            switch (flowFiles.size()) {
                                case 0:
                                    inputs = "[]";
                                    break;
                                case 1:
                                    inputs = flowFiles.get(0);
                                    break;
                                default:
                                    inputs = String.format("%d FlowFiles including %s", flowFiles.size(), flowFiles.get(0));
                                    break;
                            }
                        }
                        throw new ProcessException(String.format("Failed to process %s due to %s", inputs, e), e);
                    }
            }
            for (FlowFile f : g.getFlowFiles()) {
                final FlowFile maybePenalized = penalize(context, session, f, r.penalty());
                routingResult.routeTo(maybePenalized, routeTo);
            }
        };
    }

    /**
     * Exception을 특정 ErrorType으로 매핑하는 함수를 지정한다.
     */
    public void mapException(Function<Exception, ErrorTypes> mapException) {
        this.mapException = mapException;
    }

    /**
     * <p>함수 문맥(context)에 기반하여 ErrorType을 조정하는 함수를 지정한다.
     * <p>예를 들어 {@link RollbackOnFailure#createAdjustError(ComponentLog)}는
     * 프로세스 세션을 롤백해야 하는지, 아니면 입력을 failure/retry로 라우팅해야 하는지를 결정한다.
     */
    public void adjustError(BiFunction<C, ErrorTypes, Result> adjustError) {
        this.adjustError = adjustError;
    }

    /**
     * <p>{@link #execute(Object, Object, Procedure)} 호출 시 별도의 OnError가 지정되지 않은 경우 사용할
     * 기본 OnError 함수를 지정한다.
     */
    public void onError(OnError<C, ?> onError) {
        this.onError = onError;
    }

    /**
     * <p>지정한 procedure 함수를 입력값과 함께 실행한다.
     * <p>예외가 발생하면 기본 OnError 함수가 호출된다.
     *
     * @param context   함수 문맥
     * @param input     procedure에 전달할 입력값
     * @param procedure 입력값을 가지고 실제 작업을 수행하는 함수
     * @return procedure가 문제없이 끝났으면 true. procedure가 Exception을 던졌지만 {@link OnError}에 의해 처리되었으면 false.
     * @throws ProcessException      예외가 {@link OnError}에 의해 처리되지 않은 경우 던져짐
     * @throws DiscontinuedException 예외가 {@link OnError}에 의해 처리되었지만, 더 이상의 입력 처리를 즉시 중단해야
     *                               함을 나타냄
     */
    @SuppressWarnings("unchecked")
    public <I> boolean execute(C context, I input, Procedure<I> procedure) throws ProcessException, DiscontinuedException {
        return execute(context, input, procedure, (OnError<C, I>) onError);
    }

    /**
     * <p>지정한 procedure 함수를 입력값과 함께 실행한다.
     *
     * @param context   함수 문맥
     * @param input     procedure에 전달할 입력값
     * @param procedure 입력값을 가지고 실제 작업을 수행하는 함수
     * @param onError   이번 실행에 사용할 {@link OnError} 함수를 지정
     * @return procedure가 문제없이 끝났으면 true. procedure가 Exception을 던졌지만 {@link OnError}에 의해 처리되었으면 false.
     * @throws ProcessException      예외가 {@link OnError}에 의해 처리되지 않은 경우 던져짐
     * @throws DiscontinuedException 예외가 {@link OnError}에 의해 처리되었지만, 더 이상의 입력 처리를 즉시 중단해야
     *                               함을 나타냄
     */
    public <I> boolean execute(C context, I input, Procedure<I> procedure, OnError<C, I> onError) throws ProcessException, DiscontinuedException {
        try {
            procedure.apply(input);
            return true;
        } catch (Exception e) {

            if (mapException == null) {
                throw new ProcessException("An exception was thrown: " + e, e);
            }

            // 1) 발생한 예외를 기본 ErrorType으로 분류하고,
            final ErrorTypes type = mapException.apply(e);

            // 2) 문맥(context)에 맞게 조정 함수가 있으면 그 결과로 최종 Result를 결정한다.
            final Result result;
            if (adjustError != null) {
                result = adjustError.apply(context, type);
            } else {
                result = new Result(type.destination(), type.penalty());
            }

            if (onError == null) {
                throw new IllegalStateException("OnError is not set.");
            }

            // 3) 최종 결정된 Result에 따라 실제 라우팅/페널티 동작을 수행한다.
            onError.apply(context, input, result, e);
        }
        return false;
    }

    /**
     * 입력을 받아 실제 작업을 수행하며 Exception을 던질 수 있는 함수형 인터페이스.
     */
    @FunctionalInterface
    public interface Procedure<I> {
        void apply(I input) throws Exception;
    }

    /**
     * 최종 결정된 Result(및 원본 Exception)를 바탕으로 입력에 대해 실제 처리(라우팅, 로깅 등)를 수행하는 함수형 인터페이스.
     */
    public interface OnError<C, I> {
        void apply(C context, I input, Result result, Exception e);

        /**
         * 이 OnError를 수행한 뒤 이어서 다른 OnError도 수행하도록 체이닝한다.
         * (예: RollbackOnFailure.createOnError()가 기존 OnError 뒤에 discontinue 검사를 덧붙일 때 사용)
         */
        default OnError<C, I> andThen(OnError<C, I> after) {
            return (c, i, r, e) -> {
                apply(c, i, r, e);
                after.apply(c, i, r, e);
            };
        }
    }
}