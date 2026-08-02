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
package io.datadynamics.nifi.processors.db.bulkinsert;


import io.datadynamics.nifi.processors.db.bulkinsert.ErrorTypes.Result;
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
 * <p>ExceptionHandler는 재사용 가능한 부분 함수(partial function)들을 조합하여
 * 구조화된 예외 처리 로직을 제공한다.
 *
 * <p>
 * ExceptionHandler를 사용하는 이점:
 * <li>에러 처리 코드를 외부로 분리하여, 실제 처리 로직(정상 경로)에만 집중할 수 있는 깔끔한 코드를 작성할 수 있다.</li>
 * <li>구체적인 예외를 {@link ErrorTypes}로 분류하여, 에러 유형에 기반한 일관된 에러 처리를 수행할 수 있다.</li>
 * <li>{@link RollbackOnFailure}처럼 컨텍스트를 인지하는(context aware) 에러 처리가 가능하다.</li>
 * </p>
 *
 * 사용 흐름: mapException으로 예외를 {@link ErrorTypes}로 분류 → (선택) adjustError로 컨텍스트에 맞게
 * 결과를 재조정 → onError로 최종 결과에 따라 FlowFile을 라우팅하거나 페널티/yield를 적용한다.
 */
public class ExceptionHandler<C> {

    /**
     * 예외를 {@link ErrorTypes}로 단순 분류하는 함수.
     */
    private Function<Exception, ErrorTypes> mapException;
    /**
     * 컨텍스트(C)를 바탕으로 분류된 에러 유형을 조정하는 함수. (예: RollbackOnFailure 활성화 여부에 따른 재조정)
     */
    private BiFunction<C, ErrorTypes, Result> adjustError;
    /**
     * 최종 결정된 에러 유형에 따라 입력에 대해 실제 동작(라우팅 등)을 수행하는 함수.
     */
    private OnError<C, ?> onError;

    // Penalty 값에 따라 FlowFile에 페널티를 부여하거나 프로세서를 yield 시킨다.
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
     * {@link Result}의 destination과 penalty를 기반으로 입력을 라우팅하는 {@link OnError} 함수 인스턴스를 생성한다.
     *
     * @param context       프로세서를 yield 시키는 데 사용되는 process context
     * @param session       FlowFile에 페널티를 부여하는 데 사용되는 process session
     * @param routingResult 입력 FlowFile이 이 {@link RoutingResult}를 통해 목적지 관계로 라우팅됨
     * @param relFailure    프로세서의 failure 관계
     * @param relRetry      프로세서의 retry 관계
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
     * {@link #createOnError(ProcessContext, ProcessSession, RoutingResult, Relationship, Relationship)}와 동일하나
     * 여러 FlowFile로 구성된 FlowFileGroup을 대상으로 동작한다.
     *
     * @param context       프로세서를 yield 시키는 데 사용되는 process context
     * @param session       FlowFile들에 페널티를 부여하는 데 사용되는 process session
     * @param routingResult 입력 FlowFile들이 이 {@link RoutingResult}를 통해 목적지 관계로 라우팅됨
     * @param relFailure    프로세서의 failure 관계
     * @param relRetry      프로세서의 retry 관계
     * @return 조합된 함수
     */
    public static <C, I extends PartialFunctions.FlowFileGroup> OnError<C, I> createOnGroupError(
            final ProcessContext context, final ProcessSession session, final RoutingResult routingResult,
            final Relationship relFailure, final Relationship relRetry) {
        return (c, g, r, e) -> {
            final Relationship routeTo;
            // destination에 따라 실제 라우팅할 관계를 결정한다.
            // ProcessException/UnknownFailure 등 매핑되지 않은 경우는 아래 default에서 예외를 다시 던진다.
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
     * 예외를 특정 ErrorType으로 매핑하는 함수를 지정한다.
     */
    public void mapException(Function<Exception, ErrorTypes> mapException) {
        this.mapException = mapException;
    }

    /**
     * <p>함수 컨텍스트를 바탕으로 ErrorType을 조정하는 함수를 지정한다.
     * <p>예를 들어 {@link RollbackOnFailure#createAdjustError(ComponentLog)}는 프로세스 세션을
     * 롤백해야 하는지, 아니면 입력을 failure/retry로 전달해야 하는지를 결정한다.
     */
    public void adjustError(BiFunction<C, ErrorTypes, Result> adjustError) {
        this.adjustError = adjustError;
    }

    /**
     * <p>{@link #execute(Object, Object, Procedure)} 호출 시 별도의 OnError가 지정되지 않은 경우
     * 사용될 기본 OnError 함수를 지정한다.
     */
    public void onError(OnError<C, ?> onError) {
        this.onError = onError;
    }

    /**
     * <p>지정된 procedure 함수를 입력값과 함께 실행한다.
     * <p>예외가 발생하면 기본(default) OnError 함수가 호출된다.
     *
     * @param context   함수 컨텍스트
     * @param input     procedure에 전달할 입력값
     * @param procedure 입력값에 대해 실제 작업을 수행하는 함수
     * @return procedure가 문제 없이 완료되면 true. 예외가 발생했지만 {@link OnError}가 이를 처리했다면 false.
     * @throws ProcessException      예외가 {@link OnError}에 의해 처리되지 않은 경우 던져짐
     * @throws DiscontinuedException 예외는 {@link OnError}가 처리했으나, 이후 입력에 대한 처리를
     *                               즉시 중단해야 함을 나타냄
     */
    @SuppressWarnings("unchecked")
    public <I> boolean execute(C context, I input, Procedure<I> procedure) throws ProcessException, DiscontinuedException {
        return execute(context, input, procedure, (OnError<C, I>) onError);
    }

    /**
     * <p>지정된 procedure 함수를 입력값과 함께 실행한다.
     *
     * @param context   함수 컨텍스트
     * @param input     procedure에 전달할 입력값
     * @param procedure 입력값에 대해 실제 작업을 수행하는 함수
     * @param onError   이번 실행에 사용할 {@link OnError} 함수
     * @return procedure가 문제 없이 완료되면 true. 예외가 발생했지만 {@link OnError}가 이를 처리했다면 false.
     * @throws ProcessException      예외가 {@link OnError}에 의해 처리되지 않은 경우 던져짐
     * @throws DiscontinuedException 예외는 {@link OnError}가 처리했으나, 이후 입력에 대한 처리를
     *                               즉시 중단해야 함을 나타냄
     */
    public <I> boolean execute(C context, I input, Procedure<I> procedure, OnError<C, I> onError) throws ProcessException, DiscontinuedException {
        try {
            procedure.apply(input);
            return true;
        } catch (Exception e) {
            // 분류 함수가 없으면 컨텍스트를 알 수 없으므로 그대로 ProcessException으로 전환하여 던진다.
            if (mapException == null) {
                throw new ProcessException("An exception was thrown: " + e, e);
            }

            final ErrorTypes type = mapException.apply(e);

            final Result result;
            if (adjustError != null) {
                result = adjustError.apply(context, type);
            } else {
                result = new Result(type.destination(), type.penalty());
            }

            if (onError == null) {
                throw new IllegalStateException("OnError is not set.");
            }

            onError.apply(context, input, result, e);
        }
        return false;
    }

    // execute()에 의해 감싸져 실행되는 실제 작업. 예외를 던질 수 있으며, 던져진 예외는 ExceptionHandler가 가로챈다.
    @FunctionalInterface
    public interface Procedure<I> {
        void apply(I input) throws Exception;
    }

    // 최종 결정된 Result(destination + penalty)를 바탕으로 입력을 실제로 처리(라우팅 등)하는 함수.
    public interface OnError<C, I> {
        void apply(C context, I input, Result result, Exception e);

        // 이 OnError를 먼저 실행한 뒤, 이어서 after를 실행하는 새로운 OnError를 만든다. (예: RollbackOnFailure의 discontinue 체크 추가)
        default OnError<C, I> andThen(OnError<C, I> after) {
            return (c, i, r, e) -> {
                apply(c, i, r, e);
                after.apply(c, i, r, e);
            };
        }
    }
}