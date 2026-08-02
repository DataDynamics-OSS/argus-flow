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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-put-pattern/src/main/java/org/apache/nifi/processor/util/pattern/RollbackOnFailure.java
 */
package io.datadynamics.nifi.processors.db.bulkinsert;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * <p>RollbackOnFailure는 Put과 같은 처리 패턴(process pattern)에서 함수 컨텍스트로 사용되어
 * 설정 가능한(configurable) 에러 처리 방식을 제공한다.
 *
 * <p>
 * RollbackOnFailure는 프로세서에 다음과 같은 특성을 부여할 수 있다:
 * <li>비활성화 상태(기본값)에서는, 에러를 유발한 입력 FlowFile이 에러 유형에 따라 'failure' 또는 'retry' 관계로 라우팅된다.</li>
 * <li>활성화 상태에서는, 입력 FlowFile이 입력 큐에 그대로 유지된다. ProcessException을 던져 프로세스 세션을 롤백시킨다.</li>
 * <li>프로세서가 트랜잭션(transactional)으로 표시되어 있다면, onTrigger 중에 발생한 모든 작업이 롤백 가능하다고 가정한다.</li>
 * <li>트랜잭션이면서 활성화된 경우, 이미 일부 FlowFile이 처리되었더라도 에러 발생 시 세션 전체를 롤백한다.</li>
 * <li>트랜잭션이 아니면서 활성화된 경우, 진행된 처리가 전혀 없을 때에 한해서만 세션을 롤백한다.</li>
 * </p>
 *
 * <p>RollbackOnFailure를 적용하는 방법은 두 가지다. 하나는 {@link ExceptionHandler#adjustError(BiFunction)}를
 * 사용하는 것이고, 다른 하나는 처리 패턴(예: Put)의 adjustRoute 단계를 통해 프로세서의 onTrigger를 구현하는 것이다. </p>
 *
 * <p>두 방식을 함께 사용하는 것도 가능하다. ExceptionHandler는 예외가 발생한 즉시 적용할 수 있는 반면,
 * AdjustRoute는 처리 이후 시점에 반응하지만 더 적은 코드로 구현할 수 있다.</p>
 */
public class RollbackOnFailure {

    public static final PropertyDescriptor ROLLBACK_ON_FAILURE = createRollbackOnFailureProperty("");
    // 사용자가 프로세서 설정을 통해 지정한 "실패 시 롤백" 여부
    private final boolean rollbackOnFailure;
    // 이 프로세서의 onTrigger 내 작업들이 트랜잭션 단위로(모두 성공하거나 모두 롤백되도록) 취급되는지 여부
    private final boolean transactional;
    // 처리를 중단(discontinue)해야 하는 상태인지를 나타내는 플래그. 한번 true가 되면 이후 입력 처리를 멈춰야 한다.
    private boolean discontinue;
    // 트랜잭션이 아닌 프로세서에서, 되돌릴 수 없는 작업(예: 외부 시스템에 반영됨)을 이미 수행한 입력의 개수
    private int processedCount = 0;

    /**
     * 생성자.
     *
     * @param rollbackOnFailure 사용자가 프로세서 설정을 통해 지정해야 하는 값.
     * @param transactional     프로세서가 트랜잭션인지 여부를 지정한다.
     *                          트랜잭션이 아니라면, 프로세서 작업이 되돌릴 수 없는 상태가 되었음을 나타내기 위해
     *                          작업이 성공적으로 끝난 후 반드시 {@link #proceed()}를 호출해야 한다.
     */
    public RollbackOnFailure(boolean rollbackOnFailure, boolean transactional) {
        this.rollbackOnFailure = rollbackOnFailure;
        this.transactional = transactional;
    }

    // "Rollback On Failure" PropertyDescriptor를 생성한다. additionalDescription으로 프로세서별 부가 설명을 덧붙일 수 있다.
    public static PropertyDescriptor createRollbackOnFailureProperty(String additionalDescription) {
        return new PropertyDescriptor.Builder()
                .name("rollback-on-failure")
                .displayName("실패 시 롤백")
                .description("에러 처리 방식을 지정합니다." +
                        " 기본값(false)에서는 FlowFile 처리 중 에러가 발생하면 에러 유형에 따라 FlowFile이" +
                        " 'failure' 또는 'retry' 관계로 라우팅되고, 프로세서는 다음 FlowFile 처리를 계속 진행합니다." +
                        " 반대로, 현재까지 처리된 FlowFile들을 롤백하고 이후 처리를 즉시 중단하고 싶을 수 있습니다." +
                        " 이 경우 '실패 시 롤백' 속성을 활성화하면 됩니다. " +
                        " 활성화하면 실패한 FlowFile은 페널티 없이 입력 관계에 그대로 남아 있으며," +
                        " 성공적으로 처리되거나 다른 방법으로 제거될 때까지 반복적으로 재처리를 시도합니다." +
                        " 너무 잦은 재시도를 방지하려면 적절한 'Yield Duration'을 설정하는 것이 중요합니다." + additionalDescription)
                .allowableValues("true", "false")
                .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                .defaultValue("false")
                .required(true)
                .build();
    }

    /**
     * 함수 컨텍스트(RollbackOnFailure 상태)를 바탕으로 에러 유형을 조정하는, {@link ExceptionHandler}와 함께 사용할 함수를 생성한다.
     */
    public static <FCT extends RollbackOnFailure> BiFunction<FCT, ErrorTypes, ErrorTypes.Result> createAdjustError(final ComponentLog logger) {
        return (c, t) -> {

            ErrorTypes.Result adjusted = null;
            switch (t.destination()) {

                case ProcessException:
                    // 이 프로세스가 롤백 가능하다면 그대로 롤백되도록 둔다.
                    if (!c.canRollback()) {
                        // 예외가 발생했지만 프로세서가 트랜잭션이 아니고 이미 처리된 건수(processedCount)가 0보다 큰 경우,
                        // 이 입력이 성공적으로 처리될 때까지 이후 처리를 막기 위해 destination을 Self로 조정한다.
                        // 이 상태에서 그대로 예외를 던지면 이미 성공한 FlowFile들까지 함께 롤백되어 버린다.
                        // 앞선 입력들에 의해 이미 진행된 부분이 있다면, 그 성공한 입력들은 'success'로 보내고
                        // 이번 입력만 입력 큐에 남겨두어야 한다.
                        // 만약 이번 입력이 외부 시스템에 일부 작업을 이미 반영했다면, 재시도 시 그 부분 업데이트가
                        // 다시 재생(replay)되어 데이터 중복이 발생할 수 있다.
                        c.discontinue();
                        // 이 FlowFile에 페널티를 주면 안 된다. 페널티를 주면 다른 FlowFile이 먼저 fetch될 수 있기 때문이다.
                        // 이 입력이 끝날 때까지 다른 입력들의 처리를 막아야 한다.
                        adjusted = new ErrorTypes.Result(ErrorTypes.Destination.Self, ErrorTypes.Penalty.Yield);
                    }
                    break;

                case Failure:
                case Retry:
                    if (c.isRollbackOnFailure()) {
                        c.discontinue();
                        if (c.canRollback()) {
                            // 이 프로세스가 롤백 가능하다면, 대신 ProcessException을 던져서 롤백을 유도한다.
                            adjusted = new ErrorTypes.Result(ErrorTypes.Destination.ProcessException, ErrorTypes.Penalty.Yield);
                        } else {
                            // 롤백이 불가능하다면,
                            adjusted = new ErrorTypes.Result(ErrorTypes.Destination.Self, ErrorTypes.Penalty.Yield);
                        }
                    }
                    break;
            }

            if (adjusted != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Adjusted {} to {} based on context rollbackOnFailure={}, processedCount={}, transactional={}",
                            new Object[]{t, adjusted, c.isRollbackOnFailure(), c.getProcessedCount(), c.isTransactional()});
                }
                return adjusted;
            }

            return t.result();
        };
    }

    /**
     * 컨텍스트를 바탕으로 라우팅된 FlowFile들을 보정하는 {@link AdjustRoute} 함수를 생성한다. Put과 같은 처리 패턴과 함께 사용한다.
     * 이 함수는 프로세서 구현이 ExceptionHandler를 사용하지 않고 RollbackOnFailure 컨텍스트를 고려하지 않은 채
     * FlowFile을 직접 transfer해버리는 경우를 커버하는 일종의 안전망(safety net) 역할을 한다.
     */
    public static <FCT extends RollbackOnFailure> PartialFunctions.AdjustRoute<FCT> createAdjustRoute(Relationship... failureRelationships) {
        return (context, session, fc, result) -> {
            if (fc.isRollbackOnFailure()) {
                // 라우팅 결과에 failure 관계가 포함되어 있는지 확인한다.
                for (Relationship failureRelationship : failureRelationships) {
                    if (!result.contains(failureRelationship)) {
                        continue;
                    }
                    if (fc.canRollback()) {
                        throw new ProcessException(String.format(
                                "A FlowFile is routed to %s. Rollback session based on context rollbackOnFailure=%s, processedCount=%d, transactional=%s",
                                failureRelationship.getName(), fc.isRollbackOnFailure(), fc.getProcessedCount(), fc.isTransactional()));
                    } else {
                        // 롤백할 수 없는 경우, 실패한 FlowFile들을 self로 되돌려 보낸다.
                        final Map<Relationship, List<FlowFile>> routedFlowFiles = result.getRoutedFlowFiles();
                        final List<FlowFile> failedFlowFiles = routedFlowFiles.remove(failureRelationship);
                        result.routeTo(failedFlowFiles, Relationship.SELF);
                    }
                }
            }
        };
    }

    // 주어진 OnError 함수 실행 후, 컨텍스트가 처리 중단(discontinue) 상태라면 DiscontinuedException을 추가로 던지도록 합성한다.
    public static <FCT extends RollbackOnFailure, I> ExceptionHandler.OnError<FCT, I> createOnError(ExceptionHandler.OnError<FCT, I> onError) {
        return onError.andThen((context, input, result, e) -> {
            if (context.shouldDiscontinue()) {
                throw new DiscontinuedException("Discontinue processing due to " + e, e);
            }
        });
    }

    // RollbackOnFailure 컨텍스트를 인지하는 onTrigger 실행 래퍼. 롤백 시 페널티 부여 여부를 컨텍스트에 따라 다르게 적용한다.
    public static <FCT extends RollbackOnFailure> void onTrigger(
            ProcessContext context, ProcessSessionFactory sessionFactory, FCT functionContext, ComponentLog logger,
            PartialFunctions.OnTrigger onTrigger) throws ProcessException {

        PartialFunctions.onTrigger(context, sessionFactory, logger, onTrigger, (session, t) -> {
            // RollbackOnFailure가 활성화되어 있다면, 처리 중이던 FlowFile들이 재처리될 수 있도록
            // 롤백 시 페널티를 부여하지 않는다.
            final boolean shouldPenalize = !functionContext.isRollbackOnFailure();
            session.rollback(shouldPenalize);

            // 다만 실패한 FlowFile을 입력 관계에 그대로 두면 너무 자주 재시도될 수 있으므로,
            // 프로세서를 관리적으로(administratively) yield 시킨다.
            if (functionContext.isRollbackOnFailure()) {
                logger.warn("Administratively yielding {} after rolling back due to {}", new Object[]{context.getName(), t}, t);
                context.yield();
            }
        });
    }

    // 되돌릴 수 없는(비트랜잭션) 작업을 성공적으로 수행했을 때 호출하여 처리 건수를 1 증가시킨다.
    public int proceed() {
        return ++processedCount;
    }

    public int getProcessedCount() {
        return processedCount;
    }

    public boolean isRollbackOnFailure() {
        return rollbackOnFailure;
    }

    public boolean isTransactional() {
        return transactional;
    }

    // 트랜잭션이거나, 아직 되돌릴 수 없는 작업을 하나도 수행하지 않았다면 롤백이 가능하다.
    public boolean canRollback() {
        return transactional || processedCount == 0;
    }

    public boolean shouldDiscontinue() {
        return discontinue;
    }

    public void discontinue() {
        this.discontinue = true;
    }
}