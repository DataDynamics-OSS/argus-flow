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
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import io.datadynamics.nifi.processors.db.putdatabaserecord.PartialFunctions.AdjustRoute;
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
 * <p>RollbackOnFailure는 {@link Put}과 같은 프로세스 패턴에서 설정 가능한 오류 처리 방식을 제공하기 위한
 * 함수 문맥(function context)으로 사용될 수 있다.
 *
 * <p>
 * RollbackOnFailure는 프로세서에 다음과 같은 특성을 추가할 수 있다:
 * <li>비활성화 상태에서는, 오류가 발생한 입력 FlowFile이 오류 유형에 따라 'failure' 또는 'retry' 관계로 라우팅된다.</li>
 * <li>활성화 상태에서는, 입력 FlowFile들이 입력 큐에 그대로 유지된다. 프로세스 세션을 롤백시키기 위해 ProcessException이 던져진다.</li>
 * <li>트랜잭션(transactional)으로 표시된 경우, 프로세서의 onTrigger 중 발생한 어떤 작업도 롤백 가능하다고 가정한다.</li>
 * <li>트랜잭션이면서 활성화된 경우, 이미 일부 FlowFile이 처리되었더라도 오류가 발생하면 세션 전체를 롤백한다.</li>
 * <li>트랜잭션이 아니면서 활성화된 경우, 진행된 처리가 전혀 없을 때에만 오류 발생 시 세션을 롤백한다.</li>
 * </p>
 *
 * <p>RollbackOnFailure를 적용하는 방법은 두 가지다. 하나는 {@link ExceptionHandler#adjustError(BiFunction)}를 사용하는 것이고,
 * 다른 하나는 {@link Put#adjustRoute(AdjustRoute)}와 같은 프로세스 패턴으로 프로세서의 onTrigger를 구현하는 것이다.</p>
 *
 * <p>두 방식을 함께 사용하는 것도 가능하다. ExceptionHandler는 예외가 발생한 즉시 적용되고,
 * AdjustRoute는 이후 시점에 반응하지만 필요한 코드가 더 적다.</p>
 */
public class RollbackOnFailure {

    public static final PropertyDescriptor ROLLBACK_ON_FAILURE = createRollbackOnFailureProperty("");
    private final boolean rollbackOnFailure;
    private final boolean transactional;
    private boolean discontinue;
    private int processedCount = 0;

    /**
     * 생성자.
     *
     * @param rollbackOnFailure 프로세서 설정을 통해 사용자가 지정해야 하는 값.
     * @param transactional     프로세서가 트랜잭션 방식인지 여부를 지정.
     *                          트랜잭션 방식이 아니라면, 되돌릴 수 없는 작업을 수행했을 때
     *                          {@link #proceed()}를 반드시 호출해서 진행 상황을 표시해야 한다.
     */
    public RollbackOnFailure(boolean rollbackOnFailure, boolean transactional) {
        this.rollbackOnFailure = rollbackOnFailure;
        this.transactional = transactional;
    }

    /**
     * "Rollback On Failure" PropertyDescriptor를 생성한다.
     * additionalDescription을 통해 프로세서별로 추가 설명을 description 뒤에 덧붙일 수 있다.
     */
    public static PropertyDescriptor createRollbackOnFailureProperty(String additionalDescription) {
        return new PropertyDescriptor.Builder()
                .name("rollback-on-failure")
                .displayName("실패 시 롤백")
                .description("오류 처리 방법을 지정합니다." +
                        " 기본값(false)에서는 FlowFile을 처리하는 중 오류가 발생하면 오류 유형에 따라 FlowFile이" +
                        " 'failure' 또는 'retry' 관계로 라우팅되며, 프로세서는 다음 FlowFile 처리를 계속 진행합니다." +
                        " 대신, 현재까지 처리된 FlowFile들을 롤백하고 즉시 추가 처리를 중단하고 싶을 수 있습니다." +
                        " 이 경우 이 'Rollback On Failure' 속성을 활성화하여 그렇게 할 수 있습니다. " +
                        " 활성화되면 실패한 FlowFile은 페널티(penalize) 없이 입력 관계에 그대로 남으며, 성공적으로 처리되거나" +
                        " 다른 방법으로 제거될 때까지 반복적으로 처리가 재시도됩니다." +
                        " 너무 자주 재시도되지 않도록 적절한 'Yield Duration' 값을 설정하는 것이 중요합니다." + additionalDescription)
                .allowableValues("true", "false")
                .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                .defaultValue("false")
                .required(true)
                .build();
    }

    /**
     * 함수 문맥(functional context)에 기반하여 오류 유형을 조정하는, {@link ExceptionHandler}와 함께 사용할 함수를 생성한다.
     */
    public static <FCT extends RollbackOnFailure> BiFunction<FCT, ErrorTypes, ErrorTypes.Result> createAdjustError(final ComponentLog logger) {
        return (c, t) -> {

            ErrorTypes.Result adjusted = null;
            switch (t.destination()) {

                case ProcessException:
                    // 이 처리가 롤백 가능한 상태라면 그대로 롤백되도록 둔다(별도 조정 없음).
                    if (!c.canRollback()) {
                        // 예외가 발생했지만 프로세서가 트랜잭션 방식이 아니고 이미 일부가 처리된 상태(processedCount > 0)라면,
                        // 이 입력이 성공적으로 처리될 때까지 추가 처리를 막기 위해 목적지를 Self로 조정한다.
                        // 이 상태에서 예외를 그대로 던지면 이미 성공한 FlowFile들까지 함께 롤백되어 버린다.
                        // 앞서 처리된 다른 입력들에 의해 진행된 부분은 'success'로 보내고, 이 입력만 입력 큐에 남겨야 한다.
                        // 만약 이 입력이 외부 시스템에 이미 일부 반영을 했다면, 재처리 시 동일한 갱신이 반복되어
                        // 데이터 중복이 발생할 수 있음에 유의해야 한다.
                        c.discontinue();
                        // 이 FlowFile에는 페널티를 주면 안 된다. 페널티를 주면 다른 FlowFile이 먼저 가져와질 수 있는데,
                        // 이 입력이 끝날 때까지는 다른 입력들의 처리를 막아야 하기 때문이다.
                        adjusted = new ErrorTypes.Result(ErrorTypes.Destination.Self, ErrorTypes.Penalty.Yield);
                    }
                    break;

                case Failure:
                case Retry:
                    if (c.isRollbackOnFailure()) {
                        c.discontinue();
                        if (c.canRollback()) {
                            // 롤백이 가능한 상태라면, 세션을 롤백시키기 위해 대신 ProcessException을 던지도록 조정한다.
                            adjusted = new ErrorTypes.Result(ErrorTypes.Destination.ProcessException, ErrorTypes.Penalty.Yield);
                        } else {
                            // 롤백이 불가능한 상태라면, Self로 보내 입력 큐에 남긴다.
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
     * 문맥(context)에 기반하여 라우팅된 FlowFile들을 조정하는, {@link Put}과 같은 프로세스 패턴과 함께 사용할
     * {@link AdjustRoute} 함수를 생성한다.
     * 이 함수는 프로세서 구현체가 ExceptionHandler를 사용하지 않고 RollbackOnFailure 문맥을 고려하지 않은 채
     * FlowFile을 그냥 전송해버린 경우를 보완하는 안전망(safety net) 역할을 한다.
     */
    public static <FCT extends RollbackOnFailure> AdjustRoute<FCT> createAdjustRoute(Relationship... failureRelationships) {
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
                        // 롤백이 불가능한 상태이므로, 실패한 FlowFile들을 Self(입력 관계)로 대신 보낸다.
                        final Map<Relationship, List<FlowFile>> routedFlowFiles = result.getRoutedFlowFiles();
                        final List<FlowFile> failedFlowFiles = routedFlowFiles.remove(failureRelationship);
                        result.routeTo(failedFlowFiles, Relationship.SELF);
                    }
                }
            }
        };
    }

    /**
     * 기존 OnError 뒤에, "처리를 중단해야 하는 상태(discontinue)"인지 검사하여 {@link DiscontinuedException}을
     * 던지는 동작을 덧붙인 OnError를 생성한다. 이를 통해 이후 입력에 대한 처리가 이어지지 않도록 막는다.
     */
    public static <FCT extends RollbackOnFailure, I> ExceptionHandler.OnError<FCT, I> createOnError(ExceptionHandler.OnError<FCT, I> onError) {
        return onError.andThen((context, input, result, e) -> {
            if (context.shouldDiscontinue()) {
                throw new DiscontinuedException("Discontinue processing due to " + e, e);
            }
        });
    }

    /**
     * RollbackOnFailure 문맥을 고려하는 onTrigger 실행 도우미.
     * 세션 롤백 시 penalize 여부를 rollbackOnFailure 설정에 따라 다르게 처리하고,
     * 롤백 이후 너무 자주 재시도되지 않도록 프로세서를 yield 시킨다.
     */
    public static <FCT extends RollbackOnFailure> void onTrigger(
            ProcessContext context, ProcessSessionFactory sessionFactory, FCT functionContext, ComponentLog logger,
            PartialFunctions.OnTrigger onTrigger) throws ProcessException {

        PartialFunctions.onTrigger(context, sessionFactory, logger, onTrigger, (session, t) -> {
            // RollbackOnFailure가 활성화된 경우, 롤백 시 처리 중이던 FlowFile을 penalize 하지 않는다.
            // 이렇게 해야 해당 FlowFile이 입력 관계에 남아 다시 처리될 수 있다.
            final boolean shouldPenalize = !functionContext.isRollbackOnFailure();
            session.rollback(shouldPenalize);

            // 다만, 실패한 FlowFile을 penalize 없이 입력 관계에 그대로 두면 너무 자주 재시도될 수 있다.
            // 따라서 프로세서 차원에서 administratively yield 시킨다.
            if (functionContext.isRollbackOnFailure()) {
                logger.warn("Administratively yielding {} after rolling back due to {}", new Object[]{context.getName(), t}, t);
                context.yield();
            }
        });
    }

    /**
     * 처리(진행)가 한 건 이루어졌음을 표시한다. 트랜잭션이 아닌 프로세서는 되돌릴 수 없는 작업을 수행한 직후
     * 반드시 이 메서드를 호출해야 canRollback() 판단이 정확해진다.
     */
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

    /**
     * 현재 상태에서 세션을 안전하게 롤백할 수 있는지 판단한다.
     * 트랜잭션 방식이면 언제든 롤백 가능하고, 트랜잭션이 아니라면 아직 아무 것도 처리되지 않은 경우(processedCount == 0)에만
     * 롤백이 안전하다(이미 처리된 건이 있다면 롤백 시 외부 시스템과의 정합성이 깨질 수 있기 때문).
     */
    public boolean canRollback() {
        return transactional || processedCount == 0;
    }

    public boolean shouldDiscontinue() {
        return discontinue;
    }

    /**
     * 더 이상 다음 입력을 처리하지 말고 즉시 멈춰야 함을 표시한다.
     */
    public void discontinue() {
        this.discontinue = true;
    }
}