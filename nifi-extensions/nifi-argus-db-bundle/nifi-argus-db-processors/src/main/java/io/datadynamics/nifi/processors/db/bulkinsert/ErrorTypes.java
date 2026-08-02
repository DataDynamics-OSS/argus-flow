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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-put-pattern/src/main/java/org/apache/nifi/processor/util/pattern/ErrorTypes.java
 */
package io.datadynamics.nifi.processors.db.bulkinsert;


import static io.datadynamics.nifi.processors.db.bulkinsert.ErrorTypes.Destination.*;
import static io.datadynamics.nifi.processors.db.bulkinsert.ErrorTypes.Penalty.*;

/**
 * 발생 가능한 일반적인 에러 유형과, 각 유형이 어떻게 처리(라우팅/페널티/yield)되어야 하는지를 정의한다.
 * 각 enum 상수는 (Destination, Penalty) 조합을 가지며, {@link ExceptionHandler}가 예외를
 * 이 enum 값으로 분류(mapException)한 뒤 {@link #result()}를 통해 최종 처리 방식을 결정하는 데 사용한다.
 */
public enum ErrorTypes {

    /**
     * 프로세서 설정 자체가 잘못되어 있어, 입력이 무엇이든 동일한 에러가 반복적으로 발생하는 경우.
     * 실패하는 처리를 너무 자주 재시도하지 않도록 프로세서를 yield 시켜야 한다.
     */
    PersistentFailure(ProcessException, Yield),

    /**
     * 에러가 일시적인지 영구적인지, 그리고 입력 데이터와 관련이 있는지 여부를 알 수 없는 경우.
     */
    UnknownFailure(ProcessException, None),

    /**
     * 입력 데이터 자체에 문제가 있어 페널티 없이 failure 관계로 라우팅되어 복구를 기다리는 경우.
     * 원칙적으로 문제가 해결되기 전까지는 동일한 처리를 다시 시도해서는 안 된다.
     */
    InvalidInput(Failure, None),

    /**
     * 외부 서비스 장애 등으로 인해 프로세서가 일시적으로 사용 불가능한 경우.
     * 재시도하면 성공할 수도 있으나, 당분간은 yield 시켜야 한다.
     */
    TemporalFailure(Retry, Yield),

    /**
     * 입력 데이터의 특성과 관련된 일시적 에러로 인해 처리가 실패한 경우.
     * 재시도하면 성공할 수도 있으나, 당분간은 해당 입력에 페널티를 부여해야 한다.
     */
    TemporalInputFailure(Retry, Penalize),

    /**
     * 아직 처리할 준비가 되지 않은 입력. 입력 큐에 그대로 유지되며 페널티도 함께 부여된다.
     */
    Defer(Self, Penalize);

    private final Destination destination;
    private final Penalty penalty;

    ErrorTypes(Destination destination, Penalty penalty) {
        this.destination = destination;
        this.penalty = penalty;
    }

    public Result result() {
        return new Result(destination, penalty);
    }

    public Destination destination() {
        return this.destination;
    }

    public Penalty penalty() {
        return this.penalty;
    }

    /**
     * 입력이 최종적으로 어디로 라우팅(또는 어떻게 처리)될지를 나타낸다.
     * ProcessException은 예외를 던져 세션 롤백을 유도함을, Self는 입력을 그대로
     * 큐에 남겨둔 채 재시도를 유예함을 의미한다.
     */
    public enum Destination {
        ProcessException, Failure, Retry, Self
    }

    /**
     * 입력을 전달(transfer)할 때 프로세서를 yield 시킬지, FlowFile에 페널티를 부여할지 여부를 나타낸다.
     */
    public enum Penalty {
        Yield, Penalize, None
    }

    /**
     * 하나의 처리 결과(destination + penalty 조합)를 나타내는 값 객체.
     * ErrorTypes enum은 자주 쓰이는 기본 에러 처리 패턴들을 미리 정의해 두고 있으며,
     * {@link RollbackOnFailure#createAdjustError(org.apache.nifi.logging.ComponentLog)} 등에서
     * 컨텍스트에 맞게 이 값을 재조정(adjust)하여 사용한다.
     */
    public static class Result {
        private final Destination destination;
        private final Penalty penalty;

        public Result(Destination destination, Penalty penalty) {
            this.destination = destination;
            this.penalty = penalty;
        }

        public Destination destination() {
            return destination;
        }

        public Penalty penalty() {
            return penalty;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "destination=" + destination +
                    ", penalty=" + penalty +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Result result = (Result) o;

            if (destination != result.destination) return false;
            return penalty == result.penalty;
        }

        @Override
        public int hashCode() {
            int result = destination != null ? destination.hashCode() : 0;
            result = 31 * result + (penalty != null ? penalty.hashCode() : 0);
            return result;
        }
    }

}