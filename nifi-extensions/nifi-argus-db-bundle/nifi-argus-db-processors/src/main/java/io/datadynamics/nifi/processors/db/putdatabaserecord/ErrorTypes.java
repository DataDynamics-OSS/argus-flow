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
package io.datadynamics.nifi.processors.db.putdatabaserecord;


import static io.datadynamics.nifi.processors.db.putdatabaserecord.ErrorTypes.Destination.*;
import static io.datadynamics.nifi.processors.db.putdatabaserecord.ErrorTypes.Penalty.*;

/**
 * 처리 중 발생할 수 있는 일반적인 오류 유형과, 각 유형이 어떻게 처리(라우팅/페널티)되어야 하는지를 정의한 열거형.
 * ExceptionHandler가 잡은 Exception을 이 열거형 값 중 하나로 분류하면, 각 값에 미리 지정된
 * destination(어느 관계로 보낼지)과 penalty(페널티를 줄지/yield 할지)에 따라 일관되게 처리된다.
 */
public enum ErrorTypes {

    /**
     * 프로세서 설정 자체를 고쳐야 하는 오류. 설정을 고치지 않는 한 어떤 입력이 와도 동일한 오류가 반복된다.
     * 실패하는 처리를 너무 자주 재시도하지 않도록 yield(잠시 스케줄링을 미룸) 처리한다.
     */
    PersistentFailure(ProcessException, Yield),

    /**
     * 오류가 지속적인지 일시적인지, 입력 데이터와 관련이 있는지 여부를 알 수 없는 경우.
     */
    UnknownFailure(ProcessException, None),

    /**
     * 입력 자체에 문제가 있어 복구를 위해 failure 관계로 보낸다. 페널티는 부여하지 않는다.
     * 근본적으로 문제가 해결되기 전에는 동일한 입력을 다시 처리해서는 안 된다.
     */
    InvalidInput(Failure, None),

    /**
     * 외부 서비스 장애 등으로 인해 처리 절차 자체가 일시적으로 사용 불가능한 상태.
     * 재시도하면 성공할 수도 있지만, 당분간은 yield 하여 재시도 간격을 두어야 한다.
     */
    TemporalFailure(Retry, Yield),

    /**
     * 입력 데이터의 특수성과 관련된 일시적 오류로 인해 처리에 실패한 경우.
     * 재시도하면 성공할 수도 있지만, 당분간은 페널티를 부여해야 한다.
     */
    TemporalInputFailure(Retry, Penalize),

    /**
     * 아직 처리할 준비가 되지 않은 입력. 입력 큐에 그대로 유지하면서 페널티도 함께 부여한다.
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
     * 입력이 최종적으로 어디로 보내질지를 나타낸다.
     * ProcessException은 예외를 그대로 던져 세션을 롤백시키는 것을, Failure/Retry는 해당 관계로 라우팅하는 것을,
     * Self는 입력 관계(자기 자신)에 그대로 남겨두는 것을 의미한다.
     */
    public enum Destination {
        ProcessException, Failure, Retry, Self
    }

    /**
     * 입력을 전송할 때 페널티를 부여할지, yield(프로세서 스케줄링 지연) 할지를 나타낸다.
     */
    public enum Penalty {
        Yield, Penalize, None
    }

    /**
     * 하나의 처리 절차(procedure)에 대한 최종 처리 결과(목적지 + 페널티 조합)를 나타낸다.
     * ErrorTypes 열거형은 자주 쓰이는 기본적인 Result 조합들을 미리 정의해 둔 것이다.
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