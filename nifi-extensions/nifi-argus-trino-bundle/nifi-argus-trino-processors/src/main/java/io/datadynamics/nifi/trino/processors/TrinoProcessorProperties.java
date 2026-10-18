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
package io.datadynamics.nifi.trino.processors;

import io.datadynamics.nifi.trino.api.TrinoConnectionService;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * 세 프로세서가 공유하는 프로퍼티·관계.
 */
final class TrinoProcessorProperties {

    enum ExecutionMode implements DescribedValue {
        SYNCHRONOUS("SYNCHRONOUS", "동기",
                "트리거 스레드가 문장이 끝날 때까지 기다린다. 짧은 문장에 적합하다."),
        BACKGROUND("BACKGROUND", "백그라운드",
                "트리거 스레드는 FlowFile 을 꺼내 세션을 연 채 프로세서 전용 스레드에 넘기고 바로 돌아온다. "
                        + "전용 스레드가 문장을 끝까지 실행한 뒤 같은 세션에서 전송·커밋한다. 수십 분 걸리는 OPTIMIZE·CTAS 에 적합하다.");

        private final String value;
        private final String displayName;
        private final String description;

        ExecutionMode(final String value, final String displayName, final String description) {
            this.value = value;
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    static final PropertyDescriptor TRINO_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Trino Connection Service")
            .displayName("Trino 연결 서비스")
            .description("Trino 연결을 제공하는 TrinoConnectionService.")
            .required(true)
            .identifiesControllerService(TrinoConnectionService.class)
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Query Timeout")
            .displayName("쿼리 제한 시간")
            .description("Statement.setQueryTimeout 으로 전달하는 실행 제한 시간. 0 이면 제한 없음(클러스터 설정을 따름).")
            .required(true)
            .defaultValue("0 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor EXECUTION_MODE = new PropertyDescriptor.Builder()
            .name("Execution Mode")
            .displayName("실행 모드")
            .description("SYNCHRONOUS 는 트리거 스레드에서 실행한다. BACKGROUND 는 프로세서 전용 스레드에서 실행해 NiFi 스레드 풀을 점유하지 않는다. "
                    + "Trino 는 클라이언트가 결과를 계속 가져가지 않으면 query.client.timeout(기본 5분) 뒤 쿼리를 버리므로, "
                    + "어느 모드든 쿼리가 끝날 때까지 연결을 유지한다. 중지하면 실행 중인 문장을 취소하고 FlowFile 은 큐로 되돌린다.")
            .required(true)
            .allowableValues(ExecutionMode.class)
            .defaultValue(ExecutionMode.SYNCHRONOUS)
            .build();

    static final PropertyDescriptor MAX_BACKGROUND_QUERIES = new PropertyDescriptor.Builder()
            .name("Max Background Queries")
            .displayName("최대 백그라운드 쿼리 수")
            .description("BACKGROUND 모드에서 동시에 실행할 최대 문장 수(프로세서 전용 스레드 수). 가득 차면 FlowFile 을 꺼내지 않고 yield 한다.")
            .required(true)
            .defaultValue("4")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .dependsOn(EXECUTION_MODE, ExecutionMode.BACKGROUND)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("실행에 성공한 FlowFile.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL 오류 등으로 실행에 실패한 입력 FlowFile. trino.error.* 속성이 붙는다.")
            .build();

    private TrinoProcessorProperties() {
    }
}
