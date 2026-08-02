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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-database-utils/src/main/java/org/apache/nifi/util/db/JdbcProperties.java
 */
package io.datadynamics.nifi.processors.db.executesql;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * ExecuteSQL, ExecuteSQLRecord 등 이 번들의 여러 JDBC 관련 프로세서가 공통으로 사용하는
 * PropertyDescriptor들을 한 곳에 모아 정의한다. 프로세서마다 동일한 속성을 중복 정의하지 않고
 * 이 클래스의 상수를 참조하여 재사용함으로써 일관된 이름/설명/검증 규칙을 보장한다.
 */
public class JdbcProperties {

    public static final PropertyDescriptor NORMALIZE_NAMES_FOR_AVRO = new PropertyDescriptor.Builder()
            .name("dbf-normalize")
            .displayName("테이블/컬럼명 정규화")
            .description("컬럼명에 포함된, Avro와 호환되지 않는 문자를 Avro 호환 문자로 변경할지 여부. 예를 들어 콜론과 마침표는 "
                    + "유효한 Avro 레코드를 만들기 위해 밑줄(underscore)로 변경된다.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor USE_AVRO_LOGICAL_TYPES = new PropertyDescriptor.Builder()
            .name("dbf-user-logical-types")
            .displayName("Avro Logical Type 사용")
            .description("DECIMAL/NUMBER, DATE, TIME, TIMESTAMP 컬럼에 대해 Avro Logical Type을 사용할지 여부. "
                    + "비활성화하면 문자열로 기록된다. "
                    + "활성화하면 Logical Type이 사용되어 각각의 하위 타입으로 기록되는데, 구체적으로는 "
                    + "DECIMAL/NUMBER는 logical 'decimal'로서 정밀도(precision)와 스케일(scale) 메타데이터가 포함된 바이트로, "
                    + "DATE는 logical 'date-millis'로서 유닉스 epoch(1970-01-01)로부터의 일수를 나타내는 int로, "
                    + "TIME은 logical 'time-millis'로서 유닉스 epoch로부터의 밀리초를 나타내는 int로, "
                    + "TIMESTAMP는 logical 'timestamp-millis'로서 유닉스 epoch로부터의 밀리초를 나타내는 long으로 기록된다. "
                    + "기록된 Avro 레코드를 읽는 쪽에서도 이 Logical Type을 알고 있다면, Reader 구현에 따라 이 값들을 더 풍부한 컨텍스트로 역직렬화할 수 있다.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor DEFAULT_PRECISION = new PropertyDescriptor.Builder()
            .name("dbf-default-precision")
            .displayName("기본 소수 정밀도(Precision)")
            .description("DECIMAL/NUMBER 값을 'decimal' Avro Logical Type으로 기록할 때,"
                    + " 사용 가능한 자릿수를 나타내는 특정 '정밀도(precision)'가 필요하다."
                    + " 일반적으로 정밀도는 컬럼 데이터 타입 정의나 데이터베이스 엔진의 기본값에 의해 결정된다."
                    + " 그러나 일부 데이터베이스 엔진은 정의되지 않은 정밀도(0)를 반환할 수 있다."
                    + " '기본 소수 정밀도'는 이렇게 정밀도가 정의되지 않은 숫자를 기록할 때 사용된다.")
            .defaultValue(String.valueOf(JdbcCommon.DEFAULT_PRECISION_VALUE))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor DEFAULT_SCALE = new PropertyDescriptor.Builder()
            .name("dbf-default-scale")
            .displayName("기본 소수 스케일(Scale)")
            .description("DECIMAL/NUMBER 값을 'decimal' Avro Logical Type으로 기록할 때,"
                    + " 사용 가능한 소수 자릿수를 나타내는 특정 '스케일(scale)'이 필요하다."
                    + " 일반적으로 스케일은 컬럼 데이터 타입 정의나 데이터베이스 엔진의 기본값에 의해 결정된다."
                    + " 그러나 정의되지 않은 정밀도(0)가 반환되는 경우, 일부 데이터베이스 엔진에서는 스케일 또한 불확실할 수 있다."
                    + " '기본 소수 스케일'은 이렇게 정의되지 않은 숫자를 기록할 때 사용된다."
                    + " 값의 소수 자릿수가 지정된 스케일보다 많으면 값은 반올림되는데,"
                    + " 예를 들어 1.53은 스케일 0에서는 2가 되고, 스케일 1에서는 1.5가 된다.")
            .defaultValue(String.valueOf(JdbcCommon.DEFAULT_SCALE_VALUE))
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    // Default Precision과 Default Scale 속성의 레지스트리(Registry) 전용 버전.
    // Expression Language 평가 범위를 ENVIRONMENT로 제한하여, 파라미터 컨텍스트/레지스트리
    // 환경에서 FlowFile 속성이 아닌 환경 변수만 참조하도록 한다.
    public static final PropertyDescriptor VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(DEFAULT_PRECISION)
                    .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                    .build();

    public static final PropertyDescriptor VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(DEFAULT_SCALE)
                    .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                    .build();
}