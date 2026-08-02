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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/ExecuteSQL.java
 */
package io.datadynamics.nifi.processors.db.executesql;


import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

import static io.datadynamics.nifi.processors.db.executesql.AvroUtil.CodecType;
import static io.datadynamics.nifi.processors.db.executesql.JdbcProperties.*;

/**
 * SQL SELECT 쿼리를 실행하고 결과를 Avro 형식으로 변환하여 FlowFile로 출력하는 프로세서.
 * 스트리밍 방식으로 처리하므로 매우 큰 결과 집합도 지원한다. 타이머/cron 스케줄러로 주기적으로
 * 실행하거나, 입력 FlowFile에 의해 트리거될 수 있으며, 후자의 경우 FlowFile 속성을 이용한
 * 파라미터 바인딩(sql.args.N.type/value/format)을 지원한다.
 */
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"dd", "custom", "sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("제공된 SQL SELECT 쿼리를 실행한다. 쿼리 결과는 Avro 형식으로 변환된다."
        + " 스트리밍 방식을 사용하므로 임의로 큰 결과 집합도 지원된다. 이 프로세서는 표준 스케줄링 방법을 사용하여 "
        + "타이머나 cron 표현식으로 실행되도록 예약할 수 있으며, 입력 FlowFile에 의해 트리거될 수도 있다. "
        + "입력 FlowFile에 의해 트리거되는 경우, 해당 FlowFile의 속성을 SELECT 쿼리를 평가할 때 사용할 수 있으며 "
        + "쿼리에서는 ?를 사용해 파라미터를 이스케이프할 수 있다. 이 경우 사용할 파라미터는 sql.args.N.type 및 "
        + "sql.args.N.value라는 명명 규칙을 따르는 FlowFile 속성으로 존재해야 하며, 여기서 N은 양의 정수이다. "
        + "sql.args.N.type은 JDBC Type을 나타내는 숫자여야 한다. FlowFile의 콘텐츠는 UTF-8 형식이어야 한다. "
        + "FlowFile 속성 'executesql.row.count'는 선택된 행의 개수를 나타낸다.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "sql.args.N.type", description = "입력 FlowFile은 파라미터화된 SQL 문으로 취급된다. 각 파라미터의 타입은 해당 파라미터의 JDBC "
                + "Type을 나타내는 정수로 지정한다. 다음 타입이 허용된다: [LONGNVARCHAR: -16], [BIT: -7], [BOOLEAN: 16], [TINYINT: -6], [BIGINT: -5], "
                + "[LONGVARBINARY: -4], [VARBINARY: -3], [BINARY: -2], [LONGVARCHAR: -1], [CHAR: 1], [NUMERIC: 2], [DECIMAL: 3], [INTEGER: 4], [SMALLINT: 5] "
                + "[FLOAT: 6], [REAL: 7], [DOUBLE: 8], [VARCHAR: 12], [DATE: 91], [TIME: 92], [TIMESTAMP: 93], [VARCHAR: 12], [CLOB: 2005], [NCLOB: 2011]"),
        @ReadsAttribute(attribute = "sql.args.N.value", description = "입력 FlowFile은 파라미터화된 SQL 문으로 취급된다. 파라미터의 값은 "
                + "sql.args.1.value, sql.args.2.value, sql.args.3.value와 같은 형식으로 지정한다. sql.args.1.value 파라미터의 타입은 sql.args.1.type 속성으로 지정된다."),
        @ReadsAttribute(attribute = "sql.args.N.format", description = "이 속성은 항상 선택 사항이지만, 기본 옵션이 데이터에 맞지 않을 수도 있다. "
                + "입력 FlowFile은 파라미터화된 SQL 문으로 취급된다. 일부 경우에는 "
                + "형식(format) 옵션을 지정해야 하며, 현재 이는 이진 데이터 타입, 날짜, 시간, 타임스탬프에만 적용된다. 이진 데이터 타입(기본값 'ascii') - "
                + "ascii: 속성 값의 각 문자가 하나의 바이트를 나타낸다. 이는 Avro 프로세서가 제공하는 형식이다. "
                + "base64: 문자열이 Base64로 인코딩되어 있으며 바이트로 디코딩할 수 있다. "
                + "hex: 문자열이 모두 대문자로 hex 인코딩되어 있으며 앞에 '0x'가 붙지 않는다. "
                + "날짜/시간/타임스탬프 - "
                + "Date, Time, Timestamp 형식은 모두 사용자 정의 형식 또는 명명된 형식('yyyy-MM-dd', 'ISO_OFFSET_DATE_TIME')을 "
                + "java.time.format.DateTimeFormatter 규격에 따라 지원한다. "
                + "지정하지 않으면 long 값은 유닉스 epoch(1970/1/1부터의 밀리초)로 간주되며, 문자열 값은 "
                + "Date의 경우 'yyyy-MM-dd' 형식, Time의 경우 'HH:mm:ss.SSS' 형식(Derby나 MySQL 등 일부 데이터베이스 엔진은 밀리초를 지원하지 않아 잘라낼 수 있음), "
                + "Timestamp의 경우 'yyyy-MM-dd HH:mm:ss.SSS' 형식이 사용된다.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "executesql.row.count", description = "쿼리가 반환한 행의 개수를 포함한다. "
                + "'Max Rows Per Flow File'이 설정된 경우, 이 값은 전체 결과 집합이 아니라 해당 Flow File에 포함된 행의 개수를 반영한다."),
        @WritesAttribute(attribute = "executesql.query.duration", description = "쿼리 실행 시간과 가져오기(fetch) 시간을 합산한 시간(밀리초). "
                + "'Max Rows Per Flow File'이 설정된 경우, 이 값은 전체 결과 집합이 아니라 해당 Flow File에 포함된 행에 대한 가져오기 시간만 반영한다."),
        @WritesAttribute(attribute = "executesql.query.executiontime", description = "쿼리 실행 시간(밀리초). "
                + "이 값은 'Max Rows Per Flow File' 설정과 무관하게 쿼리 실행 시간을 반영한다."),
        @WritesAttribute(attribute = "executesql.query.fetchtime", description = "결과 집합을 가져오는 시간(밀리초). "
                + "'Max Rows Per Flow File'이 설정된 경우, 이 값은 전체 결과 집합이 아니라 해당 Flow File에 포함된 행에 대한 가져오기 시간만 반영한다."),
        @WritesAttribute(attribute = "executesql.resultset.index", description = "여러 개의 결과 집합이 반환된다고 가정할 때, "
                + "이 결과 집합의 0부터 시작하는 인덱스."),
        @WritesAttribute(attribute = "executesql.error.message", description = "입력 flow file을 처리하는 도중 "
                + "예외가 발생하면 해당 Flow File은 failure로 라우팅되며 이 속성에는 예외 메시지가 설정된다."),
        @WritesAttribute(attribute = "fragment.identifier", description = "'Max Rows Per Flow File'이 설정된 경우 동일한 쿼리 결과 집합에서 나온 모든 FlowFile은 "
                + "동일한 fragment.identifier 속성 값을 가진다. 이를 통해 결과들을 서로 연관지을 수 있다."),
        @WritesAttribute(attribute = "fragment.count", description = "'Max Rows Per Flow File'이 설정된 경우 이는 단일 ResultSet으로부터 "
                + "생성된 FlowFile의 총 개수이다. 이는 "
                + "fragment.identifier 속성과 함께 사용하여 입력 ResultSet에 속했던 FlowFile의 개수를 파악하는 데 사용할 수 있다. Output Batch Size가 설정된 경우 이 "
                + "속성은 채워지지 않는다."),
        @WritesAttribute(attribute = "fragment.index", description = "'Max Rows Per Flow File'이 설정된 경우 동일한 결과 집합 FlowFile로부터 파생된 "
                + "출력 FlowFile 목록에서 이 FlowFile의 위치이다. 이는 "
                + "fragment.identifier 속성과 함께 사용하여 어떤 FlowFile들이 동일한 쿼리 결과 집합에서 유래했는지, 그리고 어떤 순서로 "
                + "FlowFile이 생성되었는지 파악하는 데 사용할 수 있다."),
        @WritesAttribute(attribute = "input.flowfile.uuid", description = "프로세서에 입력 연결이 있는 경우, 출력 FlowFile은 이 속성이 "
                + "입력 FlowFile의 UUID 값으로 설정된다. 입력 연결이 없으면 이 속성은 추가되지 않는다.")
})
@SupportsSensitiveDynamicProperties
@DynamicProperties({
        @DynamicProperty(name = "sql.args.N.type",
                value = "SQL type argument to be supplied",
                description = "입력 FlowFile은 파라미터화된 SQL 문으로 취급된다. 각 파라미터의 타입은 해당 파라미터의 JDBC "
                        + "Type을 나타내는 정수로 지정한다. 다음 타입이 허용된다: [LONGNVARCHAR: -16], [BIT: -7], [BOOLEAN: 16], [TINYINT: -6], [BIGINT: -5], "
                        + "[LONGVARBINARY: -4], [VARBINARY: -3], [BINARY: -2], [LONGVARCHAR: -1], [CHAR: 1], [NUMERIC: 2], [DECIMAL: 3], [INTEGER: 4], [SMALLINT: 5] "
                        + "[FLOAT: 6], [REAL: 7], [DOUBLE: 8], [VARCHAR: 12], [DATE: 91], [TIME: 92], [TIMESTAMP: 93], [VARCHAR: 12], [CLOB: 2005], [NCLOB: 2011]"),
        @DynamicProperty(name = "sql.args.N.value",
                value = "Argument to be supplied",
                description = "입력 FlowFile은 파라미터화된 SQL 문으로 취급된다. 파라미터의 값은 "
                        + "sql.args.1.value, sql.args.2.value, sql.args.3.value와 같은 형식으로 지정한다. sql.args.1.value 파라미터의 타입은 sql.args.1.type 속성으로 지정된다."),
        @DynamicProperty(name = "sql.args.N.format",
                value = "SQL format argument to be supplied",
                description = "이 속성은 항상 선택 사항이지만, 기본 옵션이 데이터에 맞지 않을 수도 있다. "
                        + "입력 FlowFile은 파라미터화된 SQL 문으로 취급된다. 일부 경우에는 "
                        + "형식(format) 옵션을 지정해야 하며, 현재 이는 이진 데이터 타입, 날짜, 시간, 타임스탬프에만 적용된다. 이진 데이터 타입(기본값 'ascii') - "
                        + "ascii: 속성 값의 각 문자가 하나의 바이트를 나타낸다. 이는 Avro 프로세서가 제공하는 형식이다. "
                        + "base64: 문자열이 Base64로 인코딩되어 있으며 바이트로 디코딩할 수 있다. "
                        + "hex: 문자열이 모두 대문자로 hex 인코딩되어 있으며 앞에 '0x'가 붙지 않는다. "
                        + "날짜/시간/타임스탬프 - "
                        + "Date, Time, Timestamp 형식은 모두 사용자 정의 형식 또는 명명된 형식('yyyy-MM-dd', 'ISO_OFFSET_DATE_TIME')을 "
                        + "java.time.format.DateTimeFormatter 규격에 따라 지원한다. "
                        + "지정하지 않으면 long 값은 유닉스 epoch(1970/1/1부터의 밀리초)로 간주되며, 문자열 값은 "
                        + "Date의 경우 'yyyy-MM-dd' 형식, Time의 경우 'HH:mm:ss.SSS' 형식(Derby나 MySQL 등 일부 데이터베이스 엔진은 밀리초를 지원하지 않아 잘라낼 수 있음), "
                        + "Timestamp의 경우 'yyyy-MM-dd HH:mm:ss.SSS' 형식이 사용된다.")
})
public class ExecuteSQL extends AbstractExecuteSQL {

    public static final PropertyDescriptor COMPRESSION_FORMAT = new PropertyDescriptor.Builder()
            .name("compression-format")
            .displayName("압축 형식")
            .description("Avro 파일을 기록할 때 사용할 압축 유형. 기본값은 None(압축 없음)이다.")
            .allowableValues(CodecType.values())
            .defaultValue(CodecType.NONE.toString())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    /**
     * 이 프로세서가 지원하는 Relationship 집합과 PropertyDescriptor 목록을 구성한다.
     * ExecuteSQLRecord와 달리 결과를 항상 Avro 포맷으로 기록하며, Record Writer 대신
     * COMPRESSION_FORMAT 등 Avro 전용 속성을 사용한다.
     */
    public ExecuteSQL() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(SQL_PRE_QUERY);
        pds.add(SQL_SELECT_QUERY);
        pds.add(SQL_POST_QUERY);
        pds.add(QUERY_TIMEOUT);
        pds.add(NORMALIZE_NAMES_FOR_AVRO);
        pds.add(USE_AVRO_LOGICAL_TYPES);
        pds.add(COMPRESSION_FORMAT);
        pds.add(DEFAULT_PRECISION);
        pds.add(DEFAULT_SCALE);
        pds.add(MAX_ROWS_PER_FLOW_FILE);
        pds.add(OUTPUT_BATCH_SIZE);
        pds.add(FETCH_SIZE);
        pds.add(AUTO_COMMIT);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    /**
     * 사용자가 설정한 속성 값들로부터 Avro 변환 옵션을 구성하고, 결과 집합을 Avro 형식으로
     * 기록하는 DefaultAvroSqlWriter를 생성한다.
     */
    @Override
    protected SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess) {
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final Boolean useAvroLogicalTypes = context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer defaultPrecision = context.getProperty(DEFAULT_PRECISION).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer defaultScale = context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final String codec = context.getProperty(COMPRESSION_FORMAT).getValue();

        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                .convertNames(convertNamesForAvro)
                .useLogicalTypes(useAvroLogicalTypes)
                .defaultPrecision(defaultPrecision)
                .defaultScale(defaultScale)
                .maxRows(maxRowsPerFlowFile)
                .codecFactory(codec)
                .build();
        return new DefaultAvroSqlWriter(options);
    }

    /**
     * sql.args.N.type/value/format 형태의 동적 속성(Dynamic Property)을 허용하기 위한
     * PropertyDescriptor를 생성한다. Expression Language를 지원하는 문자열 검증기를 사용한다.
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .build();
    }
}