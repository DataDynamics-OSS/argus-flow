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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/CSVReader.java
 */
package io.datadynamics.nifi.services.record.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.NonCloseableInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tags({"csv", "parse", "record", "row", "reader", "delimited", "comma", "separated", "values"})
@CapabilityDescription("CSV 형식의 데이터를 파싱하여 CSV 파일의 각 행을 개별 레코드로 반환한다. "
        + "이 Reader는 '헤더 라인'이 존재하는 경우 CSV의 첫 번째 라인을 기반으로 스키마를 추론하거나, "
        + "값을 해석하기 위한 명시적 스키마를 제공하는 방식을 모두 지원한다. 자세한 내용은 컨트롤러 서비스의 Usage 문서를 참고하라.")
public class CSVReader extends SchemaRegistryService implements RecordReaderFactory {

    // 지원하는 CSV 파서 종류
    public static final AllowableValue APACHE_COMMONS_CSV = new AllowableValue("commons-csv", "Apache Commons CSV",
            "Apache Commons CSV 라이브러리 기반의 CSV 파서 구현체.");
    public static final AllowableValue JACKSON_CSV = new AllowableValue("jackson-csv", "Jackson CSV",
            "Jackson Dataformats 라이브러리 기반의 CSV 파서 구현체.");

    public static final PropertyDescriptor CSV_PARSER = new PropertyDescriptor.Builder()
            .name("CSV Parser")
            .displayName("CSV 파서")
            .description("CSV 레코드를 읽을 때 사용할 파서를 지정한다. 참고: 파서마다 지원하는 기능의 범위가 다를 수 있고, "
                    + "성능 수준도 서로 다를 수 있다.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(APACHE_COMMONS_CSV, JACKSON_CSV)
            .defaultValue(APACHE_COMMONS_CSV.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor TRIM_DOUBLE_QUOTE = new PropertyDescriptor.Builder()
            .name("Trim Double Quote")
            .displayName("큰따옴표 제거")
            .description("값의 시작과 끝에 있는 큰따옴표를 제거할지 여부를 지정한다. 예를 들어 trim을 사용하면 '\"test\"' 문자열은 "
                    + "'test'로 파싱되고, trim을 사용하지 않으면 '\"test\"'로 파싱된다. "
                    + "'false'로 설정하면 RFC-4180을 완전히 준수하는 것을 의미한다. 기본값은 true(trim 사용)이다.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .dependsOn(CSVUtils.CSV_FORMAT, CSVUtils.RFC_4180)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor FIELD_COUNT = new PropertyDescriptor.Builder()
            .name("필드(컬럼) 개수")
            .description("유효성 검사를 위한 CSV 파일의 필드(컬럼) 개수")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor FAIL_ON_MISMATCH_FIELD_COUNT = new PropertyDescriptor.Builder()
            .name("필드(컬럼) 개수 불일치시 실패처리")
            .description("CSV 파일의 필드(컬럼) 개수가 지정한 필드 개수와 불일치 하는 경우 CSV 파일 실패 처리")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(FIELD_COUNT)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor USE_SCHEMA_FOR_FIELD_COUNT = new PropertyDescriptor.Builder()
            .name("필드(컬럼) 개수로 스키마를 활용")
            .description("별도로 필드(컬럼) 개수를 지정하지 않고 스키마로 필드 개수를 사용")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(false)
            .build();
    private static final AllowableValue HEADER_DERIVED = new AllowableValue("csv-header-derived", "Use String Fields From Header",
            "CSV 파일의 주석이 아닌 첫 번째 라인을 컬럼명이 포함된 헤더 라인으로 간주한다. 헤더의 컬럼명을 사용하고 "
                    + "모든 컬럼을 String 타입으로 가정하여 스키마를 도출한다.");
    private volatile ConfigurationContext context;

    private volatile String csvParser;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;
    private volatile boolean firstLineIsHeader;
    private volatile boolean ignoreHeader;
    private volatile String charSet;

    // 동적인(Expression Language 기반) CSV 포맷 속성이 없는 경우에만 초기화되어 재사용되는 CSVFormat
    private volatile CSVFormat csvFormat;

    // 이 컨트롤러 서비스가 지원하는 프로퍼티 목록. 상위 클래스의 목록에 CSV 관련 속성을 추가한다.
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CSV_PARSER);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        properties.add(CSVUtils.CSV_FORMAT);
        properties.add(CSVUtils.VALUE_SEPARATOR);
        properties.add(CSVUtils.RECORD_SEPARATOR);
        properties.add(CSVUtils.FIRST_LINE_IS_HEADER);
        properties.add(CSVUtils.IGNORE_CSV_HEADER);
        properties.add(CSVUtils.QUOTE_CHAR);
        properties.add(CSVUtils.ESCAPE_CHAR);
        properties.add(CSVUtils.COMMENT_MARKER);
        properties.add(CSVUtils.NULL_STRING);
        properties.add(CSVUtils.TRIM_FIELDS);
        properties.add(CSVUtils.CHARSET);
        properties.add(CSVUtils.ALLOW_DUPLICATE_HEADER_NAMES);
        properties.add(TRIM_DOUBLE_QUOTE);
        properties.add(FIELD_COUNT);
        properties.add(FAIL_ON_MISMATCH_FIELD_COUNT);
        properties.add(USE_SCHEMA_FOR_FIELD_COUNT);
        return properties;
    }

    // 컨트롤러 서비스가 활성화될 때 정적(Expression Language를 사용하지 않는) 속성 값들을 미리 읽어 캐싱한다.
    @OnEnabled
    public void storeStaticProperties(final ConfigurationContext context) {
        this.context = context;

        this.csvParser = context.getProperty(CSV_PARSER).getValue();
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        this.firstLineIsHeader = context.getProperty(CSVUtils.FIRST_LINE_IS_HEADER).asBoolean();
        this.ignoreHeader = context.getProperty(CSVUtils.IGNORE_CSV_HEADER).asBoolean();
        this.charSet = context.getProperty(CSVUtils.CHARSET).getValue();

        // 헤더에서 스키마를 도출하는 전략을 사용하는 경우, 'First Line is Header' 속성 값과 무관하게
        // 첫 번째 라인은 항상 헤더로 취급되도록 보장한다.
        final String accessStrategy = context.getProperty(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY).getValue();
        if (HEADER_DERIVED.getValue().equals(accessStrategy) || SchemaInferenceUtil.INFER_SCHEMA.getValue().equals(accessStrategy)) {
            this.firstLineIsHeader = true;
        }

        if (!CSVUtils.isDynamicCSVFormat(context)) {
            this.csvFormat = CSVUtils.createCSVFormat(context, Collections.emptyMap());
        } else {
            this.csvFormat = null;
        }
    }

    /**
     * 입력 스트림으로부터 스키마를 얻은 뒤(필요 시 헤더 라인을 읽기 위해 스트림을 소비), 스트림을 원위치로 되돌리고
     * 설정된 CSV 파서(Apache Commons CSV 또는 Jackson CSV)에 맞는 RecordReader 인스턴스를 생성한다.
     */
    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        // 헤더를 읽기 위해 Input Stream을 미리 소비할 수 있으므로, BufferedInputStream의 Mark/Reset을 사용한다.
        in.mark(1024 * 1024);
        final RecordSchema schema = getSchema(variables, new NonCloseableInputStream(in), null);
        in.reset();

        final CSVFormat format;
        if (this.csvFormat != null) {
            format = this.csvFormat;
        } else {
            format = CSVUtils.createCSVFormat(context, variables);
        }

        final boolean trimDoubleQuote = context.getProperty(TRIM_DOUBLE_QUOTE).asBoolean();
        final Integer fieldCount = context.getProperty(FIELD_COUNT).evaluateAttributeExpressions(variables).asInteger();
        final boolean failOnMismatchFieldCount = Boolean.TRUE.equals(context.getProperty(FAIL_ON_MISMATCH_FIELD_COUNT).evaluateAttributeExpressions(variables).asBoolean());
        final boolean useSchemaForFieldCount = Boolean.TRUE.equals(context.getProperty(USE_SCHEMA_FOR_FIELD_COUNT).evaluateAttributeExpressions(variables).asBoolean());

        if (APACHE_COMMONS_CSV.getValue().equals(csvParser)) {
            return new CSVRecordReader(in, logger, schema, format, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet, trimDoubleQuote,
                    useSchemaForFieldCount ? Integer.valueOf(schema.getFieldCount()) : fieldCount, failOnMismatchFieldCount);
        } else if (JACKSON_CSV.getValue().equals(csvParser)) {
            return new JacksonCSVRecordReader(in, logger, schema, format, firstLineIsHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, charSet, trimDoubleQuote);
        } else {
            throw new IOException("Parser not supported");
        }
    }

    // 과거 NiFi 1.x 시절에 사용되던 프로퍼티 이름을 현재 이름으로 마이그레이션한다(플로우 업그레이드 시 자동 변환).
    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        super.migrateProperties(config);
        // NiFi 1.x 시절의 프로퍼티 이름
        config.renameProperty("csv-reader-csv-parser", CSV_PARSER.getName());
        config.renameProperty("Trim double quote", TRIM_DOUBLE_QUOTE.getName());
        config.renameProperty(CSVUtils.OLD_FIRST_LINE_IS_HEADER_PROPERTY_NAME, CSVUtils.FIRST_LINE_IS_HEADER.getName());
        config.renameProperty(CSVUtils.OLD_IGNORE_CSV_HEADER_PROPERTY_NAME, CSVUtils.IGNORE_CSV_HEADER.getName());
        config.renameProperty(CSVUtils.OLD_CHARSET_PROPERTY_NAME, CSVUtils.CHARSET.getName());
        config.renameProperty(CSVUtils.OLD_ALLOW_DUPLICATE_HEADER_NAMES_PROPERTY_NAME, CSVUtils.ALLOW_DUPLICATE_HEADER_NAMES.getName());
    }

    // 선택된 스키마 접근 전략 값에 따라 실제 SchemaAccessStrategy 구현체를 반환한다.
    // 헤더 기반 도출과 스키마 추론은 이 클래스에서 직접 처리하고, 그 외는 상위 클래스에 위임한다.
    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(HEADER_DERIVED.getValue())) {
            return new CSVHeaderSchemaStrategy(context);
        } else if (allowableValue.equalsIgnoreCase(SchemaInferenceUtil.INFER_SCHEMA.getValue())) {
            final RecordSourceFactory<CSVRecordAndFieldNames> sourceFactory = (variables, in) -> new CSVRecordSource(in, context, variables);
            final SchemaInferenceEngine<CSVRecordAndFieldNames> inference = new CSVSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));
            return new InferSchemaAccessStrategy<>(sourceFactory, inference, getLogger());
        }

        return super.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    // 스키마 접근 전략 선택지 목록에 헤더 도출 전략과 스키마 추론 전략을 추가로 노출한다.
    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(HEADER_DERIVED);
        allowableValues.add(SchemaInferenceUtil.INFER_SCHEMA);
        return allowableValues;
    }

    // 사용자가 별도로 지정하지 않았을 때 기본으로 사용할 스키마 접근 전략은 스키마 추론이다.
    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return SchemaInferenceUtil.INFER_SCHEMA;
    }
}
