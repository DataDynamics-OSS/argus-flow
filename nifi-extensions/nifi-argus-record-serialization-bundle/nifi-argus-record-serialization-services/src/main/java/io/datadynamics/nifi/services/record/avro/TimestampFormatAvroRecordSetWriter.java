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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/avro/AvroRecordSetWriter.java
 */
package io.datadynamics.nifi.services.record.avro;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.io.BinaryEncoder;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Timestamp 필드의 포맷 지정 및 시간 보정 기능을 지원하는 Avro RecordSetWriter 컨트롤러 서비스.
 *
 * <p>기본 Avro RecordSetWriter와 달리, Avro 스키마에 정의된 Property(기본 이름: {@code properties})를
 * 통해 Timestamp(logical type이 {@code timestamp-millis} 또는 {@code timestamp-micros}인) 필드를
 * 파싱할 때 사용할 날짜/시간 패턴을 컬럼별로 지정할 수 있으며, 변환된 타임스탬프 값에 시간(hour) 단위의
 * 오프셋을 더할 수 있다. 실제 변환 로직은 {@link TimestampFormatAvroTypeUtil}에 위임한다.</p>
 */
@Tags({"custom", "timestamp", "datadynamics", "avro", "result", "set", "writer", "serializer", "record", "recordset", "row"})
@CapabilityDescription("RecordSet의 내용을 Binary Avro 형식으로 기록한다.")
public class TimestampFormatAvroRecordSetWriter extends SchemaRegistryRecordSetWriter implements RecordSetWriterFactory {
    // Timestamp 파싱 패턴 정보가 담긴 Avro 스키마 Property의 이름을 지정하는 속성
    public static final PropertyDescriptor TIMESTAMP_FORMAT_PROPERTY_NAME = new PropertyDescriptor.Builder()
            .name("timestamp-format-property-name")
            .displayName("Timestamp 형식을 위한 Avro Schema Property 이름")
            .description("Timestamp 컬럼을 처리할 때 사용할 형식(Format)을 정의한 Avro Schema의 Property Name (기본값: properties)")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("properties")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    // 변환된 Timestamp 값에 더할 시간(hour) 오프셋을 지정하는 속성
    public static final PropertyDescriptor ADD_HOURS = new PropertyDescriptor.Builder()
            .name("add-hours")
            .displayName("시간 추가")
            .description("Timestamp 컬럼 값에 지정한 시간(hour)만큼 더한다.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();
    // Encoder 재사용 풀의 최대 크기를 지정하는 속성. Encoder 생성 비용이 크므로 풀링하여 재사용한다.
    static final PropertyDescriptor ENCODER_POOL_SIZE = new Builder()
            .name("encoder-pool-size")
            .displayName("Encoder 풀 크기")
            .description("Avro Writer는 Encoder를 사용해야 한다. Encoder 생성 비용은 크지만, 한 번 생성하면 재사용할 수 있다. 이 속성은 풀링되어 재사용될 수 있는 Encoder의 최대 개수를 제어한다." +
                    " 이 값을 너무 작게 설정하면 성능이 저하될 수 있고, 너무 크게 설정하면 더 많은 힙 메모리를 사용하게 된다. Avro Writer의 Schema Write Strategy가 'Embed Avro Schema'로" +
                    " 설정된 경우에는 이 속성이 무시된다.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("32")
            .build();
    // Schema Write Strategy 중 하나로, Avro 스키마를 콘텐츠에 직접 내장(embed)하는 방식을 나타내는 값
    static final AllowableValue AVRO_EMBEDDED = new AllowableValue("avro-embedded", "Avro Schema 내장",
            "Avro에서 일반적으로 그러하듯, FlowFile의 콘텐츠에 Avro 스키마가 내장된다.");
    // Avro 스키마 파싱 결과를 캐싱할 때 사용할 캐시 크기를 지정하는 속성
    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("캐시 크기")
            .description("캐시할 스키마 개수를 지정한다.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();
    // Schema Write Strategy가 'Embed Avro Schema'인 경우 반드시 필요한 스키마 필드 집합
    private static final Set<SchemaField> requiredSchemaFields = EnumSet.of(SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);
    // Avro 파일을 기록할 때 사용할 압축 형식을 지정하는 속성
    private static final PropertyDescriptor COMPRESSION_FORMAT = new Builder()
            .name("compression-format")
            .displayName("압축 형식")
            .description("Avro 파일을 기록할 때 사용할 압축 유형. 기본값은 None(압축 없음)이다.")
            .allowableValues(CodecType.values())
            .defaultValue(CodecType.NONE.toString())
            .required(true)
            .build();
    // 파싱된 Avro 스키마를 스키마 텍스트 기준으로 캐싱하는 로딩 캐시
    private LoadingCache<String, Schema> compiledAvroSchemaCache;
    // 재사용을 위한 BinaryEncoder 풀 (생성 비용이 크므로 풀링하여 재사용)
    private volatile BlockingQueue<BinaryEncoder> encoderPool;
    // Timestamp 패턴이 정의된 Avro 스키마 Property의 이름 (onEnabled 시점에 캐싱)
    private String timestampFormatPropertyKeyName;
    // Timestamp 값에 더할 시간(hour) 오프셋 (onEnabled 시점에 캐싱)
    private int addHours;

    /**
     * 컨트롤러 서비스가 활성화될 때 호출되며, 스키마 캐시와 Encoder 풀을 초기화하고
     * Timestamp 관련 속성 값을 필드에 캐싱한다.
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        compiledAvroSchemaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build(schemaText -> new Schema.Parser().parse(schemaText));

        final int capacity = context.getProperty(ENCODER_POOL_SIZE).evaluateAttributeExpressions().asInteger();
        encoderPool = new LinkedBlockingQueue<>(capacity);

        if (context.getProperty(TIMESTAMP_FORMAT_PROPERTY_NAME).isSet()) {
            timestampFormatPropertyKeyName = context.getProperty(TIMESTAMP_FORMAT_PROPERTY_NAME).evaluateAttributeExpressions().getValue();
        } else {
            timestampFormatPropertyKeyName = "properties";
        }

        if (context.getProperty(ADD_HOURS).isSet()) {
            addHours = Integer.parseInt(context.getProperty(ADD_HOURS).evaluateAttributeExpressions().getValue());
        } else {
            addHours = 0;
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[DFM] TimestampFormatAvroRecordSetWriter : Timestamp Pattern Property Key Name = {}, Add Hour = {}", timestampFormatPropertyKeyName, addHours);
        }
    }

    /**
     * 컨트롤러 서비스가 비활성화될 때 호출되며, Encoder 풀을 비워 리소스를 정리한다.
     */
    @OnDisabled
    public void cleanup() {
        if (encoderPool != null) {
            encoderPool.clear();
        }
    }

    /**
     * 지정된 Schema Write Strategy에 따라 Avro 스키마를 콘텐츠에 내장하는 Writer 또는 외부 스키마 접근
     * 방식을 사용하는 Writer를 생성한다. 두 경우 모두 캐싱된 Timestamp 패턴 속성 이름과 시간 오프셋을
     * Writer에 전달한다.
     */
    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema recordSchema, final OutputStream out, final Map<String, String> variables) throws IOException {
        final String strategyValue = getConfigurationContext().getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        final String compressionFormat = getConfigurationContext().getProperty(COMPRESSION_FORMAT).getValue();

        try {
            final Schema avroSchema;
            try {
                if (recordSchema.getSchemaFormat().isPresent() && recordSchema.getSchemaFormat().get().equals(AvroTypeUtil.AVRO_SCHEMA_FORMAT)) {
                    final Optional<String> textOption = recordSchema.getSchemaText();
                    if (textOption.isPresent()) {
                        avroSchema = compiledAvroSchemaCache.get(textOption.get());
                    } else {
                        avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                    }
                } else {
                    avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                }
            } catch (final Exception e) {
                throw new SchemaNotFoundException("Failed to compile Avro Schema", e);
            }

            if (AVRO_EMBEDDED.getValue().equals(strategyValue)) {
                return new WriteAvroResultWithSchema(avroSchema, out, getCodecFactory(compressionFormat), timestampFormatPropertyKeyName, addHours);
            } else {
                return new WriteAvroResultWithExternalSchema(avroSchema, recordSchema, getSchemaAccessWriter(recordSchema, variables), out, encoderPool, getLogger(),
                        timestampFormatPropertyKeyName, addHours);
            }
        } catch (final SchemaNotFoundException e) {
            throw new ProcessException("Could not determine the Avro Schema to use for writing the content", e);
        }
    }

    /**
     * 압축 형식 속성 값(문자열)을 실제 Avro {@link CodecFactory}로 변환한다.
     * LZO는 별도 코덱이 없어 xz 코덱으로 대체 처리한다.
     */
    private CodecFactory getCodecFactory(String property) {
        CodecType type = CodecType.valueOf(property);
        switch (type) {
            case BZIP2:
                return CodecFactory.bzip2Codec();
            case DEFLATE:
                return CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
            case LZO:
                return CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
            case SNAPPY:
                return CodecFactory.snappyCodec();
            case NONE:
            default:
                return CodecFactory.nullCodec();
        }
    }

    /**
     * 상위 클래스가 제공하는 속성 목록에 이 컨트롤러 서비스 고유의 속성(압축 형식, 캐시 크기,
     * Encoder 풀 크기, Timestamp 형식 Property 이름, 시간 추가)을 추가하여 반환한다.
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(COMPRESSION_FORMAT);
        properties.add(CACHE_SIZE);
        properties.add(ENCODER_POOL_SIZE);
        properties.add(TIMESTAMP_FORMAT_PROPERTY_NAME);
        properties.add(ADD_HOURS);
        return properties;
    }

    /**
     * 상위 클래스가 제공하는 Schema Write Strategy 값 목록의 맨 앞에 'Avro Schema 내장' 전략을 추가한다.
     */
    @Override
    protected List<AllowableValue> getSchemaWriteStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(AVRO_EMBEDDED);
        allowableValues.addAll(super.getSchemaWriteStrategyValues());
        return allowableValues;
    }

    /**
     * 기본 Schema Write Strategy로 'Avro Schema 내장' 전략을 사용한다.
     */
    @Override
    protected AllowableValue getDefaultSchemaWriteStrategy() {
        return AVRO_EMBEDDED;
    }

    /**
     * Schema Write Strategy가 'Avro Schema 내장'인 경우, 스키마 텍스트 및 텍스트 형식 필드만
     * 필수로 요구하고, 그 외의 경우에는 상위 클래스의 기본 동작을 따른다.
     */
    @Override
    protected Set<SchemaField> getRequiredSchemaFields(final ValidationContext validationContext) {
        final String writeStrategyValue = validationContext.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        if (writeStrategyValue.equalsIgnoreCase(AVRO_EMBEDDED.getValue())) {
            return requiredSchemaFields;
        }

        return super.getRequiredSchemaFields(validationContext);
    }

    /**
     * Schema Write Strategy가 'Avro Schema 내장'이 아닌데 압축 형식이 None이 아닌 경우, 압축
     * 코덱 정보는 Avro 파일 헤더에 저장되므로 헤더가 콘텐츠에 내장되어야 한다는 검증 오류를 추가한다.
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final String writeStrategyValue = validationContext.getProperty(getSchemaWriteStrategyDescriptor()).getValue();
        final String compressionFormatValue = validationContext.getProperty(COMPRESSION_FORMAT).getValue();
        if (!writeStrategyValue.equalsIgnoreCase(AVRO_EMBEDDED.getValue())
                && !CodecType.NONE.toString().equals(compressionFormatValue)) {
            results.add(new ValidationResult.Builder()
                    .subject(COMPRESSION_FORMAT.getName())
                    .valid(false)
                    .explanation("Avro 압축 코덱은 Avro 파일의 헤더에 저장되므로, 헤더가 콘텐츠에 "
                            + "내장되어 있어야 한다.")
                    .build());
        }

        return results;
    }

    // 지원되는 Avro 압축 코덱 유형
    private enum CodecType {
        BZIP2,
        DEFLATE,
        NONE,
        SNAPPY,
        LZO
    }
}
