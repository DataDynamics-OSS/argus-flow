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
 *   nifi-nar-bundles/nifi-parquet-bundle/nifi-parquet-processors/src/main/java/org/apache/nifi/parquet/ParquetRecordSetWriter.java
 */
package io.datadynamics.nifi.services.record.parquet;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.parquet.utils.ParquetUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.parquet.utils.ParquetUtils.createParquetConfig;

/**
 * Parquet 형식으로 RecordSet을 기록하는 Record Writer 컨트롤러 서비스.
 * <p>
 * {@link SchemaRegistryRecordSetWriter}를 확장하여 표준 Parquet 기록 기능(압축, 행 그룹 크기,
 * 페이지 크기, INT96 필드 처리 등)을 그대로 제공하면서, 다음 두 가지 커스텀 기능을 추가로 지원한다.
 * <ul>
 *     <li>Avro 스키마의 특정 Property(기본값: {@code properties})에 정의된 포맷 문자열을 이용해
 *     타임스탬프 컬럼을 원하는 형식으로 기록</li>
 *     <li>타임스탬프 컬럼 값에 지정한 시간(시)만큼 더하여 기록 (타임존 보정 등에 사용)</li>
 * </ul>
 * 컴파일된 Avro 스키마는 {@link LoadingCache}를 이용해 캐싱하여 매 FlowFile마다 스키마를 다시
 * 파싱하는 비용을 줄인다.
 */
@Tags({"custom", "timestamp", "datadynamics", "parquet", "result", "set", "writer", "serializer", "record", "recordset", "row"})
@CapabilityDescription("RecordSet의 내용을 Parquet 형식으로 기록합니다.")
public class TimestampFormatParquetRecordSetWriter extends SchemaRegistryRecordSetWriter implements RecordSetWriterFactory {

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Cache Size")
            .displayName("캐시 크기")
            .description("캐시할 스키마의 개수를 지정합니다")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    public static final PropertyDescriptor INT96_FIELDS = new PropertyDescriptor.Builder()
            .name("INT96 Fields")
            .displayName("INT96 필드")
            .description("INT96 타임스탬프로 처리해야 하는 필드의 전체 경로 목록입니다.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor TIMESTAMP_FORMAT_PROPERTY_NAME = new PropertyDescriptor.Builder()
            .name("timestamp-format-property-name")
            .displayName("타임스탬프 형식을 위한 Avro 스키마 Properties 이름")
            .description("Timestamp 컬럼을 처리할 때 사용할 형식(Format)을 정의한 Avro 스키마의 Property Name (기본값: properties)")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("properties")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor ADD_HOURS = new PropertyDescriptor.Builder()
            .name("add-hours")
            .displayName("시간 추가")
            .description("Timestamp 컬럼 값에 지정한 시간(시)만큼 더합니다.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(false)
            .build();

    // 컴파일된 Avro 스키마 캐시. 스키마 텍스트(String)를 key로 하여 Schema.Parser로 파싱한
    // 결과를 재사용함으로써, 동일한 스키마를 매 FlowFile마다 반복 파싱하지 않도록 한다.
    private LoadingCache<String, Schema> compiledAvroSchemaCache;
    // INT96 타임스탬프로 처리할 필드의 전체 경로 목록 (콤마로 구분된 문자열, onEnabled에서 설정됨)
    private String int96Fields;
    // 타임스탬프 포맷 문자열이 정의되어 있는 Avro 스키마 Property의 키 이름 (기본값: properties)
    private String timestampFormatPropertyKeyName;
    // 타임스탬프 컬럼 값에 더할 시간(시) 오프셋
    private int addHours;

    /**
     * 컨트롤러 서비스가 활성화될 때 호출되어 프로퍼티 값을 읽어들이고, Avro 스키마 캐시를 초기화한다.
     * CACHE_SIZE 값으로 스키마 캐시의 최대 크기를 설정하며, INT96_FIELDS / TIMESTAMP_FORMAT_PROPERTY_NAME /
     * ADD_HOURS 프로퍼티는 설정되지 않은 경우 각각 null, "properties", 0을 기본값으로 사용한다.
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        compiledAvroSchemaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build(schemaText -> new Schema.Parser().parse(schemaText));

        if (context.getProperty(INT96_FIELDS).isSet()) {
            int96Fields = context.getProperty(INT96_FIELDS).getValue();
        } else {
            int96Fields = null;
        }

        if (context.getProperty(TIMESTAMP_FORMAT_PROPERTY_NAME).isSet()) { // FIXED
            timestampFormatPropertyKeyName = context.getProperty(TIMESTAMP_FORMAT_PROPERTY_NAME).evaluateAttributeExpressions().getValue();
        } else {
            timestampFormatPropertyKeyName = "properties";
        }

        if (context.getProperty(ADD_HOURS).isSet()) { // FIXED
            addHours = Integer.parseInt(context.getProperty(ADD_HOURS).evaluateAttributeExpressions().getValue());
        } else {
            addHours = 0;
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[DFM] TimestampFormatParquetRecordSetWriter : Timestamp Pattern Property Key Name = {}, Add Hour = {}", timestampFormatPropertyKeyName, addHours);
        }
    }

    /**
     * 실제로 레코드를 Parquet 형식으로 기록할 {@link RecordSetWriter} 인스턴스를 생성한다.
     * RecordSchema가 Avro 형식의 스키마 텍스트를 가지고 있으면 캐시를 통해 파싱된 Avro Schema를
     * 재사용하고, 그렇지 않으면 RecordSchema로부터 Avro Schema를 새로 추출한다.
     * 이렇게 얻은 Avro Schema와 타임스탬프 포맷 관련 설정(timestampFormatPropertyKeyName, addHours)을
     * {@link WriteParquetResult}에 전달하여 실제 기록을 수행하도록 한다.
     */
    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema recordSchema,
                                        final OutputStream out, final Map<String, String> variables) throws IOException {
        final ParquetConfig parquetConfig = createParquetConfig(getConfigurationContext(), variables);
        parquetConfig.setInt96Fields(int96Fields);

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

            // 아래 속성들은 새로 기록되는 Parquet RecordSet으로 전달되어서는 안 된다.
            // 이 속성들이 그대로 유지되면, 이후 해당 FlowFile을 읽는 프로세서가
            // 존재하지 않는(옛 데이터 기준) 오프셋/위치로 접근을 시도할 수 있기 때문이다.
            final Set<String> propertiesToBeCleared = new HashSet<>(Arrays.asList(
                    ParquetAttribute.RECORD_OFFSET,
                    ParquetAttribute.FILE_RANGE_START_OFFSET,
                    ParquetAttribute.FILE_RANGE_END_OFFSET
            ));
            final Map<String, String> filteredVariables = variables.entrySet()
                    .stream()
                    .filter(entry -> !propertiesToBeCleared.contains(entry.getKey()))
                    .collect(toMap(Entry::getKey, Entry::getValue));

            return new WriteParquetResult(avroSchema, recordSchema, getSchemaAccessWriter(recordSchema, filteredVariables), out, parquetConfig, logger, timestampFormatPropertyKeyName, addHours);

        } catch (final SchemaNotFoundException e) {
            throw new ProcessException("Could not determine the Avro Schema to use for writing the content", e);
        }
    }

    /**
     * 이전 버전(NiFi 1.x)의 레거시 속성명을 NiFi 2.x upstream 표준 속성명으로 자동 변환하여
     * 기존 플로우의 하위 호환성을 유지한다.
     */
    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        // NiFi 1.x 레거시 속성명 -> NiFi 2.x upstream 속성명 자동 변환
        config.renameProperty("cache-size", CACHE_SIZE.getName());
        config.renameProperty("int96-fields", INT96_FIELDS.getName());
        config.renameProperty(ParquetUtils.OLD_ROW_GROUP_SIZE_PROPERTY_NAME, ParquetUtils.ROW_GROUP_SIZE.getName());
        config.renameProperty(ParquetUtils.OLD_PAGE_SIZE_PROPERTY_NAME, ParquetUtils.PAGE_SIZE.getName());
        config.renameProperty(ParquetUtils.OLD_DICTIONARY_PAGE_SIZE_PROPERTY_NAME, ParquetUtils.DICTIONARY_PAGE_SIZE.getName());
        config.renameProperty(ParquetUtils.OLD_MAX_PADDING_SIZE_PROPERTY_NAME, ParquetUtils.MAX_PADDING_SIZE.getName());
        config.renameProperty(ParquetUtils.OLD_ENABLE_DICTIONARY_ENCODING_PROPERTY_NAME, ParquetUtils.ENABLE_DICTIONARY_ENCODING.getName());
        config.renameProperty(ParquetUtils.OLD_ENABLE_VALIDATION_PROPERTY_NAME, ParquetUtils.ENABLE_VALIDATION.getName());
        config.renameProperty(ParquetUtils.OLD_WRITER_VERSION_PROPERTY_NAME, ParquetUtils.WRITER_VERSION.getName());
        config.renameProperty(ParquetUtils.OLD_AVRO_ADD_LIST_ELEMENT_RECORDS_PROPERTY_NAME, ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS.getName());
        config.renameProperty(ParquetUtils.OLD_AVRO_WRITE_OLD_LIST_STRUCTURE_PROPERTY_NAME, ParquetUtils.AVRO_WRITE_OLD_LIST_STRUCTURE.getName());
    }

    /**
     * 상위 클래스(SchemaRegistryRecordSetWriter)가 제공하는 공통 프로퍼티에 Parquet 관련
     * 프로퍼티(압축, 행 그룹/페이지 크기 등)와 본 컨트롤러 서비스가 추가로 정의한 타임스탬프
     * 처리용 프로퍼티(TIMESTAMP_FORMAT_PROPERTY_NAME, ADD_HOURS, INT96_FIELDS)를 덧붙여 반환한다.
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CACHE_SIZE);
        properties.add(ParquetUtils.COMPRESSION_TYPE);
        properties.add(ParquetUtils.ROW_GROUP_SIZE);
        properties.add(ParquetUtils.PAGE_SIZE);
        properties.add(ParquetUtils.DICTIONARY_PAGE_SIZE);
        properties.add(ParquetUtils.MAX_PADDING_SIZE);
        properties.add(ParquetUtils.ENABLE_DICTIONARY_ENCODING);
        properties.add(ParquetUtils.ENABLE_VALIDATION);
        properties.add(ParquetUtils.WRITER_VERSION);
        properties.add(ParquetUtils.AVRO_WRITE_OLD_LIST_STRUCTURE);
        properties.add(ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS);
        properties.add(TIMESTAMP_FORMAT_PROPERTY_NAME);
        properties.add(ADD_HOURS);
        properties.add(INT96_FIELDS);
        return properties;
    }
}
