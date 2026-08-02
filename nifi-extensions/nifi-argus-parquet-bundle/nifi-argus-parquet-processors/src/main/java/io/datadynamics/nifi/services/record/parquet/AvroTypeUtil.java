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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-record-utils/nifi-avro-record-utils/src/main/java/org/apache/nifi/avro/AvroTypeUtil.java
 */
package io.datadynamics.nifi.services.record.parquet;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * NiFi 1.x 레거시의 전체 fork(약 1,460라인) 대신 upstream(NiFi 2.10.0)
 * {@link org.apache.nifi.avro.AvroTypeUtil}에 위임하고, 레거시 커스텀 변경분
 * (Avro Schema property 기반 Timestamp Format 지정 + Add Hours 보정)만 유지한 클래스.
 * <p>
 * 레거시 fork가 추가했던 INT96 FIXED(Object[] -> GenericData.Fixed) 변환은
 * upstream 2.10.0의 convertToAvroObject가 이미 일반화하여 지원하므로 fork를 유지하지 않는다.
 */
public class AvroTypeUtil {

    public static final String AVRO_SCHEMA_FORMAT = org.apache.nifi.avro.AvroTypeUtil.AVRO_SCHEMA_FORMAT;

    private static final Logger logger = LoggerFactory.getLogger(AvroTypeUtil.class);

    private static final String LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String LOGICAL_TYPE_TIMESTAMP_MICROS = "timestamp-micros";
    private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * NiFi RecordSchema를 Avro Schema로 변환한다. upstream 구현에 그대로 위임한다.
     */
    public static Schema extractAvroSchema(final RecordSchema recordSchema) {
        return org.apache.nifi.avro.AvroTypeUtil.extractAvroSchema(recordSchema);
    }

    public static GenericRecord createAvroRecord(final Record record, final Schema avroSchema,
                                                 final String timestampFormatPropertyKeyName, final int addHours) throws IOException {
        return createAvroRecord(record, avroSchema, StandardCharsets.UTF_8, timestampFormatPropertyKeyName, addHours);
    }

    /**
     * NiFi {@link Record}을 지정한 Avro Schema에 맞는 {@link GenericRecord}로 변환한다.
     * <ol>
     *     <li>Record의 각 필드값을 순회하면서 이름이 같은 Avro 필드를 찾는다(이름이 다르면 alias로 매핑을 시도).</li>
     *     <li>매핑된 Avro 필드의 스키마를 기준으로 {@link #convertToAvroObject}를 호출하여 타입을 변환한다
     *     (Timestamp 컬럼은 timestampFormatPropertyKeyName/addHours 보정이 적용됨).</li>
     *     <li>Record에는 없지만 Avro Schema에 기본값이 정의된 필드는 기본값을 변환하여 채워 넣는다.</li>
     * </ol>
     *
     * @param record                        변환할 원본 NiFi Record
     * @param avroSchema                    변환 대상 Avro Schema
     * @param charset                       문자열 변환에 사용할 Charset
     * @param timestampFormatPropertyKeyName Timestamp 포맷 정보를 담고 있는 Avro Schema property 키 이름
     * @param addHours                      Timestamp 값에 보정할 시간(시 단위, 음수 가능)
     * @return 변환이 완료된 Avro GenericRecord
     * @throws IOException 변환 중 오류가 발생한 경우
     */
    public static GenericRecord createAvroRecord(final Record record, final Schema avroSchema, final Charset charset,
                                                 final String timestampFormatPropertyKeyName, final int addHours) throws IOException {
        final GenericRecord rec = new GenericData.Record(avroSchema);
        final RecordSchema recordSchema = record.getSchema();

        final Map<String, Object> recordValues = record.toMap();
        for (final Map.Entry<String, Object> entry : recordValues.entrySet()) {
            final Object rawValue = entry.getValue();
            if (rawValue == null) {
                continue;
            }

            final String rawFieldName = entry.getKey();
            final Optional<RecordField> optionalRecordField = recordSchema.getField(rawFieldName);
            if (!optionalRecordField.isPresent()) {
                continue;
            }

            final RecordField recordField = optionalRecordField.get();

            final Field field;
            final Field avroField = avroSchema.getField(rawFieldName);
            if (avroField == null) {
                final Pair<String, Field> fieldPair = lookupField(avroSchema, recordField);
                field = fieldPair.getRight();

                if (field == null) {
                    continue;
                }
            } else {
                field = avroField;
            }

            final String fieldName = field.name();
            String timestampPattern = getTimestampPattern(avroSchema, timestampFormatPropertyKeyName, fieldName); // FIXED
            final Object converted = convertToAvroObject(rawValue, field.schema(), fieldName, charset, timestampPattern, addHours); // FIXED
            rec.put(fieldName, converted);
        }

        // RecordSchema에는 없지만 Avro Schema에 기본값이 정의된 필드가 있는지 확인하고,
        // 있다면 그 기본값을 생성 중인 GenericRecord에 채워 넣는다.
        for (final Field field : avroSchema.getFields()) {
            final Object defaultValue = field.defaultVal();
            if (defaultValue == null || defaultValue == JsonProperties.NULL_VALUE) {
                continue;
            }

            if (rec.get(field.name()) == null) {
                // 기본값이 Avro 입장에서 올바른 타입이 아닐 수 있다(예: long 타입 필드인데 기본값이 0으로 지정된 경우).
                // 따라서 기본값이라 하더라도 Avro Schema 기준으로 타입을 맞추기 위해 convertToAvroObject를 반드시 거쳐야 한다.
                String timestampPattern = getTimestampPattern(avroSchema, timestampFormatPropertyKeyName, field.name());
                final Object normalized = convertToAvroObject(defaultValue, field.schema(), field.name(), StandardCharsets.UTF_8, timestampPattern, addHours);
                rec.put(field.name(), normalized);
            }
        }

        return rec;
    }

    /**
     * Avro Schema의 property(기본 key : properties)에서 Timestamp 컬럼에 적용할 패턴을 찾는다.
     * <ul>
     *     <li>column.all.pattern : 전체 컬럼에 일괄 적용</li>
     *     <li>column.[컬럼명].pattern : 컬럼별 적용</li>
     *     <li>없으면 기본값(yyyy-MM-dd HH:mm:ss)</li>
     * </ul>
     */
    public static String getTimestampPattern(final Schema avroSchema, final String timestampFormatPropertyKeyName, final String fieldName) {
        String timestampPattern;
        if (StringUtils.isEmpty(timestampFormatPropertyKeyName)) {
            // 별도 설정이 없으면 기본값을 적용한다.
            timestampPattern = DEFAULT_TIMESTAMP_FORMAT;
        } else {
            if (avroSchema.getObjectProp(timestampFormatPropertyKeyName) == null) {
                // 별도 설정이 없으면 기본값을 적용한다.
                timestampPattern = DEFAULT_TIMESTAMP_FORMAT;
            } else {
                Map<?, ?> props = (Map<?, ?>) avroSchema.getObjectProp(timestampFormatPropertyKeyName.trim());
                if (props.containsKey("column.all.pattern")) {
                    // 전체가 일괄 적용하는 속성이 있다면 일괄 적용한다.
                    timestampPattern = (String) props.get("column.all.pattern");
                } else {
                    if (props.containsKey(String.format("column.%s.pattern", fieldName))) {
                        // 컬럼별로 적용해야 한다면 컬럼으로 찾는다.
                        timestampPattern = (String) props.get(String.format("column.%s.pattern", fieldName));
                    } else {
                        // 컬럼별로 적용할 것도 없다면 기본값을 그대로 사용한다.
                        timestampPattern = DEFAULT_TIMESTAMP_FORMAT;
                    }
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[DFM] Timestamp Format '{}' = {}", fieldName, timestampPattern);
        }
        return timestampPattern;
    }

    /**
     * NiFi 2.x에서 DataTypeUtils.toTimestamp()/getDateFormat()이 제거되어
     * FieldConverter(StandardFieldConverterRegistry) 기반으로 대체하였다.
     */
    private static Long getLongFromTimestamp(final Object rawValue, final Schema fieldSchema, final String fieldName, final String timestampFormat) {
        final String format;
        if (StringUtils.isEmpty(timestampFormat)) {
            format = org.apache.nifi.avro.AvroTypeUtil.determineDataType(fieldSchema).getFormat();
        } else {
            format = timestampFormat.trim();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[DFM] Timestamp Format = {}", format);
        }

        final FieldConverter<Object, Timestamp> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(Timestamp.class);
        final Timestamp timestamp = converter.convertField(rawValue, Optional.ofNullable(format), fieldName);
        return timestamp.getTime();
    }

    /**
     * 레거시 fork의 _convertToAvroObject() 대응.
     * Timestamp 로지컬 타입(LONG)과 컬렉션 타입(MAP/RECORD)의 재귀 경로만 커스텀으로 유지하고,
     * 나머지 타입 변환은 upstream {@link org.apache.nifi.avro.AvroTypeUtil#convertToAvroObject(Object, Schema, Charset)}에 위임한다.
     * (레거시와 동일하게 UNION 타입은 패턴을 적용하지 않는 기본 변환 경로를 사용한다.)
     */
    @SuppressWarnings("unchecked")
    private static Object convertToAvroObject(final Object rawValue, final Schema fieldSchema, final String fieldName, final Charset charset,
                                              final String timestampPattern, final int addHours) {
        if (rawValue == null) {
            return null;
        }

        switch (fieldSchema.getType()) {
            case LONG: {
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType != null && LOGICAL_TYPE_TIMESTAMP_MILLIS.equals(logicalType.getName())) {
                    Long longFromTimestamp = getLongFromTimestamp(rawValue, fieldSchema, fieldName, timestampPattern);
                    long result = longFromTimestamp + (((addHours * 60 * 60) + 0) * 1000);
                    if (logger.isDebugEnabled()) {
                        logger.debug("[DFM] [TIMESTAMP_MILLIS] Raw Value = {}, Original Value = {}, Converted Value = {}, Timestamp Pattern = {}",
                                rawValue, longFromTimestamp, result, timestampPattern);
                    }
                    return result; // FIXED
                } else if (logicalType != null && LOGICAL_TYPE_TIMESTAMP_MICROS.equals(logicalType.getName())) {
                    long longFromTimestamp = getLongFromTimestamp(rawValue, fieldSchema, fieldName, timestampPattern) * 1000L;
                    long result = longFromTimestamp + (((addHours * 60 * 60) + 0) * 1000);
                    if (logger.isDebugEnabled()) {
                        logger.debug("[DFM] [TIMESTAMP_MICROS] Raw Value = {}, Original Value = {}, Converted Value = {}, Timestamp Pattern = {}",
                                rawValue, longFromTimestamp, result, timestampPattern);
                    }
                    return result; // FIXED
                }
                return org.apache.nifi.avro.AvroTypeUtil.convertToAvroObject(rawValue, fieldSchema, charset);
            }
            case MAP:
                if (rawValue instanceof Record) {
                    final Record recordValue = (Record) rawValue;
                    final Map<String, Object> map = new HashMap<>();
                    for (final RecordField recordField : recordValue.getSchema().getFields()) {
                        final Object v = recordValue.getValue(recordField);
                        if (v != null) {
                            map.put(recordField.getFieldName(),
                                    convertToAvroObject(v, fieldSchema.getValueType(), fieldName + "[" + recordField.getFieldName() + "]", charset, timestampPattern, addHours)); // FIXED
                        }
                    }

                    return map;
                } else if (rawValue instanceof Map) {
                    final Map<String, Object> objectMap = (Map<String, Object>) rawValue;
                    final Map<String, Object> map = new HashMap<>(objectMap.size());
                    for (final String s : objectMap.keySet()) {
                        final Object converted = convertToAvroObject(objectMap.get(s), fieldSchema.getValueType(), fieldName + "[" + s + "]", charset, timestampPattern, addHours); // FIXED
                        map.put(s, converted);
                    }
                    return map;
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a Map");
                }
            case RECORD:
                final GenericData.Record avroRecord = new GenericData.Record(fieldSchema);

                final Set<Map.Entry<String, Object>> entries;
                if (rawValue instanceof Map) {
                    final Map<String, Object> map = (Map<String, Object>) rawValue;
                    entries = map.entrySet();
                } else if (rawValue instanceof Record) {
                    entries = new HashSet<>();
                    final Record record = (Record) rawValue;
                    record.getSchema().getFields().forEach(field -> entries.add(new AbstractMap.SimpleEntry<>(field.getFieldName(), record.getValue(field))));
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a Record");
                }
                for (final Map.Entry<String, Object> e : entries) {
                    final Object recordFieldValue = e.getValue();
                    final String recordFieldName = e.getKey();

                    final Field field = fieldSchema.getField(recordFieldName);
                    if (field == null) {
                        continue;
                    }

                    final Object converted = convertToAvroObject(recordFieldValue, field.schema(), fieldName + "/" + recordFieldName, charset, timestampPattern, addHours); // FIXED
                    avroRecord.put(recordFieldName, converted);
                }
                return avroRecord;
            default:
                return org.apache.nifi.avro.AvroTypeUtil.convertToAvroObject(rawValue, fieldSchema, charset);
        }
    }

    /**
     * upstream의 lookupField()는 protected라 접근할 수 없어 로컬로 유지한다.
     */
    private static Pair<String, Field> lookupField(final Schema avroSchema, final RecordField recordField) {
        String fieldName = recordField.getFieldName();

        // 먼저 동일한 이름으로 1:1 매핑되는 필드가 있는지 그대로 찾아본다.
        Field field = avroSchema.getField(fieldName);
        if (field == null) {
            // 동일한 이름의 필드가 없다면, RecordField에 정의된 alias로 매핑 가능한지 확인한다.
            for (final String alias : recordField.getAliases()) {
                field = avroSchema.getField(alias);
                if (field != null) {
                    fieldName = alias;
                    break;
                }
            }
        }

        if (field == null) {
            for (final Field childField : avroSchema.getFields()) {
                final Set<String> aliases = childField.aliases();
                if (aliases.isEmpty()) {
                    continue;
                }

                if (aliases.contains(fieldName)) {
                    field = childField;
                    break;
                }

                for (final String alias : recordField.getAliases()) {
                    if (aliases.contains(alias)) {
                        field = childField;
                        fieldName = alias;
                        break;
                    }
                }
            }
        }

        return new ImmutablePair<>(fieldName, field);
    }
}
