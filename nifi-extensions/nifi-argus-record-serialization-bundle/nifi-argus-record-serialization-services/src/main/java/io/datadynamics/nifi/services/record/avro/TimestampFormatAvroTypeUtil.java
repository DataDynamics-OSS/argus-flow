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
package io.datadynamics.nifi.services.record.avro;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * NiFi Record를 Avro GenericRecord로 변환할 때 Timestamp 형식(format)을 인식하여 처리하는 유틸리티 클래스.
 *
 * <p>이 클래스는 기존 {@code io.datadynamics.nifi.record.parquet.AvroTypeUtil} 커스텀 포크를 NiFi 2.x로
 * 이식한 것이다. 업스트림 {@link AvroTypeUtil} 전체를 포크하는 대신, 커스텀 변환 로직만 이 클래스에 남기고
 * 나머지는 업스트림 2.10.0 구현에 위임한다.</p>
 *
 * <p>커스텀 동작 방식: Avro logical type이 {@code timestamp-millis} 또는 {@code timestamp-micros}인
 * 필드의 경우, 타임스탬프 파싱 패턴을 스키마 레벨의 Property Map(기본 Property 이름: {@code properties})에서
 * {@code column.all.pattern} 또는 {@code column.<fieldName>.pattern} 키를 사용해 지정할 수 있으며,
 * 변환된 타임스탬프 값에 고정된 시간(hour) 오프셋을 더할 수 있다.</p>
 */
public final class TimestampFormatAvroTypeUtil {

    // 스키마 Property에 타임스탬프 패턴이 지정되지 않았을 때 사용할 기본 타임스탬프 형식
    static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final Logger logger = LoggerFactory.getLogger(TimestampFormatAvroTypeUtil.class);

    private static final String LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String LOGICAL_TYPE_TIMESTAMP_MICROS = "timestamp-micros";

    // 유틸리티 클래스이므로 인스턴스화를 막는다.
    private TimestampFormatAvroTypeUtil() {
    }

    /**
     * 기본 문자셋(UTF-8)을 사용하여 NiFi Record를 Avro GenericRecord로 변환한다.
     */
    public static GenericRecord createAvroRecord(final Record record, final Schema avroSchema, final String timestampFormatPropertyKeyName, final int addHours) {
        return createAvroRecord(record, avroSchema, StandardCharsets.UTF_8, timestampFormatPropertyKeyName, addHours);
    }

    /**
     * NiFi Record를 Avro GenericRecord로 변환한다. Record의 각 필드 값을 대상 Avro 스키마의 필드에
     * 매핑하며, Timestamp 관련 logical type 필드는 지정된 패턴과 시간 오프셋을 적용하여 변환한다.
     * Record에 없는 필드 중 Avro 스키마에 기본값이 정의된 필드는 그 기본값으로 채운다.
     */
    public static GenericRecord createAvroRecord(final Record record, final Schema avroSchema, final Charset charset, final String timestampFormatPropertyKeyName, final int addHours) {
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

            Field field = avroSchema.getField(rawFieldName);
            if (field == null) {
                field = lookupField(avroSchema, recordField);
                if (field == null) {
                    continue;
                }
            }

            final String fieldName = field.name();
            final String timestampPattern = getTimestampPattern(avroSchema, timestampFormatPropertyKeyName, fieldName);
            final Object converted = convertToAvroObject(rawValue, field.schema(), fieldName, charset, timestampPattern, addHours);
            rec.put(fieldName, converted);
        }

        // RecordSchema에는 없지만 Avro 스키마에는 존재하는 필드가 있는지 확인하고, 그런 필드에
        // 기본값이 정의되어 있다면 생성 중인 GenericRecord에 그 기본값을 채워 넣는다.
        for (final Field field : avroSchema.getFields()) {
            final Object defaultValue = field.defaultVal();
            if (defaultValue == null || defaultValue == JsonProperties.NULL_VALUE) {
                continue;
            }

            if (rec.get(field.name()) == null) {
                final String timestampPattern = getTimestampPattern(avroSchema, timestampFormatPropertyKeyName, field.name());
                final Object normalized = convertToAvroObject(defaultValue, field.schema(), field.name(), StandardCharsets.UTF_8, timestampPattern, addHours);
                rec.put(field.name(), normalized);
            }
        }

        return rec;
    }

    /**
     * 스키마 레벨의 Property Map에서 지정된 필드에 사용할 타임스탬프 패턴을 조회한다.
     * {@code column.all.pattern}이 있으면 모든 컬럼에 우선 적용되고, 없으면
     * {@code column.<fieldName>.pattern}을 찾으며, 둘 다 없으면 기본 형식을 반환한다.
     */
    public static String getTimestampPattern(final Schema avroSchema, final String timestampFormatPropertyKeyName, final String fieldName) {
        if (timestampFormatPropertyKeyName == null || timestampFormatPropertyKeyName.trim().isEmpty()) {
            return DEFAULT_TIMESTAMP_FORMAT;
        }

        final Object prop = avroSchema.getObjectProp(timestampFormatPropertyKeyName.trim());
        if (!(prop instanceof Map)) {
            return DEFAULT_TIMESTAMP_FORMAT;
        }

        final Map<?, ?> props = (Map<?, ?>) prop;
        if (props.containsKey("column.all.pattern")) {
            return (String) props.get("column.all.pattern");
        }

        final String columnKey = String.format("column.%s.pattern", fieldName);
        if (props.containsKey(columnKey)) {
            return (String) props.get(columnKey);
        }

        return DEFAULT_TIMESTAMP_FORMAT;
    }

    /**
     * 원본 값을 지정된 타임스탬프 형식(패턴이 없으면 필드 스키마로부터 유추한 형식)으로 파싱하여
     * epoch 밀리초(Long) 값으로 변환한다.
     */
    private static Long getLongFromTimestamp(final Object rawValue, final Schema fieldSchema, final String fieldName, final String timestampFormat) {
        final String format;
        if (timestampFormat == null || timestampFormat.trim().isEmpty()) {
            format = AvroTypeUtil.determineDataType(fieldSchema).getFormat();
        } else {
            format = timestampFormat.trim();
        }

        final FieldConverter<Object, Timestamp> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(Timestamp.class);
        final Timestamp timestamp = converter.convertField(rawValue, Optional.ofNullable(format), fieldName);
        return timestamp.getTime();
    }

    /**
     * 원본 값을 대상 Avro 필드 스키마 타입에 맞게 재귀적으로 변환한다.
     * LONG 타입이면서 logical type이 timestamp-millis/timestamp-micros인 경우 커스텀 타임스탬프
     * 패턴과 시간 오프셋을 적용하여 변환하고, MAP과 RECORD 타입은 각 요소를 재귀적으로 변환한다.
     * 그 외 타입(UNION, ARRAY, INT, STRING, BYTES 등)은 업스트림 표준 변환을 그대로 사용한다.
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
                    final long longFromTimestamp = getLongFromTimestamp(rawValue, fieldSchema, fieldName, timestampPattern);
                    final long result = longFromTimestamp + (((addHours * 60 * 60) + 0) * 1000);
                    if (logger.isDebugEnabled()) {
                        logger.debug("[DFM] [TIMESTAMP_MILLIS] Raw Value = {}, Original Value = {}, Converted Value = {}, Timestamp Pattern = {}",
                                rawValue, longFromTimestamp, result, timestampPattern);
                    }
                    return result;
                } else if (logicalType != null && LOGICAL_TYPE_TIMESTAMP_MICROS.equals(logicalType.getName())) {
                    final long longFromTimestamp = getLongFromTimestamp(rawValue, fieldSchema, fieldName, timestampPattern) * 1000L;
                    final long result = longFromTimestamp + (((addHours * 60 * 60) + 0) * 1000);
                    if (logger.isDebugEnabled()) {
                        logger.debug("[DFM] [TIMESTAMP_MICROS] Raw Value = {}, Original Value = {}, Converted Value = {}, Timestamp Pattern = {}",
                                rawValue, longFromTimestamp, result, timestampPattern);
                    }
                    return result;
                }

                return AvroTypeUtil.convertToAvroObject(rawValue, fieldSchema, charset);
            }
            case MAP:
                if (rawValue instanceof Record) {
                    final Record recordValue = (Record) rawValue;
                    final Map<String, Object> map = new HashMap<>();
                    for (final RecordField recordField : recordValue.getSchema().getFields()) {
                        final Object v = recordValue.getValue(recordField);
                        if (v != null) {
                            map.put(recordField.getFieldName(),
                                    convertToAvroObject(v, fieldSchema.getValueType(), fieldName + "[" + recordField.getFieldName() + "]", charset, timestampPattern, addHours));
                        }
                    }

                    return map;
                } else if (rawValue instanceof Map) {
                    final Map<String, Object> objectMap = (Map<String, Object>) rawValue;
                    final Map<String, Object> map = new HashMap<>(objectMap.size());
                    for (final String s : objectMap.keySet()) {
                        final Object converted = convertToAvroObject(objectMap.get(s), fieldSchema.getValueType(), fieldName + "[" + s + "]", charset, timestampPattern, addHours);
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

                    final Object converted = convertToAvroObject(recordFieldValue, field.schema(), fieldName + "/" + recordFieldName, charset, timestampPattern, addHours);
                    avroRecord.put(recordFieldName, converted);
                }
                return avroRecord;
            default:
                // 그 외 모든 타입(UNION, ARRAY, INT, STRING, BYTES 등)은 업스트림 표준 변환 방식을 따른다.
                // 참고: 이는 기존 레거시 포크의 동작과 동일하며, UNION(예: nullable timestamp)과 ARRAY 요소
                // 변환에는 커스텀 타임스탬프 패턴이나 시간 오프셋이 적용되지 않는다.
                return AvroTypeUtil.convertToAvroObject(rawValue, fieldSchema, charset);
        }
    }

    /**
     * 업스트림의 (패키지 외부에서 접근 불가능한) 필드 조회 로직을 복사한 것으로, Record 필드를
     * 이름으로 먼저 매핑을 시도하고, 실패하면 양방향으로 alias를 사용하여 Avro 스키마 필드와 매핑을 시도한다.
     */
    private static Field lookupField(final Schema avroSchema, final RecordField recordField) {
        String fieldName = recordField.getFieldName();

        Field field = avroSchema.getField(fieldName);
        if (field == null) {
            for (final String alias : recordField.getAliases()) {
                field = avroSchema.getField(alias);
                if (field != null) {
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

                boolean matched = false;
                for (final String alias : recordField.getAliases()) {
                    if (aliases.contains(alias)) {
                        field = childField;
                        matched = true;
                        break;
                    }
                }
                if (matched) {
                    break;
                }
            }
        }

        return field;
    }
}
