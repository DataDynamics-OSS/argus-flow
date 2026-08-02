/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/converter/GenericDataConverters.java
 */
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
package org.apache.nifi.processors.iceberg.converter;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.nifi.processors.iceberg.converter.RecordFieldGetter.createFieldGetter;

/**
 * 각기 다른 데이터 타입들을 위한 {@link DataConverter} 구현체들을 모아 둔 클래스.
 * <p>
 * {@link IcebergRecordConverter}가 스키마를 순회하며 필요한 변환기를 선택해 조립하는데,
 * 여기 정의된 각 내부 클래스는 기본 타입(불리언/숫자/문자열 등), 시간/타임스탬프,
 * UUID, 고정 길이·가변 길이 바이너리, 소수(decimal), 그리고 배열·맵·레코드 같은
 * 컬렉션/복합 타입을 NiFi 레코드 값 <-> Iceberg 값 사이에서 변환하는 역할을 각각 담당한다.
 */
public class GenericDataConverters {

    // NiFi 레코드의 기본(primitive) 필드 값을 Iceberg가 기대하는 기본 타입 값으로 변환한다.
    // BOOLEAN/INTEGER/LONG/FLOAT/DOUBLE/DATE/UUID/STRING 등 대부분의 스칼라 타입을 처리하며,
    // 그 외 타입은 문자열로 변환한다.
    static class PrimitiveTypeConverter extends DataConverter<Object, Object> {
        final Type.PrimitiveType targetType;
        final DataType sourceType;

        public PrimitiveTypeConverter(final Type.PrimitiveType type, final DataType dataType) {
            targetType = type;
            sourceType = dataType;
        }

        @Override
        public Object convert(Object data) {
            switch (targetType.typeId()) {
                case BOOLEAN:
                    return DataTypeUtils.toBoolean(data, null);
                case INTEGER:
                    return DataTypeUtils.toInteger(data, null);
                case LONG:
                    return DataTypeUtils.toLong(data, null);
                case FLOAT:
                    return DataTypeUtils.toFloat(data, null);
                case DOUBLE:
                    return DataTypeUtils.toDouble(data, null);
                case DATE:
                    return StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDate.class)
                            .convertField(data, Optional.ofNullable(sourceType.getFormat()), null);
                case UUID:
                    return DataTypeUtils.toUUID(data);
                case STRING:
                default:
                    return DataTypeUtils.toString(data, (String) null);
            }
        }
    }

    // NiFi 필드 값을 Iceberg의 TIME 타입에 대응하는 java.time.LocalTime으로 변환한다.
    static class TimeConverter extends DataConverter<Object, LocalTime> {

        private final String timeFormat;

        public TimeConverter(final String format) {
            this.timeFormat = format;
        }

        @Override
        public LocalTime convert(Object data) {
            return StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalTime.class)
                    .convertField(data, Optional.ofNullable(timeFormat), null);
        }
    }

    // 타임존 정보가 없는(local) Iceberg TIMESTAMP 타입을 위한 변환기. NiFi 필드 값을
    // java.time.LocalDateTime으로 변환한다.
    static class TimestampConverter extends DataConverter<Object, LocalDateTime> {

        private final DataType dataType;

        public TimestampConverter(final DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public LocalDateTime convert(Object data) {
            return StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDateTime.class)
                    .convertField(data, Optional.ofNullable(dataType.getFormat()), null);
        }
    }

    // 타임존을 UTC로 조정해야 하는(shouldAdjustToUTC) Iceberg TIMESTAMP 타입을 위한 변환기.
    // NiFi 필드 값을 OffsetDateTime으로 변환한 뒤, 동일 시점(instant)을 유지한 채 UTC 오프셋으로 맞춘다.
    static class TimestampWithTimezoneConverter extends DataConverter<Object, OffsetDateTime> {

        private final DataType dataType;

        public TimestampWithTimezoneConverter(final DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public OffsetDateTime convert(Object data) {
            final OffsetDateTime converted = StandardFieldConverterRegistry.getRegistry().getFieldConverter(OffsetDateTime.class)
                    .convertField(data, Optional.ofNullable(dataType.getFormat()), null);
            return converted == null ? null : converted.withOffsetSameInstant(ZoneOffset.UTC);
        }
    }

    // UUID 값을 16바이트 배열로 변환한다. Parquet 라이터가 UUID를 이 바이너리 형식으로
    // 기대하기 때문에 필요한 변환기이며(most/least significant bits를 빅엔디안으로 기록),
    // Parquet 이외의 포맷에서는 PrimitiveTypeConverter가 대신 사용된다.
    static class UUIDtoByteArrayConverter extends DataConverter<Object, byte[]> {

        @Override
        public byte[] convert(Object data) {
            if (data == null) {
                return null;
            }
            final UUID uuid = DataTypeUtils.toUUID(data);
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
            byteBuffer.putLong(uuid.getMostSignificantBits());
            byteBuffer.putLong(uuid.getLeastSignificantBits());
            return byteBuffer.array();
        }
    }

    // Iceberg의 고정 길이(FIXED) 바이너리 타입을 위한 변환기. 입력 바이트 배열의 길이가
    // 스키마에 정의된 고정 길이와 정확히 일치하는지 검증한 뒤 원시(primitive) byte 배열로 변환한다.
    static class FixedConverter extends DataConverter<Byte[], byte[]> {

        private final int length;

        FixedConverter(int length) {
            this.length = length;
        }

        @Override
        public byte[] convert(Byte[] data) {
            if (data == null) {
                return null;
            }
            Validate.isTrue(data.length == length, String.format("Cannot write byte array of length %s as fixed[%s]", data.length, length));
            return ArrayUtils.toPrimitive(data);
        }
    }

    // Iceberg의 가변 길이 BINARY 타입을 위한 변환기. Byte[] 값을 ByteBuffer로 감싼다.
    static class BinaryConverter extends DataConverter<Byte[], ByteBuffer> {

        @Override
        public ByteBuffer convert(Byte[] data) {
            if (data == null) {
                return null;
            }
            return ByteBuffer.wrap(ArrayUtils.toPrimitive(data));
        }
    }

    // Iceberg의 DECIMAL(precision, scale) 타입을 위한 변환기. 값의 scale이 대상보다 작으면
    // 0으로 채워 맞추고, 최종적으로 scale과 precision이 스키마 제약을 만족하는지 검증한다.
    static class BigDecimalConverter extends DataConverter<Object, BigDecimal> {
        private final int precision;
        private final int scale;

        BigDecimalConverter(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public BigDecimal convert(Object data) {
            if (data == null) {
                return null;
            }

            BigDecimal bigDecimal = DataTypeUtils.toBigDecimal(data, null);

            if (bigDecimal.scale() < scale) {
                bigDecimal = bigDecimal.setScale(scale);
            }

            Validate.isTrue(bigDecimal.scale() == scale, "Cannot write value as decimal(%s,%s), wrong scale %s for value: %s", precision, scale, bigDecimal.scale(), data);
            Validate.isTrue(bigDecimal.precision() <= precision, "Cannot write value as decimal(%s,%s), invalid precision %s for value: %s",
                    precision, scale, bigDecimal.precision(), data);
            return bigDecimal;
        }
    }

    // Iceberg LIST 타입을 위한 변환기. 배열의 각 원소에 대해 하위 fieldConverter를 적용해
    // 새로운 List로 변환한다. elementGetter는 원소 값을 배열에서 안전하게(null 허용) 꺼내는 역할을 한다.
    static class ArrayConverter<S, T> extends DataConverter<S[], List<T>> {
        private final DataConverter<S, T> fieldConverter;
        private final ArrayElementGetter.ElementGetter elementGetter;

        ArrayConverter(DataConverter<S, T> elementConverter, DataType dataType) {
            this.fieldConverter = elementConverter;
            this.elementGetter = ArrayElementGetter.createElementGetter(dataType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<T> convert(S[] data) {
            if (data == null) {
                return null;
            }
            final int numElements = data.length;
            final List<T> result = new ArrayList<>(numElements);
            for (int i = 0; i < numElements; i += 1) {
                result.add(i, fieldConverter.convert((S) elementGetter.getElementOrNull(data[i])));
            }
            return result;
        }
    }

    // Iceberg MAP 타입을 위한 변환기. 키와 값 각각에 대해 별도의 하위 변환기(keyConverter/valueConverter)를
    // 적용하여 새로운 Map을 구성한다. keyGetter/valueGetter는 원본 컬렉션에서 항목을 안전하게 꺼낸다.
    static class MapConverter<SK, SV, TK, TV> extends DataConverter<Map<SK, SV>, Map<TK, TV>> {
        private final DataConverter<SK, TK> keyConverter;
        private final DataConverter<SV, TV> valueConverter;
        private final ArrayElementGetter.ElementGetter keyGetter;
        private final ArrayElementGetter.ElementGetter valueGetter;

        MapConverter(DataConverter<SK, TK> keyConverter, DataType keyType, DataConverter<SV, TV> valueConverter, DataType valueType) {
            this.keyConverter = keyConverter;
            this.keyGetter = ArrayElementGetter.createElementGetter(keyType);
            this.valueConverter = valueConverter;
            this.valueGetter = ArrayElementGetter.createElementGetter(valueType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Map<TK, TV> convert(Map<SK, SV> data) {
            if (data == null) {
                return null;
            }
            final int mapSize = data.size();
            final Object[] keyArray = data.keySet().toArray();
            final Object[] valueArray = data.values().toArray();
            final Map<TK, TV> result = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i += 1) {
                result.put(keyConverter.convert((SK) keyGetter.getElementOrNull(keyArray[i])), valueConverter.convert((SV) valueGetter.getElementOrNull(valueArray[i])));
            }

            return result;
        }
    }

    // Iceberg 구조체(STRUCT)/레코드 타입을 위한 최상위 변환기. 하위 필드별 변환기 목록(converters)과
    // 각 필드 값을 NiFi 레코드에서 꺼내는 FieldGetter들을 보관했다가, convert() 호출 시
    // 필드마다 값을 조회하여 변환한 뒤 새로운 Iceberg GenericRecord를 조립한다.
    static class RecordConverter extends DataConverter<Record, GenericRecord> {

        private final List<DataConverter<?, ?>> converters;
        private final Map<String, RecordFieldGetter.FieldGetter> getters;

        private final Types.StructType schema;

        RecordConverter(List<DataConverter<?, ?>> converters, RecordSchema recordSchema, Types.StructType schema) {
            this.schema = schema;
            this.converters = converters;
            this.getters = new HashMap<>(converters.size());

            for (DataConverter<?, ?> converter : converters) {
                final Optional<RecordField> recordField = recordSchema.getField(converter.getSourceFieldName());
                if (!recordField.isPresent()) {
                    final Types.NestedField missingField = schema.field(converter.getTargetFieldName());
                    if (missingField != null) {
                        getters.put(converter.getTargetFieldName(), createFieldGetter(convertSchemaTypeToDataType(missingField.type()), missingField.name(), missingField.isOptional()));
                    }
                } else {
                    final RecordField field = recordField.get();
                    // 변환기마다 레코드 필드 접근자(accessor)를 하나씩 생성한다
                    getters.put(converter.getTargetFieldName(), createFieldGetter(field.getDataType(), field.getFieldName(), field.isNullable()));
                }
            }
        }

        @Override
        public GenericRecord convert(Record data) {
            if (data == null) {
                return null;
            }
            final GenericRecord record = GenericRecord.create(schema);

            for (DataConverter<?, ?> converter : converters) {
                record.setField(converter.getTargetFieldName(), convert(data, converter));
            }

            return record;
        }

        @SuppressWarnings("unchecked")
        private <S, T> T convert(Record record, DataConverter<S, T> converter) {
            return converter.convert((S) getters.get(converter.getTargetFieldName()).getFieldOrNull(record));
        }
    }

    // Iceberg 스키마 타입(Type)을 그에 대응하는 NiFi 레코드 DataType으로 변환한다.
    // 스칼라 타입은 1:1로 매핑하고, FIXED/BINARY는 바이트 배열(ARRAY of BYTE)로,
    // STRUCT/LIST/MAP 같은 복합 타입은 내부 필드/원소 타입을 재귀적으로 변환하여 조립한다.
    // 이 메서드는 NiFi 레코드 스키마에 존재하지 않는 필드에 대해 Iceberg 스키마 기준으로
    // 기본 DataType을 추론해야 할 때 사용된다.
    public static DataType convertSchemaTypeToDataType(Type schemaType) {
        switch (schemaType.typeId()) {
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case INTEGER:
                return RecordFieldType.INT.getDataType();
            case LONG:
                return RecordFieldType.LONG.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case DATE:
                return RecordFieldType.DATE.getDataType();
            case TIME:
                return RecordFieldType.TIME.getDataType();
            case TIMESTAMP:
                return RecordFieldType.TIMESTAMP.getDataType();
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case UUID:
                return RecordFieldType.UUID.getDataType();
            case FIXED:
            case BINARY:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case DECIMAL:
                return RecordFieldType.DECIMAL.getDataType();
            case STRUCT:
                // 구조체 타입으로부터 레코드(RECORD) 타입을 구성한다
                Types.StructType structType = schemaType.asStructType();
                List<Types.NestedField> fields = structType.fields();
                List<RecordField> recordFields = new ArrayList<>(fields.size());
                for (Types.NestedField field : fields) {
                    DataType dataType = convertSchemaTypeToDataType(field.type());
                    recordFields.add(new RecordField(field.name(), dataType, field.isOptional()));
                }
                RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            case LIST:
                // 원소 타입으로부터 리스트(ARRAY) 타입을 구성한다
                Types.ListType listType = schemaType.asListType();
                return RecordFieldType.ARRAY.getArrayDataType(convertSchemaTypeToDataType(listType.elementType()), listType.isElementOptional());
            case MAP:
                // 값 타입으로부터 맵(MAP) 타입을 구성한다
                Types.MapType mapType = schemaType.asMapType();
                return RecordFieldType.MAP.getMapDataType(convertSchemaTypeToDataType(mapType.valueType()), mapType.isValueOptional());
        }
        throw new IllegalArgumentException("Invalid or unsupported type: " + schemaType);
    }
}
