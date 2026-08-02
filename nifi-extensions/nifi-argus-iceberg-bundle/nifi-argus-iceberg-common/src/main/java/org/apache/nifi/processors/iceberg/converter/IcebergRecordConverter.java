/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/converter/IcebergRecordConverter.java
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.iceberg.converter;

import org.apache.commons.lang.Validate;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.iceberg.UnmatchedColumnBehavior;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Iceberg 스키마와 NiFi 레코드 스키마를 함께 순회(traversal)하면서, 두 스키마 구조가
 * 일치하는 지점마다 알맞은 {@link DataConverter} 트리를 생성하는 클래스.
 * <p>
 * 생성자에서 {@link SchemaWithPartnerVisitor} 기반의 방문자 패턴을 이용해 Iceberg
 * {@link Schema}와 NiFi {@link RecordDataType}을 짝지어(partner) 재귀적으로 탐색하고,
 * 그 결과로 얻어진 최상위 레코드 변환기를 보관한다. 이후 {@link #convert(Record)}를
 * 호출하면 실제 NiFi 레코드 하나를 Iceberg의 {@link GenericRecord}로 변환해 준다.
 */
public class IcebergRecordConverter {

    // 스키마 순회를 통해 미리 구성해 둔 최상위(레코드) 변환기. 실제 변환은 이 객체에 위임한다.
    private final DataConverter<Record, GenericRecord> converter;
    // NiFi 레코드에는 있지만 Iceberg 스키마에는 없는(또는 그 반대) 컬럼을 만났을 때의 처리 방침
    public final UnmatchedColumnBehavior unmatchedColumnBehavior;
    public ComponentLog logger;

    // NiFi 레코드 한 건을 Iceberg GenericRecord로 변환한다.
    public GenericRecord convert(Record record) {
        return converter.convert(record);
    }


    @SuppressWarnings("unchecked")
    public IcebergRecordConverter(Schema schema, RecordSchema recordSchema, FileFormat fileFormat, UnmatchedColumnBehavior unmatchedColumnBehavior, ComponentLog logger) {
        this.converter = (DataConverter<Record, GenericRecord>) IcebergSchemaVisitor.visit(schema, new RecordDataType(recordSchema), fileFormat, unmatchedColumnBehavior, logger);
        this.unmatchedColumnBehavior = unmatchedColumnBehavior;
        this.logger = logger;
    }

    // Iceberg 스키마 트리를 순회하며 각 노드(스키마/필드/기본 타입/구조체/리스트/맵)에 대응하는
    // DataConverter 인스턴스를 생성하는 방문자(visitor) 구현체.
    private static class IcebergSchemaVisitor extends SchemaWithPartnerVisitor<DataType, DataConverter<?, ?>> {

        public static DataConverter<?, ?> visit(Schema schema, RecordDataType recordDataType, FileFormat fileFormat, UnmatchedColumnBehavior unmatchedColumnBehavior, ComponentLog logger) {
            return visit(schema, new RecordTypeWithFieldNameMapper(schema, recordDataType), new IcebergSchemaVisitor(),
                    new IcebergPartnerAccessors(schema, fileFormat, unmatchedColumnBehavior, logger));
        }

        @Override
        public DataConverter<?, ?> schema(Schema schema, DataType dataType, DataConverter<?, ?> converter) {
            return converter;
        }

        @Override
        public DataConverter<?, ?> field(Types.NestedField field, DataType dataType, DataConverter<?, ?> converter) {
            // 데이터 변환기에 Iceberg 스키마의 필드 이름(targetFieldName)을 설정한다
            converter.setTargetFieldName(field.name());
            return converter;
        }

        // Iceberg 기본(primitive) 타입을 대상 DataType과 조합해 알맞은 DataConverter로 매핑한다.
        // UUID/TIMESTAMP 등 일부 타입은 파일 포맷이나 타임존 여부에 따라 세부 변환기가 갈라진다.
        @Override
        public DataConverter<?, ?> primitive(Type.PrimitiveType type, DataType dataType) {
            if (type.typeId() != null) {
                switch (type.typeId()) {
                    case BOOLEAN:
                    case INTEGER:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case DATE:
                    case STRING:
                        return new GenericDataConverters.PrimitiveTypeConverter(type, dataType);
                    case TIME:
                        return new GenericDataConverters.TimeConverter(dataType.getFormat());
                    case TIMESTAMP:
                        final Types.TimestampType timestampType = (Types.TimestampType) type;
                        if (timestampType.shouldAdjustToUTC()) {
                            return new GenericDataConverters.TimestampWithTimezoneConverter(dataType);
                        }
                        return new GenericDataConverters.TimestampConverter(dataType);
                    case UUID:
                        final UUIDDataType uuidType = (UUIDDataType) dataType;
                        if (uuidType.getFileFormat() == FileFormat.PARQUET) {
                            return new GenericDataConverters.UUIDtoByteArrayConverter();
                        }
                        return new GenericDataConverters.PrimitiveTypeConverter(type, dataType);
                    case FIXED:
                        final Types.FixedType fixedType = (Types.FixedType) type;
                        return new GenericDataConverters.FixedConverter(fixedType.length());
                    case BINARY:
                        return new GenericDataConverters.BinaryConverter();
                    case DECIMAL:
                        final Types.DecimalType decimalType = (Types.DecimalType) type;
                        return new GenericDataConverters.BigDecimalConverter(decimalType.precision(), decimalType.scale());
                    default:
                        throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
                }
            }
            throw new UnsupportedOperationException("Missing type id from PrimitiveType " + type);
        }

        // 구조체(struct) 타입, 즉 중첩 레코드를 만났을 때 하위 필드 변환기들을 모아
        // 하나의 RecordConverter로 묶는다. 이때 각 하위 변환기에 실제 NiFi 필드 이름을
        // (대소문자 무관 매핑을 통해) 채워 넣어 값 조회에 사용할 수 있게 한다.
        @Override
        public DataConverter<?, ?> struct(Types.StructType type, DataType dataType, List<DataConverter<?, ?>> converters) {
            Validate.notNull(type, "Can not create reader for null type");
            final RecordTypeWithFieldNameMapper recordType = (RecordTypeWithFieldNameMapper) dataType;
            final RecordSchema recordSchema = recordType.getChildSchema();

            // 데이터 변환기에 NiFi 스키마의 필드 이름(sourceFieldName)을 설정한다
            for (DataConverter<?, ?> converter : converters) {
                final Optional<String> mappedFieldName = recordType.getNameMapping(converter.getTargetFieldName());
                if (mappedFieldName.isPresent()) {
                    final Optional<RecordField> recordField = recordSchema.getField(mappedFieldName.get());
                    converter.setSourceFieldName(recordField.get().getFieldName());
                }
            }

            return new GenericDataConverters.RecordConverter(converters, recordSchema, type);
        }

        // 리스트(배열) 타입: 원소 변환기를 감싸는 ArrayConverter를 생성한다.
        @Override
        public DataConverter<?, ?> list(Types.ListType listTypeInfo, DataType dataType, DataConverter<?, ?> converter) {
            return new GenericDataConverters.ArrayConverter<>(converter, ((ArrayDataType) dataType).getElementType());
        }

        // 맵 타입: 키/값 변환기를 감싸는 MapConverter를 생성한다. Iceberg 맵의 키는 항상 문자열로 취급한다.
        @Override
        public DataConverter<?, ?> map(Types.MapType mapType, DataType dataType, DataConverter<?, ?> keyConverter, DataConverter<?, ?> valueConverter) {
            return new GenericDataConverters.MapConverter<>(keyConverter, RecordFieldType.STRING.getDataType(), valueConverter, ((MapDataType) dataType).getValueType());
        }
    }

    // Iceberg 스키마 순회 중 "짝(partner)"이 되는 NiFi DataType을 찾아주는 접근자(accessor).
    // 필드/맵 값/리스트 원소 각각에 대해 Iceberg 노드에 대응하는 NiFi DataType을 반환하며,
    // 필드가 NiFi 레코드 스키마에 없을 경우 unmatchedColumnBehavior 설정에 따라
    // 예외를 던지거나 경고 로그를 남기고 Iceberg 스키마 기준 타입으로 대체한다.
    public static class IcebergPartnerAccessors implements SchemaWithPartnerVisitor.PartnerAccessors<DataType> {
        private final Schema schema;
        private final FileFormat fileFormat;
        private final UnmatchedColumnBehavior unmatchedColumnBehavior;
        private final ComponentLog logger;

        IcebergPartnerAccessors(Schema schema, FileFormat fileFormat, UnmatchedColumnBehavior unmatchedColumnBehavior, ComponentLog logger) {
            this.schema = schema;
            this.fileFormat = fileFormat;
            this.unmatchedColumnBehavior = unmatchedColumnBehavior;
            this.logger = logger;
        }

        // 이름(name)으로 NiFi 레코드 스키마에서 필드를 찾아 대응하는 DataType을 반환한다.
        // 대소문자 무관 매핑을 통해 이름이 일치하는 필드가 없으면 unmatchedColumnBehavior에 따라
        // 예외를 던지거나(FAIL) 경고 후 Iceberg 스키마 기준 타입을 대신 사용한다.
        @Override
        public DataType fieldPartner(DataType dataType, int fieldId, String name) {
            Validate.isTrue(dataType instanceof RecordTypeWithFieldNameMapper, String.format("Invalid record: %s is not a record", dataType));
            final RecordTypeWithFieldNameMapper recordType = (RecordTypeWithFieldNameMapper) dataType;

            final Optional<String> mappedFieldName = recordType.getNameMapping(name);
            if (UnmatchedColumnBehavior.FAIL_UNMATCHED_COLUMN.equals(unmatchedColumnBehavior)) {
                Validate.isTrue(mappedFieldName.isPresent(), String.format("Cannot find field with name '%s' in the record schema", name));
            }
            if (!mappedFieldName.isPresent()) {
                if (UnmatchedColumnBehavior.WARNING_UNMATCHED_COLUMN.equals(unmatchedColumnBehavior)) {
                    if (logger != null) {
                        logger.warn("Cannot find field with name '" + name + "' in the record schema, using the target schema for datatype and a null value");
                    }
                }
                // 필드가 없으면 Iceberg 스키마가 기대하는 타입을 NiFi DataType으로 변환해 사용한다
                final Types.NestedField schemaField = schema.findField(fieldId);
                final Type schemaFieldType = schemaField.type();
                if (schemaField.isRequired()) {
                    // Iceberg는 필수(required) 필드에 대해 null이 아닌 값을 요구한다
                    throw new IllegalArgumentException("Iceberg requires a non-null value for required fields, field: "
                            + schemaField.name() + ", type: " + schemaFieldType);
                }
                return GenericDataConverters.convertSchemaTypeToDataType(schemaFieldType);
            }
            final Optional<RecordField> recordField = recordType.getChildSchema().getField(mappedFieldName.get());
            final DataType fieldType = recordField.get().getDataType();

            // 실제 레코드가 중첩 레코드를 담고 있다면 RecordTypeWithFieldNameMapper로 감싸서 반환해야 한다
            if (fieldType instanceof RecordDataType) {
                return new RecordTypeWithFieldNameMapper(new Schema(schema.findField(fieldId).type().asStructType().fields()), (RecordDataType) fieldType);
            }

            // 필드가 배열이고 그 원소가 레코드라면, RecordTypeWithFieldNameMapper 생성에 쓸 Iceberg 스키마를 함께 넘긴다
            if (fieldType instanceof ArrayDataType && ((ArrayDataType) fieldType).getElementType() instanceof RecordDataType) {
                return new ArrayTypeWithIcebergSchema(
                        new Schema(schema.findField(fieldId).type().asListType().elementType().asStructType().fields()),
                        ((ArrayDataType) fieldType).getElementType()
                );
            }

            // 필드가 맵이고 그 값(value)이 레코드라면, RecordTypeWithFieldNameMapper 생성에 쓸 Iceberg 스키마를 함께 넘긴다
            if (fieldType instanceof MapDataType && ((MapDataType) fieldType).getValueType() instanceof RecordDataType) {
                return new MapTypeWithIcebergSchema(
                        new Schema(schema.findField(fieldId).type().asMapType().valueType().asStructType().fields()),
                        ((MapDataType) fieldType).getValueType()
                );
            }

            // 소스 필드 또는 대상 필드가 UUID 타입이면 UUIDDataType으로 감싸서 반환한다
            if (fieldType.getFieldType().equals(RecordFieldType.UUID) || schema.findField(fieldId).type().typeId() == Type.TypeID.UUID) {
                return new UUIDDataType(fieldType, fileFormat);
            }

            return fieldType;
        }

        // 맵의 키 타입은 항상 문자열로 취급한다 (Iceberg 맵 키는 String으로 고정)
        @Override
        public DataType mapKeyPartner(DataType dataType) {
            return RecordFieldType.STRING.getDataType();
        }

        // 맵의 값(value) 타입에 대응하는 NiFi DataType을 반환한다. 값이 레코드를 담는 맵이었다면
        // MapTypeWithIcebergSchema에 저장해 둔 Iceberg 스키마를 이용해 RecordTypeWithFieldNameMapper로 감싼다.
        @Override
        public DataType mapValuePartner(DataType dataType) {
            Validate.isTrue(dataType instanceof MapDataType, String.format("Invalid map: %s is not a map", dataType));
            final MapDataType mapType = (MapDataType) dataType;
            if (mapType instanceof MapTypeWithIcebergSchema) {
                MapTypeWithIcebergSchema typeWithSchema = (MapTypeWithIcebergSchema) mapType;
                return new RecordTypeWithFieldNameMapper(typeWithSchema.getValueSchema(), (RecordDataType) typeWithSchema.getValueType());
            }
            return mapType.getValueType();
        }

        // 리스트 원소 타입에 대응하는 NiFi DataType을 반환한다. 원소가 레코드를 담는 배열이었다면
        // ArrayTypeWithIcebergSchema에 저장해 둔 Iceberg 스키마를 이용해 RecordTypeWithFieldNameMapper로 감싼다.
        @Override
        public DataType listElementPartner(DataType dataType) {
            Validate.isTrue(dataType instanceof ArrayDataType, String.format("Invalid array: %s is not an array", dataType));
            final ArrayDataType arrayType = (ArrayDataType) dataType;
            if (arrayType instanceof ArrayTypeWithIcebergSchema) {
                ArrayTypeWithIcebergSchema typeWithSchema = (ArrayTypeWithIcebergSchema) arrayType;
                return new RecordTypeWithFieldNameMapper(typeWithSchema.getElementSchema(), (RecordDataType) typeWithSchema.getElementType());
            }
            return arrayType.getElementType();
        }
    }

    /**
     * Parquet 라이터는 UUID 값을 다른 형식으로 기대하기 때문에, 이를 구분하여 다르게 변환해야 한다: <a href="https://github.com/apache/iceberg/issues/1881">#1881</a>
     */
    private static class UUIDDataType extends DataType {

        private final FileFormat fileFormat;

        UUIDDataType(DataType dataType, FileFormat fileFormat) {
            super(dataType.getFieldType(), dataType.getFormat());
            this.fileFormat = fileFormat;
        }

        public FileFormat getFileFormat() {
            return fileFormat;
        }
    }

    /**
     * {@link RecordSchema}는 필드 이름과 값 쌍을 HashMap에 저장하기 때문에 조회 시 대소문자를
     * 구분한다. 이를 보완하기 위해 대소문자를 구분하지 않는 이름 매퍼를 만들어,
     * Iceberg 스키마의 필드 이름을 실제 NiFi 레코드 스키마의 필드 이름으로 매핑해 준다.
     */
    private static class RecordTypeWithFieldNameMapper extends RecordDataType {

        // Iceberg 필드 이름 -> NiFi 레코드 스키마의 실제(원본 대소문자) 필드 이름 매핑
        private final Map<String, String> fieldNameMap;

        RecordTypeWithFieldNameMapper(Schema schema, RecordDataType recordType) {
            super(recordType.getChildSchema());

            // NiFi 레코드 스키마 필드들에 대한 소문자 맵을 생성한다
            final Map<String, String> lowerCaseMap = recordType.getChildSchema().getFieldNames().stream()
                    .collect(Collectors.toMap(String::toLowerCase, s -> s));

            // Iceberg 레코드 스키마 필드를 NiFi 레코드 스키마 필드에 매핑한다
            this.fieldNameMap = new HashMap<>();
            schema.columns().forEach((s) -> this.fieldNameMap.put(s.name(), lowerCaseMap.get(s.name().toLowerCase())));
        }

        Optional<String> getNameMapping(String name) {
            return Optional.ofNullable(fieldNameMap.get(name));
        }
    }

    /**
     * 레코드를 원소로 담는 배열(Array)을 위한 데이터 타입. 원소 타입에 대응하는
     * Iceberg 스키마를 함께 보관하여, 중첩 레코드 변환 시 필요한 스키마 정보를 전달한다.
     */
    private static class ArrayTypeWithIcebergSchema extends ArrayDataType {

        private final Schema elementSchema;

        public ArrayTypeWithIcebergSchema(Schema elementSchema, DataType elementType) {
            super(elementType);
            this.elementSchema = elementSchema;
        }

        public Schema getElementSchema() {
            return elementSchema;
        }
    }

    /**
     * 값(value)으로 레코드를 담는 맵(Map)을 위한 데이터 타입. 값 타입에 대응하는
     * Iceberg 스키마를 함께 보관하여, 중첩 레코드 변환 시 필요한 스키마 정보를 전달한다.
     */
    private static class MapTypeWithIcebergSchema extends MapDataType {

        private final Schema valueSchema;

        public MapTypeWithIcebergSchema(Schema valueSchema, DataType valueType) {
            super(valueType);
            this.valueSchema = valueSchema;
        }

        public Schema getValueSchema() {
            return valueSchema;
        }
    }

}
