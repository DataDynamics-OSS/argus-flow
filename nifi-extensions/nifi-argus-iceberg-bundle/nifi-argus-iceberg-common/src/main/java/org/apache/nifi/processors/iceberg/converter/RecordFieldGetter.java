/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/converter/RecordFieldGetter.java
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

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import jakarta.annotation.Nullable;
import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Optional;

/**
 * NiFi 레코드의 특정 필드 값을 Iceberg가 요구하는 타입으로 변환해 주는 접근자(getter)를
 * 생성하는 유틸리티 클래스. 필드 타입별로 적절한 변환 로직을 담은 {@link FieldGetter}를 만들어 낸다.
 */
public class RecordFieldGetter {

    /**
     * 내부 레코드 데이터 구조에서 주어진 필드 이름에 해당하는 값을 가져오기 위한 접근자를 생성한다.
     * 필드의 타입에 따라 적절한 변환 로직을 가진 {@link FieldGetter}를 반환하며,
     * isNullable이 true인 경우 필드 값이 null일 때 변환을 시도하지 않고 null을 그대로 반환하도록 감싸서 반환한다.
     *
     * @param dataType   필드의 데이터 타입
     * @param fieldName  필드 이름
     * @param isNullable 필드 값이 null을 허용하는지 여부
     */
    public static FieldGetter createFieldGetter(DataType dataType, String fieldName, boolean isNullable) {
        FieldGetter fieldGetter;
        switch (dataType.getFieldType()) {
            case STRING:
                fieldGetter = record -> record.getAsString(fieldName);
                break;
            case CHAR:
                fieldGetter = record -> DataTypeUtils.toCharacter(record.getValue(fieldName), fieldName);
                break;
            case BOOLEAN:
                fieldGetter = record -> record.getAsBoolean(fieldName);
                break;
            case DECIMAL:
                fieldGetter = record -> DataTypeUtils.toBigDecimal(record.getValue(fieldName), fieldName);
                break;
            case BYTE:
                fieldGetter = record -> DataTypeUtils.toByte(record.getValue(fieldName), fieldName);
                break;
            case SHORT:
                fieldGetter = record -> DataTypeUtils.toShort(record.getValue(fieldName), fieldName);
                break;
            case INT:
                fieldGetter = record -> record.getAsInt(fieldName);
                break;
            case DATE:
                fieldGetter = record -> StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDate.class)
                        .convertField(record.getValue(fieldName), Optional.ofNullable(dataType.getFormat()), fieldName);
                break;
            case TIME:
                fieldGetter = record -> StandardFieldConverterRegistry.getRegistry().getFieldConverter(Time.class)
                        .convertField(record.getValue(fieldName), Optional.ofNullable(dataType.getFormat()), fieldName);
                break;
            case LONG:
                fieldGetter = record -> record.getAsLong(fieldName);
                break;
            case BIGINT:
                fieldGetter = record -> DataTypeUtils.toBigInt(record.getValue(fieldName), fieldName);
                break;
            case FLOAT:
                fieldGetter = record -> record.getAsFloat(fieldName);
                break;
            case DOUBLE:
                fieldGetter = record -> record.getAsDouble(fieldName);
                break;
            case TIMESTAMP:
                fieldGetter = record -> StandardFieldConverterRegistry.getRegistry().getFieldConverter(Timestamp.class)
                        .convertField(record.getValue(fieldName), Optional.ofNullable(dataType.getFormat()), fieldName);
                break;
            case UUID:
                fieldGetter = record -> DataTypeUtils.toUUID(record.getValue(fieldName));
                break;
            case ENUM:
                fieldGetter = record -> DataTypeUtils.toEnum(record.getValue(fieldName), (EnumDataType) dataType, fieldName);
                break;
            case ARRAY:
                fieldGetter = record -> DataTypeUtils.toArray(record.getValue(fieldName), fieldName, ((ArrayDataType) dataType).getElementType());
                break;
            case MAP:
                fieldGetter = record -> DataTypeUtils.toMap(record.getValue(fieldName), fieldName);
                break;
            case RECORD:
                fieldGetter = record -> record.getAsRecord(fieldName, ((RecordDataType) dataType).getChildSchema());
                break;
            case CHOICE:
                fieldGetter = record -> {
                    final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                    final Object value = record.getValue(fieldName);
                    final DataType chosenDataType = DataTypeUtils.chooseDataType(value, choiceDataType);
                    if (chosenDataType == null) {
                        throw new IllegalTypeConversionException(String.format(
                                "Cannot convert value [%s] of type %s for field %s to any of the following available Sub-Types for a Choice: %s",
                                value, value.getClass(), fieldName, choiceDataType.getPossibleSubTypes()));
                    }

                    return DataTypeUtils.convertType(record.getValue(fieldName), chosenDataType, fieldName);
                };
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type: " + dataType.getFieldType());
        }

        if (!isNullable) {
            return fieldGetter;
        }

        return record -> {
            if (record.getValue(fieldName) == null) {
                return null;
            }

            return fieldGetter.getFieldOrNull(record);
        };
    }

    /**
     * 런타임에 레코드의 필드를 가져오기 위한 접근자 인터페이스.
     */

    public interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(Record record);
    }
}
