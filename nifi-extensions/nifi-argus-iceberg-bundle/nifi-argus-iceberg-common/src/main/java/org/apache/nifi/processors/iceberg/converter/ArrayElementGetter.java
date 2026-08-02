/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-iceberg-bundle/nifi-iceberg-common/src/main/java/org/apache/nifi/processors/iceberg/converter/ArrayElementGetter.java
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
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
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
 * NiFi 레코드의 배열(ARRAY) 필드 값을 Iceberg가 요구하는 타입으로 변환해 주는 접근자(getter)를
 * 생성하는 유틸리티 클래스. 배열의 원소 타입별로 적절한 변환 로직을 담은 {@link ElementGetter}를 만들어 낸다.
 */
public class ArrayElementGetter {

    // DataTypeUtils 변환 실패 시 오류 메시지에 표기되는 필드 이름(디버깅 목적의 고정 문자열)
    private static final String ARRAY_FIELD_NAME = "array element";

    /**
     * 내부 배열 데이터 구조에서 주어진 위치의 원소를 가져오기 위한 접근자를 생성한다.
     * 배열 원소의 타입에 따라 적절한 변환 로직을 가진 {@link ElementGetter}를 반환하며,
     * 원소 값이 null인 경우에는 변환을 시도하지 않고 null을 그대로 반환하도록 감싸서 반환한다.
     *
     * @param dataType 배열 원소의 데이터 타입
     */
    public static ElementGetter createElementGetter(DataType dataType) {
        ElementGetter elementGetter;
        switch (dataType.getFieldType()) {
            case STRING:
                elementGetter = element -> DataTypeUtils.toString(element, ARRAY_FIELD_NAME);
                break;
            case CHAR:
                elementGetter = element -> DataTypeUtils.toCharacter(element, ARRAY_FIELD_NAME);
                break;
            case BOOLEAN:
                elementGetter = element -> DataTypeUtils.toBoolean(element, ARRAY_FIELD_NAME);
                break;
            case DECIMAL:
                elementGetter = element -> DataTypeUtils.toBigDecimal(element, ARRAY_FIELD_NAME);
                break;
            case BYTE:
                elementGetter = element -> DataTypeUtils.toByte(element, ARRAY_FIELD_NAME);
                break;
            case SHORT:
                elementGetter = element -> DataTypeUtils.toShort(element, ARRAY_FIELD_NAME);
                break;
            case INT:
                elementGetter = element -> DataTypeUtils.toInteger(element, ARRAY_FIELD_NAME);
                break;
            case DATE:
                elementGetter = element -> StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDate.class)
                        .convertField(element, Optional.ofNullable(dataType.getFormat()), ARRAY_FIELD_NAME);
                break;
            case TIME:
                elementGetter = element -> StandardFieldConverterRegistry.getRegistry().getFieldConverter(Time.class)
                        .convertField(element, Optional.ofNullable(dataType.getFormat()), ARRAY_FIELD_NAME);
                break;
            case LONG:
                elementGetter = element -> DataTypeUtils.toLong(element, ARRAY_FIELD_NAME);
                break;
            case BIGINT:
                elementGetter = element -> DataTypeUtils.toBigInt(element, ARRAY_FIELD_NAME);
                break;
            case FLOAT:
                elementGetter = element -> DataTypeUtils.toFloat(element, ARRAY_FIELD_NAME);
                break;
            case DOUBLE:
                elementGetter = element -> DataTypeUtils.toDouble(element, ARRAY_FIELD_NAME);
                break;
            case TIMESTAMP:
                elementGetter = element -> StandardFieldConverterRegistry.getRegistry().getFieldConverter(Timestamp.class)
                        .convertField(element, Optional.ofNullable(dataType.getFormat()), ARRAY_FIELD_NAME);
                break;
            case ENUM:
                elementGetter = element -> DataTypeUtils.toEnum(element, (EnumDataType) dataType, ARRAY_FIELD_NAME);
                break;
            case UUID:
                elementGetter = DataTypeUtils::toUUID;
                break;
            case ARRAY:
                elementGetter = element -> DataTypeUtils.toArray(element, ARRAY_FIELD_NAME, ((ArrayDataType) dataType).getElementType());
                break;
            case MAP:
                elementGetter = element -> DataTypeUtils.toMap(element, ARRAY_FIELD_NAME);
                break;
            case RECORD:
                elementGetter = element -> DataTypeUtils.toRecord(element, ARRAY_FIELD_NAME);
                break;
            case CHOICE:
                elementGetter = element -> {
                    final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                    final DataType chosenDataType = DataTypeUtils.chooseDataType(element, choiceDataType);
                    if (chosenDataType == null) {
                        throw new IllegalTypeConversionException(String.format(
                                "Cannot convert value [%s] of type %s for array element to any of the following available Sub-Types for a Choice: %s",
                                element, element.getClass(), choiceDataType.getPossibleSubTypes()));
                    }

                    return DataTypeUtils.convertType(element, chosenDataType, ARRAY_FIELD_NAME);
                };
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type: " + dataType.getFieldType());
        }

        return element -> {
            if (element == null) {
                return null;
            }

            return elementGetter.getElementOrNull(element);
        };
    }

    /**
     * 런타임에 배열의 원소를 가져오기 위한 접근자 인터페이스.
     */
    public interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(Object element);
    }
}
