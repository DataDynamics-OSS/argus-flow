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
 *   nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/record/util/DataTypeUtils.java
 */
package io.datadynamics.nifi.processors.kudu.util;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.type.*;
import org.apache.nifi.serialization.record.util.DataTypeSet;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * NiFi Record API의 {@link DataType}과 Java 자료형 사이의 변환, 호환성 검사, 스키마 병합 등을 담당하는 유틸리티 클래스입니다.
 * Apache NiFi 프레임워크 내부의 {@code org.apache.nifi.serialization.record.util.DataTypeUtils}가
 * 외부 모듈에서 직접 참조할 수 없는 internal API이므로, 필요한 기능을 이 번들 내부로 이식(vendoring)하여 사용합니다.
 * PutKudu가 RecordReader로 읽은 레코드 값을 Kudu의 PartialRow에 채울 때 이 클래스를 통해 자료형을 변환합니다.
 */
public class DataTypeUtils {
    private static final Logger logger = LoggerFactory.getLogger(DataTypeUtils.class);

    /**
     * 부동소숫점을 파싱하기 위한 정규 표현식
     */
    private static final String OptionalSign = "[\\-\\+]?";
    private static final String Infinity = "(Infinity)";
    private static final String NotANumber = "(NaN)";
    private static final String Base10Digits = "\\d+";
    private static final String Base10Decimal = "\\." + Base10Digits;
    private static final String OptionalBase10Decimal = "(\\.\\d*)?";
    private static final String Base10Exponent = "[eE]" + OptionalSign + Base10Digits;
    private static final String OptionalBase10Exponent = "(" + Base10Exponent + ")?";
    private static final String doubleRegex =
            OptionalSign +
                    "(" +
                    Infinity + "|" +
                    NotANumber + "|" +
                    "(" + Base10Digits + OptionalBase10Decimal + ")" + "|" +
                    "(" + Base10Digits + OptionalBase10Decimal + Base10Exponent + ")" + "|" +
                    "(" + Base10Decimal + OptionalBase10Exponent + ")" +
                    ")";
    private static final Pattern FLOATING_POINT_PATTERN = Pattern.compile(doubleRegex);
    private static final String decimalRegex =
            OptionalSign +
                    "(" + Base10Digits + OptionalBase10Decimal + ")" + "|" +
                    "(" + Base10Digits + OptionalBase10Decimal + Base10Exponent + ")" + "|" +
                    "(" + Base10Decimal + OptionalBase10Exponent + ")";
    private static final Pattern DECIMAL_PATTERN = Pattern.compile(decimalRegex);
    private static final Supplier<DateFormat> DEFAULT_DATE_FORMAT = () -> getDateFormat(RecordFieldType.DATE.getDefaultFormat());
    private static final Supplier<DateFormat> DEFAULT_TIME_FORMAT = () -> getDateFormat(RecordFieldType.TIME.getDefaultFormat());
    private static final Supplier<DateFormat> DEFAULT_TIMESTAMP_FORMAT = () -> getDateFormat(RecordFieldType.TIMESTAMP.getDefaultFormat());
    private static final int FLOAT_SIGNIFICAND_PRECISION = 24; // As specified in IEEE 754 binary32
    private static final int DOUBLE_SIGNIFICAND_PRECISION = 53; // As specified in IEEE 754 binary64
    private static final Long MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT = Double.valueOf(Math.pow(2, FLOAT_SIGNIFICAND_PRECISION)).longValue();
    private static final Long MIN_GUARANTEED_PRECISE_WHOLE_IN_FLOAT = -MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT;
    private static final BigInteger MIN_FLOAT_VALUE_IN_BIGINT = BigInteger.valueOf(MIN_GUARANTEED_PRECISE_WHOLE_IN_FLOAT);
    private static final BigInteger MAX_FLOAT_VALUE_IN_BIGINT = BigInteger.valueOf(MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT);
    private static final Long MAX_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE = Double.valueOf(Math.pow(2, DOUBLE_SIGNIFICAND_PRECISION)).longValue();
    private static final Long MIN_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE = -MAX_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE;
    private static final BigInteger MIN_DOUBLE_VALUE_IN_BIGINT = BigInteger.valueOf(MIN_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE);
    private static final BigInteger MAX_DOUBLE_VALUE_IN_BIGINT = BigInteger.valueOf(MAX_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE);
    private static final double MAX_FLOAT_VALUE_IN_DOUBLE = Float.valueOf(Float.MAX_VALUE).doubleValue();
    private static final double MIN_FLOAT_VALUE_IN_DOUBLE = -MAX_FLOAT_VALUE_IN_DOUBLE;
    private static final Map<RecordFieldType, Predicate<Object>> NUMERIC_VALIDATORS = new EnumMap<>(RecordFieldType.class);
    // FIXED
    public static DateTimeFormatter DEFAULT_NANOSECONDS_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .toFormatter();
    // FIXED
    public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    static {
        NUMERIC_VALIDATORS.put(RecordFieldType.BIGINT, value -> value instanceof BigInteger);
        NUMERIC_VALIDATORS.put(RecordFieldType.LONG, value -> value instanceof Long);
        NUMERIC_VALIDATORS.put(RecordFieldType.INT, value -> value instanceof Integer);
        NUMERIC_VALIDATORS.put(RecordFieldType.BYTE, value -> value instanceof Byte);
        NUMERIC_VALIDATORS.put(RecordFieldType.SHORT, value -> value instanceof Short);
        NUMERIC_VALIDATORS.put(RecordFieldType.DOUBLE, value -> value instanceof Double);
        NUMERIC_VALIDATORS.put(RecordFieldType.FLOAT, value -> value instanceof Float);
        NUMERIC_VALIDATORS.put(RecordFieldType.DECIMAL, value -> value instanceof BigDecimal);
    }

    public static Object convertType(final Object value, final DataType dataType, final String fieldName) {
        return convertType(value, dataType, fieldName, StandardCharsets.UTF_8);
    }

    public static Object convertType(final Object value, final DataType dataType, final String fieldName, final Charset charset) {
        return convertType(value, dataType, DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_TIMESTAMP_FORMAT, fieldName, charset);
    }

    public static DateFormat getDateFormat(final RecordFieldType fieldType, final Supplier<DateFormat> dateFormat,
                                           final Supplier<DateFormat> timeFormat, final Supplier<DateFormat> timestampFormat) {
        switch (fieldType) {
            case DATE:
                return dateFormat.get();
            case TIME:
                return timeFormat.get();
            case TIMESTAMP:
                return timestampFormat.get();
        }

        return null;
    }

    public static Object convertType(final Object value, final DataType dataType, final Supplier<DateFormat> dateFormat, final Supplier<DateFormat> timeFormat,
                                     final Supplier<DateFormat> timestampFormat, final String fieldName) {
        return convertType(value, dataType, dateFormat, timeFormat, timestampFormat, fieldName, StandardCharsets.UTF_8);
    }

    /**
     * value를 dataType의 RecordFieldType에 맞는 Java 자료형으로 변환하는 핵심 디스패치 메서드입니다.
     * dataType.getFieldType()에 따라 각 타입 전용 변환(toInteger, toBigDecimal, toTimestamp 등)으로 분기하며,
     * CHOICE 타입은 실제 값과 가장 잘 맞는 하위 타입을 선택한 뒤 재귀적으로 다시 변환합니다.
     */
    public static Object convertType(final Object value, final DataType dataType, final Supplier<DateFormat> dateFormat, final Supplier<DateFormat> timeFormat,
                                     final Supplier<DateFormat> timestampFormat, final String fieldName, final Charset charset) {

        if (value == null) {
            return null;
        }

        switch (dataType.getFieldType()) {
            case BIGINT:
                return toBigInt(value, fieldName);
            case BOOLEAN:
                return toBoolean(value, fieldName);
            case BYTE:
                return toByte(value, fieldName);
            case CHAR:
                return toCharacter(value, fieldName);
            case DATE:
                return convertTypeToDate(value, dateFormat, fieldName);
            case DECIMAL:
                return toBigDecimal(value, fieldName);
            case DOUBLE:
                return toDouble(value, fieldName);
            case FLOAT:
                return toFloat(value, fieldName);
            case INT:
                return toInteger(value, fieldName);
            case LONG:
                return toLong(value, fieldName);
            case SHORT:
                return toShort(value, fieldName);
            case ENUM:
                return toEnum(value, (EnumDataType) dataType, fieldName);
            case STRING:
                return toString(value, () -> getDateFormat(dataType.getFieldType(), dateFormat, timeFormat, timestampFormat), charset);
            case TIME:
                return toTime(value, timeFormat, fieldName);
            case TIMESTAMP:
                return toTimestamp(value, timestampFormat, fieldName);
            case UUID:
                return toUUID(value);
            case ARRAY:
                return toArray(value, fieldName, ((ArrayDataType) dataType).getElementType(), charset);
            case MAP:
                return toMap(value, fieldName);
            case RECORD:
                final RecordDataType recordType = (RecordDataType) dataType;
                final RecordSchema childSchema = recordType.getChildSchema();
                return toRecord(value, childSchema, fieldName, charset);
            case CHOICE: {
                final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                final DataType chosenDataType = chooseDataType(value, choiceDataType);
                if (chosenDataType == null) {
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                            + " for field " + fieldName + " to any of the following available Sub-Types for a Choice: " + choiceDataType.getPossibleSubTypes());
                }

                return convertType(value, chosenDataType, fieldName, charset);
            }
        }

        return null;
    }

    private static Object toUUID(Object value) {
        if (value == null) {
            throw new IllegalTypeConversionException("Null values cannot be converted to a UUID");
        }

        if (value instanceof String) {
            try {
                return UUID.fromString((String) value);
            } catch (Exception ex) {
                throw new IllegalTypeConversionException(String.format("Could not parse %s into a UUID", value), ex);
            }
        } else if (value instanceof byte[]) {
            return uuidFromBytes((byte[]) value);
        } else if (value instanceof Byte[]) {
            Byte[] array = (Byte[]) value;
            byte[] converted = new byte[array.length];
            for (int x = 0; x < array.length; x++) {
                converted[x] = array[x];
            }
            return uuidFromBytes(converted);
        } else {
            throw new IllegalTypeConversionException(value.getClass() + " cannot be converted into a UUID");
        }
    }

    private static UUID uuidFromBytes(byte[] bytes) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return new UUID(buffer.getLong(), buffer.getLong());
        } catch (Exception ex) {
            throw new IllegalTypeConversionException("Could not convert bytes to UUID");
        }
    }

    public static boolean isCompatibleDataType(final Object value, final DataType dataType) {
        return isCompatibleDataType(value, dataType, false);
    }

    /**
     * value가 dataType으로 변환/취급 가능한지 자료형 호환성을 검사합니다. strict가 true이면 RECORD 타입에 대해
     * 레코드의 모든 필드가 스키마에 존재해야 하는 등 더 엄격한 기준을 적용합니다.
     */
    public static boolean isCompatibleDataType(final Object value, final DataType dataType, final boolean strict) {
        switch (dataType.getFieldType()) {
            case ARRAY:
                return isArrayTypeCompatible(value, ((ArrayDataType) dataType).getElementType(), strict);
            case BIGINT:
                return isBigIntTypeCompatible(value);
            case BOOLEAN:
                return isBooleanTypeCompatible(value);
            case BYTE:
                return isByteTypeCompatible(value);
            case CHAR:
                return isCharacterTypeCompatible(value);
            case DATE:
                return isDateTypeCompatible(value, dataType.getFormat());
            case DECIMAL:
                return isDecimalTypeCompatible(value);
            case DOUBLE:
                return isDoubleTypeCompatible(value);
            case FLOAT:
                return isFloatTypeCompatible(value);
            case INT:
                return isIntegerTypeCompatible(value);
            case LONG:
                return isLongTypeCompatible(value);
            case RECORD: {
                final RecordSchema schema = ((RecordDataType) dataType).getChildSchema();
                return isRecordTypeCompatible(schema, value, strict);
            }
            case SHORT:
                return isShortTypeCompatible(value);
            case TIME:
                return isTimeTypeCompatible(value, dataType.getFormat());
            case TIMESTAMP:
                return isTimestampTypeCompatible(value, dataType.getFormat());
            case STRING:
                return isStringTypeCompatible(value);
            case ENUM:
                return isEnumTypeCompatible(value, (EnumDataType) dataType);
            case MAP:
                return isMapTypeCompatible(value);
            case CHOICE: {
                final DataType chosenDataType = chooseDataType(value, (ChoiceDataType) dataType);
                return chosenDataType != null;
            }
        }

        return false;
    }

    /**
     * CHOICE 타입에 포함된 여러 하위 타입(중첩된 CHOICE는 재귀적으로 펼침) 중에서 value와 호환되는 타입들을 찾고,
     * 후보가 여러 개면 findMostSuitableType()으로 가장 적합한 하나를 선택하여 반환합니다.
     */
    public static DataType chooseDataType(final Object value, final ChoiceDataType choiceType) {
        Queue<DataType> possibleSubTypes = new LinkedList<>(choiceType.getPossibleSubTypes());
        List<DataType> compatibleSimpleSubTypes = new ArrayList<>();

        DataType subType;
        while ((subType = possibleSubTypes.poll()) != null) {
            if (subType instanceof ChoiceDataType) {
                possibleSubTypes.addAll(((ChoiceDataType) subType).getPossibleSubTypes());
            } else {
                if (isCompatibleDataType(value, subType)) {
                    compatibleSimpleSubTypes.add(subType);
                }
            }
        }

        int nrOfCompatibleSimpleSubTypes = compatibleSimpleSubTypes.size();

        final DataType chosenSimpleType;
        if (nrOfCompatibleSimpleSubTypes == 0) {
            chosenSimpleType = null;
        } else if (nrOfCompatibleSimpleSubTypes == 1) {
            chosenSimpleType = compatibleSimpleSubTypes.get(0);
        } else {
            chosenSimpleType = findMostSuitableType(value, compatibleSimpleSubTypes, Function.identity())
                    .orElse(compatibleSimpleSubTypes.get(0));
        }

        return chosenSimpleType;
    }

    public static <T> Optional<T> findMostSuitableType(Object value, List<T> types, Function<T, DataType> dataTypeMapper) {
        if (value instanceof String) {
            return findMostSuitableTypeByStringValue((String) value, types, dataTypeMapper);
        } else {
            DataType inferredDataType = inferDataType(value, null);

            if (inferredDataType != null && !inferredDataType.getFieldType().equals(RecordFieldType.STRING)) {
                for (T type : types) {
                    if (inferredDataType.equals(dataTypeMapper.apply(type))) {
                        return Optional.of(type);
                    }
                }

                for (T type : types) {
                    if (getWiderType(dataTypeMapper.apply(type), inferredDataType).isPresent()) {
                        return Optional.of(type);
                    }
                }
            }
        }

        return Optional.empty();
    }

    public static <T> Optional<T> findMostSuitableTypeByStringValue(String valueAsString, List<T> types, Function<T, DataType> dataTypeMapper) {
        // Sorting based on the RecordFieldType enum ordering looks appropriate here as we want simpler types
        //  first and the enum's ordering seems to reflect that
        Collections.sort(types, Comparator.comparing(type -> dataTypeMapper.apply(type).getFieldType()));

        for (T type : types) {
            try {
                if (isCompatibleDataType(valueAsString, dataTypeMapper.apply(type))) {
                    return Optional.of(type);
                }
            } catch (Exception e) {
                logger.error("Exception thrown while checking if '" + valueAsString + "' is compatible with '" + type + "'", e);
            }
        }

        return Optional.empty();
    }

    public static Record toRecord(final Object value, final RecordSchema recordSchema, final String fieldName) {
        return toRecord(value, recordSchema, fieldName, StandardCharsets.UTF_8);
    }

    public static Record toRecord(final Object value, final RecordSchema recordSchema, final String fieldName, final Charset charset) {
        if (value == null) {
            return null;
        }

        if (value instanceof Record) {
            return ((Record) value);
        }

        if (value instanceof Map) {
            if (recordSchema == null) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                        + " to Record for field " + fieldName + " because the value is a Map but no Record Schema was provided");
            }

            final Map<?, ?> map = (Map<?, ?>) value;
            final Map<String, Object> coercedValues = new LinkedHashMap<>();

            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                final Object keyValue = entry.getKey();
                if (keyValue == null) {
                    continue;
                }

                final String key = keyValue.toString();
                final Optional<DataType> desiredTypeOption = recordSchema.getDataType(key);
                if (!desiredTypeOption.isPresent()) {
                    continue;
                }

                final Object rawValue = entry.getValue();
                final Object coercedValue = convertType(rawValue, desiredTypeOption.get(), fieldName, charset);
                coercedValues.put(key, coercedValue);
            }

            return new MapRecord(recordSchema, coercedValues);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Record for field " + fieldName);
    }

    public static Record toRecord(final Object value, final String fieldName) {
        return toRecord(value, fieldName, StandardCharsets.UTF_8);
    }

    public static RecordSchema inferSchema(final Map<String, Object> values, final String fieldName, final Charset charset) {
        if (values == null) {
            return null;
        }

        final List<RecordField> inferredFieldTypes = new ArrayList<>();
        final Map<String, Object> coercedValues = new LinkedHashMap<>();

        for (final Map.Entry<?, ?> entry : values.entrySet()) {
            final Object keyValue = entry.getKey();
            if (keyValue == null) {
                continue;
            }

            final String key = keyValue.toString();
            final Object rawValue = entry.getValue();
            final DataType inferredDataType = inferDataType(rawValue, RecordFieldType.STRING.getDataType());

            final RecordField recordField = new RecordField(key, inferredDataType, true);
            inferredFieldTypes.add(recordField);

            final Object coercedValue = convertType(rawValue, inferredDataType, fieldName, charset);
            coercedValues.put(key, coercedValue);
        }

        final RecordSchema inferredSchema = new SimpleRecordSchema(inferredFieldTypes);
        return inferredSchema;
    }

    public static Record toRecord(final Object value, final String fieldName, final Charset charset) {
        if (value == null) {
            return null;
        }

        if (value instanceof Record) {
            return ((Record) value);
        }

        final List<RecordField> inferredFieldTypes = new ArrayList<>();
        if (value instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) value;
            final Map<String, Object> coercedValues = new LinkedHashMap<>();

            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                final Object keyValue = entry.getKey();
                if (keyValue == null) {
                    continue;
                }

                final String key = keyValue.toString();
                final Object rawValue = entry.getValue();
                final DataType inferredDataType = inferDataType(rawValue, RecordFieldType.STRING.getDataType());

                final RecordField recordField = new RecordField(key, inferredDataType, true);
                inferredFieldTypes.add(recordField);

                final Object coercedValue = convertType(rawValue, inferredDataType, fieldName, charset);
                coercedValues.put(key, coercedValue);
            }

            final RecordSchema inferredSchema = new SimpleRecordSchema(inferredFieldTypes);
            return new MapRecord(inferredSchema, coercedValues);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Record for field " + fieldName);
    }

    /**
     * value의 실제 Java 타입을 검사하여 가장 근접한 RecordFieldType의 DataType을 추론합니다.
     * 스칼라 값은 해당 타입으로 직접 매핑하고, Map/배열/Iterable은 각 원소 타입을 병합하여 ARRAY 또는 RECORD 타입으로 추론합니다.
     * 추론할 수 없으면 defaultType을 반환합니다.
     */
    public static DataType inferDataType(final Object value, final DataType defaultType) {
        if (value == null) {
            return defaultType;
        }

        if (value instanceof String) {
            return RecordFieldType.STRING.getDataType();
        }

        if (value instanceof Record) {
            final RecordSchema schema = ((Record) value).getSchema();
            return RecordFieldType.RECORD.getRecordDataType(schema);
        }

        if (value instanceof Number) {
            if (value instanceof Long) {
                return RecordFieldType.LONG.getDataType();
            }
            if (value instanceof Integer) {
                return RecordFieldType.INT.getDataType();
            }
            if (value instanceof Short) {
                return RecordFieldType.SHORT.getDataType();
            }
            if (value instanceof Byte) {
                return RecordFieldType.BYTE.getDataType();
            }
            if (value instanceof Float) {
                return RecordFieldType.FLOAT.getDataType();
            }
            if (value instanceof Double) {
                return RecordFieldType.DOUBLE.getDataType();
            }
            if (value instanceof BigInteger) {
                return RecordFieldType.BIGINT.getDataType();
            }
            if (value instanceof BigDecimal) {
                final BigDecimal bigDecimal = (BigDecimal) value;
                return RecordFieldType.DECIMAL.getDecimalDataType(bigDecimal.precision(), bigDecimal.scale());
            }
        }

        if (value instanceof Boolean) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        if (value instanceof java.sql.Time) {
            return RecordFieldType.TIME.getDataType();
        }
        if (value instanceof java.sql.Timestamp) {
            return RecordFieldType.TIMESTAMP.getDataType();
        }
        if (value instanceof java.util.Date) {
            return RecordFieldType.DATE.getDataType();
        }
        if (value instanceof Character) {
            return RecordFieldType.CHAR.getDataType();
        }

        // A value of a Map could be either a Record or a Map type. In either case, it must have Strings as keys.
        if (value instanceof Map) {
            final Map<String, Object> map;
            // Only transform the map if the keys aren't strings
            boolean allStrings = true;
            for (final Object key : ((Map<?, ?>) value).keySet()) {
                if (!(key instanceof String)) {
                    allStrings = false;
                    break;
                }
            }

            if (allStrings) {
                map = (Map<String, Object>) value;
            } else {
                final Map<?, ?> m = (Map<?, ?>) value;
                map = new LinkedHashMap<>(m.size());
                m.forEach((k, v) -> map.put(k == null ? null : k.toString(), v));
            }
            return inferRecordDataType(map);
        }

        if (value.getClass().isArray()) {
            DataType mergedDataType = null;

            int length = Array.getLength(value);
            for (int index = 0; index < length; index++) {
                final DataType inferredDataType = inferDataType(Array.get(value, index), RecordFieldType.STRING.getDataType());
                mergedDataType = mergeDataTypes(mergedDataType, inferredDataType);
            }

            if (mergedDataType == null) {
                mergedDataType = RecordFieldType.STRING.getDataType();
            }

            return RecordFieldType.ARRAY.getArrayDataType(mergedDataType);
        }

        if (value instanceof Iterable) {
            final Iterable<?> iterable = (Iterable<?>) value;

            DataType mergedDataType = null;
            for (final Object arrayValue : iterable) {
                final DataType inferredDataType = inferDataType(arrayValue, RecordFieldType.STRING.getDataType());
                mergedDataType = mergeDataTypes(mergedDataType, inferredDataType);
            }

            if (mergedDataType == null) {
                mergedDataType = RecordFieldType.STRING.getDataType();
            }

            return RecordFieldType.ARRAY.getArrayDataType(mergedDataType);
        }

        return defaultType;
    }

    private static DataType inferRecordDataType(final Map<String, ?> map) {
        final List<RecordField> fields = new ArrayList<>(map.size());
        for (final Map.Entry<String, ?> entry : map.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            final DataType dataType = inferDataType(value, RecordFieldType.STRING.getDataType());
            final RecordField field = new RecordField(key, dataType, true);
            fields.add(field);
        }

        final RecordSchema schema = new SimpleRecordSchema(fields);
        return RecordFieldType.RECORD.getRecordDataType(schema);
    }

    /**
     * 주어진 레코드 구조 객체가 스키마와 호환되는지 확인합니다.
     *
     * @param schema 레코드 스키마. schema가 null이면 스키마 검증을 수행하지 않습니다.
     * @param value  레코드 구조를 갖는 객체, 즉 Record 또는 Map
     * @param strict 엄격한 일치 여부 확인. true이면 레코드의 모든 필드가 스키마에 대응하는 항목을 가져야 합니다.
     * @return 객체가 스키마와 호환되면 true
     */
    private static boolean isRecordTypeCompatible(RecordSchema schema, Object value, boolean strict) {

        if (value == null) {
            return false;
        }

        if (!(value instanceof Record) && !(value instanceof Map)) {
            return false;
        }

        if (schema == null) {
            return true;
        }

        if (strict) {
            if (value instanceof Record) {
                if (!schema.getFieldNames().containsAll(((Record) value).getRawFieldNames())) {
                    return false;
                }
            }
        }

        for (final RecordField childField : schema.getFields()) {
            final Object childValue;
            if (value instanceof Record) {
                childValue = ((Record) value).getValue(childField);
            } else {
                childValue = ((Map) value).get(childField.getFieldName());
            }

            if (childValue == null && !childField.isNullable()) {
                logger.debug("Value is not compatible with schema because field {} has a null value, which is not allowed in the schema", childField.getFieldName());
                return false;
            }
            if (childValue == null) {
                continue; // consider compatible
            }

            if (!isCompatibleDataType(childValue, childField.getDataType(), strict)) {
                return false;
            }
        }
        return true;
    }

    public static Object[] toArray(final Object value, final String fieldName, final DataType elementDataType) {
        return toArray(value, fieldName, elementDataType, StandardCharsets.UTF_8);
    }

    public static Object[] toArray(final Object value, final String fieldName, final DataType elementDataType, final Charset charset) {
        if (value == null) {
            return null;
        }

        if (value instanceof Object[]) {
            return (Object[]) value;
        }

        if (value instanceof String && RecordFieldType.BYTE.getDataType().equals(elementDataType)) {
            byte[] src = ((String) value).getBytes(charset);
            Byte[] dest = new Byte[src.length];
            for (int i = 0; i < src.length; i++) {
                dest[i] = src[i];
            }
            return dest;
        }

        if (value instanceof byte[]) {
            byte[] src = (byte[]) value;
            Byte[] dest = new Byte[src.length];
            for (int i = 0; i < src.length; i++) {
                dest[i] = src[i];
            }
            return dest;
        }

        if (value instanceof UUID) {
            UUID uuid = (UUID) value;
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
            Byte[] result = new Byte[16];
            byte[] array = buffer.array();
            for (int index = 0; index < array.length; index++) {
                result[index] = array[index];
            }

            return result;
        }

        if (value instanceof List) {
            final List<?> list = (List<?>) value;
            return list.toArray();
        }

        try {
            if (value instanceof Blob) {
                Blob blob = (Blob) value;
                long rawBlobLength = blob.length();
                if (rawBlobLength > Integer.MAX_VALUE) {
                    throw new IllegalTypeConversionException("Value of type " + value.getClass() + " too large to convert to Object Array for field " + fieldName);
                }
                int blobLength = (int) rawBlobLength;
                byte[] src = blob.getBytes(1, blobLength);
                Byte[] dest = new Byte[blobLength];
                for (int i = 0; i < src.length; i++) {
                    dest[i] = src[i];
                }
                return dest;
            } else {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Object Array for field " + fieldName);
            }
        } catch (IllegalTypeConversionException itce) {
            throw itce;
        } catch (Exception e) {
            throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Object Array for field " + fieldName, e);
        }
    }

    public static boolean isArrayTypeCompatible(final Object value, final DataType elementDataType) {
        return isArrayTypeCompatible(value, elementDataType, false);
    }

    public static boolean isArrayTypeCompatible(final Object value, final DataType elementDataType, final boolean strict) {
        if (value == null) {
            return false;
        }
        // Either an object array (check the element type) or a String to be converted to byte[]
        if (value instanceof Object[]) {
            for (Object o : ((Object[]) value)) {
                // Check each element to ensure its type is the same or can be coerced (if need be)
                if (!isCompatibleDataType(o, elementDataType, strict)) {
                    return false;
                }
            }
            return true;
        } else {
            return value instanceof String && RecordFieldType.BYTE.getDataType().equals(elementDataType);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            final Map<?, ?> original = (Map<?, ?>) value;

            boolean keysAreStrings = true;
            for (final Object key : original.keySet()) {
                if (!(key instanceof String)) {
                    keysAreStrings = false;
                    break;
                }
            }

            if (keysAreStrings) {
                return (Map<String, Object>) value;
            }

            final Map<String, Object> transformed = new LinkedHashMap<>();
            for (final Map.Entry<?, ?> entry : original.entrySet()) {
                final Object key = entry.getKey();
                if (key == null) {
                    transformed.put(null, entry.getValue());
                } else {
                    transformed.put(key.toString(), entry.getValue());
                }
            }

            return transformed;
        }

        if (value instanceof Record) {
            final Record record = (Record) value;
            final RecordSchema recordSchema = record.getSchema();
            if (recordSchema == null) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type Record to Map for field " + fieldName
                        + " because Record does not have an associated Schema");
            }

            final Map<String, Object> map = new LinkedHashMap<>();
            for (final String recordFieldName : recordSchema.getFieldNames()) {
                map.put(recordFieldName, record.getValue(recordFieldName));
            }

            return map;
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Map for field " + fieldName);
    }

    /**
     * 지정한 타입의 객체로부터 순수 Java 객체를 생성합니다. 스칼라가 아닌(복합, 중첩 등) 자료형은 재귀적으로 처리되어
     * 포함된 모든 객체가 Record API 객체나 구현 특화 객체가 아닌 순수 Java 객체가 되도록 합니다.
     *
     * @param value    변환할 객체
     * @param dataType 주어진 객체의 타입
     * @return 입력 객체를 순수 Java 형태로 변환한 결과 객체
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Object convertRecordFieldtoObject(final Object value, final DataType dataType) {

        if (value == null) {
            return null;
        }

        if (value instanceof Record) {
            Record record = (Record) value;
            RecordSchema recordSchema = record.getSchema();
            if (recordSchema == null) {
                throw new IllegalTypeConversionException("Cannot convert value of type Record to Map because Record does not have an associated Schema");
            }

            final Map<String, Object> recordMap = new LinkedHashMap<>();
            for (RecordField field : recordSchema.getFields()) {
                final DataType fieldDataType = field.getDataType();
                final String fieldName = field.getFieldName();
                Object fieldValue = record.getValue(fieldName);

                if (fieldValue == null) {
                    recordMap.put(fieldName, null);
                } else if (isScalarValue(fieldDataType, fieldValue)) {
                    recordMap.put(fieldName, fieldValue);
                } else if (fieldDataType instanceof RecordDataType) {
                    Record nestedRecord = (Record) fieldValue;
                    recordMap.put(fieldName, convertRecordFieldtoObject(nestedRecord, fieldDataType));
                } else if (fieldDataType instanceof MapDataType) {
                    recordMap.put(fieldName, convertRecordMapToJavaMap((Map) fieldValue, ((MapDataType) fieldDataType).getValueType()));

                } else if (fieldDataType instanceof ArrayDataType) {
                    recordMap.put(fieldName, convertRecordArrayToJavaArray((Object[]) fieldValue, ((ArrayDataType) fieldDataType).getElementType()));
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value [" + fieldValue + "] of type " + fieldDataType
                            + " to Map for field " + fieldName + " because the type is not supported");
                }
            }
            return recordMap;
        } else if (value instanceof Map) {
            return convertRecordMapToJavaMap((Map) value, ((MapDataType) dataType).getValueType());
        } else if (dataType != null && isScalarValue(dataType, value)) {
            return value;
        } else if (value instanceof Object[] && dataType instanceof ArrayDataType) {
            // This is likely a Map whose values are represented as an array. Return a new array with each element converted to a Java object
            return convertRecordArrayToJavaArray((Object[]) value, ((ArrayDataType) dataType).getElementType());
        }

        throw new IllegalTypeConversionException("Cannot convert value of class " + value.getClass().getName() + " because the type is not supported");
    }


    public static Map<String, Object> convertRecordMapToJavaMap(final Map<String, Object> map, DataType valueDataType) {

        if (map == null) {
            return null;
        }

        Map<String, Object> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            resultMap.put(entry.getKey(), convertRecordFieldtoObject(entry.getValue(), valueDataType));
        }
        return resultMap;
    }

    public static Object[] convertRecordArrayToJavaArray(final Object[] array, DataType elementDataType) {

        if (array == null || array.length == 0 || isScalarValue(elementDataType, array[0])) {
            return array;
        } else {
            // Must be an array of complex types, build an array of converted values
            Object[] resultArray = new Object[array.length];
            for (int i = 0; i < array.length; i++) {
                resultArray[i] = convertRecordFieldtoObject(array[i], elementDataType);
            }
            return resultArray;
        }
    }

    public static boolean isMapTypeCompatible(final Object value) {
        return value != null && (value instanceof Map || value instanceof MapRecord);
    }


    public static String toString(final Object value, final Supplier<DateFormat> format) {
        return toString(value, format, StandardCharsets.UTF_8);
    }

    public static String toString(final Object value, final Supplier<DateFormat> format, final Charset charset) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return (String) value;
        }

        if (format == null && value instanceof java.util.Date) {
            return String.valueOf(((java.util.Date) value).getTime());
        }

        if (value instanceof java.util.Date) {
            return formatDate((java.util.Date) value, format);
        }

        if (value instanceof byte[]) {
            return new String((byte[]) value, charset);
        }

        if (value instanceof Byte[]) {
            Byte[] src = (Byte[]) value;
            byte[] dest = new byte[src.length];
            for (int i = 0; i < src.length; i++) {
                dest[i] = src[i];
            }
            return new String(dest, charset);
        }
        if (value instanceof Object[]) {
            Object[] o = (Object[]) value;
            if (o.length > 0) {

                byte[] dest = new byte[o.length];
                for (int i = 0; i < o.length; i++) {
                    dest[i] = (byte) o[i];
                }
                return new String(dest, charset);
            } else {
                return ""; // Empty array = empty string
            }
        }
        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[32 * 1024]; // 32K default buffer
            try (Reader reader = clob.getCharacterStream()) {
                int charsRead;
                while ((charsRead = reader.read(buffer)) != -1) {
                    sb.append(buffer, 0, charsRead);
                }
                return sb.toString();
            } catch (Exception e) {
                throw new IllegalTypeConversionException("Cannot convert value " + value + " of type " + value.getClass() + " to a valid String", e);
            }
        }

        return value.toString();
    }

    private static String formatDate(final java.util.Date date, final Supplier<DateFormat> formatSupplier) {
        final DateFormat dateFormat = formatSupplier.get();
        if (dateFormat == null) {
            return String.valueOf((date).getTime());
        }

        return dateFormat.format(date);
    }

    public static String toString(final Object value, final String format) {
        return toString(value, format, StandardCharsets.UTF_8);
    }

    public static String toString(final Object value, final String format, final Charset charset) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return (String) value;
        }

        if (format == null && value instanceof java.util.Date) {
            return String.valueOf(((java.util.Date) value).getTime());
        }

        if (value instanceof java.sql.Date) {
            return getDateFormat(format).format((java.util.Date) value);
        }
        if (value instanceof java.sql.Time) {
            return getDateFormat(format).format((java.util.Date) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return getDateFormat(format).format((java.util.Date) value);
        }
        if (value instanceof java.util.Date) {
            return getDateFormat(format).format((java.util.Date) value);
        }
        if (value instanceof Blob) {
            Blob blob = (Blob) value;
            StringBuilder sb = new StringBuilder();
            byte[] buffer = new byte[32 * 1024]; // 32K default buffer
            try (InputStream inStream = blob.getBinaryStream()) {
                int bytesRead;
                while ((bytesRead = inStream.read(buffer)) != -1) {
                    sb.append(new String(buffer, charset), 0, bytesRead);
                }
                return sb.toString();
            } catch (Exception e) {
                throw new IllegalTypeConversionException("Cannot convert value " + value + " of type " + value.getClass() + " to a valid String", e);
            }
        }
        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[32 * 1024]; // 32K default buffer
            try (Reader reader = clob.getCharacterStream()) {
                int charsRead;
                while ((charsRead = reader.read(buffer)) != -1) {
                    sb.append(buffer, 0, charsRead);
                }
                return sb.toString();
            } catch (Exception e) {
                throw new IllegalTypeConversionException("Cannot convert value " + value + " of type " + value.getClass() + " to a valid String", e);
            }
        }

        if (value instanceof Object[]) {
            return Arrays.toString((Object[]) value);
        }

        if (value instanceof byte[]) {
            return new String((byte[]) value, charset);
        }

        return value.toString();
    }

    public static boolean isStringTypeCompatible(final Object value) {
        return value != null;
    }

    public static boolean isEnumTypeCompatible(final Object value, final EnumDataType enumType) {
        return enumType.getEnums() != null && enumType.getEnums().contains(value);
    }

    private static Object toEnum(Object value, EnumDataType dataType, String fieldName) {
        if (dataType.getEnums() != null && dataType.getEnums().contains(value)) {
            return value.toString();
        }
        throw new IllegalTypeConversionException("Cannot convert value " + value + " of type " + dataType + " for field " + fieldName);
    }

    public static java.sql.Date toDate(final Object value, final Supplier<DateFormat> format, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Date) {
            return (Date) value;
        }

        if (value instanceof java.util.Date) {
            java.util.Date _temp = (java.util.Date) value;
            return new Date(_temp.getTime());
        }

        if (value instanceof Number) {
            final long longValue = ((Number) value).longValue();
            return new Date(longValue);
        }

        if (value instanceof String) {
            try {
                final String string = ((String) value).trim();
                if (string.isEmpty()) {
                    return null;
                }

                if (format == null) {
                    return new Date(Long.parseLong(string));
                }

                final DateFormat dateFormat = format.get();
                if (dateFormat == null) {
                    return new Date(Long.parseLong(string));
                }
                final java.util.Date utilDate = dateFormat.parse(string);
                return new Date(utilDate.getTime());
            } catch (final ParseException | NumberFormatException e) {
                throw new IllegalTypeConversionException("Could not convert value [" + value
                        + "] of type java.lang.String to Date because the value is not in the expected date format: " + format + " for field " + fieldName);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Date for field " + fieldName);
    }

    /**
     * Zone Identifier를 사용하여 Date Time Formatter를 가져옵니다.
     *
     * @param pattern 날짜 형식 패턴
     * @param zoneId  타임존 식별자
     * @return Date Time Formatter, pattern이 null이면 null을 반환
     */
    public static DateTimeFormatter getDateTimeFormatter(final String pattern, final ZoneId zoneId) {
        if (pattern == null || zoneId == null) {
            return null;
        }
        return DateTimeFormatter.ofPattern(pattern).withZone(zoneId);
    }

    /**
     * 숫자 또는 형식화된 문자열로부터의 변환을 지원하며 값을 Local Date로 변환합니다.
     *
     * @param value     변환할 값
     * @param formatter Date Time Formatter를 제공하는 Supplier. 문자열 파싱이 필요하지 않으면 null 가능
     * @param fieldName 변환 대상 값의 필드명
     * @return Local Date, 변환할 값이 null이면 null 반환
     * @throws IllegalTypeConversionException 문자열 변환에 실패하거나 지원하지 않는 값이 제공된 경우 발생
     */
    public static LocalDate toLocalDate(final Object value, final Supplier<DateTimeFormatter> formatter, final String fieldName) {
        LocalDate localDate;

        if (value == null) {
            return null;
        } else if (value instanceof LocalDate) {
            localDate = (LocalDate) value;
        } else if (value instanceof java.sql.Date) {
            final java.sql.Date date = (java.sql.Date) value;
            localDate = date.toLocalDate();
        } else if (value instanceof java.util.Date) {
            final java.util.Date date = (java.util.Date) value;
            localDate = parseLocalDateEpochMillis(date.getTime());
        } else if (value instanceof Number) {
            final long epochMillis = ((Number) value).longValue();
            localDate = parseLocalDateEpochMillis(epochMillis);
        } else if (value instanceof String) {
            try {
                localDate = parseLocalDate((String) value, formatter);
            } catch (final RuntimeException e) {
                final String message = String.format("Failed Conversion of Field [%s] from String [%s] to LocalDate --> Cause: %s", fieldName, value, "Date pattern cannot parse actual date.");
                throw new IllegalTypeConversionException(message, e);
            }
        } else {
            final String message = String.format("Failed Conversion of Field [%s] from Value [%s] Type [%s] to LocalDate", fieldName, value, value.getClass());
            throw new IllegalTypeConversionException(message);
        }

        return localDate;
    }

    /**
     * java.time.LocalDate 파싱과 DateFormat에서 DateTimeFormatter로의 변환을 이용하여 값을 java.sql.Date로 변환합니다.
     * <p>
     * 레거시 java.text.DateFormat에서 java.time.DateTimeFormatter로의 변환을 지원하는 과도기적(transitional) 메서드입니다.
     *
     * @param value     변환할 값 객체
     * @param format    파싱시 필요한 java.text.DateFormat을 제공하는 Supplier
     * @param fieldName 파싱 대상 필드명
     * @return java.sql.Date, 값이 null이면 null 반환
     */
    private static Date convertTypeToDate(final Object value, final Supplier<DateFormat> format, final String fieldName) {
        if (value == null) {
            return null;
        } else {
            final LocalDate localDate = toLocalDate(value, () -> {
                final SimpleDateFormat dateFormat = (SimpleDateFormat) format.get();
                return dateFormat == null ? null : DateTimeFormatter.ofPattern(dateFormat.toPattern());
            }, fieldName);
            return Date.valueOf(localDate);
        }
    }

    /**
     * Date Time Formatter가 제공된 경우 이를 사용하여 문자열로부터 Local Date를 파싱합니다.
     *
     * @param value     형식화된 문자열 또는 Epoch 밀리초 숫자를 담고 있는 null이 아닌 문자열
     * @param formatter Date Time Formatter를 제공하는 Supplier
     * @return Local Date, 제공된 값이 비어있으면 null 반환
     */
    private static LocalDate parseLocalDate(final String value, final Supplier<DateTimeFormatter> formatter) {
        LocalDate localDate = null;

        final String normalized = value.trim();
        if (!normalized.isEmpty()) {
            if (formatter == null) {
                localDate = parseLocalDateEpochMillis(normalized);
            } else {
                final DateTimeFormatter dateTimeFormatter = formatter.get();
                if (dateTimeFormatter == null) {
                    localDate = parseLocalDateEpochMillis(normalized);
                } else {
                    localDate = LocalDate.parse(normalized, dateTimeFormatter);
                }
            }
        }

        return localDate;
    }


    /**
     * Epoch 밀리초 숫자를 담고 있을 것으로 예상되는 문자열로부터 Local Date를 파싱합니다.
     *
     * @param number Epoch 밀리초를 담고 있을 것으로 예상되는 숫자 문자열
     * @return Epoch 밀리초로부터 변환된 Local Date
     */
    private static LocalDate parseLocalDateEpochMillis(final String number) {
        final long epochMillis = Long.parseLong(number);
        return parseLocalDateEpochMillis(epochMillis);
    }

    /**
     * 시스템 기본 Zone Offset을 사용하여 Epoch 밀리초로부터 Local Date를 파싱합니다.
     *
     * @param epochMillis Epoch 밀리초
     * @return Epoch 밀리초로부터 변환된 Local Date
     */
    private static LocalDate parseLocalDateEpochMillis(final long epochMillis) {
        final Instant instant = Instant.ofEpochMilli(epochMillis);
        final ZonedDateTime zonedDateTime = instant.atZone(ZoneOffset.systemDefault());
        return zonedDateTime.toLocalDate();
    }

    /**
     * 로컬 타임존 기준의 java.sql.Date 객체(일반적으로 java.sql.ResultSet에서 오며 시간 부분이 00:00:00)를
     * UTC 정규화 형태(입력과 동일한 날짜/시간을 갖는 UTC 시각에 해당하는 epoch로 저장)로 변환합니다.
     *
     * @param dateLocalTZ 로컬 타임존 기준의 java.sql.Date
     * @return UTC 정규화 형태의 java.sql.Date
     */
    public static Date convertDateToUTC(Date dateLocalTZ) {
        ZonedDateTime zdtLocalTZ = ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateLocalTZ.getTime()), ZoneId.systemDefault());
        ZonedDateTime zdtUTC = zdtLocalTZ.withZoneSameLocal(ZoneOffset.UTC);
        return new Date(zdtUTC.toInstant().toEpochMilli());
    }

    public static boolean isDateTypeCompatible(final Object value, final String format) {
        if (value == null) {
            return false;
        }

        if (value instanceof java.util.Date || value instanceof Number) {
            return true;
        }

        if (value instanceof String) {
            if (format == null) {
                return isInteger((String) value);
            }

            try {
                getDateFormat(format).parse((String) value);
                return true;
            } catch (final ParseException e) {
                return false;
            }
        }

        return false;
    }

    private static boolean isInteger(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static Time toTime(final Object value, final Supplier<DateFormat> format, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Time) {
            return (Time) value;
        }

        if (value instanceof Number) {
            final long longValue = ((Number) value).longValue();
            return new Time(longValue);
        }

        if (value instanceof String) {
            try {
                final String string = ((String) value).trim();
                if (string.isEmpty()) {
                    return null;
                }

                if (format == null) {
                    return new Time(Long.parseLong(string));
                }

                final DateFormat dateFormat = format.get();
                if (dateFormat == null) {
                    return new Time(Long.parseLong(string));
                }
                final java.util.Date utilDate = dateFormat.parse(string);
                return new Time(utilDate.getTime());
            } catch (final ParseException e) {
                throw new IllegalTypeConversionException("Could not convert value [" + value
                        + "] of type java.lang.String to Time for field " + fieldName + " because the value is not in the expected date format: " + format);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Time for field " + fieldName);
    }

    /**
     * 기본 로컬 타임존을 사용하여 Date Format을 가져옵니다.
     *
     * @param pattern new SimpleDateFormat()에 사용할 날짜 형식 패턴
     * @return Date Format, pattern이 제공되지 않으면 null 반환
     */
    public static DateFormat getDateFormat(final String pattern) {
        if (pattern == null) {
            return null;
        }
        return getDateFormat(pattern, TimeZone.getDefault());
    }

    /**
     * 처리 중 날짜를 조정하기 위해 지정한 타임존을 사용하여 Date Format을 가져옵니다.
     *
     * @param pattern    new SimpleDateFormat()에 사용할 날짜 형식 패턴
     * @param timeZoneId TimeZone.getTimeZone()에 사용할 타임존 식별자
     * @return Date Format, 입력 파라미터가 제공되지 않으면 null 반환
     */
    public static DateFormat getDateFormat(final String pattern, final String timeZoneId) {
        if (pattern == null || timeZoneId == null) {
            return null;
        }
        return getDateFormat(pattern, TimeZone.getTimeZone(timeZoneId));
    }

    private static DateFormat getDateFormat(final String pattern, final TimeZone timeZone) {
        if (pattern == null) {
            return null;
        }
        final DateFormat dateFormat = new SimpleDateFormat(pattern);
        dateFormat.setTimeZone(timeZone);
        return dateFormat;
    }

    public static boolean isTimeTypeCompatible(final Object value, final String format) {
        return isDateTypeCompatible(value, format);
    }

    public static Timestamp toTimestamp(final Object value, final Supplier<DateFormat> format, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }

        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }

        if (value instanceof Number) {
            final long longValue = ((Number) value).longValue();
            return new Timestamp(longValue);
        }

        if (value instanceof String) {
            final String string = ((String) value).trim();
            if (string.isEmpty()) {
                return null;
            }

            try {
                if (format == null) {
                    return new Timestamp(Long.parseLong(string));
                }

                final DateFormat dateFormat = format.get();
                if (dateFormat == null) {
                    return new Timestamp(Long.parseLong(string));
                }

                /* Removed
                    final java.util.Date utilDate = dateFormat.parse(string);
                    return new Timestamp(utilDate.getTime());
                */

                // FIXED
                final java.util.Date utilDate = dateFormat.parse(string);
                Timestamp timestamp = new Timestamp(utilDate.getTime());
                LocalDateTime localDateTime = timestamp.toLocalDateTime();
                LocalDateTime plus = localDateTime.plus(Duration.ofHours(9)); // Local Time으로 9을 +한다. Kudu는 기본을 GMT로 본다.
                return Timestamp.valueOf(plus);
            } catch (final ParseException e) {
                final DateFormat dateFormat = format.get();
                final String formatDescription;
                if (dateFormat == null) {
                    formatDescription = "Numeric";
                } else if (dateFormat instanceof SimpleDateFormat) {
                    formatDescription = ((SimpleDateFormat) dateFormat).toPattern();
                } else {
                    formatDescription = dateFormat.toString();
                }

                throw new IllegalTypeConversionException("Could not convert value [" + value
                        + "] of type java.lang.String to Timestamp for field " + fieldName + " because the value is not in the expected date format: "
                        + formatDescription);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Timestamp for field " + fieldName);
    }

    public static boolean isTimestampTypeCompatible(final Object value, final String format) {
        return isDateTypeCompatible(value, format);
    }


    public static BigInteger toBigInt(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }

        if (value instanceof Number) {
            return BigInteger.valueOf(((Number) value).longValue());
        }

        if (value instanceof String) {
            try {
                return new BigInteger((String) value);
            } catch (NumberFormatException nfe) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigInteger for field " + fieldName
                        + ", value is not a valid representation of BigInteger", nfe);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigInteger for field " + fieldName);
    }

    public static boolean isBigIntTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, DataTypeUtils::isIntegral);
    }

    public static boolean isDecimalTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, DataTypeUtils::isDecimal);
    }

    public static Boolean toBoolean(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            final String string = (String) value;
            if (string.equalsIgnoreCase("true")) {
                return Boolean.TRUE;
            } else if (string.equalsIgnoreCase("false")) {
                return Boolean.FALSE;
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Boolean for field " + fieldName);
    }

    public static boolean isBooleanTypeCompatible(final Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return true;
        }
        if (value instanceof String) {
            final String string = (String) value;
            return string.equalsIgnoreCase("true") || string.equalsIgnoreCase("false");
        }
        return false;
    }

    public static BigDecimal toBigDecimal(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }

        if (value instanceof Number) {
            final Number number = (Number) value;

            if (number instanceof Byte
                    || number instanceof Short
                    || number instanceof Integer
                    || number instanceof Long) {
                return BigDecimal.valueOf(number.longValue());
            }

            if (number instanceof BigInteger) {
                return new BigDecimal((BigInteger) number);
            }

            if (number instanceof Float) {
                return new BigDecimal(Float.toString((Float) number));
            }

            if (number instanceof Double) {
                return new BigDecimal(Double.toString((Double) number));
            }
        }

        if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException nfe) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigDecimal for field " + fieldName
                        + ", value is not a valid representation of BigDecimal", nfe);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigDecimal for field " + fieldName);
    }

    public static Double toDouble(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Double for field " + fieldName);
    }

    public static boolean isDoubleTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isDouble(s));
    }

    private static boolean isNumberTypeCompatible(final Object value, final Predicate<String> stringPredicate) {
        if (value == null) {
            return false;
        }

        if (value instanceof Number) {
            return true;
        }

        if (value instanceof String) {
            return stringPredicate.test((String) value);
        }

        return false;
    }

    public static Float toFloat(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }

        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Float for field " + fieldName);
    }

    public static boolean isFloatTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isFloatingPoint(s));
    }

    private static boolean isDecimal(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        return DECIMAL_PATTERN.matcher(value).matches();
    }

    private static boolean isFloatingPoint(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        if (!FLOATING_POINT_PATTERN.matcher(value).matches()) {
            return false;
        }

        // Just to ensure that the exponents are in range, etc.
        try {
            Float.parseFloat(value);
        } catch (final NumberFormatException nfe) {
            return false;
        }

        return true;
    }

    private static boolean isDouble(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        if (!FLOATING_POINT_PATTERN.matcher(value).matches()) {
            return false;
        }

        // Just to ensure that the exponents are in range, etc.
        try {
            Double.parseDouble(value);
        } catch (final NumberFormatException nfe) {
            return false;
        }

        return true;
    }

    public static Long toLong(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof String) {
            return Long.parseLong((String) value);
        }

        if (value instanceof java.util.Date) {
            return ((java.util.Date) value).getTime();
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Long for field " + fieldName);
    }

    public static boolean isLongTypeCompatible(final Object value) {
        if (value == null) {
            return false;
        }

        if (value instanceof Number) {
            return true;
        }

        if (value instanceof java.util.Date) {
            return true;
        }

        if (value instanceof String) {
            return isIntegral((String) value, Long.MIN_VALUE, Long.MAX_VALUE);
        }

        return false;
    }

    /**
     * 값이 정수 형태인지 확인합니다.
     */
    private static boolean isIntegral(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        int initialPosition = 0;
        final char firstChar = value.charAt(0);
        if (firstChar == '+' || firstChar == '-') {
            initialPosition = 1;

            if (value.length() == 1) {
                return false;
            }
        }

        for (int i = initialPosition; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    /**
     * 값이 지정한 범위 내의 정수 형태인지 확인합니다.
     */
    private static boolean isIntegral(final String value, final long minValue, final long maxValue) {

        if (!isIntegral(value)) {
            return false;
        }

        try {
            final long longValue = Long.parseLong(value);
            return longValue >= minValue && longValue <= maxValue;
        } catch (final NumberFormatException nfe) {
            // In case the value actually exceeds the max value of a Long
            return false;
        }
    }

    public static Integer toInteger(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            try {
                return Math.toIntExact(((Number) value).longValue());
            } catch (ArithmeticException ae) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Integer for field " + fieldName
                        + " as it causes an arithmetic overflow (the value is too large, e.g.)", ae);
            }
        }

        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Integer for field " + fieldName);
    }

    public static boolean isIntegerTypeCompatible(final Object value) {
        if (value instanceof Number) {
            try {
                Math.toIntExact(((Number) value).longValue());
                return true;
            } catch (ArithmeticException ae) {
                return false;
            }
        }
        return isNumberTypeCompatible(value, s -> isIntegral(s, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }


    public static Short toShort(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }

        if (value instanceof String) {
            return Short.parseShort((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Short for field " + fieldName);
    }

    public static boolean isShortTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isIntegral(s, Short.MIN_VALUE, Short.MAX_VALUE));
    }

    public static Byte toByte(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }

        if (value instanceof String) {
            return Byte.parseByte((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Byte for field " + fieldName);
    }

    public static boolean isByteTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isIntegral(s, Byte.MIN_VALUE, Byte.MAX_VALUE));
    }


    public static Character toCharacter(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Character) {
            return ((Character) value);
        }

        if (value instanceof CharSequence) {
            final CharSequence charSeq = (CharSequence) value;
            if (charSeq.length() == 0) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                        + " to Character because it has a length of 0 for field " + fieldName);
            }

            return charSeq.charAt(0);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Character for field " + fieldName);
    }

    public static boolean isCharacterTypeCompatible(final Object value) {
        return value != null && (value instanceof Character || (value instanceof CharSequence && ((CharSequence) value).length() > 0));
    }

    /**
     * 두 RecordSchema를 병합합니다. 필드명(및 별칭)이 같은 필드는 merge(RecordField, RecordField)로 병합하고,
     * 한쪽에만 존재하는 필드는 결과 스키마에 그대로 추가합니다. 스키마 드리프트(누락 컬럼) 처리 등에 활용됩니다.
     */
    public static RecordSchema merge(final RecordSchema thisSchema, final RecordSchema otherSchema) {
        if (thisSchema == null) {
            return otherSchema;
        }
        if (otherSchema == null) {
            return thisSchema;
        }
        if (thisSchema == otherSchema) {
            return thisSchema;
        }

        final List<RecordField> otherFields = otherSchema.getFields();
        if (otherFields.isEmpty()) {
            return thisSchema;
        }

        final List<RecordField> thisFields = thisSchema.getFields();
        if (thisFields.isEmpty()) {
            return otherSchema;
        }

        final Map<String, Integer> fieldIndices = new HashMap<>();
        final List<RecordField> fields = new ArrayList<>();
        for (int i = 0; i < thisFields.size(); i++) {
            final RecordField field = thisFields.get(i);

            final Integer index = Integer.valueOf(i);

            fieldIndices.put(field.getFieldName(), index);
            for (final String alias : field.getAliases()) {
                fieldIndices.put(alias, index);
            }

            fields.add(field);
        }

        for (final RecordField otherField : otherFields) {
            Integer fieldIndex = fieldIndices.get(otherField.getFieldName());

            // Find the field in 'thisSchema' that corresponds to 'otherField',
            // if one exists.
            if (fieldIndex == null) {
                for (final String alias : otherField.getAliases()) {
                    fieldIndex = fieldIndices.get(alias);
                    if (fieldIndex != null) {
                        break;
                    }
                }
            }

            // If there is no field with the same name then just add 'otherField'.
            if (fieldIndex == null) {
                fields.add(otherField);
                continue;
            }

            // Merge the two fields, if necessary
            final RecordField thisField = fields.get(fieldIndex);
            if (isMergeRequired(thisField, otherField)) {
                final RecordField mergedField = merge(thisField, otherField);
                fields.set(fieldIndex, mergedField);
            }
        }

        return new SimpleRecordSchema(fields);
    }


    private static boolean isMergeRequired(final RecordField thisField, final RecordField otherField) {
        if (!thisField.getDataType().equals(otherField.getDataType())) {
            return true;
        }

        if (!thisField.getAliases().equals(otherField.getAliases())) {
            return true;
        }

        return !Objects.equals(thisField.getDefaultValue(), otherField.getDefaultValue());
    }

    public static RecordField merge(final RecordField thisField, final RecordField otherField) {
        final String fieldName = thisField.getFieldName();
        final Set<String> aliases = new HashSet<>();
        aliases.addAll(thisField.getAliases());
        aliases.addAll(otherField.getAliases());

        final Object defaultValue;
        if (thisField.getDefaultValue() == null && otherField.getDefaultValue() != null) {
            defaultValue = otherField.getDefaultValue();
        } else {
            defaultValue = thisField.getDefaultValue();
        }

        final DataType dataType = mergeDataTypes(thisField.getDataType(), otherField.getDataType());
        return new RecordField(fieldName, dataType, defaultValue, aliases, thisField.isNullable() || otherField.isNullable());
    }

    /**
     * 두 DataType을 병합합니다. 동일하면 그대로 반환하고, 한쪽이 다른 쪽을 포괄하는 '더 넓은(wider)' 타입이면
     * (예: INT와 LONG일 때 LONG) 그 넓은 타입을 반환합니다. 그렇지 않으면 두 타입을 모두 포함하는 CHOICE 타입을 생성합니다.
     */
    public static DataType mergeDataTypes(final DataType thisDataType, final DataType otherDataType) {
        if (thisDataType == null) {
            return otherDataType;
        }

        if (otherDataType == null) {
            return thisDataType;
        }

        if (thisDataType.equals(otherDataType)) {
            return thisDataType;
        } else {
            // If one type is 'wider' than the other (such as an INT and a LONG), just use the wider type (LONG, in this case),
            // rather than using a CHOICE of the two.
            final Optional<DataType> widerType = getWiderType(thisDataType, otherDataType);
            if (widerType.isPresent()) {
                return widerType.get();
            }

            final DataTypeSet dataTypeSet = new DataTypeSet();
            dataTypeSet.add(thisDataType);
            dataTypeSet.add(otherDataType);

            final List<DataType> possibleChildTypes = dataTypeSet.getTypes();
            possibleChildTypes.sort(Comparator.comparing(DataType::getFieldType));

            return RecordFieldType.CHOICE.getChoiceDataType(possibleChildTypes);
        }
    }

    /**
     * 두 DataType 중 정밀도 손실 없이 다른 하나를 포괄할 수 있는 '더 넓은(wider)' 타입을 찾습니다.
     * 정수형끼리는 크기가 큰 쪽을, 정수-실수/DECIMAL 조합은 정해진 우선순위 규칙에 따라 판단합니다.
     * 포괄 관계가 없으면 빈 Optional을 반환합니다.
     */
    public static Optional<DataType> getWiderType(final DataType thisDataType, final DataType otherDataType) {
        if (thisDataType == null) {
            return Optional.ofNullable(otherDataType);
        }
        if (otherDataType == null) {
            return Optional.of(thisDataType);
        }

        final RecordFieldType thisFieldType = thisDataType.getFieldType();
        final RecordFieldType otherFieldType = otherDataType.getFieldType();

        final int thisIntTypeValue = getIntegerTypeValue(thisFieldType);
        final int otherIntTypeValue = getIntegerTypeValue(otherFieldType);
        final boolean thisIsInt = thisIntTypeValue > -1;
        final boolean otherIsInt = otherIntTypeValue > -1;

        if (thisIsInt && otherIsInt) {
            if (thisIntTypeValue > otherIntTypeValue) {
                return Optional.of(thisDataType);
            }

            return Optional.of(otherDataType);
        }

        final boolean otherIsDecimal = isDecimalType(otherFieldType);

        switch (thisFieldType) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                if (otherIsDecimal) {
                    return Optional.of(otherDataType);
                }
                break;
            case FLOAT:
                if (otherFieldType == RecordFieldType.DOUBLE || otherFieldType == RecordFieldType.DECIMAL) {
                    return Optional.of(otherDataType);
                }
                if (otherFieldType == RecordFieldType.BYTE || otherFieldType == RecordFieldType.SHORT || otherFieldType == RecordFieldType.INT || otherFieldType == RecordFieldType.LONG) {
                    return Optional.of(thisDataType);
                }
                break;
            case DOUBLE:
                if (otherFieldType == RecordFieldType.DECIMAL) {
                    return Optional.of(otherDataType);
                }
                if (otherFieldType == RecordFieldType.BYTE || otherFieldType == RecordFieldType.SHORT || otherFieldType == RecordFieldType.INT || otherFieldType == RecordFieldType.LONG
                        || otherFieldType == RecordFieldType.FLOAT) {

                    return Optional.of(thisDataType);
                }
                break;
            case DECIMAL:
                if (otherFieldType == RecordFieldType.DOUBLE || otherFieldType == RecordFieldType.FLOAT || otherIsInt) {
                    return Optional.of(thisDataType);
                } else if (otherFieldType == RecordFieldType.DECIMAL) {
                    final DecimalDataType thisDecimalDataType = (DecimalDataType) thisDataType;
                    final DecimalDataType otherDecimalDataType = (DecimalDataType) otherDataType;

                    final int precision = Math.max(thisDecimalDataType.getPrecision(), otherDecimalDataType.getPrecision());
                    final int scale = Math.max(thisDecimalDataType.getScale(), otherDecimalDataType.getScale());
                    return Optional.of(RecordFieldType.DECIMAL.getDecimalDataType(precision, scale));
                }
                break;
            case CHAR:
            case UUID:
                if (otherFieldType == RecordFieldType.STRING) {
                    return Optional.of(otherDataType);
                }
                break;
            case STRING:
                if (otherFieldType == RecordFieldType.CHAR || otherFieldType == RecordFieldType.UUID) {
                    return Optional.of(thisDataType);
                }
                break;
        }

        return Optional.empty();
    }

    private static boolean isDecimalType(final RecordFieldType fieldType) {
        switch (fieldType) {
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    private static int getIntegerTypeValue(final RecordFieldType fieldType) {
        switch (fieldType) {
            case BIGINT:
                return 4;
            case LONG:
                return 3;
            case INT:
                return 2;
            case SHORT:
                return 1;
            case BYTE:
                return 0;
            default:
                return -1;
        }
    }

    /**
     * 지정한 필드 데이터 타입을 java.sql.Types 상수(예: INTEGER = 4)로 변환합니다.
     *
     * @param dataType 변환할 DataType
     * @return 지정한 RecordFieldType에 대응하는 SQL 타입
     */
    public static int getSQLTypeValue(final DataType dataType) {
        if (dataType == null) {
            return Types.NULL;
        }
        RecordFieldType fieldType = dataType.getFieldType();
        switch (fieldType) {
            case BIGINT:
            case LONG:
                return Types.BIGINT;
            case BOOLEAN:
                return Types.BOOLEAN;
            case BYTE:
                return Types.TINYINT;
            case CHAR:
                return Types.CHAR;
            case DATE:
                return Types.DATE;
            case DOUBLE:
                return Types.DOUBLE;
            case FLOAT:
                return Types.FLOAT;
            case DECIMAL:
                return Types.NUMERIC;
            case INT:
                return Types.INTEGER;
            case SHORT:
                return Types.SMALLINT;
            case STRING:
                return Types.VARCHAR;
            case TIME:
                return Types.TIME;
            case TIMESTAMP:
                return Types.TIMESTAMP;
            case ARRAY:
                return Types.ARRAY;
            case MAP:
            case RECORD:
                return Types.STRUCT;
            case CHOICE:
                throw new IllegalTypeConversionException("Cannot convert CHOICE, type must be explicit");
            default:
                throw new IllegalTypeConversionException("Cannot convert unknown type " + fieldType.name());
        }
    }

    /**
     * 지정한 java.sql.Types 상수 필드 데이터 타입(예: INTEGER = 4)을 DataType으로 변환합니다.
     *
     * @param sqlType 변환할 SQL 타입
     * @return 지정한 SQL 타입에 대응하는 RecordFieldType의 DataType
     */
    public static DataType getDataTypeFromSQLTypeValue(final int sqlType) {
        switch (sqlType) {
            case Types.BIGINT:
                return RecordFieldType.BIGINT.getDataType();
            case Types.BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case Types.TINYINT:
                return RecordFieldType.BYTE.getDataType();
            case Types.DATE:
                return RecordFieldType.DATE.getDataType();
            case Types.DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case Types.FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case Types.NUMERIC:
                return RecordFieldType.DECIMAL.getDataType();
            case Types.INTEGER:
                return RecordFieldType.INT.getDataType();
            case Types.SMALLINT:
                return RecordFieldType.SHORT.getDataType();
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.OTHER:
            case Types.SQLXML:
            case Types.CLOB:
                return RecordFieldType.STRING.getDataType();
            case Types.TIME:
                return RecordFieldType.TIME.getDataType();
            case Types.TIMESTAMP:
                return RecordFieldType.TIMESTAMP.getDataType();
            case Types.ARRAY:
                return RecordFieldType.ARRAY.getDataType();
            case Types.BINARY:
            case Types.BLOB:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case Types.STRUCT:
                return RecordFieldType.RECORD.getDataType();
            default:
                return null;
        }
    }

    public static boolean isScalarValue(final DataType dataType, final Object value) {
        final RecordFieldType fieldType = dataType.getFieldType();

        final RecordFieldType chosenType;
        if (fieldType == RecordFieldType.CHOICE) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            final DataType chosenDataType = chooseDataType(value, choiceDataType);
            if (chosenDataType == null) {
                return false;
            }

            chosenType = chosenDataType.getFieldType();
        } else {
            chosenType = fieldType;
        }

        switch (chosenType) {
            case ARRAY:
            case MAP:
            case RECORD:
                return false;
        }

        return true;
    }

    public static Charset getCharset(String charsetName) {
        if (charsetName == null) {
            return StandardCharsets.UTF_8;
        } else {
            return Charset.forName(charsetName);
        }
    }

    /**
     * 주어진 값이 정수(Integer)이며 정밀도 손실 없이 float 변수에 담길 수 있으면 true를 반환합니다.
     * 이는 입력값의 수치와 float에서 사용하는 유효 바이트 수를 기준으로 판단합니다.
     *
     * @param value 확인할 값
     * @return 조건을 만족하면 true, 그렇지 않으면 false
     */
    public static boolean isIntegerFitsToFloat(final Object value) {
        if (!(value instanceof Integer)) {
            return false;
        }

        final int intValue = (Integer) value;
        return MIN_GUARANTEED_PRECISE_WHOLE_IN_FLOAT <= intValue && intValue <= MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT;
    }

    /**
     * 주어진 값이 long이며 정밀도 손실 없이 float 변수에 담길 수 있으면 true를 반환합니다.
     * 이는 입력값의 수치와 float에서 사용하는 유효 바이트 수를 기준으로 판단합니다.
     *
     * @param value 확인할 값
     * @return 조건을 만족하면 true, 그렇지 않으면 false
     */
    public static boolean isLongFitsToFloat(final Object value) {
        if (!(value instanceof Long)) {
            return false;
        }

        final long longValue = (Long) value;
        return MIN_GUARANTEED_PRECISE_WHOLE_IN_FLOAT <= longValue && longValue <= MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT;
    }

    /**
     * 주어진 값이 long이며 정밀도 손실 없이 double 변수에 담길 수 있으면 true를 반환합니다.
     * 이는 입력값의 수치와 double에서 사용하는 유효 바이트 수를 기준으로 판단합니다.
     *
     * @param value 확인할 값
     * @return 조건을 만족하면 true, 그렇지 않으면 false
     */
    public static boolean isLongFitsToDouble(final Object value) {
        if (!(value instanceof Long)) {
            return false;
        }

        final long longValue = (Long) value;
        return MIN_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE <= longValue && longValue <= MAX_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE;
    }

    /**
     * 주어진 값이 BigInteger이며 정밀도 손실 없이 float 변수에 담길 수 있으면 true를 반환합니다.
     * 이는 입력값의 수치와 float에서 사용하는 유효 바이트 수를 기준으로 판단합니다.
     *
     * @param value 확인할 값
     * @return 조건을 만족하면 true, 그렇지 않으면 false
     */
    public static boolean isBigIntFitsToFloat(final Object value) {
        if (!(value instanceof BigInteger)) {
            return false;
        }

        final BigInteger bigIntValue = (BigInteger) value;
        return bigIntValue.compareTo(MIN_FLOAT_VALUE_IN_BIGINT) >= 0 && bigIntValue.compareTo(MAX_FLOAT_VALUE_IN_BIGINT) <= 0;
    }

    /**
     * 주어진 값이 BigInteger이며 정밀도 손실 없이 double 변수에 담길 수 있으면 true를 반환합니다.
     * 이는 입력값의 수치와 double에서 사용하는 유효 바이트 수를 기준으로 판단합니다.
     *
     * @param value 확인할 값
     * @return 조건을 만족하면 true, 그렇지 않으면 false
     */
    public static boolean isBigIntFitsToDouble(final Object value) {
        if (!(value instanceof BigInteger)) {
            return false;
        }

        final BigInteger bigIntValue = (BigInteger) value;
        return bigIntValue.compareTo(MIN_DOUBLE_VALUE_IN_BIGINT) >= 0 && bigIntValue.compareTo(MAX_DOUBLE_VALUE_IN_BIGINT) <= 0;
    }

    /**
     * 입력값이 double이며 float 변수 타입의 범위 내에 있는 경우 true를 반환합니다.
     *
     * <p>
     * 참고: 이 메서드는 값의 범위만 고려하며 정밀도는 고려하지 않습니다. 이 시점에서는 double 표현이
     * 원본 텍스트 값과 이미 미세하게 다를 수 있기 때문입니다.
     * </p>
     *
     * @param value 확인할 값
     * @return double 값이 float 자료형 범위에 들어가면 true
     */
    public static boolean isDoubleWithinFloatInterval(final Object value) {

        if (!(value instanceof Double)) {
            return false;
        }

        final Double doubleValue = (Double) value;
        return MIN_FLOAT_VALUE_IN_DOUBLE <= doubleValue && doubleValue <= MAX_FLOAT_VALUE_IN_DOUBLE;
    }

    /**
     * 입력값이 주어진 (숫자) 타입 또는 그보다 좁은(narrow) 데이터 타입 중 하나의 요구사항을 만족하는지 확인합니다.
     *
     * @param value     입력값
     * @param fieldType 기대하는 필드 타입
     * @return 입력값이 좁은 데이터 타입 중 하나라도 만족하면 true, 그렇지 않으면 false. 숫자형 데이터 타입만 지원합니다.
     */
    public static boolean isFittingNumberType(final Object value, final RecordFieldType fieldType) {
        if (NUMERIC_VALIDATORS.get(fieldType).test(value)) {
            return true;
        }

        for (final RecordFieldType recordFieldType : fieldType.getNarrowDataTypes()) {
            if (NUMERIC_VALIDATORS.get(recordFieldType).test(value)) {
                return true;
            }
        }

        return false;
    }
}