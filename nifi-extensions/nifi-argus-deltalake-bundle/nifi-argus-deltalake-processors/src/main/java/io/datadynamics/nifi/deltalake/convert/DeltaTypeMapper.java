/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.deltalake.convert;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.DecimalDataType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * NiFi RecordSchema/값 ↔ Delta Kernel 타입/표현 변환기.
 *
 * <p>MVP 범위: 원시 타입(문자열/정수/실수/불리언/십진수/날짜/타임스탬프/이진)만 지원한다.
 * ARRAY/RECORD/MAP/CHOICE 같은 중첩 타입은 명시적으로 거부한다(후속 과제).</p>
 *
 * <p>Delta 물리 표현 규약: DATE는 epoch 이후 <b>일수(int)</b>, TIMESTAMP는 epoch 이후
 * <b>마이크로초(long, UTC)</b>로 저장한다. 이 클래스가 coerce 단계에서 그 표현으로 정규화한다.</p>
 */
public final class DeltaTypeMapper {

    private DeltaTypeMapper() {
    }

    /** NiFi 레코드 스키마 전체를 Delta StructType으로 변환한다(테이블 최초 생성 시 사용). */
    public static StructType toDeltaSchema(final RecordSchema schema) {
        StructType struct = new StructType();
        for (final RecordField field : schema.getFields()) {
            struct = struct.add(new StructField(field.getFieldName(), toDeltaType(field.getDataType()), field.isNullable()));
        }
        return struct;
    }

    /** 단일 NiFi 필드 타입을 Delta 타입으로 매핑한다. */
    public static DataType toDeltaType(final org.apache.nifi.serialization.record.DataType nifiType) {
        final RecordFieldType fieldType = nifiType.getFieldType();
        switch (fieldType) {
            case STRING:
            case CHAR:
            case ENUM:
            case UUID:
            case TIME: // Delta에는 TIME 타입이 없어 ISO 문자열로 저장한다.
                return StringType.STRING;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BYTE:
                return ByteType.BYTE;
            case SHORT:
                return ShortType.SHORT;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
            case BIGINT:
                return LongType.LONG;
            case FLOAT:
                return FloatType.FLOAT;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case DECIMAL:
                final DecimalDataType decimal = (DecimalDataType) nifiType;
                return new DecimalType(decimal.getPrecision(), decimal.getScale());
            case DATE:
                return DateType.DATE;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP;
            case ARRAY:
            case RECORD:
            case MAP:
            case CHOICE:
            default:
                throw new IllegalArgumentException(
                        "Delta Lake 싱크(MVP)가 지원하지 않는 필드 타입: " + fieldType
                                + ". 원시 타입만 지원합니다(중첩/컬렉션 타입은 후속 과제).");
        }
    }

    /**
     * 원시 값을 Delta 컬럼 벡터가 반환할 정규 표현으로 변환한다.
     * DATE→Integer(일수), TIMESTAMP→Long(마이크로초), 그 외는 대응 Java 박싱 타입.
     */
    public static Object coerce(final DataType deltaType, final Object raw) {
        if (raw == null) {
            return null;
        }
        if (deltaType instanceof StringType) {
            return raw.toString();
        }
        if (deltaType instanceof BooleanType) {
            return toBoolean(raw);
        }
        if (deltaType instanceof ByteType) {
            return (byte) toLong(raw);
        }
        if (deltaType instanceof ShortType) {
            return (short) toLong(raw);
        }
        if (deltaType instanceof IntegerType) {
            return (int) toLong(raw);
        }
        if (deltaType instanceof LongType) {
            return toLong(raw);
        }
        if (deltaType instanceof FloatType) {
            return (float) toDouble(raw);
        }
        if (deltaType instanceof DoubleType) {
            return toDouble(raw);
        }
        if (deltaType instanceof DecimalType) {
            final DecimalType dt = (DecimalType) deltaType;
            return toBigDecimal(raw).setScale(dt.getScale(), BigDecimal.ROUND_HALF_UP);
        }
        if (deltaType instanceof DateType) {
            return dateToEpochDay(raw);
        }
        if (deltaType instanceof TimestampType) {
            return timestampToEpochMicros(raw);
        }
        if (deltaType instanceof BinaryType) {
            return toBytes(raw);
        }
        throw new IllegalArgumentException("지원하지 않는 Delta 타입: " + deltaType);
    }

    /** 파티션 컬럼 값(coerce된 표현)을 Delta Literal로 변환한다. */
    public static Literal toLiteral(final DataType deltaType, final Object coerced) {
        if (coerced == null) {
            return Literal.ofNull(deltaType);
        }
        if (deltaType instanceof StringType) {
            return Literal.ofString((String) coerced);
        }
        if (deltaType instanceof BooleanType) {
            return Literal.ofBoolean((Boolean) coerced);
        }
        if (deltaType instanceof ByteType) {
            return Literal.ofByte((Byte) coerced);
        }
        if (deltaType instanceof ShortType) {
            return Literal.ofShort((Short) coerced);
        }
        if (deltaType instanceof IntegerType) {
            return Literal.ofInt((Integer) coerced);
        }
        if (deltaType instanceof LongType) {
            return Literal.ofLong((Long) coerced);
        }
        if (deltaType instanceof FloatType) {
            return Literal.ofFloat((Float) coerced);
        }
        if (deltaType instanceof DoubleType) {
            return Literal.ofDouble((Double) coerced);
        }
        if (deltaType instanceof DecimalType) {
            final DecimalType dt = (DecimalType) deltaType;
            return Literal.ofDecimal((BigDecimal) coerced, dt.getPrecision(), dt.getScale());
        }
        if (deltaType instanceof DateType) {
            return Literal.ofDate((Integer) coerced);
        }
        if (deltaType instanceof TimestampType) {
            return Literal.ofTimestamp((Long) coerced);
        }
        if (deltaType instanceof BinaryType) {
            return Literal.ofBinary((byte[]) coerced);
        }
        throw new IllegalArgumentException("파티션 컬럼으로 지원하지 않는 Delta 타입: " + deltaType);
    }

    // --- 값 변환 헬퍼 ---

    private static boolean toBoolean(final Object raw) {
        if (raw instanceof Boolean) {
            return (Boolean) raw;
        }
        return Boolean.parseBoolean(raw.toString());
    }

    private static long toLong(final Object raw) {
        if (raw instanceof Number) {
            return ((Number) raw).longValue();
        }
        return Long.parseLong(raw.toString().trim());
    }

    private static double toDouble(final Object raw) {
        if (raw instanceof Number) {
            return ((Number) raw).doubleValue();
        }
        return Double.parseDouble(raw.toString().trim());
    }

    private static BigDecimal toBigDecimal(final Object raw) {
        if (raw instanceof BigDecimal) {
            return (BigDecimal) raw;
        }
        if (raw instanceof Number) {
            return new BigDecimal(raw.toString());
        }
        return new BigDecimal(raw.toString().trim());
    }

    private static byte[] toBytes(final Object raw) {
        if (raw instanceof byte[]) {
            return (byte[]) raw;
        }
        return raw.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static int dateToEpochDay(final Object raw) {
        if (raw instanceof LocalDate) {
            return (int) ((LocalDate) raw).toEpochDay();
        }
        if (raw instanceof java.sql.Date) {
            return (int) ((java.sql.Date) raw).toLocalDate().toEpochDay();
        }
        if (raw instanceof java.util.Date) {
            return (int) ((java.util.Date) raw).toInstant().atZone(ZoneOffset.UTC).toLocalDate().toEpochDay();
        }
        if (raw instanceof Number) {
            // epoch 이후 일수로 간주.
            return ((Number) raw).intValue();
        }
        return (int) LocalDate.parse(raw.toString()).toEpochDay();
    }

    private static long timestampToEpochMicros(final Object raw) {
        if (raw instanceof Timestamp) {
            return instantToMicros(((Timestamp) raw).toInstant());
        }
        if (raw instanceof java.util.Date) {
            return instantToMicros(((java.util.Date) raw).toInstant());
        }
        if (raw instanceof Instant) {
            return instantToMicros((Instant) raw);
        }
        if (raw instanceof LocalDateTime) {
            return instantToMicros(((LocalDateTime) raw).toInstant(ZoneOffset.UTC));
        }
        if (raw instanceof Number) {
            // epoch 이후 밀리초로 간주.
            return ((Number) raw).longValue() * 1_000L;
        }
        return instantToMicros(Instant.parse(raw.toString()));
    }

    private static long instantToMicros(final Instant instant) {
        return Math.multiplyExact(instant.getEpochSecond(), 1_000_000L) + instant.getNano() / 1_000L;
    }
}
