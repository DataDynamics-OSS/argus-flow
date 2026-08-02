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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-database-utils/src/main/java/org/apache/nifi/util/db/JdbcCommon.java
 */
package io.datadynamics.nifi.processors.db.executesql;


import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.NullDefault;
import org.apache.avro.SchemaBuilder.UnionAccumulator;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.Base64;
import java.util.HexFormat;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLXML;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.sql.Types.*;

/**
 * JDBC / SQL 공통 유틸리티 클래스.
 * <p>
 * JDBC {@link ResultSet}을 순회하면서 Avro {@link Schema}를 생성하고, 각 로우(row)의 값을
 * Avro {@link GenericRecord}로 변환하여 스트림에 기록하는 핵심 로직을 담당한다.
 * 또한 반대 방향으로, FlowFile 속성(attribute)에 담긴 문자열 값을 적절한 JDBC 타입으로
 * 변환하여 {@link PreparedStatement}의 파라미터로 바인딩하는 기능도 제공한다.
 * <p>
 * SQL 타입(java.sql.Types)과 Avro 타입 간의 매핑은 데이터베이스 벤더(Oracle, MS SQL 등)별
 * 고유 타입 코드까지 고려해야 하므로 다소 복잡하다. 각 분기의 의도는 메서드 내부 주석을 참고할 것.
 */
public class JdbcCommon {

    // BIGINT(19자리)가 Java long 범위를 넘는지 판단하는 기준 자릿수.
    // 이 자릿수를 초과하면 Avro long 타입으로 표현할 수 없으므로 문자열로 변환한다.
    public static final int MAX_DIGITS_IN_BIGINT = 19;
    // INTEGER 값이 Avro int(4바이트) 범위 안에 들어가는지 판단하는 기준 자릿수.
    public static final int MAX_DIGITS_IN_INT = 9;
    // Derived from MySQL default precision.
    // DECIMAL/NUMERIC 컬럼의 precision 정보를 드라이버가 제공하지 않을 때 사용할 기본 precision(MySQL 기본값 기준).
    public static final int DEFAULT_PRECISION_VALUE = 10;
    // DECIMAL/NUMERIC 컬럼의 scale 정보를 드라이버가 제공하지 않을 때 사용할 기본 scale.
    public static final int DEFAULT_SCALE_VALUE = 0;

    // 정수 문자열(부호 포함 최대 19자리, 즉 long 범위)을 판별하기 위한 정규식.
    // epoch millis 형태의 숫자 문자열인지, 아니면 날짜/시각 포맷 문자열인지 구분하는 데 사용된다.
    public static final Pattern LONG_PATTERN = Pattern.compile("^-?\\d{1,19}$");
    // FlowFile 속성 중 "sql.args.<index>.type" 형태(파라미터 바인딩용 SQL 타입 지정 속성)를 찾기 위한 정규식.
    public static final Pattern SQL_TYPE_ATTRIBUTE_PATTERN = Pattern.compile("sql\\.args\\.(\\d+)\\.type");
    // sql.args.N.type 속성 값이 숫자(SQL 타입 코드)로만 구성되어 있는지 검증하기 위한 정규식.
    public static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");

    public static final String MIME_TYPE_AVRO_BINARY = "application/avro-binary";
    // 민감(sensitive) 값을 로그에 출력할 때 실제 값 대신 표시할 마스킹 문자열.
    public static final String MASKED_LOG_VALUE = "MASKED VALUE";

    // 아래 4개의 convertToAvroStream 오버로드는 모두 마지막의 완전한 버전으로 위임되는 편의 메서드다.
    // 기본값(레코드 이름 없음, 콜백 없음, 최대 행 수 제한 없음, logical type 미사용)을 채워 넣는 역할만 한다.
    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, boolean convertNames) throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, null, null, convertNames);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, boolean convertNames)
            throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, recordName, null, convertNames);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback, boolean convertNames)
            throws IOException, SQLException {
        return convertToAvroStream(rs, outStream, recordName, callback, 0, convertNames);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback, final int maxRows, boolean convertNames)
            throws SQLException, IOException {
        final AvroConversionOptions options = AvroConversionOptions.builder()
                .recordName(recordName)
                .maxRows(maxRows)
                .convertNames(convertNames)
                .useLogicalTypes(false).build();
        return convertToAvroStream(rs, outStream, options, callback);
    }

    // ResultSet에 로우가 하나도 없을 때(또는 결과 집합 자체가 없을 때) 필드가 없는 빈 Avro 레코드
    // 스키마로 빈 Avro 스트림을 생성한다. 다운스트림에서 항상 유효한 Avro 파일을 기대하는 경우를 위한 안전장치.
    public static void createEmptyAvroStream(final OutputStream outStream) throws IOException {
        final FieldAssembler<Schema> builder = SchemaBuilder.record("NiFi_ExecuteSQL_Record").namespace("any.data").fields();
        final Schema schema = builder.endRecord();

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);
        }
    }

    /**
     * ResultSet의 전체 로우를 순회하며 Avro 바이너리 스트림으로 직렬화하는 핵심 메서드.
     * <p>
     * 처리 순서:
     * 1) {@link #createSchema}로 컬럼 메타데이터 기반 Avro 스키마를 먼저 생성한다.
     * 2) 컬럼마다 JDBC 타입(java.sql.Types)에 따라 적절한 getter(getClob/getBlob/getTimestamp 등)를
     *    호출하여 값을 읽고, Avro가 이해할 수 있는 자바 타입으로 변환하여 GenericRecord에 채운다.
     * 3) 콜백이 있으면 각 로우 처리 시점에 호출하고, maxRows에 도달하면 조기 종료한다.
     * <p>
     * CLOB/BLOB는 ResultSet의 "최대 이식성(maximum portability)" 규약상 getObject() 호출 전에
     * 별도로 처리해야 하므로 타입 분기 맨 앞에서 먼저 처리한다.
     */
    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, final AvroConversionOptions options, final ResultSetRowCallback callback) throws SQLException, IOException {
        final Schema schema = createSchema(rs, options);
        final GenericRecord rec = new GenericData.Record(schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.setCodec(options.codec);
            dataFileWriter.create(schema, outStream);

            final ResultSetMetaData meta = rs.getMetaData();
            final int nrOfColumns = meta.getColumnCount();
            long nrOfRows = 0;
            while (rs.next()) {
                if (callback != null) {
                    callback.processRow(rs);
                }
                for (int i = 1; i <= nrOfColumns; i++) {
                    final int javaSqlType = meta.getColumnType(i);
                    final Schema fieldSchema = schema.getFields().get(i - 1).schema();

                    // Need to handle CLOB and BLOB before getObject() is called, due to ResultSet's maximum portability statement
                    // CLOB/NCLOB은 getObject()를 호출하기 전에 getClob()으로 먼저 읽어야 이식성이 보장된다.
                    // 문자 스트림(Reader)을 끝까지 읽어 하나의 문자열로 합친 뒤 Avro 필드에 저장한다.
                    if (javaSqlType == CLOB || javaSqlType == NCLOB) {
                        Clob clob = rs.getClob(i);
                        if (clob != null) {
                            StringBuilder sb = new StringBuilder();
                            char[] buffer = new char[32 * 1024]; // 32K default buffer
                            try (Reader reader = clob.getCharacterStream()) {
                                int charsRead;
                                while ((charsRead = reader.read(buffer)) != -1) {
                                    sb.append(buffer, 0, charsRead);
                                }
                            }
                            rec.put(i - 1, sb.toString());
                        } else {
                            rec.put(i - 1, null);
                        }
                        continue;
                    }

                    // BLOB도 CLOB과 마찬가지로 getObject() 대신 getBlob()으로 먼저 읽는다.
                    // 바이너리 스트림을 바이트 배열로 모두 읽어 ByteBuffer로 감싸 저장하고,
                    // 드라이버가 free()를 지원하지 않는 경우(SQLFeatureNotSupportedException)는 무시하고 계속 진행한다.
                    if (javaSqlType == BLOB) {
                        Blob blob = rs.getBlob(i);
                        if (blob != null) {
                            long numChars = blob.length();
                            byte[] buffer = new byte[(int) numChars];
                            try (InputStream is = blob.getBinaryStream()) {
                                int index = 0;
                                int c = is.read();
                                while (c >= 0) {
                                    buffer[index++] = (byte) c;
                                    c = is.read();
                                }
                            }
                            ByteBuffer bb = ByteBuffer.wrap(buffer);
                            rec.put(i - 1, bb);
                            try {
                                blob.free();
                            } catch (SQLFeatureNotSupportedException sfnse) {
                                // The driver doesn't support free, but allow processing to continue
                            }
                        } else {
                            rec.put(i - 1, null);
                        }
                        continue;
                    }

                    Object value;

                    // If a Timestamp type, try getTimestamp() rather than getObject()
                    // TIMESTAMP 계열 타입은 getObject() 대신 getTimestamp()를 우선 시도한다(더 정확한 값을 얻기 위함).
                    if (javaSqlType == TIMESTAMP
                            || javaSqlType == TIMESTAMP_WITH_TIMEZONE
                            // The following are Oracle-specific codes for TIMESTAMP WITH TIME ZONE and TIMESTAMP WITH LOCAL TIME ZONE. This would be better
                            // located in the DatabaseAdapter interfaces, but some processors (like ExecuteSQL) use this method but don't specify a DatabaseAdapter.
                            // 아래 두 코드는 Oracle 전용 TIMESTAMP WITH TIME ZONE / TIMESTAMP WITH LOCAL TIME ZONE 타입 코드다.
                            // 원래는 DatabaseAdapter 구현체에 두는 것이 더 적절하지만, ExecuteSQL 등 일부 프로세서는
                            // DatabaseAdapter를 지정하지 않고 이 메서드를 직접 사용하므로 여기서 함께 처리한다.
                            || javaSqlType == -101
                            || javaSqlType == -102) {
                        try {
                            value = rs.getTimestamp(i);
                            // Some drivers (like Derby) return null for getTimestamp() but return a Timestamp object in getObject()
                            // Derby 같은 일부 드라이버는 getTimestamp()가 null을 반환하지만 getObject()는 Timestamp 객체를 반환하는 경우가 있다.
                            if (value == null) {
                                value = rs.getObject(i);
                            }
                        } catch (Exception e) {
                            // The cause of the exception is not known, but we'll fall back to call getObject() and handle any "real" exception there
                            // 예외의 정확한 원인은 알 수 없으나, getObject()로 폴백하여 "진짜" 예외가 있다면 거기서 드러나도록 한다.
                            value = rs.getObject(i);
                        }
                    } else {
                        value = rs.getObject(i);
                    }

                    if (value == null) {
                        rec.put(i - 1, null);

                    } else if (javaSqlType == BINARY || javaSqlType == VARBINARY || javaSqlType == LONGVARBINARY || javaSqlType == ARRAY) {
                        // bytes requires little bit different handling
                        // 바이너리 계열 타입은 getBytes()로 읽어 ByteBuffer로 감싸야 한다(다른 타입과 처리 방식이 다름).
                        byte[] bytes = rs.getBytes(i);
                        ByteBuffer bb = ByteBuffer.wrap(bytes);
                        rec.put(i - 1, bb);
                    } else if (javaSqlType == 100) { // Handle Oracle BINARY_FLOAT data type
                        // Oracle 전용 BINARY_FLOAT 타입 처리
                        rec.put(i - 1, rs.getFloat(i));
                    } else if (javaSqlType == 101) { // Handle Oracle BINARY_DOUBLE data type
                        // Oracle 전용 BINARY_DOUBLE 타입 처리
                        rec.put(i - 1, rs.getDouble(i));
                    } else if (value instanceof Byte) {
                        // tinyint(1) type is returned by JDBC driver as java.sql.Types.TINYINT
                        // But value is returned by JDBC as java.lang.Byte
                        // (at least H2 JDBC works this way)
                        // direct put to avro record results:
                        // org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Byte
                        // tinyint(1) 타입은 JDBC 드라이버가 java.sql.Types.TINYINT로 보고하지만 실제 값은
                        // java.lang.Byte로 반환된다(H2 JDBC 기준 확인됨). Avro는 Byte 타입을 이해하지 못해
                        // 그대로 put하면 "Unknown datum type java.lang.Byte" 예외가 발생하므로 int로 변환한다.
                        rec.put(i - 1, ((Byte) value).intValue());
                    } else if (value instanceof Short) {
                        //MS SQL returns TINYINT as a Java Short, which Avro doesn't understand.
                        // MS SQL은 TINYINT를 Java Short로 반환하는데 Avro는 Short를 이해하지 못하므로 int로 변환한다.
                        rec.put(i - 1, ((Short) value).intValue());
                    } else if (value instanceof BigDecimal) {
                        if (options.useLogicalTypes) {
                            // Delegate mapping to AvroTypeUtil in order to utilize logical types.
                            // logical type을 활용하기 위해 실제 변환은 AvroTypeUtil에 위임한다.
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, fieldSchema));
                        } else {
                            // As string for backward compatibility.
                            // 하위 호환성을 위해 문자열로 저장한다.
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof BigInteger) {
                        // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                        // It the SQL type is BIGINT and the precision is between 0 and 19 (inclusive); if so, the BigInteger is likely a
                        // long (and the schema says it will be), so try to get its value as a long.
                        // Otherwise, Avro can't handle BigInteger as a number - it will throw an AvroRuntimeException
                        // such as: "Unknown datum type: java.math.BigInteger: 38". In this case the schema is expecting a string.
                        // BIGINT의 precision을 확인한다. 일부 DB는 임의 정밀도(19자리 초과)를 허용하지만 Avro는 이를 다룰 수 없다.
                        // SQL 타입이 BIGINT이고 precision이 0~19(포함) 범위이면 BigInteger 값이 long 범위 안에 들어올
                        // 가능성이 높으므로(스키마도 long으로 정의되어 있음) long 변환을 시도한다.
                        // 그렇지 않으면 Avro가 BigInteger를 숫자로 다루지 못해
                        // "Unknown datum type: java.math.BigInteger: 38" 같은 AvroRuntimeException을 던지므로,
                        // 이 경우 스키마도 문자열을 기대하도록 되어 있어 문자열로 변환한다.
                        if (javaSqlType == BIGINT) {
                            int precision = meta.getPrecision(i);
                            if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                                rec.put(i - 1, value.toString());
                            } else {
                                try {
                                    rec.put(i - 1, ((BigInteger) value).longValueExact());
                                } catch (ArithmeticException ae) {
                                    // Since the value won't fit in a long, convert it to a string
                                    // long 범위를 벗어나는 값이므로 문자열로 변환한다.
                                    rec.put(i - 1, value.toString());
                                }
                            }
                        } else {
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof Number || value instanceof Boolean) {
                        // 일반적인 숫자/불리언 값 처리. BIGINT는 precision을 다시 확인하여 long 범위를 벗어나면
                        // 문자열로, Long인데 int로도 표현 가능한 정밀도면 int로 축소해서 저장한다.
                        if (javaSqlType == BIGINT) {
                            int precision = meta.getPrecision(i);
                            if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                                rec.put(i - 1, value.toString());
                            } else {
                                rec.put(i - 1, value);
                            }
                        } else if ((value instanceof Long) && meta.getPrecision(i) < MAX_DIGITS_IN_INT) {
                            int intValue = ((Long) value).intValue();
                            rec.put(i - 1, intValue);
                        } else {
                            rec.put(i - 1, value);
                        }
                    } else if (javaSqlType == DATE) {
                        if (options.useLogicalTypes) {
                            // Handle SQL DATE fields using system default time zone without conversion
                            // SQL DATE 필드는 별도 변환 없이 시스템 기본 시간대를 사용하여 처리한다.
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, fieldSchema));
                        } else {
                            // As string for backward compatibility.
                            rec.put(i - 1, value.toString());
                        }
                    } else if (value instanceof java.sql.Date) {
                        if (options.useLogicalTypes) {
                            // Delegate mapping to AvroTypeUtil in order to utilize logical types.
                            // AvroTypeUtil.convertToAvroObject() expects java.sql.Date object as a UTC normalized date (UTC 00:00:00)
                            // but it comes from the driver in JVM's local time zone 00:00:00 and needs to be converted.
                            // logical type 변환은 AvroTypeUtil에 위임한다.
                            // AvroTypeUtil.convertToAvroObject()는 java.sql.Date가 UTC 기준 00:00:00으로 정규화되어
                            // 있을 것을 기대하지만, 드라이버는 JVM 로컬 시간대 기준 00:00:00 값을 반환하므로 변환이 필요하다.
                            java.sql.Date normalizedDate = DataTypeUtils.convertDateToUTC((java.sql.Date) value);
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(normalizedDate, fieldSchema));
                        } else {
                            // As string for backward compatibility.
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof Date) {
                        if (options.useLogicalTypes) {
                            // Delegate mapping to AvroTypeUtil in order to utilize logical types.
                            // logical type 변환은 AvroTypeUtil에 위임한다.
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, fieldSchema));
                        } else {
                            // As string for backward compatibility.
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof java.sql.SQLXML) {
                        // SQLXML은 문자열로 변환하여 저장한다.
                        rec.put(i - 1, ((SQLXML) value).getString());
                    } else {
                        // The different types that we support are numbers (int, long, double, float),
                        // as well as boolean values and Strings. Since Avro doesn't provide
                        // timestamp types, we want to convert those to Strings. So we will cast anything other
                        // than numbers or booleans to strings by using the toString() method.
                        // 우리가 지원하는 타입은 숫자(int, long, double, float), 불리언, 문자열이다.
                        // Avro에는 타임스탬프 전용 타입이 없으므로 문자열로 변환하고 싶은 값들도 있다.
                        // 따라서 숫자나 불리언이 아닌 나머지 타입은 모두 toString()으로 문자열 변환하여 저장한다.
                        rec.put(i - 1, value.toString());
                    }
                }
                try {
                    dataFileWriter.append(rec);
                    nrOfRows += 1;
                } catch (DataFileWriter.AppendWriteException awe) {
                    // union 타입 스키마와 실제 값의 타입이 맞지 않아 Avro가 union 내 어떤 브랜치를 써야 할지
                    // 판단하지 못하는 경우(UnresolvedUnionException)를 더 명확한 메시지로 감싸서 재발생시킨다.
                    Throwable rootCause = ExceptionUtils.getRootCause(awe);
                    if (rootCause instanceof UnresolvedUnionException) {
                        UnresolvedUnionException uue = (UnresolvedUnionException) rootCause;
                        throw new RuntimeException("Unable to resolve union for value " + uue.getUnresolvedDatum() + " with type " + uue.getUnresolvedDatum().getClass().getCanonicalName() + " while appending record " + rec, awe);
                    } else {
                        throw awe;
                    }
                }

                // maxRows가 지정되어 있고 해당 개수만큼 처리했다면 나머지 로우는 읽지 않고 종료한다.
                // (호출부에서 결과를 여러 FlowFile로 나눌 때 사용)
                if (options.maxRows > 0 && nrOfRows == options.maxRows)
                    break;
            }

            return nrOfRows;
        }
    }

    public static Schema createSchema(final ResultSet rs) throws SQLException {
        return createSchema(rs, null, false);
    }

    public static Schema createSchema(final ResultSet rs, String recordName, boolean convertNames) throws SQLException {
        final AvroConversionOptions options = AvroConversionOptions.builder().recordName(recordName).convertNames(convertNames).build();
        return createSchema(rs, options);
    }

    // 컬럼 하나를 "null 또는 지정된 타입"의 union 필드로 스키마에 추가하는 헬퍼 메서드.
    // JDBC 컬럼은 실제로 NULL 값을 가질 수 있으므로, 모든 필드를 nullable(union[null, T]) 형태로 정의한다.
    private static void addNullableField(
            FieldAssembler<Schema> builder,
            String columnName,
            Function<BaseTypeBuilder<UnionAccumulator<NullDefault<Schema>>>, UnionAccumulator<NullDefault<Schema>>> func
    ) {
        final BaseTypeBuilder<UnionAccumulator<NullDefault<Schema>>> and = builder.name(columnName).type().unionOf().nullBuilder().endNull().and();
        func.apply(and).endUnion().noDefault();
    }

    /**
     * Creates an Avro schema from a result set. If the table/record name is known a priori and provided, use that as a
     * fallback for the record name if it cannot be retrieved from the result set, and finally fall back to a default value.
     * <p>
     * ResultSet의 메타데이터(ResultSetMetaData)만으로 Avro 스키마를 생성한다.
     * 레코드 이름은 (1) 결과 집합의 첫 번째 컬럼이 속한 테이블명 → (2) 호출 시 넘겨준 recordName →
     * (3) 기본값("NiFi_ExecuteSQL_Record") 순으로 우선순위를 적용해 결정한다.
     * 각 컬럼은 java.sql.Types 코드를 기준으로 대응하는 Avro 타입으로 매핑되며,
     * 모든 컬럼은 NULL 가능성을 반영해 union(null, 실제타입)으로 정의된다.
     *
     * @param rs      The result set to convert to Avro
     * @param options Specify various options
     * @return A Schema object representing the result set converted to an Avro record
     * @throws SQLException if any error occurs during conversion
     */
    public static Schema createSchema(final ResultSet rs, AvroConversionOptions options) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        String tableName = StringUtils.isEmpty(options.recordName) ? "NiFi_ExecuteSQL_Record" : options.recordName;
        if (nrOfColumns > 0) {
            String tableNameFromMeta = meta.getTableName(1);
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = tableNameFromMeta;
            }
        }

        if (options.convertNames) {
            tableName = normalizeNameForAvro(tableName);
        }

        final FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();

        /**
         * Some missing Avro types - Decimal, Date types. May need some additional work.
         * Decimal, Date 등 일부 Avro 타입은 아직 완전하지 않을 수 있어 추가 작업이 필요할 수 있다.
         */
        for (int i = 1; i <= nrOfColumns; i++) {
            /**
             *   as per jdbc 4 specs, getColumnLabel will have the alias for the column, if not it will have the column name.
             *  so it may be a better option to check for columnlabel first and if in case it is null is someimplementation,
             *  check for alias. Postgres is the one that has the null column names for calculated fields.
             *  JDBC 4 스펙에 따르면 getColumnLabel()은 컬럼에 별칭(alias)이 있으면 그 별칭을, 없으면 컬럼명을 반환한다.
             *  따라서 컬럼 라벨을 먼저 확인하고, 일부 구현체에서 null일 경우에만 컬럼명으로 대체하는 것이 안전하다.
             *  PostgreSQL은 계산된(calculated) 컬럼에 대해 컬럼명이 null인 대표적인 예다.
             */
            String nameOrLabel = StringUtils.isNotEmpty(meta.getColumnLabel(i)) ? meta.getColumnLabel(i) : meta.getColumnName(i);
            String columnName = options.convertNames ? normalizeNameForAvro(nameOrLabel) : nameOrLabel;
            switch (meta.getColumnType(i)) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                case CLOB:
                case NCLOB:
                case OTHER:
                case SQLXML:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    if (meta.isSigned(i) || (meta.getPrecision(i) > 0 && meta.getPrecision(i) < MAX_DIGITS_IN_INT)) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                case SMALLINT:
                case TINYINT:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    break;

                case BIGINT:
                    // Check the precision of the BIGINT. Some databases allow arbitrary precision (> 19), but Avro won't handle that.
                    // If the precision > 19 (or is negative), use a string for the type, otherwise use a long. The object(s) will be converted
                    // to strings as necessary
                    // BIGINT의 precision을 확인한다. 일부 DB는 임의 정밀도(19자리 초과)를 허용하지만 Avro는 이를 다룰 수 없다.
                    // precision이 19를 초과하거나 음수이면 문자열 타입을, 그 외에는 long 타입을 스키마에 사용한다.
                    // 실제 값은 필요 시 convertToAvroStream()에서 문자열로 변환된다.
                    int precision = meta.getPrecision(i);
                    if (precision < 0 || precision > MAX_DIGITS_IN_BIGINT) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                // java.sql.RowId is interface, is seems to be database
                // implementation specific, let's convert to String
                // java.sql.RowId는 인터페이스이며 데이터베이스 구현체마다 다르게 동작하므로 문자열로 변환한다.
                case ROWID:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case FLOAT:
                case REAL:
                case 100: //Oracle BINARY_FLOAT type
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
                    break;

                case DOUBLE:
                case 101: //Oracle BINARY_DOUBLE type
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                    break;

                // Since Avro 1.8, LogicalType is supported.
                // Avro 1.8부터 LogicalType(decimal 등)이 지원된다.
                case DECIMAL:
                case NUMERIC:
                    if (options.useLogicalTypes) {
                        int decimalPrecision;
                        final int decimalScale;
                        if (meta.getPrecision(i) > 0) {
                            // When database returns a certain precision, we can rely on that.
                            // 데이터베이스가 유효한 precision을 반환하면 그 값을 그대로 신뢰한다.
                            decimalPrecision = meta.getPrecision(i);
                            //For the float data type Oracle return decimalScale < 0 which cause is not expected to org.apache.avro.LogicalTypes
                            //Hence falling back to default scale if decimalScale < 0
                            // float 계열 타입에 대해 Oracle은 음수 scale을 반환할 수 있는데, 이는 Avro LogicalTypes가
                            // 예상하지 못하는 값이므로 scale이 음수이면 기본 scale로 대체한다.
                            decimalScale = meta.getScale(i) >= 0 ? meta.getScale(i) : options.defaultScale;
                        } else {
                            // If not, use default precision.
                            // precision을 알 수 없으면 기본 precision을 사용한다.
                            decimalPrecision = options.defaultPrecision;
                            // Oracle returns precision=0, scale=-127 for variable scale value such as ROWNUM or function result.
                            // Specifying 'oracle.jdbc.J2EE13Compliant' SystemProperty makes it to return scale=0 instead.
                            // Queries for example, 'SELECT 1.23 as v from DUAL' can be problematic because it can't be mapped with decimal with scale=0.
                            // Default scale is used to preserve decimals in such case.
                            // Oracle은 ROWNUM이나 함수 결과처럼 scale이 가변적인 값에 대해 precision=0, scale=-127을 반환한다.
                            // 'oracle.jdbc.J2EE13Compliant' 시스템 속성을 지정하면 scale=0을 반환하도록 바꿀 수 있는데,
                            // 예를 들어 'SELECT 1.23 as v FROM DUAL' 같은 쿼리는 scale=0인 decimal로는 매핑할 수 없어 문제가 된다.
                            // 이런 경우 소수점 자리를 보존하기 위해 기본 scale을 사용한다.
                            decimalScale = meta.getScale(i) > 0 ? meta.getScale(i) : options.defaultScale;
                        }
                        // Scale can be bigger than precision in some cases (Oracle, e.g.) If this is the case, assume precision refers to the number of
                        // decimal digits and thus precision = scale
                        // Oracle 등 일부 경우 scale이 precision보다 클 수 있다. 이 경우 precision이 실제로는
                        // 소수점 이하 자릿수를 의미한다고 가정하고 precision = scale로 맞춰준다.
                        if (decimalScale > decimalPrecision) {
                            decimalPrecision = decimalScale;
                        }
                        final LogicalTypes.Decimal decimal = LogicalTypes.decimal(decimalPrecision, decimalScale);
                        addNullableField(builder, columnName,
                                u -> u.type(decimal.addToSchema(SchemaBuilder.builder().bytesType())));
                    } else {
                        addNullableField(builder, columnName, u -> u.stringType());
                    }
                    break;

                case DATE:

                    addNullableField(builder, columnName,
                            u -> options.useLogicalTypes
                                    ? u.type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()))
                                    : u.stringType());
                    break;

                case TIME:
                    addNullableField(builder, columnName,
                            u -> options.useLogicalTypes
                                    ? u.type(LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType()))
                                    : u.stringType());
                    break;

                case TIMESTAMP:
                case TIMESTAMP_WITH_TIMEZONE:
                case -101: // Oracle's TIMESTAMP WITH TIME ZONE
                case -102: // Oracle's TIMESTAMP WITH LOCAL TIME ZONE
                case -155: // SQL Server's DATETIMEOFFSET
                    addNullableField(builder, columnName,
                            u -> options.useLogicalTypes
                                    ? u.type(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType()))
                                    : u.stringType());
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case ARRAY:
                case BLOB:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                    break;

                case -150: // SQLServer may return -150 from the driver even though it's really -156 (sql_variant), treat as a union since we don't know what the values will actually be
                    // SQL Server 드라이버가 실제로는 -156(sql_variant)인 타입을 -150으로 반환하는 경우가 있다.
                    // 실제 값의 타입을 미리 알 수 없으므로 가능한 모든 타입을 포함하는 union으로 정의한다.
                case -156:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().and().intType().and().longType().and().booleanType().and().bytesType().and()
                            .doubleType().and().floatType().endUnion().noDefault();
                    break;

                default:
                    throw new IllegalArgumentException("createSchema: Unknown SQL type " + meta.getColumnType(i) + " / " + meta.getColumnTypeName(i) + " (table: " + tableName + ", column: " + columnName + ") cannot be converted to Avro type");
            }
        }

        return builder.endRecord();
    }

    // Avro는 필드명에 사용할 수 있는 문자에 제약이 있다(영문자/숫자/밑줄만 허용, 숫자로 시작 불가).
    // 컬럼명이 이 규칙을 어길 경우(공백, 특수문자, 숫자로 시작 등) 유효한 Avro 필드명으로 변환한다.
    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
    }

    /**
     * Sets all of the appropriate parameters on the given PreparedStatement, based on the given FlowFile attributes.
     * <p>
     * 주어진 FlowFile 속성 맵을 감지 불필요(non-sensitive) 값으로 감싸서 {@link #setSensitiveParameters}에
     * 위임하는 편의 메서드. sql.args.N.type / sql.args.N.value 형태의 속성을 파싱해 PreparedStatement에 바인딩한다.
     *
     * @param stmt       the statement to set the parameters on
     * @param attributes the attributes from which to derive parameter indices, values, and types
     * @throws SQLException if the PreparedStatement throws a SQLException when the appropriate setter is called
     */
    public static void setParameters(final PreparedStatement stmt, final Map<String, String> attributes) throws SQLException {
        final Map<String, SensitiveValueWrapper> sensitiveValueWrapperMap = attributes.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new SensitiveValueWrapper(e.getValue(), false)));
        setSensitiveParameters(stmt, sensitiveValueWrapperMap);
    }

    /**
     * Sets all of the appropriate parameters on the given PreparedStatement, based on the given FlowFile attributes
     * and masks sensitive values.
     * <p>
     * 속성 맵 전체를 순회하며 "sql.args.N.type" 패턴에 매칭되는 속성마다 setParameterAtIndex()를 호출해
     * 실제 파라미터 바인딩을 수행한다. 민감(sensitive) 값은 예외 메시지 등 로그에 노출되지 않도록
     * SensitiveValueWrapper로 감싸져 전달된다.
     *
     * @param stmt       the statement to set the parameters on
     * @param attributes the attributes from which to derive parameter indices, values, and types
     * @throws SQLException if the PreparedStatement throws a SQLException when the appropriate setter is called
     */
    public static void setSensitiveParameters(final PreparedStatement stmt, final Map<String, SensitiveValueWrapper> attributes) throws SQLException {
        for (final Map.Entry<String, SensitiveValueWrapper> entry : attributes.entrySet()) {
            final String flowFileAttributeKey = entry.getKey();
            setParameterAtIndex(stmt, attributes, flowFileAttributeKey);
        }
    }

    // "sql.args.<index>.type" 형태의 속성 키 하나를 처리한다.
    // 대응하는 "sql.args.<index>.value"와 "sql.args.<index>.format" 속성을 함께 찾아
    // JdbcCommon.setParameter()로 실제 바인딩을 위임한다. 값 파싱 실패 시 예외 메시지에는
    // 민감 값이 노출되지 않도록 마스킹된 로그 값을 사용한다.
    private static void setParameterAtIndex(final PreparedStatement stmt, final Map<String, SensitiveValueWrapper> attributes, final String flowFileAttributeKey) throws SQLException {
        final Matcher sqlArgumentTypeMatcher = SQL_TYPE_ATTRIBUTE_PATTERN.matcher(flowFileAttributeKey);
        if (sqlArgumentTypeMatcher.matches()) {
            final int sqlArgumentIndex = Integer.parseInt(sqlArgumentTypeMatcher.group(1));

            final String sqlArgumentTypeAttributeName = flowFileAttributeKey;
            final String sqlArgumentType = attributes.get(sqlArgumentTypeAttributeName).getValue();
            final boolean isNumeric = NUMBER_PATTERN.matcher(sqlArgumentType).matches();
            if (!isNumeric) {
                throw new SQLDataException("Value of the " + sqlArgumentTypeAttributeName + " attribute is '" + sqlArgumentType + "', which is not a valid numeral SQL data type");
            }

            final int sqlType = Integer.parseInt(sqlArgumentType);
            final String sqlArgumentValueAttributeName = "sql.args." + sqlArgumentIndex + ".value";
            final Optional<SensitiveValueWrapper> sqlArgumentValueWrapper = Optional.ofNullable(attributes.get(sqlArgumentValueAttributeName));
            final String sqlArgumentValue = sqlArgumentValueWrapper.map(SensitiveValueWrapper::getValue).orElse(null);
            final String sqlArgumentLogValue = sqlArgumentValueWrapper.map(a -> a.isSensitive() ? MASKED_LOG_VALUE : a.getValue()).orElse(null);
            final String sqlArgumentFormatAttributeName = "sql.args." + sqlArgumentIndex + ".format";
            final String sqlArgumentFormat = attributes.containsKey(sqlArgumentFormatAttributeName) ? attributes.get(sqlArgumentFormatAttributeName).getValue() : "";

            try {
                JdbcCommon.setParameter(stmt, sqlArgumentIndex, sqlArgumentValue, sqlType, sqlArgumentFormat);
            } catch (final NumberFormatException nfe) {
                throw new SQLDataException("The value of the " + sqlArgumentValueAttributeName + " is '" + sqlArgumentLogValue + "', which cannot be converted into the necessary data type", nfe);
            } catch (ParseException pe) {
                throw new SQLDataException("The value of the " + sqlArgumentValueAttributeName + " is '" + sqlArgumentLogValue + "', which cannot be converted to a timestamp", pe);
            } catch (UnsupportedEncodingException uee) {
                throw new SQLDataException("The value of the " + sqlArgumentValueAttributeName + " is '" + sqlArgumentLogValue + "', which cannot be converted to UTF-8", uee);
            }
        }
    }

    /**
     * Determines how to map the given value to the appropriate JDBC data type and sets the parameter on the
     * provided PreparedStatement
     * <p>
     * 문자열 형태로 전달된 파라미터 값을 jdbcType(java.sql.Types 코드)에 맞는 자바 타입으로 파싱하여
     * PreparedStatement의 지정된 위치(parameterIndex)에 바인딩한다. DATE/TIME/TIMESTAMP의 경우
     * valueFormat이 비어 있으면 epoch millis 또는 기본 포맷(yyyy-MM-dd 등)으로, 지정되어 있으면
     * {@link #getDateTimeFormatter}가 반환하는 포맷터로 파싱한다. 바이너리 타입은 valueFormat에 따라
     * ascii/hex/base64 인코딩 중 하나로 해석한다.
     *
     * @param stmt           the PreparedStatement to set the parameter on
     * @param parameterIndex the index of the SQL parameter to set
     * @param parameterValue the value of the SQL parameter to set
     * @param jdbcType       the JDBC Type of the SQL parameter to set
     * @throws SQLException if the PreparedStatement throws a SQLException when calling the appropriate setter
     */
    public static void setParameter(final PreparedStatement stmt, final int parameterIndex, final String parameterValue, final int jdbcType, final String valueFormat) throws SQLException, ParseException, UnsupportedEncodingException {
        if (parameterValue == null) {
            stmt.setNull(parameterIndex, jdbcType);
        } else {
            switch (jdbcType) {
                case Types.BIT:
                    stmt.setBoolean(parameterIndex, "1".equals(parameterValue) || "t".equalsIgnoreCase(parameterValue) || Boolean.parseBoolean(parameterValue));
                    break;
                case Types.BOOLEAN:
                    stmt.setBoolean(parameterIndex, Boolean.parseBoolean(parameterValue));
                    break;
                case Types.TINYINT:
                    stmt.setByte(parameterIndex, Byte.parseByte(parameterValue));
                    break;
                case Types.SMALLINT:
                    stmt.setShort(parameterIndex, Short.parseShort(parameterValue));
                    break;
                case Types.INTEGER:
                    stmt.setInt(parameterIndex, Integer.parseInt(parameterValue));
                    break;
                case Types.BIGINT:
                    stmt.setLong(parameterIndex, Long.parseLong(parameterValue));
                    break;
                case Types.REAL:
                    stmt.setFloat(parameterIndex, Float.parseFloat(parameterValue));
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    stmt.setDouble(parameterIndex, Double.parseDouble(parameterValue));
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    stmt.setBigDecimal(parameterIndex, new BigDecimal(parameterValue));
                    break;
                case Types.DATE:
                    java.sql.Date date;

                    if (valueFormat.equals("")) {
                        if (LONG_PATTERN.matcher(parameterValue).matches()) {
                            date = new java.sql.Date(Long.parseLong(parameterValue));
                        } else {
                            String dateFormatString = "yyyy-MM-dd";
                            SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);
                            Date parsedDate = dateFormat.parse(parameterValue);
                            date = new java.sql.Date(parsedDate.getTime());
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalDate parsedDate = LocalDate.parse(parameterValue, dtFormatter);
                        date = new java.sql.Date(java.sql.Date.from(parsedDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()).getTime());
                    }

                    stmt.setDate(parameterIndex, date);
                    break;
                case Types.TIME:
                    Time time;

                    if (valueFormat.equals("")) {
                        if (LONG_PATTERN.matcher(parameterValue).matches()) {
                            time = new Time(Long.parseLong(parameterValue));
                        } else {
                            String timeFormatString = "HH:mm:ss.SSS";
                            SimpleDateFormat dateFormat = new SimpleDateFormat(timeFormatString);
                            Date parsedDate = dateFormat.parse(parameterValue);
                            time = new Time(parsedDate.getTime());
                        }
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalTime parsedTime = LocalTime.parse(parameterValue, dtFormatter);
                        LocalDateTime localDateTime = parsedTime.atDate(LocalDate.ofEpochDay(0));
                        Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
                        time = new Time(instant.toEpochMilli());
                    }

                    stmt.setTime(parameterIndex, time);
                    break;
                case Types.TIMESTAMP:
                    Timestamp ts;

                    // Backwards compatibility note: Format was unsupported for a timestamp field.
                    // 하위 호환성 참고: 과거에는 timestamp 필드에 대해 포맷 지정이 지원되지 않았다.
                    if (valueFormat.equals("")) {
                        long lTimestamp = 0L;
                        if (LONG_PATTERN.matcher(parameterValue).matches()) {
                            lTimestamp = Long.parseLong(parameterValue);
                        } else {
                            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                            Date parsedDate = dateFormat.parse(parameterValue);
                            lTimestamp = parsedDate.getTime();
                        }
                        ts = new Timestamp(lTimestamp);
                    } else {
                        final DateTimeFormatter dtFormatter = getDateTimeFormatter(valueFormat);
                        LocalDateTime ldt = LocalDateTime.parse(parameterValue, dtFormatter);
                        ts = Timestamp.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
                    }

                    stmt.setTimestamp(parameterIndex, ts);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    byte[] bValue;

                    switch (valueFormat) {
                        case "":
                        case "ascii":
                            bValue = parameterValue.getBytes("ASCII");
                            break;
                        case "hex":
                            bValue = HexFormat.of().parseHex(parameterValue);
                            break;
                        case "base64":
                            bValue = Base64.getMimeDecoder().decode(parameterValue);
                            break;
                        default:
                            throw new ParseException("Unable to parse binary data using the formatter `" + valueFormat + "`.", 0);
                    }

                    try {
                        stmt.setBinaryStream(parameterIndex, new ByteArrayInputStream(bValue), bValue.length);
                    } catch (Exception ex) {
                        stmt.setBytes(parameterIndex, bValue);
                    }

                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                    stmt.setString(parameterIndex, parameterValue);
                    break;
                case Types.CLOB:
                    try (final StringReader reader = new StringReader(parameterValue)) {
                        stmt.setCharacterStream(parameterIndex, reader);
                    }
                    break;
                case Types.NCLOB:
                    try (final StringReader reader = new StringReader(parameterValue)) {
                        stmt.setNCharacterStream(parameterIndex, reader);
                    }
                    break;
                default:
                    stmt.setObject(parameterIndex, parameterValue, jdbcType);
                    break;
            }
        }
    }

    // NiFi 프로퍼티에서 지정한 날짜/시간 포맷 이름(예: "ISO_LOCAL_DATE")을 실제 DateTimeFormatter로 변환한다.
    // 미리 정의된 java.time.format.DateTimeFormatter 상수 이름과 일치하면 해당 상수를 사용하고,
    // 그렇지 않으면 사용자 지정 패턴 문자열로 간주하여 DateTimeFormatter.ofPattern()으로 생성한다.
    public static DateTimeFormatter getDateTimeFormatter(String pattern) {
        switch (pattern) {
            case "BASIC_ISO_DATE":
                return DateTimeFormatter.BASIC_ISO_DATE;
            case "ISO_LOCAL_DATE":
                return DateTimeFormatter.ISO_LOCAL_DATE;
            case "ISO_OFFSET_DATE":
                return DateTimeFormatter.ISO_OFFSET_DATE;
            case "ISO_DATE":
                return DateTimeFormatter.ISO_DATE;
            case "ISO_LOCAL_TIME":
                return DateTimeFormatter.ISO_LOCAL_TIME;
            case "ISO_OFFSET_TIME":
                return DateTimeFormatter.ISO_OFFSET_TIME;
            case "ISO_TIME":
                return DateTimeFormatter.ISO_TIME;
            case "ISO_LOCAL_DATE_TIME":
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            case "ISO_OFFSET_DATE_TIME":
                return DateTimeFormatter.ISO_OFFSET_DATE_TIME;
            case "ISO_ZONED_DATE_TIME":
                return DateTimeFormatter.ISO_ZONED_DATE_TIME;
            case "ISO_DATE_TIME":
                return DateTimeFormatter.ISO_DATE_TIME;
            case "ISO_ORDINAL_DATE":
                return DateTimeFormatter.ISO_ORDINAL_DATE;
            case "ISO_WEEK_DATE":
                return DateTimeFormatter.ISO_WEEK_DATE;
            case "ISO_INSTANT":
                return DateTimeFormatter.ISO_INSTANT;
            case "RFC_1123_DATE_TIME":
                return DateTimeFormatter.RFC_1123_DATE_TIME;
            default:
                return DateTimeFormatter.ofPattern(pattern);
        }
    }

    /**
     * An interface for callback methods which allows processing of a row during the convertToAvroStream() processing.
     * <b>IMPORTANT:</b> This method should only work on the row pointed at by the current ResultSet reference.
     * Advancing the cursor (e.g.) can cause rows to be skipped during Avro transformation.
     * <p>
     * convertToAvroStream() 처리 도중 각 로우를 다룰 수 있게 해주는 콜백 인터페이스.
     * <b>중요:</b> processRow()는 현재 ResultSet 커서가 가리키는 로우에 대해서만 동작해야 한다.
     * 커서를 임의로 이동시키면 Avro 변환 과정에서 일부 로우가 누락될 수 있다.
     */
    public interface ResultSetRowCallback {
        void processRow(ResultSet resultSet) throws IOException;

        void applyStateChanges();
    }

    // convertToAvroStream() / createSchema()의 동작을 제어하는 옵션 모음.
    // Builder 패턴으로 생성하며, logical type 사용 여부, 레코드 이름, 최대 행 수, 이름 정규화 여부,
    // DECIMAL/NUMERIC 기본 precision/scale, Avro 압축 코덱 등을 지정할 수 있다.
    public static class AvroConversionOptions {

        private final String recordName;
        private final int maxRows;
        private final boolean convertNames;
        private final boolean useLogicalTypes;

        private final int defaultPrecision;
        private final int defaultScale;
        private final CodecFactory codec;

        private AvroConversionOptions(String recordName, int maxRows, boolean convertNames, boolean useLogicalTypes,
                                      int defaultPrecision, int defaultScale, CodecFactory codec) {
            this.recordName = recordName;
            this.maxRows = maxRows;
            this.convertNames = convertNames;
            this.useLogicalTypes = useLogicalTypes;
            this.defaultPrecision = defaultPrecision;
            this.defaultScale = defaultScale;
            this.codec = codec;
        }

        public static Builder builder() {
            return new Builder();
        }

        public int getDefaultPrecision() {
            return defaultPrecision;
        }

        public int getDefaultScale() {
            return defaultScale;
        }

        public boolean isUseLogicalTypes() {
            return useLogicalTypes;
        }

        public static class Builder {
            private String recordName;
            private int maxRows = 0;
            private boolean convertNames = false;
            private boolean useLogicalTypes = false;
            private int defaultPrecision = DEFAULT_PRECISION_VALUE;
            private int defaultScale = DEFAULT_SCALE_VALUE;
            private CodecFactory codec = CodecFactory.nullCodec();

            /**
             * Specify a priori record name to use if it cannot be determined from the result set.
             * 결과 집합에서 레코드 이름을 알아낼 수 없을 때 대신 사용할 이름을 미리 지정한다.
             */
            public Builder recordName(String recordName) {
                this.recordName = recordName;
                return this;
            }

            public Builder maxRows(int maxRows) {
                this.maxRows = maxRows;
                return this;
            }

            public Builder convertNames(boolean convertNames) {
                this.convertNames = convertNames;
                return this;
            }

            public Builder useLogicalTypes(boolean useLogicalTypes) {
                this.useLogicalTypes = useLogicalTypes;
                return this;
            }

            public Builder defaultPrecision(int defaultPrecision) {
                this.defaultPrecision = defaultPrecision;
                return this;
            }

            public Builder defaultScale(int defaultScale) {
                this.defaultScale = defaultScale;
                return this;
            }

            public Builder codecFactory(String codec) {
                this.codec = AvroUtil.getCodecFactory(codec);
                return this;
            }

            public AvroConversionOptions build() {
                return new AvroConversionOptions(recordName, maxRows, convertNames, useLogicalTypes, defaultPrecision, defaultScale, codec);
            }
        }
    }
}