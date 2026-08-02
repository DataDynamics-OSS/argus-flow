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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/util/hive/HiveJdbcCommon.java
 */
package io.datadynamics.nifi.processors.hive.util;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.sql.Types.*;

/**
 * JDBC / HiveQL 관련 공통 유틸리티 함수 모음.
 * <p>
 * ResultSet을 Avro 또는 CSV 스트림으로 변환하는 기능과, Hive 컬럼/테이블 이름을
 * Avro 규격에 맞게 정규화하는 기능, Hive 설정 파일(Configuration)을 로드하는
 * 기능 등을 정적 메서드로 제공한다. GetHiveQL/SelectHiveQL 계열 프로세서에서
 * 공통으로 사용된다.
 */
public class HiveJdbcCommon {

    public static final String AVRO = "Avro";
    public static final String CSV = "CSV";

    public static final String MIME_TYPE_AVRO_BINARY = "application/avro-binary";
    public static final String CSV_MIME_TYPE = "text/csv";
    // 컬럼/테이블 이름에 포함된 Avro 비호환 문자(콜론, 마침표 등)를 밑줄로 치환할지 여부를 결정하는 프로퍼티.
    public static final PropertyDescriptor NORMALIZE_NAMES_FOR_AVRO = new PropertyDescriptor.Builder()
            .name("hive-normalize-avro")
            .displayName("테이블/컬럼 이름 정규화")
            .description("컬럼 이름에 포함된 Avro와 호환되지 않는 문자를 Avro 호환 문자로 변경할지 여부. 예를 들어 콜론과 마침표는 "
                    + "유효한 Avro 레코드를 생성하기 위해 밑줄로 변경된다.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    // Avro logical type: 밀리초 단위 타임스탬프를 표현하기 위한 스키마 (long 위에 timestamp-millis 논리 타입을 부여).
    private static final Schema TIMESTAMP_MILLIS_SCHEMA = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    // Avro logical type: 날짜를 표현하기 위한 스키마 (int 위에 date 논리 타입을 부여).
    private static final Schema DATE_SCHEMA = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    // ResultSet에서 유효하지 않은 precision/scale 값이 조회될 때 사용할 Hive 기본값.
    private static final int DEFAULT_PRECISION = 10;
    private static final int DEFAULT_SCALE = 0;

    /**
     * ResultSet의 전체 로우를 Avro 바이너리 스트림으로 변환하여 outStream에 기록한다.
     * 레코드 이름과 행 콜백을 지정하지 않는 간소화된 오버로드.
     *
     * @param rs             변환 대상 ResultSet
     * @param outStream      Avro 데이터가 기록될 출력 스트림
     * @param maxRows        FlowFile 하나에 기록할 최대 행 수 (0 이하이면 제한 없음)
     * @param convertNames   컬럼/테이블 이름을 Avro 호환 이름으로 변환할지 여부
     * @param useLogicalTypes decimal/date/timestamp에 대해 Avro logical type을 사용할지 여부
     * @return 변환된 행 수
     */
    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, final int maxRows, boolean convertNames, final boolean useLogicalTypes) throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, null, maxRows, convertNames, null, useLogicalTypes);
    }


    /**
     * ResultSet의 로우를 순회하며 Avro 레코드로 변환한 뒤 outStream에 Avro Object Container File 형식으로 기록한다.
     * JDBC 타입별로 적절한 Java 객체로 변환(BINARY 계열은 ByteBuffer, BigDecimal은 logical type 적용 여부에 따라
     * decimal 또는 문자열, 그 외 미지원 타입은 toString()) 후 GenericRecord에 채워 넣는다.
     * callback이 주어지면 각 행을 처리하기 전에 호출되며(예: 진행 상황 추적), maxRows에 도달하면 조회를 중단한다.
     *
     * @param rs             변환 대상 ResultSet
     * @param outStream      Avro 데이터가 기록될 출력 스트림
     * @param recordName     생성할 Avro 레코드의 이름 (null이면 ResultSet 메타데이터 또는 기본값 사용)
     * @param maxRows        FlowFile 하나에 기록할 최대 행 수 (0 이하이면 제한 없음)
     * @param convertNames   컬럼/테이블 이름을 Avro 호환 이름으로 변환할지 여부
     * @param callback       각 행을 처리하기 전에 호출할 콜백 (null 가능)
     * @param useLogicalTypes decimal/date/timestamp에 대해 Avro logical type을 사용할지 여부
     * @return 변환된 행 수
     */
    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, final int maxRows, boolean convertNames,
                                           ResultSetRowCallback callback, final boolean useLogicalTypes)
            throws SQLException, IOException {
        final Schema schema = createSchema(rs, recordName, convertNames, useLogicalTypes);
        final GenericRecord rec = new GenericData.Record(schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
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
                    Object value = rs.getObject(i);

                    if (value == null) {
                        rec.put(i - 1, null);

                    } else if (javaSqlType == BINARY || javaSqlType == VARBINARY || javaSqlType == LONGVARBINARY || javaSqlType == BLOB || javaSqlType == CLOB) {
                        // 바이너리 계열 타입은 다른 타입과 조금 다르게 처리해야 한다 (byte[]/ByteBuffer만 허용).
                        ByteBuffer bb = null;
                        if (value instanceof byte[]) {
                            bb = ByteBuffer.wrap((byte[]) value);
                        } else if (value instanceof ByteBuffer) {
                            bb = (ByteBuffer) value;
                        }
                        if (bb != null) {
                            rec.put(i - 1, bb);
                        } else {
                            throw new IOException("Could not process binary object of type " + value.getClass().getName());
                        }

                    } else if (value instanceof Byte) {
                        // tinyint(1) 타입은 JDBC 드라이버가 java.sql.Types.TINYINT로 반환하지만
                        // 실제 값은 java.lang.Byte로 반환된다 (적어도 H2 JDBC는 이렇게 동작함).
                        // Byte를 Avro 레코드에 그대로 넣으면 다음 예외가 발생한다:
                        // org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Byte
                        // 따라서 int로 변환해서 넣어야 한다.
                        rec.put(i - 1, ((Byte) value).intValue());

                    } else if (value instanceof BigDecimal) {
                        if (useLogicalTypes) {
                            final int precision = getPrecision(meta.getPrecision(i));
                            final int scale = getScale(meta.getScale(i));
                            rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES))));
                        } else {
                            rec.put(i - 1, value.toString());
                        }

                    } else if (value instanceof BigInteger) {
                        // Avro는 BigDecimal, BigInteger를 숫자 타입으로 직접 다루지 못한다 -
                        // "Unknown datum type: java.math.BigDecimal: 38"과 같은 AvroRuntimeException이 발생하므로 문자열로 변환한다.
                        rec.put(i - 1, value.toString());

                    } else if (value instanceof Number) {
                        // 예를 들어 JDBC 타입이 6(Float)인 경우에도 Double이 반환되는 경우가 있으므로,
                        // 위의 getObject() 대신 타입에 맞는 getXYZ() 메서드를 호출해서 정확한 값을 얻어야 한다.
                        if (javaSqlType == FLOAT) {
                            value = rs.getFloat(i);
                        } else if (javaSqlType == DOUBLE) {
                            value = rs.getDouble(i);
                        } else if (javaSqlType == INTEGER || javaSqlType == TINYINT || javaSqlType == SMALLINT) {
                            value = rs.getInt(i);
                        }

                        rec.put(i - 1, value);

                    } else if (value instanceof Boolean) {
                        rec.put(i - 1, value);
                    } else if (value instanceof java.sql.SQLXML) {
                        rec.put(i - 1, ((java.sql.SQLXML) value).getString());
                    } else if (useLogicalTypes && javaSqlType == DATE) {
                        rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, DATE_SCHEMA));
                    } else if (useLogicalTypes && javaSqlType == TIMESTAMP) {
                        rec.put(i - 1, AvroTypeUtil.convertToAvroObject(value, TIMESTAMP_MILLIS_SCHEMA));
                    } else {
                        // 지원 대상 타입은 숫자(int, long, double, float), boolean, decimal, date,
                        // timestamp, 문자열이다. Avro는 time 타입을 제공하지 않으므로 문자열로 변환하며,
                        // 숫자나 boolean이 아닌 나머지 값은 모두 toString()으로 문자열 처리한다.
                        rec.put(i - 1, value.toString());
                    }
                }
                dataFileWriter.append(rec);
                nrOfRows += 1;

                if (maxRows > 0 && nrOfRows == maxRows)
                    break;
            }

            return nrOfRows;
        }
    }

    public static Schema createSchema(final ResultSet rs, boolean convertNames) throws SQLException {
        return createSchema(rs, null, false, false);
    }

    /**
     * ResultSet으로부터 Avro 스키마를 생성한다. 테이블/레코드 이름을 미리 알고 있어 인자로 전달한 경우,
     * ResultSet에서 이름을 얻지 못할 때의 대체값으로 사용하며, 그마저도 없으면 최종적으로 기본값을 사용한다.
     *
     * @param rs           Avro로 변환할 ResultSet
     * @param recordName   ResultSet에서 이름을 확인할 수 없을 때 사용할 사전 지정 레코드 이름
     * @param convertNames 컬럼/테이블 이름을 Avro에서 사용 가능한 이름으로 변환할지 여부
     * @return ResultSet을 Avro 레코드로 표현한 Schema 객체
     * @throws SQLException 변환 중 오류가 발생한 경우
     */
    public static Schema createSchema(final ResultSet rs, String recordName, boolean convertNames, final boolean useLogicalTypes) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        String tableName = StringUtils.isEmpty(recordName) ? "NiFi_SelectHiveQL_Record" : recordName;
        try {
            if (nrOfColumns > 0) {
                // Hive JDBC는 getTableName을 지원하지 않고, 대신 컬럼 이름을 "table.column" 형태로 반환한다.
                // 따라서 첫 번째 컬럼 이름에서 테이블 이름을 추출한다.
                String firstColumnNameFromMeta = meta.getColumnName(1);
                int tableNameDelimiter = firstColumnNameFromMeta.lastIndexOf(".");
                if (tableNameDelimiter > -1) {
                    String tableNameFromMeta = firstColumnNameFromMeta.substring(0, tableNameDelimiter);
                    if (!StringUtils.isBlank(tableNameFromMeta)) {
                        tableName = tableNameFromMeta;
                    }
                }
            }
        } catch (SQLException se) {
            // 모든 드라이버가 getTableName을 지원하는 것은 아니므로, 이 경우 앞서 설정한 기본값을 그대로 사용한다.
        }

        if (convertNames) {
            tableName = normalizeNameForAvro(tableName);
        }
        final FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();

        /**
         * Avro에는 대응되는 타입이 없는 SQL 타입(Decimal, Date 등)이 있어 별도 처리가 필요하다.
         * 아래 switch 문에서 SQL 타입별로 가장 가까운 Avro 타입에 매핑하며, 모든 컬럼은 null을 허용하는
         * union 타입(["null", 실제타입])으로 정의하여 NULL 값을 안전하게 처리한다.
         */
        for (int i = 1; i <= nrOfColumns; i++) {
            String columnNameFromMeta = meta.getColumnName(i);
            // Hive는 컬럼 이름을 "table.column" 형태로 반환하므로, 마지막 마침표 뒤의 문자열만 컬럼 이름으로 취한다.
            int columnNameDelimiter = columnNameFromMeta.lastIndexOf(".");
            String columnName = columnNameFromMeta.substring(columnNameDelimiter + 1);
            switch (meta.getColumnType(i)) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                case ARRAY:
                case STRUCT:
                case JAVA_OBJECT:
                case OTHER:
                case SQLXML:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    // Default to signed type unless otherwise noted. Some JDBC drivers don't implement isSigned()
                    boolean signedType = true;
                    try {
                        signedType = meta.isSigned(i);
                    } catch (SQLException se) {
                        // Use signed types as default
                    }
                    if (signedType) {
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
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    break;

                // java.sql.RowId는 인터페이스이며 데이터베이스 구현체마다 다르게 동작하는 것으로 보이므로
                // 문자열로 변환해서 처리한다.
                case ROWID:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case FLOAT:
                case REAL:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
                    break;

                case DOUBLE:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                    break;

                // Avro에 직접 대응되는 타입을 찾지 못했다. 추후 보완이 필요할 수 있음!!!!
                case DECIMAL:
                case NUMERIC:
                    if (useLogicalTypes) {
                        final int precision = getPrecision(meta.getPrecision(i));
                        final int scale = getScale(meta.getScale(i));
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and()
                                .type(LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES))).endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    }
                    break;

                // DATE 타입은 Hive 0.12.0에서 도입되었다.
                case DATE:
                    if (useLogicalTypes) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().type(DATE_SCHEMA).endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    }
                    break;
                // Avro에 직접 대응되는 타입을 찾지 못했다. 추후 보완이 필요할 수 있음!!!!
                case TIME:
                case TIMESTAMP:
                    if (useLogicalTypes) {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().type(TIMESTAMP_MILLIS_SCHEMA).endUnion().noDefault();
                    } else {
                        builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    }
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case BLOB:
                case CLOB:
                    builder.name(columnName).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                    break;


                default:
                    throw new IllegalArgumentException("createSchema: Unknown SQL type " + meta.getColumnType(i) + " cannot be converted to Avro type");
            }
        }

        return builder.endRecord();
    }

    /**
     * ResultSet의 전체 로우를 CSV 스트림으로 변환하여 outStream에 기록한다.
     * 레코드 이름과 행 콜백을 지정하지 않는 간소화된 오버로드.
     */
    public static long convertToCsvStream(final ResultSet rs, final OutputStream outStream, CsvOutputOptions outputOptions) throws SQLException, IOException {
        return convertToCsvStream(rs, outStream, null, null, outputOptions);
    }

    /**
     * ResultSet의 로우를 순회하며 CSV 텍스트로 변환해 outStream에 기록한다. outputOptions에 따라 헤더 출력 여부,
     * 대체 헤더 이름, 구분자, 따옴표 및 이스케이프 처리, FlowFile당 최대 행 수를 제어한다. 문자열/배열/구조체/SQLXML
     * 타입은 필요 시 CSV 이스케이프 처리를 하며, 그 외 타입은 toString()으로 변환한다.
     * callback이 주어지면 각 행을 처리하기 전에 호출된다.
     */
    public static long convertToCsvStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback, CsvOutputOptions outputOptions)
            throws SQLException, IOException {

        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        List<String> columnNames = new ArrayList<>(nrOfColumns);

        if (outputOptions.isHeader()) {
            if (outputOptions.getAltHeader() == null) {
                for (int i = 1; i <= nrOfColumns; i++) {
                    String columnNameFromMeta = meta.getColumnName(i);
                    // Hive는 컬럼 이름을 "table.column" 형태로 반환하므로, 마지막 마침표 뒤의 문자열만 컬럼 이름으로 취한다.
                    int columnNameDelimiter = columnNameFromMeta.lastIndexOf(".");
                    columnNames.add(columnNameFromMeta.substring(columnNameDelimiter + 1));
                }
            } else {
                String[] altHeaderNames = outputOptions.getAltHeader().split(",");
                columnNames = Arrays.asList(altHeaderNames);
            }
        }

        // Write column names as header row
        outStream.write(StringUtils.join(columnNames, outputOptions.getDelimiter()).getBytes(StandardCharsets.UTF_8));
        if (outputOptions.isHeader()) {
            outStream.write("\n".getBytes(StandardCharsets.UTF_8));
        }

        // Iterate over the rows
        int maxRows = outputOptions.getMaxRowsPerFlowFile();
        long nrOfRows = 0;
        while (rs.next()) {
            if (callback != null) {
                callback.processRow(rs);
            }
            List<String> rowValues = new ArrayList<>(nrOfColumns);
            for (int i = 1; i <= nrOfColumns; i++) {
                final int javaSqlType = meta.getColumnType(i);
                final Object value = rs.getObject(i);

                switch (javaSqlType) {
                    case CHAR:
                    case LONGNVARCHAR:
                    case LONGVARCHAR:
                    case NCHAR:
                    case NVARCHAR:
                    case VARCHAR:
                        String valueString = rs.getString(i);
                        if (valueString != null) {
                            // escapeCsv가 필요한 경우 이미 처리하므로, 별도의 따옴표는 붙이지 않는다.
                            StringBuilder sb = new StringBuilder();
                            if (outputOptions.isQuote()) {
                                sb.append("\"");
                                if (outputOptions.isEscape()) {
                                    sb.append(StringEscapeUtils.escapeCsv(valueString));
                                } else {
                                    sb.append(valueString);
                                }
                                sb.append("\"");
                                rowValues.add(sb.toString());
                            } else {
                                if (outputOptions.isEscape()) {
                                    rowValues.add(StringEscapeUtils.escapeCsv(valueString));
                                } else {
                                    rowValues.add(valueString);
                                }
                            }
                        } else {
                            rowValues.add("");
                        }
                        break;
                    case ARRAY:
                    case STRUCT:
                    case JAVA_OBJECT:
                        String complexValueString = rs.getString(i);
                        if (complexValueString != null) {
                            rowValues.add(StringEscapeUtils.escapeCsv(complexValueString));
                        } else {
                            rowValues.add("");
                        }
                        break;
                    case SQLXML:
                        if (value != null) {
                            rowValues.add(StringEscapeUtils.escapeCsv(((java.sql.SQLXML) value).getString()));
                        } else {
                            rowValues.add("");
                        }
                        break;
                    default:
                        if (value != null) {
                            rowValues.add(value.toString());
                        } else {
                            rowValues.add("");
                        }
                }
            }
            // Write row values
            outStream.write(StringUtils.join(rowValues, outputOptions.getDelimiter()).getBytes(StandardCharsets.UTF_8));
            outStream.write("\n".getBytes(StandardCharsets.UTF_8));
            nrOfRows++;

            if (maxRows > 0 && nrOfRows == maxRows)
                break;
        }
        return nrOfRows;
    }

    /**
     * 이름에 포함된 Avro와 호환되지 않는 문자(영문/숫자/밑줄이 아닌 모든 문자)를 밑줄로 치환하여
     * 유효한 Avro 이름으로 정규화한다. 첫 글자가 숫자이면 Avro 명명 규칙 위반이므로 앞에 밑줄을 붙인다.
     */
    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
    }

    /**
     * 콤마로 구분된 Hive 설정 파일 경로 목록(hive-site.xml, core-site.xml 등)을 읽어 하나의
     * Hadoop Configuration 객체로 합쳐서 반환한다. configFiles가 비어 있으면 기본 HiveConf만 반환한다.
     */
    public static Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration hiveConfig = new HiveConf();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hiveConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hiveConfig;
    }

    // ResultSet에 유효하지 않은 precision 값이 들어있는 경우 Hive 기본 precision을 사용한다.
    private static int getPrecision(int precision) {
        return precision > 1 ? precision : DEFAULT_PRECISION;
    }

    // ResultSet에 유효하지 않은 scale 값이 들어있는 경우 Hive 기본 scale을 사용한다.
    private static int getScale(int scale) {
        return scale > 0 ? scale : DEFAULT_SCALE;
    }

    /**
     * convertToXYZStream() 처리 과정에서 각 행을 처리할 수 있도록 하는 콜백 인터페이스.
     * <b>중요:</b> 이 메서드는 현재 ResultSet 커서가 가리키는 행에 대해서만 동작해야 한다.
     * 커서를 이동시키는 등의 동작을 하면 Avro 변환 과정에서 행이 누락될 수 있다.
     */
    public interface ResultSetRowCallback {
        void processRow(ResultSet resultSet) throws IOException;
    }
}
