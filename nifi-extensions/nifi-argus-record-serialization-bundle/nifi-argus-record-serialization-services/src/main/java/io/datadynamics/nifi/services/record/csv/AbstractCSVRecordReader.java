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
 *   nifi-nar-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/csv/AbstractCSVRecordReader.java
 */
package io.datadynamics.nifi.services.record.csv;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.Optional;

/**
 * org.apache.nifi.csv.AbstractCSVRecordReader (NiFi 2.10.0)를 포크하여, 커스텀 CSVReader에서
 * 사용하는 필드(컬럼) 개수 유효성 검사 옵션을 추가로 지원하도록 확장한 추상 클래스다.
 * CSV 파싱 결과 값을 스키마의 데이터 타입으로 변환하는 공통 로직을 제공하며,
 * 실제 레코드 파싱은 하위 클래스(CSVRecordReader, JacksonCSVRecordReader)에서 구현한다.
 */
public abstract class AbstractCSVRecordReader implements RecordReader {

    protected final ComponentLog logger;
    /** CSV 데이터에 헤더 라인이 존재하는지 여부 */
    protected final boolean hasHeader;
    /** 헤더 라인을 무시하고 스키마의 필드명을 그대로 사용할지 여부 */
    protected final boolean ignoreHeader;
    protected final String dateFormat;
    protected final String timeFormat;
    protected final String timestampFormat;
    /** 레코드를 해석할 때 사용하는 스키마 */
    protected final RecordSchema schema;

    /** 유효성 검사에 사용할 기대 필드(컬럼) 개수. null이면 검사하지 않음 */
    final Integer fieldCount;
    /** 파싱된 필드 개수가 fieldCount와 다를 경우 레코드 읽기를 실패 처리할지 여부 */
    final boolean failOnMismatchFieldCount;

    /** 값의 시작/끝에 있는 큰따옴표를 제거할지 여부 (false면 RFC-4180을 그대로 준수) */
    private final boolean trimDoubleQuote;

    // 필드 개수 검증 옵션 없이 생성하는 생성자 (기본값: 검증하지 않음)
    AbstractCSVRecordReader(final ComponentLog logger, final RecordSchema schema, final boolean hasHeader, final boolean ignoreHeader,
                            final String dateFormat, final String timeFormat, final String timestampFormat, final boolean trimDoubleQuote) {
        this(logger, schema, hasHeader, ignoreHeader, dateFormat, timeFormat, timestampFormat, trimDoubleQuote, -1, false);
    }

    // 필드 개수 검증 옵션을 포함하는 생성자. dateFormat/timeFormat/timestampFormat이 비어있으면 null로 정규화한다.
    AbstractCSVRecordReader(final ComponentLog logger, final RecordSchema schema, final boolean hasHeader, final boolean ignoreHeader,
                            final String dateFormat, final String timeFormat, final String timestampFormat, final boolean trimDoubleQuote,
                            final Integer fieldCount, final boolean failOnMismatchFieldCount) {
        this.logger = logger;
        this.schema = schema;
        this.hasHeader = hasHeader;
        this.ignoreHeader = ignoreHeader;
        this.trimDoubleQuote = trimDoubleQuote;
        this.fieldCount = fieldCount;
        this.failOnMismatchFieldCount = failOnMismatchFieldCount;

        this.dateFormat = (dateFormat == null || dateFormat.isEmpty()) ? null : dateFormat;
        this.timeFormat = (timeFormat == null || timeFormat.isEmpty()) ? null : timeFormat;
        this.timestampFormat = (timestampFormat == null || timestampFormat.isEmpty()) ? null : timestampFormat;
    }

    /**
     * 원시 문자열 값을 지정된 데이터 타입으로 변환한다.
     * STRING/CHOICE 타입이고 trimDoubleQuote가 false인 경우를 제외하고는 큰따옴표를 제거한 뒤 변환한다.
     * 변환 결과가 빈 문자열이면 null을 반환한다.
     */
    protected final Object convert(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;
        final RecordFieldType type = dataType.getFieldType();

        if (!trimDoubleQuote && (type.equals(RecordFieldType.STRING) || type.equals(RecordFieldType.CHOICE))) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }

        return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
    }

    /**
     * 가능한 경우에만 값을 목표 데이터 타입으로 변환한다(coerceTypes가 false일 때 사용).
     * 값이 해당 타입과 호환되는지 먼저 검사한 후 호환되는 경우에만 변환하며,
     * 호환되지 않으면 원본 문자열 값을 그대로 반환한다.
     */
    protected final Object convertSimpleIfPossible(final String value, final DataType dataType, final String fieldName) {
        if (dataType == null || value == null) {
            return value;
        }

        final String trimmed;

        if (!trimDoubleQuote && dataType.getFieldType().equals(RecordFieldType.STRING)) {
            trimmed = value;
        } else {
            trimmed = trim(value);
        }

        if (trimmed.isEmpty()) {
            return null;
        }

        switch (dataType.getFieldType()) {
            case STRING:
                return value;
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BYTE:
            case CHAR:
            case SHORT:
                if (DataTypeUtils.isCompatibleDataType(trimmed, dataType)) {
                    return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case DATE:
                if (DataTypeUtils.isDateTypeCompatible(trimmed, dateFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case TIME:
                if (DataTypeUtils.isTimeTypeCompatible(trimmed, timeFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            case TIMESTAMP:
                if (DataTypeUtils.isTimestampTypeCompatible(trimmed, timestampFormat)) {
                    return DataTypeUtils.convertType(trimmed, dataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName);
                }
                break;
            default:
                break;
        }

        return value;
    }

    // 값이 큰따옴표로 감싸져 있으면 앞뒤의 큰따옴표를 제거한다.
    protected String trim(String value) {
        return (value.length() > 1) && value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }
}
