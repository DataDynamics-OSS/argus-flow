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
 *   nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/record/field/ObjectTimestampFieldConverter.java
 */
package io.datadynamics.nifi.processors.kudu.converter;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Optional;

/**
 * 다양한 자바 타입(Timestamp, Date, Number, String)의 값을 {@link Timestamp}로 변환하는 Field Converter 구현체입니다.
 * Kudu는 시간을 GMT 기준으로 저장하므로, addHour 파라미터를 통해 원본 값에 시(Hour) 단위 보정을 적용할 수 있습니다.
 */
public class ObjectTimestampFieldConverter implements FieldConverter<Object, Timestamp> {

    /**
     * 입력 값을 Timestamp로 변환합니다.
     * 입력 값의 실제 타입(Timestamp, Date, Number, String)에 따라 변환 방식이 달라지며,
     * addHour가 0이 아니면 변환 결과에 지정한 시간만큼 더합니다.
     * String 타입인 경우 timestampPattern으로 우선 파싱을 시도하고, 실패하면 숫자(Epoch)로 재해석을 시도합니다.
     */
    @Override
    public Timestamp convertField(final Object field,
                                  final Optional<String> pattern,
                                  final String name,
                                  final int addHour,
                                  String timestampPattern) {
        if (field == null) {
            return null;
        }

        if (field instanceof Timestamp) {
            if (addHour != 0) {
                long number = ((Timestamp) field).getTime();
                Date newDate = DateUtils.addMilliseconds(new Date(number), addHour * (60 * 1000) * 60);
                return new Timestamp(newDate.getTime());
            } else {
                return (Timestamp) field;
            }
        }

        if (field instanceof Date) {
            if (addHour != 0) {
                final Date date = (Date) field;
                Date newDate = DateUtils.addMilliseconds(date, addHour * (60 * 1000) * 60);
                return new Timestamp(newDate.getTime());
            } else {
                final Date date = (Date) field;
                return new Timestamp(date.getTime());
            }
        }

        if (field instanceof Number) {
            if (addHour != 0) {
                final Number number = (Number) field;
                Date newDate = DateUtils.addMilliseconds(new Date(number.longValue()), addHour * (60 * 1000) * 60);
                return new Timestamp(newDate.getTime());
            } else {
                final Number number = (Number) field;
                return new Timestamp(number.longValue());
            }
        }

        if (field instanceof String) {
            final String string = field.toString().trim();
            if (string.isEmpty()) {
                return null;
            }

            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampPattern);
            try {
                // 날짜 패턴으로 데이터를 파싱하고 AddHour를 추가한다.
                final LocalDateTime localDateTime = LocalDateTime.parse(string, formatter);
                if (addHour != 0) {
                    LocalDateTime plus = localDateTime.plus(Duration.ofHours(addHour));
                    return Timestamp.valueOf(plus);
                } else {
                    return Timestamp.valueOf(localDateTime);
                }
            } catch (final DateTimeParseException e1) {
                // 파싱 에러가 발생하면 숫자로 다시 파싱을 시도한다.
                try {
                    final long number = Long.parseLong(string);
                    if (addHour != 0) {
                        Date newDate = DateUtils.addMilliseconds(new Date(number), addHour * (60 * 1000) * 60);
                        return new Timestamp(newDate.getTime());
                    } else {
                        return new Timestamp(number);
                    }
                } catch (final NumberFormatException e2) {
                    final String message = String.format("필드명 [%s] 값 [%s]을 Timestamp로 변환할 수 없습니다: %s", name, field, e2.getMessage());
                    throw new IllegalTypeConversionException(message);
                }
            }
        }

        final String message = String.format("필드명 [%s] 값 [%s] 클래스 [%s]는 Timestamp로 변환을 지원하지 않습니다.", name, field, field.getClass());
        throw new IllegalTypeConversionException(message);
    }
}