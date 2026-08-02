/*
 * Copyright 2026 Data Dynamics Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.processors.kudu.util;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;

/**
 * Kudu의 UNIXTIME_MICROS(마이크로초 단위 Timestamp) 컬럼 처리를 위한 Timestamp 변환 유틸리티입니다.
 * java.sql.Timestamp와 마이크로초(long) 값, 그리고 문자열 표현 사이의 변환을 담당합니다.
 */
public class TimestampUtils {

    /**
     * 쓰레드마다 독립적인 DateFormat 인스턴스를 사용하기 위한 ThreadLocal.
     * SimpleDateFormat은 쓰레드에 안전하지 않으므로 이렇게 격리하여 사용합니다.
     */
    private static final ThreadLocal<DateFormat> DATE_FORMAT = new ThreadLocal<DateFormat>() {
        protected DateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getDefault());
            return sdf;
        }
    };

    /**
     * 마이크로초까지 포함하는 패턴(yyyy-MM-dd HH:mm:ss.SSSSSS)으로 문자열을 파싱하여 Timestamp로 변환합니다.
     */
    public static Timestamp parseTimestampMicro(String value) {
        return parseTimestamp(value, "yyyy-MM-dd HH:mm:ss.SSSSSS");
    }

    /**
     * 밀리초까지 포함하는 패턴(yyyy-MM-dd HH:mm:ss.SSS)으로 문자열을 파싱하여 Timestamp로 변환합니다.
     */
    public static Timestamp parseTimestampMillis(String value) {
        return parseTimestamp(value, "yyyy-MM-dd HH:mm:ss.SSS");
    }

    /**
     * 지정한 패턴으로 문자열을 파싱하여 Timestamp로 변환합니다.
     */
    public static Timestamp parseTimestamp(String value, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
        return Timestamp.valueOf(dateTime);
    }


    /**
     * Timestamp를 Kudu가 저장에 사용하는 Epoch 이후 마이크로초 값으로 변환합니다.
     * 밀리초 단위 값과 나노초에서 도출한 마이크로초 나머지를 합산하며, 나머지가 음수인 경우 보정합니다.
     */
    public static long timestampToMicros(Timestamp timestamp) {
        long millis = timestamp.getTime() * 1000L;
        long micros = (long) timestamp.getNanos() % 1000000L / 1000L;
        return micros >= 0L ? millis + micros : millis + 1000000L + micros;
    }

    /**
     * Epoch 이후 마이크로초 값을 Timestamp로 변환합니다. 나노초가 음수가 되지 않도록 밀리초를 보정합니다.
     */
    public static Timestamp microsToTimestamp(long micros) {
        long millis = micros / 1000L;
        long nanos = micros % 1000000L * 1000L;
        if (nanos < 0L) {
            --millis;
            nanos += 1000000000L;
        }

        Timestamp timestamp = new Timestamp(millis);
        timestamp.setNanos((int) nanos);
        return timestamp;
    }

    /**
     * Timestamp를 마이크로초로 변환한 뒤 사람이 읽을 수 있는 문자열로 포맷합니다.
     */
    public static String timestampToString(Timestamp timestamp) {
        long micros = timestampToMicros(timestamp);
        return timestampToString(micros);
    }

    /**
     * Epoch 이후 마이크로초 값을 "yyyy-MM-dd HH:mm:ss.SSSSSSZ" 형태의 문자열로 포맷합니다.
     */
    public static String timestampToString(long micros) {
        long tsMillis = micros / 1000L;
        long tsMicros = micros % 1000000L;
        String tsStr = DATE_FORMAT.get().format(new Date(tsMillis));
        return String.format("%s.%06dZ", tsStr, tsMicros);
    }
}
