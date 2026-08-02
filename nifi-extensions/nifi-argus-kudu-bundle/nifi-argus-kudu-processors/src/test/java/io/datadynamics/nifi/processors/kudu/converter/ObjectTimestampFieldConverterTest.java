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
package io.datadynamics.nifi.processors.kudu.converter;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

/**
 * {@link ObjectTimestampFieldConverter}가 long, String(날짜 문자열), String(Epoch 숫자) 등
 * 다양한 입력 타입을 올바르게 Timestamp로 변환하는지, 그리고 addHour 보정과 잘못된 입력에 대한 예외 처리가
 * 정상 동작하는지 검증합니다.
 */
public class ObjectTimestampFieldConverterTest {

    @Test
    public void convertField_long() throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = "2023-01-01 11:11:11.111";
        Date date = formatter.parse(dateString);
        long current = date.getTime();

        ObjectTimestampFieldConverter converter = new ObjectTimestampFieldConverter();
        Timestamp output = converter.convertField(current, Optional.of("yyyy-MM-dd"), "helloworld", 0, "yyyy-MM-dd HH:mm:ss.SSS");
        Assertions.assertEquals(current, output.getTime());
    }

    @Test
    public void convertField_string() throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = "2023-01-01 11:11:11.111";
        Date date = formatter.parse(dateString);
        long current = date.getTime();

        ObjectTimestampFieldConverter converter = new ObjectTimestampFieldConverter();
        Timestamp output = converter.convertField(dateString, Optional.of("yyyy-MM-dd"), "helloworld", 0, "yyyy-MM-dd HH:mm:ss.SSS");
        Assertions.assertEquals(current, output.getTime());
    }

    @Test
    public void convertField_string_long() throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = "2023-01-01 11:11:11.111";
        Date date = formatter.parse(dateString);
        long current = date.getTime();

        ObjectTimestampFieldConverter converter = new ObjectTimestampFieldConverter();
        Timestamp output = converter.convertField(String.valueOf(current), Optional.of("yyyy-MM-dd"), "helloworld", 0, "yyyy-MM-dd HH:mm:ss.SSS");
        Assertions.assertEquals(current, output.getTime());
    }

    @Test
    public void convertField_string_invalid_long() {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String dateString = "2023-01-01 11:11:11.111";
            Date date = formatter.parse(dateString);
            long current = date.getTime();

            ObjectTimestampFieldConverter converter = new ObjectTimestampFieldConverter();
            converter.convertField(String.valueOf(current) + "asdfasdf", Optional.of("yyyy-MM-dd"), "helloworld", 0, "yyyy-MM-dd HH:mm:ss.SSS");
            Assertions.assertTrue(false);
        } catch (IllegalTypeConversionException | ParseException e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void convertField_string_addhour() throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateString = "2023-01-01 11:11:11.111";
        Date date = formatter.parse(dateString);

        ObjectTimestampFieldConverter converter = new ObjectTimestampFieldConverter();
        Timestamp output = converter.convertField(dateString, Optional.of("yyyy-MM-dd"), "helloworld", 9, "yyyy-MM-dd HH:mm:ss.SSS");
        Assertions.assertEquals(formatter.format(DateUtils.addHours(date, +9)), formatter.format(output));
    }

}