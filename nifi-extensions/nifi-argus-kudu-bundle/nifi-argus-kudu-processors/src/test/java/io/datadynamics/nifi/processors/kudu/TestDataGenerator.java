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
package io.datadynamics.nifi.processors.kudu;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kudu.shaded.com.google.common.base.Joiner;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 테스트에 사용할 임의의 CSV 샘플 데이터(nifi_sample.csv)를 생성하는 유틸리티입니다.
 * 정수, 실수, 날짜, 초/밀리초/마이크로초 단위의 Timestamp 컬럼값을 무작위로 생성하여 파일로 기록합니다.
 */
public class TestDataGenerator {
    public static void main(String[] args) throws IOException {
        System.out.println(System.currentTimeMillis());
        DateTimeFormatter date = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter timestamp1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter timestamp2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        DateTimeFormatter timestamp3 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

        FileOutputStream fos = new FileOutputStream("nifi_sample.csv");
        OutputStreamWriter writer = new OutputStreamWriter(fos, "UTF-8");

        int maxRow = 10;
        int i = 0;
        while (i < maxRow) {
            List<String> row = new ArrayList();
            row.add("" + RandomUtils.nextInt()); // COL_INT
            row.add("" + RandomUtils.nextLong()); // COL_FLOAT
            row.add(toString(date)); // COL_DATE
            row.add(toString(timestamp1)); // COL_TIMESTAMP
            row.add(toString(timestamp2)); // COL_TIMESTAMP_MILLIS
            row.add(toString(timestamp3)); // COL_TIMESTAMP_MICROS

            String csv = Joiner.on(",").join(row) + "\n";
            writer.write(csv);

            i++;
        }

        writer.close();
        fos.close();
    }

    public static String toString(DateTimeFormatter formatter) {
        Timestamp timestamp = randomTimestamp();
        return toString(formatter, timestamp);
    }

    public static String toString(DateTimeFormatter formatter, Timestamp timestamp) {
        return formatter.format(timestamp.toLocalDateTime());
    }

    public static Timestamp randomTimestamp() {
        long offset = Timestamp.valueOf("2012-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2013-01-01 00:00:00").getTime();
        long diff = end - offset + 1;
        return new Timestamp(offset + (long) (Math.random() * diff));
    }
}
