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
package io.datadynamics.nifi.processors.csv;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * {@link MultilineCsvParser} 프로세서의 동작을 검증하는 통합 테스트.
 * 파일 기반 입력을 실제 TestRunner에 enqueue하여 인코딩 변환, 헤더 처리,
 * 컬럼 수 검증, 구분자 변경, 필드 내부 개행 처리 등을 확인한다.
 */
public class MultilineCsvParserTest {

    @TempDir
    Path tmpDir;

    private static final String IN_COL = "^|";
    private static final String IN_ROW = "@@\n";

    /* -------------------- 유틸 -------------------- */

    /**
     * 지정한 구분자/인코딩으로 파일에 행들을 씁니다.
     */
    private static void writeRowsToFile(Path path, Charset cs, String colDelim, String rowDelim, List<List<String>> rows) throws IOException {
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(path), cs), 8192)) {
            for (List<String> r : rows) {
                for (int i = 0; i < r.size(); i++) {
                    if (i > 0) w.write(colDelim);
                    w.write(r.get(i));
                }
                w.write(rowDelim);
            }
            w.flush();
        }
    }

    private static int countToken(String text, String token) {
        int cnt = 0, from = 0;
        while (true) {
            int idx = text.indexOf(token, from);
            if (idx < 0) break;
            cnt++;
            from = idx + token.length();
        }
        return cnt;
    }

    private static List<String> header10() {
        return Arrays.asList("id", "name", "city", "amount", "date", "status", "memo", "note", "json", "email");
    }

    /* -------------------- 테스트 -------------------- */

    /**
     * 파일: CP949 → UTF-8 변환 + 선행 2행 스킵 후 헤더를 첫 행으로 기록
     */
    @Test
    public void file_cp949_to_utf8_skip2_then_header_first() throws Exception {
        Path in = tmpDir.resolve("in_cp949.dat");
        Path out = tmpDir.resolve("out_utf8.dat"); // (검증용으로 저장하지 않아도 되지만 경로만 확보)

        writeRowsToFile(in, Charset.forName("CP949"), IN_COL, IN_ROW, Arrays.asList(
                Arrays.asList("SKIP1", "x"),
                Arrays.asList("SKIP2", "y"),
                header10(),
                Arrays.asList("1", "사용자1", "Seoul", "10.50", "2025-08-01", "ACTIVE", "메모", "노트", "{\"k\":\"v1\"}", "user1@example.com"),
                Arrays.asList("2", "사용자2", "Busan", "22.00", "2025-08-02", "PENDING", "메모2", "노트2", "텍스트", "user2@example.com")
        ));

        TestRunner runner = TestRunners.newTestRunner(new MultilineCsvParser());
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "CP949");
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, IN_COL);
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, IN_ROW);
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, IN_COL);
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, IN_ROW);
        runner.setProperty(MultilineCsvParser.IN_QUOTE, "\"");
        runner.setProperty(MultilineCsvParser.OUT_QUOTE, "\"");
        runner.setProperty(MultilineCsvParser.OUT_QUOTE_MODE, "AS_NEEDED");
        runner.setProperty(MultilineCsvParser.PRESERVE_INPUT_QUOTES, "true");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "10");
        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "2");
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
        // Row Processor Class 미지정 → 가공 없음

        // 파일을 "실제 파일에서 읽어" enqueue
        runner.enqueue(Files.newInputStream(in));
        runner.run();

        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_SUCCESS, 1);
        MockFlowFile outFF = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        String outText = new String(outFF.toByteArray(), StandardCharsets.UTF_8);

        String expectedHeader = String.join(IN_COL, header10()) + IN_ROW;
        Assertions.assertTrue(outText.startsWith(expectedHeader), "헤더가 출력 첫 행이어야 함");
        Assertions.assertFalse(outText.contains("SKIP1"));
        Assertions.assertFalse(outText.contains("SKIP2"));
        Assertions.assertTrue(outText.contains("사용자1"));

        outFF.assertAttributeEquals("csv.rows.skipped", "2");
        outFF.assertAttributeEquals("csv.rows.header", "1");
        outFF.assertAttributeEquals("csv.rows.data.in", "2");
        outFF.assertAttributeEquals("csv.rows.data.out", "2");
    }

    /**
     * 파일: 컬럼 수 불일치 시 failure 라우트 + csv.error 포함
     */
    @Test
    public void file_expectedColumnsMismatch_failure() throws Exception {
        Path in = tmpDir.resolve("in_mismatch.dat");

        writeRowsToFile(in, StandardCharsets.UTF_8, IN_COL, IN_ROW, Arrays.asList(
                header10(), // 10개
                Arrays.asList("1", "사용자1", "Seoul", "10.50", "2025-08-01", "ACTIVE", "메모", "노트", "{\"k\":\"v1\"}") // 9개
        ));

        TestRunner runner = TestRunners.newTestRunner(new MultilineCsvParser());
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, IN_COL);
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, IN_ROW);
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, IN_COL);
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, IN_ROW);
        runner.setProperty(MultilineCsvParser.IN_QUOTE, "\"");
        runner.setProperty(MultilineCsvParser.OUT_QUOTE, "\"");
        runner.setProperty(MultilineCsvParser.OUT_QUOTE_MODE, "AS_NEEDED");
        runner.setProperty(MultilineCsvParser.PRESERVE_INPUT_QUOTES, "true");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "10");
        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "0");
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");

        runner.enqueue(Files.newInputStream(in));
        runner.run();

        runner.assertTransferCount(MultilineCsvParser.REL_SUCCESS, 0);
        runner.assertTransferCount(MultilineCsvParser.REL_FAILURE, 1);

        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        ff.assertAttributeExists("csv.error");
        String err = ff.getAttribute("csv.error");
        Assertions.assertTrue(err.contains("Column count mismatch") || err.contains("Header column count mismatch"));
    }

    /**
     * 파일: 출력 구분자 변경이 제대로 반영되는지
     */
    @Test
    public void file_outputDelimiters_applied() throws Exception {
        Path in = tmpDir.resolve("in_outdelim.dat");

        writeRowsToFile(in, StandardCharsets.UTF_8, IN_COL, IN_ROW, Arrays.asList(
                header10(),
                Arrays.asList("1", "Alice", "Seoul", "10.00", "2025-08-01", "ACTIVE", "memo", "note", "txt", "a@example.com")
        ));

        final String OUT_COL = "||";
        final String OUT_ROW = "##\n";

        TestRunner runner = TestRunners.newTestRunner(new MultilineCsvParser());
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, IN_COL);
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, IN_ROW);
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, OUT_COL);
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, OUT_ROW);
        runner.setProperty(MultilineCsvParser.IN_QUOTE, "\"");
        runner.setProperty(MultilineCsvParser.OUT_QUOTE, "\"");
        runner.setProperty(MultilineCsvParser.OUT_QUOTE_MODE, "AS_NEEDED");
        runner.setProperty(MultilineCsvParser.PRESERVE_INPUT_QUOTES, "true");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "10");
        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "0");
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");

        runner.enqueue(Files.newInputStream(in));
        runner.run();

        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_SUCCESS, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        String outText = ff.getContent();

        String expectedHeader = String.join(OUT_COL, header10()) + OUT_ROW;
        Assertions.assertTrue(outText.startsWith(expectedHeader), "헤더는 출력 구분자 사용");
        Assertions.assertTrue(outText.contains("Alice"));
        Assertions.assertFalse(outText.contains(IN_COL), "입력 컬럼 구분자가 결과에 남으면 안 됨");
        Assertions.assertFalse(outText.contains(IN_ROW), "입력 행 구분자가 결과에 남으면 안 됨");
    }

    /**
     * 파일: 필드 중간의 '\n'은 행 경계를 깨면 안 됨 (커스텀 EOR 토큰 사용)
     */
    @Test
    public void file_randomNewlines_doNotBreakParsing() throws Exception {
        Path in = tmpDir.resolve("in_random_newlines.dat");

        String COL = "^|";
        String EOR = "<<EOR>>\n";

        // 헤더 + 데이터 100행(일부 컬럼에 임의로 \n 삽입)
        int rows = 100;
        List<List<String>> all = new java.util.ArrayList<>();
        all.add(header10());
        java.util.Random rnd = new java.util.Random(7);
        for (int i = 1; i <= rows; i++) {
            String c1 = "사용자" + i;
            if (rnd.nextDouble() < 0.4) { // 가끔 줄바꿈 삽입
                int pos = 1 + rnd.nextInt(c1.length() - 1);
                c1 = c1.substring(0, pos) + "\n" + c1.substring(pos);
            }
            String memo = (i % 5 == 0) ? ("메모" + COL + "포함") : "메모";
            if (rnd.nextDouble() < 0.25) {
                memo = memo + "\n추가";
            }
            // 컬럼 구분자를 포함하는 필드는 인용 처리해야 컬럼 수가 유지된다(미인용 시 11컬럼으로 파싱되어 실패).
            if (memo.contains(COL)) {
                memo = "\"" + memo + "\"";
            }
            all.add(Arrays.asList(
                    String.valueOf(i), c1, "Seoul", "10.00", "2025-08-01", "ACTIVE", memo, "note", "txt", "a" + i + "@ex.com"
            ));
        }
        writeRowsToFile(in, StandardCharsets.UTF_8, COL, EOR, all);

        TestRunner runner = TestRunners.newTestRunner(new MultilineCsvParser());
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, COL);
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, EOR);
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, COL);
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, EOR);
        runner.setProperty(MultilineCsvParser.IN_QUOTE, "\"");
        runner.setProperty(MultilineCsvParser.OUT_QUOTE, "\"");
        runner.setProperty(MultilineCsvParser.OUT_QUOTE_MODE, "AS_NEEDED");
        runner.setProperty(MultilineCsvParser.PRESERVE_INPUT_QUOTES, "true");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "10");
        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "0");
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "false"); // 헤더를 일반 데이터로 둘 수도 있음

        runner.enqueue(Files.newInputStream(in));
        runner.run();

        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_SUCCESS, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
        String outText = ff.getContent();

        // 헤더 1 + 데이터 100 = 101개의 EOR 토큰이 있어야 함
        Assertions.assertEquals(101, countToken(outText, EOR));
    }
}
