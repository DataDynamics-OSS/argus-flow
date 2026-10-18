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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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

    /* -------------------- 엣지 케이스 / 실패 라우팅 / 설정 검증 -------------------- */

    /**
     * 기본 구분자("^|", "@@\n")와 UTF-8 입력/출력, Quote 모드를 지정한 TestRunner를 만든다.
     */
    private static TestRunner newRunner(String quoteMode, boolean preserveQuotes) {
        TestRunner runner = TestRunners.newTestRunner(new MultilineCsvParser());
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "UTF-8");
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, IN_COL);
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, IN_ROW);
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, IN_COL);
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, IN_ROW);
        runner.setProperty(MultilineCsvParser.OUT_QUOTE_MODE, quoteMode);
        runner.setProperty(MultilineCsvParser.PRESERVE_INPUT_QUOTES, String.valueOf(preserveQuotes));
        return runner;
    }

    private static MockFlowFile runSingle(TestRunner runner, String content) {
        return runSingle(runner, content.getBytes(StandardCharsets.UTF_8), Collections.emptyMap());
    }

    private static MockFlowFile runSingle(TestRunner runner, byte[] content, Map<String, String> attrs) {
        runner.enqueue(content, attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_SUCCESS, 1);
        return runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
    }

    /**
     * Quote로 감싼 필드 내부의 개행(\n, \r\n), 컬럼 구분자, 행 구분자, 이스케이프된 Quote("")는
     * 필드 값으로 보존되어야 한다(입력 Quote 제거 + 출력 ALWAYS Quote).
     */
    @Test
    public void quotedField_embeddedNewlinesDelimitersAndEscapedQuotes() {
        TestRunner runner = newRunner("ALWAYS", false);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        String in = "1^|\"line1\nline2\r\nline3\"^|x@@\n"
                + "2^|\"has ^| col and @@\n row\"^|y@@\n"
                + "3^|\"say \"\"hi\"\"\"^|z@@\n";
        MockFlowFile ff = runSingle(runner, in);
        ff.assertContentEquals("\"1\"^|\"line1\nline2\r\nline3\"^|\"x\"@@\n"
                + "\"2\"^|\"has ^| col and @@\n row\"^|\"y\"@@\n"
                + "\"3\"^|\"say \"\"hi\"\"\"^|\"z\"@@\n");
        ff.assertAttributeEquals("csv.rows.data.in", "3");
        ff.assertAttributeEquals("csv.rows.data.out", "3");
    }

    /**
     * 기본 설정(입력 Quote 보존 + 출력 NEVER)에서는 입력이 바이트 단위로 그대로 통과되어야 한다.
     */
    @Test
    public void preserveQuotes_neverMode_isExactPassthrough() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        String in = "a^|\"b ^| \"\" \n c\"^|@@\n^|^|@@\n";
        MockFlowFile ff = runSingle(runner, in);
        ff.assertContentEquals(in);
    }

    /**
     * 빈 필드(선두/중간/말미)와 빈 Quote 필드("")가 컬럼 수를 유지해야 한다.
     */
    @Test
    public void emptyFields_areCounted() {
        TestRunner runner = newRunner("NEVER", false);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "4");
        MockFlowFile ff = runSingle(runner, "^|a^|^|@@\n\"\"^|\"\"^|b^|\"\"@@\n");
        ff.assertContentEquals("^|a^|^|@@\n^|^|b^|@@\n");
        ff.assertAttributeEquals("csv.rows.data.in", "2");
    }

    /**
     * 마지막 행에 행 구분자가 없어도 마지막 행이 유실되지 않아야 한다.
     */
    @Test
    public void lastRowWithoutRowDelimiter_isNotLost() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        MockFlowFile ff = runSingle(runner, "a^|b@@\nc^|d");
        ff.assertContentEquals("a^|b@@\nc^|d@@\n");
        ff.assertAttributeEquals("csv.rows.data.in", "2");
    }

    /**
     * CRLF 기반 행 구분자("@@\r\n")를 지정하면 Quote 내부의 CRLF는 보존되고 행 경계로 쓰이지 않아야 한다.
     */
    @Test
    public void crlfRowDelimiter_withEmbeddedCrlfInQuotes() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, "@@\r\n");
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, "@@\n");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        MockFlowFile ff = runSingle(runner, "1^|\"a\r\nb\"@@\r\n2^|c@@\r\n");
        ff.assertContentEquals("1^|\"a\r\nb\"@@\n2^|c@@\n");
        ff.assertAttributeEquals("csv.rows.data.in", "2");
    }

    /**
     * 빈 입력: 헤더 미사용이면 성공(0행), 헤더 사용이면 failure로 라우팅되어야 한다.
     */
    @Test
    public void emptyInput_successWithoutHeader_failureWithHeader() {
        TestRunner runner = newRunner("NEVER", true);
        MockFlowFile ff = runSingle(runner, "");
        ff.assertContentEquals("");
        ff.assertAttributeEquals("csv.rows.data.in", "0");

        TestRunner withHeader = newRunner("NEVER", true);
        withHeader.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
        withHeader.enqueue(new byte[0]);
        withHeader.run();
        withHeader.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile failed = withHeader.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        Assertions.assertTrue(failed.getAttribute("csv.error").contains("No header"));
        failed.assertContentEquals(""); // 실패 시 원본 콘텐츠 유지
    }

    /**
     * 스킵할 행 수가 전체 행 수보다 많을 때: 헤더 미사용이면 빈 출력으로 성공, 헤더 사용이면 failure.
     */
    @Test
    public void skipMoreRowsThanPresent() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "5");
        MockFlowFile ff = runSingle(runner, "a^|b@@\nc^|d@@\n");
        ff.assertContentEquals("");
        ff.assertAttributeEquals("csv.rows.skipped", "2");
        ff.assertAttributeEquals("csv.rows.data.in", "0");

        TestRunner withHeader = newRunner("NEVER", true);
        withHeader.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "5");
        withHeader.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
        withHeader.enqueue("a^|b@@\n");
        withHeader.run();
        withHeader.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        Assertions.assertTrue(withHeader.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0)
                .getAttribute("csv.error").contains("EOF while skipping"));
    }

    /**
     * 헤더 컬럼 수가 기대 컬럼 수와 다르면 failure + 원본 콘텐츠/속성 유지.
     */
    @Test
    public void headerColumnMismatch_routesToFailureWithOriginalContent() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
        String in = "h1^|h2@@\n1^|2^|3@@\n";
        runner.enqueue(in, Map.of("filename", "x.dat"));
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        ff.assertContentEquals(in);
        ff.assertAttributeEquals("filename", "x.dat");
        Assertions.assertTrue(ff.getAttribute("csv.error").startsWith("Header column count mismatch"));
    }

    /**
     * 구분자 길이가 2 미만이면(EL로 1문자가 들어온 경우 포함) failure로 라우팅되어야 한다.
     */
    @Test
    public void singleCharDelimiter_routesToFailure() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, "${col}");
        runner.enqueue("a,b@@\n", Map.of("col", ","));
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0)
                .assertAttributeEquals("csv.error", "Delimiters must be length >= 2");
    }

    /**
     * 구분자/문자셋/기대 컬럼 수/스킵 행 수를 FlowFile 속성 기반 EL로 지정할 수 있어야 한다.
     */
    @Test
    public void expressionLanguage_fromFlowFileAttributes() {
        TestRunner runner = TestRunners.newTestRunner(new MultilineCsvParser());
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "${in.cs}");
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "${out.cs}");
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, "${in.col}");
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, "${in.row}");
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, "${out.col}");
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, "${out.row}");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "${cols}");
        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "${skip}");
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
        runner.assertValid();

        Map<String, String> attrs = Map.of(
                "in.cs", "CP949", "out.cs", "UTF-8",
                "in.col", "~~", "in.row", "##",
                "out.col", "\t\t", "out.row", "\r\n",
                "cols", "2", "skip", "1");
        byte[] in = "skip~~me##이름~~도시##홍길동~~서울##".getBytes(Charset.forName("CP949"));
        MockFlowFile ff = runSingle(runner, in, attrs);
        ff.assertContentEquals("이름\t\t도시\r\n홍길동\t\t서울\r\n", StandardCharsets.UTF_8);
        ff.assertAttributeEquals("csv.input.charset", "x-windows-949"); // C-11: CP949 → x-windows-949
        ff.assertAttributeEquals("csv.rows.skipped", "1");
        ff.assertAttributeEquals("csv.rows.header", "1");
        ff.assertAttributeEquals("csv.rows.data.in", "1");
    }

    /**
     * 설정 검증: 잘못된 문자셋, 정수가 아닌 기대 컬럼 수, 음수 스킵, 허용되지 않은 Quote 모드는 invalid.
     */
    @Test
    public void configValidation() {
        TestRunner runner = newRunner("NEVER", true);
        runner.assertValid();

        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "NOT-A-CHARSET");
        runner.assertNotValid();
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "UTF-8");

        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "abc");
        runner.assertNotValid();
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "0");

        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "-1");
        runner.assertNotValid();
        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "0");

        runner.setProperty(MultilineCsvParser.OUT_QUOTE_MODE, "SOMETIMES");
        runner.assertNotValid();
        runner.setProperty(MultilineCsvParser.OUT_QUOTE_MODE, "AS_NEEDED");

        runner.setProperty(MultilineCsvParser.PRESERVE_INPUT_QUOTES, "yes");
        runner.assertNotValid();
        runner.setProperty(MultilineCsvParser.PRESERVE_INPUT_QUOTES, "false");
        runner.assertValid();
    }

    /**
     * 여러 FlowFile을 한 번에 처리해도 파서 상태가 FlowFile 간에 섞이지 않아야 한다
     * (앞 FlowFile이 Quote가 열린 채 끝나 failure가 되어도 다음 FlowFile에 영향 없음).
     */
    @Test
    public void multipleFlowFiles_stateIsolated() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        runner.enqueue("a^|\"open");
        runner.enqueue("c^|d@@\n");
        runner.run(2);
        runner.assertTransferCount(MultilineCsvParser.REL_FAILURE, 1); // C-3: 닫히지 않은 Quote
        runner.assertTransferCount(MultilineCsvParser.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0).assertContentEquals("a^|\"open");
        runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0).assertContentEquals("c^|d@@\n");
    }

    /* -------------------- Quote 보존 × 출력 Quote 모드 조합 / Round-trip -------------------- */

    /**
     * 구분자/Quote/개행/행 구분자/빈 값을 포함하는 입력 1행(8컬럼).
     * plain | "d^|x" | "q""x" | "n\nx" | "r@@\nx" | "" | (빈 값) | "simple"
     */
    private static final String MATRIX_IN =
            "plain^|\"d^|x\"^|\"q\"\"x\"^|\"n\nx\"^|\"r@@\nx\"^|\"\"^|^|\"simple\"@@\n";

    /** 위 입력을 Quote 제거 + ALWAYS로 기록한 정규형(값 비교의 기준). */
    private static final String MATRIX_CANONICAL =
            "\"plain\"^|\"d^|x\"^|\"q\"\"x\"^|\"n\nx\"^|\"r@@\nx\"^|\"\"^|\"\"^|\"simple\"@@\n";

    private static String transform(String quoteMode, boolean preserve, String in) {
        TestRunner runner = newRunner(quoteMode, preserve);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "8");
        return runSingle(runner, in).getContent();
    }

    /**
     * 출력을 다시 파싱(Quote 제거 + ALWAYS 정규형)했을 때 원본과 같은 값이 나와야 하고,
     * 같은 설정으로 한 번 더 변환해도 출력이 변하지 않아야 한다(Quote가 누적되지 않음).
     */
    private static void assertRoundTrip(String quoteMode, boolean preserve, String out) {
        Assertions.assertEquals(MATRIX_CANONICAL, transform("ALWAYS", false, out),
                "round-trip 값 불일치: mode=" + quoteMode + ", preserve=" + preserve);
        Assertions.assertEquals(out, transform(quoteMode, preserve, out),
                "재변환 시 출력이 달라짐(Quote 누적): mode=" + quoteMode + ", preserve=" + preserve);
    }

    /** 보존 + NEVER: 원래 Quote된 필드만 그대로 Quote되어 입력과 동일하다. */
    @Test
    public void matrix_preserve_never() {
        String out = transform("NEVER", true, MATRIX_IN);
        Assertions.assertEquals(MATRIX_IN, out);
        assertRoundTrip("NEVER", true, out);
    }

    /** 보존 + AS_NEEDED: 원래 Quote된 필드는 한 번만 감싸고, 나머지는 필요 없으므로 그대로 → 입력과 동일. */
    @Test
    public void matrix_preserve_asNeeded() {
        String out = transform("AS_NEEDED", true, MATRIX_IN);
        Assertions.assertEquals(MATRIX_IN, out);
        assertRoundTrip("AS_NEEDED", true, out);
    }

    /** 보존 + ALWAYS: 모든 필드를 정확히 한 번씩 감싼다(이중 Quote 없음). */
    @Test
    public void matrix_preserve_always() {
        String out = transform("ALWAYS", true, MATRIX_IN);
        Assertions.assertEquals(MATRIX_CANONICAL, out);
        assertRoundTrip("ALWAYS", true, out);
    }

    /** 제거 + NEVER: Quote를 모두 제거한다(값에 구분자가 있으면 재파싱 불가 - 의도된 손실 모드라 round-trip 대상 아님). */
    @Test
    public void matrix_remove_never() {
        String out = transform("NEVER", false, MATRIX_IN);
        Assertions.assertEquals("plain^|d^|x^|q\"x^|n\nx^|r@@\nx^|^|^|simple@@\n", out);
    }

    /** 제거 + AS_NEEDED: Quote 문자/컬럼 구분자/행 구분자를 포함한 값만 감싸고 내부 Quote는 한 번만 이스케이프. */
    @Test
    public void matrix_remove_asNeeded() {
        String out = transform("AS_NEEDED", false, MATRIX_IN);
        Assertions.assertEquals("plain^|\"d^|x\"^|\"q\"\"x\"^|n\nx^|\"r@@\nx\"^|^|^|simple@@\n", out);
        assertRoundTrip("AS_NEEDED", false, out);
    }

    /** 제거 + ALWAYS: 모든 필드를 한 번씩 감싼다. */
    @Test
    public void matrix_remove_always() {
        String out = transform("ALWAYS", false, MATRIX_IN);
        Assertions.assertEquals(MATRIX_CANONICAL, out);
        assertRoundTrip("ALWAYS", false, out);
    }

    /**
     * 보존 모드에서도 Quote 문자 자체를 값으로 쓰지 않으므로, 입력 Quote와 출력 Quote가 다르면
     * 원래 Quote된 필드는 출력 Quote 문자로 감싸고 내부 Quote도 출력 Quote 기준으로 이스케이프한다.
     */
    @Test
    public void preserve_differentOutputQuote_requotesWithOutputQuote() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.OUT_QUOTE, "'");
        MockFlowFile ff = runSingle(runner, "\"it's\"^|\"a\"\"b\"^|c@@\n");
        ff.assertContentEquals("'it''s'^|'a\"b'^|c@@\n");
    }

    /**
     * 자기 겹침 구분자(컬럼 "||", 행 "##")에서 Quote 값 끝의 구분자 접두 문자가 구분자로 오인되지 않아야 한다
     * (보존/제거 두 모드 모두, 이스케이프된 Quote가 끝에 오는 경우 포함).
     */
    @Test
    public void selfOverlappingDelimiters_afterClosingQuote() {
        for (boolean preserve : new boolean[]{true, false}) {
            TestRunner runner = newRunner("AS_NEEDED", preserve);
            runner.setProperty(MultilineCsvParser.IN_COL_DELIM, "||");
            runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, "^|");
            runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, "##");
            runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, "@@\n");
            runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
            MockFlowFile ff = runSingle(runner, "\"a|\"||\"b\"\"|\"||\"c#\"##x||y||z##");
            String expected = preserve
                    ? "\"a|\"^|\"b\"\"|\"^|\"c#\"@@\nx^|y^|z@@\n"
                    : "a|^|\"b\"\"|\"^|c#@@\nx^|y^|z@@\n";
            ff.assertContentEquals(expected);
            ff.assertAttributeEquals("csv.rows.data.in", "2");
        }
    }

    /**
     * 마지막 필드가 빈 Quote("")이고 행 구분자 없이 끝나도 필드가 유실되지 않아야 한다.
     */
    @Test
    public void emptyQuotedFieldAtEof_isNotLost() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "1");
        MockFlowFile ff = runSingle(runner, "\"\"");
        ff.assertContentEquals("\"\"@@\n");
    }

    /* ==================== 구분자가 값으로 들어가는 경우 (파라미터화) ==================== */

    private static final char Q = '"';
    private static final List<String> QUOTE_MODES = List.of("NEVER", "AS_NEEDED", "ALWAYS");

    /** 입력/출력 구분자 설정. 입력과 출력이 다른 설정은 구분자 변환 경로를 검증한다. */
    record DelimCfg(String name, String inCol, String inRow, String outCol, String outRow) {
        @Override
        public String toString() {
            return name;
        }
    }

    /*
     * 참고: 프로세서는 모든 구분자에 2문자 이상을 요구하므로 단일 "\n" 라인 구분자는 사용할 수 없다
     * (singleCharRowDelimiter_routesToFailure 참조). 대신 "\r\n", "@@\n", "<<EOR>>\n", "##"를 쓴다.
     */
    static List<DelimCfg> delimCfgs() {
        return List.of(
                new DelimCfg("default(^|,@@\\n)", "^|", "@@\n", "^|", "@@\n"),
                new DelimCfg("(||,\\r\\n)", "||", "\r\n", "||", "\r\n"),
                new DelimCfg("(|^|,<<EOR>>\\n)", "|^|", "<<EOR>>\n", "|^|", "<<EOR>>\n"),
                new DelimCfg("(^|,\\r\\n)", "^|", "\r\n", "^|", "\r\n"),
                new DelimCfg("(||,##)", "||", "##", "||", "##"),
                new DelimCfg("(^|,@@\\n)->(||,\\r\\n)", "^|", "@@\n", "||", "\r\n"),
                new DelimCfg("(|^|,\\r\\n)->(^|,@@\\n)", "|^|", "\r\n", "^|", "@@\n"));
    }

    /**
     * 설정의 모든 구분자(입력/출력)에 대해 구분자 전체·연속·접두/접미·앞뒤에 붙은 값, CR/LF 단독,
     * Quote 문자, 빈 값, 컬럼+라인 구분자+Quote 혼합 값을 만든다.
     */
    static List<String> delimValues(DelimCfg c) {
        Set<String> v = new LinkedHashSet<>();
        for (String d : List.of(c.inCol(), c.inRow(), c.outCol(), c.outRow())) {
            v.add(d);
            v.add(d + d);
            v.add(d + d + d);
            v.add("a" + d);
            v.add(d + "b");
            v.add("a" + d + "b");
            for (int k = 1; k < d.length(); k++) {
                String prefix = d.substring(0, k), suffix = d.substring(k);
                v.add(prefix);
                v.add(suffix);
                v.add("a" + prefix);
                v.add(suffix + "b");
            }
        }
        v.add("");
        v.add("\r");
        v.add("\n");
        v.add("\r\n");
        v.add("\"");
        v.add("\"\"");
        v.add("q\"x");
        v.add("\"x\"");
        v.add(c.inCol() + c.inRow());
        v.add(c.inRow() + c.inCol());
        v.add("a" + c.inCol() + "b" + c.inRow() + "c\"d" + c.outCol() + "e\"\"" + c.outRow());
        v.add("한글" + c.inCol() + "값" + c.inRow() + "끝");
        return new ArrayList<>(v);
    }

    private static String printable(String s) {
        return s.isEmpty() ? "<empty>" : s.replace("\r", "\\r").replace("\n", "\\n");
    }

    /**
     * 처리기와 같은 "버퍼 끝 일치" 방식으로 t를 읽을 때 가장 먼저 일치하는 구분자가 t의 맨 끝에 있는 d인지 검사한다.
     */
    private static boolean firstDelimiterIsAtEnd(String t, String d, String col, String row) {
        for (int e = 1; e <= t.length(); e++) {
            String p = t.substring(0, e);
            boolean r = p.endsWith(row), cc = p.endsWith(col);
            if (r || cc) return e == t.length() && (d.equals(row) ? r : (cc && !r));
        }
        return false;
    }

    /**
     * 값을 Quote 없이 쓰면 다시 읽을 때 달라지는지(= Quote가 반드시 필요한지) 판정한다.
     * Quote 문자/구분자를 포함하거나, 값 끝과 다음 구분자가 겹쳐 더 이른 위치에서 구분자가 일치하는 경우.
     */
    static boolean needsQuote(String v, String col, String row) {
        return v.indexOf(Q) >= 0 || v.contains(col) || v.contains(row)
                || !firstDelimiterIsAtEnd(v + col, col, col, row)
                || !firstDelimiterIsAtEnd(v + row, row, col, row);
    }

    private static String quoted(String v) {
        return Q + v.replace("\"", "\"\"") + Q;
    }

    /** 입력 텍스트를 만든다. quotedFlags가 true인 필드만 Quote로 감싸고 내부 Quote는 ""로 이스케이프. */
    static String encodeInput(List<List<String>> rows, List<List<Boolean>> quotedFlags, String col, String row, boolean trailingRowDelim) {
        StringBuilder sb = new StringBuilder();
        for (int r = 0; r < rows.size(); r++) {
            for (int i = 0; i < rows.get(r).size(); i++) {
                if (i > 0) sb.append(col);
                String v = rows.get(r).get(i);
                sb.append(quotedFlags.get(r).get(i) ? quoted(v) : v);
            }
            if (r < rows.size() - 1 || trailingRowDelim) sb.append(row);
        }
        return sb.toString();
    }

    /** 값의 끝 k문자(1 <= k < 구분자 길이)가 구분자의 앞 k문자와 같은지. */
    private static boolean tailIsProperPrefixOf(String v, String d) {
        for (int k = 1; k < d.length() && k <= v.length(); k++) {
            if (v.endsWith(d.substring(0, k))) return true;
        }
        return false;
    }

    /**
     * 출력 Quote 규칙(명세): ALWAYS 이거나, 보존 모드에서 입력이 Quote된 필드이거나, AS_NEEDED에서
     * Quote/구분자 전체를 포함하거나 값의 접미가 구분자의 진접두와 같은 값(C-10).
     */
    static boolean outputQuoted(String v, boolean inputQuoted, String mode, boolean preserve, String outCol, String outRow) {
        return mode.equals("ALWAYS") || (preserve && inputQuoted)
                || (mode.equals("AS_NEEDED") && (v.indexOf(Q) >= 0 || v.contains(outCol) || v.contains(outRow)
                || tailIsProperPrefixOf(v, outCol) || tailIsProperPrefixOf(v, outRow)));
    }

    static String expectedOutput(List<List<String>> rows, List<List<Boolean>> inQuoted, String mode, boolean preserve,
                                 String outCol, String outRow) {
        StringBuilder sb = new StringBuilder();
        for (int r = 0; r < rows.size(); r++) {
            for (int i = 0; i < rows.get(r).size(); i++) {
                if (i > 0) sb.append(outCol);
                String v = rows.get(r).get(i);
                sb.append(outputQuoted(v, inQuoted.get(r).get(i), mode, preserve, outCol, outRow) ? quoted(v) : v);
            }
            sb.append(outRow);
        }
        return sb.toString();
    }

    /** 출력의 모든 필드가 Quote되었거나 Quote 없이도 안전하면 출력을 다시 파싱했을 때 원래 값이 나와야 한다. */
    static boolean roundTrippable(List<List<String>> rows, List<List<Boolean>> inQuoted, String mode, boolean preserve,
                                  String outCol, String outRow) {
        for (int r = 0; r < rows.size(); r++) {
            for (int i = 0; i < rows.get(r).size(); i++) {
                String v = rows.get(r).get(i);
                if (!outputQuoted(v, inQuoted.get(r).get(i), mode, preserve, outCol, outRow) && needsQuote(v, outCol, outRow)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 처리기와 독립적인 참조 파서(위치 기반 startsWith 매칭). round-trip 검증에 사용한다.
     * Quote 필드는 닫는 Quote 직후에 구분자/EOF가 와야 하며, 그렇지 않으면 예외.
     */
    static List<List<String>> refParse(String s, String col, String row) {
        List<List<String>> rows = new ArrayList<>();
        if (s.isEmpty()) return rows;
        List<String> cur = new ArrayList<>();
        int i = 0, n = s.length();
        while (true) {
            StringBuilder sb = new StringBuilder();
            if (i < n && s.charAt(i) == Q) {
                i++;
                while (true) {
                    if (i >= n) throw new IllegalStateException("unterminated quote: " + printable(s));
                    char ch = s.charAt(i);
                    if (ch == Q) {
                        if (i + 1 < n && s.charAt(i + 1) == Q) {
                            sb.append(Q);
                            i += 2;
                        } else {
                            i++;
                            break;
                        }
                    } else {
                        sb.append(ch);
                        i++;
                    }
                }
                if (i < n && !s.startsWith(row, i) && !s.startsWith(col, i)) {
                    throw new IllegalStateException("garbage after closing quote at " + i + ": " + printable(s));
                }
            } else {
                while (i < n && !s.startsWith(row, i) && !s.startsWith(col, i)) sb.append(s.charAt(i++));
            }
            cur.add(sb.toString());
            if (i >= n) {
                rows.add(cur);
                return rows;
            }
            if (s.startsWith(row, i)) {
                i += row.length();
                rows.add(cur);
                cur = new ArrayList<>();
                if (i >= n) return rows;
            } else {
                i += col.length();
            }
        }
    }

    private static List<List<Boolean>> quoteFlagsFor(List<List<String>> rows, String col, String row, Set<String> alwaysQuote) {
        List<List<Boolean>> flags = new ArrayList<>();
        for (List<String> r : rows) {
            List<Boolean> f = new ArrayList<>();
            for (String v : r) f.add(alwaysQuote.contains(v) || needsQuote(v, col, row));
            flags.add(f);
        }
        return flags;
    }

    private static TestRunner cfgRunner(DelimCfg c, String mode, boolean preserve, String cs) {
        TestRunner runner = newRunner(mode, preserve);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, cs);
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, cs);
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, c.inCol());
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, c.inRow());
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, c.outCol());
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, c.outRow());
        return runner;
    }

    static Stream<Arguments> delimiterValueCases() {
        return delimCfgs().stream().flatMap(c -> delimValues(c).stream().map(v -> Arguments.of(c, printable(v), v)));
    }

    /**
     * 구분자 관련 값을 헤더/첫·중간·마지막 컬럼/빈 값과 혼합/파일 마지막 행(끝 라인 구분자 유무)에 배치하고,
     * Quote 보존(true/false) × 출력 Quote 모드(NEVER/AS_NEEDED/ALWAYS) 6개 조합 모두에서
     * (1) 출력이 명세대로 정확한지, (2) 출력을 참조 파서로 다시 읽으면 원래 값과 같은지(round-trip) 검증한다.
     * 입력은 Quote가 필요한 값만 Quote하고, "m"은 보존 모드 검증을 위해 불필요해도 항상 Quote한다.
     * NEVER처럼 구조적으로 손실될 수 있는 조합은 출력만 비교하고, AS_NEEDED/ALWAYS는 항상 round-trip 되어야 한다
     * (AS_NEEDED의 구분자 접두 겹침 C-10 포함).
     */
    @ParameterizedTest(name = "[{index}] {0} value={1}")
    @MethodSource("delimiterValueCases")
    void delimiterInValue_allPositions_allQuoteCombos(DelimCfg c, String label, String v) {
        List<List<String>> rows = List.of(
                List.of(v, "name", "city"), // 헤더 행
                List.of(v, "m", "e"),       // 첫 컬럼
                List.of("f", v, "e"),       // 중간 컬럼
                List.of("f", "m", v),       // 마지막 컬럼
                List.of("", v, ""),         // 빈 값과 혼합
                List.of(v, v, v));          // 파일 마지막 행
        List<List<Boolean>> inQuoted = quoteFlagsFor(rows, c.inCol(), c.inRow(), Set.of("m"));
        for (boolean trailing : new boolean[]{true, false}) {
            String in = encodeInput(rows, inQuoted, c.inCol(), c.inRow(), trailing);
            for (boolean preserve : new boolean[]{true, false}) {
                for (String mode : QUOTE_MODES) {
                    boolean rt = roundTrippable(rows, inQuoted, mode, preserve, c.outCol(), c.outRow());
                    boolean lossyByDesign = mode.equals("NEVER");
                    String ctx = c + " value=" + printable(v) + " trailing=" + trailing + " preserve=" + preserve + " mode=" + mode;
                    Assertions.assertTrue(rt || lossyByDesign, "AS_NEEDED/ALWAYS는 round-trip 가능해야 함: " + ctx);
                    TestRunner runner = cfgRunner(c, mode, preserve, "UTF-8");
                    runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
                    runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
                    runner.enqueue(in);
                    runner.run();
                    runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_SUCCESS, 1);
                    MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_SUCCESS).get(0);
                    String out = ff.getContent();
                    Assertions.assertEquals(expectedOutput(rows, inQuoted, mode, preserve, c.outCol(), c.outRow()), out, ctx);
                    Assertions.assertEquals("5", ff.getAttribute("csv.rows.data.in"), ctx);
                    if (rt) {
                        Assertions.assertEquals(rows, refParse(out, c.outCol(), c.outRow()), "round-trip: " + ctx);
                    }
                }
            }
        }
    }

    static Stream<Arguments> cfgPreserveMode() {
        List<Arguments> list = new ArrayList<>();
        for (DelimCfg c : delimCfgs())
            for (boolean preserve : new boolean[]{true, false})
                for (String mode : QUOTE_MODES) list.add(Arguments.of(c, preserve, mode));
        return list.stream();
    }

    /**
     * 설정의 모든 구분자 관련 값을 한 파일(여러 행)에 담아 조합별로 한 번에 변환한다.
     * 값 하나씩 검증하는 위 테스트를 보완하여 행 간 상태가 섞이지 않는지(긴 스트림, 버퍼 경계) 확인한다.
     */
    @ParameterizedTest(name = "[{index}] {0} preserve={1} mode={2}")
    @MethodSource("cfgPreserveMode")
    void allDelimiterValuesInOneFile(DelimCfg c, boolean preserve, String mode) {
        List<List<String>> rows = new ArrayList<>();
        List<String> values = delimValues(c);
        for (int k = 0; k < values.size(); k++) {
            String v = values.get(k);
            String w = values.get((k + 1) % values.size());
            rows.add(List.of(v, w, v));
        }
        // 버퍼(8KB) 경계를 넘기도록 반복
        List<List<String>> all = new ArrayList<>();
        for (int rep = 0; rep < 20; rep++) all.addAll(rows);
        List<List<Boolean>> inQuoted = quoteFlagsFor(all, c.inCol(), c.inRow(), Set.of());
        TestRunner runner = cfgRunner(c, mode, preserve, "UTF-8");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        MockFlowFile ff = runSingle(runner, encodeInput(all, inQuoted, c.inCol(), c.inRow(), true));
        String out = ff.getContent();
        Assertions.assertEquals(expectedOutput(all, inQuoted, mode, preserve, c.outCol(), c.outRow()), out);
        ff.assertAttributeEquals("csv.rows.data.in", String.valueOf(all.size()));
        boolean rt = roundTrippable(all, inQuoted, mode, preserve, c.outCol(), c.outRow());
        Assertions.assertTrue(rt || mode.equals("NEVER"), "AS_NEEDED/ALWAYS는 round-trip 가능해야 함");
        if (rt) {
            Assertions.assertEquals(all, refParse(out, c.outCol(), c.outRow()));
        }
    }

    /**
     * 1문자 라인/컬럼 구분자(단일 "\n" 포함)는 설정 검증에서 invalid가 되고(C-9),
     * EL로 평가되어 1문자가 된 경우에는 실행 시점 검사로 failure로 라우팅된다.
     */
    @ParameterizedTest
    @ValueSource(strings = {"\n", "|", ",", "\r"})
    void singleCharRowDelimiter_routesToFailure(String d) {
        for (var prop : List.of(MultilineCsvParser.IN_ROW_DELIM, MultilineCsvParser.OUT_ROW_DELIM,
                MultilineCsvParser.IN_COL_DELIM, MultilineCsvParser.OUT_COL_DELIM)) {
            TestRunner invalid = newRunner("NEVER", true);
            invalid.setProperty(prop, d);
            invalid.assertNotValid();

            TestRunner runner = newRunner("NEVER", true);
            runner.setProperty(prop, "${d}");
            runner.assertValid();
            runner.enqueue("a^|b@@\n", Map.of("d", d));
            runner.run();
            runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
            runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0)
                    .assertAttributeEquals("csv.error", "Delimiters must be length >= 2");
        }
    }

    /** 새 행이 시작될 때 Quote/필드 상태가 초기화되어야 한다: 앞 행의 마지막 필드가 Quote여도 다음 행 첫 필드는 일반 필드. */
    @Test
    public void quotedStateResetsBetweenRows() {
        TestRunner runner = newRunner("AS_NEEDED", false);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        MockFlowFile ff = runSingle(runner, "\"a\"^|\"b|\"@@\nc|^|d@@\n");
        ff.assertContentEquals("a^|b|@@\nc|^|d@@\n");
        Assertions.assertEquals(List.of(List.of("a", "b|"), List.of("c|", "d")), refParse(ff.getContent(), "^|", "@@\n"));
    }

    /**
     * [C-10 수정 확인] AS_NEEDED가 Quote 문자나 구분자 "전체"를 포함하는 값만 Quote하여, 값의 끝이 자기 겹침 구분자의
     * 접두와 겹치면(예: 구분자 "||"에서 값 "a|", 라인 구분자 "##"에서 마지막 컬럼 값 "y#") Quote 없이 기록되고
     * 다시 읽으면 [a, |b]처럼 다른 값이 되던 결함. C-2(파서 측)와 같은 원인의 Writer 측 결함.
     */
    @Test
    public void bug_asNeeded_valueEndingWithOverlappingDelimiterPrefix_notQuoted() {
        TestRunner runner = newRunner("AS_NEEDED", false);
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, "||");
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, "||");
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, "##");
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, "##");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        MockFlowFile ff = runSingle(runner, "\"a|\"||b##x||\"y#\"##");
        ff.assertContentEquals("\"a|\"||b##x||\"y#\"##");
        Assertions.assertEquals(List.of(List.of("a|", "b"), List.of("x", "y#")), refParse(ff.getContent(), "||", "##"));
    }

    /* ==================== 한글 인코딩 ==================== */

    private static final List<String> KO_CHARSETS = List.of("EUC-KR", "UTF-8", "MS949", "x-windows-949", "CP949");
    /** EUC-KR(KS X 1001)에 없고 CP949(MS949, 통합형 한글)에만 있는 확장 한글. */
    private static final String EXT_HANGUL = "똠쀍햏";

    /**
     * JVM이 한글 문자셋 별칭을 지원하는지 확인하고, 별칭이 실제로 어떤 문자셋으로 해석되는지 명시한다.
     * 주의: Java에서 "CP949"는 MS949(x-windows-949)가 아니라 IBM949(x-IBM949)로 해석되며,
     * x-IBM949는 확장 한글(똠/쀍/햏 등 8,822자)을 지원하지 않는다. 그래서 처리기는 "CP949"를 x-windows-949로 해석한다(C-11).
     */
    @Test
    public void koreanCharsets_supportedAndAliasResolution() {
        for (String cs : KO_CHARSETS) Assertions.assertTrue(Charset.isSupported(cs), cs + " 미지원");
        Assertions.assertEquals("x-windows-949", Charset.forName("MS949").name());
        Assertions.assertEquals("x-windows-949", Charset.forName("x-windows-949").name());
        Assertions.assertEquals("x-IBM949", Charset.forName("CP949").name());
        Assertions.assertEquals("EUC-KR", Charset.forName("EUC-KR").name());
        Assertions.assertTrue(Charset.forName("MS949").newEncoder().canEncode(EXT_HANGUL));
        Assertions.assertFalse(Charset.forName("CP949").newEncoder().canEncode(EXT_HANGUL));
        Assertions.assertFalse(Charset.forName("EUC-KR").newEncoder().canEncode(EXT_HANGUL));
        // 처리기는 "CP949"를 x-windows-949로 해석한다(C-11).
        Assertions.assertEquals("x-windows-949", MultilineCsvParser.resolveCharset("CP949").name());
        Assertions.assertTrue(MultilineCsvParser.resolveCharset("CP949").newEncoder().canEncode(EXT_HANGUL));
    }

    private static List<List<String>> koreanRows() {
        return List.of(
                List.of("이름", "주소^|상세", "메모"),                   // 헤더에 구분자
                List.of("홍길동", "서울^|강남구", "한글\n줄바꿈\r\n둘째줄"),
                List.of("\"인용\"된 한글", "", "가나@@\n다라"),           // Quote + 라인 구분자
                List.of("끝", "한글\r\n", "값^|\"혼합\"@@\n끝"));
    }

    static Stream<Arguments> koreanCharsetCombos() {
        List<Arguments> list = new ArrayList<>();
        for (String cs : KO_CHARSETS)
            for (boolean preserve : new boolean[]{true, false})
                for (String mode : QUOTE_MODES) list.add(Arguments.of(cs, preserve, mode));
        return list.stream();
    }

    /**
     * 입력/출력 문자셋을 같은 한글 인코딩으로 두고, 일반 한글·한글+구분자·한글+줄바꿈·Quote 안의 한글을
     * 6개 Quote 조합으로 변환했을 때 출력 바이트가 명세와 정확히 같고 round-trip 되는지 검증한다.
     * "홍길동"은 불필요해도 Quote하여 Quote 안의 한글(보존 모드)도 확인한다.
     */
    @ParameterizedTest(name = "[{index}] {0} preserve={1} mode={2}")
    @MethodSource("koreanCharsetCombos")
    void koreanValues_sameCharset_byteExact(String cs, boolean preserve, String mode) {
        Charset charset = MultilineCsvParser.resolveCharset(cs);
        List<List<String>> rows = koreanRows();
        List<List<Boolean>> inQuoted = quoteFlagsFor(rows, IN_COL, IN_ROW, Set.of("홍길동"));
        TestRunner runner = newRunner(mode, preserve);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, cs);
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, cs);
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        MockFlowFile ff = runSingle(runner, encodeInput(rows, inQuoted, IN_COL, IN_ROW, true).getBytes(charset), Collections.emptyMap());
        byte[] expected = expectedOutput(rows, inQuoted, mode, preserve, IN_COL, IN_ROW).getBytes(charset);
        Assertions.assertArrayEquals(expected, ff.toByteArray());
        if (roundTrippable(rows, inQuoted, mode, preserve, IN_COL, IN_ROW)) {
            Assertions.assertEquals(rows, refParse(new String(ff.toByteArray(), charset), IN_COL, IN_ROW));
        }
    }

    /** 입력/출력 문자셋이 다른 변환: 출력 바이트가 같은 텍스트를 출력 문자셋으로 인코딩한 것과 정확히 같아야 한다. */
    @ParameterizedTest(name = "[{index}] {0} -> {1}")
    @CsvSource({"CP949,UTF-8", "UTF-8,EUC-KR", "EUC-KR,UTF-8", "MS949,CP949", "UTF-8,MS949",
            "EUC-KR,x-windows-949", "x-windows-949,EUC-KR", "CP949,EUC-KR", "UTF-8,CP949", "MS949,UTF-8"})
    void koreanValues_charsetConversion(String inCs, String outCs) {
        List<List<String>> rows = koreanRows();
        List<List<Boolean>> inQuoted = quoteFlagsFor(rows, IN_COL, IN_ROW, Set.of("홍길동"));
        for (boolean preserve : new boolean[]{true, false}) {
            TestRunner runner = newRunner("AS_NEEDED", preserve);
            runner.setProperty(MultilineCsvParser.INPUT_CHARSET, inCs);
            runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, outCs);
            runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
            MockFlowFile ff = runSingle(runner, encodeInput(rows, inQuoted, IN_COL, IN_ROW, true).getBytes(MultilineCsvParser.resolveCharset(inCs)),
                    Collections.emptyMap());
            Assertions.assertArrayEquals(expectedOutput(rows, inQuoted, "AS_NEEDED", preserve, IN_COL, IN_ROW).getBytes(MultilineCsvParser.resolveCharset(outCs)),
                    ff.toByteArray(), inCs + "->" + outCs + " preserve=" + preserve);
            ff.assertAttributeEquals("csv.input.charset", MultilineCsvParser.resolveCharset(inCs).name());
        }
    }

    /** MS949(x-windows-949) 입력의 확장 한글은 같은 문자셋 출력에서 바이트 그대로, UTF-8 출력에서 정확한 문자로 변환된다. */
    @ParameterizedTest
    @ValueSource(strings = {"MS949", "x-windows-949"})
    void extendedHangul_ms949_preservedAndConvertedToUtf8(String cs) {
        String text = "\"" + EXT_HANGUL + "\"^|\"\"\"똠\"\"\"^|햏@@\n쀍^|\"똠\n햏\"^|" + EXT_HANGUL + "@@\n";
        byte[] in = text.getBytes(MultilineCsvParser.resolveCharset(cs));
        TestRunner same = newRunner("NEVER", true);
        same.setProperty(MultilineCsvParser.INPUT_CHARSET, cs);
        same.setProperty(MultilineCsvParser.OUTPUT_CHARSET, cs);
        same.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        Assertions.assertArrayEquals(in, runSingle(same, in, Collections.emptyMap()).toByteArray());

        TestRunner toUtf8 = newRunner("NEVER", false);
        toUtf8.setProperty(MultilineCsvParser.INPUT_CHARSET, cs);
        toUtf8.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        runSingle(toUtf8, in, Collections.emptyMap())
                .assertContentEquals(EXT_HANGUL + "^|\"똠\"^|햏@@\n쀍^|똠\n햏^|" + EXT_HANGUL + "@@\n", StandardCharsets.UTF_8);
    }

    /** [C-4 수정 확인] EUC-KR 출력에서 표현 불가 확장 한글은 '?'로 치환되지 않고 failure(문자/위치 포함)로 라우팅된다. */
    @Test
    public void bug_extendedHangul_toEucKr_silentlyReplaced() throws IOException {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "MS949");
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "EUC-KR");
        byte[] in = ("가^|" + EXT_HANGUL + "@@\n").getBytes(Charset.forName("MS949"));
        runner.enqueue(in);
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        ff.assertContentEquals(in);
        Assertions.assertEquals("Output charset EUC-KR cannot encode character '똠' (U+B620) in data row 1, column 2",
                ff.getAttribute("csv.error"));
    }

    /**
     * [C-11 수정 확인] 입력 문자셋 "CP949"(기본값)를 MS949(x-windows-949)로 해석하므로, Windows가 만든 CP949 파일의
     * 확장 한글 바이트(예: '똠' = 8C 63)가 정상 디코딩된다(수정 전: x-IBM949로 해석되어 U+FFFD + 'c').
     */
    @Test
    public void cp949Alias_decodesExtendedHangul() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "CP949");
        MockFlowFile ff = runSingle(runner, ("a^|" + EXT_HANGUL + "@@\n").getBytes(Charset.forName("MS949")), Collections.emptyMap());
        ff.assertContentEquals("a^|" + EXT_HANGUL + "@@\n", StandardCharsets.UTF_8);
        ff.assertAttributeEquals("csv.input.charset", "x-windows-949");
    }

    /**
     * [C-11 수정 확인] 기본 입력 문자셋 "CP949"가 x-IBM949로 해석되어 확장 한글이 깨지고, 깨진 바이트의 trail byte가
     * ASCII 문자('c' 등)로 노출되어 문자 구분자와 겹치면 컬럼이 잘못 분할되던 결함.
     */
    @Test
    public void bug_cp949Alias_extendedHangulCorruptedAndSplit() {
        TestRunner runner = newRunner("NEVER", false);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "CP949");
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, "cX");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        MockFlowFile ff = runSingle(runner, "똠XcX햏@@\n".getBytes(Charset.forName("MS949")), Collections.emptyMap());
        ff.assertContentEquals("똠X^|햏@@\n", StandardCharsets.UTF_8);
    }

    static Stream<Arguments> cp949NameVariants() {
        List<Arguments> list = new ArrayList<>();
        for (String name : List.of("CP949", "cp949", " CP949 ", "Cp949")) {
            list.add(Arguments.of(name, false));
            list.add(Arguments.of(name, true)); // EL 평가 결과로 전달
        }
        return list.stream();
    }

    /**
     * [C-11] "CP949"(대소문자/앞뒤 공백 무관, EL 평가 결과 포함)를 입력·출력 문자셋으로 쓰면 MS949와 같게 동작한다:
     * 확장 한글 입력→출력 바이트가 MS949 인코딩과 동일하고, UTF-8 입력을 CP949로 출력해도 확장 한글이 정상 인코딩된다.
     */
    @ParameterizedTest(name = "[{index}] name=''{0}'' viaEL={1}")
    @MethodSource("cp949NameVariants")
    void cp949Names_behaveLikeMs949(String name, boolean viaEl) {
        Charset ms949 = Charset.forName("MS949");
        Map<String, String> attrs = viaEl ? Map.of("cs", name) : Collections.emptyMap();
        String prop = viaEl ? "${cs}" : name;
        String text = "\"" + EXT_HANGUL + "^|가\"^|똠@@\n쀍^|\"햏\n똠\"@@\n";

        TestRunner same = newRunner("NEVER", true);
        same.setProperty(MultilineCsvParser.INPUT_CHARSET, prop);
        same.setProperty(MultilineCsvParser.OUTPUT_CHARSET, prop);
        same.assertValid();
        byte[] in = text.getBytes(ms949);
        MockFlowFile ff = runSingle(same, in, attrs);
        Assertions.assertArrayEquals(in, ff.toByteArray());
        ff.assertAttributeEquals("csv.input.charset", "x-windows-949");

        TestRunner fromUtf8 = newRunner("NEVER", true);
        fromUtf8.setProperty(MultilineCsvParser.INPUT_CHARSET, "UTF-8");
        fromUtf8.setProperty(MultilineCsvParser.OUTPUT_CHARSET, prop);
        Assertions.assertArrayEquals(in, runSingle(fromUtf8, text.getBytes(StandardCharsets.UTF_8), attrs).toByteArray());
    }

    /** CP949 외의 이름은 기존대로 해석된다: x-IBM949를 명시하면 IBM949, MS949/EUC-KR/UTF-8은 그대로. */
    @Test
    public void otherCharsetNames_resolvedAsBefore() {
        Assertions.assertEquals("x-IBM949", MultilineCsvParser.resolveCharset("x-IBM949").name());
        Assertions.assertEquals("x-IBM949", MultilineCsvParser.resolveCharset("IBM949").name());
        Assertions.assertEquals("x-windows-949", MultilineCsvParser.resolveCharset("MS949").name());
        Assertions.assertEquals("EUC-KR", MultilineCsvParser.resolveCharset("EUC-KR").name());
        Assertions.assertEquals("UTF-8", MultilineCsvParser.resolveCharset("UTF-8").name());
        Assertions.assertEquals("x-windows-949", MultilineCsvParser.resolveCharset(" cP949 ").name());

        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "x-IBM949");
        runSingle(runner, "가^|나@@\n".getBytes(Charset.forName("x-IBM949")), Collections.emptyMap())
                .assertAttributeEquals("csv.input.charset", "x-IBM949");

        TestRunner invalid = newRunner("NEVER", true);
        invalid.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "CP949X");
        invalid.assertNotValid();
    }

    /**
     * 바이트 수준 검증: MS949의 한글 음절(11,172자) trail byte는 영문자(0x41-0x5A, 0x61-0x7A) 또는 0x81 이상이며
     * '\\'(0x5C), '|'(0x7C), '^'(0x5E), '"', '@', CR/LF와 겹치지 않는다. EUC-KR 한글은 두 바이트 모두 0xA1 이상이다.
     */
    @Test
    public void koreanTrailBytes_neverCollideWithAsciiDelimiterSymbols() {
        Charset ms949 = Charset.forName("MS949"), euckr = Charset.forName("EUC-KR");
        Set<Integer> forbidden = Set.of(0x5C, 0x7C, 0x5E, 0x22, 0x40, 0x0A, 0x0D, 0x23, 0x3C, 0x3E);
        int letterTrails = 0;
        for (char ch = 0xAC00; ch <= 0xD7A3; ch++) {
            byte[] b = String.valueOf(ch).getBytes(ms949);
            Assertions.assertEquals(2, b.length);
            int trail = b[1] & 0xFF;
            Assertions.assertFalse(forbidden.contains(trail), ch + " trail=" + Integer.toHexString(trail));
            if (trail < 0x80) letterTrails++;
            if (euckr.newEncoder().canEncode(ch)) {
                byte[] e = String.valueOf(ch).getBytes(euckr);
                Assertions.assertTrue((e[0] & 0xFF) >= 0xA1 && (e[1] & 0xFF) >= 0xA1, String.valueOf(ch));
            }
        }
        Assertions.assertTrue(letterTrails > 0, "MS949에는 ASCII 영문자 trail byte를 갖는 확장 한글이 있어야 함");
    }

    /**
     * MS949 확장 한글의 trail byte가 영문자일 때, 그 바이트와 같은 문자로 된 구분자(예: 컬럼 "cX", 라인 "d@")를
     * 써도 파서가 문자 단위로 동작하므로 오분할되지 않는다. '똠'(8C 63)+'X' 바이트열에 "cX"가, '햏'(C1 64)+'@'에 "d@"가 들어 있다.
     */
    @ParameterizedTest
    @ValueSource(strings = {"MS949", "x-windows-949"})
    void ms949_trailByteMatchingDelimiterBytes_notSplit(String cs) {
        Charset charset = MultilineCsvParser.resolveCharset(cs);
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, cs);
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, cs);
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, "cX");
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, "cX");
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, "d@");
        runner.setProperty(MultilineCsvParser.OUT_ROW_DELIM, "d@");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        byte[] in = "똠XcX햏@d@쀍cX\"똠X햏@\"d@".getBytes(charset);
        MockFlowFile ff = runSingle(runner, in, Collections.emptyMap());
        Assertions.assertArrayEquals(in, ff.toByteArray());
        ff.assertAttributeEquals("csv.rows.data.in", "2");
    }

    /**
     * 각 문자셋이 표현 가능한 한글 음절 전체(MS949 11,172자, EUC-KR 2,350자 등)를 여러 필드/행으로 나눠 넣고
     * (8KB 버퍼 경계를 여러 번 넘음) 같은 문자셋 출력은 바이트 그대로, UTF-8 출력은 같은 텍스트인지 검증한다.
     */
    @ParameterizedTest
    @ValueSource(strings = {"EUC-KR", "UTF-8", "MS949", "x-windows-949", "CP949"})
    void allEncodableHangulSyllables_passthroughAndToUtf8(String cs) {
        Charset charset = MultilineCsvParser.resolveCharset(cs);
        var enc = charset.newEncoder();
        StringBuilder text = new StringBuilder();
        int n = 0;
        for (char ch = 0xAC00; ch <= 0xD7A3; ch++) {
            if (!enc.canEncode(ch)) continue;
            text.append(ch);
            n++;
            if (n % 37 == 0) text.append(n % 74 == 0 ? IN_ROW : IN_COL);
        }
        text.append(IN_ROW);
        byte[] in = text.toString().getBytes(charset);

        TestRunner same = newRunner("NEVER", true);
        same.setProperty(MultilineCsvParser.INPUT_CHARSET, cs);
        same.setProperty(MultilineCsvParser.OUTPUT_CHARSET, cs);
        Assertions.assertArrayEquals(in, runSingle(same, in, Collections.emptyMap()).toByteArray());

        TestRunner toUtf8 = newRunner("NEVER", true);
        toUtf8.setProperty(MultilineCsvParser.INPUT_CHARSET, cs);
        runSingle(toUtf8, in, Collections.emptyMap()).assertContentEquals(text.toString(), StandardCharsets.UTF_8);
    }

    /* -------------------- 확인된 결함(BUG) 재현 테스트 -------------------- */

    /**
     * [C-1 수정 확인] 입력 Quote 보존(true) + AS_NEEDED(또는 ALWAYS)에서 이미 Quote가 포함된 값을 다시 Quote/escape하여
     * "x" 가 """x""" 로 바뀌던 결함. 수정 후에는 원래 Quote된 필드가 한 번만 감싸져야 한다.
     */
    @Test
    public void bug_preserveQuotesWithAsNeeded_doubleQuotesValues() {
        TestRunner runner = newRunner("AS_NEEDED", true);
        MockFlowFile ff = runSingle(runner, "\"x^|y\"^|z@@\n");
        ff.assertContentEquals("\"x^|y\"^|z@@\n");
    }

    /**
     * [C-2 수정 확인] 입력 Quote 제거(false) 모드에서 Quote 종료 후 구분자 탐지를 "Quote 내용 + 이후 문자" 버퍼 끝에서 하여,
     * 자기 겹침(border)이 있는 구분자("||")를 쓰면 Quote 값 끝의 '|'가 구분자로 오인되던 결함.
     * "a|"||b  →  기대 [a|, b] (수정 전 실제 [a, |b] - 컬럼 수는 같아 성공으로 라우팅되는 조용한 데이터 손상).
     */
    @Test
    public void bug_quotedValueEndingWithDelimiterPrefix_splitWrongly() {
        TestRunner runner = newRunner("ALWAYS", false);
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, "||");
        runner.setProperty(MultilineCsvParser.OUT_COL_DELIM, "||");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        MockFlowFile ff = runSingle(runner, "\"a|\"||b@@\n");
        ff.assertContentEquals("\"a|\"||\"b\"@@\n");
    }

    /**
     * [C-6 수정 확인] UTF-8 BOM이 제거되지 않아 헤더 첫 컬럼명이 "﻿id"가 되고, 첫 필드가 Quote로 시작하면
     * Quote 인식도 실패한다.
     */
    @Test
    public void bug_utf8Bom_notStripped() {
        TestRunner runner = newRunner("NEVER", false);
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        byte[] body = "\"i^|d\"^|name@@\n1^|a@@\n".getBytes(StandardCharsets.UTF_8);
        byte[] in = new byte[body.length + 3];
        in[0] = (byte) 0xEF;
        in[1] = (byte) 0xBB;
        in[2] = (byte) 0xBF;
        System.arraycopy(body, 0, in, 3, body.length);
        MockFlowFile ff = runSingle(runner, in, Collections.emptyMap());
        ff.assertContentEquals("i^|d^|name@@\n1^|a@@\n");
    }

    /**
     * [C-3 수정 확인] 닫히지 않은 Quote가 있으면 나머지 파일 전체가 한 필드로 흡수되는데, 오류 없이 success로 라우팅된다
     * (EXPECTED_COLUMNS=0). 또한 이 경우 필드 버퍼가 파일 끝까지 메모리에 누적된다.
     */
    @Test
    public void bug_unterminatedQuote_silentlySwallowsRestOfFile() {
        TestRunner runner = newRunner("NEVER", true);
        String in = "a^|\"b@@\nc^|d@@\ne^|f@@\n";
        runner.enqueue(in);
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        ff.assertContentEquals(in); // 원본 유지
        Assertions.assertEquals("Unterminated quote: quote opened at record 1, field 2 (character offset 3) "
                + "is not closed before end of input", ff.getAttribute("csv.error"));
    }

    /**
     * [C-4 수정 확인] 입력 문자셋과 맞지 않는 바이트(잘못된 UTF-8)는 U+FFFD로 조용히 치환되어 success로 라우팅된다.
     */
    @Test
    public void bug_malformedInputBytes_silentlyReplaced() throws IOException {
        TestRunner runner = newRunner("NEVER", true);
        byte[] in = {'a', '^', '|', (byte) 0xFF, (byte) 0xFE, '@', '@', '\n'};
        runner.enqueue(in);
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        ff.assertContentEquals(in);
        Assertions.assertEquals("Input charset UTF-8 cannot decode malformed byte sequence (length=1) at byte offset 3 "
                + "in record 1 (1-based, including skipped/header rows)", ff.getAttribute("csv.error"));
    }

    /**
     * [C-4 수정 확인] 출력 문자셋으로 표현할 수 없는 문자는 '?'로 조용히 치환되어 success로 라우팅된다.
     */
    @Test
    public void bug_unmappableOutputChars_silentlyReplaced() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "ISO-8859-1");
        runner.enqueue("x^|y@@\n홍길동^|a@@\n");
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        ff.assertContentEquals("x^|y@@\n홍길동^|a@@\n");
        Assertions.assertEquals("Output charset ISO-8859-1 cannot encode character '홍' (U+D64D) in data row 2, column 1",
                ff.getAttribute("csv.error"));
    }

    /**
     * [C-5 수정 확인] EL로 평가한 문자셋이 잘못되면 Charset.forName 예외가 try 블록 밖(onTrigger 초반)에서 발생해
     * failure로 라우팅되지 않고 세션이 롤백된다(FlowFile이 큐에서 무한 재시도).
     */
    @Test
    public void bug_invalidCharsetFromEL_notRoutedToFailure() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, "${cs}");
        runner.enqueue("a^|b@@\n", Map.of("cs", "NO-SUCH-CHARSET"));
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        Assertions.assertTrue(ff.isPenalized());
        Assertions.assertTrue(ff.getAttribute("csv.error").contains("NO-SUCH-CHARSET"), ff.getAttribute("csv.error"));
    }

    /**
     * [C-5 수정 확인] EL로 평가한 기대 컬럼 수가 정수가 아니면 NumberFormatException이 try 밖에서 발생해 failure로 라우팅되지 않는다.
     */
    @Test
    public void bug_nonIntegerExpectedColumnsFromEL_notRoutedToFailure() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "${cols}");
        runner.enqueue("a^|b@@\n", Map.of("cols", "two"));
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        Assertions.assertTrue(ff.isPenalized());
        ff.assertContentEquals("a^|b@@\n");
        Assertions.assertTrue(ff.getAttribute("csv.error").startsWith("Failed to evaluate properties"), ff.getAttribute("csv.error"));
    }

    /**
     * [C-7 수정 확인] 컬럼 수 불일치 시 오류 메시지에 행 전체 내용을 그대로 넣어 csv.error 속성이 무제한으로 커진다
     * (대형 멀티라인 필드/닫히지 않은 Quote 시 수 MB 속성 → FlowFile Repository/힙 압박).
     */
    @Test
    public void bug_csvErrorAttribute_unbounded() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 200_000; i++) big.append('x');
        runner.enqueue("a^|" + big + "@@\n");
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        String err = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0).getAttribute("csv.error");
        Assertions.assertTrue(err.length() < 8192, "csv.error length=" + err.length());
        Assertions.assertTrue(err.length() <= 1024 + 32, "csv.error length=" + err.length());
        Assertions.assertTrue(err.startsWith("Column count mismatch at data row 1"), err);
        Assertions.assertTrue(err.matches("(?s).*\\.\\.\\.\\(생략, 총 \\d+자\\)$"), err.substring(err.length() - 40));
    }

    /* -------------------- C-3 ~ C-7 추가 검증 -------------------- */

    /** [C-3] Quote 안의 Quote 종료 직전에 EOF(닫힘)면 정상, 데이터 행 중간의 닫히지 않은 Quote는 레코드/필드 위치와 함께 failure. */
    @Test
    public void unterminatedQuote_positionInLaterRecord() {
        TestRunner ok = newRunner("NEVER", true);
        runSingle(ok, "a^|\"b\"").assertContentEquals("a^|\"b\"@@\n");

        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.SKIP_LEADING_ROWS, "1");
        String in = "skip^|x@@\nh1^|h2@@\n1^|2^|\"open \"\" still@@\n3^|4@@\n";
        runner.enqueue(in);
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        ff.assertContentEquals(in);
        Assertions.assertFalse(ff.isPenalized());
        Assertions.assertEquals("Unterminated quote: quote opened at record 3, field 3 (character offset " + in.indexOf("\"open")
                + ") is not closed before end of input", ff.getAttribute("csv.error"));
    }

    /**
     * [C-4] 디코딩 오류 위치는 디코더 버퍼(8KB)와 무관하게 정확해야 한다: 수천 행 뒤의 잘못된 바이트도
     * 해당 레코드 번호와 원본 바이트 오프셋으로 보고된다(UTF-8 BOM이 있으면 BOM 포함 오프셋).
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void malformedInput_exactRecordAndByteOffset(boolean withBom) throws IOException {
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        if (withBom) bos.write(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF});
        for (int i = 1; i < 3000; i++) bos.write(("행" + i + "^|값@@\n").getBytes(StandardCharsets.UTF_8));
        bos.write("bad^|".getBytes(StandardCharsets.UTF_8));
        int offset = bos.size();
        bos.write(new byte[]{(byte) 0xED, (byte) 0xA0, (byte) 0x80}); // UTF-8로 인코딩된 서로게이트(잘못된 UTF-8)
        bos.write("@@\n끝^|x@@\n".getBytes(StandardCharsets.UTF_8));
        TestRunner runner = newRunner("NEVER", true);
        runner.enqueue(bos.toByteArray());
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        String err = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0).getAttribute("csv.error");
        Assertions.assertTrue(err.startsWith("Input charset UTF-8 cannot decode malformed byte sequence"), err);
        Assertions.assertTrue(err.contains("at byte offset " + offset + " in record 3000 "), err);
    }

    /** [C-4] 입력 문자셋에서 정의되지 않은 바이트(EUC-KR의 0xFF 등)와 불완전한 멀티바이트로 끝나는 입력도 failure. */
    @ParameterizedTest
    @CsvSource({"EUC-KR,'61,5E,7C,FF,FF,40,40,0A'", "MS949,'61,5E,7C,B0,40,40,0A'", "UTF-8,'61,5E,7C,EA,B0'", "MS949,'61,5E,7C,B0'"})
    void invalidKoreanBytes_routeToFailure(String cs, String hex) throws IOException {
        String[] parts = hex.split(",");
        byte[] in = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) in[i] = (byte) Integer.parseInt(parts[i], 16);
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.INPUT_CHARSET, cs);
        runner.enqueue(in);
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
        ff.assertContentEquals(in);
        Assertions.assertTrue(ff.getAttribute("csv.error").contains("at byte offset 3 in record 1"), ff.getAttribute("csv.error"));
    }

    /** [C-4] 헤더 행의 표현 불가 문자도 위치(header row)와 함께 failure. */
    @Test
    public void unmappableOutputChar_inHeader() {
        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "US-ASCII");
        runner.setProperty(MultilineCsvParser.READ_HEADER_AFTER_SKIP, "true");
        runner.enqueue("id^|이름@@\n1^|a@@\n");
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        Assertions.assertEquals("Output charset US-ASCII cannot encode character '이' (U+C774) in header row, column 2",
                runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0).getAttribute("csv.error"));
    }

    /** [C-4] 보조 평면 문자(서로게이트 쌍)는 UTF-8 → UTF-8에서 정상 처리되고, 표현 불가 문자셋에서는 코드 포인트로 보고된다. */
    @Test
    public void supplementaryCharacters() {
        TestRunner ok = newRunner("NEVER", true);
        runSingle(ok, "\uD83D\uDE00^|a@@\n").assertContentEquals("\uD83D\uDE00^|a@@\n");

        TestRunner runner = newRunner("NEVER", true);
        runner.setProperty(MultilineCsvParser.OUTPUT_CHARSET, "MS949");
        runner.enqueue("a^|b\uD83D\uDE00@@\n");
        runner.run();
        runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
        Assertions.assertEquals("Output charset x-windows-949 cannot encode character '\uD83D\uDE00' (U+1F600) in data row 1, column 2",
                runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0).getAttribute("csv.error"));
    }

    /** [C-5] Quote 문자가 EL로 빈 문자열이 되거나 출력 문자셋 EL이 잘못되면 penalize 후 failure(원본 유지). */
    @Test
    public void invalidElValues_penalizedFailure() {
        for (var prop : List.of(MultilineCsvParser.IN_QUOTE, MultilineCsvParser.OUT_QUOTE,
                MultilineCsvParser.OUTPUT_CHARSET, MultilineCsvParser.SKIP_LEADING_ROWS)) {
            TestRunner runner = newRunner("NEVER", true);
            runner.setProperty(prop, "${v}");
            runner.enqueue("a^|b@@\n", Map.of("v", prop == MultilineCsvParser.SKIP_LEADING_ROWS ? "x" : ""));
            runner.run();
            runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
            MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
            Assertions.assertTrue(ff.isPenalized(), prop.getName());
            ff.assertContentEquals("a^|b@@\n");
            Assertions.assertTrue(ff.getAttribute("csv.error").startsWith("Failed to evaluate properties"), prop.getName());
        }
    }

    /** [C-6] BOM 제거는 UTF-8 입력에만 적용된다. BOM이 없는 UTF-8과 다른 문자셋의 EF BB BF 바이트는 그대로 둔다. */
    @Test
    public void bom_strippedOnlyForUtf8() {
        byte[] bomA = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, 'a', '^', '|', 'b', '@', '@', '\n'};
        TestRunner utf8 = newRunner("NEVER", true);
        runSingle(utf8, bomA, Collections.emptyMap()).assertContentEquals("a^|b@@\n");

        TestRunner latin1 = newRunner("NEVER", true);
        latin1.setProperty(MultilineCsvParser.INPUT_CHARSET, "ISO-8859-1");
        runSingle(latin1, bomA, Collections.emptyMap()).assertContentEquals("\u00EF\u00BB\u00BFa^|b@@\n", StandardCharsets.UTF_8);

        TestRunner noBom = newRunner("NEVER", true);
        runSingle(noBom, "a^|b@@\n").assertContentEquals("a^|b@@\n");

        // BOM만 있는 입력 = 빈 입력
        TestRunner onlyBom = newRunner("NEVER", true);
        runSingle(onlyBom, new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, Collections.emptyMap()).assertContentEquals("");
    }

    /** [C-7] 잘라내기 경계: 1024자 이하 메시지는 그대로, 초과 시 1024자 + 생략 표기, 서로게이트 쌍은 가르지 않는다. */
    @Test
    public void truncateError_boundaries() {
        String exact = "x".repeat(MultilineCsvParser.MAX_ERROR_LENGTH);
        Assertions.assertEquals(exact, MultilineCsvParser.truncateError(exact));
        Assertions.assertEquals(exact + "...(생략, 총 1025자)", MultilineCsvParser.truncateError(exact + "y"));
        String surrogate = "x".repeat(MultilineCsvParser.MAX_ERROR_LENGTH - 1) + "\uD83D\uDE00zz";
        Assertions.assertEquals("x".repeat(MultilineCsvParser.MAX_ERROR_LENGTH - 1) + "...(생략, 총 1027자)",
                MultilineCsvParser.truncateError(surrogate));
        Assertions.assertEquals("", MultilineCsvParser.truncateError(null));
    }

    /**
     * [C-8 수정 확인] Preserve Input Quotes / Read Header After Skip은 EL 지원을 선언했지만 allowableValues("true","false")
     * 때문에 "${...}" 표현식이 항상 invalid여서 EL을 실제로 쓸 수 없었다. EL 지원 선언을 제거하여 문서와 동작을 일치시켰다:
     * EL 미지원으로 표시되고, 표현식은 invalid, true/false만 valid.
     */
    @Test
    public void bug_booleanPropertiesDeclareElButRejectExpressions() {
        for (var prop : List.of(MultilineCsvParser.READ_HEADER_AFTER_SKIP, MultilineCsvParser.PRESERVE_INPUT_QUOTES)) {
            Assertions.assertFalse(prop.isExpressionLanguageSupported(), prop.getName());
            Assertions.assertEquals(org.apache.nifi.expression.ExpressionLanguageScope.NONE, prop.getExpressionLanguageScope(), prop.getName());
            TestRunner runner = newRunner("NEVER", true);
            runner.setProperty(prop, "${v}");
            runner.assertNotValid();
            for (String ok : List.of("true", "false")) {
                runner.setProperty(prop, ok);
                runner.assertValid();
            }
        }
    }

    /* -------------------- C-9 추가 검증 -------------------- */

    /** [C-9] 입력/출력 Quote는 정확히 1문자만 허용(빈 값/2문자 이상은 invalid). EL 값은 실행 시점에 검사하여 penalize 후 failure. */
    @Test
    public void quoteCharacter_mustBeExactlyOneChar() {
        for (var prop : List.of(MultilineCsvParser.IN_QUOTE, MultilineCsvParser.OUT_QUOTE)) {
            TestRunner runner = newRunner("NEVER", true);
            runner.setProperty(prop, "''");
            runner.assertNotValid();
            runner.setProperty(prop, "\"\"");
            runner.assertNotValid();
            runner.setProperty(prop, "'");
            runner.assertValid();
            runner.setProperty(prop, "${q}");
            runner.assertValid();

            runner.enqueue("a^|b@@\n", Map.of("q", "ab"));
            runner.run();
            runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
            MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
            Assertions.assertTrue(ff.isPenalized());
            ff.assertContentEquals("a^|b@@\n");
            Assertions.assertTrue(ff.getAttribute("csv.error").contains(prop.getName() + " must be exactly 1 character but was 'ab'"),
                    ff.getAttribute("csv.error"));
        }
        // EL 값이 정확히 1문자면 정상 동작
        TestRunner runner = newRunner("NEVER", false);
        runner.setProperty(MultilineCsvParser.IN_QUOTE, "${q}");
        runSingle(runner, "'a^|b'^|c@@\n".getBytes(StandardCharsets.UTF_8), Map.of("q", "'")).assertContentEquals("a^|b^|c@@\n");
    }

    /** [C-9] 구분자 설정 검증: 2문자 이상만 valid, EL 값은 설정 단계에서 통과(실행 시 검사). */
    @Test
    public void delimiterValidation() {
        for (var prop : List.of(MultilineCsvParser.IN_COL_DELIM, MultilineCsvParser.IN_ROW_DELIM,
                MultilineCsvParser.OUT_COL_DELIM, MultilineCsvParser.OUT_ROW_DELIM)) {
            TestRunner runner = newRunner("NEVER", true);
            runner.setProperty(prop, "x");
            runner.assertNotValid();
            runner.setProperty(prop, "xy");
            runner.assertValid();
            runner.setProperty(prop, "${d}");
            runner.assertValid();
        }
    }

    static Stream<Arguments> charactersAfterClosingQuote() {
        return Stream.of(
                // 입력, 기대 레코드 번호, 기대 필드 번호, 잘못된 문자
                Arguments.of("\"ab\"cd^|x@@\n", 1, 1, 'c'),
                Arguments.of("a^|\"b\"c@@\n", 1, 2, 'c'),
                Arguments.of("ok^|ok@@\na^|\"b\" @@\n", 2, 2, ' '),
                Arguments.of("a^|\"b\"^x@@\n", 1, 2, 'x'),         // 구분자 접두 뒤 다른 문자
                Arguments.of("a^|\"b\"@x\n", 1, 2, 'x'),           // 행 구분자 접두 뒤 다른 문자
                Arguments.of("a^|\"b\"\"c\"@@\n".replace("\"\"c", "\"x\"c"), 1, 2, 'x'),
                Arguments.of("a^|\"b\"@", 1, 2, '@'),               // 구분자 일부만 남기고 EOF
                Arguments.of("a^|\"b\"^", 1, 2, '^'));
    }

    /**
     * [C-9] 닫는 Quote 뒤에는 컬럼/행 구분자(또는 EOF)만 올 수 있다. 그 외 문자가 오면(예: "ab"cd, 구분자의 일부만 오고
     * 다른 문자가 이어지는 경우, 구분자 일부만 남기고 EOF) Quote 보존/제거 모두 잘못된 CSV로 failure(위치 정보 포함).
     * 수정 전에는 "ab"cd 가 보존 모드에서 "abcd", 제거 모드에서 abcd 로 조용히 합쳐졌다.
     */
    @ParameterizedTest
    @MethodSource("charactersAfterClosingQuote")
    void charactersAfterClosingQuote_routeToFailure(String in, int record, int field, char bad) {
        for (boolean preserve : new boolean[]{true, false}) {
            TestRunner runner = newRunner("NEVER", preserve);
            runner.enqueue(in);
            runner.run();
            runner.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
            MockFlowFile ff = runner.getFlowFilesForRelationship(MultilineCsvParser.REL_FAILURE).get(0);
            ff.assertContentEquals(in);
            Assertions.assertFalse(ff.isPenalized());
            String err = ff.getAttribute("csv.error");
            Assertions.assertTrue(err.startsWith(String.format("Unexpected character U+%04X after closing quote at record %d, field %d ",
                    (int) bad, record, field)), err);
        }
    }

    /** [C-9] 닫는 Quote 직후 구분자 접두가 이어지다 구분자가 완성되면 정상(구분자 접두 문자는 잘못된 문자가 아님). */
    @Test
    public void closingQuoteFollowedByDelimiter_ok() {
        TestRunner runner = newRunner("NEVER", false);
        runner.setProperty(MultilineCsvParser.IN_COL_DELIM, "<|>");
        runner.setProperty(MultilineCsvParser.IN_ROW_DELIM, "<<EOR>>\n");
        runner.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "3");
        runSingle(runner, "\"a\"<|>\"b\"<|>\"c\"<<EOR>>\n\"\"<|><|>\"z\"").assertContentEquals("a^|b^|c@@\n^|^|z@@\n");
    }

    /**
     * [C-9 동작 명시] CRLF 줄바꿈 입력에 LF만 포함된 행 구분자("@@\n")를 지정하면 "@@\r\n"과 일치하지 않아 행이 나뉘지 않는다
     * (자동 변환 없음, 행 구분자 description에 명시). 기대 컬럼 수를 지정하면 컬럼 수 불일치로 실패한다.
     */
    @Test
    public void crlfInput_withLfRowDelimiter_isNotSplit() {
        Assertions.assertTrue(MultilineCsvParser.IN_ROW_DELIM.getDescription().contains("CRLF"));
        TestRunner runner = newRunner("NEVER", true);
        MockFlowFile ff = runSingle(runner, "a^|b@@\r\nc^|d@@\r\n");
        ff.assertAttributeEquals("csv.rows.data.in", "1"); // 한 행(필드 3개)으로 읽힌다
        ff.assertContentEquals("a^|b@@\r\nc^|d@@\r\n@@\n");

        TestRunner strict = newRunner("NEVER", true);
        strict.setProperty(MultilineCsvParser.EXPECTED_COLUMNS, "2");
        strict.enqueue("a^|b@@\r\nc^|d@@\r\n");
        strict.run();
        strict.assertAllFlowFilesTransferred(MultilineCsvParser.REL_FAILURE, 1);
    }
}
