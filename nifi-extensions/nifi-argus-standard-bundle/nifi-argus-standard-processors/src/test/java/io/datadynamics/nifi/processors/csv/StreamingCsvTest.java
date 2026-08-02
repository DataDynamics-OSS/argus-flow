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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * {@link StreamingCsv}의 저수준 스트림 파이프라인(Pipeline.run)을 직접 호출하여
 * 인코딩 변환, 헤더/스킵 처리, 컬럼 수 검증, 구분자 변경, 스트림 close 옵션 등을 검증하는 테스트.
 */
public class StreamingCsvTest {

    @TempDir
    Path tmpDir;

    private static final String IN_COL = "^|";
    private static final String IN_ROW = "@@\n";

    /* ========================= 유틸리티 ========================= */

    private static byte[] buildBytes(Charset cs, String colDelim, String rowDelim, List<List<String>> rows) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try (Writer w = new BufferedWriter(new OutputStreamWriter(bout, cs), 8192)) {
            for (List<String> r : rows) {
                for (int i = 0; i < r.size(); i++) {
                    if (i > 0) w.write(colDelim);
                    w.write(r.get(i));
                }
                w.write(rowDelim);
            }
        }
        return bout.toByteArray();
    }

    private static int countOccurrences(String text, String token) {
        int cnt = 0, from = 0;
        while (true) {
            int idx = text.indexOf(token, from);
            if (idx < 0) break;
            cnt++;
            from = idx + token.length();
        }
        return cnt;
    }

    /* ========================= 테스트 ========================= */

    /**
     * 1) InputStream/OutputStream 기반: 선행 2행 스킵 후 헤더 기록, CP949 → UTF-8 변환 검증
     */
    @Test
    public void stream_skipAndHeader_encoding_CP949_to_UTF8() throws Exception {
        List<String> header = Arrays.asList("id", "name", "city", "amount", "date", "status", "memo", "note", "json", "email");

        byte[] src = buildBytes(
                Charset.forName("CP949"), IN_COL, IN_ROW, Arrays.asList(
                        Arrays.asList("SKIP1", "x"),
                        Arrays.asList("SKIP2", "y"),
                        header,
                        Arrays.asList("1", "사용자1", "Seoul", "10.50", "2025-08-01", "ACTIVE", "메모", "노트", "{\"k\":\"v1\"}", "user1@example.com"),
                        Arrays.asList("2", "사용자2", "Busan", "22.00", "2025-08-02", "PENDING", "메모2", "노트2", "텍스트", "user2@example.com")
                )
        );

        ByteArrayInputStream in = new ByteArrayInputStream(src);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        StreamingCsv.CsvFormat inFmt = StreamingCsv.CsvFormat.builder()
                .charset("CP949").columnDelimiter(IN_COL).rowDelimiter(IN_ROW).quote('"').build();
        StreamingCsv.CsvFormat outFmt = StreamingCsv.CsvFormat.builder()
                .charset("UTF-8").columnDelimiter(IN_COL).rowDelimiter(IN_ROW).quote('"').build();

        StreamingCsv.Pipeline.run(
                in, inFmt, out, outFmt,
                StreamingCsv.OutputQuoteMode.AS_NEEDED,
                /* preserveInputQuotes */ true,
                /* expectedColumns     */ 10,
                /* skipLeadingRows     */ 2,
                /* readHeaderAfterSkip */ true,
                /* closeInput          */ false,
                /* closeOutput         */ false,
                row -> row
        );

        String result = out.toString("UTF-8");
        String expectedHeader = String.join(IN_COL, header) + IN_ROW;

        Assertions.assertTrue(result.startsWith(expectedHeader), "헤더가 출력 첫 행이어야 함");
        Assertions.assertFalse(result.contains("SKIP1"), "스킵 라인은 저장되면 안 됨");
        Assertions.assertFalse(result.contains("SKIP2"), "스킵 라인은 저장되면 안 됨");
        Assertions.assertTrue(result.contains("사용자1"), "한글 인코딩 변환 후에도 내용 유지");
    }

    /**
     * 2) InputStream/OutputStream: expectedColumns 불일치 시 RuntimeException
     */
    @Test
    public void stream_expectedColumnsMismatch_throws() throws Exception {
        byte[] src = buildBytes(
                Charset.forName("UTF-8"), IN_COL, IN_ROW, Arrays.asList(
                        Arrays.asList("id", "name", "city", "amount", "date", "status", "memo", "note", "json", "email"), // 10
                        Arrays.asList("1", "사용자1", "Seoul", "10.50", "2025-08-01", "ACTIVE", "메모", "노트", "{\"k\":\"v1\"}") // 9
                )
        );
        ByteArrayInputStream in = new ByteArrayInputStream(src);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        StreamingCsv.CsvFormat fmt = StreamingCsv.CsvFormat.builder()
                .charset("UTF-8").columnDelimiter(IN_COL).rowDelimiter(IN_ROW).quote('"').build();

        Assertions.assertThrows(RuntimeException.class, () ->
                StreamingCsv.Pipeline.run(
                        in, fmt, out, fmt,
                        StreamingCsv.OutputQuoteMode.AS_NEEDED,
                        true,
                        /* expectedColumns     */ 10,
                        /* skipLeadingRows     */ 0,
                        /* readHeaderAfterSkip */ true,
                        /* closeInput          */ false,
                        /* closeOutput         */ false,
                        row -> row
                )
        );
    }

    /**
     * 3) Output 전용 구분자 변경 적용 검증 (스트림 기반)
     */
    @Test
    public void stream_outputDelimiters_applied() throws Exception {
        byte[] src = buildBytes(
                Charset.forName("UTF-8"), IN_COL, IN_ROW, Arrays.asList(
                        Arrays.asList("id", "name", "city", "amount", "date", "status", "memo", "note", "json", "email"),
                        Arrays.asList("1", "Alice", "Seoul", "10.00", "2025-08-01", "ACTIVE", "memo", "note", "txt", "a@example.com")
                )
        );
        ByteArrayInputStream in = new ByteArrayInputStream(src);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        String OUT_COL = "||";
        String OUT_ROW = "##\n";

        StreamingCsv.CsvFormat inFmt = StreamingCsv.CsvFormat.builder()
                .charset("UTF-8").columnDelimiter(IN_COL).rowDelimiter(IN_ROW).quote('"').build();
        StreamingCsv.CsvFormat outFmt = StreamingCsv.CsvFormat.builder()
                .charset("UTF-8").columnDelimiter(OUT_COL).rowDelimiter(OUT_ROW).quote('"').build();

        StreamingCsv.Pipeline.run(
                in, inFmt, out, outFmt,
                StreamingCsv.OutputQuoteMode.AS_NEEDED,
                true,
                /* expectedColumns     */ 10,
                /* skipLeadingRows     */ 0,
                /* readHeaderAfterSkip */ true,
                /* closeInput          */ false,
                /* closeOutput         */ false,
                row -> row
        );

        String result = out.toString("UTF-8");
        String expectedHeader = String.join(OUT_COL,
                "id", "name", "city", "amount", "date", "status", "memo", "note", "json", "email") + OUT_ROW;

        Assertions.assertTrue(result.startsWith(expectedHeader), "출력 구분자 변경이 헤더에 반영되어야 함");
        Assertions.assertTrue(result.contains("Alice"), "데이터 존재");
        Assertions.assertFalse(result.contains(IN_COL), "입력 컬럼 구분자가 결과에 남으면 안 됨");
        Assertions.assertFalse(result.contains(IN_ROW), "입력 라인 구분자가 결과에 남으면 안 됨");
    }

    /**
     * 4) 필드 중간의 '\n'은 행 분할이 되지 않아야 함(EOR 토큰은 커스텀). 파일 스트림 사용
     */
    @Test
    public void file_randomNewlines_doNotBreakParsing() throws Exception {
        Path gen = tmpDir.resolve("generated.dat");
        Path out = tmpDir.resolve("parsed.dat");

        // 랜덤 개행 포함 샘플 생성 (헤더 포함 1 + 데이터 100 = 101 레코드)
        String COL = "^|";
        String EOR = "<<EOR>>\n";
        SampleDataGenerator.generate(
                gen, 100, Charset.forName("UTF-8"),
                COL, EOR, '"',
                SampleDataGenerator.QuoteMode.AS_NEEDED,
                true, 0.6
        );

        StreamingCsv.CsvFormat fmt = StreamingCsv.CsvFormat.builder()
                .charset("UTF-8").columnDelimiter(COL).rowDelimiter(EOR).quote('"').build();

        try (InputStream in = Files.newInputStream(gen);
             OutputStream o = Files.newOutputStream(out)) {
            StreamingCsv.Pipeline.run(
                    in, fmt, o, fmt,
                    StreamingCsv.OutputQuoteMode.AS_NEEDED,
                    true,
                    /* expectedColumns */ 10,
                    /* skipLeadingRows */ 0,
                    /* readHeaderAfterSkip */ false,
                    /* closeInput */ true,
                    /* closeOutput */ true,
                    row -> row
            );
        }

        String outText = Files.readString(out);
        Assertions.assertEquals(101, countOccurrences(outText, EOR), "헤더 포함 101 레코드가 EOR로 끝나야 함");
    }

    /**
     * 5) 헤더는 RowProcessor를 거치지 않고 그대로 기록되어야 함
     */
    @Test
    public void stream_header_isNotProcessedByRowProcessor() throws Exception {
        List<String> header = Arrays.asList("id", "name");
        byte[] src = buildBytes(
                Charset.forName("UTF-8"), IN_COL, IN_ROW, Arrays.asList(
                        header,
                        Arrays.asList("1", "alice")
                )
        );
        ByteArrayInputStream in = new ByteArrayInputStream(src);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        StreamingCsv.CsvFormat fmt = StreamingCsv.CsvFormat.builder()
                .charset("UTF-8").columnDelimiter(IN_COL).rowDelimiter(IN_ROW).quote('"').build();

        StreamingCsv.Pipeline.run(
                in, fmt, out, fmt,
                StreamingCsv.OutputQuoteMode.AS_NEEDED,
                true,
                /* expectedColumns     */ 2,
                /* skipLeadingRows     */ 0,
                /* readHeaderAfterSkip */ true,
                /* closeInput          */ false,
                /* closeOutput         */ false,
                row -> Arrays.asList(row.get(0), row.get(1).toUpperCase()) // name만 대문자화
        );

        String result = out.toString("UTF-8");
        String[] lines = result.split(IN_ROW, -1); // 마지막 빈조각 포함
        // 첫 행(헤더)은 대문자화되면 안 됨
        Assertions.assertTrue(lines[0].startsWith("id" + IN_COL + "name"), "헤더는 가공되면 안 됨");
        // 두 번째 행 데이터는 Processor 적용
        Assertions.assertTrue(result.contains("alice".toUpperCase()), "데이터 행은 Processor 적용");
    }

    /**
     * 6) closeInput/closeOutput=true 이면 기반 스트림이 실제로 닫혀야 함 (파일 스트림으로 검증)
     */
    @Test
    public void file_closeFlags_closeUnderlyingStreams() throws Exception {
        Path inFile = tmpDir.resolve("close_in.dat");
        Path outFile = tmpDir.resolve("close_out.dat");

        Files.write(inFile, buildBytes(Charset.forName("UTF-8"), IN_COL, IN_ROW, Arrays.asList(
                Arrays.asList("id", "name"),
                Arrays.asList("1", "x")
        )));

        StreamingCsv.CsvFormat fmt = StreamingCsv.CsvFormat.builder()
                .charset("UTF-8").columnDelimiter(IN_COL).rowDelimiter(IN_ROW).quote('"').build();

        FileInputStream fin = new FileInputStream(inFile.toFile());
        FileOutputStream fout = new FileOutputStream(outFile.toFile());

        StreamingCsv.Pipeline.run(
                fin, fmt, fout, fmt,
                StreamingCsv.OutputQuoteMode.AS_NEEDED,
                true,
                /* expectedColumns */ 2,
                /* skipLeadingRows */ 0,
                /* readHeaderAfterSkip */ true,
                /* closeInput */ true,
                /* closeOutput */ true,
                row -> row
        );

        // fin/fout 은 닫혔어야 한다.
        Assertions.assertThrows(IOException.class, () -> fin.read(), "closeInput=true 이면 입력 스트림이 닫혀야 함");
        Assertions.assertThrows(IOException.class, () -> fout.write(65), "closeOutput=true 이면 출력 스트림이 닫혀야 함");
    }
}
