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

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 필드 내부에 개행이 포함될 수 있는 멀티라인 CSV를 순수 스트림(InputStream/OutputStream 또는
 * Reader/Writer) 기반으로 파싱하고 재기록하기 위한 저수준 유틸리티 모음.
 *
 * <p>{@link MultilineCsvParser} 프로세서가 NiFi FlowFile 콘텐츠에 특화된 상위 레벨 구현이라면,
 * 이 클래스는 NiFi에 의존하지 않는 순수 자바 스트림 API로 동일한 파싱/기록 로직을 제공하여
 * 프로세서 밖에서도(예: 단위 테스트, 다른 컴포넌트) 재사용할 수 있도록 분리한 것이다.</p>
 *
 * <p>정규식을 사용하지 않고 문자 단위 상태 기계로 임의 길이의 컬럼/행 구분자를 인식하며,
 * Quote로 감싼 구간 내부의 개행/구분자는 행 경계로 취급하지 않는다({@link Parser} 참고).</p>
 */
public final class StreamingCsv {

    // 호출자가 넘긴 Reader/Writer를 close 호출해도 실제 기반 스트림은 닫히지 않도록 하는 래퍼
    private static final class NonClosingReader extends FilterReader {
        protected NonClosingReader(java.io.Reader in) {
            super(in);
        }

        @Override
        public void close() { /* no-op: underlying InputStream은 닫지 않음 */ }
    }

    private static final class NonClosingWriter extends FilterWriter {
        protected NonClosingWriter(java.io.Writer out) {
            super(out);
        }

        @Override
        public void close() throws IOException {
            super.flush(); /* 닫지 않음 */
        }
    }

    private StreamingCsv() {
    }

    /* ===================== 구성 객체 ===================== */

    public enum OutputQuoteMode {
        NEVER,     // 절대 감싸지 않음
        ALWAYS,    // 항상 감쌈
        AS_NEEDED  // 구분자/줄구분자/Quote가 포함될 때만 감쌈
    }

    public static final class CsvFormat {
        private final String columnDelimiter;  // 최소 2글자 이상 권장
        private final String rowDelimiter;     // 최소 2글자 이상 권장 (예: "@@\n")
        private final char quote;              // 입력/출력용 Quote (기본: ")
        private final Charset charset;

        private CsvFormat(String columnDelimiter, String rowDelimiter, char quote, Charset charset) {
            this.columnDelimiter = Objects.requireNonNull(columnDelimiter);
            this.rowDelimiter = Objects.requireNonNull(rowDelimiter);
            this.quote = quote;
            this.charset = Objects.requireNonNull(charset);
        }

        public String columnDelimiter() {
            return columnDelimiter;
        }

        public String rowDelimiter() {
            return rowDelimiter;
        }

        public char quote() {
            return quote;
        }

        public Charset charset() {
            return charset;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String columnDelimiter = "^|";
            private String rowDelimiter = "@@\n";
            private char quote = '"';
            private Charset charset = Charset.forName("UTF-8");

            public Builder columnDelimiter(String d) {
                this.columnDelimiter = Objects.requireNonNull(d);
                return this;
            }

            public Builder rowDelimiter(String d) {
                this.rowDelimiter = Objects.requireNonNull(d);
                return this;
            }

            public Builder quote(char q) {
                this.quote = q;
                return this;
            }

            public Builder charset(String name) {
                this.charset = Charset.forName(name);
                return this;
            }

            public Builder charset(Charset cs) {
                this.charset = Objects.requireNonNull(cs);
                return this;
            }

            public CsvFormat build() {
                return new CsvFormat(columnDelimiter, rowDelimiter, quote, charset);
            }
        }
    }

    /* ===================== 파서(Reader) ===================== */

    /**
     * 문자 단위 상태 기계로 동작하는 CSV 파서. {@link MultilineCsvParser}의 내부 CsvParser와
     * 동일한 원리로 동작한다: 필드 버퍼의 접미사가 구분자와 일치하는지 매 문자마다 검사하고,
     * Quote 구간 내부에서는 구분자 검사를 건너뛰어 필드 내 개행/구분자를 보존한다.
     */
    private static final class Parser implements Closeable {
        private final PushbackReader reader;
        private final String colDelim;
        private final String rowDelim;
        private final char quote;
        private final boolean preserveInputQuotes;

        private final StringBuilder field = new StringBuilder();
        private final List<String> row = new ArrayList<>();

        private boolean inQuote = false;
        private boolean fieldStarted = false; // 현재 필드가 시작되었는지 (Quote 시작 여부 판단용)
        private boolean eofReached = false;

        Parser(Reader reader, CsvFormat fmt, boolean preserveInputQuotes) {
            // Pushback은 최소 1 글자면 충분(Quote 닫힘 판정 시 lookahead 1)
            this.reader = new PushbackReader(new BufferedReader(reader, 8192), 1);
            this.colDelim = fmt.columnDelimiter();
            this.rowDelim = fmt.rowDelimiter();
            this.quote = fmt.quote();
            this.preserveInputQuotes = preserveInputQuotes;
        }

        /**
         * 다음 행을 읽어 반환. EOF면 null.
         * 멀티라인 행을 rowDelim이 나올 때까지 누적.
         */
        List<String> readNextRow() throws IOException {
            if (eofReached) return null;

            field.setLength(0);
            row.clear();
            inQuote = false;
            fieldStarted = false;

            int ci;
            while ((ci = reader.read()) != -1) {
                char ch = (char) ci;

                if (!inQuote) {
                    if (!fieldStarted) {
                        fieldStarted = true;
                        if (ch == quote) {
                            // 필드 시작 즉시 Quote를 만나면 인용 시작
                            inQuote = true;
                            if (preserveInputQuotes) field.append(ch);
                            continue;
                        }
                    }

                    field.append(ch);

                    // 라인 구분자 우선(더 길 수 있으므로)
                    if (endsWith(field, rowDelim)) {
                        trimTail(field, rowDelim.length());
                        // 현재 필드 종료
                        row.add(field.toString());
                        field.setLength(0);
                        fieldStarted = false;
                        // 행 종료 → 반환
                        return row;
                    }

                    // 컬럼 구분자
                    if (endsWith(field, colDelim)) {
                        trimTail(field, colDelim.length());
                        row.add(field.toString());
                        field.setLength(0);
                        fieldStarted = false;
                        continue;
                    }
                } else {
                    // inQuote 상태
                    if (ch == quote) {
                        // lookahead 1 글자로 이스케이프("")인지, 종료인지 판정
                        int next = reader.read();
                        if (next == -1) {
                            // 입력 종료. 닫히지 않은 Quote로 간주하되, 보존 옵션이면 닫는 Quote 추가
                            if (preserveInputQuotes) field.append(ch);
                            // EOF 처리는 while 밖에서
                            break;
                        }
                        char nch = (char) next;
                        if (nch == quote) {
                            // 이스케이프된 Quote ("")
                            // 보존 여부에 따라 1개 또는 2개를 어떻게 저장할지 결정
                            // 일반 CSV 규칙: "" → " 로 값에 1개로 들어가는게 자연스럽지만
                            // "입력 Quote 보존" 옵션이면 원문 그대로 두 개를 보존
                            if (preserveInputQuotes) {
                                field.append(ch).append(nch); // "" 그대로
                            } else {
                                field.append(ch);             // " 로 축약
                            }
                        } else {
                            // Quote 종료
                            if (preserveInputQuotes) field.append(ch);
                            inQuote = false;
                            // 다음 문자는 아직 소비하면 안 되므로 push back
                            reader.unread(nch);
                        }
                    } else {
                        field.append(ch);
                    }
                }
            }

            // 여기까지 오면 EOF
            eofReached = true;

            // 마지막 행(구분자로 끝나지 않은 경우) 처리
            if (inQuote) {
                // 닫히지 않은 Quote: 있는 그대로 필드로 취급
            }
            // 필드나 행에 무엇인가 있으면 반환
            if (field.length() > 0 || !row.isEmpty()) {
                row.add(field.toString());
                return row;
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        /* 접미사 매칭(정규식 미사용) */
        private static boolean endsWith(StringBuilder sb, String token) {
            int n = token.length();
            int m = sb.length();
            if (m < n) return false;
            for (int i = 0; i < n; i++) {
                if (sb.charAt(m - n + i) != token.charAt(i)) return false;
            }
            return true;
        }

        private static void trimTail(StringBuilder sb, int count) {
            sb.setLength(sb.length() - count);
        }
    }

    /* ===================== 라이터(Writer) ===================== */

    /**
     * 파싱된 행을 출력 구분자/Quote 규칙에 따라 다시 텍스트로 직렬화하는 Writer.
     * Quote 모드(NEVER/ALWAYS/AS_NEEDED)에 따라 필드를 감쌀지 결정한다.
     */
    private static final class Writer implements Closeable, Flushable {
        private final java.io.Writer writer;
        private final String colDelim;
        private final String rowDelim;
        private final char quote;
        private final OutputQuoteMode quoteMode;

        Writer(java.io.Writer writer, CsvFormat fmt, OutputQuoteMode mode) {
            this.writer = new BufferedWriter(writer, 8192);
            this.colDelim = fmt.columnDelimiter();
            this.rowDelim = fmt.rowDelimiter();
            this.quote = fmt.quote();
            this.quoteMode = mode;
        }

        void writeRow(List<String> row) throws IOException {
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) writer.write(colDelim);
                String v = row.get(i);
                writeField(v);
            }
            writer.write(rowDelim);
        }

        private void writeField(String value) throws IOException {
            boolean mustQuote;
            if (quoteMode == OutputQuoteMode.ALWAYS) {
                mustQuote = true;
            } else if (quoteMode == OutputQuoteMode.NEVER) {
                mustQuote = false;
            } else {
                // AS_NEEDED: 구분자/줄구분자/Quote 포함 시만 Quote
                mustQuote = value.indexOf(quote) >= 0
                        || value.contains(colDelim)
                        || value.contains(rowDelim);
            }

            if (!mustQuote) {
                writer.write(value);
                return;
            }

            // 내부 Quote 이스케이프: "" 규칙
            String escaped = value.replace(String.valueOf(quote), String.valueOf(quote) + quote);
            writer.write(quote);
            writer.write(escaped);
            writer.write(quote);
        }

        @Override
        public void flush() throws IOException {
            writer.flush();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    /* ===================== 파이프라인 ===================== */

    /**
     * 여러 오버로드의 run() 메서드를 통해 스트림 기반 CSV 변환 파이프라인 실행을 제공하는 진입점.
     * 가장 단순한 오버로드부터 스킵/헤더/컬럼검사/스트림 close 옵션까지 모두 지정하는
     * 완전한 오버로드까지 단계적으로 위임(delegate)하는 구조이다.
     */
    public static final class Pipeline {

        // --- 기존 run(...) 메서드들은 그대로 두세요 ---

        /**
         * 신규: InputStream/OutputStream으로 실행 (스트림은 기본적으로 닫지 않음).
         * expectedColumns=-1(검사 안 함), skipLeadingRows=0, readHeaderAfterSkip=false 기본 동작.
         */
        public static void run(
                InputStream in,
                CsvFormat inputFormat,
                OutputStream out,
                CsvFormat outputFormat,
                OutputQuoteMode outputQuoteMode,
                boolean preserveInputQuotes,
                RowProcessor processor
        ) throws IOException {
            run(in, inputFormat, out, outputFormat,
                    outputQuoteMode, preserveInputQuotes,
                    /* expectedColumns */ -1,
                    /* skipLeadingRows */ 0,
                    /* readHeaderAfterSkip */ false,
                    /* closeInput */ false,
                    /* closeOutput */ false,
                    processor);
        }

        /**
         * 신규: InputStream/OutputStream + expectedColumns 검사
         */
        public static void run(
                InputStream in,
                CsvFormat inputFormat,
                OutputStream out,
                CsvFormat outputFormat,
                OutputQuoteMode outputQuoteMode,
                boolean preserveInputQuotes,
                int expectedColumns,
                RowProcessor processor
        ) throws IOException {
            run(in, inputFormat, out, outputFormat,
                    outputQuoteMode, preserveInputQuotes,
                    expectedColumns, 0, false, false, false, processor);
        }

        /**
         * 신규: InputStream/OutputStream + 스킵/헤더/컬럼검사 + 스트림 닫기 옵션.
         * closeInput/closeOutput=true로 주면 해당 스트림을 이 메서드가 닫습니다.
         *
         * <p>이 메서드가 모든 오버로드가 최종적으로 위임하는 핵심 구현이다. 처리 순서는
         * (1) 선행 행 스킵 → (2) 헤더 읽기/기록(옵션) → (3) 데이터 행 파싱·검증·가공·기록 순이며,
         * finally 블록에서 closeInput/closeOutput 옵션에 따라 기반 스트림을 닫거나 flush한다.</p>
         */
        public static void run(
                InputStream in,
                CsvFormat inputFormat,
                OutputStream out,
                CsvFormat outputFormat,
                OutputQuoteMode outputQuoteMode,
                boolean preserveInputQuotes,
                int expectedColumns,
                int skipLeadingRows,
                boolean readHeaderAfterSkip,
                boolean closeInput,
                boolean closeOutput,
                RowProcessor processor
        ) throws IOException {

            java.io.Reader baseReader = new InputStreamReader(in, inputFormat.charset());
            java.io.Writer baseWriter = new OutputStreamWriter(out, outputFormat.charset());

            // 닫지 않도록 래핑(옵션에 따라 실제로 닫게 할 수도 있음)
            java.io.Reader r = closeInput ? baseReader : new NonClosingReader(baseReader);
            java.io.Writer w = closeOutput ? baseWriter : new NonClosingWriter(baseWriter);

            // 나머지 로직은 기존 Path 기반 run(...)과 동일
            try (Parser parser = new Parser(r, inputFormat, preserveInputQuotes);
                 StreamingCsv.Writer writer = new StreamingCsv.Writer(w, outputFormat, outputQuoteMode)) {

                // 1) 선행 N행 스킵
                for (int i = 0; i < skipLeadingRows; i++) {
                    if (parser.readNextRow() == null) {
                        if (readHeaderAfterSkip) {
                            throw new RuntimeException("EOF while skipping first " + skipLeadingRows + " rows: no header present.");
                        } else {
                            writer.flush();
                            return;
                        }
                    }
                }

                // 2) 헤더 읽기(옵션) → 첫 행으로 기록(Processor 미적용)
                if (readHeaderAfterSkip) {
                    List<String> header = parser.readNextRow();
                    if (header == null)
                        throw new RuntimeException("No header row after skipping " + skipLeadingRows + " rows.");
                    if (expectedColumns > 0 && header.size() != expectedColumns) {
                        throw new RuntimeException("Header column count mismatch (expected=" + expectedColumns + ", actual=" + header.size() + "): " + header);
                    }
                    writer.writeRow(header);
                }

                // 3) 본문 처리
                int dataRowIndex = 0;
                List<String> inRow;
                while ((inRow = parser.readNextRow()) != null) {
                    dataRowIndex++;
                    if (expectedColumns > 0 && inRow.size() != expectedColumns) {
                        throw new RuntimeException("Column count mismatch at data row " + dataRowIndex +
                                " (expected=" + expectedColumns + ", actual=" + inRow.size() + "): " + inRow);
                    }
                    List<String> outRow = processor.process(inRow);
                    writer.writeRow(outRow);
                }
                writer.flush();
            } finally {
                // closeInput/closeOutput 플래그에 따라 실제로 닫아야 하는 경우 정리
                if (closeInput) {
                    try {
                        baseReader.close();
                    } catch (IOException ignore) {
                    }
                }
                if (closeOutput) {
                    try {
                        baseWriter.close();
                    } catch (IOException ignore) {
                    }
                } else {
                    try {
                        baseWriter.flush();
                    } catch (IOException ignore) {
                    }
                }
            }
        }
    }
}