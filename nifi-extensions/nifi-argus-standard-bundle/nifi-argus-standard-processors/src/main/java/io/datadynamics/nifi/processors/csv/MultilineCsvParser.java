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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"csv", "multiline", "parser", "stream", "custom-delimiter"})
@CapabilityDescription("멀티라인 CSV를 커스텀 구분자/인코딩/Quote로 스트리밍 파싱 후 저장. " +
        "선행 N행 스킵, 그 다음 1행을 헤더로 읽어 출력의 첫 행으로 기록. " +
        "expectedColumns 불일치 시 실패. 정규식 미사용. ")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = "csv.rows.skipped", description = "스킵된 레코드 수"),
        @WritesAttribute(attribute = "csv.rows.header", description = "헤더 기록 여부(0/1)"),
        @WritesAttribute(attribute = "csv.rows.data.in", description = "입력 데이터 레코드 수(헤더 제외)"),
        @WritesAttribute(attribute = "csv.rows.data.out", description = "출력 데이터 레코드 수"),
        @WritesAttribute(attribute = "csv.error", description = "실패 시 에러 메시지")
})
/**
 * 필드 내부에 개행 문자(\n, \r\n)가 포함될 수 있는 "멀티라인" CSV 파일을
 * 정규식을 사용하지 않고 문자 단위 스트리밍 방식으로 파싱/재기록하는 프로세서.
 *
 * <p>일반적인 CSV 파서는 행 구분자로 단순히 개행 문자를 사용하지만, 이 프로세서는
 * 사용자가 지정한 임의 길이(2문자 이상)의 컬럼/행 구분자 토큰(예: "^|", "@@\n")을 사용하여
 * 필드 값 안에 실제 개행 문자가 섞여 있어도 행 경계가 깨지지 않도록 처리한다.</p>
 *
 * <p>처리 흐름:
 * <ol>
 *   <li>선행 N개 레코드를 스킵(SKIP_LEADING_ROWS)</li>
 *   <li>옵션에 따라 그다음 1개 레코드를 헤더로 읽어 출력 첫 행에 그대로 기록(READ_HEADER_AFTER_SKIP)</li>
 *   <li>나머지 데이터 레코드를 순차적으로 읽어 컬럼 수를 검증하고, RowProcessor로 가공한 뒤 출력에 기록</li>
 * </ol>
 * 컬럼 수가 EXPECTED_COLUMNS와 다르면 즉시 실패(REL_FAILURE)로 라우팅한다.</p>
 */
public class MultilineCsvParser extends AbstractProcessor {

    /* ===== Relationships ===== */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("변환 성공")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("변환 실패")
            .build();

    /* ===== Properties ===== */
    public static final PropertyDescriptor INPUT_CHARSET = new PropertyDescriptor.Builder()
            .name("Input Charset")
            .displayName("입력 문자셋")
            .description("입력 파일 문자셋")
            .defaultValue("CP949")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUTPUT_CHARSET = new PropertyDescriptor.Builder()
            .name("Output Charset")
            .displayName("출력 문자셋")
            .description("출력 파일 문자셋")
            .defaultValue("UTF-8")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_COL_DELIM = new PropertyDescriptor.Builder()
            .name("Input Column Delimiter")
            .displayName("입력 컬럼 구분자")
            .description("입력 컬럼 구분자(최소 2문자)")
            .defaultValue("^|")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_ROW_DELIM = new PropertyDescriptor.Builder()
            .name("Input Row Delimiter")
            .displayName("입력 행 구분자")
            .description("입력 행 구분자(최소 2문자, 예: \"@@\\n\")")
            .defaultValue("@@\n")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_COL_DELIM = new PropertyDescriptor.Builder()
            .name("Output Column Delimiter")
            .displayName("출력 컬럼 구분자")
            .description("출력 컬럼 구분자")
            .defaultValue("^|")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_ROW_DELIM = new PropertyDescriptor.Builder()
            .name("Output Row Delimiter")
            .displayName("출력 행 구분자")
            .description("출력 행 구분자")
            .defaultValue("@@\n")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_QUOTE = new PropertyDescriptor.Builder()
            .name("Input Quote")
            .displayName("입력 Quote")
            .description("입력 Quote 문자(정확히 1문자)")
            .defaultValue("\"")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_QUOTE = new PropertyDescriptor.Builder()
            .name("Output Quote")
            .displayName("출력 Quote")
            .description("출력 Quote 문자(정확히 1문자)")
            .defaultValue("\"")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final AllowableValue QM_NEVER = new AllowableValue("NEVER", "NEVER", "절대 Quote 사용 안 함");
    public static final AllowableValue QM_ALWAYS = new AllowableValue("ALWAYS", "ALWAYS", "항상 Quote");
    public static final AllowableValue QM_AS_NEEDED = new AllowableValue("AS_NEEDED", "AS_NEEDED", "필요 시에만 Quote");

    public static final PropertyDescriptor OUT_QUOTE_MODE = new PropertyDescriptor.Builder()
            .name("Output Quote Mode")
            .displayName("출력 Quote 모드")
            .description("출력 Quote 전략")
            .required(true)
            .defaultValue(QM_NEVER.getValue())
            .allowableValues(QM_NEVER, QM_ALWAYS, QM_AS_NEEDED)
            .build();

    public static final PropertyDescriptor PRESERVE_INPUT_QUOTES = new PropertyDescriptor.Builder()
            .name("Preserve Input Quotes")
            .displayName("입력 Quote 보존")
            .description("입력 Quote를 값에 보존할지 여부")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor EXPECTED_COLUMNS = new PropertyDescriptor.Builder()
            .name("Expected Columns")
            .displayName("기대 컬럼 수")
            .description("기대한 컬럼 수(>0이면 강제, 다르면 실패)")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SKIP_LEADING_ROWS = new PropertyDescriptor.Builder()
            .name("Skip Leading Rows")
            .displayName("선행 행 스킵")
            .description("파싱 시작 전에 스킵할 레코드 수(Row Delimiter 단위)")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor READ_HEADER_AFTER_SKIP = new PropertyDescriptor.Builder()
            .name("Read Header After Skip Leading Rows")
            .displayName("스킵 후 헤더 읽기")
            .description("스킵 직후 1행을 헤더로 읽어 출력 첫 행으로 기록")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> d = new ArrayList<>();
        d.add(INPUT_CHARSET);
        d.add(OUTPUT_CHARSET);
        d.add(IN_COL_DELIM);
        d.add(IN_ROW_DELIM);
        d.add(OUT_COL_DELIM);
        d.add(OUT_ROW_DELIM);
        d.add(IN_QUOTE);
        d.add(OUT_QUOTE);
        d.add(OUT_QUOTE_MODE);
        d.add(PRESERVE_INPUT_QUOTES);
        d.add(EXPECTED_COLUMNS);
        d.add(SKIP_LEADING_ROWS);
        d.add(READ_HEADER_AFTER_SKIP);
        descriptors = Collections.unmodifiableList(d);

        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    /* ===== 내부 출력 Writer ===== */
    private enum OutputQuoteMode {NEVER, ALWAYS, AS_NEEDED}

    /**
     * 파싱된 레코드를 지정된 출력 구분자/Quote 규칙에 따라 다시 텍스트로 직렬화하는 내부 Writer.
     * BufferedWriter로 감싸 8KB 단위로 버퍼링하여 쓰기 성능을 확보한다.
     */
    private static final class CsvWriter implements Closeable, Flushable {
        private final java.io.Writer writer;
        private final String colDelim, rowDelim;
        private final char quote;
        private final OutputQuoteMode mode;

        CsvWriter(java.io.Writer writer, String colDelim, String rowDelim, char quote, OutputQuoteMode mode) {
            this.writer = new BufferedWriter(writer, 8192);
            this.colDelim = colDelim;
            this.rowDelim = rowDelim;
            this.quote = quote;
            this.mode = mode;
        }

        // 한 행의 모든 필드를 컬럼 구분자로 이어 쓰고, 마지막에 행 구분자를 기록한다.
        void writeRow(List<String> row) throws IOException {
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) writer.write(colDelim);
                writeField(row.get(i));
            }
            writer.write(rowDelim);
        }

        // Quote 모드(ALWAYS/NEVER/AS_NEEDED)에 따라 필드를 Quote로 감쌀지 결정하고,
        // AS_NEEDED인 경우 필드 값에 Quote 문자나 구분자가 포함되어 있을 때만 감싼다.
        // Quote로 감쌀 때는 내부의 Quote 문자를 두 번 반복하여 이스케이프한다(예: " -> "").
        private void writeField(String value) throws IOException {
            String v = (value == null) ? "" : value;
            boolean mustQuote = switch (mode) {
                case ALWAYS -> true;
                case NEVER -> false;
                default -> v.indexOf(quote) >= 0 || v.contains(colDelim) || v.contains(rowDelim);
            };
            if (!mustQuote) {
                writer.write(v);
                return;
            }
            String escaped = v.replace(String.valueOf(quote), String.valueOf(quote) + quote);
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

    /* ===== 내부 입력 Parser (정규식 미사용) ===== */
    /**
     * 문자 단위 상태 기계(state machine)로 동작하는 CSV 파서.
     *
     * <p>정규식이나 사전 토큰화 없이 한 문자씩 읽으며, 지금까지 누적된 필드 버퍼({@code field})의
     * "끝부분"이 컬럼 구분자 또는 행 구분자 문자열과 일치하는지({@link #endsWith}) 매 문자마다 검사하는
     * 방식으로 임의 길이(2문자 이상)의 멀티 캐릭터 구분자를 지원한다. 구분자와 일치하면 그 구분자 길이만큼
     * 버퍼 끝을 잘라내고({@link #trimTail}) 하나의 필드/행을 확정한다.</p>
     *
     * <p>Quote 처리: 필드의 첫 문자가 Quote 문자이면 Quote 모드로 진입하며, Quote 모드 안에서는
     * 구분자 검사를 하지 않고 오직 Quote 문자만 특별 취급한다(즉, Quote로 감싼 필드 내부의 구분자나
     * 개행 문자는 그대로 필드 값에 포함된다 - 이것이 "멀티라인" 필드를 지원하는 핵심 메커니즘이다).
     * Quote 안에서 Quote 문자를 만나면 다음 문자를 미리 읽어(lookahead) 그것도 Quote 문자이면
     * 이스케이프된 Quote("")로 간주하고, 그렇지 않으면 Quote 종료로 간주하여 미리 읽은 문자를
     * {@link PushbackReader#unread(int)}로 되돌려 넣은 뒤 이후 일반 처리 흐름을 계속한다.</p>
     */
    private static final class CsvParser implements Closeable {
        // 1문자 lookahead(unread)가 가능한 Reader. Quote 종료 판단 시 다음 문자를 미리 읽었다가
        // 되돌려 넣기 위해 필요하다.
        private final PushbackReader reader;
        private final String colDelim, rowDelim;
        private final char quote;
        // true면 Quote 문자 자체도 필드 값에 그대로 남긴다(원본 형태 보존), false면 Quote 문자를 제거한다.
        private final boolean preserveInputQuotes;
        // 현재 파싱 중인 필드를 누적하는 버퍼(행/필드가 바뀔 때마다 재사용됨)
        private final StringBuilder field = new StringBuilder();
        // 현재 파싱 중인 행에서 이미 확정된 필드들의 목록(재사용됨)
        private final List<String> row = new ArrayList<>();
        // inQuote: 현재 Quote로 감싼 구간 내부인지 여부
        // fieldStarted: 현재 필드의 첫 문자를 아직 안 읽었는지 여부(Quote 시작 여부 판단에 사용)
        // eof: 스트림 끝에 도달했는지 여부(이후 readNextRow 호출은 모두 null 반환)
        private boolean inQuote = false, fieldStarted = false, eof = false;

        CsvParser(Reader r, String colDelim, String rowDelim, char quote, boolean preserveInputQuotes) {
            this.reader = new PushbackReader(new BufferedReader(r, 8192), 1);
            this.colDelim = colDelim;
            this.rowDelim = rowDelim;
            this.quote = quote;
            this.preserveInputQuotes = preserveInputQuotes;
        }

        /**
         * 스트림에서 한 행(레코드)을 읽어 필드 목록으로 반환한다.
         * 더 이상 읽을 데이터가 없으면 null을 반환한다.
         * 스트림이 행 구분자 없이 끝나더라도(마지막 줄에 구분자가 없는 경우) 남아있는 버퍼를
         * 마지막 행으로 처리하여 데이터 유실을 방지한다.
         */
        List<String> readNextRow() throws IOException {
            if (eof) return null;
            field.setLength(0);
            row.clear();
            inQuote = false;
            fieldStarted = false;

            int ci;
            while ((ci = reader.read()) != -1) {
                char ch = (char) ci;

                if (!inQuote) {
                    // 필드의 첫 문자가 Quote 문자이면 Quote 모드로 진입(이후 구분자 검사를 건너뛴다).
                    if (!fieldStarted) {
                        fieldStarted = true;
                        if (ch == quote) {
                            inQuote = true;
                            if (preserveInputQuotes) field.append(ch);
                            continue;
                        }
                    }
                    field.append(ch);

                    // 버퍼 끝이 행 구분자와 일치하면 행이 끝난 것으로 보고, 구분자를 잘라낸 뒤
                    // 마지막 필드를 확정하여 완성된 행을 반환한다.
                    if (endsWith(field, rowDelim)) {
                        trimTail(field, rowDelim.length());
                        row.add(field.toString());
                        field.setLength(0);
                        fieldStarted = false;
                        return row;
                    }
                    // 버퍼 끝이 컬럼 구분자와 일치하면 하나의 필드가 끝난 것으로 보고,
                    // 구분자를 잘라낸 뒤 필드를 확정하고 다음 필드 파싱을 계속한다.
                    if (endsWith(field, colDelim)) {
                        trimTail(field, colDelim.length());
                        row.add(field.toString());
                        field.setLength(0);
                        fieldStarted = false;
                        continue;
                    }
                } else {
                    // Quote 모드 내부: 구분자는 무시하고 오직 Quote 문자만 특별 처리한다.
                    if (ch == quote) {
                        // 다음 문자를 미리 읽어(lookahead) 이스케이프된 Quote("")인지,
                        // 아니면 실제 Quote 종료인지 판별한다.
                        int next = reader.read();
                        if (next == -1) {
                            if (preserveInputQuotes) field.append(ch);
                            break;
                        }
                        char nch = (char) next;
                        if (nch == quote) {
                            // "" 형태: 이스케이프된 Quote 문자 하나로 간주하고 필드에 반영.
                            if (preserveInputQuotes) field.append(ch).append(nch);
                            else field.append(ch);
                        } else {
                            // Quote 구간 종료. 미리 읽은 다음 문자는 되돌려 넣어 이후 일반 흐름에서 처리한다.
                            if (preserveInputQuotes) field.append(ch);
                            inQuote = false;
                            reader.unread(nch);
                        }
                    } else {
                        // Quote 내부의 일반 문자(개행 포함)는 구분자 여부와 무관하게 그대로 필드에 포함시킨다.
                        field.append(ch);
                    }
                }
            }

            // 스트림이 끝났다. 구분자 없이 끝난 마지막 데이터가 남아 있으면 그것을 마지막 행으로 반환한다.
            eof = true;
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

        // StringBuilder의 끝부분이 주어진 토큰과 일치하는지 검사한다(멀티 캐릭터 구분자 탐지용).
        private static boolean endsWith(StringBuilder sb, String token) {
            int n = token.length(), m = sb.length();
            if (m < n) return false;
            for (int i = 0; i < n; i++) {
                if (sb.charAt(m - n + i) != token.charAt(i)) return false;
            }
            return true;
        }

        // 버퍼 끝에서 count개의 문자를 제거한다(구분자 문자열 자체를 필드 값에서 잘라내기 위함).
        private static void trimTail(StringBuilder sb, int count) {
            sb.setLength(sb.length() - count);
        }
    }

    /* ===== StreamCallback ===== */
    /**
     * NiFi의 {@code session.write(FlowFile, StreamCallback)}에 전달되는 콜백으로,
     * FlowFile의 원본 콘텐츠(rawIn)를 읽어 파싱/변환한 뒤 새 콘텐츠(rawOut)로 기록한다.
     * 스킵/헤더/데이터 처리 진행 상황을 원자적 카운터(AtomicLong)에 누적하여, 처리가 끝난 뒤
     * onTrigger에서 FlowFile 속성(csv.rows.* 등)으로 기록할 수 있게 한다.
     */
    private static final class TransformCallback implements StreamCallback {
        private final Charset inCs, outCs;
        private final String inCol, inRow, outCol, outRow;
        private final char inQuote, outQuote;
        private final OutputQuoteMode quoteMode;
        private final boolean preserveInputQuotes;
        private final int expectedColumns, skipLeadingRows;
        private final boolean readHeaderAfterSkip;
        private final RowProcessor rowProcessor;

        final AtomicLong rowsSkipped = new AtomicLong(0);
        final AtomicLong headerWritten = new AtomicLong(0);
        final AtomicLong rowsIn = new AtomicLong(0);
        final AtomicLong rowsOut = new AtomicLong(0);
        volatile String errorMessage = null;

        TransformCallback(Charset inCs, Charset outCs,
                          String inCol, String inRow, String outCol, String outRow,
                          char inQuote, char outQuote, OutputQuoteMode quoteMode,
                          boolean preserveInputQuotes,
                          int expectedColumns, int skipLeadingRows, boolean readHeaderAfterSkip,
                          RowProcessor rowProcessor) {
            this.inCs = inCs;
            this.outCs = outCs;
            this.inCol = inCol;
            this.inRow = inRow;
            this.outCol = outCol;
            this.outRow = outRow;
            this.inQuote = inQuote;
            this.outQuote = outQuote;
            this.quoteMode = quoteMode;
            this.preserveInputQuotes = preserveInputQuotes;
            this.expectedColumns = expectedColumns;
            this.skipLeadingRows = skipLeadingRows;
            this.readHeaderAfterSkip = readHeaderAfterSkip;
            this.rowProcessor = rowProcessor;
        }

        /**
         * 실제 스트리밍 변환을 수행한다. 처리 순서: (1) 선행 N행 스킵 → (2) 옵션에 따라 헤더 1행을
         * 읽어 그대로 출력 → (3) 나머지 데이터 행을 읽어 컬럼 수 검증 후 RowProcessor로 가공하여 출력.
         * 중간에 예외가 발생하면 errorMessage에 메시지를 보관한 뒤 다시 던져 onTrigger에서
         * REL_FAILURE로 라우팅할 수 있게 한다.
         */
        @Override
        public void process(InputStream rawIn, OutputStream rawOut) throws IOException {
            try (Reader r = new InputStreamReader(rawIn, inCs);
                 CsvParser parser = new CsvParser(r, inCol, inRow, inQuote, preserveInputQuotes);
                 Writer w = new OutputStreamWriter(rawOut, outCs);
                 CsvWriter writer = new CsvWriter(w, outCol, outRow, outQuote, quoteMode)) {

                // (1) 선행 스킵: 지정된 행 수만큼 읽고 버린다. 스킵 도중 EOF에 도달했을 때
                // 헤더를 읽어야 하는 설정이면 오류로 처리하고, 아니면 그대로 빈 출력으로 종료한다.
                for (int i = 0; i < skipLeadingRows; i++) {
                    if (parser.readNextRow() == null) {
                        if (readHeaderAfterSkip) {
                            throw new ProcessException("EOF while skipping " + skipLeadingRows + " rows: no header present.");
                        } else {
                            writer.flush();
                            rowsSkipped.set(i);
                            return;
                        }
                    } else {
                        rowsSkipped.incrementAndGet();
                    }
                }

                // (2) 헤더 처리: 스킵 직후 1행을 읽어 컬럼 수를 검증한 뒤 가공 없이(RowProcessor를 거치지 않고)
                // 그대로 출력 첫 행에 기록한다.
                if (readHeaderAfterSkip) {
                    List<String> header = parser.readNextRow();
                    if (header == null)
                        throw new ProcessException("No header after skipping " + skipLeadingRows + " rows.");
                    if (expectedColumns > 0 && header.size() != expectedColumns) {
                        throw new ProcessException("Header column count mismatch (expected=" + expectedColumns + ", actual=" + header.size() + "): " + header);
                    }
                    writer.writeRow(header);
                    headerWritten.set(1);
                }

                // (3) 데이터 행 처리: 컬럼 수가 EXPECTED_COLUMNS와 다르면 즉시 예외를 던져 전체 처리를 실패시킨다
                // (부분적으로 기록된 출력은 FlowFile에 반영되지 않는다 - session.write가 실패 처리).
                List<String> inRow;
                int dataIndex = 0;
                while ((inRow = parser.readNextRow()) != null) {
                    dataIndex++;
                    rowsIn.incrementAndGet();
                    if (expectedColumns > 0 && inRow.size() != expectedColumns) {
                        throw new ProcessException("Column count mismatch at data row " + dataIndex +
                                " (expected=" + expectedColumns + ", actual=" + inRow.size() + "): " + inRow);
                    }
                    List<String> outRowList = (rowProcessor != null) ? rowProcessor.process(inRow) : inRow;
                    writer.writeRow(outRowList);
                    rowsOut.incrementAndGet();
                }
                writer.flush();
            } catch (ProcessException e) {
                errorMessage = e.getMessage();
                throw e;
            }
        }
    }

    /**
     * FlowFile 하나를 받아 프로퍼티 값을 평가한 뒤 스트리밍 CSV 변환을 수행하고,
     * 성공하면 처리 통계를 속성으로 기록하여 REL_SUCCESS로, 실패하면 오류 메시지를
     * csv.error 속성에 담아 REL_FAILURE로 라우팅한다.
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog log = getLogger();

        FlowFile ff = session.get();
        if (ff == null) return;

        Map<String, String> orgAttributes = ff.getAttributes();

        // NiFi 2.x: Expression Language를 지원하는 프로퍼티는 evaluateAttributeExpressions()로 읽어야 한다.
        final Charset inCs = Charset.forName(context.getProperty(INPUT_CHARSET).evaluateAttributeExpressions(ff).getValue());
        final Charset outCs = Charset.forName(context.getProperty(OUTPUT_CHARSET).evaluateAttributeExpressions(ff).getValue());
        final String inCol = context.getProperty(IN_COL_DELIM).evaluateAttributeExpressions(ff).getValue();
        final String inRow = context.getProperty(IN_ROW_DELIM).evaluateAttributeExpressions(ff).getValue();
        final String outCol = context.getProperty(OUT_COL_DELIM).evaluateAttributeExpressions(ff).getValue();
        final String outRow = context.getProperty(OUT_ROW_DELIM).evaluateAttributeExpressions(ff).getValue();

        final char inQuote = context.getProperty(IN_QUOTE).evaluateAttributeExpressions(ff).getValue().charAt(0);
        final char outQuote = context.getProperty(OUT_QUOTE).evaluateAttributeExpressions(ff).getValue().charAt(0);
        final OutputQuoteMode outMode = OutputQuoteMode.valueOf(context.getProperty(OUT_QUOTE_MODE).getValue());
        final boolean preserve = context.getProperty(PRESERVE_INPUT_QUOTES).evaluateAttributeExpressions(ff).asBoolean();

        final int expectedCols = context.getProperty(EXPECTED_COLUMNS).evaluateAttributeExpressions(ff).asInteger();
        final int skipRows = context.getProperty(SKIP_LEADING_ROWS).evaluateAttributeExpressions(ff).asInteger();
        final boolean readHeader = context.getProperty(READ_HEADER_AFTER_SKIP).evaluateAttributeExpressions(ff).asBoolean();

        // 구분자 길이 방어
        if (inCol.length() < 2 || inRow.length() < 2 || outCol.length() < 2 || outRow.length() < 2) {
            ff = session.putAttribute(ff, "csv.error", "Delimiters must be length >= 2");
            session.transfer(ff, REL_FAILURE);
            return;
        }

        // RowProcessor 인스턴스 준비(옵션).
        // 참고: 현재 구현은 개행을 공백으로 치환한 newList를 만들지만 실제로는 가공 전
        // 원본 rows를 그대로 반환하고 있어(아래 return rows), 사실상 데이터 행을 변경하지 않는다.
        RowProcessor rp = new RowProcessor() {
            @Override
            public List<String> process(List<String> rows) {
                List<String> newList = new ArrayList<>(rows.size());
                for (String row : rows) {
                    newList.add(row.replace("\r\n", " ").replace("\n", " "));
                }
                return rows;
            }
        };

        final TransformCallback cb = new TransformCallback(
                inCs, outCs, inCol, inRow, outCol, outRow,
                inQuote, outQuote, outMode, preserve,
                expectedCols, skipRows, readHeader, rp
        );

        try {
            ff = session.write(ff, cb);

            // 카운터는 session.write() 완료 이후에 확정되므로 그 이후에 Attribute로 기록한다.
            final Map<String, String> attrs = new HashMap<>(orgAttributes); // Upstream의 FlowFile Attributes 전체를 그대로 전달.
            attrs.put("csv.input.charset", inCs.name());
            attrs.put("csv.rows.skipped", String.valueOf(cb.rowsSkipped.get()));
            attrs.put("csv.rows.header", String.valueOf(cb.headerWritten.get()));
            attrs.put("csv.rows.data.in", String.valueOf(cb.rowsIn.get()));
            attrs.put("csv.rows.data.out", String.valueOf(cb.rowsOut.get()));

            ff = session.putAllAttributes(ff, attrs);
            session.transfer(ff, REL_SUCCESS);
        } catch (ProcessException pe) {
            String err = (cb.errorMessage != null) ? cb.errorMessage : pe.getMessage();
            log.error("CSV parsing failed: {}", err, pe);
            ff = session.putAttribute(ff, "csv.error", err);
            session.transfer(ff, REL_FAILURE);
        }
    }
}