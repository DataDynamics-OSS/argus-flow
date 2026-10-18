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
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"csv", "multiline", "parser", "stream", "custom-delimiter"})
@CapabilityDescription("멀티라인 CSV를 커스텀 구분자/인코딩/Quote로 스트리밍 파싱 후 저장. " +
        "선행 N행 스킵, 그 다음 1행을 헤더로 읽어 출력의 첫 행으로 기록. " +
        "expectedColumns 불일치 시 실패. 정규식 미사용. " +
        "닫히지 않은 Quote, 입력 문자셋으로 디코딩할 수 없는 바이트, 출력 문자셋으로 표현할 수 없는 문자는 " +
        "조용히 치환하지 않고 실패로 라우팅(원본 FlowFile 유지, csv.error에 위치 정보). " +
        "프로퍼티(EL) 평가 오류는 penalize 후 실패로 라우팅. UTF-8 입력의 선두 BOM은 제거. ")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = "csv.rows.skipped", description = "스킵된 레코드 수"),
        @WritesAttribute(attribute = "csv.rows.header", description = "헤더 기록 여부(0/1)"),
        @WritesAttribute(attribute = "csv.rows.data.in", description = "입력 데이터 레코드 수(헤더 제외)"),
        @WritesAttribute(attribute = "csv.rows.data.out", description = "출력 데이터 레코드 수"),
        @WritesAttribute(attribute = "csv.error", description = "실패 시 에러 메시지(최대 " + MultilineCsvParser.MAX_ERROR_LENGTH +
                "자, 초과분은 \"...(생략, 총 N자)\"로 표기)")
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
 *   <li>나머지 데이터 레코드를 순차적으로 읽어 컬럼 수를 검증한 뒤 출력에 기록</li>
 * </ol>
 * 컬럼 수가 EXPECTED_COLUMNS와 다르면 즉시 실패(REL_FAILURE)로 라우팅한다.</p>
 */
public class MultilineCsvParser extends AbstractProcessor {

    /** csv.error 속성에 기록할 메시지의 최대 길이(문자 수). 초과분은 잘라내고 생략 표기한다. */
    static final int MAX_ERROR_LENGTH = 1024;

    /**
     * 오류 메시지를 {@link #MAX_ERROR_LENGTH}자로 자른다. 잘린 경우 "...(생략, 총 N자)"를 붙인다.
     * 행 전체 내용이 메시지에 포함되어 속성이 무제한으로 커지는 것을 막기 위함이다(C-7).
     */
    static String truncateError(String msg) {
        if (msg == null) return "";
        if (msg.length() <= MAX_ERROR_LENGTH) return msg;
        int cut = MAX_ERROR_LENGTH;
        if (Character.isHighSurrogate(msg.charAt(cut - 1))) cut--; // 서로게이트 쌍을 가르지 않는다
        return msg.substring(0, cut) + "...(생략, 총 " + msg.length() + "자)";
    }

    /* ===== Relationships ===== */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("변환 성공")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("변환 실패")
            .build();

    /* ===== 문자셋 해석 ===== */

    /**
     * 문자셋 이름을 {@link Charset}으로 해석한다(입력/출력 문자셋, EL 평가 결과, 설정 검증 모두 이 메서드를 사용).
     *
     * <p>Java에서 "CP949"는 IBM 코드 페이지 949(x-IBM949)로 해석되는데, 이 문자셋에는 확장 한글(똠, 쀍, 햏 등
     * 8,822자)이 없어 Windows가 만든 CP949 파일이 깨진다. 따라서 "CP949"(대소문자 무시, 앞뒤 공백 무시)는
     * Windows 코드 페이지 949(MS949, x-windows-949)로 해석한다. 그 외 이름은 {@link Charset#forName(String)} 그대로.</p>
     */
    static Charset resolveCharset(String name) {
        if (name != null && name.trim().equalsIgnoreCase("CP949")) {
            return Charset.forName("x-windows-949");
        }
        return Charset.forName(name);
    }

    // resolveCharset으로 해석 가능한지 검사하는 Validator. EL이 포함된 값은 실행 시점에 평가되므로 통과시킨다.
    private static final Validator CHARSET_VALIDATOR = (subject, input, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(true)
                    .explanation("Expression Language Present").build();
        }
        boolean ok;
        try {
            resolveCharset(input);
            ok = true;
        } catch (Exception e) {
            ok = false;
        }
        return new ValidationResult.Builder().subject(subject).input(input).valid(ok)
                .explanation(ok ? null : "Character Set is not supported by this JVM.").build();
    };

    // 최소 길이 검사 Validator(구분자용). EL이 포함된 값은 실행 시점에 평가 후 onTrigger에서 검사한다.
    private static Validator minLengthValidator(int min) {
        return (subject, input, context) -> {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true)
                        .explanation("Expression Language Present").build();
            }
            boolean ok = input != null && input.length() >= min;
            return new ValidationResult.Builder().subject(subject).input(input).valid(ok)
                    .explanation(ok ? null : "최소 " + min + "문자여야 합니다(현재 " + (input == null ? 0 : input.length()) + "문자).")
                    .build();
        };
    }

    private static final Validator DELIMITER_VALIDATOR = minLengthValidator(2);

    // Quote 문자 Validator: 정확히 1문자만 허용한다. EL이 포함된 값은 실행 시점에 평가 후 검사한다({@link #singleChar}).
    private static final Validator QUOTE_CHAR_VALIDATOR = (subject, input, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(true)
                    .explanation("Expression Language Present").build();
        }
        boolean ok = input != null && input.length() == 1;
        return new ValidationResult.Builder().subject(subject).input(input).valid(ok)
                .explanation(ok ? null : "정확히 1문자여야 합니다(현재 " + (input == null ? 0 : input.length()) + "문자).")
                .build();
    };

    // EL 평가 결과 Quote 문자가 정확히 1문자인지 검사한다. 아니면 예외(→ penalize 후 failure).
    private static char singleChar(PropertyDescriptor property, String value) {
        if (value == null || value.length() != 1) {
            throw new IllegalArgumentException(property.getName() + " must be exactly 1 character but was '" + value + "'");
        }
        return value.charAt(0);
    }

    /* ===== Properties ===== */
    public static final PropertyDescriptor INPUT_CHARSET = new PropertyDescriptor.Builder()
            .name("Input Charset")
            .displayName("입력 문자셋")
            .description("입력 파일 문자셋. CP949는 Windows 코드 페이지 949(MS949, x-windows-949)로 해석된다. " +
                    "이 문자셋으로 디코딩할 수 없는 바이트가 있으면 실패로 라우팅한다(치환하지 않음). " +
                    "UTF-8이면 파일 선두의 BOM(EF BB BF)을 제거한다.")
            .defaultValue("CP949")
            .required(true)
            .addValidator(CHARSET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUTPUT_CHARSET = new PropertyDescriptor.Builder()
            .name("Output Charset")
            .displayName("출력 문자셋")
            .description("출력 파일 문자셋. CP949는 Windows 코드 페이지 949(MS949, x-windows-949)로 해석된다. " +
                    "이 문자셋으로 표현할 수 없는 문자가 있으면 실패로 라우팅한다('?' 등으로 치환하지 않음).")
            .defaultValue("UTF-8")
            .required(true)
            .addValidator(CHARSET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_COL_DELIM = new PropertyDescriptor.Builder()
            .name("Input Column Delimiter")
            .displayName("입력 컬럼 구분자")
            .description("입력 컬럼 구분자(최소 2문자)")
            .defaultValue("^|")
            .required(true)
            .addValidator(DELIMITER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_ROW_DELIM = new PropertyDescriptor.Builder()
            .name("Input Row Delimiter")
            .displayName("입력 행 구분자")
            .description("입력 행 구분자(최소 2문자, 예: \"@@\\n\"). 입력의 줄바꿈 형식과 정확히 같게 지정해야 한다: " +
                    "CRLF(\\r\\n) 줄바꿈 파일에 LF만 포함된 구분자(예: \"@@\\n\")를 지정하면 \"@@\\r\\n\"과 일치하지 않아 " +
                    "행이 나뉘지 않는다(자동 변환하지 않음). 이 경우 \"@@\\r\\n\"을 지정한다.")
            .defaultValue("@@\n")
            .required(true)
            .addValidator(DELIMITER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_COL_DELIM = new PropertyDescriptor.Builder()
            .name("Output Column Delimiter")
            .displayName("출력 컬럼 구분자")
            .description("출력 컬럼 구분자(최소 2문자)")
            .defaultValue("^|")
            .required(true)
            .addValidator(DELIMITER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_ROW_DELIM = new PropertyDescriptor.Builder()
            .name("Output Row Delimiter")
            .displayName("출력 행 구분자")
            .description("출력 행 구분자(최소 2문자)")
            .defaultValue("@@\n")
            .required(true)
            .addValidator(DELIMITER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IN_QUOTE = new PropertyDescriptor.Builder()
            .name("Input Quote")
            .displayName("입력 Quote")
            .description("입력 Quote 문자(정확히 1문자)")
            .defaultValue("\"")
            .required(true)
            .addValidator(QUOTE_CHAR_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUT_QUOTE = new PropertyDescriptor.Builder()
            .name("Output Quote")
            .displayName("출력 Quote")
            .description("출력 Quote 문자(정확히 1문자)")
            .defaultValue("\"")
            .required(true)
            .addValidator(QUOTE_CHAR_VALIDATOR)
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
            .description("입력 Quote 보존 여부. true면 입력에서 Quote로 감싸져 있던 필드는 출력 Quote 모드와 무관하게 " +
                    "출력 Quote 문자로 다시 감싸 기록한다(이미 Quote된 값을 이중으로 감싸지 않음). " +
                    "false면 Quote를 제거하고 출력 Quote 모드에 따라서만 감싼다.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
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
     * 전달받은 Writer(REPORT 모드 인코더를 쓰는 OutputStreamWriter)에 바로 쓴다. OutputStreamWriter가 내부적으로
     * 바이트를 버퍼링하며, 앞에 BufferedWriter를 두지 않아야 표현 불가 문자 오류가 해당 행을 쓰는 시점에 발생한다.
     *
     * <p>필드 값은 항상 Quote가 제거된(이스케이프가 풀린) 순수 값으로 전달받고, Quote로 감쌀지는
     * 이 Writer가 한 곳에서 결정한다. 따라서 하나의 필드가 두 번 Quote되는 일이 없고, 내부 Quote
     * 문자의 이스케이프도 정확히 한 번만 적용된다.</p>
     */
    private static final class CsvWriter implements Closeable, Flushable {
        private final java.io.Writer writer;
        private final String colDelim, rowDelim;
        private final char quote;
        private final OutputQuoteMode mode;
        // true면 입력에서 Quote로 감싸져 있던 필드는 출력 Quote 모드와 무관하게 다시 Quote로 감싼다.
        private final boolean preserveInputQuotes;

        CsvWriter(java.io.Writer writer, String colDelim, String rowDelim, char quote, OutputQuoteMode mode,
                  boolean preserveInputQuotes) {
            this.writer = writer;
            this.colDelim = colDelim;
            this.rowDelim = rowDelim;
            this.quote = quote;
            this.mode = mode;
            this.preserveInputQuotes = preserveInputQuotes;
        }

        // 한 행의 모든 필드를 컬럼 구분자로 이어 쓰고, 마지막에 행 구분자를 기록한다.
        // quoted의 i번째 비트는 i번째 필드가 입력에서 Quote로 감싸져 있었는지를 나타낸다.
        void writeRow(List<String> row, BitSet quoted) throws IOException {
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) writer.write(colDelim);
                writeField(row.get(i), quoted.get(i));
            }
            writer.write(rowDelim);
        }

        // Quote 모드(ALWAYS/NEVER/AS_NEEDED)에 따라 필드를 Quote로 감쌀지 결정하고,
        // AS_NEEDED인 경우 Quote 없이 쓰면 다시 읽을 때 값이 달라질 수 있을 때만 감싼다({@link #needsQuote}).
        // 입력 Quote 보존 모드이면 입력에서 Quote로 감싸져 있던 필드도 감싼다.
        // Quote로 감쌀 때는 내부의 Quote 문자를 두 번 반복하여 이스케이프한다(예: " -> "").
        private void writeField(String value, boolean wasQuoted) throws IOException {
            String v = (value == null) ? "" : value;
            boolean mustQuote = (preserveInputQuotes && wasQuoted) || switch (mode) {
                case ALWAYS -> true;
                case NEVER -> false;
                default -> needsQuote(v);
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

        /**
         * AS_NEEDED 판단: 값에 Quote 문자나 컬럼/행 구분자 전체가 들어 있거나, 값의 접미가 컬럼/행 구분자의
         * 진접두(proper prefix)와 같으면 Quote가 필요하다. 후자는 "값의 끝 + 뒤따르는 구분자"가 겹쳐 더 이른 위치에서
         * 구분자가 일치하는 경우(예: 구분자 "||"와 값 "a|" → "a|||" 가 [a, |...]로 읽힘)를 막기 위한 보수적 규칙이다.
         * 반대로 값의 접두가 앞 구분자의 진접미와 같은 경우는 파서가 구분자를 확정하면서 버퍼를 비우므로 문제가 없다.
         */
        private boolean needsQuote(String v) {
            return v.indexOf(quote) >= 0 || v.contains(colDelim) || v.contains(rowDelim)
                    || endsWithProperPrefix(v, colDelim) || endsWithProperPrefix(v, rowDelim);
        }

        // 값의 끝 k문자(1 <= k < 구분자 길이)가 구분자의 앞 k문자와 같은지 검사한다.
        private static boolean endsWithProperPrefix(String v, String delim) {
            for (int k = Math.min(v.length(), delim.length() - 1); k >= 1; k--) {
                if (v.regionMatches(v.length() - k, delim, 0, k)) return true;
            }
            return false;
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

    /* ===== 입력 디코딩 ===== */

    /** 입력 바이트를 입력 문자셋으로 디코딩할 수 없을 때 던지는 예외(정확한 바이트 오프셋 포함). */
    private static final class InputDecodingException extends IOException {
        final boolean malformed;
        final int length;
        final long byteOffset;

        InputDecodingException(boolean malformed, int length, long byteOffset) {
            super((malformed ? "malformed" : "unmappable") + " input (length=" + length + ") at byte offset " + byteOffset);
            this.malformed = malformed;
            this.length = length;
            this.byteOffset = byteOffset;
        }
    }

    /**
     * 잘못된 바이트/매핑 불가 바이트를 U+FFFD로 조용히 치환하지 않고 보고하는 디코딩 Reader(C-4).
     *
     * <p>InputStreamReader(REPORT 모드)는 오류를 만나면 그 앞에서 이미 디코딩한 문자까지 버리고 즉시 예외를 던지므로
     * 오류 위치가 버퍼(수 KB) 단위로 앞당겨진다. 이 Reader는 오류 직전까지의 문자를 먼저 돌려주고, 그 문자들이
     * 소비된 다음 read 호출에서 입력 기준 정확한 바이트 오프셋과 함께 {@link InputDecodingException}을 던진다.</p>
     */
    private static final class StrictDecodingReader extends Reader {
        private final InputStream in;
        private final CharsetDecoder decoder;
        private final ByteBuffer bytes = ByteBuffer.allocate(8192);
        // 입력 앞에서 건너뛴 바이트 수(BOM). 오류 오프셋을 원본 기준으로 맞추기 위함.
        private final long baseOffset;
        // bytes 버퍼에서 이미 소비되어 compact로 버려진 바이트 수의 누계
        private long discarded = 0;
        private boolean inputEof = false, flushed = false;

        StrictDecodingReader(InputStream in, Charset cs, long baseOffset) {
            this.in = in;
            this.decoder = cs.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            this.baseOffset = baseOffset;
            bytes.flip(); // 처음에는 읽을 바이트가 없는 상태
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (len == 0) return 0;
            CharBuffer out = CharBuffer.wrap(cbuf, off, len);
            while (true) {
                if (flushed) return out.position() > off ? out.position() - off : -1;
                CoderResult cr = decoder.decode(bytes, out, inputEof);
                if (cr.isError()) {
                    // 오류 앞까지 디코딩된 문자가 있으면 먼저 돌려주고, 오류는 다음 호출에서 던진다.
                    if (out.position() > off) return out.position() - off;
                    throw new InputDecodingException(cr.isMalformed(), cr.length(), baseOffset + discarded + bytes.position());
                }
                if (cr.isOverflow()) return out.position() - off;
                // underflow: 입력 바이트가 더 필요하다.
                if (inputEof) {
                    if (decoder.flush(out).isOverflow()) return out.position() - off;
                    flushed = true;
                    continue;
                }
                if (out.position() > off) return out.position() - off;
                int consumed = bytes.position();
                bytes.compact();
                discarded += consumed;
                int n = in.read(bytes.array(), bytes.position(), bytes.remaining());
                if (n < 0) inputEof = true;
                else bytes.position(bytes.position() + n);
                bytes.flip();
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
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
     *
     * <p>필드 값은 입력 Quote 보존 여부와 관계없이 항상 Quote를 벗기고 이스케이프("")를 푼 순수 값으로
     * 확정하며, 그 필드가 입력에서 Quote로 감싸져 있었는지는 별도 플래그({@link #quotedFlags()})로 기록한다.
     * Quote를 다시 붙일지는 {@link CsvWriter}가 결정한다(이중 Quote 방지).</p>
     *
     * <p>Quote 구간이 닫힌 뒤의 구분자 검사는 Quote가 닫힌 위치({@code quoteEnd}) 이후에 읽은 문자만으로
     * 한다. Quote 내용의 꼬리와 이후 문자가 합쳐져 구분자로 오인되는 것을 막기 위함이다
     * (예: 구분자 "||"에서 {@code "a|"||b} → [a|, b]).</p>
     */
    private static final class CsvParser implements Closeable {
        // 1문자 lookahead(unread)가 가능한 Reader. Quote 종료 판단 시 다음 문자를 미리 읽었다가
        // 되돌려 넣기 위해 필요하다.
        private final PushbackReader reader;
        private final String colDelim, rowDelim;
        private final char quote;
        // 현재 파싱 중인 필드를 누적하는 버퍼(행/필드가 바뀔 때마다 재사용됨). Quote 문자는 담지 않는다.
        private final StringBuilder field = new StringBuilder();
        // 현재 파싱 중인 행에서 이미 확정된 필드들의 목록(재사용됨)
        private final List<String> row = new ArrayList<>();
        // row의 i번째 필드가 입력에서 Quote로 감싸져 있었는지 여부(재사용됨)
        private final BitSet rowQuoted = new BitSet();
        // 현재 필드가 Quote로 시작했는지 여부
        private boolean fieldQuoted = false;
        // 현재 필드에서 Quote 구간이 닫힌 시점의 field 길이. 구분자는 이 위치 이후의 문자에서만 찾는다.
        private int quoteEnd = 0;
        // inQuote: 현재 Quote로 감싼 구간 내부인지 여부
        // fieldStarted: 현재 필드의 첫 문자를 아직 안 읽었는지 여부(Quote 시작 여부 판단에 사용)
        // eof: 스트림 끝에 도달했는지 여부(이후 readNextRow 호출은 모두 null 반환)
        private boolean inQuote = false, fieldStarted = false, eof = false;
        // 오류 메시지용 위치 정보: 현재 레코드 번호(1부터, 스킵/헤더 포함), 지금까지 읽은 문자 수,
        // 현재 열린 Quote의 시작 문자 오프셋(0부터)과 필드 번호(1부터)
        private long recordNo = 0, charOffset = 0, quoteOpenOffset = -1;
        private int quoteOpenField = 0;

        CsvParser(Reader r, String colDelim, String rowDelim, char quote) {
            this.reader = new PushbackReader(new BufferedReader(r, 8192), 1);
            this.colDelim = colDelim;
            this.rowDelim = rowDelim;
            this.quote = quote;
        }

        /**
         * 마지막으로 반환한 행의 필드별 Quote 여부(i번째 비트 = i번째 필드). 다음 readNextRow 호출 시 갱신된다.
         */
        BitSet quotedFlags() {
            return rowQuoted;
        }

        /** 현재(또는 마지막으로 읽은) 레코드 번호. 1부터 시작하며 스킵 행과 헤더 행을 포함한다. */
        long recordNumber() {
            return recordNo;
        }

        /**
         * 스트림에서 한 행(레코드)을 읽어 필드 목록으로 반환한다.
         * 더 이상 읽을 데이터가 없으면 null을 반환한다.
         * 스트림이 행 구분자 없이 끝나더라도(마지막 줄에 구분자가 없는 경우) 남아있는 버퍼를
         * 마지막 행으로 처리하여 데이터 유실을 방지한다.
         */
        List<String> readNextRow() throws IOException {
            if (eof) return null;
            recordNo++;
            field.setLength(0);
            row.clear();
            rowQuoted.clear();
            inQuote = false;
            fieldStarted = false;
            fieldQuoted = false;
            quoteEnd = 0;

            int ci;
            while ((ci = reader.read()) != -1) {
                charOffset++;
                char ch = (char) ci;

                if (!inQuote) {
                    // 필드의 첫 문자가 Quote 문자이면 Quote 모드로 진입(이후 구분자 검사를 건너뛴다).
                    if (!fieldStarted) {
                        fieldStarted = true;
                        if (ch == quote) {
                            inQuote = true;
                            fieldQuoted = true;
                            quoteOpenOffset = charOffset - 1;
                            quoteOpenField = row.size() + 1;
                            continue;
                        }
                    }
                    field.append(ch);

                    // 버퍼 끝이 행 구분자와 일치하면 행이 끝난 것으로 보고, 구분자를 잘라낸 뒤
                    // 마지막 필드를 확정하여 완성된 행을 반환한다.
                    // (Quote가 닫힌 위치 이후에 읽은 문자만으로 구분자가 완성된 경우에만 인정한다)
                    if (endsWithAfter(field, rowDelim, quoteEnd)) {
                        trimTail(field, rowDelim.length());
                        completeField();
                        return row;
                    }
                    // 버퍼 끝이 컬럼 구분자와 일치하면 하나의 필드가 끝난 것으로 보고,
                    // 구분자를 잘라낸 뒤 필드를 확정하고 다음 필드 파싱을 계속한다.
                    if (endsWithAfter(field, colDelim, quoteEnd)) {
                        trimTail(field, colDelim.length());
                        completeField();
                        continue;
                    }
                    // Quote가 닫힌 뒤에는 구분자만 올 수 있다. 지금까지 읽은 문자가 어느 구분자의 접두도 아니면
                    // 잘못된 CSV(예: "ab"cd)로 보고 실패시킨다(C-9).
                    if (fieldQuoted && !isDelimiterPrefixAfterQuote()) {
                        throw afterQuoteError(ch);
                    }
                } else {
                    // Quote 모드 내부: 구분자는 무시하고 오직 Quote 문자만 특별 처리한다.
                    if (ch == quote) {
                        // 다음 문자를 미리 읽어(lookahead) 이스케이프된 Quote("")인지,
                        // 아니면 실제 Quote 종료인지 판별한다.
                        int next = reader.read();
                        if (next == -1) {
                            // 스트림 끝에서 Quote 구간 종료.
                            inQuote = false;
                            quoteEnd = field.length();
                            break;
                        }
                        charOffset++;
                        char nch = (char) next;
                        if (nch == quote) {
                            // "" 형태: 이스케이프된 Quote 문자 하나로 간주하고 필드에 반영.
                            field.append(ch);
                        } else {
                            // Quote 구간 종료. 닫힌 위치를 기억하고(이후 구분자 검사는 이 위치 뒤에서만),
                            // 미리 읽은 다음 문자는 되돌려 넣어 이후 일반 흐름에서 처리한다.
                            inQuote = false;
                            quoteEnd = field.length();
                            reader.unread(nch);
                            charOffset--;
                        }
                    } else {
                        // Quote 내부의 일반 문자(개행 포함)는 구분자 여부와 무관하게 그대로 필드에 포함시킨다.
                        field.append(ch);
                    }
                }
            }

            // 스트림이 끝났다. Quote가 열린 채 끝났으면 나머지 입력 전체가 한 필드로 흡수된 것이므로
            // 조용히 성공시키지 않고 오류로 처리한다(C-3).
            eof = true;
            if (inQuote) {
                throw new ProcessException("Unterminated quote: quote opened at record " + recordNo + ", field " + quoteOpenField
                        + " (character offset " + quoteOpenOffset + ") is not closed before end of input");
            }
            // 닫는 Quote 뒤에 구분자가 완성되지 않은 문자(구분자의 일부)가 남은 채 끝났으면 잘못된 CSV다.
            if (fieldQuoted && field.length() > quoteEnd) {
                throw afterQuoteError(field.charAt(quoteEnd));
            }
            // 구분자 없이 끝난 마지막 데이터가 남아 있으면 그것을 마지막 행으로 반환한다.
            if (field.length() > 0 || fieldQuoted || !row.isEmpty()) {
                completeField();
                return row;
            }
            return null;
        }

        // 닫는 Quote 뒤에 읽은 문자(field[quoteEnd..])가 컬럼/행 구분자의 접두인지 검사한다.
        private boolean isDelimiterPrefixAfterQuote() {
            int k = field.length() - quoteEnd;
            return startsWithTail(colDelim, k) || startsWithTail(rowDelim, k);
        }

        // token의 앞 k문자가 field의 마지막 k문자와 같은지.
        private boolean startsWithTail(String token, int k) {
            if (k > token.length()) return false;
            for (int i = 0; i < k; i++) {
                if (field.charAt(quoteEnd + i) != token.charAt(i)) return false;
            }
            return true;
        }

        // 닫는 Quote 뒤의 잘못된 문자에 대한 예외(레코드/필드/문자 오프셋 포함).
        private ProcessException afterQuoteError(char ch) {
            return new ProcessException(String.format(
                    "Unexpected character U+%04X after closing quote at record %d, field %d (quote opened at character offset %d); "
                            + "only a column or row delimiter may follow a closing quote", (int) ch, recordNo, row.size() + 1, quoteOpenOffset));
        }

        // 현재 필드 버퍼를 하나의 필드로 확정하고(Quote 여부 포함) 다음 필드를 위해 상태를 초기화한다.
        private void completeField() {
            if (fieldQuoted) rowQuoted.set(row.size());
            row.add(field.toString());
            field.setLength(0);
            fieldStarted = false;
            fieldQuoted = false;
            quoteEnd = 0;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        // StringBuilder의 끝부분이 주어진 토큰과 일치하는지 검사한다(멀티 캐릭터 구분자 탐지용).
        // 토큰 전체가 from 위치 이후(Quote가 닫힌 뒤 읽은 문자)에 있어야만 일치로 본다.
        private static boolean endsWithAfter(StringBuilder sb, String token, int from) {
            int n = token.length(), m = sb.length();
            if (m - from < n) return false;
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

        final AtomicLong rowsSkipped = new AtomicLong(0);
        final AtomicLong headerWritten = new AtomicLong(0);
        final AtomicLong rowsIn = new AtomicLong(0);
        final AtomicLong rowsOut = new AtomicLong(0);
        volatile String errorMessage = null;

        TransformCallback(Charset inCs, Charset outCs,
                          String inCol, String inRow, String outCol, String outRow,
                          char inQuote, char outQuote, OutputQuoteMode quoteMode,
                          boolean preserveInputQuotes,
                          int expectedColumns, int skipLeadingRows, boolean readHeaderAfterSkip) {
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
        }

        /**
         * 실제 스트리밍 변환을 수행한다. 처리 순서: (1) 선행 N행 스킵 → (2) 옵션에 따라 헤더 1행을
         * 읽어 그대로 출력 → (3) 나머지 데이터 행을 읽어 컬럼 수 검증 후 출력.
         * 중간에 예외가 발생하면 errorMessage에 메시지를 보관한 뒤 다시 던져 onTrigger에서
         * REL_FAILURE로 라우팅할 수 있게 한다.
         *
         * <p>입력은 REPORT 모드 디코더({@link StrictDecodingReader}), 출력은 REPORT 모드 인코더로 처리하여
         * 디코딩할 수 없는 바이트나 출력 문자셋에 없는 문자가 있으면 위치 정보와 함께 실패시킨다(C-4).
         * 입력 문자셋이 UTF-8이면 선두 BOM을 제거한다(C-6).</p>
         */
        @Override
        public void process(InputStream rawIn, OutputStream rawOut) throws IOException {
            InputStream src = rawIn;
            long skippedBomBytes = 0;
            if (StandardCharsets.UTF_8.equals(inCs)) {
                BOMInputStream bom = BOMInputStream.builder().setInputStream(rawIn).get(); // 기본: UTF-8 BOM 제외
                if (bom.hasBOM()) skippedBomBytes = 3;
                src = bom;
            }
            CharsetEncoder encoder = outCs.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            try (Reader r = new StrictDecodingReader(src, inCs, skippedBomBytes);
                 CsvParser parser = new CsvParser(r, inCol, inRow, inQuote);
                 Writer w = new OutputStreamWriter(rawOut, encoder);
                 CsvWriter writer = new CsvWriter(w, outCol, outRow, outQuote, quoteMode, preserveInputQuotes)) {

                // (1) 선행 스킵: 지정된 행 수만큼 읽고 버린다. 스킵 도중 EOF에 도달했을 때
                // 헤더를 읽어야 하는 설정이면 오류로 처리하고, 아니면 그대로 빈 출력으로 종료한다.
                for (int i = 0; i < skipLeadingRows; i++) {
                    if (readRow(parser) == null) {
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

                // (2) 헤더 처리: 스킵 직후 1행을 읽어 컬럼 수를 검증한 뒤
                // 그대로 출력 첫 행에 기록한다.
                if (readHeaderAfterSkip) {
                    List<String> header = readRow(parser);
                    if (header == null)
                        throw new ProcessException("No header after skipping " + skipLeadingRows + " rows.");
                    if (expectedColumns > 0 && header.size() != expectedColumns) {
                        throw new ProcessException("Header column count mismatch (expected=" + expectedColumns + ", actual=" + header.size() + "): " + header);
                    }
                    writeRow(writer, header, parser.quotedFlags(), "header row");
                    headerWritten.set(1);
                }

                // (3) 데이터 행 처리: 컬럼 수가 EXPECTED_COLUMNS와 다르면 즉시 예외를 던져 전체 처리를 실패시킨다
                // (부분적으로 기록된 출력은 FlowFile에 반영되지 않는다 - session.write가 실패 처리).
                List<String> inRow;
                int dataIndex = 0;
                while ((inRow = readRow(parser)) != null) {
                    dataIndex++;
                    rowsIn.incrementAndGet();
                    if (expectedColumns > 0 && inRow.size() != expectedColumns) {
                        throw new ProcessException("Column count mismatch at data row " + dataIndex +
                                " (expected=" + expectedColumns + ", actual=" + inRow.size() + "): " + inRow);
                    }
                    writeRow(writer, inRow, parser.quotedFlags(), "data row " + dataIndex);
                    rowsOut.incrementAndGet();
                }
                writer.flush();
            } catch (ProcessException e) {
                errorMessage = e.getMessage();
                throw e;
            } catch (CharacterCodingException e) {
                // 행 단위 검사에서 걸러지지 않은 인코딩 오류(예: 스트림 끝에서 flush/close 중 발생)
                errorMessage = "Output charset " + outCs.name() + " encoding error: " + e;
                throw new ProcessException(errorMessage, e);
            }
        }

        // 한 레코드를 읽는다. 입력 디코딩 오류는 레코드 번호와 바이트 오프셋을 담은 ProcessException으로 바꾼다.
        private List<String> readRow(CsvParser parser) throws IOException {
            try {
                return parser.readNextRow();
            } catch (InputDecodingException e) {
                throw new ProcessException("Input charset " + inCs.name() + " cannot decode " +
                        (e.malformed ? "malformed" : "unmappable") + " byte sequence (length=" + e.length +
                        ") at byte offset " + e.byteOffset + " in record " + parser.recordNumber() +
                        " (1-based, including skipped/header rows)", e);
            }
        }

        // 한 행을 쓴다. 출력 문자셋으로 표현할 수 없는 문자가 있으면 행/컬럼/문자를 담은 ProcessException으로 바꾼다.
        private void writeRow(CsvWriter writer, List<String> row, BitSet quoted, String where) throws IOException {
            try {
                writer.writeRow(row, quoted);
            } catch (CharacterCodingException e) {
                throw new ProcessException(describeUnencodable(row, where, e), e);
            }
        }

        // 행에서 출력 문자셋으로 인코딩할 수 없는 첫 문자를 찾아 메시지를 만든다(오류 경로에서만 호출).
        private String describeUnencodable(List<String> row, String where, CharacterCodingException e) {
            CharsetEncoder check = outCs.newEncoder();
            for (int i = 0; i < row.size(); i++) {
                String v = row.get(i);
                if (v == null) continue;
                for (int k = 0; k < v.length(); ) {
                    int cp = v.codePointAt(k);
                    String c = new String(Character.toChars(cp));
                    if (!check.canEncode(c)) {
                        return String.format("Output charset %s cannot encode character '%s' (U+%04X) in %s, column %d",
                                outCs.name(), c, cp, where, i + 1);
                    }
                    k += Character.charCount(cp);
                }
            }
            return "Output charset " + outCs.name() + " cannot encode " + where + ": " + e;
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
        // EL 평가 결과가 잘못된 경우(지원하지 않는 문자셋, 정수가 아닌 컬럼 수, 빈 Quote 문자 등) 발생하는
        // RuntimeException이 세션 롤백(무한 재시도)으로 이어지지 않도록 평가 전체를 try 안에서 수행하고,
        // 실패하면 penalize 후 REL_FAILURE로 라우팅한다(C-5).
        final Charset inCs;
        final TransformCallback cb;
        try {
            inCs = resolveCharset(context.getProperty(INPUT_CHARSET).evaluateAttributeExpressions(ff).getValue());
            final Charset outCs = resolveCharset(context.getProperty(OUTPUT_CHARSET).evaluateAttributeExpressions(ff).getValue());
            final String inCol = context.getProperty(IN_COL_DELIM).evaluateAttributeExpressions(ff).getValue();
            final String inRow = context.getProperty(IN_ROW_DELIM).evaluateAttributeExpressions(ff).getValue();
            final String outCol = context.getProperty(OUT_COL_DELIM).evaluateAttributeExpressions(ff).getValue();
            final String outRow = context.getProperty(OUT_ROW_DELIM).evaluateAttributeExpressions(ff).getValue();

            final char inQuote = singleChar(IN_QUOTE, context.getProperty(IN_QUOTE).evaluateAttributeExpressions(ff).getValue());
            final char outQuote = singleChar(OUT_QUOTE, context.getProperty(OUT_QUOTE).evaluateAttributeExpressions(ff).getValue());
            final OutputQuoteMode outMode = OutputQuoteMode.valueOf(context.getProperty(OUT_QUOTE_MODE).getValue());
            final boolean preserve = context.getProperty(PRESERVE_INPUT_QUOTES).asBoolean();

            final int expectedCols = context.getProperty(EXPECTED_COLUMNS).evaluateAttributeExpressions(ff).asInteger();
            final int skipRows = context.getProperty(SKIP_LEADING_ROWS).evaluateAttributeExpressions(ff).asInteger();
            final boolean readHeader = context.getProperty(READ_HEADER_AFTER_SKIP).asBoolean();

            // 구분자 길이 방어(EL로 평가된 값은 설정 검증을 거치지 않으므로 실행 시점에 다시 검사한다)
            if (inCol.length() < 2 || inRow.length() < 2 || outCol.length() < 2 || outRow.length() < 2) {
                ff = session.putAttribute(ff, "csv.error", "Delimiters must be length >= 2");
                session.transfer(ff, REL_FAILURE);
                return;
            }

            cb = new TransformCallback(
                    inCs, outCs, inCol, inRow, outCol, outRow,
                    inQuote, outQuote, outMode, preserve,
                    expectedCols, skipRows, readHeader
            );
        } catch (RuntimeException e) {
            String err = truncateError("Failed to evaluate properties: " + e);
            log.error("CSV parser configuration failed for {}: {}", ff, err, e);
            ff = session.penalize(ff);
            ff = session.putAttribute(ff, "csv.error", err);
            session.transfer(ff, REL_FAILURE);
            return;
        }

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
            String err = truncateError((cb.errorMessage != null) ? cb.errorMessage : pe.getMessage());
            log.error("CSV parsing failed: {}", err, pe);
            ff = session.putAttribute(ff, "csv.error", err);
            session.transfer(ff, REL_FAILURE);
        }
    }
}