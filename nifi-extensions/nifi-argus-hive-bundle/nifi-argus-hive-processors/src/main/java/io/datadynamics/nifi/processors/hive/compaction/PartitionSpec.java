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
package io.datadynamics.nifi.processors.hive.compaction;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * {@code dt='2026-10-01',hour='03'} 형식의 파티션 지정.
 * <p>
 * 키는 {@link HiveIdentifiers} 규칙을 따르고, 값은 작은따옴표로 감싼다. 값 안에서는 {@code \'}(작은따옴표)와
 * {@code \\}(역슬래시)를 이스케이프로 읽는다. 키는 Hive 처럼 소문자로 다룬다. 빈 문자열은 "파티션 없음"(테이블 전체)이다.
 */
public final class PartitionSpec {

    private static final PartitionSpec EMPTY = new PartitionSpec(new LinkedHashMap<>());

    private final Map<String, String> values;

    private PartitionSpec(final LinkedHashMap<String, String> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public static PartitionSpec empty() {
        return EMPTY;
    }

    /**
     * 문자열을 파싱한다. {@code null}·공백은 빈 지정이다.
     *
     * @throws IllegalArgumentException 형식 오류, 잘못된 키, 중복 키
     */
    public static PartitionSpec parse(final String spec) {
        if (spec == null || spec.isBlank()) {
            return EMPTY;
        }
        final LinkedHashMap<String, String> values = new LinkedHashMap<>();
        final int length = spec.length();
        int i = 0;
        while (true) {
            i = skipWhitespace(spec, i);
            final int keyStart = i;
            while (i < length && spec.charAt(i) != '=' && spec.charAt(i) != ',' && !Character.isWhitespace(spec.charAt(i))) {
                i++;
            }
            final String key = spec.substring(keyStart, i);
            if (key.isEmpty()) {
                throw new IllegalArgumentException("Partition Spec 의 " + (keyStart + 1) + "번째 문자에 파티션 키가 없다: " + spec);
            }
            HiveIdentifiers.require("파티션 키", key);
            i = skipWhitespace(spec, i);
            if (i >= length || spec.charAt(i) != '=') {
                throw new IllegalArgumentException("Partition Spec 에서 키 '" + key + "' 뒤에 '=' 가 필요하다: " + spec);
            }
            i = skipWhitespace(spec, i + 1);
            if (i >= length || spec.charAt(i) != '\'') {
                throw new IllegalArgumentException("Partition Spec 에서 키 '" + key + "' 의 값은 작은따옴표로 감싸야 한다(예: "
                        + key + "='값'): " + spec);
            }
            i++;
            final StringBuilder value = new StringBuilder();
            boolean closed = false;
            while (i < length) {
                final char c = spec.charAt(i);
                if (c == '\\' && i + 1 < length) {
                    value.append(spec.charAt(i + 1));
                    i += 2;
                } else if (c == '\'') {
                    closed = true;
                    i++;
                    break;
                } else {
                    value.append(c);
                    i++;
                }
            }
            if (!closed) {
                throw new IllegalArgumentException("Partition Spec 에서 키 '" + key + "' 의 값에 닫는 작은따옴표가 없다: " + spec);
            }
            final String normalizedKey = key.toLowerCase(Locale.ROOT);
            if (values.put(normalizedKey, value.toString()) != null) {
                throw new IllegalArgumentException("Partition Spec 에 파티션 키 '" + key + "' 가 중복됐다: " + spec);
            }
            i = skipWhitespace(spec, i);
            if (i >= length) {
                break;
            }
            if (spec.charAt(i) != ',') {
                throw new IllegalArgumentException("Partition Spec 의 " + (i + 1) + "번째 문자에서 ',' 가 필요하다: " + spec);
            }
            i++;
        }
        return new PartitionSpec(values);
    }

    private static int skipWhitespace(final String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * 키(소문자) → 값. 입력 순서를 지킨다.
     */
    public Map<String, String> values() {
        return values;
    }

    /**
     * 키 집합이 주어진 파티션 컬럼 집합과 같은지(대소문자 무시) 본다.
     */
    public boolean hasSameKeys(final List<String> partitionColumns) {
        final Set<String> expected = partitionColumns.stream().map(c -> c.toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(TreeSet::new));
        return expected.equals(new TreeSet<>(values.keySet()));
    }

    /**
     * 정규화된 문자열({@code k='v',k2='v2'}). {@link #parse(String)} 로 되읽을 수 있다. 빈 지정이면 빈 문자열.
     */
    public String toCanonicalString() {
        return values.entrySet().stream().map(e -> e.getKey() + "='" + escapeValue(e.getValue()) + "'")
                .collect(Collectors.joining(","));
    }

    /**
     * {@code PARTITION (`k`='v', ...)} 절. 빈 지정이면 빈 문자열.
     */
    public String toPartitionClause() {
        if (values.isEmpty()) {
            return "";
        }
        return values.entrySet().stream().map(e -> "`" + e.getKey() + "`='" + escapeValue(e.getValue()) + "'")
                .collect(Collectors.joining(", ", "PARTITION (", ")"));
    }

    /**
     * {@code WHERE `k` = 'v' AND ...} 절. 빈 지정이면 빈 문자열.
     */
    public String toWhereClause() {
        if (values.isEmpty()) {
            return "";
        }
        return values.entrySet().stream().map(e -> "`" + e.getKey() + "` = '" + escapeValue(e.getValue()) + "'")
                .collect(Collectors.joining(" AND ", "WHERE ", ""));
    }

    /**
     * {@code SHOW COMPACTIONS} 의 파티션 이름({@code k=v/k2=v2}, Hive 경로 이스케이프 {@code %XX})과 같은 파티션인지 본다.
     * 순서는 무시한다. 빈 지정은 파티션 없음({@code null}, 빈 값, {@code ---})과 같다.
     */
    public boolean matchesPartitionName(final String partitionName) {
        final boolean noPartition = partitionName == null || partitionName.isBlank() || partitionName.trim().equals("---");
        if (values.isEmpty() || noPartition) {
            return values.isEmpty() && noPartition;
        }
        final Map<String, String> parsed = new LinkedHashMap<>();
        for (final String part : partitionName.trim().split("/")) {
            final int eq = part.indexOf('=');
            if (eq < 0) {
                return false;
            }
            parsed.put(unescapePathName(part.substring(0, eq)).toLowerCase(Locale.ROOT), unescapePathName(part.substring(eq + 1)));
        }
        return parsed.equals(values);
    }

    /**
     * SQL 문자열 리터럴(작은따옴표) 안에 넣을 값을 이스케이프한다.
     */
    public static String escapeValue(final String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'");
    }

    /**
     * Hive {@code FileUtils.unescapePathName} 과 같은 {@code %XX} 복원.
     */
    static String unescapePathName(final String path) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < path.length(); i++) {
            final char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                final int code = hex(path.charAt(i + 1)) * 16 + hex(path.charAt(i + 2));
                if (hex(path.charAt(i + 1)) >= 0 && hex(path.charAt(i + 2)) >= 0) {
                    out.write(code);
                    i += 2;
                    continue;
                }
            }
            final byte[] bytes = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
            out.write(bytes, 0, bytes.length);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    private static int hex(final char c) {
        return Character.digit(c, 16);
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof PartitionSpec other && values.equals(other.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return toCanonicalString();
    }
}
