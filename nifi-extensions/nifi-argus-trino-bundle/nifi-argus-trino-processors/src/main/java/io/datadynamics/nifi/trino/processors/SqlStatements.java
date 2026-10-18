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
package io.datadynamics.nifi.trino.processors;

import java.util.ArrayList;
import java.util.List;

/**
 * Trino JDBC 는 한 번에 한 문장만 실행한다. 끝의 {@code ;} 하나는 제거하고, 그 밖에 따옴표·주석 밖의 {@code ;}가 있으면
 * 여러 문장으로 보고 거부한다(작은따옴표 문자열, 큰따옴표 식별자, {@code --}·{@code /* *}{@code /} 주석을 인식).
 */
public final class SqlStatements {

    private SqlStatements() {
    }

    /**
     * @return 끝의 세미콜론을 뗀 단일 문장
     * @throws IllegalArgumentException 비어 있거나 여러 문장인 경우
     */
    public static String requireSingleStatement(final String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("SQL 이 비어 있다");
        }
        final List<Integer> separators = topLevelSemicolons(sql);
        if (separators.isEmpty()) {
            return sql.trim();
        }
        final int last = separators.get(separators.size() - 1);
        if (separators.size() == 1 && isOnlyWhitespaceOrComments(sql.substring(last + 1))) {
            final String statement = sql.substring(0, last).trim();
            if (statement.isEmpty()) {
                throw new IllegalArgumentException("SQL 이 비어 있다");
            }
            return statement;
        }
        throw new IllegalArgumentException("여러 문장은 실행할 수 없다(Trino JDBC 는 한 번에 한 문장만 실행한다). 문장을 FlowFile 하나씩 나눠야 한다");
    }

    static List<Integer> topLevelSemicolons(final String sql) {
        final List<Integer> positions = new ArrayList<>();
        final int length = sql.length();
        int i = 0;
        while (i < length) {
            final char c = sql.charAt(i);
            if (c == '\'' || c == '"') {
                i = skipQuoted(sql, i, c);
            } else if (c == '-' && i + 1 < length && sql.charAt(i + 1) == '-') {
                final int newline = sql.indexOf('\n', i);
                i = newline < 0 ? length : newline + 1;
            } else if (c == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
                final int end = sql.indexOf("*/", i + 2);
                i = end < 0 ? length : end + 2;
            } else {
                if (c == ';') {
                    positions.add(i);
                }
                i++;
            }
        }
        return positions;
    }

    private static int skipQuoted(final String sql, final int start, final char quote) {
        int i = start + 1;
        while (i < sql.length()) {
            if (sql.charAt(i) == quote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                    i += 2; // '' 또는 "" 이스케이프
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return sql.length();
    }

    private static boolean isOnlyWhitespaceOrComments(final String tail) {
        int i = 0;
        while (i < tail.length()) {
            final char c = tail.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '-' && i + 1 < tail.length() && tail.charAt(i + 1) == '-') {
                final int newline = tail.indexOf('\n', i);
                i = newline < 0 ? tail.length() : newline + 1;
            } else if (c == '/' && i + 1 < tail.length() && tail.charAt(i + 1) == '*') {
                final int end = tail.indexOf("*/", i + 2);
                i = end < 0 ? tail.length() : end + 2;
            } else {
                return false;
            }
        }
        return true;
    }
}
