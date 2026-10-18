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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * HiveServer2 세션 설정({@code SET key=value})을 바꾸기 전 값을 읽어 두고 나중에 되돌린다.
 * <p>
 * 커넥션 풀은 HiveServer2 세션을 재사용하므로 바꾼 설정이 같은 풀을 쓰는 다른 프로세서로 새지 않게 해야 한다.
 * 이전 값은 {@code SET key} 로 읽는다. Hive 3.1.3·4.0.1 의 응답은 컬럼 {@code set} 한 행으로 {@code key=value} 또는
 * {@code key is undefined} 다. 정의돼 있지 않던 키는 {@code RESET key} 로 되돌린다(Hive 3.0+).
 * 한 키를 여러 번 바꿔도 처음 값만 기억한다. 되돌리기는 바꾼 역순으로 한다.
 */
public final class SessionOverrides {

    /**
     * SQL 실행 추상화(단위 테스트용).
     */
    public interface SqlRunner {
        /** 결과 집합의 첫 컬럼 값들. */
        List<String> queryFirstColumn(String sql) throws SQLException;

        void execute(String sql) throws SQLException;
    }

    private static final Pattern KEY = Pattern.compile("[A-Za-z0-9_.\\-]+");
    private static final Pattern VALUE = Pattern.compile("[A-Za-z0-9_.\\-:/]*");

    private final Map<String, String> originals = new LinkedHashMap<>();
    private final Map<String, Boolean> defined = new LinkedHashMap<>();

    /**
     * 키를 {@code value} 로 바꾼다. 처음 바꾸는 키면 이전 값을 먼저 읽는다.
     */
    public void set(final SqlRunner runner, final String key, final String value) throws SQLException {
        requireKey(key);
        if (!VALUE.matcher(value).matches()) {
            throw new IllegalArgumentException("세션 설정 값 '" + value + "' 은(는) 허용하지 않는 문자를 포함한다");
        }
        if (!originals.containsKey(key)) {
            final String original = read(runner, key);
            originals.put(key, original);
            defined.put(key, original != null);
        }
        runner.execute("SET " + key + "=" + value);
    }

    /**
     * 현재 세션 값을 읽는다. 정의돼 있지 않으면 {@code null}.
     */
    public static String read(final SqlRunner runner, final String key) throws SQLException {
        requireKey(key);
        final List<String> rows = runner.queryFirstColumn("SET " + key);
        for (final String row : rows) {
            final String parsed = parseSetRow(row, key);
            if (parsed != null || (row != null && row.trim().startsWith(key + " is undefined"))) {
                return parsed;
            }
        }
        return null;
    }

    /**
     * {@code SET key} 결과 한 행을 해석한다. {@code key=value} 면 value, {@code key is undefined} 나 다른 키면 {@code null}.
     */
    static String parseSetRow(final String row, final String key) {
        if (row == null) {
            return null;
        }
        final String trimmed = row.trim();
        if (trimmed.startsWith(key + "=")) {
            return trimmed.substring(key.length() + 1);
        }
        return null;
    }

    /**
     * 되돌릴 문장(바꾼 역순). 정의돼 있던 키는 {@code SET key=원래값}, 아니면 {@code RESET key}.
     */
    public List<String> restoreStatements() {
        final List<String> keys = new ArrayList<>(originals.keySet());
        final List<String> statements = new ArrayList<>();
        for (int i = keys.size() - 1; i >= 0; i--) {
            final String key = keys.get(i);
            statements.add(Boolean.TRUE.equals(defined.get(key)) ? "SET " + key + "=" + originals.get(key) : "RESET " + key);
        }
        return statements;
    }

    /**
     * 모든 키를 되돌린다. 한 문장이 실패해도 나머지를 계속 실행하고, 실패를 모아 마지막에 던진다.
     */
    public void restore(final SqlRunner runner) throws SQLException {
        SQLException failure = null;
        for (final String statement : restoreStatements()) {
            try {
                runner.execute(statement);
            } catch (final SQLException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        originals.clear();
        defined.clear();
        if (failure != null) {
            throw failure;
        }
    }

    public boolean isEmpty() {
        return originals.isEmpty();
    }

    /**
     * 바꾼 키와 원래 값({@code null} = 정의 안 됨).
     */
    public Map<String, String> originals() {
        return new LinkedHashMap<>(originals);
    }

    private static void requireKey(final String key) {
        if (key == null || !KEY.matcher(key).matches()) {
            throw new IllegalArgumentException("세션 설정 키 '" + key + "' 이(가) 올바르지 않다");
        }
    }
}
