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

import java.util.regex.Pattern;

/**
 * Hive 식별자(DB·테이블·파티션 키) 검증과 인용.
 * <p>
 * 생성 SQL 에 사용자 값이 그대로 들어가므로 {@code [A-Za-z0-9_]+} 만 허용하고 백틱으로 감싼다.
 */
public final class HiveIdentifiers {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");

    private HiveIdentifiers() {
    }

    /**
     * 식별자 규칙을 지키는지 본다.
     */
    public static boolean isValid(final String identifier) {
        return identifier != null && IDENTIFIER.matcher(identifier).matches();
    }

    /**
     * 식별자를 검증하고 그대로 돌려준다.
     *
     * @param kind 오류 메시지에 쓸 종류(예: "데이터베이스")
     * @throws IllegalArgumentException 규칙 위반
     */
    public static String require(final String kind, final String identifier) {
        if (!isValid(identifier)) {
            throw new IllegalArgumentException(kind + " 이름 '" + identifier + "' 은(는) 영문자·숫자·밑줄([A-Za-z0-9_]+)만 쓸 수 있다");
        }
        return identifier;
    }

    /**
     * 검증 뒤 백틱으로 감싼다.
     */
    public static String quote(final String kind, final String identifier) {
        return "`" + require(kind, identifier) + "`";
    }
}
