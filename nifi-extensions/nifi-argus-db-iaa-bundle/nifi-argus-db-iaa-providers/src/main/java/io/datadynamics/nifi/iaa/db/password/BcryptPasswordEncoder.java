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
package io.datadynamics.nifi.iaa.db.password;

import at.favre.lib.crypto.bcrypt.BCrypt;

/**
 * bcrypt 구현. cost 12, {@code $2b$} 형식 — NiFi single-user 프로바이더와 동일하므로
 * {@code nifi.sh set-single-user-credentials} 로 만든 해시를 그대로 검증할 수 있다.
 */
public class BcryptPasswordEncoder implements PasswordEncoder {

    /** NiFi single-user 프로바이더와 같은 cost. 낮추면 대입 공격 비용이 낮아진다. */
    public static final int DEFAULT_COST = 12;

    private final int cost;

    public BcryptPasswordEncoder() {
        this(DEFAULT_COST);
    }

    public BcryptPasswordEncoder(final int cost) {
        this.cost = cost;
    }

    @Override
    public String encode(final char[] password) {
        return BCrypt.with(BCrypt.Version.VERSION_2B).hashToString(cost, password);
    }

    @Override
    public boolean matches(final char[] password, final String hash) {
        if (password == null || hash == null || hash.isEmpty()) {
            return false;
        }
        // verifyStrict 는 형식이 잘못된 해시에 예외 대신 verified=false 를 돌려준다.
        // 인증 실패와 형식 오류를 호출부에서 구분할 필요가 없으므로 그대로 반환한다.
        return BCrypt.verifyer().verify(password, hash.toCharArray()).verified;
    }
}
