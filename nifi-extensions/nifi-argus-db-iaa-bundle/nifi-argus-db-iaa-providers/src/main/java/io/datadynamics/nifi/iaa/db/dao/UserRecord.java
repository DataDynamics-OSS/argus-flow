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
package io.datadynamics.nifi.iaa.db.dao;

import java.time.Instant;

/**
 * {@code argus_user} 한 행.
 *
 * @param id           불변 식별자(UUID). NiFi 접근 정책이 참조하는 값이라 identity 가 바뀌어도
 *                     유지된다
 * @param identity     로그인 ID
 * @param passwordHash bcrypt 해시. {@code null} 이면 비밀번호 인증 대상이 아니다
 *                     (인증서·OIDC 로 인증하고 인가만 DB 로 관리하는 사용자)
 * @param enabled      비활성 사용자는 비밀번호가 맞아도 로그인할 수 없다
 * @param failedCount  연속 로그인 실패 횟수
 * @param lockedUntil  잠금 해제 시각. {@code null} 이거나 과거면 잠금 아님
 */
public record UserRecord(
        String id,
        String identity,
        String passwordHash,
        boolean enabled,
        int failedCount,
        Instant lockedUntil) {

    /** 지정 시각 기준으로 잠겨 있는지. */
    public boolean isLockedAt(final Instant now) {
        return lockedUntil != null && lockedUntil.isAfter(now);
    }
}
