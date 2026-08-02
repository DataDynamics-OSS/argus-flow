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
package io.datadynamics.nifi.iaa.db;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * 만료 시간이 있는 단일 값 캐시.
 *
 * <p>NiFi 인가 계층은 요청마다 사용자·그룹 목록을 조회하므로 매번 DB 를 때리면 부하가 크다.
 * 반면 인증({@link DbLoginIdentityProvider})은 캐시하지 않는다 — 비밀번호 변경과 계정 잠금은
 * 즉시 반영돼야 한다.
 *
 * <p>따라서 사용자 추가·그룹 변경이 다른 노드에 보이기까지 최대 TTL 만큼 지연될 수 있다.
 * 이 노드에서 직접 변경한 경우는 {@link #invalidate()} 로 즉시 반영된다. TTL 을 0 으로 두면
 * 캐시가 비활성화된다.
 */
final class CachedSnapshot {

    private final Duration ttl;

    private volatile Object value;
    private volatile long expiresAtNanos;

    CachedSnapshot(final Duration ttl) {
        this.ttl = ttl == null ? Duration.ZERO : ttl;
    }

    Duration ttl() {
        return ttl;
    }

    @SuppressWarnings("unchecked")
    <T> T get(final Supplier<T> loader) {
        if (ttl.isZero() || ttl.isNegative()) {
            return loader.get();
        }
        final Object current = value;
        if (current != null && System.nanoTime() < expiresAtNanos) {
            return (T) current;
        }
        // 만료 시 여러 스레드가 동시에 적재할 수 있다. 짧은 중복 조회는 락 경합보다 낫다.
        final T loaded = loader.get();
        value = loaded;
        expiresAtNanos = System.nanoTime() + ttl.toNanos();
        return loaded;
    }

    /** 이 노드에서 변경이 일어났을 때 즉시 반영하기 위해 비운다. */
    void invalidate() {
        value = null;
        expiresAtNanos = 0L;
    }
}
