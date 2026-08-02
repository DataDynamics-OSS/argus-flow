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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProviderConfigTest {

    private ProviderConfig config(final Map<String, String> props, final Map<String, String> env) {
        return new ProviderConfig(props, env::get);
    }

    @Test
    void 빈_값은_기본값으로_대체된다() {
        final ProviderConfig c = config(Map.of("Database User", "   "), Map.of());
        assertEquals("nifi", c.getString("Database User", "nifi"));
        assertEquals("nifi", c.getString("없는키", "nifi"));
    }

    @Test
    void 필수값이_없으면_예외() {
        final ProviderConfig c = config(Map.of(), Map.of());
        assertThrows(IllegalArgumentException.class, () -> c.getRequired("Database URL"));
    }

    @Test
    void 환경변수_참조를_치환한다() {
        // XML 에 DB 비밀번호를 평문으로 남기지 않기 위한 경로
        final ProviderConfig c = config(
                Map.of("Database Password", "${ARGUS_DB_PW}"),
                Map.of("ARGUS_DB_PW", "s3cret"));
        assertEquals("s3cret", c.getString("Database Password", null));
    }

    @Test
    void 환경변수가_없으면_예외로_알린다() {
        final ProviderConfig c = config(Map.of("Database Password", "${MISSING_VAR}"), Map.of());
        // 조용히 빈 값으로 두면 인증이 실패하는 이유를 찾기 어렵다
        assertThrows(IllegalArgumentException.class, () -> c.getString("Database Password", null));
    }

    @Test
    void NiFi_표기의_기간을_읽는다() {
        final ProviderConfig c = config(Map.of(
                "a", "12 hours", "b", "15 mins", "c", "30 secs", "d", "1 day"), Map.of());
        assertEquals(Duration.ofHours(12), c.getDuration("a", null));
        assertEquals(Duration.ofMinutes(15), c.getDuration("b", null));
        assertEquals(Duration.ofSeconds(30), c.getDuration("c", null));
        assertEquals(Duration.ofDays(1), c.getDuration("d", null));
        assertEquals(Duration.ofHours(1), c.getDuration("없음", Duration.ofHours(1)));
    }

    @Test
    void 잘못된_기간_표기는_예외() {
        final ProviderConfig c = config(Map.of("a", "언젠가", "b", "5 파섹"), Map.of());
        assertThrows(IllegalArgumentException.class, () -> c.getDuration("a", null));
        assertThrows(IllegalArgumentException.class, () -> c.getDuration("b", null));
    }

    @Test
    void 정수와_불린을_읽는다() {
        final ProviderConfig c = config(Map.of("n", "7", "b", "true", "bad", "일곱"), Map.of());
        assertEquals(7, c.getInt("n", 1));
        assertEquals(1, c.getInt("없음", 1));
        assertEquals(true, c.getBoolean("b", false));
        assertThrows(IllegalArgumentException.class, () -> c.getInt("bad", 1));
    }
}
