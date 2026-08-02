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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.datadynamics.nifi.iaa.db.password.BcryptPasswordEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@link DbLoginIdentityProvider} 를 H2 인메모리 DB 로 검증한다. */
class DbLoginIdentityProviderTest {

    private static final String PASSWORD = "correct-horse-battery";

    private DataSource dataSource;
    private DbLoginIdentityProvider provider;

    @BeforeEach
    void setUp() throws SQLException {
        final JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:iaa-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        dataSource = h2;

        provider = new DbLoginIdentityProvider();
        provider.configureForTesting(dataSource, Dialect.H2, 3, Duration.ofMinutes(15));
    }

    private void insertUser(final String identity, final String plainPassword,
                            final boolean enabled) throws SQLException {
        final String hash = plainPassword == null
                ? null
                : provider.getPasswordEncoder().encode(plainPassword.toCharArray());
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement(
                     "INSERT INTO argus_user (id, identity, password_hash, enabled) VALUES (?,?,?,?)")) {
            ps.setString(1, UUID.randomUUID().toString());
            ps.setString(2, identity);
            ps.setString(3, hash);
            ps.setBoolean(4, enabled);
            ps.executeUpdate();
        }
    }

    private int failedCount(final String identity) throws SQLException {
        try (Connection c = dataSource.getConnection();
             var ps = c.prepareStatement("SELECT failed_count FROM argus_user WHERE identity = ?")) {
            ps.setString(1, identity);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    @Test
    void 올바른_비밀번호로_인증에_성공한다() throws Exception {
        insertUser("alice", PASSWORD, true);

        final AuthenticationResponse response =
                provider.authenticate(new LoginCredentials("alice", PASSWORD));

        assertNotNull(response);
        assertEquals("alice", response.getIdentity());
        assertEquals("alice", response.getUsername());
        assertEquals(Duration.ofHours(12).toMillis(), response.getExpiration());
        assertEquals(0, failedCount("alice"), "성공 시 실패 카운터가 초기화되어야 한다");
    }

    @Test
    void 잘못된_비밀번호는_거부하고_실패를_기록한다() throws Exception {
        insertUser("alice", PASSWORD, true);

        assertThrows(InvalidLoginCredentialsException.class,
                () -> provider.authenticate(new LoginCredentials("alice", "wrong")));

        assertEquals(1, failedCount("alice"));
    }

    @Test
    void 존재하지_않는_사용자도_같은_예외로_거부한다() {
        final InvalidLoginCredentialsException e = assertThrows(InvalidLoginCredentialsException.class,
                () -> provider.authenticate(new LoginCredentials("nobody", PASSWORD)));
        // 계정 열거 방지: 사용자 없음과 비밀번호 불일치의 메시지가 같아야 한다
        assertEquals("아이디 또는 비밀번호가 올바르지 않습니다.", e.getMessage());
    }

    @Test
    void 비활성_사용자는_비밀번호가_맞아도_거부한다() throws Exception {
        insertUser("bob", PASSWORD, false);

        assertThrows(InvalidLoginCredentialsException.class,
                () -> provider.authenticate(new LoginCredentials("bob", PASSWORD)));
    }

    @Test
    void 비밀번호가_없는_사용자는_비밀번호_인증을_거부한다() throws Exception {
        // 인증서·OIDC 로 인증하고 인가만 DB 로 관리하는 사용자
        insertUser("cert-user", null, true);

        assertThrows(InvalidLoginCredentialsException.class,
                () -> provider.authenticate(new LoginCredentials("cert-user", PASSWORD)));
    }

    @Test
    void 빈_자격증명은_DB_조회_없이_거부한다() {
        assertThrows(InvalidLoginCredentialsException.class,
                () -> provider.authenticate(new LoginCredentials("alice", "")));
        assertThrows(InvalidLoginCredentialsException.class,
                () -> provider.authenticate(new LoginCredentials("", PASSWORD)));
    }

    @Test
    void 연속_실패가_한계에_도달하면_계정을_잠근다() throws Exception {
        insertUser("dave", PASSWORD, true);

        for (int i = 0; i < 3; i++) {
            assertThrows(InvalidLoginCredentialsException.class,
                    () -> provider.authenticate(new LoginCredentials("dave", "wrong")));
        }
        assertEquals(3, failedCount("dave"));

        // 잠긴 뒤에는 올바른 비밀번호도 거부한다
        assertThrows(InvalidLoginCredentialsException.class,
                () -> provider.authenticate(new LoginCredentials("dave", PASSWORD)));
    }

    @Test
    void 잠금_시각이_지나면_다시_인증할_수_있다() throws Exception {
        insertUser("erin", PASSWORD, true);
        // 이미 만료된 잠금을 직접 심는다
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("UPDATE argus_user SET failed_count = 3, locked_until = TIMESTAMP '"
                    + Timestamp.from(Instant.now().minusSeconds(60)) + "' WHERE identity = 'erin'");
        }

        final AuthenticationResponse response =
                provider.authenticate(new LoginCredentials("erin", PASSWORD));

        assertNotNull(response);
        assertEquals(0, failedCount("erin"), "성공 시 잠금과 카운터가 해제되어야 한다");
    }

    @Test
    void single_user_프로바이더가_만든_해시를_그대로_검증한다() {
        // NiFi 의 nifi.sh set-single-user-credentials 는 $2b$12$ 형식을 쓴다.
        // 같은 라이브러리·형식이므로 이관한 해시가 그대로 동작해야 한다.
        final BcryptPasswordEncoder cost12 = new BcryptPasswordEncoder();
        final String hash = cost12.encode("a-twelve-char-password".toCharArray());

        assertTrue(hash.startsWith("$2b$12$"), "형식이 NiFi single-user 와 같아야 한다: " + hash);
        assertTrue(cost12.matches("a-twelve-char-password".toCharArray(), hash));
        assertFalse(cost12.matches("other".toCharArray(), hash));
        assertFalse(cost12.matches("x".toCharArray(), "형식이-아닌-문자열"));
    }
}
