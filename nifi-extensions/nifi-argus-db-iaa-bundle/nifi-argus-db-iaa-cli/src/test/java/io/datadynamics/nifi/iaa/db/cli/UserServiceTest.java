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
package io.datadynamics.nifi.iaa.db.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.datadynamics.nifi.iaa.db.Dialect;
import io.datadynamics.nifi.iaa.db.dao.UserRecord;
import io.datadynamics.nifi.iaa.db.password.BcryptPasswordEncoder;
import io.datadynamics.nifi.iaa.db.password.PasswordEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserServiceTest {

    private static final char[] PASSWORD = "correct-horse-battery".toCharArray();

    private DataSource dataSource;
    private UserService service;
    private PasswordEncoder encoder;

    @BeforeEach
    void setUp() throws SQLException {
        final JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:cli-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        dataSource = h2;
        encoder = new BcryptPasswordEncoder(4);
        service = new UserService(dataSource, Dialect.H2, encoder);
        service.initSchema();
    }

    @Test
    void schema_init_은_여러_번_실행해도_안전하다() throws Exception {
        service.initSchema();
        service.initSchema();
        assertEquals(0, service.listUsers().size());
    }

    @Test
    void 사용자를_추가하면_비밀번호가_해시로_저장된다() throws Exception {
        service.addUser("alice", PASSWORD, true);

        final UserRecord user = service.requireUser("alice");
        assertNotNull(user.passwordHash());
        assertFalse(new String(user.passwordHash()).contains("correct-horse"),
                "평문이 저장되면 안 된다");
        assertTrue(encoder.matches(PASSWORD, user.passwordHash()));
        assertTrue(user.enabled());
    }

    @Test
    void 비밀번호_없이_만들면_비밀번호_인증_대상이_아니다() throws Exception {
        // 인증서·OIDC 로 인증하고 인가만 DB 로 관리하는 사용자
        service.addUser("cert-user", null, true);
        assertNull(service.requireUser("cert-user").passwordHash());
    }

    @Test
    void 중복_identity_는_거부한다() throws Exception {
        service.addUser("alice", PASSWORD, true);
        assertThrows(CliException.class, () -> service.addUser("alice", PASSWORD, true));
    }

    @Test
    void 없는_사용자를_다루면_알기_쉬운_오류를_낸다() {
        final CliException e = assertThrows(CliException.class, () -> service.requireUser("nobody"));
        assertTrue(e.getMessage().contains("nobody"), e.getMessage());
    }

    @Test
    void 비밀번호_변경은_잠금도_함께_해제한다() throws Exception {
        service.addUser("alice", PASSWORD, true);
        lock("alice");

        service.setPassword("alice", "brand-new-password".toCharArray());

        final UserRecord user = service.requireUser("alice");
        assertTrue(encoder.matches("brand-new-password".toCharArray(), user.passwordHash()));
        assertEquals(0, user.failedCount());
        assertFalse(user.isLockedAt(Instant.now()));
    }

    @Test
    void unlock_은_실패_횟수와_잠금을_해제한다() throws Exception {
        service.addUser("alice", PASSWORD, true);
        lock("alice");

        service.unlock("alice");

        final UserRecord user = service.requireUser("alice");
        assertEquals(0, user.failedCount());
        assertFalse(user.isLockedAt(Instant.now()));
    }

    @Test
    void rename_은_식별자를_유지한다() throws Exception {
        service.addUser("alice", PASSWORD, true);
        final String id = service.requireUser("alice").id();

        service.rename("alice", "alice.kim");

        // 접근 정책은 식별자를 참조한다. 이름을 바꿔도 권한이 유지되어야 한다.
        assertEquals(id, service.requireUser("alice.kim").id());
        assertThrows(CliException.class, () -> service.requireUser("alice"));
    }

    @Test
    void 이미_있는_identity_로는_rename_하지_못한다() throws Exception {
        service.addUser("alice", PASSWORD, true);
        service.addUser("bob", PASSWORD, true);
        assertThrows(CliException.class, () -> service.rename("alice", "bob"));
    }

    @Test
    void 활성화와_비활성화() throws Exception {
        service.addUser("alice", PASSWORD, true);

        service.setEnabled("alice", false);
        assertFalse(service.requireUser("alice").enabled());

        service.setEnabled("alice", true);
        assertTrue(service.requireUser("alice").enabled());
    }

    @Test
    void 그룹_생성과_소속_관리() throws Exception {
        service.addUser("alice", PASSWORD, true);
        service.addGroup("admins");

        service.setMembership("admins", "alice", true);
        assertEquals(1, service.groupsOf(service.requireUser("alice").id()).size());

        service.setMembership("admins", "alice", false);
        assertEquals(0, service.groupsOf(service.requireUser("alice").id()).size());
    }

    @Test
    void 없는_그룹이나_사용자는_거부한다() throws Exception {
        service.addUser("alice", PASSWORD, true);
        service.addGroup("admins");
        assertThrows(CliException.class, () -> service.setMembership("없는그룹", "alice", true));
        assertThrows(CliException.class, () -> service.setMembership("admins", "없는사용자", true));
        assertThrows(CliException.class, () -> service.addGroup("admins"));
    }

    @Test
    void 사용자를_지우면_소속도_사라지고_그룹은_남는다() throws Exception {
        service.addUser("alice", PASSWORD, true);
        service.addGroup("admins");
        service.setMembership("admins", "alice", true);

        service.deleteUser("alice");

        assertEquals(0, service.listUsers().size());
        assertEquals(1, service.listGroups().size());
        assertEquals(0, service.listGroups().get(0).userIds().size());
    }

    private void lock(final String identity) throws SQLException {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("UPDATE argus_user SET failed_count = 5, "
                    + "locked_until = DATEADD('MINUTE', 15, CURRENT_TIMESTAMP) "
                    + "WHERE identity = '" + identity + "'");
        }
    }
}
