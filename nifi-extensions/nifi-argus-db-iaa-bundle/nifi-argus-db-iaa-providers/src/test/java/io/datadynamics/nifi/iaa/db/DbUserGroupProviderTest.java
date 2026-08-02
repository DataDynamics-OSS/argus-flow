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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@link DbUserGroupProvider} 를 H2 인메모리 DB 로 검증한다. */
class DbUserGroupProviderTest {

    private DataSource dataSource;
    private DbUserGroupProvider provider;

    @BeforeEach
    void setUp() throws SQLException {
        final JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:ugp-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
        dataSource = h2;

        provider = new DbUserGroupProvider();
        // 캐시를 끈 상태로 동작을 검증한다. 캐시 동작은 별도 테스트에서 다룬다.
        provider.configureForTesting(dataSource, Dialect.H2, Duration.ZERO);
    }

    private User addUser(final String identity) {
        return provider.addUser(new User.Builder()
                .identifier(UUID.randomUUID().toString())
                .identity(identity)
                .build());
    }

    @Test
    void 사용자를_추가하고_식별자와_identity_로_조회한다() {
        final User alice = addUser("alice");

        assertEquals(1, provider.getUsers().size());
        assertEquals("alice", provider.getUser(alice.getIdentifier()).getIdentity());
        assertEquals(alice.getIdentifier(), provider.getUserByIdentity("alice").getIdentifier());
        assertNull(provider.getUserByIdentity("nobody"));
        assertNull(provider.getUser("없는-식별자"));
    }

    @Test
    void 사용자의_identity_를_바꿔도_식별자는_유지된다() {
        final User alice = addUser("alice");

        provider.updateUser(new User.Builder()
                .identifier(alice.getIdentifier())
                .identity("alice.kim")
                .build());

        // 접근 정책이 참조하는 것은 식별자다. identity 변경이 권한 상실로 이어지면 안 된다.
        assertEquals("alice.kim", provider.getUser(alice.getIdentifier()).getIdentity());
        assertNull(provider.getUserByIdentity("alice"));
    }

    @Test
    void 사용자를_삭제하면_그룹_소속도_사라진다() {
        final User alice = addUser("alice");
        provider.addGroup(new Group.Builder()
                .identifier(UUID.randomUUID().toString())
                .name("admins")
                .addUser(alice.getIdentifier())
                .build());

        provider.deleteUser(alice);

        assertEquals(0, provider.getUsers().size());
        assertEquals(1, provider.getGroups().size(), "그룹 자체는 남아야 한다");
        assertTrue(provider.getGroups().iterator().next().getUsers().isEmpty());
    }

    @Test
    void 그룹을_추가하고_소속을_갱신한다() {
        final User alice = addUser("alice");
        final User bob = addUser("bob");
        final String groupId = UUID.randomUUID().toString();

        provider.addGroup(new Group.Builder()
                .identifier(groupId).name("admins").addUser(alice.getIdentifier()).build());
        assertEquals(Set.of(alice.getIdentifier()), provider.getGroup(groupId).getUsers());

        // NiFi 는 부분 변경이 아니라 최종 상태를 넘긴다 — 전량 교체되어야 한다
        provider.updateGroup(new Group.Builder()
                .identifier(groupId).name("operators").addUser(bob.getIdentifier()).build());

        final Group updated = provider.getGroup(groupId);
        assertEquals("operators", updated.getName());
        assertEquals(Set.of(bob.getIdentifier()), updated.getUsers());
    }

    @Test
    void 그룹을_삭제해도_사용자는_남는다() {
        final User alice = addUser("alice");
        final String groupId = UUID.randomUUID().toString();
        final Group group = provider.addGroup(new Group.Builder()
                .identifier(groupId).name("admins").addUser(alice.getIdentifier()).build());

        provider.deleteGroup(group);

        assertEquals(0, provider.getGroups().size());
        assertEquals(1, provider.getUsers().size());
    }

    @Test
    void getUserAndGroups_는_사용자와_소속_그룹을_함께_돌려준다() {
        final User alice = addUser("alice");
        provider.addGroup(new Group.Builder()
                .identifier(UUID.randomUUID().toString()).name("admins")
                .addUser(alice.getIdentifier()).build());
        provider.addGroup(new Group.Builder()
                .identifier(UUID.randomUUID().toString()).name("others").build());

        final UserAndGroups result = provider.getUserAndGroups("alice");

        assertNotNull(result.getUser());
        assertEquals("alice", result.getUser().getIdentity());
        assertEquals(1, result.getGroups().size());
        assertEquals("admins", result.getGroups().iterator().next().getName());
    }

    @Test
    void 없는_사용자의_getUserAndGroups_는_EMPTY() {
        assertSame(UserAndGroups.EMPTY, provider.getUserAndGroups("nobody"));
    }

    @Test
    void fingerprint_는_사용자가_바뀌어도_변하지_않는다() {
        // 공유 DB 에서 사용자 목록으로 fingerprint 를 만들면 사용자를 추가하는 순간
        // 노드 간 값이 어긋나 클러스터 조인이 거부된다.
        final String before = provider.getFingerprint();

        addUser("alice");
        addUser("bob");
        provider.addGroup(new Group.Builder()
                .identifier(UUID.randomUUID().toString()).name("admins").build());

        assertEquals(before, provider.getFingerprint());
    }

    @Test
    void fingerprint_에_JDBC_URL_원문이_들어가지_않는다() {
        // fingerprint 는 노드 간에 오가고 로그에도 남을 수 있다. URL 에 자격증명이
        // 포함된 구성이 흔하므로 해시로만 담아야 한다.
        final String fingerprint = provider.getFingerprint();
        assertTrue(fingerprint.startsWith("argus-db-user-group-provider:"), fingerprint);
        assertTrue(fingerprint.endsWith(":v1"), fingerprint);
        assertTrue(!fingerprint.contains("jdbc:test"), fingerprint);
    }

    @Test
    void 같은_fingerprint_는_상속_가능하고_다른_값은_거부한다() {
        final String own = provider.getFingerprint();

        provider.checkInheritability(own);      // 예외 없이 통과해야 한다
        provider.inheritFingerprint(own);

        assertThrows(UninheritableAuthorizationsException.class,
                () -> provider.checkInheritability("argus-db-user-group-provider:다른값:v1"));
    }

    @Test
    void 강제_상속은_공유_DB_를_덮어쓰지_않는다() {
        addUser("alice");

        // 파일 기반처럼 상대 노드 상태로 덮어쓰면 공유 DB 가 파괴된다. 무시해야 한다.
        provider.forciblyInheritFingerprint("argus-db-user-group-provider:다른값:v1");

        assertEquals(1, provider.getUsers().size());
    }

    @Test
    void 캐시가_켜져_있으면_무효화_전까지_이전_결과를_돌려준다() throws Exception {
        final DbUserGroupProvider cached = new DbUserGroupProvider();
        cached.configureForTesting(dataSource, Dialect.H2, Duration.ofMinutes(5));

        assertEquals(0, cached.getUsers().size());

        // 이 프로바이더를 거치지 않고 DB 를 직접 바꾸면(=다른 노드의 변경) 캐시가 유지된다
        provider.addUser(new User.Builder()
                .identifier(UUID.randomUUID().toString()).identity("alice").build());
        assertEquals(0, cached.getUsers().size(), "TTL 안에서는 이전 스냅샷을 쓴다");

        // 자기 자신을 통한 변경은 즉시 반영되어야 한다
        cached.addUser(new User.Builder()
                .identifier(UUID.randomUUID().toString()).identity("bob").build());
        assertEquals(2, cached.getUsers().size());
    }
}
