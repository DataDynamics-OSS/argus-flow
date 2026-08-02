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

import com.zaxxer.hikari.HikariDataSource;
import io.datadynamics.nifi.iaa.db.dao.GroupDao;
import io.datadynamics.nifi.iaa.db.dao.GroupRecord;
import io.datadynamics.nifi.iaa.db.dao.UserDao;
import io.datadynamics.nifi.iaa.db.dao.UserRecord;
import io.datadynamics.nifi.iaa.db.schema.SchemaManager;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.nifi.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RDB 를 사용자·그룹 저장소로 쓰는 인가 프로바이더.
 *
 * <p>{@link ConfigurableUserGroupProvider} 를 구현하므로 NiFi UI 에서 사용자·그룹을
 * 추가·삭제할 수 있고 결과가 DB 에 반영된다. CLI 와 같은 테이블을 쓴다.
 *
 * <p>접근 정책은 다루지 않는다 — {@code file-access-policy-provider} 의
 * {@code User Group Provider} 로 이 프로바이더를 가리키면 정책은 파일, 사용자는 DB 가 된다.
 */
public class DbUserGroupProvider implements ConfigurableUserGroupProvider {

    private static final Logger logger = LoggerFactory.getLogger(DbUserGroupProvider.class);

    public static final String PROP_AUTO_CREATE_SCHEMA = "Auto Create Schema";

    /**
     * 인가 조회 캐시의 유지 시간. {@code 0 secs} 면 캐시하지 않는다.
     *
     * <p>NiFi 인가 계층은 사실상 모든 요청마다 사용자·그룹 목록을 요구하므로 캐시가 없으면
     * 요청당 쿼리가 된다. 반면 인증({@link DbLoginIdentityProvider})은 캐시하지 않는다 —
     * 비밀번호 변경과 계정 잠금은 즉시 반영돼야 한다.
     */
    public static final String PROP_CACHE_DURATION = "Cache Duration";

    /** {@link #PROP_CACHE_DURATION} 기본값. */
    public static final java.time.Duration DEFAULT_CACHE_DURATION = java.time.Duration.ofMinutes(1);

    /** {@code Initial User Identity 1}, {@code Initial User Identity 2} … (업스트림 규약) */
    public static final String PROP_INITIAL_USER_PREFIX = "Initial User Identity";

    private String identifier;
    private HikariDataSource dataSource;
    private UserDao userDao;
    private GroupDao groupDao;
    private String fingerprint;
    private CachedSnapshot cache;

    @Override
    public void initialize(final UserGroupProviderInitializationContext context) {
        this.identifier = context.getIdentifier();
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext context) {
        final Map<String, String> properties = context.getProperties();
        final ProviderConfig config = new ProviderConfig(properties);
        try {
            final String url = config.getRequired(DataSourceFactory.PROP_URL);
            final Dialect dialect = Dialect.fromJdbcUrl(url);
            this.dataSource = DataSourceFactory.create(config, identifier);
            this.userDao = new UserDao(dataSource);
            this.groupDao = new GroupDao(dataSource);

            final SchemaManager schemaManager = new SchemaManager(dataSource, dialect);
            schemaManager.ensureSchema(config.getBoolean(PROP_AUTO_CREATE_SCHEMA, false));

            this.cache = new CachedSnapshot(
                    config.getDuration(PROP_CACHE_DURATION, DEFAULT_CACHE_DURATION));
            this.fingerprint = computeFingerprint(url, schemaManager.readVersion());

            createInitialUsers(properties);

            logger.info("DB 인가 프로바이더 [{}] 구성 완료 (방언={}, 캐시={})",
                    identifier, dialect, cache.ttl());
        } catch (final SQLException | RuntimeException e) {
            close();
            throw new AuthorizerCreationException(
                    "DB 인가 프로바이더 [" + identifier + "] 구성에 실패했습니다: " + e.getMessage(), e);
        }
    }

    /**
     * 설정된 초기 사용자를 없으면 만든다.
     *
     * <p>없으면 "사용자를 추가하려면 로그인해야 하는데 로그인할 사용자가 없는" 교착에 빠진다.
     * 비밀번호는 넣지 않는다 — 관리자가 CLI 로 설정해야 로그인할 수 있다.
     */
    private void createInitialUsers(final Map<String, String> properties) throws SQLException {
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            if (!entry.getKey().startsWith(PROP_INITIAL_USER_PREFIX)) {
                continue;
            }
            final String identity = entry.getValue() == null ? null : entry.getValue().trim();
            if (identity == null || identity.isEmpty()) {
                continue;
            }
            if (userDao.findByIdentity(identity).isPresent()) {
                continue;
            }
            userDao.insert(UUID.randomUUID().toString(), identity, null, true);
            logger.info("초기 사용자 생성 [{}]. 로그인하려면 비밀번호를 설정해야 합니다: "
                    + "bin/argus-user.sh passwd {}", identity, identity);
        }
    }

    // ── 조회 ────────────────────────────────────────────────────────────────

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return snapshot().users();
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        return snapshot().users().stream()
                .filter(u -> u.getIdentifier().equals(identifier))
                .findFirst().orElse(null);
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        return snapshot().users().stream()
                .filter(u -> u.getIdentity().equals(identity))
                .findFirst().orElse(null);
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return snapshot().groups();
    }

    @Override
    public Group getGroup(final String identifier) throws AuthorizationAccessException {
        return snapshot().groups().stream()
                .filter(g -> g.getIdentifier().equals(identifier))
                .findFirst().orElse(null);
    }

    @Override
    public UserAndGroups getUserAndGroups(final String identity) throws AuthorizationAccessException {
        final Snapshot snapshot = snapshot();
        final User user = snapshot.users().stream()
                .filter(u -> u.getIdentity().equals(identity))
                .findFirst().orElse(null);
        if (user == null) {
            return UserAndGroups.EMPTY;
        }
        final Set<Group> groups = snapshot.groups().stream()
                .filter(g -> g.getUsers().contains(user.getIdentifier()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return new SimpleUserAndGroups(user, groups);
    }

    // ── 변경 ────────────────────────────────────────────────────────────────

    @Override
    public User addUser(final User user) throws AuthorizationAccessException {
        try {
            userDao.insert(user.getIdentifier(), user.getIdentity(), null, true);
            cache.invalidate();
            return user;
        } catch (final SQLException e) {
            throw new AuthorizationAccessException("사용자를 추가할 수 없습니다: " + user.getIdentity(), e);
        }
    }

    @Override
    public User updateUser(final User user) throws AuthorizationAccessException {
        try {
            if (!userDao.updateIdentity(user.getIdentifier(), user.getIdentity())) {
                return null;
            }
            cache.invalidate();
            return user;
        } catch (final SQLException e) {
            throw new AuthorizationAccessException("사용자를 수정할 수 없습니다: " + user.getIdentity(), e);
        }
    }

    @Override
    public User deleteUser(final User user) throws AuthorizationAccessException {
        try {
            if (!userDao.delete(user.getIdentifier())) {
                return null;
            }
            cache.invalidate();
            return user;
        } catch (final SQLException e) {
            throw new AuthorizationAccessException("사용자를 삭제할 수 없습니다: " + user.getIdentity(), e);
        }
    }

    @Override
    public Group addGroup(final Group group) throws AuthorizationAccessException {
        try {
            groupDao.insert(new GroupRecord(group.getIdentifier(), group.getName(), group.getUsers()));
            cache.invalidate();
            return group;
        } catch (final SQLException e) {
            throw new AuthorizationAccessException("그룹을 추가할 수 없습니다: " + group.getName(), e);
        }
    }

    @Override
    public Group updateGroup(final Group group) throws AuthorizationAccessException {
        try {
            if (!groupDao.update(new GroupRecord(group.getIdentifier(), group.getName(), group.getUsers()))) {
                return null;
            }
            cache.invalidate();
            return group;
        } catch (final SQLException e) {
            throw new AuthorizationAccessException("그룹을 수정할 수 없습니다: " + group.getName(), e);
        }
    }

    @Override
    public Group deleteGroup(final Group group) throws AuthorizationAccessException {
        try {
            if (!groupDao.delete(group.getIdentifier())) {
                return null;
            }
            cache.invalidate();
            return group;
        } catch (final SQLException e) {
            throw new AuthorizationAccessException("그룹을 삭제할 수 없습니다: " + group.getName(), e);
        }
    }

    // ── fingerprint ─────────────────────────────────────────────────────────

    /**
     * {@inheritDoc}
     *
     * <p><strong>사용자 목록으로 계산하지 않는다.</strong> 파일 기반 프로바이더는 노드마다
     * 파일이 따로라 내용을 fingerprint 로 쓰는 것이 맞지만, 여기서는 모든 노드가 같은 DB 를
     * 본다. 사용자 목록을 직렬화하면 사용자를 추가하는 순간 노드 간 fingerprint 가 어긋나
     * 조인이 거부된다. "같은 DB 를 보고 있는가"만 검증하면 충분하고, 데이터 일관성은 DB 가
     * 보장한다.
     */
    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        return fingerprint;
    }

    @Override
    public void inheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        // 공유 DB 라 상속할 상태가 없다. 대상이 같은지만 확인한다.
        checkInheritability(fingerprint);
    }

    @Override
    public void forciblyInheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        // 파일 기반처럼 상대 노드의 사용자 목록으로 덮어쓰면 안 된다 — 공유 DB 를 파괴한다.
        if (!this.fingerprint.equals(fingerprint)) {
            logger.warn("다른 DB 를 가리키는 fingerprint 를 강제 상속하라는 요청을 무시합니다. "
                    + "모든 노드가 같은 DB 를 보도록 authorizers.xml 을 맞추십시오.");
        }
    }

    @Override
    public void checkInheritability(final String proposed)
            throws AuthorizationAccessException, UninheritableAuthorizationsException {
        if (!this.fingerprint.equals(proposed)) {
            throw new UninheritableAuthorizationsException(
                    "다른 사용자 저장소를 가리키고 있습니다. 클러스터의 모든 노드가 같은 "
                            + "Database URL 과 스키마 버전을 쓰도록 authorizers.xml 을 맞추십시오.");
        }
    }

    /**
     * JDBC URL 은 해시해서 넣는다. fingerprint 는 노드 간에 오가고 로그에도 남을 수 있는데,
     * URL 에 자격증명이 포함된 구성이 흔하기 때문이다.
     */
    private static String computeFingerprint(final String jdbcUrl, final Integer schemaVersion) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final byte[] hash = digest.digest(jdbcUrl.getBytes(StandardCharsets.UTF_8));
            return "argus-db-user-group-provider:" + HexFormat.of().formatHex(hash)
                    + ":v" + (schemaVersion == null ? "0" : schemaVersion);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 을 사용할 수 없습니다", e);
        }
    }

    // ── 캐시 ────────────────────────────────────────────────────────────────

    private Snapshot snapshot() {
        return cache.get(() -> {
            try {
                final Set<User> users = userDao.findAll().stream()
                        .map(DbUserGroupProvider::toUser)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                final Set<Group> groups = groupDao.findAll().stream()
                        .map(DbUserGroupProvider::toGroup)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                return new Snapshot(users, groups);
            } catch (final SQLException e) {
                throw new AuthorizationAccessException("사용자 저장소를 조회할 수 없습니다.", e);
            }
        });
    }

    private static User toUser(final UserRecord record) {
        return new User.Builder().identifier(record.id()).identity(record.identity()).build();
    }

    private static Group toGroup(final GroupRecord record) {
        return new Group.Builder()
                .identifier(record.id())
                .name(record.name())
                .addUsers(new LinkedHashSet<>(record.userIds()))
                .build();
    }

    @Override
    public void preDestruction() {
        close();
    }

    private void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    /** 테스트에서 풀을 직접 주입하기 위한 진입점. */
    void configureForTesting(final DataSource testDataSource, final Dialect dialect,
                             final java.time.Duration cacheTtl) throws SQLException {
        this.identifier = "test";
        this.userDao = new UserDao(testDataSource);
        this.groupDao = new GroupDao(testDataSource);
        final SchemaManager schemaManager = new SchemaManager(testDataSource, dialect);
        schemaManager.ensureSchema(true);
        this.cache = new CachedSnapshot(cacheTtl);
        this.fingerprint = computeFingerprint("jdbc:test", schemaManager.readVersion());
    }

    Optional<UserRecord> findUserRecord(final String identity) throws SQLException {
        return userDao.findByIdentity(identity);
    }

    private record Snapshot(Set<User> users, Set<Group> groups) {
    }

    private record SimpleUserAndGroups(User user, Set<Group> groups) implements UserAndGroups {
        @Override
        public User getUser() {
            return user;
        }

        @Override
        public Set<Group> getGroups() {
            return groups;
        }
    }
}
