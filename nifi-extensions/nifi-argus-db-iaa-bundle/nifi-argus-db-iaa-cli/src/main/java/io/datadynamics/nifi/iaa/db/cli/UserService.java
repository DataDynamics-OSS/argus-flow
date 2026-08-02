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

import io.datadynamics.nifi.iaa.db.Dialect;
import io.datadynamics.nifi.iaa.db.dao.GroupDao;
import io.datadynamics.nifi.iaa.db.dao.GroupRecord;
import io.datadynamics.nifi.iaa.db.dao.UserDao;
import io.datadynamics.nifi.iaa.db.dao.UserRecord;
import io.datadynamics.nifi.iaa.db.password.BcryptPasswordEncoder;
import io.datadynamics.nifi.iaa.db.password.PasswordEncoder;
import io.datadynamics.nifi.iaa.db.schema.SchemaManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * CLI 가 수행하는 사용자·그룹 조작.
 *
 * <p>비밀번호 해싱과 스키마 접근은 프로바이더와 같은 코드를 쓴다 — 갈리면 "CLI 로 만든
 * 사용자로 로그인이 안 되는" 사고가 난다.
 */
public class UserService {

    private final DataSource dataSource;
    private final UserDao userDao;
    private final GroupDao groupDao;
    private final PasswordEncoder passwordEncoder;
    private final Dialect dialect;

    public UserService(final DataSource dataSource, final Dialect dialect) {
        this(dataSource, dialect, new BcryptPasswordEncoder());
    }

    UserService(final DataSource dataSource, final Dialect dialect, final PasswordEncoder encoder) {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.userDao = new UserDao(dataSource);
        this.groupDao = new GroupDao(dataSource);
        this.passwordEncoder = encoder;
    }

    /** 스키마를 적용한다. DDL 이 멱등이라 이미 적용된 상태에서도 안전하다. */
    public void initSchema() throws SQLException {
        new SchemaManager(dataSource, dialect).applyBaseline();
    }

    public List<UserRecord> listUsers() throws SQLException {
        return userDao.findAll();
    }

    public UserRecord requireUser(final String identity) throws SQLException {
        return userDao.findByIdentity(identity)
                .orElseThrow(() -> new CliException("사용자를 찾을 수 없습니다: " + identity));
    }

    public List<GroupRecord> listGroups() throws SQLException {
        return groupDao.findAll();
    }

    public List<GroupRecord> groupsOf(final String userId) throws SQLException {
        return groupDao.findByUserId(userId);
    }

    /**
     * 사용자를 만든다.
     *
     * @param password {@code null} 이면 비밀번호 없이 만든다 — 인증서·OIDC 로 인증하고
     *                 인가만 DB 로 관리하는 사용자
     */
    public void addUser(final String identity, final char[] password, final boolean enabled)
            throws SQLException {
        if (userDao.findByIdentity(identity).isPresent()) {
            throw new CliException("이미 존재하는 사용자입니다: " + identity);
        }
        final String hash = password == null ? null : passwordEncoder.encode(password);
        userDao.insert(UUID.randomUUID().toString(), identity, hash, enabled);
    }

    public void setPassword(final String identity, final char[] password) throws SQLException {
        final UserRecord user = requireUser(identity);
        updatePasswordHash(user.id(), passwordEncoder.encode(password));
    }

    public void rename(final String identity, final String newIdentity) throws SQLException {
        final UserRecord user = requireUser(identity);
        if (userDao.findByIdentity(newIdentity).isPresent()) {
            throw new CliException("이미 존재하는 사용자입니다: " + newIdentity);
        }
        userDao.updateIdentity(user.id(), newIdentity);
    }

    public void setEnabled(final String identity, final boolean enabled) throws SQLException {
        final UserRecord user = requireUser(identity);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "UPDATE argus_user SET enabled = ?, updated_at = ? WHERE id = ?")) {
            ps.setBoolean(1, enabled);
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.setString(3, user.id());
            ps.executeUpdate();
        }
    }

    public void deleteUser(final String identity) throws SQLException {
        final UserRecord user = requireUser(identity);
        userDao.delete(user.id());
    }

    /** 실패 횟수와 잠금을 해제한다. */
    public void unlock(final String identity) throws SQLException {
        final UserRecord user = requireUser(identity);
        userDao.recordSuccess(user.id());
    }

    public void addGroup(final String name) throws SQLException {
        if (findGroupByName(name).isPresent()) {
            throw new CliException("이미 존재하는 그룹입니다: " + name);
        }
        groupDao.insert(new GroupRecord(UUID.randomUUID().toString(), name, Set.of()));
    }

    public void deleteGroup(final String name) throws SQLException {
        groupDao.delete(requireGroup(name).id());
    }

    /** 그룹 소속을 추가·제거한다. */
    public void setMembership(final String groupName, final String identity, final boolean member)
            throws SQLException {
        final GroupRecord group = requireGroup(groupName);
        final UserRecord user = requireUser(identity);
        final Set<String> members = new LinkedHashSet<>(group.userIds());
        final boolean changed = member ? members.add(user.id()) : members.remove(user.id());
        if (!changed) {
            return;
        }
        groupDao.update(new GroupRecord(group.id(), group.name(), members));
    }

    public GroupRecord requireGroup(final String name) throws SQLException {
        return findGroupByName(name)
                .orElseThrow(() -> new CliException("그룹을 찾을 수 없습니다: " + name));
    }

    private Optional<GroupRecord> findGroupByName(final String name) throws SQLException {
        return groupDao.findAll().stream().filter(g -> g.name().equals(name)).findFirst();
    }

    private void updatePasswordHash(final String userId, final String hash) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "UPDATE argus_user SET password_hash = ?, failed_count = 0, "
                             + "locked_until = NULL, updated_at = ? WHERE id = ?")) {
            ps.setString(1, hash);
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.setString(3, userId);
            ps.executeUpdate();
        }
    }
}
