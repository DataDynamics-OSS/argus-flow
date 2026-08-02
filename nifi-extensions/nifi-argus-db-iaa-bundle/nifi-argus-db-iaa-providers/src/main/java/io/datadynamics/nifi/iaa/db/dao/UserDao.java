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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * {@code argus_user} 접근. 모든 SQL 은 {@link PreparedStatement} 를 쓴다.
 *
 * <p>잠금 상태(failed_count, locked_until)를 DB 에 두는 이유는 클러스터 전 노드가 상태를
 * 공유해야 하기 때문이다. 노드 로컬 메모리에 두면 3노드 클러스터에서 허용 시도 횟수가
 * 3배가 된다.
 */
public class UserDao {

    private static final String SELECT_BY_IDENTITY = """
            SELECT id, identity, password_hash, enabled, failed_count, locked_until
              FROM argus_user
             WHERE identity = ?""";

    private static final String RECORD_FAILURE = """
            UPDATE argus_user
               SET failed_count = failed_count + 1,
                   locked_until = ?,
                   updated_at   = ?
             WHERE id = ?""";

    private static final String RECORD_SUCCESS = """
            UPDATE argus_user
               SET failed_count = 0,
                   locked_until = NULL,
                   updated_at   = ?
             WHERE id = ?""";

    private final DataSource dataSource;

    public UserDao(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** identity 로 조회. 없으면 비어 있는 Optional. */
    public Optional<UserRecord> findByIdentity(final String identity) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_BY_IDENTITY)) {
            statement.setString(1, identity);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                final Timestamp lockedUntil = rs.getTimestamp("locked_until");
                return Optional.of(new UserRecord(
                        rs.getString("id"),
                        rs.getString("identity"),
                        rs.getString("password_hash"),
                        rs.getBoolean("enabled"),
                        rs.getInt("failed_count"),
                        lockedUntil == null ? null : lockedUntil.toInstant()));
            }
        }
    }

    /**
     * 로그인 실패를 기록한다. 실패 횟수가 {@code maxAttempts} 에 도달하면 잠근다.
     *
     * <p>증가 후 값을 기준으로 판단해야 하므로 현재 행의 failed_count 에 1 을 더한 값과
     * 비교한다. 갱신은 단일 UPDATE 로 처리해 노드 간 경쟁을 DB 에 맡긴다.
     *
     * @param currentFailedCount 조회 시점의 failed_count
     * @param lockUntil          잠금 해제 시각. 잠그지 않으려면 {@code null}
     */
    public void recordFailure(final String userId, final int currentFailedCount,
                              final int maxAttempts, final Instant lockUntil) throws SQLException {
        final boolean shouldLock = currentFailedCount + 1 >= maxAttempts;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(RECORD_FAILURE)) {
            if (shouldLock && lockUntil != null) {
                statement.setTimestamp(1, Timestamp.from(lockUntil));
            } else {
                statement.setNull(1, java.sql.Types.TIMESTAMP);
            }
            statement.setTimestamp(2, Timestamp.from(Instant.now()));
            statement.setString(3, userId);
            statement.executeUpdate();
        }
    }

    /** 로그인 성공. 실패 횟수와 잠금을 초기화한다. */
    public void recordSuccess(final String userId) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(RECORD_SUCCESS)) {
            statement.setTimestamp(1, Timestamp.from(Instant.now()));
            statement.setString(2, userId);
            statement.executeUpdate();
        }
    }

    // ── 인가(UserGroupProvider)에서 쓰는 CRUD ────────────────────────────────

    private static final String SELECT_ALL = """
            SELECT id, identity, password_hash, enabled, failed_count, locked_until
              FROM argus_user""";

    private static final String SELECT_BY_ID = SELECT_ALL + "\n WHERE id = ?";

    private static final String INSERT = """
            INSERT INTO argus_user (id, identity, password_hash, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)""";

    /** identity 만 갱신한다. 비밀번호 변경은 CLI 의 별도 경로를 쓴다. */
    private static final String UPDATE_IDENTITY = """
            UPDATE argus_user SET identity = ?, updated_at = ? WHERE id = ?""";

    private static final String DELETE = "DELETE FROM argus_user WHERE id = ?";

    /** 전체 사용자. NiFi 인가 계층이 사용자 목록을 요구할 때 쓴다. */
    public List<UserRecord> findAll() throws SQLException {
        final List<UserRecord> users = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_ALL);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                users.add(map(rs));
            }
        }
        return users;
    }

    public Optional<UserRecord> findById(final String id) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_BY_ID)) {
            statement.setString(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        }
    }

    /** 사용자를 만든다. 비밀번호 없이 만들면 비밀번호 인증 대상이 아니다. */
    public void insert(final String id, final String identity, final String passwordHash,
                       final boolean enabled) throws SQLException {
        final Timestamp now = Timestamp.from(Instant.now());
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT)) {
            statement.setString(1, id);
            statement.setString(2, identity);
            statement.setString(3, passwordHash);
            statement.setBoolean(4, enabled);
            statement.setTimestamp(5, now);
            statement.setTimestamp(6, now);
            statement.executeUpdate();
        }
    }

    /** @return 갱신된 행이 있으면 true */
    public boolean updateIdentity(final String id, final String identity) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(UPDATE_IDENTITY)) {
            statement.setString(1, identity);
            statement.setTimestamp(2, Timestamp.from(Instant.now()));
            statement.setString(3, id);
            return statement.executeUpdate() > 0;
        }
    }

    /** 삭제. 소속(argus_user_group)은 FK 의 ON DELETE CASCADE 로 함께 지워진다. */
    public boolean delete(final String id) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE)) {
            statement.setString(1, id);
            return statement.executeUpdate() > 0;
        }
    }

    private static UserRecord map(final ResultSet rs) throws SQLException {
        final Timestamp lockedUntil = rs.getTimestamp("locked_until");
        return new UserRecord(
                rs.getString("id"),
                rs.getString("identity"),
                rs.getString("password_hash"),
                rs.getBoolean("enabled"),
                rs.getInt("failed_count"),
                lockedUntil == null ? null : lockedUntil.toInstant());
    }
}
