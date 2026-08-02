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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;

/**
 * {@code argus_group} 과 {@code argus_user_group} 접근.
 *
 * <p>그룹은 소속 사용자 집합을 함께 다뤄야 하므로(NiFi 의 {@code Group} 이 users 를 포함한다)
 * 조회는 두 테이블을 조인하고, 갱신은 소속을 지웠다 다시 넣는다.
 */
public class GroupDao {

    private static final String SELECT_GROUPS = """
            SELECT g.id AS gid, g.name AS gname, ug.user_id AS uid
              FROM argus_group g
              LEFT JOIN argus_user_group ug ON ug.group_id = g.id""";

    private static final String SELECT_GROUP_BY_ID = SELECT_GROUPS + "\n WHERE g.id = ?";

    private static final String SELECT_GROUPS_FOR_USER = """
            SELECT g.id AS gid, g.name AS gname, ug2.user_id AS uid
              FROM argus_group g
              JOIN argus_user_group ug ON ug.group_id = g.id AND ug.user_id = ?
              LEFT JOIN argus_user_group ug2 ON ug2.group_id = g.id""";

    private static final String INSERT_GROUP =
            "INSERT INTO argus_group (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)";
    private static final String UPDATE_GROUP =
            "UPDATE argus_group SET name = ?, updated_at = ? WHERE id = ?";
    private static final String DELETE_GROUP = "DELETE FROM argus_group WHERE id = ?";
    private static final String DELETE_MEMBERS = "DELETE FROM argus_user_group WHERE group_id = ?";
    private static final String INSERT_MEMBER =
            "INSERT INTO argus_user_group (user_id, group_id) VALUES (?, ?)";

    private final DataSource dataSource;

    public GroupDao(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 전체 그룹과 소속. */
    public List<GroupRecord> findAll() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_GROUPS);
             ResultSet rs = statement.executeQuery()) {
            return collect(rs);
        }
    }

    public Optional<GroupRecord> findById(final String id) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_GROUP_BY_ID)) {
            statement.setString(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                final List<GroupRecord> groups = collect(rs);
                return groups.isEmpty() ? Optional.empty() : Optional.of(groups.get(0));
            }
        }
    }

    /** 지정 사용자가 속한 그룹들. 각 그룹의 전체 소속도 함께 채운다. */
    public List<GroupRecord> findByUserId(final String userId) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_GROUPS_FOR_USER)) {
            statement.setString(1, userId);
            try (ResultSet rs = statement.executeQuery()) {
                return collect(rs);
            }
        }
    }

    /** 그룹과 소속을 함께 만든다. */
    public void insert(final GroupRecord group) throws SQLException {
        final Timestamp now = Timestamp.from(Instant.now());
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement ps = connection.prepareStatement(INSERT_GROUP)) {
                    ps.setString(1, group.id());
                    ps.setString(2, group.name());
                    ps.setTimestamp(3, now);
                    ps.setTimestamp(4, now);
                    ps.executeUpdate();
                }
                replaceMembers(connection, group.id(), group.userIds());
                connection.commit();
            } catch (final SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    /**
     * 이름과 소속을 갱신한다. 소속은 전량 교체한다 — NiFi 가 넘겨주는 {@code Group} 이
     * 부분 변경이 아니라 최종 상태이기 때문이다.
     *
     * @return 대상 그룹이 있었으면 true
     */
    public boolean update(final GroupRecord group) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                final int updated;
                try (PreparedStatement ps = connection.prepareStatement(UPDATE_GROUP)) {
                    ps.setString(1, group.name());
                    ps.setTimestamp(2, Timestamp.from(Instant.now()));
                    ps.setString(3, group.id());
                    updated = ps.executeUpdate();
                }
                if (updated == 0) {
                    connection.rollback();
                    return false;
                }
                replaceMembers(connection, group.id(), group.userIds());
                connection.commit();
                return true;
            } catch (final SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    /** 삭제. 소속은 FK 의 ON DELETE CASCADE 로 함께 지워진다. */
    public boolean delete(final String id) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_GROUP)) {
            statement.setString(1, id);
            return statement.executeUpdate() > 0;
        }
    }

    private void replaceMembers(final Connection connection, final String groupId,
                                final Set<String> userIds) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(DELETE_MEMBERS)) {
            ps.setString(1, groupId);
            ps.executeUpdate();
        }
        if (userIds == null || userIds.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = connection.prepareStatement(INSERT_MEMBER)) {
            for (final String userId : userIds) {
                ps.setString(1, userId);
                ps.setString(2, groupId);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** 조인 결과(그룹당 여러 행)를 그룹 단위로 접는다. */
    private static List<GroupRecord> collect(final ResultSet rs) throws SQLException {
        final Map<String, String> names = new LinkedHashMap<>();
        final Map<String, Set<String>> members = new LinkedHashMap<>();
        while (rs.next()) {
            final String gid = rs.getString("gid");
            names.putIfAbsent(gid, rs.getString("gname"));
            final String uid = rs.getString("uid");
            final Set<String> set = members.computeIfAbsent(gid, k -> new LinkedHashSet<>());
            if (uid != null) {
                set.add(uid);
            }
        }
        final List<GroupRecord> groups = new ArrayList<>(names.size());
        names.forEach((gid, name) -> groups.add(new GroupRecord(gid, name, members.get(gid))));
        return groups;
    }
}
