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
package io.datadynamics.nifi.iaa.db.schema;

import io.datadynamics.nifi.iaa.db.Dialect;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 스키마 확인과 적용.
 *
 * <p>적용은 기본적으로 하지 않는다. 인증 계통이 기동 시 DDL 을 실행하는 것은 운영에서
 * 거부되는 경우가 많고, 실패하면 NiFi 가 아예 뜨지 않는다. 관리자가 CLI 의 schema-init
 * 이나 DBA 가 배포본의 sql/ 로 미리 적용하는 것을 기본 경로로 둔다.
 */
public class SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    /** 이 릴리스가 요구하는 스키마 버전. */
    public static final int REQUIRED_VERSION = 1;

    private final DataSource dataSource;
    private final Dialect dialect;

    public SchemaManager(final DataSource dataSource, final Dialect dialect) {
        this.dataSource = dataSource;
        this.dialect = dialect;
    }

    /**
     * 스키마가 준비돼 있는지 확인하고, 아니면 {@code autoCreate} 에 따라 적용하거나 실패한다.
     *
     * @throws SQLException 스키마가 없는데 autoCreate 가 꺼져 있거나, 적용에 실패한 경우
     */
    public void ensureSchema(final boolean autoCreate) throws SQLException {
        final Integer current = readVersion();
        if (current != null && current >= REQUIRED_VERSION) {
            logger.debug("스키마 버전 {} 확인", current);
            return;
        }
        if (!autoCreate) {
            throw new SQLException(String.format(
                    "스키마가 준비되지 않았습니다(현재 버전: %s, 필요: %d). "
                            + "bin/argus-user.sh schema-init 을 실행하거나 배포본의 sql/db-iaa/ 를 "
                            + "DBA 가 적용한 뒤 다시 시작하십시오. 자동 적용을 원하면 "
                            + "'Auto Create Schema' 를 true 로 설정하십시오.",
                    current == null ? "없음" : current, REQUIRED_VERSION));
        }
        applyBaseline();
    }

    /** baseline DDL 을 적용한다. DDL 자체가 멱등이라 이미 적용된 상태에서도 안전하다. */
    public void applyBaseline() throws SQLException {
        final String ddl = readResource(dialect.getBaselineResource());
        logger.info("스키마 적용: {}", dialect.getBaselineResource());
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        }
    }

    /**
     * 적용된 스키마 버전. 테이블 자체가 없으면 {@code null}.
     *
     * <p>테이블 부재를 방언별 오류 코드로 판별하지 않고 예외를 그대로 "없음"으로 해석한다 —
     * 이 시점에 다른 SQL 오류가 났다면 뒤이은 ensureSchema 가 어차피 실패시킨다.
     */
    public Integer readVersion() {
        final String sql = "SELECT MAX(version) FROM argus_schema_version";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
                final int version = rs.getInt(1);
                return rs.wasNull() ? null : version;
            }
            return null;
        } catch (final SQLException e) {
            logger.debug("스키마 버전을 읽을 수 없습니다(테이블 미생성으로 간주): {}", e.getMessage());
            return null;
        }
    }

    private String readResource(final String resource) throws SQLException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new SQLException("스키마 리소스를 찾을 수 없습니다: " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new SQLException("스키마 리소스를 읽을 수 없습니다: " + resource, e);
        }
    }
}
