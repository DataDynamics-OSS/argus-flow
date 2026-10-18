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
package io.datadynamics.nifi.trino.processors;

import io.datadynamics.nifi.trino.api.TrinoConnectionService;
import io.datadynamics.nifi.trino.api.TrinoQueryInfo;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

/**
 * H2 에 위임하는 가짜 TrinoConnectionService. getQueryInfo 는 고정값을 돌려준다.
 */
public class H2TrinoConnectionService extends AbstractControllerService implements TrinoConnectionService {

    static final TrinoQueryInfo INFO = new TrinoQueryInfo("20261018_000000_00001_h2test", "FINISHED", 10, 100, 1, 2, 3);

    private final String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return DriverManager.getConnection(url);
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    void execute(final String sql) throws SQLException {
        try (Connection connection = getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:trino://h2-fake:8080";
    }

    @Override
    public TrinoQueryInfo getQueryInfo(final ResultSet resultSet) {
        return INFO;
    }

    @Override
    public TrinoQueryInfo getQueryInfo(final Statement statement) {
        return INFO;
    }
}
