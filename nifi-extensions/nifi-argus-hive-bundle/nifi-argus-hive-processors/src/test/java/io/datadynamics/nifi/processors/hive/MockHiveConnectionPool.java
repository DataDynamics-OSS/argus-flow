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
package io.datadynamics.nifi.processors.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import io.datadynamics.nifi.services.api.hive.VendorHiveDBCPService;

/**
 * 테스트용 Hive DBCP 커넥션 풀.
 *
 * Hive QL 프로세서는 표준 JDBC(Connection/Statement/ResultSet)만 사용하므로, 실제 Hive 대신
 * H2 인메모리 DB로 백엔드를 대체해 프로세서 로직을 검증한다. 인스턴스마다 고유 DB 이름을 받아
 * 테스트 간 격리하며, DB_CLOSE_DELAY=-1로 JVM 생존 동안 인메모리 DB를 유지한다.
 */
public class MockHiveConnectionPool extends AbstractControllerService implements VendorHiveDBCPService {

    private final String url;

    public MockHiveConnectionPool(final String dbName) {
        this.url = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return DriverManager.getConnection(url);
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public Connection getConnection(final Map<String, String> attributes) throws ProcessException {
        return getConnection();
    }

    @Override
    public String getConnectionURL() {
        return url;
    }
}
