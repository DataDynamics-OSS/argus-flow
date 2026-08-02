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
package io.datadynamics.nifi.processors.db;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 테스트에서 사용하는 H2 기반의 간단한 DBCPService 구현체.
 * 실제 데이터베이스 대신 H2 인메모리 데이터베이스에 연결하여 프로세서 테스트에 필요한 Connection을 제공한다.
 */
public class H2DBCPService extends AbstractControllerService implements DBCPService {

    private final String jdbcUrl;

    // dbName을 기반으로 H2 인메모리 데이터베이스의 JDBC URL을 생성한다. DB_CLOSE_DELAY=-1 옵션으로
    // 마지막 연결이 종료되어도 데이터베이스가 즉시 사라지지 않도록 한다.
    public H2DBCPService(final String dbName) {
        this.jdbcUrl = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";
    }

    @Override
    public String getIdentifier() {
        return "dbcp";
    }

    // H2 인메모리 데이터베이스에 대한 JDBC Connection을 반환한다. 연결 실패 시 ProcessException으로 감싸서 던진다.
    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }
}
