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
package io.datadynamics.nifi.trino.api;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.dbcp.DBCPService;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Trino JDBC 연결을 제공하는 컨트롤러 서비스 계약.
 * <p>
 * {@link DBCPService}를 확장하므로 {@code ExecuteSQL}·{@code PutDatabaseRecord} 같은 범용 SQL 프로세서에도 연결할 수 있다.
 * Trino 전용 기능(query id·실행 통계)은 이 인터페이스의 메서드로 노출해, 프로세서가 trino-jdbc 에 컴파일 의존하지 않게 한다.
 */
@Tags({"trino", "jdbc", "database", "connection", "sql"})
@CapabilityDescription("Trino JDBC 연결을 제공하는 서비스. 연결마다 query id·실행 통계를 조회할 수 있다.")
public interface TrinoConnectionService extends DBCPService {

    /**
     * @return 표시·provenance 용 JDBC URL. 자격 증명은 포함하지 않는다.
     */
    String getJdbcUrl();

    /**
     * 결과 집합의 query id·통계를 돌려준다.
     *
     * @param resultSet 이 서비스가 만든 연결에서 얻은 결과 집합
     * @return Trino 결과가 아니면 {@code null}
     */
    TrinoQueryInfo getQueryInfo(ResultSet resultSet);

    /**
     * 문장의 마지막 실행에 대한 query id·통계를 돌려준다(업데이트 문장용).
     *
     * @param statement 이 서비스가 만든 연결에서 얻은 문장
     * @return Trino 문장이 아니거나 아직 실행되지 않았으면 {@code null}
     */
    TrinoQueryInfo getQueryInfo(Statement statement);
}
