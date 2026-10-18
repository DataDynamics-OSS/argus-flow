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
package io.datadynamics.nifi.services.api.hive;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

/**
 * Cloudera 배포판의 Hive에 특화된 데이터베이스 커넥션 풀링 서비스 인터페이스.
 * Cloudera 환경에서 사용하는 JDBC 드라이버/URL 형식의 차이를 흡수하기 위한 서비스 타입이다.
 * <p>
 * Hive QL 프로세서(SelectHive3QL, PutHive3QL, UpdateHive3Table)는 공통 부모인 {@link Hive3DBCPService}를
 * 요구하므로, 이 인터페이스도 {@link Hive3DBCPService}를 상속해 벤더 풀({@code VendorHive3ConnectionPool})과
 * 표준 풀({@code Hive3ConnectionPool})을 모두 같은 프로세서에 연결할 수 있게 한다.
 */
@Tags({"cloudera", "hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Cloudera Hive를 위한 데이터베이스 커넥션 풀링 서비스를 제공한다. 풀에서 커넥션을 요청하고 사용 후 반환할 수 있다.")
public interface VendorHiveDBCPService extends Hive3DBCPService {

    /**
     * 이 컨트롤러 서비스가 연결하는 Hive의 JDBC 커넥션 URL을 반환한다.
     */
    @Override
    String getConnectionURL();

}
