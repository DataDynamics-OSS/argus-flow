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

import java.util.function.Supplier;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.util.TestRunner;

import com.cloudera.hive.jdbc.HS2Driver;

import io.datadynamics.nifi.services.dbcp.hive.Hive3ConnectionPool;
import io.datadynamics.nifi.services.dbcp.hive.VendorHive3ConnectionPool;

/**
 * Hive QL 프로세서가 받아야 하는 실제 커넥션 풀 구현체 두 가지.
 *
 * 두 풀 모두 드라이버 클래스 {@code com.cloudera.hive.jdbc.HS2Driver}를 로드하므로, 테스트 소스의 가짜 드라이버
 * ({@link HS2Driver}, H2 인메모리 위임)로 실제 풀 코드(DBCP2 BasicDataSource)를 거쳐 프로세서를 실행할 수 있다.
 * 같은 URL 접미사는 같은 H2 DB를 가리키므로, 풀에서 얻은 커넥션으로 테스트 데이터를 미리 넣을 수 있다.
 */
enum HivePoolType {
    VENDOR(VendorHive3ConnectionPool::new),
    STANDARD(Hive3ConnectionPool::new);

    // 두 풀 모두 같은 이름의 URL 프로퍼티를 쓴다(DATABASE_URL은 패키지 전용이라 이름으로 지정).
    private static final String DATABASE_URL_PROPERTY = "hive-db-connect-url";

    private final Supplier<DBCPService> factory;

    HivePoolType(final Supplier<DBCPService> factory) {
        this.factory = factory;
    }

    /**
     * 풀을 만들어 러너에 등록·활성화하고, 프로세서의 DBCP 서비스 프로퍼티에 연결한다.
     *
     * @param runner      프로세서 테스트 러너
     * @param property    프로세서의 Hive DBCP 서비스 프로퍼티
     * @param dbSuffix    H2 DB 이름에 쓰일 URL 접미사(테스트마다 고유하게)
     * @return 활성화된 풀
     */
    DBCPService addTo(final TestRunner runner, final PropertyDescriptor property, final String dbSuffix) throws Exception {
        final DBCPService pool = factory.get();
        final String id = "pool-" + name().toLowerCase();
        runner.addControllerService(id, pool);
        runner.setProperty(pool, DATABASE_URL_PROPERTY, HS2Driver.URL_PREFIX + name().toLowerCase() + "_" + dbSuffix);
        runner.enableControllerService(pool);
        runner.setProperty(property, id);
        return pool;
    }
}
