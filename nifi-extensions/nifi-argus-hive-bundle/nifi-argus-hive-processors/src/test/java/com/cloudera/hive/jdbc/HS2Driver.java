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
package com.cloudera.hive.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * 테스트 전용 가짜(fake) 벤더 Hive JDBC 드라이버.
 *
 * VendorHive3ConnectionPool은 드라이버 클래스명 {@code com.cloudera.hive.jdbc.HS2Driver}를 하드코딩하고
 * 라이선스상 실제 드라이버는 클래스패스에 없다. 같은 FQCN의 테스트 클래스를 두어 풀 로직(프로퍼티 반영,
 * 풀 크기/대기, 검증 쿼리, 비활성화 시 close)을 실제 Hive 없이 검증한다.
 * {@code jdbc:hive2://} URL만 수락하며, 실제 연결은 H2 인메모리 DB로 위임한다.
 * 마지막으로 전달된 URL/Properties(사용자/비밀번호)를 기록해 프로퍼티 조립을 검증할 수 있게 한다.
 */
public class HS2Driver implements Driver {

    public static final String URL_PREFIX = "jdbc:hive2://";

    public static final AtomicInteger CONNECT_COUNT = new AtomicInteger();
    public static volatile String lastUrl;
    public static volatile Properties lastInfo;
    // connect 호출 시점의 javax.security.auth.Subject 주체 이름(없으면 "<none>")을 호출 순서대로 기록한다.
    public static final List<String> CONNECT_SUBJECTS = new CopyOnWriteArrayList<>();

    public static void reset() {
        CONNECT_COUNT.set(0);
        CONNECT_SUBJECTS.clear();
        lastUrl = null;
        lastInfo = null;
    }

    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        CONNECT_COUNT.incrementAndGet();
        final javax.security.auth.Subject subject = javax.security.auth.Subject.current();
        CONNECT_SUBJECTS.add(subject == null || subject.getPrincipals().isEmpty()
                ? "<none>" : subject.getPrincipals().iterator().next().getName());
        lastUrl = url;
        lastInfo = info == null ? new Properties() : (Properties) info.clone();
        // URL 나머지를 DB 이름으로 사용해 테스트 간 격리. 자격 증명은 H2로 전달하지 않는다.
        final String dbName = "vendorhive_" + url.substring(URL_PREFIX.length()).replaceAll("[^A-Za-z0-9]", "_");
        return new org.h2.Driver().connect("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1", new Properties());
    }

    @Override
    public boolean acceptsURL(final String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
