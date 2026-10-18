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

import io.datadynamics.nifi.services.api.hive.Hive3DBCPService;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * 통합 테스트용 Hive3DBCPService. Apache {@code hive-jdbc-<버전>-standalone.jar} 를 별도 클래스로더(부모: 플랫폼 클래스로더)로
 * 읽어 번들의 hive-exec 3.1.3·Hadoop 3.5 와 섞이지 않게 한다. {@code java.sql} 인터페이스만 공유하므로 프로세서는 평소처럼
 * {@link Connection} 을 쓴다. 풀 없이 요청마다 새 커넥션을 연다.
 */
class IsolatedHiveJdbcService extends AbstractControllerService implements Hive3DBCPService {

    private final Driver driver;
    private final String url;
    /** 공유 모드면 모든 getConnection 이 이 커넥션(같은 HiveServer2 세션)을 돌려준다. close 는 무시한다. */
    private volatile Connection shared;

    IsolatedHiveJdbcService(final Driver driver, final String url) {
        this.driver = driver;
        this.url = url;
    }

    /**
     * standalone jar 에서 {@code org.apache.hive.jdbc.HiveDriver} 를 읽는다.
     */
    static Driver loadDriver(final File standaloneJar) throws Exception {
        if (!standaloneJar.isFile()) {
            throw new IllegalStateException("hive-jdbc standalone jar 가 없다(mvn generate-test-resources 필요): " + standaloneJar);
        }
        final URLClassLoader classLoader = new URLClassLoader(new URL[]{standaloneJar.toURI().toURL()}, ClassLoader.getPlatformClassLoader());
        return (Driver) Class.forName("org.apache.hive.jdbc.HiveDriver", true, classLoader).getDeclaredConstructor().newInstance();
    }

    Connection connect() throws SQLException {
        final Properties properties = new Properties();
        properties.setProperty("user", "hive");
        final Connection connection = driver.connect(url, properties);
        if (connection == null) {
            throw new SQLException("드라이버가 URL 을 받지 않았다: " + url);
        }
        return connection;
    }

    /**
     * 커넥션 풀처럼 같은 세션을 재사용하게 한다(세션 설정 복원 확인용). 돌려준 커넥션의 close 는 아무것도 하지 않는다.
     */
    Connection shareOneConnection() throws SQLException {
        final Connection real = connect();
        shared = (Connection) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (final java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        return real;
    }

    @Override
    public Connection getConnection() throws ProcessException {
        if (shared != null) {
            return shared;
        }
        try {
            return connect();
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
