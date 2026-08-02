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
package io.datadynamics.nifi.iaa.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * 커넥션 풀 생성.
 *
 * <p>NiFi 의 DBCP 컨트롤러 서비스를 쓸 수 없다 — 컨트롤러 서비스는 플로우 계층 자원이고
 * 인증 프로바이더는 그보다 훨씬 먼저 초기화된다. 따라서 프로바이더가 자체 풀을 갖는다.
 *
 * <p>PostgreSQL 드라이버는 번들되어 있다. MariaDB 드라이버는 LGPL-2.1 이라 배포하지 않으므로
 * {@code Database Driver Location} 으로 경로를 받아 별도 클래스로더에서 로드한다.
 */
public final class DataSourceFactory {

    public static final String PROP_URL = "Database URL";
    public static final String PROP_DRIVER_CLASS = "Database Driver Class Name";
    public static final String PROP_DRIVER_LOCATION = "Database Driver Location";
    public static final String PROP_USER = "Database User";
    public static final String PROP_PASSWORD = "Database Password";
    public static final String PROP_MAX_CONNECTIONS = "Max Connections";

    private DataSourceFactory() {
    }

    /**
     * 설정으로부터 풀을 만든다.
     *
     * @param poolName 로그·JMX 에 나타나는 이름. 프로바이더 identifier 를 쓴다
     */
    public static HikariDataSource create(final ProviderConfig config, final String poolName) {
        final String url = config.getRequired(PROP_URL);
        final String driverClass = config.getString(PROP_DRIVER_CLASS, null);
        final String driverLocation = config.getString(PROP_DRIVER_LOCATION, null);

        final HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("argus-iaa-" + poolName);
        hikari.setJdbcUrl(url);

        final String user = config.getString(PROP_USER, null);
        if (user != null) {
            hikari.setUsername(user);
        }
        final String password = config.getString(PROP_PASSWORD, null);
        if (password != null) {
            hikari.setPassword(password);
        }

        // 인증 경로는 짧고 드물다. 풀을 크게 잡을 이유가 없고, 오히려 DB 쪽 커넥션을
        // 낭비한다. 기본 4.
        hikari.setMaximumPoolSize(config.getInt(PROP_MAX_CONNECTIONS, 4));
        hikari.setMinimumIdle(1);
        // 로그인 요청이 DB 장애로 무한 대기하지 않도록 짧게 끊는다.
        hikari.setConnectionTimeout(10_000L);
        hikari.setInitializationFailTimeout(-1);   // 기동 시 DB 미가용이어도 NiFi 는 뜨게 한다
        hikari.setReadOnly(false);

        if (driverLocation != null) {
            hikari.setDataSourceProperties(new Properties());
            hikari.setDriverClassName(resolveExternalDriver(driverClass, driverLocation));
        } else if (driverClass != null) {
            hikari.setDriverClassName(driverClass);
        }
        return new HikariDataSource(hikari);
    }

    /**
     * 번들되지 않은 드라이버를 지정 경로에서 로드한다.
     *
     * <p>Hikari 는 드라이버 클래스명을 현재 스레드 컨텍스트 클래스로더로 찾으므로, 로드한
     * 클래스로더를 컨텍스트에 설정한 뒤 클래스명을 그대로 돌려준다.
     *
     * @return 사용할 드라이버 클래스명
     */
    private static String resolveExternalDriver(final String driverClass, final String location) {
        final File file = new File(location);
        if (!file.exists()) {
            throw new IllegalArgumentException(
                    PROP_DRIVER_LOCATION + " 경로에 파일이 없습니다: " + location);
        }
        try {
            final URL[] urls = {file.toURI().toURL()};
            final URLClassLoader loader =
                    new URLClassLoader(urls, DataSourceFactory.class.getClassLoader());
            if (driverClass != null) {
                // 존재 확인. 잘못된 클래스명을 커넥션 시점이 아니라 기동 시점에 잡는다.
                Class.forName(driverClass, true, loader);
                Thread.currentThread().setContextClassLoader(loader);
                return driverClass;
            }
            // 클래스명이 없으면 jar 의 ServiceLoader 등록을 따른다.
            final Driver driver = ServiceLoader.load(Driver.class, loader).findFirst().orElseThrow(
                    () -> new IllegalArgumentException(
                            "드라이버 jar 에서 java.sql.Driver 를 찾을 수 없습니다: " + location));
            Thread.currentThread().setContextClassLoader(loader);
            return driver.getClass().getName();
        } catch (final MalformedURLException e) {
            throw new IllegalArgumentException("드라이버 경로가 올바르지 않습니다: " + location, e);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "드라이버 클래스를 찾을 수 없습니다: " + driverClass + " (" + location + ")", e);
        }
    }
}
