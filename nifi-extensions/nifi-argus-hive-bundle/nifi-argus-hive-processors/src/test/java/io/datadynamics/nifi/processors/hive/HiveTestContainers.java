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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;

/**
 * 통합 테스트 컨테이너 정의.
 */
final class HiveTestContainers {

    /** 컨테이너 env 에 기록한 호스트 웨어하우스 경로(컨테이너 안에서도 같은 경로). */
    static final String WAREHOUSE_ENV = "IT_WAREHOUSE";

    private HiveTestContainers() {
    }

    /**
     * 호스트 웨어하우스 디렉토리를 만든다(target 아래). 컨테이너 사용자(hive, uid 1000)가 쓸 수 있게 777 로 연다.
     * 컨테이너에는 같은 절대 경로로 바인드 마운트하므로 DESCRIBE FORMATTED 의 Location(file:/...)을 호스트에서 그대로 읽을 수 있다.
     */
    static Path warehouse(final String label) {
        try {
            final Path dir = Path.of(System.getProperty("hive.it.warehouse.root", "target/hive-it-warehouse")).toAbsolutePath()
                    .resolve(label + "-" + System.nanoTime());
            Files.createDirectories(dir);
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));
            return dir;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Path warehouseOf(final GenericContainer<?> container) {
        return Path.of(container.getEnvMap().get(WAREHOUSE_ENV));
    }

    /**
     * apache/hive:3.1.x — 독립 Metastore(compactor Initiator/Worker/Cleaner) + HiveServer2 를 한 컨테이너에서 띄운다
     * (hive-it/hive3/start-hive.sh 참조).
     *
     * @param metastoreWorkers Metastore 의 Worker 스레드 수. 0 이면 요청이 'initiated' 에 머문다.
     */
    @SuppressWarnings("resource")
    static GenericContainer<?> hive3(final int metastoreWorkers) {
        final Path warehouse = warehouse("hive3-w" + metastoreWorkers);
        return new GenericContainer<>(DockerImageName.parse(System.getProperty("hive.it.hive3.image", "apache/hive:3.1.3")))
                .withExposedPorts(10000)
                .withEnv("METASTORE_WORKERS", Integer.toString(metastoreWorkers))
                .withEnv("WAREHOUSE_DIR", warehouse.toString())
                .withEnv(WAREHOUSE_ENV, warehouse.toString())
                .withFileSystemBind(warehouse.toString(), warehouse.toString())
                .withCopyFileToContainer(MountableFile.forClasspathResource("hive-it/hive3/start-hive.sh", 0755), "/it/start-hive.sh")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/it/start-hive.sh"))
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }

    /**
     * apache/hive:4.0.x — 기본 엔트리포인트(SERVICE_NAME=hiveserver2, 내장 Metastore) + hive-it/hive4/hive-site.xml.
     * HiveServer2 가 Worker 를 띄우지만 내장 Metastore 는 Cleaner 를 띄우지 않으므로 'ready for cleaning' 까지만 간다.
     *
     * @param queryBased hive.compactor.crud.query.based (REBALANCE 에 필요)
     */
    @SuppressWarnings("resource")
    static GenericContainer<?> hive4(final boolean queryBased) {
        final Path warehouse = warehouse("hive4");
        final Path site;
        try (InputStream in = HiveTestContainers.class.getClassLoader().getResourceAsStream("hive-it/hive4/hive-site.xml")) {
            final String template = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            site = Files.createDirectories(warehouse.resolveSibling(warehouse.getFileName() + "-conf")).resolve("hive-site.xml");
            Files.writeString(site, template.replace("@WAREHOUSE_DIR@", warehouse.toString()).replace("@QUERY_BASED@", Boolean.toString(queryBased)));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return new GenericContainer<>(DockerImageName.parse(System.getProperty("hive.it.hive4.image", "apache/hive:4.0.1")))
                .withExposedPorts(10000)
                .withEnv("SERVICE_NAME", "hiveserver2")
                .withEnv("HIVE_CUSTOM_CONF_DIR", "/it/conf")
                .withEnv(WAREHOUSE_ENV, warehouse.toString())
                .withFileSystemBind(warehouse.toString(), warehouse.toString())
                .withCopyFileToContainer(MountableFile.forHostPath(site, 0644), "/it/conf/hive-site.xml")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }
}
