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

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.ExecConfig;
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
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * 통합 테스트 컨테이너 정의.
 */
final class HiveTestContainers {

    /** 컨테이너 env 에 기록한 호스트 웨어하우스 경로(컨테이너 안에서도 같은 경로). */
    static final String WAREHOUSE_ENV = "IT_WAREHOUSE";

    private HiveTestContainers() {
    }

    /**
     * 호스트 웨어하우스 디렉토리를 만든다. 컨테이너 사용자(hive, uid 1000)가 쓸 수 있게 777 로 연다(umask 무시).
     * 컨테이너에는 같은 절대 경로로 바인드 마운트하므로 DESCRIBE FORMATTED 의 Location(file:/...)을 호스트에서 그대로 읽을 수 있다.
     */
    static Path warehouse(final String label) {
        try {
            final Path dir = warehouseRoot().resolve(label + "-" + System.nanoTime());
            Files.createDirectories(dir);
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));
            return dir;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 웨어하우스 루트. 기본은 target/hive-it-warehouse 이지만, 절대 경로의 어느 구성 요소라도 '_' 또는 '.' 로 시작하면
     * (예: GitHub Actions 러너의 .../_work/...) java.io.tmpdir 아래로 옮긴다. Hive 4 의 ACID 읽기·compactor 는 테이블 위치의
     * 상위 경로에 숨김 이름('_x', '.x')이 있으면 delta 를 하나도 보지 못해 SELECT 가 0 행, compaction 이 'refused' 가 된다
     * (Hive 3 와 non-ACID 테이블은 영향 없음). hive.it.warehouse.root 로 직접 지정할 수 있다.
     */
    static Path warehouseRoot() {
        final String configured = System.getProperty("hive.it.warehouse.root");
        Path root = Path.of(configured == null ? "target/hive-it-warehouse" : configured).toAbsolutePath().normalize();
        if (configured == null && hasHiddenComponent(root)) {
            root = Path.of(System.getProperty("java.io.tmpdir"), "argus-hive-it-" + System.getProperty("user.name"))
                    .toAbsolutePath().normalize();
        }
        if (hasHiddenComponent(root)) {
            throw new IllegalStateException("Hive 통합 테스트 웨어하우스 경로에 '_' 또는 '.' 로 시작하는 구성 요소가 있다(Hive 4 ACID 가 "
                    + "숨김 경로로 보고 데이터를 읽지 못한다): " + root + ". -Dhive.it.warehouse.root 로 다른 경로를 지정한다");
        }
        return root;
    }

    static boolean hasHiddenComponent(final Path path) {
        for (final Path name : path) {
            final String s = name.toString();
            if (s.startsWith("_") || s.startsWith(".")) {
                return true;
            }
        }
        return false;
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
    static HiveContainer hive3(final int metastoreWorkers) {
        final Path warehouse = warehouse("hive3-w" + metastoreWorkers);
        return new HiveContainer(DockerImageName.parse(System.getProperty("hive.it.hive3.image", "apache/hive:3.1.3")), warehouse)
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
    static HiveContainer hive4(final boolean queryBased) {
        final Path warehouse = warehouse("hive4");
        final Path site;
        try (InputStream in = HiveTestContainers.class.getClassLoader().getResourceAsStream("hive-it/hive4/hive-site.xml")) {
            final String template = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            site = Files.createDirectories(warehouse.resolveSibling(warehouse.getFileName() + "-conf")).resolve("hive-site.xml");
            Files.writeString(site, template.replace("@WAREHOUSE_DIR@", warehouse.toString()).replace("@QUERY_BASED@", Boolean.toString(queryBased)));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return new HiveContainer(DockerImageName.parse(System.getProperty("hive.it.hive4.image", "apache/hive:4.0.1")), warehouse)
                .withExposedPorts(10000)
                .withEnv("SERVICE_NAME", "hiveserver2")
                .withEnv("HIVE_CUSTOM_CONF_DIR", "/it/conf")
                .withEnv(WAREHOUSE_ENV, warehouse.toString())
                .withFileSystemBind(warehouse.toString(), warehouse.toString())
                .withCopyFileToContainer(MountableFile.forHostPath(site, 0644), "/it/conf/hive-site.xml")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }

    /**
     * 웨어하우스를 바인드 마운트한 Hive 컨테이너. 컨테이너(uid 1000)가 umask 022 로 만든 파일·디렉토리는 호스트의 테스트 사용자
     * (예: 러너 계정)가 지울 수 없어 다음 빌드의 mvn clean·워크스페이스 정리가 실패하므로, 멈추기 전에 컨테이너 안에서 권한을 연다.
     * 웨어하우스가 target 밖(java.io.tmpdir)에 있으면 멈춘 뒤 지운다.
     */
    static final class HiveContainer extends GenericContainer<HiveContainer> {

        private final Path warehouse;

        HiveContainer(final DockerImageName image, final Path warehouse) {
            super(image);
            this.warehouse = warehouse;
        }

        @Override
        protected void containerIsStopping(final InspectContainerResponse containerInfo) {
            try {
                execInContainer(ExecConfig.builder().user("root").command(new String[] {"chmod", "-R", "a+rwX", warehouse.toString()}).build());
            } catch (final Exception e) {
                logger().warn("웨어하우스 권한 변경 실패: {}", warehouse, e);
            }
        }

        @Override
        protected void containerIsStopped(final InspectContainerResponse containerInfo) {
            if (System.getProperty("hive.it.warehouse.root") == null && !warehouse.startsWith(Path.of("target").toAbsolutePath().normalize())) {
                deleteQuietly(warehouse);
                deleteQuietly(warehouse.resolveSibling(warehouse.getFileName() + "-conf"));
            }
        }

        private void deleteQuietly(final Path dir) {
            if (!Files.exists(dir)) {
                return;
            }
            try (Stream<Path> paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            } catch (final IOException | UncheckedIOException e) {
                logger().warn("웨어하우스 삭제 실패: {}", dir, e);
            }
        }
    }
}
