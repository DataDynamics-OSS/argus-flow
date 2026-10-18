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

import java.time.Duration;

/**
 * 통합 테스트 컨테이너 정의.
 */
final class HiveTestContainers {

    private HiveTestContainers() {
    }

    /**
     * apache/hive:3.1.x — 독립 Metastore(compactor Initiator/Worker/Cleaner) + HiveServer2 를 한 컨테이너에서 띄운다
     * (hive-it/hive3/start-hive.sh 참조).
     *
     * @param metastoreWorkers Metastore 의 Worker 스레드 수. 0 이면 요청이 'initiated' 에 머문다.
     */
    @SuppressWarnings("resource")
    static GenericContainer<?> hive3(final int metastoreWorkers) {
        return new GenericContainer<>(DockerImageName.parse(System.getProperty("hive.it.hive3.image", "apache/hive:3.1.3")))
                .withExposedPorts(10000)
                .withEnv("METASTORE_WORKERS", Integer.toString(metastoreWorkers))
                .withCopyFileToContainer(MountableFile.forClasspathResource("hive-it/hive3/start-hive.sh", 0755), "/it/start-hive.sh")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/it/start-hive.sh"))
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }

    /**
     * apache/hive:4.0.x — 기본 엔트리포인트(SERVICE_NAME=hiveserver2, 내장 Metastore) + hive-it/hive4/hive-site.xml.
     * HiveServer2 가 Worker 를 띄우지만 내장 Metastore 는 Cleaner 를 띄우지 않으므로 'ready for cleaning' 까지만 간다.
     */
    @SuppressWarnings("resource")
    static GenericContainer<?> hive4() {
        return new GenericContainer<>(DockerImageName.parse(System.getProperty("hive.it.hive4.image", "apache/hive:4.0.1")))
                .withExposedPorts(10000)
                .withEnv("SERVICE_NAME", "hiveserver2")
                .withEnv("HIVE_CUSTOM_CONF_DIR", "/it/conf")
                .withCopyFileToContainer(MountableFile.forClasspathResource("hive-it/hive4/hive-site.xml", 0644), "/it/conf/hive-site.xml")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }
}
