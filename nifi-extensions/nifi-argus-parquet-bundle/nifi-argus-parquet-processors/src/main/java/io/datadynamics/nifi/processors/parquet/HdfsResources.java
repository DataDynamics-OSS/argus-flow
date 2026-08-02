/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-extension-utils/nifi-hadoop-utils/src/main/java/org/apache/nifi/processors/hadoop/HdfsResources.java
 */
package io.datadynamics.nifi.processors.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * HDFS 접근에 필요한 Hadoop Configuration과 FileSystem 인스턴스를 함께 보관하는 불변 홀더 클래스.
 * 프로세서가 매 실행마다 Configuration/FileSystem을 새로 생성하지 않고 재사용할 수 있도록
 * 두 리소스를 하나의 단위로 묶어 캐싱/전달하기 위한 용도로 사용된다.
 */
public class HdfsResources {
    // HDFS 연결에 사용되는 Hadoop 설정 객체
    private final Configuration configuration;
    // 위 설정으로 생성된 HDFS FileSystem 인스턴스
    private final FileSystem fileSystem;

    public HdfsResources(Configuration configuration, FileSystem fileSystem) {
        this.configuration = configuration;
        this.fileSystem = fileSystem;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

}
