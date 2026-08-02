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
 *   nifi-nar-bundles/nifi-parquet-bundle/nifi-parquet-processors/src/main/java/org/apache/nifi/processors/parquet/PutParquet.java
 */
package io.datadynamics.nifi.processors.parquet;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.RequiredPermission;

/**
 * upstream {@link org.apache.nifi.processors.parquet.PutParquet}의 io.datadynamics 네임스페이스 버전.
 * 레거시(NiFi 1.x) 커스텀 PutParquet은 upstream과 기능이 동일하여 fork를 유지하지 않고
 * upstream 구현을 상속하여 FQCN만 유지한다. 레거시 속성명(remove-crc-files, row-group-size 등)은
 * upstream의 migrateProperties()가 신규 속성명으로 자동 변환한다.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"custom", "put", "parquet", "hadoop", "HDFS", "filesystem", "record"})
@CapabilityDescription("제공된 Record Reader를 사용하여 수신된 FlowFile로부터 레코드를 읽고, 해당 레코드를 " +
        "Parquet 파일로 기록한다. Parquet 파일의 스키마는 프로세서 속성에 지정되어야 한다. 이 프로세서는 " +
        "먼저 임시 dot 파일(숨김 파일)을 기록하며, 모든 레코드를 dot 파일에 성공적으로 기록한 후에는 " +
        "dot 파일의 이름을 최종 파일명으로 변경한다. 이름 변경에 실패할 경우 최대 10회까지 재시도하며, " +
        "그래도 성공하지 못하면 dot 파일을 삭제하고 flow file을 failure로 라우팅한다. " +
        " 입력으로부터 레코드를 읽거나 출력에 레코드를 기록하는 도중 오류가 발생하면, " +
        "dot 파일 전체를 삭제하고 오류 유형에 따라 flow file을 failure 또는 retry로 라우팅한다.")
@ReadsAttribute(attribute = "filename", description = "기록할 파일명은 이 속성 값으로부터 가져온다.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "파일명이 이 속성에 저장된다."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "파일의 절대 경로가 이 속성에 저장된다."),
        @WritesAttribute(attribute = "hadoop.file.url", description = "파일의 hadoop url이 이 속성에 저장된다."),
        @WritesAttribute(attribute = "record.count", description = "Parquet 파일에 기록된 레코드 수")
})
@Restricted(restrictions = {
        @Restriction(
                requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
                explanation = "운영자에게 NiFi가 접근 가능한 HDFS 또는 로컬 파일시스템의 모든 파일을 기록할 수 있는 권한을 부여한다.")
})
public class PutParquet extends org.apache.nifi.processors.parquet.PutParquet {
}
