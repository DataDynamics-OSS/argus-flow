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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive-services-api/src/main/java/org/apache/nifi/dbcp/hive/Hive3DBCPService.java
 */
package io.datadynamics.nifi.services.api.hive;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

/**
 * 데이터베이스 커넥션 풀링 서비스에 대한 정의.
 * Hive 3(HiveServer2 등) 대상 DBCP 컨트롤러 서비스임을 나타내는 마커 인터페이스로,
 * {@link HiveDBCPService}의 커넥션 URL 조회 계약을 그대로 상속한다.
 */
@Tags({"cloudera", "hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Apache Hive를 위한 데이터베이스 커넥션 풀링 서비스를 제공한다. 풀에서 커넥션을 요청하고 사용 후 반환할 수 있다.")
public interface Hive3DBCPService extends HiveDBCPService {
}
