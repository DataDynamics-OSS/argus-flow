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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive-services-api/src/main/java/org/apache/nifi/dbcp/hive/HiveDBCPService.java
 */
package io.datadynamics.nifi.services.api.hive;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.dbcp.DBCPService;

/**
 * 데이터베이스 커넥션 풀링 서비스에 대한 정의.
 * Apache Hive에 연결하기 위한 커넥션 풀을 제공하는 컨트롤러 서비스가 구현해야 하는 공통 계약을 정의한다.
 */
@Tags({"cloudera", "hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Apache Hive를 위한 데이터베이스 커넥션 풀링 서비스를 제공한다. 풀에서 커넥션을 요청하고 사용 후 반환할 수 있다.")
public interface HiveDBCPService extends DBCPService {
    /**
     * 이 컨트롤러 서비스가 연결하는 Hive의 JDBC 커넥션 URL을 반환한다.
     */
    String getConnectionURL();
}