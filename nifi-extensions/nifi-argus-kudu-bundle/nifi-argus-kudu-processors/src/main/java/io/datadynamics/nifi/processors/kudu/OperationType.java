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
 *   nifi-nar-bundles/nifi-kudu-bundle/nifi-kudu-processors/src/main/java/org/apache/nifi/processors/kudu/OperationType.java
 */
package io.datadynamics.nifi.processors.kudu;

/**
 * PutKudu 프로세서가 지원하는 Kudu Operation의 유형을 정의합니다.
 * 각 레코드를 Kudu 테이블에 반영할때 이 유형에 따라서 실제 Kudu Operation(Insert, Upsert, Update, Delete 등)이 생성됩니다.
 */
public enum OperationType {
    INSERT,
    INSERT_IGNORE, // 구버전의 Kudu는 Insert Ignore를 지원하지 않습니다.
    UPSERT,
    UPDATE,
    DELETE,
    UPDATE_IGNORE,
    DELETE_IGNORE
}