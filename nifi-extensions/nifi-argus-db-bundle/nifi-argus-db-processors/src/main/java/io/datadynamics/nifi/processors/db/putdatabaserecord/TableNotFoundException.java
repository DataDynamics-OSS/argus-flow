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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/db/TableNotFoundException.java
 */
package io.datadynamics.nifi.processors.db.putdatabaserecord;

import java.sql.SQLException;

/**
 * 대상 테이블을 찾을 수 없는 상황을 다른 일반적인 SQLException과 구분하기 위한 마커(marker) 예외 클래스.
 * 이 예외로 구분함으로써 호출부에서 "테이블 미존재"를 다른 SQL 오류와 다르게 처리(예: 자동 테이블 생성 시도)할 수 있다.
 */
public class TableNotFoundException extends SQLException {
    public TableNotFoundException(String s) {
        super(s);
    }
}