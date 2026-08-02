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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/util/hive/AuthenticationFailedException.java
 */
package io.datadynamics.nifi.processors.hive.util;

/**
 * Hive/Kerberos 인증 과정에서 실패가 발생했을 때 던지는 예외.
 * 원인 예외(cause)와 실패 사유(reason)를 함께 보관하여 상위 호출부에서 로그 및 에러 처리에 활용할 수 있도록 한다.
 */
public class AuthenticationFailedException extends Exception {
    /**
     * @param reason 인증 실패 사유를 설명하는 메시지
     * @param cause  실패를 유발한 원본 예외
     */
    public AuthenticationFailedException(String reason, Exception cause) {
        super(reason, cause);
    }
}