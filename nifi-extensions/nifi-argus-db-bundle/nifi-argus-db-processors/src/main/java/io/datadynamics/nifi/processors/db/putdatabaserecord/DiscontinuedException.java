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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-put-pattern/src/main/java/org/apache/nifi/processor/util/pattern/DiscontinuedException.java
 */
package io.datadynamics.nifi.processors.db.putdatabaserecord;

/**
 * 반복 처리 중이던 작업이 중단되었음을 나타내는 예외.
 * 이 예외가 발생하면 호출한 쪽에서는 남은 입력들에 대한 처리를 즉시 멈춰야 한다.
 * (예: RollbackOnFailure 정책에 의해 더 이상 진행하면 안 되는 경우)
 */
public class DiscontinuedException extends RuntimeException {
    public DiscontinuedException(String message) {
        super(message);
    }

    public DiscontinuedException(String message, Throwable cause) {
        super(message, cause);
    }
}