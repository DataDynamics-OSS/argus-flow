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
package io.datadynamics.nifi.processors.db.bulkinsert;

/**
 * 반복적으로 입력을 처리하던 프로세스가 중단(discontinue)되었음을 나타내는 예외.
 * 이 예외가 던져지면 호출한 쪽(caller)은 이후 입력에 대한 처리를 즉시 멈춰야 한다.
 * 주로 {@link RollbackOnFailure}가 활성화된 상태에서 하나의 FlowFile 처리 중
 * 롤백 불가능한 에러가 발생하여 나머지 입력을 더 이상 진행하면 안 될 때 사용된다.
 */
public class DiscontinuedException extends RuntimeException {
    public DiscontinuedException(String message) {
        super(message);
    }

    public DiscontinuedException(String message, Throwable cause) {
        super(message, cause);
    }
}