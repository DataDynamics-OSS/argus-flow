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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/hive/streaming/RecordsEOFException.java
 */
/*
 * 참고: 이 클래스는 (io.datadynamics.* 가 아니라) 의도적으로 org.apache.hive.streaming 패키지에 위치한다.
 * Hive Streaming 라이브러리(hive-streaming 3.1.3)의 패키지 프라이빗(package-private) 멤버,
 * 특히 org.apache.hive.streaming 패키지 내부에서만 접근 가능한 패키지 프라이빗 생성자
 * SerializationError(String, Exception)에 의존하기 때문이다.
 */
package org.apache.hive.streaming;

/**
 * HiveRecordWriter가 입력 스트림에 더 이상 레코드가 없음을 알리기 위해 사용하는 "마커 클래스(marker class)"이다.
 * PutHive3Streaming은 이 예외를 통해 모든 레코드가 트랜잭션(들)에 정상적으로 기록되었음을 판단한다.
 */
public class RecordsEOFException extends SerializationError {

    RecordsEOFException(String msg, Exception e) {
        super(msg, e);
    }
}
