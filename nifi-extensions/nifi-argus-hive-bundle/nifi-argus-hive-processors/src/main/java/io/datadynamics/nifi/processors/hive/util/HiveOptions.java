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
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/util/hive/HiveOptions.java
 */
package io.datadynamics.nifi.processors.hive.util;

import org.apache.hadoop.hive.conf.HiveConf;

import java.io.Serializable;
import java.util.List;


/**
 * Hive Streaming 연결에 필요한 옵션들을 담는 값 객체(빌더 스타일).
 * 필수 값(메타스토어 URI, 데이터베이스명, 테이블명)은 생성자에서 받고,
 * 나머지 선택적 옵션들은 with* 메서드를 체이닝하여 설정한다.
 */
public class HiveOptions implements Serializable {

    protected String databaseName;
    protected String tableName;
    protected String metaStoreURI;
    // 커넥션이 유휴 상태로 있다가 종료되기까지의 대기 시간(ms)
    protected Integer idleTimeout = 60000;
    // Hive Streaming API 호출에 대한 타임아웃(ms). 0이면 타임아웃 없음
    protected Integer callTimeout = 0;
    // 정적 파티션 사용 시 파티션 컬럼에 대응하는 값 목록
    protected List<String> staticPartitionValues = null;
    protected String kerberosPrincipal;
    protected String kerberosKeytab;
    protected HiveConf hiveConf;
    // true면 스트리밍 연결 및 트랜잭션 처리에 대한 최적화(예: 지연 초기화)를 적용한다
    protected boolean streamingOptimizations = true;
    // 하나의 트랜잭션 배치에 포함할 트랜잭션 개수
    protected int transactionBatchSize = 1;

    public HiveOptions(String metaStoreURI, String databaseName, String tableName) {
        this.metaStoreURI = metaStoreURI;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public HiveOptions withCallTimeout(Integer callTimeout) {
        this.callTimeout = callTimeout;
        return this;
    }

    public HiveOptions withStaticPartitionValues(List<String> staticPartitionValues) {
        this.staticPartitionValues = staticPartitionValues;
        return this;
    }

    public HiveOptions withKerberosKeytab(String kerberosKeytab) {
        this.kerberosKeytab = kerberosKeytab;
        return this;
    }

    public HiveOptions withKerberosPrincipal(String kerberosPrincipal) {
        this.kerberosPrincipal = kerberosPrincipal;
        return this;
    }

    public HiveOptions withHiveConf(HiveConf hiveConf) {
        this.hiveConf = hiveConf;
        return this;
    }

    public HiveOptions withStreamingOptimizations(boolean streamingOptimizations) {
        this.streamingOptimizations = streamingOptimizations;
        return this;
    }

    public HiveOptions withTransactionBatchSize(int transactionBatchSize) {
        this.transactionBatchSize = transactionBatchSize;
        return this;
    }

    public String getMetaStoreURI() {
        return metaStoreURI;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getQualifiedTableName() {
        return databaseName + "." + tableName;
    }

    public List<String> getStaticPartitionValues() {
        return staticPartitionValues;
    }

    public Integer getCallTimeOut() {
        return callTimeout;
    }

    public Integer getIdleTimeout() {
        return idleTimeout;
    }

    public HiveConf getHiveConf() {
        return hiveConf;
    }

    public boolean getStreamingOptimizations() {
        return streamingOptimizations;
    }

    public int getTransactionBatchSize() {
        return transactionBatchSize;
    }
}
