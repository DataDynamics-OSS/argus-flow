#!/bin/bash
#
# Copyright 2026 Data Dynamics Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CompactHive3Table 통합 테스트용 apache/hive:3.1.x 엔트리포인트.
# 한 컨테이너에서 독립 Metastore(내장 Derby, compactor Initiator/Worker/Cleaner)와 HiveServer2(원격 Metastore 사용)를 띄운다.
# Hive 3.1 의 HiveServer2 는 내장 Metastore 로 띄우면 compactor 스레드를 하나도 띄우지 않아 요청이 'initiated' 에 머문다.
# (Hive 4 는 HiveServer2 가 compaction 큐를 Derby 에 직접 쓰므로 이 방식을 쓸 수 없고, 기본 엔트리포인트 + hive-site 로 띄운다.)
#   COMPACTOR_ON        : Initiator/Cleaner 실행 여부 (기본 true)
#   METASTORE_WORKERS   : Metastore 프로세스의 Worker 스레드 수 (0 이면 요청이 'initiated' 에 머문다)
#   HS2_WORKERS         : HiveServer2 프로세스의 Worker 스레드 수 (기본 0)
#   WAREHOUSE_DIR       : 웨어하우스 경로(기본 이미지 값 /opt/hive/data/warehouse). 테스트는 호스트 디렉토리를 같은 경로로 바인드 마운트한다.
export HIVE_CONF_DIR=$HIVE_HOME/conf
$HIVE_HOME/bin/schematool -dbType derby -initSchema > /tmp/schematool.log 2>&1 || { cat /tmp/schematool.log; exit 1; }
WAREHOUSE_OPTS=""
if [ -n "${WAREHOUSE_DIR:-}" ]; then
  WAREHOUSE_OPTS="-Dhive.metastore.warehouse.dir=$WAREHOUSE_DIR -Dmetastore.warehouse.dir=$WAREHOUSE_DIR"
fi
COMMON="$WAREHOUSE_OPTS -Dhive.support.concurrency=true -Dhive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager -Dhive.compactor.check.interval=5s -Dmetastore.compactor.check.interval=5s -Dhive.compactor.cleaner.run.interval=5s -Dmetastore.compactor.cleaner.run.interval=5s"
ON=${COMPACTOR_ON:-true}
MW=${METASTORE_WORKERS:-0}
HW=${HS2_WORKERS:-0}
HADOOP_CLIENT_OPTS="-Xmx768m $COMMON -Dhive.compactor.initiator.on=$ON -Dmetastore.compactor.initiator.on=$ON -Dhive.compactor.worker.threads=$MW -Dmetastore.compactor.worker.threads=$MW" \
  nohup $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service metastore > /tmp/metastore.out 2>&1 &
for i in $(seq 1 180); do (echo > /dev/tcp/127.0.0.1/9083) 2>/dev/null && break; sleep 1; done
echo "metastore up"
# Tez lib 의 hadoop-mapreduce-client 2.7 jar 는 Metastore Worker(MR 로컬 잡)와 충돌하므로 HiveServer2 에만 넣는다
export HADOOP_CLASSPATH=$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH
# Hive 3.1 원격 Metastore 의 컬럼 통계 병합 버그(ClassCastException)를 피하려고 컬럼 통계 자동 수집을 끈다
export HADOOP_CLIENT_OPTS="-Xmx1G $COMMON -Dhive.metastore.uris=thrift://localhost:9083 -Dhive.compactor.initiator.on=false -Dmetastore.compactor.initiator.on=false -Dhive.compactor.worker.threads=$HW -Dmetastore.compactor.worker.threads=$HW -Dhive.stats.column.autogather=false"
exec $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2
