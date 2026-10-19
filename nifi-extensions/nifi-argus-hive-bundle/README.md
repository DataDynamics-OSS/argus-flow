# nifi-argus-hive-bundle

Apache Hive 3.x 연동 확장 번들. Hive/ORC 프로세서와 Hive DBCP 커넥션 풀 컨트롤러 서비스를 제공한다.

## 구성 요소

| 유형 | 이름 | 기능 |
|---|---|---|
| 프로세서 | `SelectHive3QL` / `PutHive3QL` | Hive3 SELECT / DDL·DML 실행 |
| 프로세서 | `PutHive3Streaming` | Hive3 Streaming API 적재 |
| 프로세서 | `UpdateHive3Table` | Hive3 테이블 스키마 동기화 |
| 프로세서 | `TriggerHiveMetaStoreEvent` | Hive Metastore 이벤트 트리거 |
| 프로세서 | `CompactHive3Table` | ACID 테이블 major/minor compaction 요청·상태 추적, Non-ACID ORC `CONCATENATE`, 출력 파일 수 지정(`REBALANCE`·`REWRITE`), 전후 행 수 검증 |
| 프로세서 | `ListHiveCompactionCandidates` | 파티션 디렉토리(base/delta, 파일 수·크기)를 세어 compaction이 필요한 파티션마다 FlowFile 생성 |
| 프로세서 | `PutORC` | ORC 파일 기록 |
| 컨트롤러 서비스 | `Hive3ConnectionPool` | Hive3 JDBC 커넥션 풀 |
| 컨트롤러 서비스 | `VendorHive3ConnectionPool` | 벤더 Hive JDBC 드라이버용 커넥션 풀 (드라이버 별도 제공) |

---

## `CompactHive3Table` — Hive 테이블 compaction

`PutHive3Streaming`·`PutHive3QL`(INSERT)로 ACID 테이블에 자주 적재하면 파티션마다 `delta_*` 디렉토리가 쌓인다.
`CompactHive3Table`은 HiveServer2(`hive3-dbcp-service`)로 compaction을 요청하고 끝날 때까지 추적한다.

| 테이블 | 실행 | 완료 판단 |
|---|---|---|
| Full ACID / insert-only ACID | `ALTER TABLE db.t [PARTITION (...)] COMPACT 'major'\|'minor'` (비동기, `AND WAIT` 없음) | `SHOW COMPACTIONS`의 해당 id가 `Completion State`에 도달 |
| Non-ACID ORC (managed) | `ALTER TABLE db.t [PARTITION (...)] CONCATENATE` (동기) | 문장이 끝나면 완료 |
| 그 밖의 Non-ACID, external ORC | 실행하지 않음(파일 수를 줄이려면 `REWRITE`) | `failure` |

`Compaction Type`으로 출력 파일 수를 정하는 두 가지를 더 고를 수 있다.

| `Compaction Type` | 실행 | 대상 | 추적 |
|---|---|---|---|
| `REBALANCE` | `ALTER TABLE ... [PARTITION (...)] COMPACT 'rebalance' CLUSTERED INTO <N> BUCKETS` | Hive 4 이상, 버킷 없는 Full ACID | 비동기(MAJOR와 같은 `in progress` 루프) |
| `REWRITE` | `INSERT OVERWRITE TABLE ... [PARTITION (...)] SELECT <데이터 컬럼> FROM ... [WHERE <파티션>] DISTRIBUTE BY <키>` (리듀서 N개) | ACID·Non-ACID(external 포함), 버킷 테이블 제외 | 동기(트리거 스레드에서 끝까지) |

### 동작

1. 처음 들어온 FlowFile(`hive.compaction.id` 없음): `SHOW TBLPROPERTIES`·`DESCRIBE FORMATTED`로 테이블 종류와 파티션 컬럼을
   판별하고, `Validate Row Count=true`면 `SET hive.compute.query.using.stats=false` 후 `COUNT(*)`를 센다.
   요청 직전 `SHOW COMPACTIONS`의 최대 id를 기록하고 요청 뒤 새로 생긴 id를 `hive.compaction.id`에 넣는다.
   같은 대상에 이미 대기·실행 중인 compaction이 있으면 그 id를 이어서 추적한다.
2. 아직 진행 중(`initiated`, `working`)이면 FlowFile을 **penalize해서 `in progress`로** 보낸다.
3. `in progress`를 **이 프로세서 자신에게 다시 연결**해 두면, Penalty Duration이 지난 뒤 다시 들어와 `SHOW COMPACTIONS`로
   상태만 확인한다. 진행 상황이 FlowFile 속성에 있으므로 NiFi를 재시작해도 이어서 추적한다.
4. 완료되면 사후 `COUNT(*)`를 세어 같으면 `success`, 다르면 `count mismatch`로 보낸다.
   `failed`·`refused` 등 실패 상태, `Initiation Timeout`·`Compaction Timeout` 초과는 `failure`(이유는 `hive.compaction.error`).

### 사용 예 — 전날 파티션 compaction

```
GenerateFlowFile (매일 02:00, Cron)
  └─ UpdateAttribute  dt = ${now():toNumber():minus(86400000):format('yyyy-MM-dd')}
       └─ CompactHive3Table ──success────────▶ (완료 로그/다음 단계)
            │  ▲            ──count mismatch─▶ PutEmail / PutSlack (알림)
            │  │            ──failure────────▶ PutEmail / PutSlack (알림)
            └──┘ in progress (자기 자신으로 연결)
```

| 프로퍼티 | 값 |
|---|---|
| Hive 데이터베이스 연결 풀링 서비스 (`hive3-dbcp-service`) | `Hive3ConnectionPool` 또는 `VendorHive3ConnectionPool` |
| 데이터베이스 (`Database`) | `sales` |
| 테이블 (`Table`) | `events` |
| 파티션 지정 (`Partition Spec`) | `dt='${dt}'` (여러 키면 `dt='${dt}',hour='03'`. 비파티션 테이블이면 비움) |
| Compaction 종류 (`Compaction Type`) | `MAJOR` (조회 성능 우선) 또는 `MINOR` |
| 테이블 종류 처리 (`Table Type Handling`) | `ACID_AND_NON_ACID_ORC` (기본) |
| 행 수 검증 (`Validate Row Count`) | `true` |
| 완료 기준 상태 (`Completion State`) | `READY_FOR_CLEANING` (기본) 또는 `SUCCEEDED` |
| 시작 대기 타임아웃 (`Initiation Timeout`) | `30 min` |
| Compaction 타임아웃 (`Compaction Timeout`) | `6 hours` |

**Settings 탭**

- **Penalty Duration = 폴링 간격.** 예: `1 min`. 진행 중인 FlowFile은 이 시간만큼 기다린 뒤 다시 상태를 확인한다.
  짧게 잡으면 `SHOW COMPACTIONS` 호출이 잦아진다.
- `in progress` relationship은 auto-terminate하지 말고 **자기 자신에게 연결**한다. 연결하지 않으면 추적이 끊긴다
  (요청 자체는 Hive에 남는다).

### 출력 파일 수 지정 — `REBALANCE`·`REWRITE`

**N(출력 파일 수)**: `Target File Count`를 주면 그 값. 없으면 `N = max(1, ceil(파티션 크기 / Target File Size))`(기본 512 MB).

| `Partition Size Source` | 파티션 크기 |
|---|---|
| `HDFS` (기본, 권장) | 파티션 Location을 Hadoop FileSystem으로 직접 센다. ACID는 현재 base·delta만 센다. `Hive 설정 리소스`(`hive3-config-resources`: core-site.xml, hdfs-site.xml)와 보안 클러스터면 `Kerberos 사용자 서비스`가 필요하다 |
| `TABLE_STATISTICS` | `DESCRIBE FORMATTED`의 `totalSize`. 통계가 오래됐거나 없을 수 있다(없으면 `failure`) |

**REWRITE**

- `Distribute By Columns`(필수): 값이 고르게 퍼진 컬럼(예: PK). Hive는 이 키의 해시로 행을 리듀서 N개에 나누므로 값 종류가 충분해야 파일이 N개 나온다.
  `rand()` 같은 함수는 쓸 수 없다(태스크 재시도 때 행이 중복되거나 빠질 수 있다). 파티션 컬럼도 쓸 수 없다.
- SELECT 목록은 `DESCRIBE`의 데이터 컬럼을 정의 순서대로 자동으로 만든다.
- 세션에 `hive.tez.auto.reducer.parallelism=false`, `mapreduce.job.reduces=N`, `hive.merge.tezfiles/mapfiles/mapredfiles=false`를 걸고,
  실행 전 값을 읽어 두었다가 끝나면(실패 포함) 원래 값으로 되돌린다(`hive.compute.query.using.stats`도 같다). 커넥션 풀이 세션을 재사용하므로
  다른 프로세서로 설정이 새지 않게 하기 위해서다. SQL 표준 권한 모드면 이 키들이 `hive.security.authorization.sqlstd.confwhitelist`에 있어야 한다.
- 파티션에 배타 lock을 잡고 Tez 작업이 끝날 때까지 NiFi 스레드를 쓴다. `쿼리 타임아웃`(`hive3-query-timeout`)으로 상한을 두고, 적재가 끝난 파티션에만 쓴다.

**REBALANCE**

- HiveServer2 버전(`SELECT version()`)이 4 미만이면 `failure`.
- Worker 쪽 Hive 서버에 query-based compaction(`hive.compactor.crud.query.based=true`, Tez)이 켜져 있어야 한다. 꺼져 있으면 요청은 받아들여지지만
  compaction이 `failed`("Rebalancing compaction is only supported in Tez, and via Query based compaction")로 끝나고 `failure`로 보낸다.
- `Completion State`, Timeout 규칙은 MAJOR와 같다.

```
CompactHive3Table
  Compaction Type        = REWRITE
  Target File Size       = 512 MB          (또는 Target File Count = 8)
  Partition Size Source  = HDFS
  Hive 설정 리소스        = /etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
  Distribute By Columns  = order_id
  쿼리 타임아웃           = 3600
```

### 운영 주의

- 적재가 끝난 파티션(보통 D-1 이전)만 대상으로 한다. `CONCATENATE`는 배타 lock을 잡는다.
- `ready for cleaning`은 새 base/delta가 커밋돼 조회가 이미 새 파일을 쓰는 상태다. 옛 디렉토리는 Metastore의 Cleaner가
  열린 트랜잭션이 끝난 뒤 지운다. 디스크 정리까지 기다리려면 `Completion State=SUCCEEDED`를 쓴다.
- 요청이 `initiated`에 머물면 Hive 서버의 `hive.compactor.initiator.on=true`, `hive.compactor.worker.threads>0` 설정과
  Worker 큐 적체를 확인한다(서버 설정은 이 프로세서가 바꾸지 않는다).
- 자동 compaction(Initiator)과 겹칠 수 있다. 수동으로만 관리할 테이블에는 `TBLPROPERTIES ('no_auto_compaction'='true')`를 권장한다.
- 파티션 테이블은 모든 파티션 키를 지정해야 한다(테이블 전체를 한 번에 compaction하지 않는다).
- Hive 4는 Non-ACID managed 테이블을 기본적으로 EXTERNAL로 바꿔 만든다. external 테이블은 `CONCATENATE`를 할 수 없어 `failure`로 보낸다.
- Hive 4는 delta가 하나뿐인 경우처럼 compaction 임계값에 못 미치면 요청을 `refused`로 끝낸다(`failure`, 이유에 Hive 메시지 포함).
- Hive 4는 ACID 테이블 Location의 전체 경로에 `_`나 `.`로 시작하는 디렉토리(예: `/data/_work/warehouse`)가 있으면 그 아래 delta를
  숨김 경로로 보고 읽지 않는다. 조회 결과가 0행이 되고 compaction도 `refused`("None of the compaction thresholds met")로 끝난다.
  웨어하우스·테이블 경로에는 `_`·`.`로 시작하는 구성 요소를 쓰지 않는다(Hive 3.1에는 이 검사가 없다).

### 출력 속성

`hive.compaction.id`, `hive.compaction.type`(MAJOR/MINOR/REBALANCE/REWRITE/CONCATENATE), `hive.compaction.state`, `hive.compaction.requested.at`(epoch ms),
`hive.compaction.duration.ms`, `hive.compaction.rowcount.before`, `hive.compaction.rowcount.after`, `hive.compaction.table`(db.table),
`hive.compaction.partition`, `hive.compaction.target.file.count`, `hive.compaction.partition.size.bytes`(크기로 N을 계산한 경우), `hive.compaction.error`.

검증한 서버: Apache Hive 3.1.3, 4.0.1 (공식 `apache/hive` Docker 이미지, Apache `hive-jdbc` 드라이버). REBALANCE는 4.0.1 + query-based compaction에서 확인했다.

---

## `ListHiveCompactionCandidates` — compaction 대상 파티션 찾기

자동 compaction(Initiator)에 맡기지 않는 테이블에서, 한 테이블의 파티션을 훑어 compaction이 필요한 파티션마다 빈 FlowFile을 하나씩 낸다.

1. `SHOW PARTITIONS db.t [PARTITION (<Partition Filter>)]`로 파티션을 얻고, 파티션마다 `DESCRIBE FORMATTED ... PARTITION (...)`의 Location을
   Hadoop FileSystem으로 센다(`hive3-config-resources`, `kerberos-user-service`).
2. 판정
   - ACID: Hive Initiator와 같은 기준. 현재 delta 크기 / base 크기 > `Delta Size Ratio Threshold`(기본 0.1, base가 없으면 delta가 있으면) → **MAJOR**,
     아니면 delta + delete_delta 디렉토리 수 > `Delta Count Threshold`(기본 10) → **MINOR**. 이미 합쳐졌지만 Cleaner가 아직 지우지 않은 옛 base·delta는 세지 않는다.
   - Non-ACID: 파일 수 > `Small File Count Threshold`(기본 10)이고 평균 파일 크기 < `Small File Size`(기본 64 MB) → managed ORC면 **CONCATENATE**, 아니면 **REWRITE**.
3. 제외: 마지막 수정이 `Minimum Partition Age`(기본 1 hour)보다 최근인 파티션(적재 중일 수 있음), `SHOW COMPACTIONS`에서 `initiated`·`working`인 파티션.
4. delta 수(Non-ACID는 파일 수) 내림차순으로 최대 `Maximum Candidates`(기본 100)개.

출력 속성: `hive.compaction.database`, `hive.compaction.table`, `hive.compaction.partition.spec`(Partition Spec 형식), `hive.compaction.recommended.type`,
`hive.compaction.delta.count`, `hive.compaction.delete.delta.count`, `hive.compaction.base.bytes`, `hive.compaction.delta.bytes`,
`hive.compaction.file.count`, `hive.compaction.partition.location`. Relationships: `success`(후보마다), `original`(입력이 있을 때), `failure`.

### 사용 흐름 — 매일 후보를 찾아 compaction

```
GenerateFlowFile (Cron 0 30 2 * * ?)
  └─ ListHiveCompactionCandidates  (Database=sales, Table=events, Partition Filter 선택)
       └─success─ RouteOnAttribute  (hive.compaction.recommended.type 별: major / minor / concatenate / rewrite)
            ├─major──── CompactHive3Table (Compaction Type=MAJOR)  ──success──▶ (로그)
            │             Database       = ${hive.compaction.database}
            │             Table          = ${hive.compaction.table}
            │             Partition Spec = ${hive.compaction.partition.spec}
            │             in progress    → 자기 자신, Penalty Duration = 1 min
            └─minor──── CompactHive3Table (Compaction Type=MINOR)  ... (같은 설정)
```

`Compaction Type`은 Expression Language를 지원하지 않으므로, 권장 유형별로 나누려면 `RouteOnAttribute`로 서로 다른 `CompactHive3Table`에 보낸다.
권장 유형을 무시하고 모두 MAJOR로 처리해도 된다. `CompactHive3Table`은 처리하면서 `hive.compaction.table`을 `db.table`로 덮어쓴다.
파티션마다 `DESCRIBE`와 디렉토리 목록을 읽으므로 파티션이 많은 테이블은 `Partition Filter`(예: 전날 `dt='${now():toNumber():minus(86400000):format('yyyy-MM-dd')}'`)로 범위를 줄인다.

---

## `VendorHive3ConnectionPool` — JDBC 드라이버 수동 적용

`VendorHive3ConnectionPool`은 벤더 Hive JDBC 드라이버(`com.cloudera.hive.jdbc.HS2Driver`)로
CDP Hive에 연결한다. **이 드라이버는 라이선스 문제로 번들·배포판에 포함하지 않는다.** CDP를
사용하는 사용자는 라이선스 문제가 없으므로 드라이버를 직접 내려받아 적용한다.

### 1. 드라이버 다운로드

- Cloudera 다운로드 포털에서 **"Cloudera JDBC Connector for Apache Hive"** 를 받는다(CDP 이용자 라이선스 무상).
- zip 압축을 풀면 `HiveJDBC42.jar`(2.6.x 최신은 단일 fat jar)가 나온다. 구버전(2.5.x)은 의존 jar가
  여러 개 동봉되므로 **전부** 사용한다.
- 드라이버 클래스명 `com.cloudera.hive.jdbc.HS2Driver` 는 서비스에 하드코딩되어 있어 별도 지정이 필요 없다.

### 2. 드라이버 적용 — 두 가지 방법

#### 방법 A — "드라이버 위치(들)" 프로퍼티 (권장)

서비스별로 드라이버를 격리 로드하며, **NiFi 재기동이 필요 없고** `lib/` 를 건드리지 않는다.

1. 내려받은 드라이버 jar를 NiFi가 접근 가능한 경로에 둔다(예: `/opt/argus-flow/drivers/HiveJDBC42.jar`).
   - 클러스터라면 **모든 노드의 동일 경로**에 두거나, 공유 스토리지/URL을 사용한다.
2. NiFi UI에서 `VendorHive3ConnectionPool` 컨트롤러 서비스의 **드라이버 위치(들)** 프로퍼티에
   jar 파일/디렉터리 경로 또는 URL을 지정한다(쉼표로 여러 개 지정 가능, 디렉터리 지정 시 하위 jar 포함).
3. 서비스를 **Enable** 한다. 지정한 jar가 이 서비스 전용 클래스로더에 로드된다.

> 동작 원리: 이 프로퍼티는 `dynamicallyModifiesClasspath` + `@RequiresInstanceClassLoading` 조합으로,
> 지정한 jar를 서비스 인스턴스 전용 클래스로더에 주입한다. 프로퍼티만 바꿔 Enable/Disable 하면 되므로
> 재기동이 필요 없다.

#### 방법 B — NiFi `lib/` 에 배치 (전역·재기동 필요)

```bash
cp HiveJDBC42.jar  $NIFI_HOME/lib/     # 구버전이면 동봉 jar 전부 복사 · 클러스터는 전 노드
$NIFI_HOME/bin/nifi.sh restart          # lib/ 는 기동 시에만 로드됨
```

`lib/` 의 jar는 시스템 클래스로더가 로드하여 모든 확장에서 보인다. 방법 A를 쓰기 어려운
환경(예: 자동화된 이미지 빌드 단계에서 드라이버를 미리 심는 경우)에 적합하다.

### 3. 커넥션 풀 설정 (NiFi UI)

- **데이터베이스 연결 URL** — CDP 배포 형태별로 상이:
  - 온프레(Kerberos): `jdbc:hive2://<host>:10000/default;AuthMech=1;KrbServiceName=hive;KrbHostFQDN=<host>;KrbRealm=<REALM>`
  - CDP Public Cloud(HTTP/SSL): `jdbc:hive2://<host>:443/default;ssl=1;transportMode=http;httpPath=cliservice`
- **데이터베이스 사용자 / 비밀번호** — 기본 인증 시.
- **Kerberos 사용자 서비스** — 커버로스 환경이면 `KerberosUserService` 컨트롤러 서비스를 연결한다
  (`hive-site.xml` 등은 **Hive 설정 리소스** 프로퍼티로 지정).

### 4. 문제 해결

| 증상 | 원인 / 조치 |
|---|---|
| 서비스 Enable 실패: `Hive JDBC 드라이버 클래스(com.cloudera.hive.jdbc.HS2Driver)를 찾을 수 없습니다` (원인 `ClassNotFoundException`) | 드라이버 jar 미적용. 방법 A는 경로/URL 확인, 방법 B는 `lib/` 배치 후 **재기동** 여부 확인 |
| 클러스터 일부 노드만 실패 | 해당 노드에 드라이버 jar 누락. 모든 노드에 동일 적용 |
| Kerberos 인증 실패 | `KerberosUserService` 미설정 또는 `hive-site.xml`의 보안 속성 누락 |
