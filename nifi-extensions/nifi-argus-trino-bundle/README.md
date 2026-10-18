# nifi-argus-trino-bundle

Trino를 **조회·변환·유지보수 엔진**으로 쓰는 확장 번들. Trino JDBC 483 기반이다. 적재는 `PutIceberg`/`PutDeltaLake`가 파일을 직접 쓰고,
Trino는 그 테이블을 조회·변환·정리하는 역할을 맡는다(Trino를 통한 INSERT 적재 프로세서는 의도적으로 두지 않는다).

## 구성 요소

| 유형 | 이름 | 기능 |
|---|---|---|
| 컨트롤러 서비스 | `StandardTrinoConnectionService` | Trino JDBC 연결. `DBCPService` 호환이라 `ExecuteSQL` 등 기존 프로세서에도 연결 가능 |
| 프로세서 | `QueryTrinoRecord` | SELECT 결과를 Record Writer로 스트리밍, query id·통계 속성, 중지 시 쿼리 취소 |
| 프로세서 | `ExecuteTrinoStatement` | DDL/DML/ELT 단일 문장 실행(CTAS, `INSERT INTO ... SELECT`, `MERGE` 등) |
| 프로세서 | `MaintainTrinoTable` | Iceberg/Delta Lake/Hive 테이블 유지보수 |

아래 예제에서 프로퍼티는 **UI 표시명(프로퍼티 이름)** 형식으로 적는다.

---

## 1. 연결 서비스 설정

### 1.1 비밀번호 인증 + TLS (일반적인 운영 구성)

`StandardTrinoConnectionService`를 추가하고 다음처럼 설정한다.

| 프로퍼티 | 값 |
|---|---|
| Trino URL (`Trino URL`) | `jdbc:trino://trino.example.com:8443` |
| 인증 방식 (`Authentication Strategy`) | `PASSWORD` |
| 사용자 (`User`) | `nifi_etl` |
| 비밀번호 (`Password`) | (sensitive) |
| SSL Context Service (`SSL Context Service`) | Trino 서버 인증서를 신뢰하는 truststore가 있는 `StandardSSLContextService` |
| 기본 카탈로그 (`Default Catalog`) | `iceberg` |
| 기본 스키마 (`Default Schema`) | `sales` |
| Source (`Source`) | `argus-flow` (기본값. Trino UI와 resource group 선택에 쓰임) |
| Client Tags (`Client Tags`) | `etl,nightly` |

- catalog/schema는 URL 경로(`jdbc:trino://host:8443/iceberg/sales`)로 써도 되고, URL에 경로가 없으면 기본 카탈로그·스키마가 경로로 붙는다.
- `PASSWORD`·`JWT`·`KERBEROS`·`OAUTH2`는 TLS가 필수다. `NONE`(사용자 이름만 전달)은 개발·내부망 전용이다.
- 비밀번호·토큰을 URL 쿼리(`?password=...`)에 넣으면 invalid다. 같은 파라미터를 URL과 프로퍼티 양쪽에 지정해도 invalid다.
- 서비스 메뉴의 **Verify**로 드라이버 로드, 연결, `SELECT 1, current_user`를 확인할 수 있다.

### 1.2 세션 프로퍼티

동적 프로퍼티 `session.<이름>`은 Trino 세션 프로퍼티로 전달된다.

| 동적 프로퍼티 | 값 | 효과 |
|---|---|---|
| `session.query_max_run_time` | `2h` | 이 서비스를 쓰는 쿼리의 최대 실행 시간 |
| `session.iceberg.compression_codec` | `ZSTD` | 카탈로그 세션 프로퍼티(`<catalog>.<name>`) |

`session.`으로 시작하지 않는 동적 프로퍼티는 JDBC 드라이버 파라미터로 그대로 전달된다(예: `disableCompression=true`).

### 1.3 Kerberos / OAuth2

- **Kerberos**: 인증 방식 `KERBEROS`, `Kerberos 원격 서비스 이름`(보통 `trino`), `Kerberos Principal`, `Kerberos Keytab`(파일 경로), 필요하면 `Kerberos 설정 파일`(krb5.conf). 드라이버가 keytab으로 직접 로그인한다.
- **OAuth2**: 인증 방식 `OAUTH2`, `OAuth2 Access Token Provider`에 NiFi `StandardOauth2AccessTokenProvider`(client credentials 등)를 연결한다. 연결할 때마다 provider에서 토큰을 받으므로 만료 갱신은 provider가 맡는다. 브라우저 기반 외부 인증(`externalAuthentication`)은 지원하지 않는다.

### 1.4 외부 드라이버(Starburst JDBC 등) 사용

번들된 Trino JDBC 483 대신 다른 Trino 호환 드라이버를 쓰려면 다음처럼 설정한다.

| 프로퍼티 | 값 |
|---|---|
| 드라이버 위치(들) (`Database Driver Location(s)`) | `/opt/nifi/drivers/trino/trino-jdbc-<버전>.jar` (파일·디렉토리·URL, 쉼표로 여러 개) |
| 드라이버 클래스 이름 (`Database Driver Class Name`) | `io.trino.jdbc.TrinoDriver` ([Starburst JDBC 문서](https://docs.starburst.io/latest/client/jdbc.html) 기준) |

- 지정한 jar는 서비스 전용 child-first 클래스로더로 로드되므로, 번들 드라이버와 패키지가 같아도 외부 jar가 쓰인다.
- 드라이버 버전은 클러스터 버전과 같거나 높아야 한다(350 미만 미지원).
- Trino 진행 통계 API가 없는 드라이버면 query id·통계 속성만 빠지고 쿼리는 정상 실행된다.
- 모든 NiFi 노드의 같은 경로에 jar가 있어야 한다.

---

## 2. 예제: Iceberg 테이블 조회 결과를 Parquet 파일로 내보내기

```
GenerateFlowFile (CRON 0 0 2 * * ?)  →  UpdateAttribute  →  QueryTrinoRecord  →  PutHDFS
                                         base.date=${now():minus(86400000):format('yyyy-MM-dd')}
```

`QueryTrinoRecord` 설정:

| 프로퍼티 | 값 |
|---|---|
| Trino 연결 서비스 (`Trino Connection Service`) | 위에서 만든 서비스 |
| SQL 쿼리 (`SQL Query`) | `SELECT order_id, customer_id, amount, order_ts FROM iceberg.sales.orders WHERE order_date = DATE '${base.date}'` |
| Record Writer (`Record Writer`) | `ParquetRecordSetWriter` (또는 `TimestampFormatParquetRecordSetWriter`) |
| FlowFile 당 최대 행 수 (`Max Rows Per FlowFile`) | `1000000` (0이면 전부 한 FlowFile) |
| 쿼리 제한 시간 (`Query Timeout`) | `30 min` |

- `SQL 쿼리`를 비워 두면 입력 FlowFile 콘텐츠(UTF-8)를 쿼리로 쓴다.
- 결과 FlowFile에는 `trino.query.id`, `trino.processed.rows`, `trino.processed.bytes`, `trino.cpu.time.ms`, `trino.wall.time.ms`, `trino.queued.time.ms`, `record.count`가 붙는다. 분할하면 `fragment.identifier`/`fragment.index`/`fragment.count`도 붙는다.
- 입력 FlowFile은 `original`로, 결과는 `success`로 나간다. 결과가 0건이어도 스키마만 있는 FlowFile 1개(`record.count=0`)가 나간다.
- 프로세서를 중지하면 실행 중인 Trino 쿼리도 취소된다(Trino UI에서 `USER_CANCELED`).

---

## 3. 예제: Iceberg 적재 후 small file 정리

`PutIceberg`로 자주 적재하면 작은 파일이 쌓인다(Flow Analysis Rule `IcebergSinkMergeRule`이 경고하는 문제). 적재 성공 뒤 대상 테이블을 compaction하고, 하루 한 번 스냅샷을 만료시킨다.

### 3.1 적재 직후 OPTIMIZE

```
... → PutIceberg ─success→ UpdateAttribute ─→ MaintainTrinoTable (OPTIMIZE)
                          trino.catalog=iceberg
                          trino.schema=sales
                          trino.table=orders
```

| 프로퍼티 | 값 |
|---|---|
| 테이블 포맷 (`Table Format`) | `ICEBERG` |
| 작업 (`Operation`) | `OPTIMIZE` |
| 카탈로그 / 스키마 / 테이블 | `${trino.catalog}` / `${trino.schema}` / `${trino.table}` |
| 파일 크기 임계값 (`File Size Threshold`) | `128MB` (이보다 작은 파일을 합침) |
| 파티션 필터 (`Partition Filter`) | `order_date >= current_date - INTERVAL '1' DAY` (선택, 최근 파티션만) |
| 실행 모드 (`Execution Mode`) | `BACKGROUND` (OPTIMIZE는 오래 걸릴 수 있음) |

생성되는 SQL:

```sql
ALTER TABLE "iceberg"."sales"."orders" EXECUTE optimize(file_size_threshold => '128MB')
WHERE order_date >= current_date - INTERVAL '1' DAY
```

- 적재 FlowFile마다 OPTIMIZE가 돌면 과하므로, 앞에 `ControlRate`나 `MergeContent`로 빈도를 줄이는 것을 권장한다.
- `파티션 필터`는 SQL 조각을 그대로 붙이므로 신뢰할 수 있는 값만 넣는다(EL 미지원).

### 3.2 매일 스냅샷 만료 + orphan 파일 정리 (입력 없이 스케줄 실행)

`MaintainTrinoTable`을 입력 연결 없이 두고 **CRON 스케줄**(`0 30 3 * * ?`)로 실행한다. 작업별로 프로세서를 하나씩 둔다.

| 프로퍼티 | 프로세서 A | 프로세서 B |
|---|---|---|
| 테이블 포맷 | `ICEBERG` | `ICEBERG` |
| 작업 | `EXPIRE_SNAPSHOTS` | `REMOVE_ORPHAN_FILES` |
| 카탈로그 / 스키마 / 테이블 | `iceberg` / `sales` / `orders` | `iceberg` / `sales` / `orders` |
| 보존 기간 (`Retention Threshold`) | `7d` | `7d` |

```sql
ALTER TABLE "iceberg"."sales"."orders" EXECUTE expire_snapshots(retention_threshold => '7d')
ALTER TABLE "iceberg"."sales"."orders" EXECUTE remove_orphan_files(retention_threshold => '7d')
```

- 보존 기간이 Trino Iceberg 카탈로그의 최소 보존 기간(`iceberg.expire-snapshots.min-retention`, `iceberg.remove-orphan-files.min-retention`, 기본 7d)보다 짧으면 Trino가 거부하고 `failure`로 간다.
- 결과 FlowFile에는 `trino.maintenance.sql`, `trino.maintenance.operation`, `trino.query.id`, `trino.wall.time.ms`가 붙는다. optimize·remove_orphan_files가 돌려주는 지표 행 수는 `trino.result.row.count`에 남는다.

### 3.3 그 밖의 유지보수 작업

| 테이블 포맷 | 작업 | 생성 SQL |
|---|---|---|
| `DELTA_LAKE` | `OPTIMIZE` | `ALTER TABLE "c"."s"."t" EXECUTE optimize(file_size_threshold => '128MB')` |
| `DELTA_LAKE` | `VACUUM` | `CALL "c".system.vacuum('s', 't', '7d')` |
| `HIVE` | `SYNC_PARTITION_METADATA` | `CALL "c".system.sync_partition_metadata('s', 't', 'ADD')` (`동기화 모드`: ADD/DROP/FULL) |
| 전체 | `ANALYZE` | `ANALYZE "c"."s"."t"` |

포맷과 작업이 맞지 않는 조합(예: `HIVE` + `VACUUM`)은 invalid다.

---

## 4. 예제: Hive 외부 테이블 파티션 등록

`PutORC`/`PutParquet`로 HDFS 파티션 디렉토리에 파일을 쓴 뒤, 메타스토어에 새 파티션을 등록한다.

```
... → PutORC ─success→ MaintainTrinoTable
```

| 프로퍼티 | 값 |
|---|---|
| 테이블 포맷 | `HIVE` |
| 작업 | `SYNC_PARTITION_METADATA` |
| 카탈로그 / 스키마 / 테이블 | `hive` / `logs` / `access_log` |
| 동기화 모드 (`Sync Mode`) | `ADD` |

---

## 5. 예제: ELT — 집계 테이블 갱신

원천 데이터를 NiFi로 가져오지 않고 Trino 안에서 변환한다.

```
GenerateFlowFile (CRON 0 10 * * * ?)  →  UpdateAttribute  →  ExecuteTrinoStatement
                                          base.hour=${now():minus(3600000):format('yyyy-MM-dd HH:00:00')}
```

| 프로퍼티 | 값 |
|---|---|
| SQL 문장 (`SQL Statement`) | 아래 SQL |
| 실행 모드 (`Execution Mode`) | `BACKGROUND` |
| 최대 백그라운드 쿼리 수 (`Max Background Queries`) | `2` |
| 쿼리 제한 시간 (`Query Timeout`) | `1 hour` |

```sql
INSERT INTO iceberg.mart.hourly_sales
SELECT date_trunc('hour', order_ts) AS sales_hour, customer_id, sum(amount) AS total_amount
FROM iceberg.sales.orders
WHERE order_ts >= TIMESTAMP '${base.hour}'
  AND order_ts <  TIMESTAMP '${base.hour}' + INTERVAL '1' HOUR
GROUP BY 1, 2
```

- 한 번에 한 문장만 실행한다. 끝의 `;` 하나는 무시하지만, 여러 문장을 `;`로 이으면 `failure`다.
- 성공 FlowFile에는 `trino.query.id`, `trino.update.count`(INSERT된 행 수)와 통계 속성이 붙는다.
- 결과 집합을 돌려주는 문장(`SHOW` 등)은 결과를 버리고 행 수만 `trino.result.row.count`에 남긴다.
- **재처리 주의**: `BACKGROUND` 모드에서 노드가 죽거나 프로세서를 중지하면 FlowFile이 큐로 돌아가 다시 실행된다. 위 INSERT는 다시 실행하면 중복되므로, 운영에서는 `DELETE ... WHERE sales_hour = ...` 뒤 `INSERT` 하거나 `MERGE`로 바꿔 재실행해도 결과가 같게 만든다.

### 실행 모드

| 모드 | 동작 | 언제 |
|---|---|---|
| `SYNCHRONOUS` (기본) | NiFi 타이머 스레드에서 쿼리를 끝까지 실행 | 짧은 문장 |
| `BACKGROUND` | 프로세서 전용 스레드 풀(`최대 백그라운드 쿼리 수`)에서 실행하고 NiFi 스레드는 바로 반환. 풀이 가득 차면 새 FlowFile을 꺼내지 않고 대기 | OPTIMIZE·대형 INSERT처럼 수 분 이상 걸리는 작업 |

`QueryTrinoRecord`는 동기 실행만 지원한다. Trino는 클라이언트가 결과를 계속 가져가지 않으면 일정 시간(`query.client.timeout`, 기본 5분) 뒤 쿼리를 버리므로, query id만 저장해 두고 나중에 확인하는 방식은 쓰지 않는다.

---

## 6. 예제: 기존 SQL 프로세서에서 Trino 연결 쓰기

`StandardTrinoConnectionService`는 `DBCPService`를 구현하므로 NiFi 기본 `ExecuteSQL`, `ExecuteSQLRecord`, `PutDatabaseRecord`의
`Database Connection Pooling Service`에 그대로 지정할 수 있다. 다만 query id·통계 속성과 중지 시 취소는 이 번들의 프로세서에서만 제공된다.

---

## 7. 운영 주의사항

- 사용자에게 `system.jdbc` 스키마 조회 권한이 필요하다.
- 클러스터에서 spooling protocol을 켰다면 NiFi JVM이 spooling 객체 스토리지에도 네트워크로 접근할 수 있어야 한다.
- 실패 FlowFile에는 `trino.error.message`, `trino.error.code`, `trino.query.id`가 붙는다. query id로 Trino UI에서 원인을 찾는다.
- 연결 풀은 없다. Trino JDBC 연결은 HTTP 기반이라 요청마다 연결을 만들어도 비용이 작다.
- Kerberos는 실제 KDC 환경에서, Delta Lake OPTIMIZE는 실제 객체 스토리지에서 아직 검증하지 않았다. Starburst 배포 jar로도 시험하지 않았다.
