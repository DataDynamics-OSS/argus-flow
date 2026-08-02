# Argus Flow — NiFi Extensions

Apache NiFi **2.10.0 / Java 21** 기반 커스텀 확장 번들 모음(Maven reactor).
도메인별 번들로 나뉘며 각 번들은 독립 NAR로 패키징되어 서드파티 의존성이 격리된다.
업스트림에서 포팅한 번들과 자체 구현 번들이 섞여 있다. 파생 관계와 라이선스 귀속은
루트 [`NOTICE`](../NOTICE), 기여 규약은 [`CONTRIBUTING.md`](../CONTRIBUTING.md) 참조.

전체 컴포넌트를 **유형별**로 정리한다. 패키지는 `io.datadynamics.nifi.*`로 통일하되,
업스트림에서 포팅한 iceberg만 원 패키지(`org.apache.nifi.*`)를 유지한다.

---

## 프로세서 (Processors) — 22종

### CDC 소스 — `nifi-argus-debezium-bundle`
Debezium Embedded Engine 3.2.2 기반. 변경 이벤트를 JSON FlowFile로 방출(`@PrimaryNodeOnly`, at-least-once).

| 이름 | 기능 |
|---|---|
| `CaptureChangeMySQL` | MySQL binlog CDC 소스 |
| `CaptureChangePostgreSQL` | PostgreSQL 논리 복제(pgoutput/decoderbufs) CDC 소스 |
| `CaptureChangeOracle` | Oracle LogMiner(CDB/PDB) CDC 소스 |
| `CaptureChangeSQLServer` | SQL Server CDC 소스 |

### 데이터베이스 — `nifi-argus-db-bundle`
| 이름 | 기능 |
|---|---|
| `ExecuteSQL` | SQL SELECT 실행 → Avro 결과 |
| `ExecuteSQLRecord` | SQL SELECT 실행 → 레코드 라이터로 출력 |
| `ExecuteFastSQL` | 대용량 결과셋 고속 스트리밍 실행 |
| `PutDatabaseRecord` | 레코드를 DB 테이블에 INSERT/UPSERT |
| `BulkOracleInsertProcessor` | Oracle 배열 바인딩 대량 insert |

### 레이크하우스 싱크
| 이름 | 번들 | 기능 |
|---|---|---|
| `PutIceberg` | `nifi-argus-iceberg-bundle` | Apache Iceberg 테이블에 레코드 적재 |
| `PutDeltaLake` | `nifi-argus-deltalake-bundle` | Delta Kernel 기반 경로형 Delta Lake 싱크 |
| `PutKudu` | `nifi-argus-kudu-bundle` | Apache Kudu 테이블에 레코드 적재 |

### Hive / ORC — `nifi-argus-hive-bundle`
| 이름 | 기능 |
|---|---|
| `SelectHive3QL` | Hive3 SELECT 실행 |
| `PutHive3QL` | Hive3 DDL/DML 실행 |
| `PutHive3Streaming` | Hive3 Streaming API로 스트리밍 적재 |
| `UpdateHive3Table` | Hive3 테이블 스키마 동기화(컬럼/파티션) |
| `TriggerHiveMetaStoreEvent` | Hive Metastore 이벤트 발생 트리거 |
| `PutORC` | ORC 파일로 기록 |

### 파일 포맷 — `nifi-argus-parquet-bundle`
| 이름 | 기능 |
|---|---|
| `PutParquet` | Parquet 파일로 기록 |
| `MergeParquet` | 다수 Parquet FlowFile을 하나로 병합 |

### 범용 — `nifi-argus-standard-bundle`
| 이름 | 기능 |
|---|---|
| `PrePostExecuteStreamCommand` | 외부 명령 실행 + 전/후처리 커맨드 |
| `MultilineCsvParser` | 멀티라인 필드를 포함한 CSV 파싱 |

---

## 컨트롤러 서비스 (Controller Services) — 7종

### 레코드 리더/라이터 — `nifi-argus-record-serialization-bundle`, `nifi-argus-parquet-bundle`
| 이름 | 기능 |
|---|---|
| `CSVReader` | 타임스탬프 포맷 지정 지원 CSV 레코드 리더 |
| `TimestampFormatAvroRecordSetWriter` | 타임스탬프 포맷 지정 Avro 라이터 |
| `TimestampFormatParquetRecordSetWriter` | 타임스탬프 포맷 지정 Parquet 라이터 |

### Hive DBCP 커넥션 풀 — `nifi-argus-hive-bundle`
| 이름 | 기능 |
|---|---|
| `Hive3ConnectionPool` | Hive3 JDBC 커넥션 풀 |
| `VendorHive3ConnectionPool` | 벤더 Hive JDBC 드라이버용 커넥션 풀 (드라이버 별도 제공, [번들 README](nifi-argus-hive-bundle/README.md)) |

### Iceberg 카탈로그 — `nifi-argus-iceberg-bundle`
| 이름 | 기능 |
|---|---|
| `HiveCatalogService` | Hive Metastore 기반 Iceberg 카탈로그 |
| `HadoopCatalogService` | Hadoop 파일시스템 기반 Iceberg 카탈로그 |

---

## 리포팅 태스크 (Reporting Tasks) — 5종 · `nifi-argus-reporting-bundle`

| 이름 | 기능 |
|---|---|
| `MonitorDiskUsageReportingTask` | 리포지토리 디스크 사용률 임계 초과 시 경고 |
| `MonitorMemoryUsageReportingTask` | JVM 힙 사용률 임계 초과 시 경고 |
| `MonitorMemoryPoolReportingTask` | 개별 메모리 풀 사용률 임계 초과 시 경고 |
| `MonitorThreadReportingTask` | 스레드 상태(데드락 등) 모니터링 |
| `HttpNotificationReportingTask` | 이벤트를 웹훅 URL로 HTTP POST 알림 (design §6.1 재구현) |

---

## Flow Analysis Rule — 6종 · `nifi-argus-flow-analysis-bundle`

운영 거버넌스용 정적 검사 규칙(신규 구현). 플로우 구성의 안티패턴을 위반으로 표시한다.

| 이름 | 기능 |
|---|---|
| `TimerThreadPoolCeilingRule` | 전역 Timer-Driven 스레드 수 > 코어×배수(기본 4) 검사 |
| `ProcessorThreadShareLimitRule` | 단일 프로세서가 전역 스레드 풀의 지정 비율(기본 50%) 초과 점유 검사 |
| `ProcessorConcurrencyCapRule` | 지정 타입(정규식) 프로세서의 Concurrent Tasks 상한 검사 |
| `ListingScheduleGuardRule` | List계열 프로세서의 0초 스케줄(소스 과부하) 검사 |
| `DeadEndFunnelRule` | 유입만 있고 유출 없는 막다른 Funnel(정체·백프레셔) 검사 |
| `IcebergSinkMergeRule` | PutIceberg 상류 N홉 내 Merge 부재(small-file 문제) 검사 |

---

## 서비스 API 인터페이스 (services-api)

NAR 클래스로더 계층에서 상위 API 번들로 분리되어 다른 번들이 참조한다.

| 인터페이스 | 번들(services-api) | 용도 |
|---|---|---|
| `HiveDBCPService`, `Hive3DBCPService`, `VendorHiveDBCPService` | `nifi-argus-hive-bundle` | Hive DBCP 커넥션 풀 계약 |
| `IcebergCatalogService`, `IcebergCatalogType`, `IcebergCatalogProperty` | `nifi-argus-iceberg-bundle` | Iceberg 카탈로그 서비스 계약 |

---

## 빌드

```bash
../mvnw -B package          # 전체 번들 NAR 빌드 (nifi-extensions/*/*/target/*.nar)
# 또는 리포지토리 루트에서
make -C .. extensions
```

산출 NAR은 `distribution/`(tar.gz/RPM)과 `docker/` 이미지에 편입된다
(distribution 편입 목록은 [`distribution/pom.xml`](../distribution/pom.xml) 참조).
