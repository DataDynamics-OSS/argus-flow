# Changelog

이 프로젝트의 주요 변경 사항을 기록합니다.
형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/)를 따릅니다.

버전은 `<NiFi 버전>-<릴리스 번호>` 형태입니다. 예를 들어 `2.10.0-1`은 Apache NiFi 2.10.0을
기반으로 한 첫 번째 릴리스를 뜻합니다. 기반 NiFi 버전이 올라가면 릴리스 번호는 1로 돌아갑니다.

## [Unreleased]

### Added
- **Trino 번들** (`nifi-argus-trino`) — Trino를 조회·변환·유지보수 엔진으로 씁니다(Trino JDBC 483).
  사용 예제는 [`nifi-extensions/nifi-argus-trino-bundle/README.md`](nifi-extensions/nifi-argus-trino-bundle/README.md).
  - `StandardTrinoConnectionService`: `DBCPService`를 구현해 `ExecuteSQL`·`PutDatabaseRecord`에도 연결됩니다.
    인증 NONE/PASSWORD/JWT/Kerberos/OAuth2, `source`·client tags·세션 프로퍼티(`session.*`)·roles·spooling 인코딩.
    Trino JDBC 연결은 HTTP 기반이라 풀을 두지 않습니다.
  - 외부 드라이버 jar 경로와 클래스명을 지정해 Starburst JDBC 등 Trino 호환 드라이버를 쓸 수 있습니다.
    외부 jar는 서비스 전용 child-first 클래스로더로 로드되어 번들 드라이버에 가려지지 않습니다.
  - `QueryTrinoRecord`: SELECT 결과를 Record Writer로 스트리밍합니다. `trino.query.id`와 실행 통계를 속성으로
    남기고, 중지하면 Trino 쿼리도 취소합니다.
  - `ExecuteTrinoStatement`: CTAS·`INSERT INTO ... SELECT`·`MERGE` 등 DDL/DML 한 문장을 실행합니다.
  - `MaintainTrinoTable`: Iceberg `optimize`·`expire_snapshots`·`remove_orphan_files`, Delta Lake `optimize`·`vacuum`,
    Hive `sync_partition_metadata`, `ANALYZE`.
  - 오래 걸리는 문장용 `BACKGROUND` 실행 모드: 프로세서 전용 스레드 풀에서 실행하고 NiFi 스레드는 바로 돌려줍니다.
  - Trino를 통한 INSERT 적재 프로세서는 두지 않습니다. 적재는 `PutIceberg`·`PutDeltaLake`가 맡습니다.
- `MergeParquet`: 선택 프로퍼티 `Kerberos User Service`로 보안 HDFS를 지원합니다. 원본 경로로 일반 디렉토리를
  주면 그 안의 `*.parquet`를 병합합니다.
- `VendorHive3ConnectionPool`: 동적 프로퍼티를 HiveConf에 반영합니다. `Max Wait Time`에 `-1`(무한 대기)을 쓸 수 있습니다.
- `PutKudu`: Apache NiFi 1.28의 프로퍼티 이름을 현재 이름으로 마이그레이션합니다(`migrateProperties`).
- `Hive3ConnectionPool`·`PutHive3Streaming`: 1.28의 Kerberos principal·keytab/password 설정을 Kerberos User Service로
  자동 이전합니다.
- **트리거형 List 프로세서** (`nifi-argus-listing`) — `ListFile`, `ListSFTP`, `ListFTP`, `ListS3`,
  `ListHDFS`(패키지 `io.datadynamics.nifi.processors.listing.*`). NiFi 2.10.0의 List 프로세서를 포크해 Upstream
  연결을 받도록(INPUT_REQUIRED) 바꿨습니다. 입력 FlowFile마다 그 속성으로 정한 대상 하나를 List하고,
  결과를 입력의 자식으로 `success`에, 입력을 `original`에, 실패를 `failure`에 보냅니다.
  - 추적 전략은 대상별로 분리됩니다: `No Tracking`(기본), `Tracking Timestamps`(CLUSTER 상태, CAS 저장),
    `Tracking Entities`(DistributedMapCache).
  - 추적 상태의 키에는 대상과 함께 결과에 영향을 주는 설정(필터·재귀·파일 나이·크기 등)이 들어가, 필터를 바꾸면
    같은 경로라도 처음부터 다시 List합니다. 대상·필터·전략을 바꾸면 다음 시작 시 추적 상태와 캐시 항목을 지웁니다
    (업스트림의 속성 변경 시 상태 초기화와 같은 시점). `Maximum Tracked Targets`를 넘은 오래된 대상의 상태와
    캐시 항목도 지웁니다.
  - FlowFile이 대상을 정하므로 허용 범위를 제한하는 속성을 둡니다: `Allowed Base Directory`
    (File 필수, HDFS 선택), `Allowed Hosts`(SFTP/FTP), `Allowed Buckets`(S3). 결과 폭증을 막는
    `Maximum Listing Size`도 있습니다.
  - `@PrimaryNodeOnly`가 없어 클러스터의 모든 노드가 각자 받은 FlowFile을 처리합니다.
  - `scripts/port-listing.py`가 업스트림 원문에 이름만 바꾼 기준본을 재현합니다(업그레이드 시 3-way 병합용).
- `ListingScheduleGuardRule`이 트리거형 List 프로세서(`io.datadynamics.nifi.processors.listing.*`)는 검사하지
  않습니다. 단순 클래스명이 업스트림과 같지만 입력이 있을 때만 실행되어 0초 스케줄이 정상입니다.
- Apache License 2.0으로 공개. `LICENSE`, `NOTICE`, `CONTRIBUTING.md`, `SECURITY.md` 추가.
- `apache-rat-plugin` 기반 라이선스 헤더 검사를 빌드(`validate` 단계)와 CI에 추가.
- **설정 도구를 배포본에 포함** (`bin/argus-config.sh`) — `conf/` 를 대화형(TUI)으로
  편집합니다. 지금까지는 소스 저장소에서 pip 로 설치해야 했으나, 2.3MB zipapp 으로
  `tools/argus-config/` 에 담겨 tar.gz·RPM 만 받은 환경에서도 바로 쓸 수 있습니다.
  python3 3.10 이상이 필요하며, 없으면 도구만 동작하지 않고 NiFi 자체에는 영향이 없습니다.
- **설정 마법사와 진단** — `argus-config.sh`에 다음이 추가되었습니다.
  - **처음 설정하기**: 접속 주소 → TLS 인증서 → 로그인 방식을 순서대로 안내합니다.
    접속 주소를 한 번만 받아 인증서 SAN에 그대로 쓰므로 `Invalid SNI`를 예방합니다.
  - **`--check`**: SAN 불일치, IP 접속 불가, 인증서 만료, 주석 상태인 로그인 프로바이더,
    DB 인증의 인증·인가 불일치를 진단합니다. 문제가 있으면 종료 코드 1.
  - **레시피 `tls:generate`**: 인증서 생성 명령을 안내하고 keystore 관련 키를 설정합니다.
  - **레시피 `login:db`**: `login-identity-providers.xml`과 `authorizers.xml`을 한 번에
    일관되게 설정합니다.
  - **사용자 관리 메뉴**: `argus-user.sh`에 위임합니다.
- **`bin/argus-ssl.sh`** — 인증서 생성 스크립트를 배포본에 포함. 지금까지는 소스 저장소에만
  있었습니다.
- **DB 기반 인증·인가 확장** (`nifi-argus-db-iaa`) — 사용자와 비밀번호를 RDB에서 관리합니다.
  `DbLoginIdentityProvider`(인증)와 `DbUserGroupProvider`(인가)를 함께 제공하며, NiFi UI에서
  사용자·그룹을 관리할 수 있습니다. PostgreSQL과 MariaDB를 지원하고, PostgreSQL 드라이버만
  번들합니다(MariaDB Connector/J는 LGPL-2.1이라 미포함 — 직접 조달).
  두 프로바이더 블록은 `conf/authorizers.xml`·`conf/login-identity-providers.xml`에
  **주석 처리된 상태**로 배포됩니다.
- **사용자 관리 CLI** (`bin/argus-user.sh`) — 사용자·그룹 추가/삭제/변경, 비밀번호 설정,
  계정 잠금 해제, 스키마 적용. NiFi가 떠 있는 상태에서도 사용할 수 있습니다.
  비밀번호는 인자로 받지 않습니다(프롬프트 또는 `--password-stdin`).
- 스키마 DDL을 배포본 `sql/db-iaa/`에 노출 — DDL 권한을 애플리케이션에 주지 않는 환경용.
- `scripts/ssl/ssl-generate-openssl.sh` — SAN에 DNS 이름과 IP 주소를 명시 지정하는
  openssl 기반 인증서 생성기. CA 100년 / 서버 10년(각각 `NIFI_SSL_CA_DAYS`,
  `NIFI_SSL_DAYS`로 조정). IP로 NiFi에 접속할 때 발생하는 `Invalid SNI` 해결용.

### Changed
- **업그레이드 시 동작이 바뀝니다.** 이전에 success로 넘어가던 입력 일부가 failure로 갑니다.
  - `MultilineCsvParser`: 잘못된 입력 바이트, 출력 문자셋에 없는 문자(예: EUC-KR 출력의 확장 한글), 닫히지 않은
    Quote, 닫는 Quote 뒤의 구분자가 아닌 문자는 치환·병합하지 않고 failure로 보냅니다. 오류 위치를 `csv.error`에
    남기며(최대 1024자), UTF-8 BOM은 제거합니다.
  - `MergeParquet`: 대상 파일이 원본 glob에 포함되거나, 입력 파일 간 스키마가 다르면 failure입니다.
  - 설정 검증 강화: `MultilineCsvParser`의 2글자 이상 Quote·1글자 구분자, `BulkOracleInsertProcessor`의
    Bulk Rows 0, `PutKudu`의 잘못된 타임스탬프 패턴 JSON(EL 미사용 시)은 invalid입니다.
- `MultilineCsvParser`: 문자셋 `CP949`를 Java 기본 해석(`x-IBM949`) 대신 `x-windows-949`로 해석합니다. 기존 설정을
  바꿀 필요는 없으며 `csv.input.charset` 값이 `x-windows-949`가 됩니다.
- `BulkOracleInsertProcessor`: 값을 PreparedStatement로 바인딩하고 스키마·테이블·컬럼명을 검증합니다(영문자로
  시작하는 영숫자·`_`·`$`·`#` 또는 큰따옴표 인용). 입력 연결이 필요합니다(`INPUT_REQUIRED`). decimal이 아닌
  bytes 값은 문자열 대신 `setBytes`로 바인딩합니다.
- `ExecuteFastSQL`: 성공하면 입력 FlowFile을 제거하고, 오류 시 penalize 후 `failure`로 보냅니다. BINARY·BLOB은
  Base64, CLOB은 내용 그대로 씁니다. 쿼리도 입력 연결도 없으면 시작을 거부합니다. 쿼리 콘텐츠는 UTF-8로 읽습니다.
- `VendorHive3ConnectionPool`: 벤더 드라이버가 없으면 Enable 단계에서 실패합니다. 클래스패스 설정이나 동적
  프로퍼티로 Kerberos가 켜져 있는데 Kerberos User Service가 없으면 invalid입니다.
- `SelectHive3QL`·`PutHive3QL`·`UpdateHive3Table`: `Hive3ConnectionPool`과 `VendorHive3ConnectionPool`을 모두 받습니다.
  `hive-normalize-avro=true`이면 Avro 컬럼명을 정규화합니다(Apache NiFi 1.28과 같은 동작).
- `MonitorMemoryUsageReportingTask`: 리포팅 간격 안에서는 알림을 반복하지 않고, 백분율 임계치를 max 힙 기준으로
  계산합니다. 임계치 초과는 두 Monitor 태스크 모두 WARN으로 남겨 bulletin이 생깁니다.
- `MonitorThreadReportingTask`·`HttpNotificationReportingTask` payload: `currentThreadCpuTime`·`currentThreadUserTime`을
  빼고 JVM 프로세스 전체 `processCpuTime`을 넣었습니다. 이 키를 읽는 수신 측은 변경이 필요합니다.
- `MergeParquet`: `@TriggerSerially`, 인스턴스별 클래스로더(`@RequiresInstanceClassLoading`)를 씁니다.
  `merge.target.qualifiedPath`는 스킴을 포함한 실제 경로입니다. 대상을 쓰지 않은 경우(매칭 0건) 대상 속성을 남기지 않습니다.
- parquet NAR이 업스트림 `org.apache.nifi.*` parquet 컴포넌트를 함께 노출하지 않습니다(argus 3종만 등록).
  업스트림 컴포넌트는 NiFi 기본 `nifi-parquet-nar`에서 제공됩니다.
- 배포 패키지(tar.gz/RPM)가 `conf/login-identity-providers.xml` 오버레이를 포함하여
  single-user 계정 `admin`을 기본값으로 설정. **공개된 기본값이므로 설치 후
  `nifi.sh set-single-user-credentials`로 반드시 교체해야 합니다** ([SECURITY.md](SECURITY.md) 참조).
  RPM에서는 `config|noreplace`로 지정되어 업그레이드 시 교체한 값이 보존됩니다.
- Apache NiFi에서 파생된 소스에 ASF 라이선스 헤더와 변경 고지를 복원하고, 각 NAR에
  파생 원본의 `META-INF/NOTICE`를 이관.
- `scripts/ssl/ssl-generate.sh`: 하드코딩된 비밀번호를 제거하고 `NIFI_SSL_PASSWORD`
  환경변수로 지정하도록 변경. 툴킷·출력 경로도 환경변수로 재정의 가능. 출력 디렉터리를
  비우기 전에 확인 프롬프트 추가(`NIFI_SSL_FORCE=1`로 생략).
- Maven wrapper가 Maven Central에서 배포판을 받도록 변경. 다른 미러가 필요하면
  `MVNW_REPOURL` 환경변수를 사용.

### Fixed
- **NiFi 2.x 오퍼레이터** (`operator/`) — k3s 실클러스터 통합 테스트로 확인한 결함 수정.
  - 기동: 오퍼레이터 이미지가 uid 1000의 passwd 항목이 없어 kopf가 기동 직후 죽던 문제, kopf가 요구하는
    CRD/네임스페이스 조회 권한이 ClusterRole에 없던 문제, init 컨테이너의 `cp -a`가 emptyDir에서 실패해
    NiFi Pod가 한 번도 뜨지 못하던 문제.
  - 설정: 이미지 `start.sh`가 병합된 nifi.properties를 덮어써(`cluster.is.node=false`, Curator 리더 선출,
    zk state provider, http+https 동시 지정 등) 클러스터가 구성되지 않던 문제 — `nifi.sh run`을 직접 실행.
    비-TLS에서 기본 keystore 경로 때문에 기동 실패, UI/headless Service가 비-TLS에서도 8443만 노출하던 문제,
    TLS에서 Jetty가 Pod IP에만 바인딩되어 `kubectl port-forward`가 안 되던 문제.
  - TLS 인가: `single-user-authorizer`가 그대로라 TLS 클러스터가 기동하지 못하던 문제, authorizers.xml이
    `Node Identity`를 사용자로 시드하지 않고 실제 인증서 DN과도 달라 노드 정책 시드가 실패하던 문제,
    Initial Admin(오퍼레이터)에게 root Process Group 정책이 없어 Flow를 만들 수 없던 문제.
  - 영속성: flow·FlowFile/Content/Provenance/DB 리포지토리·로컬 state·users/authorizations가 emptyDir에 있어
    Pod 재시작마다 사라지던 문제 — 모두 PVC로 이동.
  - 스케일링: Offload를 Disconnect 전에 요청해 항상 409로 실패하고도 Pod를 줄여 데이터가 유실될 수 있던 문제
    (Disconnect → Offload → Delete 순서, 실패 시 중단·재시도), 실제 응답 필드(`nodeId`)와 달라 노드 ID 조회가
    KeyError로 실패하던 문제, PDB가 replicas 변경을 따라가지 않던 문제.
  - 운영: 자동 복구가 관리자가 끊은 노드/축소 대상까지 재연결하던 문제, `Degraded` phase가 CRD enum에 없어
    status 갱신이 거부되던 문제, version/image/resources 변경이 무시되던 문제(노드 단위 롤링 재시작으로 반영),
    롤링 재시작이 노드 재조인을 기다리지 않아 전체 노드가 동시에 이탈하던 문제, TLS 클라이언트 인증서가 없을 때
    검증 없는 TLS로 대체하던 문제, CR 삭제 시 TLS Secret/Lease/state ConfigMap이 남던 문제.
  - NiFiFlow: Registry 클라이언트를 임의(첫 번째)로 고르던 문제, 재시도 시 Process Group이 중복 생성될 수 있던
    문제, 버전 변경 완료를 기다리지 않던 문제, 삭제 시 중지만 하고 Process Group을 지우지 않던 문제.
- `ExecuteFastSQL`: Fetch Size 경계마다 한 행씩 빠지던 문제, 기본값(Fetch Size 0)에서 결과가 0건이던 문제,
  여러 스레드가 ProcessSession을 동시에 사용하던 문제.
- `BulkOracleInsertProcessor`: 입력 연결을 받을 수 없게 선언되어 동작하지 않던 문제, 생성 SQL 끝의 `;` 때문에
  Oracle이 INSERT를 거부하던 문제, 배치 경계에서 레코드가 빠지던 문제, SQL 인젝션, NULL이 문자열로 들어가던 문제,
  Query Timeout이 1000배로 적용되던 문제, 스키마 없이 테이블명만 쓸 수 없던 문제.
- `MergeParquet`: 대상 파일이 원본 glob 안에 있으면 재실행 때 이전 병합 결과가 지워지던 문제, 병합 실패 시
  기존 대상과 원본이 사라지거나 깨진 파일이 남던 문제, 스키마가 다른 입력의 값이 뒤바뀌거나 컬럼이 빠지던 문제,
  파일마다 FileSystem 인스턴스가 누수되던 문제, REQUIRED·중첩 INT96 컬럼 타입이 바뀌던 문제, 파일명 `.`·`..`로
  디렉토리가 재귀 삭제될 수 있던 문제.
- `MultilineCsvParser`: Quote 보존 모드에서 값이 이중으로 감싸지던 문제, `||`처럼 자기 겹침 구분자에서 값이 잘못
  나뉘던 문제, `CP949`에서 확장 한글 8,822자가 깨지던 문제, EL 평가 오류 시 FlowFile이 무한 재시도되던 문제.
- `VendorHive3ConnectionPool`: Disable 후에도 Kerberos 상태가 남아 이전 principal로 접속하던 문제, KerberosUser를
  logout하지 않던 문제, DBCP evictor가 만드는 커넥션이 Kerberos Subject 밖에서 생성되던 문제.
- `TimestampFormat*RecordSetWriter`: `add-hours`가 `timestamp-micros` 필드에 1/1000만 적용되던 문제.
- `PutKudu`: 타임스탬프 패턴 JSON 파싱에 실패한 뒤에도 처리를 계속해 세션 오류가 나던 문제.
- 배포 tar.gz에 빌드 캐시의 런타임 산출물(생성된 keystore 등)이 포함될 수 있던 문제.
  조립에서 `conf/*.p12`·`logs/`·`work/` 등을 제외합니다.
- `scripts/ssl/ssl-generate.sh`가 `#!/bin/sh`로 선언되었으나 bash 전용 문법을 사용해
  dash에서 실패하던 문제.

## [2.10.0-1]

첫 통합 릴리스. 흩어져 있던 NiFi 확장·오퍼레이터·SSL 스크립트 저장소를 하나의 모노레포로
통합했습니다.

### Added
- **확장 번들** — standard / db / kudu / parquet / record-serialization / hive / reporting
  (기존 저장소에서 재편성), debezium CDC / deltalake (신규 구현), iceberg
  (Apache NiFi 1.28.0에서 포팅), flow-analysis 룰.
- **배포 패키징** — 공식 NiFi 2.10.0 바이너리에 Argus NAR과 conf 오버레이를 얹어
  tar.gz와 RPM 생성.
- **Kubernetes 오퍼레이터** — `NiFiCluster` / `NiFiFlow` 커스텀 리소스, 스케일링,
  헬스체크와 장애 노드 복구, 웹훅 알림.
- **Helm 차트** — 오퍼레이터 배포용.
- **컨테이너 이미지** — `apache/nifi:2.10.0` 기반에 Argus NAR 포함.
- **`nifi-config` CLI** — NiFi `conf/` 대화형 설정 도구.

[Unreleased]: https://github.com/DataDynamics-OSS/argus-flow/compare/v2.10.0-1...HEAD
[2.10.0-1]: https://github.com/DataDynamics-OSS/argus-flow/releases/tag/v2.10.0-1
