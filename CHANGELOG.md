# Changelog

이 프로젝트의 주요 변경 사항을 기록합니다.
형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/)를 따릅니다.

버전은 `<NiFi 버전>-<릴리스 번호>` 형태입니다. 예를 들어 `2.10.0-1`은 Apache NiFi 2.10.0을
기반으로 한 첫 번째 릴리스를 뜻합니다. 기반 NiFi 버전이 올라가면 릴리스 번호는 1로 돌아갑니다.

## [Unreleased]

### Added
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
