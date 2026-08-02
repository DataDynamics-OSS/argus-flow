# Changelog

이 프로젝트의 주요 변경 사항을 기록합니다.
형식은 [Keep a Changelog](https://keepachangelog.com/ko/1.1.0/)를 따릅니다.

버전은 `<NiFi 버전>-<릴리스 번호>` 형태입니다. 예를 들어 `2.10.0-1`은 Apache NiFi 2.10.0을
기반으로 한 첫 번째 릴리스를 뜻합니다. 기반 NiFi 버전이 올라가면 릴리스 번호는 1로 돌아갑니다.

## [Unreleased]

### Added
- Apache License 2.0으로 공개. `LICENSE`, `NOTICE`, `CONTRIBUTING.md`, `SECURITY.md` 추가.
- `apache-rat-plugin` 기반 라이선스 헤더 검사를 빌드(`validate` 단계)와 CI에 추가.

### Changed
- Apache NiFi에서 파생된 소스에 ASF 라이선스 헤더와 변경 고지를 복원하고, 각 NAR에
  파생 원본의 `META-INF/NOTICE`를 이관.
- `scripts/ssl/ssl-generate.sh`: 하드코딩된 비밀번호를 제거하고 `NIFI_SSL_PASSWORD`
  환경변수로 지정하도록 변경. 툴킷·출력 경로도 환경변수로 재정의 가능. 출력 디렉터리를
  비우기 전에 확인 프롬프트 추가(`NIFI_SSL_FORCE=1`로 생략).
- Maven wrapper가 Maven Central에서 배포판을 받도록 변경. 다른 미러가 필요하면
  `MVNW_REPOURL` 환경변수를 사용.

### Fixed
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
