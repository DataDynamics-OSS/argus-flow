# Argus Flow

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-2.10.0-green.svg)](https://nifi.apache.org/)

Argus Flow는 Apache NiFi 2.10.0을 실제 운영 환경에 올리는 데 필요한 것들을 한곳에 모은
모노레포입니다. 커스텀 확장 번들(NAR)과 배포 패키지(tar.gz, RPM, 컨테이너 이미지),
그리고 Kubernetes 오퍼레이터를 함께 담고 있습니다.

원래는 NiFi 확장, 오퍼레이터, 인증서 생성 스크립트가 각각 다른 저장소에 흩어져 있었습니다.
버전을 서로 맞추기 어렵고 릴리스 절차도 제각각이었기 때문에, 하나의 빌드 파이프라인에서
모든 산출물을 만들어 내도록 통합했습니다.

## 구성

저장소는 산출물의 종류에 따라 다음과 같이 나뉩니다. 각 디렉터리는 독립적으로 빌드할 수
있으며, 서로 다른 도구 체인을 사용합니다.

| 경로 | 내용 | 빌드 |
|---|---|---|
| `nifi-extensions/` | NiFi 확장 번들 — 레거시 재편성(standard / db / kudu / parquet / record-serialization / hive / reporting) + 신규·포팅(debezium CDC / deltalake / iceberg) | Maven, Java 21 |
| `distribution/` | 공식 NiFi 바이너리 재패키징 → tar.gz, RPM | Maven `-Pdist`, nfpm |
| `operator/` | NiFi 2.x K8s 오퍼레이터 (Python, kopf) | pip + pytest |
| `charts/` | Helm 차트 | helm |
| `docker/` | NAR 포함 NiFi 이미지 | docker |
| `tools/nifi-config/` | NiFi `conf/` 대화형 설정 CLI (Python, questionary) | pip + pytest |
| `scripts/ssl/` | 인증서 생성 스크립트 | — |

확장은 도메인별 번들로 나뉘고 번들마다 독립된 NAR로 패키징되므로, 서드파티 의존성이
번들 경계를 넘어 충돌하지 않습니다. 현재 11개 번들에서 17개의 NAR이 만들어집니다.

## 빌드

빌드는 모두 `make` 타깃으로 감싸 두었습니다. 자주 쓰는 것들은 다음과 같습니다.

```bash
make extensions      # 확장 NAR 빌드 (= mvn -B package)
make dist            # NiFi 재패키징 tar.gz (upstream 바이너리 다운로드 포함)
make rpm             # RPM (nfpm 필요)
make docker-image    # NAR 포함 NiFi 이미지
make operator-test   # 오퍼레이터 단위 테스트
make nifi-config-test  # conf 설정 CLI 단위 테스트
```

`conf/` 설정 CLI는 대화형으로도 실행할 수 있습니다. 설치 루트나 `conf` 디렉터리 중 편한
쪽을 지정하면 됩니다.

```bash
make nifi-config NIFI_HOME=/opt/nifi     # 또는 CONF=/opt/nifi/conf
```

빌드에는 JDK 21 이상이 필요하고, 오퍼레이터와 `nifi-config`를 다루려면 Python 3.12 이상이
있어야 합니다. RPM을 만들려면 nfpm이, 컨테이너 이미지를 만들려면 docker가 추가로
필요합니다. Maven은 시스템에 설치된 것 대신 동봉된 wrapper(`./mvnw`)를 사용하십시오.
NiFi 2.10 parent POM이 Maven 3.9.16 이상을 요구하기 때문에, wrapper가 해당 버전을 받아
고정해 줍니다.

## ⚠️ 운영 환경 배포 전 확인

이 저장소에 들어 있는 기본값은 개발과 시연을 위한 것입니다. 그대로 운영에 올리면 안 됩니다.
TLS 키스토어 비밀번호, NiFi 관리자 자격증명, 민감 속성 암호화 키
(`nifi.sensitive.props.key`), 오퍼레이터의 RBAC 범위는 배포 전에 반드시 교체하십시오.
무엇을 어떻게 바꿔야 하는지는 [SECURITY.md](SECURITY.md)에 항목별로 정리해 두었습니다.

## Apache NiFi와의 관계

`nifi-extensions/`의 여러 번들은 Apache NiFi 1.28.0의 소스를 가져와 NiFi 2.10용으로 개작한
것입니다. standard, db, kudu, parquet, record-serialization, hive, reporting, iceberg가
여기에 해당합니다. 이렇게 파생된 파일은 원본의 ASF 라이선스 헤더를 그대로 유지하고, 어느
업스트림 파일에서 왔는지 밝힌 변경 고지를 덧붙였습니다. 각 NAR에도 파생 원본 번들의
`META-INF/NOTICE`를 함께 담았습니다. 반면 debezium, deltalake, flow-analysis 번들은
업스트림에 대응하는 원본 없이 새로 구현한 것입니다. 전체 귀속 내역은 [NOTICE](NOTICE)에
정리되어 있습니다.

`distribution/`은 공식 NiFi 바이너리를 내려받아 재패키징하고, 컨테이너 이미지는 공식
`apache/nifi` 이미지를 베이스로 사용합니다. 어느 쪽도 이 저장소에 벤더링하지 않고 빌드
시점에 버전을 고정해 가져옵니다.

Cloudera JDBC 드라이버는 이 저장소에 포함되어 있지 않습니다. `VendorHive3ConnectionPool`을
사용하려면 드라이버를 직접 받아 NiFi의 `lib/` 디렉터리에 배치해야 하며, 해당 드라이버는
벤더가 정한 라이선스를 따릅니다.

## 기여 · 라이선스

기여하실 때는 [CONTRIBUTING.md](CONTRIBUTING.md)를 먼저 읽어 주십시오. 빌드 환경과 PR 절차
외에, 이 저장소에서 특히 중요한 **라이선스 헤더 규약**을 설명하고 있습니다. 업스트림에서
가져온 파일과 새로 작성한 파일에 각각 어떤 헤더를 붙여야 하는지가 정해져 있고, 빌드 단계의
`apache-rat-plugin`이 이를 검사합니다.

보안 취약점을 발견하셨다면 공개 이슈로 올리지 마시고 [SECURITY.md](SECURITY.md)에 안내된
비공개 제보 절차를 이용해 주십시오.

이 프로젝트는 [Apache License 2.0](LICENSE)으로 배포됩니다.
