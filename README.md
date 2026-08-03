# Argus Flow

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Apache NiFi](https://img.shields.io/badge/Apache%20NiFi-2.10.0-green.svg)](https://nifi.apache.org/)

Argus Flow는 Apache NiFi 2을 실제 운영 환경에 올리는 데 필요한 것들을 한곳에 모은
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
| `nifi-extensions/` | NiFi 확장 번들 — 레거시 재편성(standard / db / kudu / parquet / record-serialization / hive / reporting) + 신규·포팅(debezium CDC / deltalake / iceberg / db-iaa) | Maven, Java 21 |
| `distribution/` | 공식 NiFi 바이너리 재패키징 → tar.gz, RPM | Maven `-Pdist`, nfpm |
| `operator/` | NiFi 2.x K8s 오퍼레이터 (Python, kopf) | pip + pytest |
| `charts/` | Helm 차트 | helm |
| `docker/` | NAR 포함 NiFi 이미지 | docker |
| `tools/nifi-config/` | NiFi `conf/` 대화형 설정 도구 (Python, questionary) — 배포본에도 포함 | pip + pytest |
| `scripts/ssl/` | 인증서 생성 스크립트 | — |

확장은 도메인별 번들로 나뉘고 번들마다 독립된 NAR로 패키징되므로, 서드파티 의존성이
번들 경계를 넘어 충돌하지 않습니다. 현재 12개 번들에서 18개의 NAR이 만들어집니다.

`db-iaa` 번들은 성격이 조금 다릅니다. 프로세서가 아니라 **NiFi의 인증·인가 프로바이더**로,
사용자와 비밀번호를 RDB에서 관리합니다. LDAP도 외부 IdP도 도입할 수 없는 환경에서
`users.xml`을 손으로 고치는 대신 쓰라고 만든 것입니다. 기본값은 비활성(설정 파일에 주석
처리)이며, 사용자 관리는 `bin/argus-user.sh`로 합니다.

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

## 설치본 설정

배포본에는 설정과 운영을 위한 도구 두 개가 함께 들어갑니다. 둘 다 `NIFI_HOME` 을 자동으로
인식하므로 경로를 입력할 필요가 없습니다.

```bash
./bin/argus-config.sh          # conf/ 설정 (대화형 TUI)
./bin/argus-config.sh --check  # 설정 진단
./bin/argus-ssl.sh             # TLS 인증서 생성 (보통은 argus-config.sh 가 안내)
./bin/argus-user.sh --help     # DB 인증을 쓸 때의 사용자 관리
```

`argus-config.sh` 는 200줄이 넘는 `nifi.properties` 와 XML 을 직접 편집하는 대신, 마법사와
검색으로 값을 고르게 합니다. 원본의 주석과 순서는 보존되고, 저장 전에 diff 를 보여줍니다.

처음 설치했다면 메뉴의 **처음 설정하기**로 시작하십시오. 접속 주소 → TLS 인증서 → 로그인
방식을 순서대로 안내하며, **접속 주소를 한 번만 받아 인증서 SAN 에 그대로 씁니다.** 이 둘이
어긋나면 NiFi 가 `Invalid SNI` 로 접속을 거절하는데, 두 곳에 따로 입력하면 어긋나기 쉽습니다.

설정이 서로 맞는지 진단할 수도 있습니다:

```bash
./bin/argus-config.sh --check
```

인증서 SAN 과 바인드 주소의 불일치, IP 접속 불가, 만료 임박, 로그인 프로바이더가 주석 상태인
경우, DB 인증에서 인증·인가가 서로 다른 DB 를 보는 경우를 잡습니다. 문제가 있으면 종료 코드
1 을 돌려주므로 배포 스크립트에 넣을 수 있습니다(경고만 있으면 0).

자동화에는 비대화형 모드를 씁니다:

```bash
./bin/argus-config.sh --set nifi.web.https.port=9443
./bin/argus-config.sh --recipe tls:generate --param hosts=nifi1.example.com --param ips=10.0.0.5
./bin/argus-config.sh --recipe login:db --param db_url=jdbc:postgresql://db:5432/nifi ...
```

비대화형에서는 인증서 생성 같은 외부 명령을 **실행하지 않고 출력만** 합니다. CI 가 인증서를
의도치 않게 재발급하는 것을 막기 위해서입니다.

python3 3.10 이상이 필요합니다. 없으면 도구만 동작하지 않고 NiFi 자체에는 영향이 없습니다.

## ⚠️ 운영 환경 배포 전 확인

이 저장소에 들어 있는 기본값은 개발과 시연을 위한 것입니다. 그대로 운영에 올리면 안 됩니다.
TLS 키스토어 비밀번호, NiFi 관리자 자격증명, 민감 속성 암호화 키
(`nifi.sensitive.props.key`), 오퍼레이터의 RBAC 범위는 배포 전에 반드시 교체하십시오.
무엇을 어떻게 바꿔야 하는지는 [SECURITY.md](SECURITY.md)에 항목별로 정리해 두었습니다.

## Apache NiFi와의 관계

`nifi-extensions/`의 여러 번들은 Apache NiFi의 구버전과 Data Dynamics에서 자체 갭라한 소스코드를 NiFi 2.x용으로 개발한
것입니다. 

그리고 `distribution/`은 공식 NiFi 바이너리를 내려받아 재패키징하고, 컨테이너 이미지는 공식
`apache/nifi` 이미지를 베이스로 사용합니다. 어느 쪽도 이 저장소에 벤더링하지 않고 빌드
시점에 버전을 고정해 가져옵니다.

Cloudera JDBC 드라이버는 이 저장소에 포함되어 있지 않습니다. `VendorHive3ConnectionPool`을
사용하려면 드라이버를 직접 받아 NiFi의 `lib/` 디렉터리에 배치해야 하며, 해당 드라이버는
벤더가 정한 라이선스를 따릅니다.

같은 이유로 **MariaDB Connector/J도 포함하지 않습니다.** `db-iaa` 확장은 MariaDB를
지원하지만 드라이버가 LGPL-2.1이라 재배포하지 않습니다. 필요하면 직접 받아
`Database Driver Location` 속성으로 경로를 지정하십시오. PostgreSQL 드라이버(BSD)는
번들되어 있습니다.

## 기여 · 라이선스

기여하실 때는 [CONTRIBUTING.md](CONTRIBUTING.md)를 먼저 읽어 주십시오. 빌드 환경과 PR 절차
외에, 이 저장소에서 특히 중요한 **라이선스 헤더 규약**을 설명하고 있습니다. 업스트림에서
가져온 파일과 새로 작성한 파일에 각각 어떤 헤더를 붙여야 하는지가 정해져 있고, 빌드 단계의
`apache-rat-plugin`이 이를 검사합니다.

보안 취약점을 발견하셨다면 공개 이슈로 올리지 마시고 [SECURITY.md](SECURITY.md)에 안내된
비공개 제보 절차를 이용해 주십시오.

이 프로젝트는 [Apache License 2.0](LICENSE)으로 배포됩니다.
