# 기여 안내

Argus Flow에 기여해 주셔서 감사합니다. 이 문서는 개발 환경 구성, 라이선스 헤더 규약,
그리고 PR 절차를 설명합니다.

## 요구 사항

| 대상 | 필요한 것 |
|---|---|
| `nifi-extensions/`, `distribution/` | JDK 21+, Maven wrapper(`./mvnw`, 3.9.16 자동 설치) |
| `operator/`, `tools/nifi-config/` | Python 3.12+ |
| RPM 패키징 | [nfpm](https://nfpm.goreleaser.com) |
| 컨테이너 이미지 | docker |
| Helm 차트 | helm 3 |

NiFi 2.10 parent POM이 Maven 3.9.16+를 강제하므로 시스템 Maven 대신 동봉된
wrapper(`./mvnw`)를 사용하십시오. 사내 미러 등 다른 저장소에서 Maven 배포판을 받아야 한다면
파일을 고치지 말고 환경변수를 쓰십시오:

```bash
export MVNW_REPOURL=https://repo.example.com/maven2
```

## 빌드와 테스트

```bash
make extensions        # NAR 빌드 (= ./mvnw -B package)
make dist              # NiFi 재패키징 tar.gz (upstream 바이너리 다운로드 포함)
make rpm               # RPM (nfpm 필요)
make docker-image      # NAR 포함 NiFi 이미지
make operator-test     # 오퍼레이터 단위 테스트
make nifi-config-test  # conf 설정 CLI 단위 테스트
```

특정 모듈만 테스트하려면:

```bash
./mvnw -B -pl nifi-extensions/nifi-argus-db-bundle/nifi-argus-db-processors test
```

## 라이선스 헤더 규약 — 중요

이 저장소는 Apache License 2.0으로 배포되며, `nifi-extensions/`의 상당수 소스가
**Apache NiFi에서 파생**된 것입니다. Apache-2.0 §4(b)(c)(d)는 파생물 배포 시 원본의 고지를
유지하고 변경 사실을 밝힐 것을 요구합니다. 따라서 모든 소스 파일은 헤더를 가져야 하며,
`apache-rat-plugin`이 빌드(`validate` 단계)에서 이를 강제합니다.

### 새 파일을 추가할 때

**자체 작성한 파일**은 Data Dynamics 헤더를 붙입니다:

```java
/*
 * Copyright 2026 Data Dynamics Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * ... (전문은 기존 파일 참조)
 */
```

**Apache NiFi에서 가져온 파일**은 원본의 ASF 헤더를 **그대로 유지**하고, 그 아래에 변경
고지를 덧붙입니다. 출처 파일 경로를 반드시 명시하십시오:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * ... (업스트림 원문 그대로) ...
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-kudu-bundle/.../PutKudu.java
 */
```

### 업스트림 소스를 가져오는 방법

포팅 소스는 **`apache/nifi` 공식 저장소에서만** 가져옵니다. 벤더 배포판 저장소나 출처가
불명확한 파생 소스는 단 한 줄도 복사·참조하지 마십시오.

```bash
./scripts/fetch-upstream.sh    # rel/nifi-1.28.0 을 커밋 해시까지 검증하고 받는다
```

업스트림에 없는 기능이 필요하면 **원본 코드를 보지 않고** 공개 API 문서를 기반으로 새로
구현하십시오.

### 새 NAR 번들을 추가할 때

`src/main/resources/META-INF/NOTICE`를 반드시 두십시오. 업스트림 번들에서 파생했다면 그
번들의 NOTICE를 이관하고 앞에 파생 사실을 밝힙니다. 자체 구현이라면 저작권과 주요 제3자
구성요소를 적습니다. 기존 번들의 NOTICE를 참고하십시오.

### 설정 도구(`tools/nifi-config`)를 바꿀 때

개발 중에는 pip 로 설치해 씁니다(`make nifi-config`). 배포본에는 같은 코드가 zipapp
(`.pyz`)으로 들어가므로 **두 경로가 같은 소스를 씁니다.**

- 의존성을 추가·변경하면 `pyproject.toml` 과 `requirements-pyz.txt` 를 **함께** 고치십시오.
  후자는 해시까지 고정하며 `--require-hashes` 로 설치하므로, 목록에 없는 전이 의존성이
  생기면 빌드가 실패합니다. 갱신 절차는 그 파일 주석에 있습니다.
- **순수 Python 패키지만 쓸 수 있습니다.** 네이티브 확장이 섞이면 아키텍처마다 다른
  산출물이 필요해져 단일 파일 배포가 성립하지 않습니다.
- 대화형 동작을 바꿨다면 zipapp 으로도 확인하십시오. 파이프로는 검증되지 않습니다 —
  questionary 는 실제 터미널을 요구하므로 pty 로 구동해야 합니다.

```bash
scripts/build-config-pyz.sh /tmp/argus-config.pyz 2.10.0-1
python3 /tmp/argus-config.pyz --conf-dir <conf> --get nifi.web.https.port
```

### DB 스키마를 바꿀 때

`nifi-argus-db-iaa` 확장의 스키마 정본은
`nifi-extensions/nifi-argus-db-iaa-bundle/nifi-argus-db-iaa-providers/src/main/resources/db/`
에 있습니다. 배포본의 `sql/db-iaa/`는 여기서 복사됩니다.

- **방언 세 벌을 모두 고치십시오** — `postgresql`, `mariadb`, `h2`. h2는 테스트 전용이지만
  단위 테스트가 이 파일을 그대로 적용하므로 어긋나면 테스트가 먼저 깨집니다.
- **DDL은 멱등이어야 합니다.** `argus-user.sh schema-init`은 여러 번 실행될 수 있습니다.
- 스키마를 바꾸면 `V<n+1>__<설명>.sql`을 추가하고 `SchemaManager.REQUIRED_VERSION`을
  올리십시오. 기존 `V1__baseline.sql`은 고치지 마십시오 — 이미 적용된 설치본이 있습니다.
- 방언별 함정이 있습니다. MariaDB는 `ENGINE=InnoDB`가 없으면 FK가 생기지 않고, 기본
  collation이 대소문자를 구분하지 않아 `utf8mb4_bin`이 필요하며, `TIMESTAMP` 컬럼에는
  암묵적 `ON UPDATE CURRENT_TIMESTAMP`가 붙습니다. 실제 엔진으로 확인하십시오.

## 코드 스타일

- Java 21, 들여쓰기 4칸. 기존 파일의 스타일을 따릅니다.
- 주석과 NiFi UI에 노출되는 속성 레이블·설명은 한국어로 작성합니다.
- Python은 `ruff` 기본 규칙을 따릅니다.

## 커밋과 PR

- 브랜치는 `main`에서 따고, 커밋 메시지는 `<scope>: <요약>` 형태로 씁니다
  (예: `hive: fix connection pool validation`). 본문에 **왜** 바꿨는지를 적으십시오.
- PR을 열기 전에 로컬에서 빌드와 테스트가 통과하는지 확인하십시오.
- PR 템플릿의 체크리스트, 특히 **라이선스 헤더 항목**을 확인해 주십시오.
- 보안 취약점은 이슈로 올리지 말고 [SECURITY.md](SECURITY.md)의 절차를 따라 주십시오.

## 라이선스

기여하신 코드는 [Apache License 2.0](LICENSE)으로 배포됩니다. PR을 제출하는 것은 해당
라이선스로 기여물을 제공하는 데 동의하는 것으로 간주합니다.
