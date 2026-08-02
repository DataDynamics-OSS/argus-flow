# argus-nifi-config

NiFi `conf/` 디렉터리를 **대화형(TUI)** 으로 설정하는 CLI. 200줄이 넘는
`nifi.properties`를 손으로 편집하는 대신, 그룹별 마법사와 검색으로 값을 골라 입력한다.
Claude CLI처럼 화살표 선택 + 검증 입력으로 동작한다.

## 대상 파일

| 파일 | 방식 |
|---|---|
| `nifi.properties` | key=value 큐레이션 마법사 + 전체 키 검색/편집 |
| `bootstrap.conf` | JVM 힙·프로세스 설정 마법사 |
| `authorizers.xml` | provider/property 직접 편집 + 레시피 |
| `login-identity-providers.xml` | 〃 (single-user / LDAP 레시피) |
| `state-management.xml` | 〃 (ZooKeeper / Kubernetes 레시피) |

- **원본은 보존**: 기존 키는 제자리에서 값만 치환하고 upstream 주석/순서를 유지한다.
  카탈로그에 없는 키를 새로 넣으면 파일 끝 관리 섹션에 추가한다.
- **XML도 주석 보존**: provider의 `<property>` 값만 바꾸고 설명 주석은 유지한다.
- **파일 간 정합성**: 레시피는 XML과 `nifi.properties`의 연관 키를 함께 설정한다.

## 배포본에서 실행 (설치 불필요)

tar.gz·RPM 에는 이 도구가 zipapp 으로 포함되어 있습니다. pip 설치 없이 바로 쓸 수 있습니다.

```bash
<NIFI_HOME>/bin/argus-config.sh              # 대화형
<NIFI_HOME>/bin/argus-config.sh --list
```

`NIFI_HOME` 은 스크립트 위치에서 자동으로 정해집니다. 다른 설치본을 대상으로 하려면
`--nifi-home` 또는 `--conf-dir` 을 주면 그쪽이 우선합니다.
python3 3.10 이상이 필요하고, 다른 경로의 python 을 쓰려면 `ARGUS_PYTHON` 을 지정합니다.

## 설치 (소스에서 개발할 때)

```bash
cd tools/nifi-config
python3 -m venv .venv
.venv/bin/pip install -e .
```

## 사용

### 대화형 (기본)

```bash
# 실제 설치본을 제자리 편집
argus-nifi-config --nifi-home /opt/nifi

# conf 디렉터리 직접 지정
argus-nifi-config --conf-dir /opt/nifi/conf

# 원본은 그대로 두고, 바뀐 파일만 오버레이 디렉터리에 출력
argus-nifi-config --conf-dir /opt/nifi/conf --out ./my-overlay
```

메뉴: 가이드 마법사 · 전체 키 검색/편집 · XML 레시피 · XML provider 직접 편집 · 저장(미리보기).

### 비대화형 (스크립트/자동화/CI)

```bash
# 값 설정 (검증 후 저장; --dry-run 으로 diff만 확인)
argus-nifi-config --conf-dir CONF --set nifi.web.https.port=9443 \
                                  --set nifi.cluster.is.node=true

# 레시피 적용
argus-nifi-config --conf-dir CONF --recipe state:zookeeper \
                  --param connect_string=zk1:2181,zk2:2181
argus-nifi-config --conf-dir CONF --recipe authorizers:file-admin \
                  --param "admin_identity=CN=admin, OU=NiFi"

# 조회
argus-nifi-config --list [--file nifi.properties] [--conf-dir CONF]
argus-nifi-config --describe nifi.sensitive.props.key
argus-nifi-config --conf-dir CONF --get nifi.web.https.port
argus-nifi-config --conf-dir CONF --search sensitive
```

레시피 ID: `authorizers:file-admin`, `login:single-user`, `login:ldap`,
`state:zookeeper`, `state:kubernetes`.

## 활용 시나리오

- **운영 설치본 조정**: `--nifi-home` 으로 제자리 편집.
- **배포 표준 오버레이 작성**: `--out distribution/src/main/resources/conf` 로 바뀐 파일만
  생성 → `dist.xml` excludes + `nfpm.yaml` config 엔트리에 추가(README 참고).

## 테스트

```bash
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest -q
```
