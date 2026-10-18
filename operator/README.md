# NiFi 2.x Kubernetes Operator

Python과 [kopf](https://kopf.readthedocs.io/)로 구현한 Apache NiFi 2.x 클러스터 Kubernetes Operator입니다.

## 주요 기능

- **NiFi 2.x 네이티브** — ZooKeeper 없이 Kubernetes Lease 리더 선출(`KubernetesLeaderElectionManager`)과 ConfigMap 상태 저장(`KubernetesConfigMapStateProvider`) 사용
- **선언적 관리** — NiFiCluster 커스텀 리소스(CR)로 클러스터를 선언적으로 정의
- **자동 클러스터 구성** — Kubernetes Headless Service DNS와 Lease로 노드가 스스로 조인
- **영속성** — flow 정의, FlowFile/Content/Provenance/DB 리포지토리, 로컬 state, users.xml/authorizations.xml을 노드별 PVC에 저장 (Pod 재시작 후에도 유지)
- **안전한 스케일링** — Scale Down 시 Disconnect → Offload → Remove 순서로 데이터를 남은 노드로 이전, 실패 시 중단
- **무중단 업데이트** — 설정/버전/이미지/리소스 변경 시 노드를 한 대씩 재시작하고 재조인을 확인한 뒤 다음 노드 진행
- **TLS 자동화** — cert-manager로 공유 서버 인증서(headless 와일드카드 SAN)와 오퍼레이터 mTLS 클라이언트 인증서 발급
- **Flow 배포** — NiFiFlow CR로 NiFi Registry의 Flow를 GitOps 방식으로 배포/버전 변경/삭제
- **자동 복구** — 주기적 헬스체크로 연결 끊긴 노드 자동 재연결 (관리자가 끊은 노드/축소 대상 제외)
- **알림** — 클러스터가 비정상(Degraded/Error)으로 전이하면 `spec.notification.webhookUrl`로 웹훅 전송

## 아키텍처

```
사용자가 NiFiCluster CR 적용
         │
         ▼
┌─────────────────────┐
│   NiFi Operator     │  ← Python (kopf)
│   (Deployment)      │
└────────┬────────────┘
         │ 감시 및 조정 (Reconcile)
         ▼
┌─────────────────────┐    ┌─────────────────┐    ┌──────────────┐
│  StatefulSet        │    │ Headless Service │    │  ConfigMap   │
│  (NiFi 노드 Pod)    │◄──│ (노드 디스커버리)  │    │ (nifi.props) │
└─────────────────────┘    └─────────────────┘    └──────────────┘
```

## 빠른 시작

### 사전 요구사항

- Kubernetes 클러스터 (1.27 이상)
- Python 3.11 이상
- kubectl 설정 완료
- (선택) cert-manager — TLS 사용 시 (멀티노드는 TLS 필수)
- (선택) NiFi Registry — Flow 관리 시

### 1. CRD 설치

```bash
kubectl apply -f deploy/crds/
```

### 2. Operator 로컬 실행 (개발 모드)

```bash
pip install -r requirements.txt
kopf run nifi_operator/main.py --verbose
```

### 3. NiFi 클러스터 생성

```bash
kubectl create namespace nifi
kubectl apply -f examples/nifi-cluster-simple.yaml   # 단일 노드, TLS 없음 (개발용)
```

> 오퍼레이터는 노드의 Pod FQDN(`*.svc.cluster.local`)으로 NiFi REST API를 호출하므로
> 클러스터 **내부**에서 실행해야 한다. `kopf run`(로컬 실행)은 클러스터 DNS에 접근할 수 있는
> 환경에서만 동작한다.

### 4. 상태 확인

```bash
kubectl get nifi -n nifi
# NAME      REPLICAS   READY   PHASE     VERSION   AGE
# my-nifi   1          1       Running   2.10.0    3m

kubectl get pods -n nifi
# NAME              READY   STATUS    RESTARTS   AGE
# my-nifi-nifi-0    1/1     Running   0          3m
```

`status.phase`: `Creating`(노드 기동/조인 대기) → `Running`(spec.replicas 노드가 모두 CONNECTED).
진행 중에는 `Scaling`/`Updating`, 일부 노드가 끊기면 `Degraded`, API 연결 실패 시 `Error`.

### 5. NiFi UI 접속

```bash
# 비-TLS (HTTP 8080, 인증 없음)
kubectl port-forward svc/my-nifi-ui -n nifi 8080:8080
# http://localhost:8080/nifi

# TLS 클러스터 (HTTPS 8443, 클라이언트 인증서 필수)
kubectl port-forward svc/nifi-secure-ui -n nifi 8443:8443
# https://localhost:8443/nifi — 브라우저/클라이언트에 <name>-operator-tls Secret의
# tls.crt/tls.key(또는 tls.initialAdminIdentity에 해당하는 인증서)를 등록해야 한다.
```

NiFi는 Host 헤더를 검사한다. 로컬 포트를 8443이 아닌 값으로 포워딩하면
`spec.config.webProxyHost`에 `localhost:<로컬포트>`를 추가해야 한다(421 Invalid Port 방지).

## 프로덕션 배포

### Docker 이미지 빌드 및 Push

```bash
make build IMAGE_NAME=your-registry/nifi-operator IMAGE_TAG=v0.1.0
make push IMAGE_NAME=your-registry/nifi-operator IMAGE_TAG=v0.1.0
```

### 클러스터에 Operator 배포

```bash
# deploy/operator-deployment.yaml의 이미지를 수정한 뒤
kubectl create namespace nifi-operator-system
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/operator-deployment.yaml
```

- 이미지 기본 실행 인자는 `--all-namespaces`(Dockerfile `CMD`)다. 특정 네임스페이스만 감시하려면
  Deployment에 `args: ["--namespace=<ns>", "--standalone"]`를 지정한다.
- kopf는 감시 범위와 무관하게 시작 시 CRD 목록과 네임스페이스 목록을 클러스터 범위로 조회하므로
  `deploy/rbac.yaml`의 ClusterRole에 `customresourcedefinitions`/`namespaces` 읽기 권한이 있다.
- k3s 등 실제 클러스터에서 네임스페이스 한정으로 통합 테스트하는 매니페스트와 절차는
  `test/k3s/`를 참고한다.

## 커스텀 리소스 (CR)

### NiFiCluster — NiFi 클러스터 정의

```yaml
apiVersion: datadynamics.io/v1alpha1
kind: NiFiCluster
metadata:
  name: my-nifi
spec:
  replicas: 3                              # 클러스터 노드 수 (2 이상이면 tls.enabled 필수)
  version: "2.10.0"                         # NiFi 이미지 태그
  resources:
    requests: { cpu: "1", memory: "2Gi" }   # 리소스 요청
    limits:   { cpu: "2", memory: "4Gi" }   # 리소스 제한
  storage:
    size: "20Gi"                            # 노드당 PVC 크기 (생성 후 변경 불가)
  config:
    jvmHeapSize: "1536m"                    # JVM 힙 크기
    webProxyHost: "nifi.example.com"        # 외부 프록시/Ingress 호스트 (쉼표 구분)
  tls:
    enabled: true                           # TLS 활성화 (생성 후 변경 불가)
    issuerRef:
      name: internal-ca                     # 와일드카드 SAN을 발급할 수 있는 사설 CA Issuer
      kind: ClusterIssuer
  notification:
    webhookUrl: "https://hooks.example.com/nifi"   # (선택) 비정상 전이 알림
```

- `spec.auth`는 CRD에 정의되어 있지만 **아직 구현되지 않았다**. TLS 클러스터의 인증은
  클라이언트 인증서(mTLS)만 지원한다(`nifi.security.user.authorizer=managed-authorizer`,
  login identity provider 없음). Initial Admin은 `CN=<name>-operator`(오퍼레이터 인증서) 또는
  `tls.initialAdminIdentity`.
- TLS 클러스터가 처음 `Running`이 되면 오퍼레이터가 root Process Group 정책
  (관리자: 컴포넌트/데이터 read·write, operation write / 노드: 데이터 read·write)을 1회 부여한다
  (`status.rootPoliciesApplied`). NiFi는 최초 기동 시 flow가 없으면 이 정책을 시드하지 않는다.

### NiFiFlow — Flow 배포 정의

```yaml
apiVersion: datadynamics.io/v1alpha1
kind: NiFiFlow
metadata:
  name: etl-pipeline
spec:
  clusterRef: my-nifi                       # 대상 NiFiCluster
  registryUrl: "https://nifi-registry.example.com"   # 사용할 Registry 클라이언트의 url 속성
  bucketId: "..."                           # Registry 버킷 ID
  flowId: "..."                             # Flow ID
  flowVersion: 3                            # 배포할 버전
  autoStart: true                           # 배포 후 자동 시작
```

- NiFi에 Registry 클라이언트(`NifiRegistryFlowRegistryClient`)가 미리 등록되어 있어야 한다.
  `registryUrl`과 url 속성이 같은 클라이언트를 사용하며, `registryUrl`을 생략하면 클라이언트가
  정확히 하나일 때만 사용한다.
- 생성은 재시도에 안전하다(이미 배포한 PG 또는 같은 registry/bucket/flow로 버전 관리되는
  미관리 PG를 재사용). 버전 변경은 NiFi update-request 완료까지 기다린다.
- CR 삭제 시 Process Group을 중지 → Controller Service 비활성화 → 삭제한다. 큐에 FlowFile이
  남아 있으면 NiFi가 삭제를 거부하므로 데이터를 버리지 않고 재시도한다(큐를 비우면 완료).

## 동작 방식

### 설정 렌더링과 실행

- init 컨테이너(`merge-config`)가 이미지 기본 `conf/`를 emptyDir로 복사하고,
  ConfigMap `<name>-config`의 `nifi-overrides.properties`를 병합한다(비밀값은 Secret env로 치환).
  JVM 힙(`bootstrap.conf`), 로컬 state 디렉터리, 클러스터 state ConfigMap 접두어도 여기서 설정한다.
- 메인 컨테이너는 이미지 ENTRYPOINT(`/opt/nifi/scripts/start.sh`)를 쓰지 않고
  `start-nifi.sh`로 `bin/nifi.sh run`을 직접 실행한다. `start.sh`는 기동 직전에 nifi.properties를
  환경변수 기본값으로 덮어써서(`nifi.cluster.is.node=false`, `https.port=8443`, Curator 리더 선출 등)
  병합된 설정을 무효화하기 때문이다. `nifi-app.log`는 stdout으로 나가고, SIGTERM 시
  `nifi.sh stop`으로 정상 종료한다(`terminationGracePeriodSeconds: 120`).
- 영속 데이터는 PVC(`/opt/nifi/nifi-current/data`) 아래에 둔다: `flow/flow.json.gz`,
  `flowfile_repository`, `content_repository`, `provenance_repository`, `database_repository`,
  `state/local`, `conf/users.xml`, `conf/authorizations.xml`, `nar_repository`, `assets`.

### 스케일링

```bash
# 스케일 업 (3 → 5) — 새 노드가 스스로 조인, 기존 노드 재시작/인증서 재발급 없음
kubectl patch nifi my-nifi -n nifi --type merge -p '{"spec":{"replicas":5}}'

# 스케일 다운 (5 → 3) — 높은 ordinal부터 한 노드씩 안전하게 제거
kubectl patch nifi my-nifi -n nifi --type merge -p '{"spec":{"replicas":3}}'
```

- 스케일 다운은 노드마다 `DISCONNECTING`(→DISCONNECTED 대기) → `OFFLOADING`(→OFFLOADED 대기, FlowFile을
  남은 노드로 이전) → 클러스터에서 DELETE → StatefulSet replicas 1 감소 순서로 진행한다.
  NiFi는 연결된 노드의 오프로드를 거부한다(409 "not currently disconnected").
- 어느 단계든 실패/타임아웃이면 그 노드의 Pod는 줄이지 않고 `status.phase=Error`와 사유를 남긴 뒤
  60초 후 재시도한다(이미 제거한 노드는 건너뜀).
- PodDisruptionBudget(minAvailable=과반)은 replicas 변경 시 함께 갱신되고, 1노드가 되면 삭제된다.
- 제거된 노드의 PVC(`data-<name>-nifi-N`)는 StatefulSet 기본 정책에 따라 남는다.

### 변경 반영

| spec 필드 | 동작 |
|-----------|------|
| `replicas` | 스케일 업/다운 (위 참조) |
| `version`, `image`, `resources`, `config` | ConfigMap·Pod 템플릿 갱신 후 높은 ordinal부터 한 노드씩 재시작, 각 노드 재조인 확인 후 다음 노드 |
| `storage`, `tls` | 생성 후 변경 미지원 — 적용하지 않고 `status.message`와 `UnsupportedChange` Event로 알림 |
| `notification` | 다음 알림부터 반영 |

Pod 템플릿에는 ConfigMap 내용 해시(`nifi-operator/config-hash`)가 들어가므로, 설정이 같으면
재적용/재시도해도 재시작하지 않는다.

### 자동 복구

60초마다 DISCONNECTED 노드에 재연결(`CONNECTING`)을 요청한다. 다음은 제외한다:
관리자가 UI/API로 직접 연결 해제한 노드(최근 노드 이벤트가 "User ... requested that node be
disconnected"), ordinal ≥ `spec.replicas`인 노드, OFFLOADING/OFFLOADED 노드, `Scaling`/`Updating`/
`Creating` 진행 중(status.phase로 영속되어 오퍼레이터 재시작 후에도 유지).
CR 어노테이션 `nifi.datadynamics.io/auto-heal: "false"`로 클러스터 단위로 끌 수 있다.

### 삭제

CR을 삭제하면 Owner Reference로 StatefulSet/Service/ConfigMap/Secret/RBAC/PDB/Certificate가 함께
삭제되고, 오퍼레이터가 Owner Reference가 없는 cert-manager TLS Secret(`<name>-nifi-tls`,
`<name>-operator-tls`), 리더 선출 Lease(`<name>-cluster-coordinator`, `<name>-primary-node`),
클러스터 state ConfigMap(`<name>-nifi-component-*`)을 정리한다. **PVC는 데이터 보호를 위해 남긴다**
(`kubectl delete pvc -l cluster=<name>`으로 수동 삭제).

## 테스트

```bash
# 의존성 설치
pip install -r requirements-dev.txt

# 전체 테스트 실행
pytest

# 특정 테스트 파일 실행
pytest tests/test_resources.py -v
pytest tests/test_nifi_api_client.py -v
pytest tests/test_controllers.py -v
pytest tests/test_main_handlers.py -v
```

실제 클러스터(k3s) 통합 테스트용 매니페스트와 REST 헬퍼는 `test/k3s/`에 있다.

## 프로젝트 구조

```
nifi2-k8s-operator/
├── nifi_operator/
│   ├── main.py              # kopf 핸들러 (진입점)
│   ├── controllers/
│   │   ├── cluster.py       # 클러스터 헬스체크, 자동 복구
│   │   └── scaling.py       # 안전한 Scale Up/Down 로직
│   ├── resources/
│   │   ├── statefulset.py   # StatefulSet 빌더
│   │   ├── service.py       # Service 빌더 (Headless + UI)
│   │   ├── configmap.py     # ConfigMap 빌더 (override, merge/start 스크립트, authorizers.xml)
│   │   ├── certificate.py   # cert-manager Certificate 빌더
│   │   ├── rbac.py          # NiFi Pod용 SA/Role/RoleBinding, PDB
│   │   └── secret.py        # 자격 증명 Secret
│   └── nifi_api/
│       └── client.py        # NiFi REST API 클라이언트
├── tests/
│   ├── conftest.py          # 테스트 공통 Fixture
│   ├── test_resources.py    # 리소스 빌더 단위 테스트
│   ├── test_nifi_api_client.py  # NiFi API 클라이언트 테스트
│   ├── test_controllers.py  # 컨트롤러 로직 테스트
│   └── test_main_handlers.py  # kopf 핸들러 테스트
├── test/k3s/                # k3s 통합 테스트 매니페스트/헬퍼
├── deploy/
│   ├── crds/                # CRD 정의 파일
│   ├── rbac.yaml            # RBAC 설정
│   └── operator-deployment.yaml  # Operator 배포
├── examples/                # 예시 CR 매니페스트
├── Dockerfile
├── Makefile
├── pytest.ini
├── requirements.txt
└── requirements-dev.txt
```

## 라이선스

Apache License 2.0
