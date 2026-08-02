# NiFi 2.x Kubernetes Operator

Python과 [kopf](https://kopf.readthedocs.io/)로 구현한 Apache NiFi 2.x 클러스터 Kubernetes Operator입니다.

## 주요 기능

- **NiFi 2.x 네이티브** — ZooKeeper 없이 NiFi 2.x 자체 Raft 기반 클러스터 코디네이션 사용
- **선언적 관리** — NiFiCluster 커스텀 리소스(CR)로 클러스터를 선언적으로 정의
- **자동 클러스터 구성** — Kubernetes Headless Service DNS를 통한 자동 노드 디스커버리
- **안전한 스케일링** — Scale Down 시 Offload → Disconnect → Remove 순서로 데이터 안전하게 이전
- **무중단 업데이트** — 설정 변경 시 롤링 재시작으로 무중단 적용
- **TLS 자동화** — cert-manager 연동으로 노드별 인증서 자동 발급/갱신
- **Flow 배포** — NiFiFlow CR로 NiFi Registry의 Flow를 GitOps 방식으로 자동 배포
- **자동 복구** — 주기적 헬스체크로 연결 끊긴 노드 자동 재연결
- **모니터링** — Prometheus 메트릭으로 클러스터 상태 관찰

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
- (선택) cert-manager — TLS 사용 시
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
kubectl apply -f examples/nifi-cluster-simple.yaml
```

### 4. 상태 확인

```bash
kubectl get nifi -n nifi
# NAME      REPLICAS   READY   PHASE     VERSION   AGE
# my-nifi   3          3       Running   2.4.0     5m

kubectl get pods -n nifi
# NAME              READY   STATUS    RESTARTS   AGE
# my-nifi-nifi-0    1/1     Running   0          5m
# my-nifi-nifi-1    1/1     Running   0          4m
# my-nifi-nifi-2    1/1     Running   0          3m
```

### 5. NiFi UI 접속

```bash
kubectl port-forward svc/my-nifi-ui -n nifi 8443:8443
# https://localhost:8443/nifi 접속
```

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

## 커스텀 리소스 (CR)

### NiFiCluster — NiFi 클러스터 정의

```yaml
apiVersion: datadynamics.io/v1alpha1
kind: NiFiCluster
metadata:
  name: my-nifi
spec:
  replicas: 3                              # 클러스터 노드 수
  version: "2.4.0"                          # NiFi 버전
  resources:
    requests: { cpu: "1", memory: "2Gi" }   # 리소스 요청
    limits:   { cpu: "2", memory: "4Gi" }   # 리소스 제한
  storage:
    size: "20Gi"                            # 노드당 PVC 크기
  config:
    jvmHeapSize: "1536m"                    # JVM 힙 크기
  tls:
    enabled: true                           # TLS 활성화
    issuerRef:
      name: letsencrypt-prod                # cert-manager Issuer
      kind: ClusterIssuer
```

### NiFiFlow — Flow 배포 정의

```yaml
apiVersion: datadynamics.io/v1alpha1
kind: NiFiFlow
metadata:
  name: etl-pipeline
spec:
  clusterRef: my-nifi                       # 대상 NiFiCluster
  registryUrl: "https://nifi-registry.example.com"
  bucketId: "..."                           # Registry 버킷 ID
  flowId: "..."                             # Flow ID
  flowVersion: 3                            # 배포할 버전
  autoStart: true                           # 배포 후 자동 시작
```

## 스케일링

```bash
# 스케일 업 (3 → 5)
kubectl patch nifi my-nifi -n nifi --type merge -p '{"spec":{"replicas":5}}'

# 스케일 다운 (5 → 2) — 안전한 오프보딩 자동 수행
kubectl patch nifi my-nifi -n nifi --type merge -p '{"spec":{"replicas":2}}'
```

## 테스트

```bash
# 의존성 설치
pip install -r requirements.txt

# 전체 테스트 실행
pytest

# 특정 테스트 파일 실행
pytest tests/test_resources.py -v
pytest tests/test_nifi_api_client.py -v
pytest tests/test_controllers.py -v
```

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
│   │   ├── configmap.py     # ConfigMap 빌더 (nifi.properties)
│   │   └── certificate.py   # cert-manager Certificate 빌더
│   └── nifi_api/
│       └── client.py        # NiFi REST API 클라이언트
├── tests/
│   ├── conftest.py          # 테스트 공통 Fixture
│   ├── test_resources.py    # 리소스 빌더 단위 테스트
│   ├── test_nifi_api_client.py  # NiFi API 클라이언트 테스트
│   └── test_controllers.py  # 컨트롤러 로직 테스트
├── deploy/
│   ├── crds/                # CRD 정의 파일
│   ├── rbac.yaml            # RBAC 설정
│   └── operator-deployment.yaml  # Operator 배포
├── examples/                # 예시 CR 매니페스트
├── Dockerfile
├── Makefile
├── pytest.ini
└── requirements.txt
```

## 라이선스

Apache License 2.0
