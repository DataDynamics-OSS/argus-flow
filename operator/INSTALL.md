# NiFi Operator 설치 가이드

이 문서는 Kubernetes에 NiFi Operator를 설치하고, 첫 번째 NiFi 클러스터를 띄워 화면(UI)까지 여는 과정을
처음 해 보는 사람도 따라 할 수 있도록 순서대로 설명합니다. 명령은 위에서부터 차례대로 실행하면 됩니다.

> **Operator란?** NiFi 클러스터를 직접 만드는 대신 "NiFi 3대짜리 클러스터가 필요하다"는 선언(YAML)만 올리면,
> 필요한 Pod·Service·인증서·설정을 대신 만들고 관리해 주는 프로그램입니다. 노드 추가·제거, 설정 변경,
> 장애 노드 재연결도 Operator가 처리합니다.

## 전체 흐름

```
1. 준비물 확인 ─▶ 2. Operator 이미지 준비 ─▶ 3. Operator 설치 ─▶ 4. 설치 확인
      ─▶ 5. 단일 노드 NiFi 만들기(연습) ─▶ 6. TLS 3노드 클러스터 만들기(운영) ─▶ 7. 정리
```

처음이라면 5단계(단일 노드)까지 먼저 해 보고, 운영용 클러스터가 필요할 때 6단계로 넘어가세요.

---

## 1. 준비물 확인

### 1.1 필요한 것

| 항목 | 필요한 이유 | 확인 명령 |
|---|---|---|
| Kubernetes 클러스터 1.27 이상 | Operator와 NiFi가 실행될 곳 (k3s, kind, minikube, 사내 클러스터 모두 가능) | `kubectl version` |
| `kubectl` | 클러스터에 명령을 보내는 도구 | `kubectl version --client` |
| 기본 StorageClass | NiFi 데이터(PVC)를 저장할 디스크 | `kubectl get storageclass` |
| 인터넷(Docker Hub) 접근 | 클러스터 노드가 Operator 이미지 `datadynamics/argus-flow-operator`를 받음. 폐쇄망이면 2.2 참고 | — |
| 이 저장소 또는 릴리스 첨부 파일 | 설치 파일(`operator/`, Helm 차트) 또는 `argus-flow-operator-0.5.1.yaml` | `git clone` 후 `git checkout v0.5.1` |
| (선택) Docker | Operator 이미지를 직접 빌드하거나 폐쇄망으로 옮길 때만 | `docker version` |
| (선택) Helm 3 이상 | Helm으로 설치할 때만 | `helm version` |
| (6단계) cert-manager | TLS 인증서 자동 발급. 2대 이상 클러스터는 TLS가 필수 | 6.1에서 설치 |

### 1.2 클러스터 연결 확인

```bash
kubectl get nodes
```

`STATUS`가 `Ready`인 노드가 1개 이상 보이면 됩니다.

```
NAME        STATUS   ROLES           AGE   VERSION
node-1      Ready    control-plane   12d   v1.36.5
```

> **k3s를 쓴다면** `kubectl`이 설정 파일을 찾지 못해 연결 오류가 날 수 있습니다. 이때는 먼저
> `export KUBECONFIG=/etc/rancher/k3s/k3s.yaml`을 실행하세요(읽기 권한이 필요하면 `sudo`).

### 1.3 기본 StorageClass 확인

```bash
kubectl get storageclass
```

이름 옆에 `(default)`가 붙은 줄이 하나 있어야 합니다.

```
NAME                   PROVISIONER             ...
local-path (default)   rancher.io/local-path   ...
```

`(default)`가 없으면 NiFi Pod가 `Pending`에서 멈춥니다. 이 경우 클러스터 관리자에게 기본 StorageClass를
요청하거나, NiFiCluster YAML의 `spec.storage.storageClassName`에 사용할 StorageClass 이름을 적으세요.

### 1.4 리소스 여유

예제 설정 기준으로 필요한 최소 자원입니다. 노드에 여유가 부족하면 Pod가 `Pending`이 됩니다.

| 대상 | CPU 요청 | 메모리 요청 | 디스크 |
|---|---|---|---|
| Operator | 0.1 | 256Mi | — |
| 단일 노드 NiFi (`nifi-cluster-simple.yaml`) | 1 | 2Gi | 20Gi |
| TLS 3노드 NiFi (`nifi-cluster-tls.yaml`) | 노드당 2 (총 6) | 노드당 4Gi (총 12Gi) | 노드당 50Gi |

자원이 부족한 테스트 환경이라면 YAML의 `resources`, `storage.size`, `config.jvmHeapSize` 값을 줄이세요.

---

## 2. Operator 이미지 준비

Operator 이미지는 Docker Hub에 **공개 이미지**로 올라가 있습니다. 클러스터 노드가 Docker Hub에 접근할 수 있으면
이 단계에서 할 일은 이미지 이름을 정해 두는 것뿐입니다. 이후 단계는 이 변수를 씁니다.

```bash
export OPERATOR_IMAGE=datadynamics/argus-flow-operator:0.5.1
```

- 태그는 릴리스 버전과 같습니다(`0.5.1`, 같은 MINOR의 최신 패치를 가리키는 `0.5`). `latest` 태그는 없습니다.
- `operator/deploy/operator-deployment.yaml`과 Helm 차트의 기본 이미지가 이미 이 이름입니다. 그대로 쓰면
  3단계에서 이미지 이름을 바꿀 필요가 없습니다.
- Docker Hub는 인증하지 않은 pull의 횟수를 제한합니다. 노드가 많거나 자주 재배포한다면 Docker Hub 계정으로
  `imagePullSecrets`를 만들어 쓰거나, 사내 레지스트리에 미러링하세요(2.2).

### 2.1 이미지가 받아지는지 확인 (선택)

```bash
docker pull "$OPERATOR_IMAGE"
docker inspect "$OPERATOR_IMAGE" --format '{{ index .Config.Labels "org.opencontainers.image.version" }}'
```

`0.5.1`이 출력되면 됩니다.

### 2.2 폐쇄망 또는 사내 레지스트리

클러스터가 Docker Hub에 접근할 수 없으면 GitHub Release에 첨부된 이미지 파일
`argus-flow-operator-image-0.5.1.tar`(`docker save` 결과)를 옮겨서 씁니다. 받은 뒤 `SHA256SUMS`로 무결성을 확인하세요
(`sha256sum -c SHA256SUMS --ignore-missing`).

| 환경 | 방법 |
|---|---|
| **사내/클라우드 클러스터** (노드가 여러 대) | 사내 레지스트리에 올립니다. 아래 "레지스트리" 참고 |
| **k3s** (단일 서버) | `sudo k3s ctr images import argus-flow-operator-image-0.5.1.tar` |
| **kind** | `kind load image-archive argus-flow-operator-image-0.5.1.tar` |
| **minikube** | `minikube image load argus-flow-operator-image-0.5.1.tar` |

**레지스트리에 올리는 경우**: 이미지를 불러온 뒤 레지스트리 주소를 붙여 다시 정하고 push합니다.

```bash
docker load -i argus-flow-operator-image-0.5.1.tar          # 또는 연결된 환경에서 docker pull
export OPERATOR_IMAGE=registry.example.com/platform/argus-flow-operator:0.5.1
docker tag datadynamics/argus-flow-operator:0.5.1 "$OPERATOR_IMAGE"
docker login registry.example.com
docker push "$OPERATOR_IMAGE"
```

> 비공개 레지스트리라면 클러스터가 이미지를 받을 수 있도록 `imagePullSecrets`가 필요합니다.
> 이 경우 3단계 설치 후 Deployment에 pull secret을 추가하세요(클러스터 관리자에게 문의).

### 2.3 직접 빌드 (선택 — 소스를 고쳤을 때)

공개 이미지 대신 저장소의 소스로 직접 빌드할 수도 있습니다. `operator/` 디렉토리에서 실행합니다.

```bash
export OPERATOR_IMAGE=argus-flow-operator:dev
cd operator
docker build -t "$OPERATOR_IMAGE" .
```

마지막에 `naming to docker.io/library/argus-flow-operator:dev` 같은 줄이 나오면 성공입니다. 만든 이미지는
2.2의 표와 같은 방법으로 클러스터에 옮깁니다(k3s는 `docker save "$OPERATOR_IMAGE" | sudo k3s ctr images import -`,
kind는 `kind load docker-image "$OPERATOR_IMAGE"`, minikube는 `minikube image load "$OPERATOR_IMAGE"`).

---

## 3. Operator 설치

세 방법 중 **하나만** 고르세요. 결과는 같습니다.

- **방법 A — kubectl**: 저장소의 파일을 직접 적용합니다. 무엇이 설치되는지 눈으로 확인하기 좋습니다.
- **방법 B — Helm**: 명령 한 줄로 설치·업그레이드·삭제합니다.
- **방법 C — 릴리스 매니페스트 한 파일**: 저장소 없이 GitHub Release의 `argus-flow-operator-0.5.1.yaml` 하나로 설치합니다.

설치되는 것은 다음 네 가지입니다.

| 리소스 | 설명 |
|---|---|
| CRD 2개 (`nificlusters`, `nififlows`) | Kubernetes가 `NiFiCluster`, `NiFiFlow`라는 새 리소스 종류를 알게 함 |
| 네임스페이스 `nifi-operator-system` | Operator가 실행되는 곳 |
| ServiceAccount·ClusterRole·ClusterRoleBinding | Operator에게 NiFi 리소스를 만들 권한 부여 |
| Deployment `nifi-operator` | Operator 프로그램 |

### 방법 A — kubectl

`operator/` 디렉토리에서 실행합니다.

```bash
# 1) CRD
kubectl apply -f deploy/crds/

# 2) 네임스페이스 (RBAC·Deployment보다 먼저 만들어야 합니다)
kubectl create namespace nifi-operator-system

# 3) 권한
kubectl apply -f deploy/rbac.yaml

# 4) Operator 배포 (기본 이미지 datadynamics/argus-flow-operator:0.5.1)
kubectl apply -f deploy/operator-deployment.yaml
```

> 2.2·2.3에서 이미지 이름을 바꿨다면 4번 대신 이미지 이름을 치환해서 적용합니다.
>
> ```bash
> sed -e "s#image: datadynamics/argus-flow-operator:0.5.1#image: ${OPERATOR_IMAGE}#" \
>     deploy/operator-deployment.yaml | kubectl apply -f -
> ```

### 방법 B — Helm

저장소 최상위 디렉토리에서 실행합니다. 차트의 기본 이미지가 `datadynamics/argus-flow-operator:0.5.1`입니다.

```bash
cd ..   # operator/ 에 있었다면 저장소 최상위로

helm install argus-flow charts/argus-flow \
  --namespace nifi-operator-system --create-namespace
```

저장소 대신 GitHub Release의 차트 파일을 써도 됩니다: `helm install argus-flow ./argus-flow-0.5.1.tgz --namespace nifi-operator-system --create-namespace`.

2.2·2.3에서 이미지 이름을 바꿨다면 저장소 주소(`repository`)와 태그(`tag`)로 나눠 넣습니다.

```bash
helm install argus-flow charts/argus-flow \
  --namespace nifi-operator-system --create-namespace \
  --set operator.image.repository="${OPERATOR_IMAGE%:*}" \
  --set operator.image.tag="${OPERATOR_IMAGE##*:}"
```

Helm은 차트의 `crds/` 디렉토리에 있는 CRD를 먼저 설치합니다. 따로 `kubectl apply`할 필요가 없습니다.

### 방법 C — 릴리스 매니페스트

GitHub Release에 첨부된 `argus-flow-operator-0.5.1.yaml`은 CRD·네임스페이스 `nifi-operator-system`·권한·Deployment를
한 파일로 묶은 것입니다(Helm 차트를 `helm template`으로 렌더링한 결과, 이미지 `datadynamics/argus-flow-operator:0.5.1`).

```bash
kubectl apply -f argus-flow-operator-0.5.1.yaml
```

이미지 이름을 바꿔야 하면 방법 A처럼 `sed`로 `datadynamics/argus-flow-operator:0.5.1`을 치환해서 적용합니다.

### 감시 범위 (참고)

Operator는 기본적으로 **모든 네임스페이스**의 `NiFiCluster`를 관리합니다. 특정 네임스페이스만 관리하게
하려면 Deployment에 실행 인자를 지정합니다(방법 A, B, C 모두 같은 명령).

```bash
kubectl -n nifi-operator-system patch deploy nifi-operator --type=json -p \
  '[{"op":"add","path":"/spec/template/spec/containers/0/args","value":["--namespace=nifi","--standalone"]}]'
```

---

## 4. 설치 확인

### 4.1 Operator Pod가 Running인지

```bash
kubectl -n nifi-operator-system get pods
```

```
NAME                             READY   STATUS    RESTARTS   AGE
nifi-operator-6d9f8c7b5d-x2k4q   1/1     Running   0          40s
```

`READY 1/1`, `STATUS Running`이면 성공입니다. 30초 정도 걸릴 수 있습니다.

### 4.2 CRD가 등록됐는지

```bash
kubectl get crd nificlusters.datadynamics.io nififlows.datadynamics.io
```

두 줄이 모두 나오면 됩니다.

### 4.3 Operator 로그

```bash
kubectl -n nifi-operator-system logs deploy/nifi-operator --tail=20
```

`NiFi Operator 시작됨` 줄이 있으면 정상입니다. `Forbidden`(403)이나 오류가 보이면 [9. 문제 해결](#9-문제-해결)을 보세요.

---

## 5. 첫 NiFi 만들기 — 단일 노드 (연습용)

TLS·인증 없이 NiFi 1대를 띄웁니다. **개발·연습 전용**입니다(누구나 접속 가능하므로 운영에 쓰지 마세요).

### 5.1 생성

```bash
kubectl create namespace nifi
kubectl apply -f operator/examples/nifi-cluster-simple.yaml
```

예제 파일의 핵심 내용은 다음과 같습니다.

```yaml
apiVersion: datadynamics.io/v1alpha1
kind: NiFiCluster
metadata:
  name: my-nifi          # 클러스터 이름 (Pod 이름 앞부분이 됨)
  namespace: nifi
spec:
  replicas: 1            # NiFi 노드 수
  version: "2.10.0"      # apache/nifi 이미지 태그
  storage:
    size: "20Gi"         # 노드당 데이터 디스크
  config:
    jvmHeapSize: "1536m" # NiFi JVM 힙
```

> 예제는 공식 `apache/nifi` 이미지를 씁니다. Argus 확장(NAR)이 들어 있는 이미지를 쓰려면 `image`와 `version`을
> Docker Hub의 Argus Flow NiFi 이미지로 바꿉니다(`version`이 이미지 태그가 됩니다).
>
> ```yaml
>   image: "datadynamics/argus-flow-nifi"
>   version: "0.5.1"       # Argus Flow 버전 (기반 NiFi 2.10.0, 태그 0.5.1-nifi2.10.0 과 같은 이미지)
> ```

### 5.2 기동 기다리기

```bash
kubectl get nifi -n nifi -w
```

`-w`는 상태가 바뀔 때마다 새 줄을 보여 줍니다. 약 2분 뒤 `PHASE`가 `Running`, `READY`가 `1`이 되면
`Ctrl+C`로 빠져나옵니다.

```
NAME      REPLICAS   READY   PHASE      VERSION   AGE
my-nifi   1          0       Creating   2.10.0    10s
my-nifi   1          1       Running    2.10.0    2m7s
```

| PHASE | 뜻 |
|---|---|
| `Creating` | 노드가 뜨고 클러스터에 붙기를 기다리는 중 (정상) |
| `Running` | 모든 노드가 연결됨 |
| `Scaling` / `Updating` | 노드 수 변경 / 설정 변경 진행 중 |
| `Degraded` | 일부 노드가 끊김 |
| `Error` | NiFi API에 연결하지 못함 (`kubectl describe nifi my-nifi -n nifi`의 Message 확인) |

Pod도 확인해 봅니다.

```bash
kubectl get pods -n nifi
```

```
NAME             READY   STATUS    RESTARTS   AGE
my-nifi-nifi-0   1/1     Running   0          2m10s
```

### 5.3 NiFi 화면 열기

내 PC에서 클러스터 안의 NiFi로 연결 통로를 엽니다. 이 명령은 켜 둔 채로 두세요.

```bash
kubectl port-forward svc/my-nifi-ui -n nifi 8080:8080
```

브라우저에서 **http://localhost:8080/nifi** 를 엽니다. NiFi 캔버스가 보이면 성공입니다.

> port-forward는 `Ctrl+C`로 끄면 연결이 끊깁니다. NiFi는 계속 실행 중입니다.

### 5.4 데이터가 유지되는지 (선택)

NiFi 화면에서 프로세서를 하나 올려 둔 뒤 Pod를 지워 보세요. 새 Pod가 뜨면 같은 flow가 그대로 남아 있습니다
(flow와 큐의 데이터는 PVC에 저장됩니다).

```bash
kubectl delete pod my-nifi-nifi-0 -n nifi
```

---

## 6. 운영용 — TLS 3노드 클러스터

2대 이상의 NiFi 클러스터는 노드 간 통신에 TLS가 **필수**입니다. 인증서는 cert-manager가 자동으로 발급·갱신합니다.
접속은 **클라이언트 인증서(mTLS)** 로만 합니다.

### 6.1 cert-manager 설치

이미 설치돼 있다면 건너뜁니다(`kubectl get ns cert-manager`로 확인).

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
kubectl -n cert-manager rollout status deploy/cert-manager-webhook
```

`successfully rolled out`이 나오면 됩니다. cert-manager는 클러스터 전체에 설치되므로, 공용 클러스터라면
설치 전에 관리자와 상의하세요.

### 6.2 사설 CA 만들기

NiFi 인증서를 발급할 사설 CA(인증 기관)를 만듭니다. 아래 내용을 `internal-ca.yaml`로 저장하고 적용합니다.
공개 인증서(Let's Encrypt 등)는 쓸 수 없습니다 — 클러스터 내부 주소용 인증서와 클라이언트 인증서가 필요하기 때문입니다.

```yaml
# internal-ca.yaml — 자체 서명 Issuer → CA 인증서 → CA ClusterIssuer
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: internal-ca
  namespace: cert-manager          # ClusterIssuer의 CA Secret은 cert-manager 네임스페이스에 둡니다
spec:
  isCA: true
  commonName: internal-ca
  secretName: internal-ca
  duration: 87600h                 # 10년
  privateKey: { algorithm: RSA, size: 2048 }
  issuerRef: { name: selfsigned, kind: ClusterIssuer }
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: internal-ca                # 예제 nifi-cluster-tls.yaml 의 issuerRef 이름과 같아야 합니다
spec:
  ca:
    secretName: internal-ca
```

```bash
kubectl apply -f internal-ca.yaml
kubectl get clusterissuer internal-ca
```

`READY`가 `True`이면 됩니다. 사내에 이미 사설 CA Issuer가 있다면 그것을 써도 됩니다.

### 6.3 클러스터 YAML 준비

`operator/examples/nifi-cluster-tls.yaml`을 복사해서 다음을 **반드시** 바꾸세요.

| 항목 | 바꿀 내용 |
|---|---|
| `spec.config.sensitivePropsKey` | 12자 이상 임의의 문자열. NiFi가 flow 안의 비밀번호를 암호화하는 키입니다. **한 번 정하면 바꾸지 마세요**(바꾸면 기존 flow의 비밀번호를 풀 수 없습니다) |
| `spec.config.webProxyHost` | 외부에서 접속할 주소(Ingress 등). 없으면 이 줄을 지웁니다 |
| `spec.tls.issuerRef` | 6.2에서 만든 `internal-ca` / `ClusterIssuer` (예제 그대로면 수정 불필요) |
| `spec.resources`, `spec.storage.size` | 클러스터 자원에 맞게 |

### 6.4 생성과 확인

```bash
kubectl apply -f nifi-cluster-tls.yaml
kubectl get nifi -n nifi -w
```

노드가 하나씩 차례로 뜨므로 3노드는 수 분 걸립니다. `READY 3`, `PHASE Running`이 되면 완료입니다.

```
NAME          REPLICAS   READY   PHASE     VERSION   AGE
nifi-secure   3          3       Running   2.10.0    6m
```

인증서도 확인해 봅니다. 서버용과 Operator(관리자)용 두 개가 `True`여야 합니다.

```bash
kubectl get certificate -n nifi
```

### 6.5 화면 열기 — 관리자 인증서 등록

TLS 클러스터는 아이디·비밀번호 대신 **인증서**로 로그인합니다. Operator가 만든 관리자 인증서
(Secret `<클러스터이름>-operator-tls`)를 브라우저에 등록합니다.

```bash
# 1) 인증서 파일 꺼내기
kubectl get secret nifi-secure-operator-tls -n nifi -o jsonpath='{.data.tls\.crt}' | base64 -d > admin.crt
kubectl get secret nifi-secure-operator-tls -n nifi -o jsonpath='{.data.tls\.key}' | base64 -d > admin.key
kubectl get secret nifi-secure-operator-tls -n nifi -o jsonpath='{.data.ca\.crt}'  | base64 -d > ca.crt

# 2) 브라우저용 파일(.p12)로 묶기 — 실행하면 내보내기 비밀번호를 물어봅니다
openssl pkcs12 -export -in admin.crt -inkey admin.key -certfile ca.crt -out admin.p12
```

`admin.p12`를 브라우저의 인증서 관리(Chrome: 설정 → 개인정보 및 보안 → 보안 → 인증서 관리)에서 가져오기 합니다.
`ca.crt`를 신뢰할 수 있는 루트 인증서로 등록하면 "안전하지 않음" 경고도 사라집니다.

```bash
kubectl port-forward svc/nifi-secure-ui -n nifi 8443:8443
```

브라우저에서 **https://localhost:8443/nifi** 를 열고, 인증서 선택 창이 뜨면 방금 등록한 인증서를 고릅니다.

> - 로컬 포트를 8443이 아닌 값(예: 9443)으로 쓰려면 `spec.config.webProxyHost`에 `localhost:9443`을 추가해야 합니다.
>   그렇지 않으면 NiFi가 `421`(Invalid Port) 오류를 냅니다.
> - `admin.key`와 `admin.p12`는 관리자 권한 파일입니다. 사용 후 안전한 곳에 보관하거나 지우세요.

### 6.6 노드 수 바꾸기

YAML의 `replicas`를 바꿔 다시 적용하면 됩니다.

```bash
kubectl patch nifi nifi-secure -n nifi --type merge -p '{"spec":{"replicas":4}}'
```

줄일 때는 Operator가 없어질 노드의 데이터를 다른 노드로 먼저 옮긴 뒤(Offload) 제거합니다. 옮기기에 실패하면
노드를 줄이지 않고 다시 시도합니다. `PHASE`가 `Scaling`에서 `Running`으로 돌아오면 끝난 것입니다.

---

## 7. 정리 (삭제)

### 7.1 NiFi 클러스터만 지우기

```bash
kubectl delete nifi my-nifi -n nifi
```

Pod·Service·ConfigMap·인증서는 함께 지워지지만, **데이터 디스크(PVC)는 일부러 남깁니다**(실수로 지웠을 때
데이터를 살릴 수 있도록). 데이터까지 지우려면:

```bash
kubectl get pvc -n nifi                    # data-my-nifi-nifi-0 같은 이름 확인
kubectl delete pvc data-my-nifi-nifi-0 -n nifi
```

### 7.2 Operator 지우기

먼저 7.1로 NiFi 클러스터를 모두 지운 뒤 진행하세요. CRD를 지우면 남아 있던 `NiFiCluster`도 함께 지워집니다.

**방법 A로 설치했다면**

```bash
cd operator
kubectl delete -f deploy/operator-deployment.yaml --ignore-not-found
kubectl delete -f deploy/rbac.yaml --ignore-not-found
kubectl delete namespace nifi-operator-system
kubectl delete -f deploy/crds/
```

**방법 B로 설치했다면**

```bash
helm uninstall argus-flow -n nifi-operator-system
kubectl delete namespace nifi-operator-system
kubectl delete crd nificlusters.datadynamics.io nififlows.datadynamics.io   # Helm은 CRD를 지우지 않습니다
```

**방법 C로 설치했다면** (네임스페이스·CRD까지 한 번에 지워집니다)

```bash
kubectl delete -f argus-flow-operator-0.5.1.yaml
```

cert-manager와 6.2의 CA는 다른 서비스도 쓸 수 있으므로 필요 없을 때만 지우세요
(`kubectl delete -f internal-ca.yaml`, cert-manager는 설치에 쓴 URL로 `kubectl delete -f`).

---

## 8. Operator 업그레이드

새 릴리스의 이미지로 배포된 이미지만 바꿉니다. 아래 `<새 버전>`은 올릴 릴리스 버전입니다(예: `0.6.0`).
저장소는 그 릴리스 태그로 받아 둡니다(`git checkout v<새 버전>`).

```bash
export OPERATOR_IMAGE=datadynamics/argus-flow-operator:<새 버전>
# 폐쇄망·직접 빌드라면 2.2·2.3으로 이미지를 옮긴 뒤
kubectl apply -f operator/deploy/crds/      # CRD가 바뀌었을 수 있으므로 먼저 적용
kubectl -n nifi-operator-system set image deploy/nifi-operator operator="$OPERATOR_IMAGE"
kubectl -n nifi-operator-system rollout status deploy/nifi-operator
```

Helm이라면 새 버전 차트로 `helm upgrade argus-flow charts/argus-flow -n nifi-operator-system`(차트의 기본 이미지 태그가 새 버전).
`helm upgrade`는 CRD를 갱신하지 않으므로 `kubectl apply -f charts/argus-flow/crds/`를 먼저 실행하세요.
방법 C로 설치했다면 새 릴리스의 `argus-flow-operator-<새 버전>.yaml`을 `kubectl apply -f`하면 CRD와 이미지가 함께 갱신됩니다.

Operator를 업그레이드하는 것만으로는 실행 중인 NiFi가 재시작되지 않습니다. 다만 그 뒤 `NiFiCluster`의 spec을
바꾸면, 새 버전 Operator가 만든 설정으로 노드가 하나씩 롤링 재시작될 수 있습니다. 업그레이드 직후 첫 spec 변경은
작업량이 적은 시간에 하세요.

---

## 9. 문제 해결

먼저 아래 세 명령으로 상황을 봅니다.

```bash
kubectl -n nifi-operator-system logs deploy/nifi-operator --tail=50   # Operator가 무엇을 하는지
kubectl describe nifi <클러스터이름> -n nifi                           # Status의 Phase·Message와 Events
kubectl get pods -n nifi                                              # NiFi Pod 상태
```

| 증상 | 원인 | 해결 |
|---|---|---|
| Operator Pod가 `ImagePullBackOff` / `ErrImagePull` | 클러스터가 Operator 이미지를 찾지 못함, 또는 Docker Hub pull 횟수 제한 | 2단계를 다시 확인. 이미지 이름이 3단계와 같은지, 노드가 Docker Hub에 접근할 수 있는지(`toomanyrequests`면 횟수 제한 → imagePullSecrets), 폐쇄망 k3s·kind·minikube는 이미지를 옮겼는지, 사내 레지스트리는 pull 권한(imagePullSecrets)이 있는지 |
| Operator 로그에 `Forbidden` / `403` | 권한(RBAC) 미적용 | `kubectl apply -f deploy/rbac.yaml` 다시 적용. ClusterRoleBinding의 네임스페이스가 Operator 네임스페이스와 같은지 확인 |
| `kubectl apply` 시 `no matches for kind "NiFiCluster"` | CRD 미설치 | `kubectl apply -f operator/deploy/crds/` |
| NiFi Pod가 `Pending` | 기본 StorageClass 없음, 또는 CPU·메모리 부족 | `kubectl describe pod <이름> -n nifi`의 Events 확인. 1.3·1.4 참고 |
| NiFi Pod가 `ContainerCreating`에서 오래 멈춤 (TLS) | 인증서 Secret이 아직 없음 | `kubectl get certificate -n nifi`가 `True`인지, Issuer가 `READY True`인지 확인 |
| `PHASE`가 계속 `Creating` | NiFi가 처음 뜨는 데 2분 이상 걸림 (정상) | 3노드는 수 분 기다립니다. 10분 넘게 그대로면 `kubectl logs <pod> -n nifi`로 NiFi 로그 확인 |
| 노드 재시작 직후 1~3분간 `PHASE`가 `Error` | NiFi가 클러스터를 다시 구성하는 동안 API가 일시적으로 409를 돌려줌 | 대부분 저절로 `Running`으로 돌아옵니다. 계속되면 NiFi 로그 확인 |
| 생성 시 `멀티노드 NiFi 2.x 클러스터는 tls.enabled=true가 필요합니다` | `replicas`가 2 이상인데 TLS 없음 | 6단계처럼 TLS를 켜거나 `replicas: 1` |
| 브라우저에서 `421` 오류 | port-forward 로컬 포트와 NiFi가 허용한 주소가 다름 | 로컬 포트를 8443(TLS)/8080(비-TLS)으로 쓰거나 `webProxyHost`에 `localhost:<포트>` 추가 |
| 브라우저에서 인증서 선택 창이 안 뜨고 접속 거부 | 관리자 인증서 미등록 | 6.5 다시 확인. `admin.p12`를 "개인" 인증서로 가져왔는지 |
| `storage` 변경이 적용되지 않음 | 디스크 크기 변경은 지원하지 않음 | Events에 거부 사유가 남습니다. 새 클러스터로 옮겨야 합니다 |

그래도 해결되지 않으면 위 세 명령의 출력을 함께 첨부해 이슈로 남겨 주세요.
