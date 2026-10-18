"""
NiFi 설정 파일용 ConfigMap 빌더.

NiFi 2.x는 설정 항목이 수백 개이고 대부분 기본값이 필수다. 따라서 이 오퍼레이터는
nifi.properties를 통째로 생성하지 않고, **공식 이미지의 기본 conf 위에 필요한 키만
override로 병합**한다(StatefulSet의 init 컨테이너가 merge-config.sh로 수행).

NiFi 2.x 클러스터링 (ZooKeeper 불필요):
    - nifi.cluster.leader.election.implementation = KubernetesLeaderElectionManager
      → Kubernetes Lease 기반 리더 선출 (별도 ZooKeeper 없이 동작)
    - nifi.state.management.provider.cluster = kubernetes-provider
      → state-management.xml에 기본 정의된 KubernetesConfigMapStateProvider 사용
    노드는 초기 노드 목록이 아니라 리더 선출/flow election으로 클러스터를 구성한다.
    (구버전에서 쓰던 nifi.cluster.node.N 초기 목록 프로퍼티는 NiFi 2.x에 존재하지 않음)

이 방식은 NiFi Pod의 ServiceAccount에 leases/configmaps에 대한 RBAC 권한을 요구한다
(rbac.py의 build_node_role 참조).
"""

from kubernetes import client

# 플레이스홀더 — merge-config.sh가 실행 시점에 실제 값(Downward API / Secret env)으로 치환한다.
POD_FQDN = "@POD_FQDN@"
SENSITIVE = "@SENSITIVE_PROPS_KEY@"
KEYSTORE_PW = "@KEYSTORE_PASSWORD@"
TRUSTSTORE_PW = "@TRUSTSTORE_PASSWORD@"

# TLS 활성화 시 인증서 Secret이 마운트되는 경로 (statefulset.py와 일치해야 함)
TLS_MOUNT = "/opt/nifi/tls"

NIFI_HOME = "/opt/nifi/nifi-current"
# 노드별 PVC 마운트 경로 (statefulset.py와 일치해야 함). 재시작 후에도 유지되어야 하는
# 모든 상태(flow 정의, FlowFile/Content/Provenance/DB 리포지토리, 로컬 state,
# users.xml/authorizations.xml, 업로드 NAR/asset)를 이 아래에 둔다.
DATA_DIR = f"{NIFI_HOME}/data"

# init 컨테이너(공식 NiFi 이미지 기반)가 실행하는 conf 병합 스크립트.
# 기본 conf를 복사한 뒤 override 프로퍼티를 병합하고 플레이스홀더를 실제 값으로 치환한다.
MERGE_SCRIPT = r"""#!/bin/sh
set -eu
SRC=/opt/nifi/nifi-current/conf
DST=/work-conf
DATA=/opt/nifi/nifi-current/data
POD_FQDN="${NIFI_POD_NAME}.${HEADLESS_SERVICE}.${NIFI_POD_NAMESPACE}.svc.cluster.local"

# 1) 이미지 기본 conf를 작업 디렉터리로 복사 (필수 키 전부 보존)
#    cp -a 는 emptyDir 루트(root 소유)의 시간 속성 보존에 실패하므로 -R 만 사용한다.
cp -R "$SRC"/. "$DST"/

# 2) PVC(data) 아래 영속 디렉터리 준비 — NiFi가 상위 디렉터리를 만들지 않는 경로 대비
mkdir -p "$DATA/conf" "$DATA/flow/archive" "$DATA/state/local"

# 3) 플레이스홀더 치환에 쓸 안전한 sed 이스케이프 (구분자 | 및 &, \ 처리)
esc() { printf '%s' "$1" | sed -e 's/[\\|&]/\\&/g'; }
E_FQDN=$(esc "$POD_FQDN")
E_SENS=$(esc "${SENSITIVE_PROPS_KEY:-}")
E_KS=$(esc "${KEYSTORE_PASSWORD:-}")
E_TS=$(esc "${TRUSTSTORE_PASSWORD:-}")

subst() {
  printf '%s' "$1" | sed \
    -e "s|@POD_FQDN@|${E_FQDN}|g" \
    -e "s|@SENSITIVE_PROPS_KEY@|${E_SENS}|g" \
    -e "s|@KEYSTORE_PASSWORD@|${E_KS}|g" \
    -e "s|@TRUSTSTORE_PASSWORD@|${E_TS}|g"
}

# 4) override 프로퍼티를 nifi.properties에 병합 (있으면 치환, 없으면 추가)
PROPS="$DST/nifi.properties"
while IFS= read -r line; do
  case "$line" in ''|\#*) continue;; esac
  key=${line%%=*}
  rawval=${line#*=}
  val=$(subst "$rawval")
  eval_key=$(printf '%s' "$key" | sed -e 's/[].[^$*\/]/\\&/g')
  eval_val=$(printf '%s' "$val" | sed -e 's/[\\/&]/\\&/g')
  if grep -q "^${eval_key}=" "$PROPS"; then
    sed -i "s/^${eval_key}=.*/${eval_key}=${eval_val}/" "$PROPS"
  else
    printf '%s=%s\n' "$key" "$val" >> "$PROPS"
  fi
done < /overrides/nifi-overrides.properties

# 5) 로컬 state provider 디렉터리를 PVC로, 클러스터 state ConfigMap 이름에 클러스터 접두어
SM="$DST/state-management.xml"
sed -i \
  -e "s|<property name=\"Directory\">./state/local</property>|<property name=\"Directory\">${DATA}/state/local</property>|" \
  -e "s|<property name=\"ConfigMap Name Prefix\"></property>|<property name=\"ConfigMap Name Prefix\">${STATE_CONFIGMAP_PREFIX:-}</property>|" \
  "$SM"

# 6) JVM 힙 (bootstrap.conf java.arg.2/3) — 이미지 start.sh를 쓰지 않으므로 여기서 설정
if [ -n "${NIFI_JVM_HEAP_INIT:-}" ]; then
  sed -i "s|^java.arg.2=.*|java.arg.2=-Xms${NIFI_JVM_HEAP_INIT}|" "$DST/bootstrap.conf"
fi
if [ -n "${NIFI_JVM_HEAP_MAX:-}" ]; then
  sed -i "s|^java.arg.3=.*|java.arg.3=-Xmx${NIFI_JVM_HEAP_MAX}|" "$DST/bootstrap.conf"
fi

# 7) TLS 시 authorizers.xml 교체
if [ -f /overrides/authorizers.xml ]; then
  cp /overrides/authorizers.xml "$DST/authorizers.xml"
fi

echo "merge-config: nifi.properties/authorizers rendered for ${POD_FQDN}"
"""

# 메인 컨테이너 실행 스크립트. 이미지 기본 ENTRYPOINT(/opt/nifi/scripts/start.sh)는
# 기동 직전에 nifi.properties를 NIFI_* 환경변수 기본값으로 무조건 덮어써서
# (nifi.cluster.is.node=false, https.port=8443, leader election=Curator, state=zk-provider 등)
# init 컨테이너가 병합한 설정을 무효화한다. 따라서 start.sh를 쓰지 않고 nifi.sh run을
# 직접 실행한다. start.sh와 동일하게 nifi-app.log를 stdout으로 내보내고,
# SIGTERM 시 nifi.sh stop으로 정상 종료(graceful shutdown)한다.
START_SCRIPT = r"""#!/bin/sh
cd /opt/nifi/nifi-current
mkdir -p logs
touch logs/nifi-app.log

bin/nifi.sh run &
nifi_pid=$!
tail -n +1 -F --pid="${nifi_pid}" logs/nifi-app.log &
tail_pid=$!

shutdown() {
  echo "nifi-operator start-nifi.sh: 종료 신호 수신 — nifi.sh stop 실행"
  bin/nifi.sh stop
}
trap shutdown TERM INT HUP

# trap 처리 후에도 NiFi 프로세스가 끝날 때까지 기다린다
rc=0
while kill -0 "${nifi_pid}" 2>/dev/null; do
  wait "${nifi_pid}"
  rc=$?
done
# 기동 실패 시 마지막 오류 로그가 잘리지 않도록 tail이 따라잡을 시간을 준다
sleep 2
kill "${tail_pid}" 2>/dev/null || true
exit "${rc}"
"""


def node_identity(name: str, namespace: str) -> str:
    """NiFi 노드 간 요청(복제/프록시)에 쓰이는 노드 인증서 DN.

    모든 노드가 하나의 서버 인증서(certificate.py, commonName만 지정)를 공유하므로
    노드 identity는 노드 수와 무관하게 하나다. 스케일 업 시 authorizers.xml을
    고칠 필요가 없다.
    """
    return f"CN={name}-nifi.{namespace}.svc.cluster.local"


def _nifi_property_overrides(name: str, namespace: str, spec: dict) -> str:
    """기본 nifi.properties 위에 덮어쓸 키만 담은 override 문자열을 생성한다.

    merge-config.sh가 각 줄을 읽어, 키가 있으면 치환하고 없으면 추가한다.
    """
    replicas = spec.get("replicas", 1)
    config = spec.get("config", {})
    tls_enabled = spec.get("tls", {}).get("enabled", False)
    web_proxy_host = config.get("webProxyHost", "")

    # web.proxy.host: NiFi는 자신의 https.host로 온 요청은 항상 허용하므로, 추가로 UI
    # Service 이름과 사용자 지정 외부 호스트만 허용 목록에 둔다(노드 수와 무관).
    web_port = 8443 if tls_enabled else 8080
    proxy_hosts = [
        f"{name}-ui.{namespace}.svc.cluster.local",
        f"{name}-ui.{namespace}.svc.cluster.local:{web_port}",
        f"{name}-ui",
        f"{name}-ui:{web_port}",
        f"localhost:{web_port}",
    ]
    if web_proxy_host:
        proxy_hosts += [h.strip() for h in web_proxy_host.split(",") if h.strip()]

    lines = [
        "# ==== Argus Flow operator overrides (기본 conf 위에 병합됨) ====",
        "",
        "# ---- 클러스터 (NiFi 2.x, Kubernetes 리더 선출 — ZooKeeper 불필요) ----",
        "nifi.cluster.is.node=true",
        f"nifi.cluster.node.address={POD_FQDN}",
        "nifi.cluster.node.protocol.port=11443",
        f"nifi.cluster.node.protocol.max.threads={config.get('maxThreads', 10)}",
        "nifi.cluster.leader.election.implementation=KubernetesLeaderElectionManager",
        f"nifi.cluster.leader.election.kubernetes.lease.prefix={name}",
        "nifi.cluster.flow.election.max.wait.time=1 min",
        f"nifi.cluster.flow.election.max.candidates={replicas}",
        "",
        "# ---- 클러스터 상태 관리 (Kubernetes ConfigMap Provider) ----",
        "nifi.state.management.provider.cluster=kubernetes-provider",
        "nifi.state.management.embedded.zookeeper.start=false",
        "",
        "# ---- 로드 밸런싱 ----",
        f"nifi.cluster.load.balance.host={POD_FQDN}",
        "nifi.cluster.load.balance.port=6342",
        "",
        "# ---- 영속 데이터 (PVC: data 볼륨) — 재시작 시 flow/FlowFile/state 보존 ----",
        f"nifi.flow.configuration.file={DATA_DIR}/flow/flow.json.gz",
        f"nifi.flow.configuration.archive.dir={DATA_DIR}/flow/archive/",
        f"nifi.flowfile.repository.directory={DATA_DIR}/flowfile_repository",
        f"nifi.content.repository.directory.default={DATA_DIR}/content_repository",
        f"nifi.provenance.repository.directory.default={DATA_DIR}/provenance_repository",
        f"nifi.database.directory={DATA_DIR}/database_repository",
        f"nifi.nar.persistence.provider.properties.directory={DATA_DIR}/nar_repository",
        f"nifi.asset.manager.properties.directory={DATA_DIR}/assets",
        f"nifi.connector.asset.manager.properties.directory={DATA_DIR}/connector_assets",
        "",
        "# ---- 민감 프로퍼티 암호화 키 (Secret에서 주입) ----",
        f"nifi.sensitive.props.key={SENSITIVE}",
        "",
    ]

    if tls_enabled:
        lines += [
            "# ---- 웹 (HTTPS) ----",
            f"nifi.web.https.host={POD_FQDN}",
            "nifi.web.https.port=8443",
            "# https.host(FQDN)는 노드 API 주소로 광고되지만 바인딩은 인터페이스 단위로 한다 —",
            "# FQDN(Pod IP)에만 바인딩하면 kubectl port-forward(Pod 내 localhost 접속)가 실패한다.",
            "nifi.web.https.network.interface.default=eth0",
            "nifi.web.https.network.interface.lo=lo",
            "nifi.web.http.host=",
            "nifi.web.http.port=",
            f"nifi.web.proxy.host={','.join(proxy_hosts)}",
            f"nifi.remote.input.host={POD_FQDN}",
            "nifi.remote.input.secure=true",
            "",
            "# ---- 인증/인가: 클라이언트 인증서(mTLS) + managed-authorizer ----",
            "# 이미지 기본값(single-user-authorizer/single-user-provider)은 클러스터에서",
            "# 노드/오퍼레이터 인증서 identity를 인가할 수 없으므로 교체한다.",
            "nifi.security.user.authorizer=managed-authorizer",
            "nifi.security.user.login.identity.provider=",
            "",
            "# ---- 보안 (cert-manager 발급 PKCS12, statefulset이 마운트) ----",
            "nifi.cluster.protocol.is.secure=true",
            f"nifi.security.keystore={TLS_MOUNT}/keystore.p12",
            "nifi.security.keystoreType=PKCS12",
            f"nifi.security.keystorePasswd={KEYSTORE_PW}",
            f"nifi.security.keyPasswd={KEYSTORE_PW}",
            f"nifi.security.truststore={TLS_MOUNT}/truststore.p12",
            "nifi.security.truststoreType=PKCS12",
            f"nifi.security.truststorePasswd={TRUSTSTORE_PW}",
            "# cert-manager 갱신 인증서를 재시작 없이 반영",
            "nifi.security.autoreload.enabled=true",
        ]
    else:
        # 비-TLS: 단일 노드 개발용. 멀티노드는 create 핸들러에서 거부한다.
        lines += [
            "# ---- 웹 (HTTP, 개발 전용 — 멀티노드 미지원) ----",
            "nifi.web.http.host=0.0.0.0",
            "nifi.web.http.port=8080",
            "nifi.web.https.host=",
            "nifi.web.https.port=",
            f"nifi.web.proxy.host={','.join(proxy_hosts)}",
            "nifi.cluster.protocol.is.secure=false",
            "nifi.remote.input.secure=false",
            "# 이미지 기본값(./conf/keystore.p12)이 남아 있으면 HTTP 모드에서도 keystore 로드를",
            "# 시도하다 기동 실패한다 → 비운다.",
            "nifi.security.keystore=",
            "nifi.security.keystoreType=",
            "nifi.security.truststore=",
            "nifi.security.truststoreType=",
        ]

    return "\n".join(lines) + "\n"


def _authorizers_xml(name: str, namespace: str, spec: dict) -> str:
    """TLS 클러스터용 authorizers.xml.

    클라이언트 인증서 기반 인증에서, 오퍼레이터의 클라이언트 CN을 Initial Admin으로,
    노드 서버 인증서 DN을 Node Identity로 등록한다.

    - FileUserGroupProvider는 ``Initial User Identity *`` 속성만 사용자로 시드한다
      (``Node Identity *`` 속성은 무시됨). 따라서 노드 identity도 Initial User로 넣어야
      FileAccessPolicyProvider가 노드 정책을 시드할 때 사용자를 찾을 수 있다.
    - 모든 노드가 같은 서버 인증서를 공유하므로 노드 identity는 하나다(node_identity()).
    - users.xml/authorizations.xml은 PVC에 두어 UI/API로 변경한 정책이 재시작 후에도
      유지되게 한다(파일이 없을 때만 위 초기값으로 시드됨).
    """
    admin_identity = spec.get("tls", {}).get(
        "initialAdminIdentity", f"CN={name}-operator"
    )
    node_dn = node_identity(name, namespace)
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!-- Argus Flow operator generated. DO NOT EDIT. -->
<authorizers>
    <userGroupProvider>
        <identifier>file-user-group-provider</identifier>
        <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
        <property name="Users File">{DATA_DIR}/conf/users.xml</property>
        <property name="Initial User Identity operator">{admin_identity}</property>
        <property name="Initial User Identity nodes">{node_dn}</property>
    </userGroupProvider>
    <accessPolicyProvider>
        <identifier>file-access-policy-provider</identifier>
        <class>org.apache.nifi.authorization.FileAccessPolicyProvider</class>
        <property name="User Group Provider">file-user-group-provider</property>
        <property name="Authorizations File">{DATA_DIR}/conf/authorizations.xml</property>
        <property name="Initial Admin Identity">{admin_identity}</property>
        <property name="Node Identity 1">{node_dn}</property>
    </accessPolicyProvider>
    <authorizer>
        <identifier>managed-authorizer</identifier>
        <class>org.apache.nifi.authorization.StandardManagedAuthorizer</class>
        <property name="Access Policy Provider">file-access-policy-provider</property>
    </authorizer>
</authorizers>
"""


def build_nifi_config(name: str, namespace: str, spec: dict) -> client.V1ConfigMap:
    """NiFi override 설정을 담는 ConfigMap을 생성한다.

    ConfigMap 키:
      - nifi-overrides.properties: 기본 nifi.properties 위에 병합할 키
      - merge-config.sh: init 컨테이너의 conf 병합 스크립트
      - start-nifi.sh: 메인 컨테이너 실행 스크립트(이미지 start.sh 대체)
      - authorizers.xml: TLS 시 클라이언트 인증서 기반 인가 설정
    비밀값(sensitive key, keystore 비밀번호)은 여기에 넣지 않고 Secret env로 주입되어
    init 컨테이너가 치환한다.
    """
    data = {
        "nifi-overrides.properties": _nifi_property_overrides(name, namespace, spec),
        "merge-config.sh": MERGE_SCRIPT,
        "start-nifi.sh": START_SCRIPT,
    }
    if spec.get("tls", {}).get("enabled", False):
        data["authorizers.xml"] = _authorizers_xml(name, namespace, spec)

    return client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=f"{name}-config",
            namespace=namespace,
            labels={
                "app": "nifi",
                "cluster": name,
                "app.kubernetes.io/managed-by": "nifi-operator",
            },
        ),
        data=data,
    )
