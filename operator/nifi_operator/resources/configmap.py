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
(statefulset.py의 build_node_rbac 참조).
"""

from kubernetes import client

# 플레이스홀더 — merge-config.sh가 실행 시점에 실제 값(Downward API / Secret env)으로 치환한다.
POD_FQDN = "@POD_FQDN@"
SENSITIVE = "@SENSITIVE_PROPS_KEY@"
KEYSTORE_PW = "@KEYSTORE_PASSWORD@"
TRUSTSTORE_PW = "@TRUSTSTORE_PASSWORD@"

# TLS 활성화 시 인증서 Secret이 마운트되는 경로 (statefulset.py와 일치해야 함)
TLS_MOUNT = "/opt/nifi/tls"

# init 컨테이너(공식 NiFi 이미지 기반)가 실행하는 conf 병합 스크립트.
# 기본 conf를 복사한 뒤 override 프로퍼티를 병합하고 플레이스홀더를 실제 값으로 치환한다.
MERGE_SCRIPT = r"""#!/bin/sh
set -eu
SRC=/opt/nifi/nifi-current/conf
DST=/work-conf
POD_FQDN="${NIFI_POD_NAME}.${HEADLESS_SERVICE}.${NIFI_POD_NAMESPACE}.svc.cluster.local"

# 1) 이미지 기본 conf를 작업 디렉터리로 복사 (필수 키 전부 보존)
cp -a "$SRC"/. "$DST"/

# 2) 플레이스홀더 치환에 쓸 안전한 sed 이스케이프 (구분자 | 및 &, \ 처리)
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

# 3) override 프로퍼티를 nifi.properties에 병합 (있으면 치환, 없으면 추가)
PROPS="$DST/nifi.properties"
while IFS= read -r line; do
  case "$line" in ''|\#*) continue;; esac
  key=${line%%=*}
  rawval=${line#*=}
  val=$(subst "$rawval")
  eval_key=$(printf '%s' "$key" | sed -e 's/[].[^$*\/]/\\&/g')
  eval_val=$(printf '%s' "$val" | sed -e 's/[\\/&]/\\&/g')
  if grep -q "^${key}=" "$PROPS"; then
    sed -i "s/^${eval_key}=.*/${eval_key}=${eval_val}/" "$PROPS"
  else
    printf '%s=%s\n' "$key" "$val" >> "$PROPS"
  fi
done < /overrides/nifi-overrides.properties

# 4) TLS 시 authorizers.xml 교체
if [ -f /overrides/authorizers.xml ]; then
  cp /overrides/authorizers.xml "$DST/authorizers.xml"
fi

echo "merge-config: nifi.properties/authorizers rendered for ${POD_FQDN}"
"""


def _nifi_property_overrides(name: str, namespace: str, spec: dict) -> str:
    """기본 nifi.properties 위에 덮어쓸 키만 담은 override 문자열을 생성한다.

    merge-config.sh가 각 줄을 읽어, 키가 있으면 치환하고 없으면 추가한다.
    """
    replicas = spec.get("replicas", 1)
    config = spec.get("config", {})
    tls_enabled = spec.get("tls", {}).get("enabled", False)
    headless = f"{name}-headless"
    web_proxy_host = config.get("webProxyHost", "")

    # 모든 노드 FQDN — web.proxy.host 화이트리스트에 필요 (호스트 헤더 검증)
    node_hosts = [
        f"{name}-nifi-{i}.{headless}.{namespace}.svc.cluster.local"
        for i in range(replicas)
    ]
    proxy_hosts = node_hosts + [f"{name}-ui.{namespace}.svc.cluster.local"]
    if web_proxy_host:
        proxy_hosts.append(web_proxy_host)

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
        "# ---- 민감 프로퍼티 암호화 키 (Secret에서 주입) ----",
        f"nifi.sensitive.props.key={SENSITIVE}",
        "",
    ]

    if tls_enabled:
        lines += [
            "# ---- 웹 (HTTPS) ----",
            f"nifi.web.https.host={POD_FQDN}",
            "nifi.web.https.port=8443",
            "nifi.web.http.host=",
            "nifi.web.http.port=",
            f"nifi.web.proxy.host={','.join(proxy_hosts)}",
            f"nifi.remote.input.host={POD_FQDN}",
            "nifi.remote.input.secure=true",
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
        ]

    return "\n".join(lines) + "\n"


def _authorizers_xml(name: str, namespace: str, spec: dict) -> str:
    """TLS 클러스터용 authorizers.xml.

    클라이언트 인증서 기반 인증에서, 오퍼레이터의 클라이언트 CN을 Initial Admin으로,
    각 노드의 서버 인증서 CN을 Node Identity로 등록한다. cert-manager가 발급하는
    인증서 CN은 노드 FQDN, 오퍼레이터는 별도 client cert(certificate.py)를 사용한다.
    """
    replicas = spec.get("replicas", 1)
    headless = f"{name}-headless"
    admin_identity = spec.get("tls", {}).get(
        "initialAdminIdentity", f"CN={name}-operator"
    )
    node_identities = "\n".join(
        f'        <property name="Node Identity {i + 1}">'
        f"CN={name}-nifi-{i}.{headless}.{namespace}.svc.cluster.local</property>"
        for i in range(replicas)
    )
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!-- Argus Flow operator generated. DO NOT EDIT. -->
<authorizers>
    <userGroupProvider>
        <identifier>file-user-group-provider</identifier>
        <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
        <property name="Users File">./conf/users.xml</property>
        <property name="Initial User Identity operator">{admin_identity}</property>
{node_identities}
    </userGroupProvider>
    <accessPolicyProvider>
        <identifier>file-access-policy-provider</identifier>
        <class>org.apache.nifi.authorization.FileAccessPolicyProvider</class>
        <property name="User Group Provider">file-user-group-provider</property>
        <property name="Authorizations File">./conf/authorizations.xml</property>
        <property name="Initial Admin Identity">{admin_identity}</property>
{node_identities}
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
      - authorizers.xml: TLS 시 클라이언트 인증서 기반 인가 설정
    비밀값(sensitive key, keystore 비밀번호)은 여기에 넣지 않고 Secret env로 주입되어
    init 컨테이너가 치환한다.
    """
    data = {
        "nifi-overrides.properties": _nifi_property_overrides(name, namespace, spec),
        "merge-config.sh": MERGE_SCRIPT,
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
