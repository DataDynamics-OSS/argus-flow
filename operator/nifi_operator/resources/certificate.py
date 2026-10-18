"""
cert-manager Certificate 리소스 빌더.

두 종류의 인증서를 발급한다:

1. **서버 인증서** (`{name}-nifi-tls`): 모든 노드가 공유하는 단일 인증서.
   StatefulSet의 모든 Pod가 같은 Secret을 마운트하므로, 인증서의 SAN에 전체 노드
   FQDN을 포함한다. PKCS12(keystore.p12/truststore.p12)로 생성하여 NiFi가 직접
   사용한다. PKCS12 비밀번호는 `{name}-credentials` Secret의 keystore-password를
   공유한다(nifi.properties와 동일 값 참조).

2. **오퍼레이터 클라이언트 인증서** (`{name}-operator-tls`): 오퍼레이터가 NiFi REST
   API에 mTLS로 인증할 때 사용. CN은 authorizers.xml의 Initial Admin Identity와
   일치해야 한다(configmap.py의 admin_identity, 기본 `CN={name}-operator`).

주의: cert-manager CRD는 K8s 표준 API가 아니므로 dict로 반환하고 CustomObjectsApi로
     생성한다.
"""


def _base(name: str, namespace: str, obj_name: str) -> dict:
    return {
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": obj_name,
            "namespace": namespace,
            "labels": {
                "app": "nifi",
                "cluster": name,
                "app.kubernetes.io/managed-by": "nifi-operator",
            },
        },
    }


def _issuer_ref(spec: dict) -> dict:
    tls = spec.get("tls", {})
    return {
        "name": tls["issuerRef"]["name"],
        "kind": tls["issuerRef"].get("kind", "ClusterIssuer"),
    }


def build_server_certificate(name: str, namespace: str, spec: dict) -> dict:
    """모든 NiFi 노드가 공유하는 서버 인증서(PKCS12).

    SAN에 headless Service 아래 와일드카드(``*.{headless}.{ns}.svc.cluster.local``)와
    서비스 이름을 넣어, 어느 Pod가 이 인증서를 제시해도 호스트명 검증을 통과하게 한다.
    와일드카드를 쓰므로 SAN이 노드 수와 무관하다 — 스케일 업 시 인증서 재발급/기존 노드
    재시작이 필요 없다. (사용하는 Issuer가 와일드카드 DNS SAN을 발급할 수 있어야 한다:
    CA/Vault/SelfSigned Issuer 등 사설 PKI. 공개 ACME는 .svc.cluster.local 자체를 발급 불가.)

    commonName은 노드 identity(configmap.node_identity)와 일치해야 한다.
    """
    headless = f"{name}-headless"
    dns_names: list[str] = [
        f"*.{headless}.{namespace}.svc.cluster.local",
        f"*.{headless}.{namespace}.svc",
        f"*.{headless}",
        # 서비스 이름들 (UI 접근)
        f"{name}-ui.{namespace}.svc.cluster.local",
        f"{name}-ui",
        headless,
        "localhost",
    ]

    cert = _base(name, namespace, f"{name}-nifi-tls")
    cert["spec"] = {
        "secretName": f"{name}-nifi-tls",
        "issuerRef": _issuer_ref(spec),
        "commonName": f"{name}-nifi.{namespace}.svc.cluster.local",
        "dnsNames": dns_names,
        "duration": "8760h",
        "renewBefore": "720h",
        "privateKey": {"algorithm": "RSA", "size": 2048},
        # NiFi가 사용하는 PKCS12 keystore/truststore 생성.
        # 비밀번호는 credentials Secret과 공유 (nifi.properties가 같은 값을 참조).
        "keystores": {
            "pkcs12": {
                "create": True,
                "passwordSecretRef": {
                    "name": f"{name}-credentials",
                    "key": "keystore-password",
                },
            }
        },
    }
    return cert


def build_operator_certificate(name: str, namespace: str, spec: dict) -> dict:
    """오퍼레이터가 NiFi REST API에 mTLS 클라이언트로 인증하기 위한 인증서.

    CN은 authorizers.xml의 Initial Admin Identity와 일치해야 한다. 오퍼레이터는
    이 Secret(tls.crt/tls.key/ca.crt)을 K8s API로 읽어 httpx 클라이언트 인증에 쓴다.
    """
    admin_identity = spec.get("tls", {}).get("initialAdminIdentity")
    common_name = (
        admin_identity.split("CN=", 1)[1]
        if admin_identity and "CN=" in admin_identity
        else f"{name}-operator"
    )

    cert = _base(name, namespace, f"{name}-operator-tls")
    cert["spec"] = {
        "secretName": f"{name}-operator-tls",
        "issuerRef": _issuer_ref(spec),
        "commonName": common_name,
        "duration": "8760h",
        "renewBefore": "720h",
        "privateKey": {"algorithm": "RSA", "size": 2048},
        # 클라이언트 인증 용도
        "usages": ["client auth"],
    }
    return cert
