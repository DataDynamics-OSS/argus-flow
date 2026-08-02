"""
NiFi 클러스터 자격 증명 Secret 빌더.

민감 프로퍼티 암호화 키와 keystore/truststore 비밀번호를 담는다. 이 값들은
ConfigMap에 평문으로 두면 안 되고, 한번 생성되면 절대 바뀌면 안 된다
(sensitive.props.key가 바뀌면 기존 암호화된 값 복호화 불가). 따라서 main.py는
이 Secret을 create-if-not-exists로만 다루고 갱신하지 않는다.

keystore 비밀번호는 cert-manager가 PKCS12를 생성할 때(certificate.py의
passwordSecretRef)와 NiFi가 그 PKCS12를 열 때 모두 같은 값을 참조해야 하므로
동일 Secret의 keystore-password 키를 공유한다.
"""

import secrets

from kubernetes import client


def _token(nbytes: int = 24) -> str:
    """URL-safe 랜덤 토큰(영숫자+-_)을 생성한다. sed 치환에 안전한 문자만 사용."""
    return secrets.token_urlsafe(nbytes)


def build_credentials_secret(name: str, namespace: str, spec: dict) -> client.V1Secret:
    """NiFi 클러스터용 자격 증명 Secret을 생성한다.

    - sensitive-props-key: nifi.sensitive.props.key (spec.config.sensitivePropsKey 우선)
    - keystore-password: keystore/truststore PKCS12 비밀번호 (cert-manager와 공유)

    Args:
        name: NiFiCluster CR 이름
        namespace: 네임스페이스
        spec: NiFiCluster spec

    Returns:
        V1Secret (stringData 사용)
    """
    config = spec.get("config", {})
    sensitive_key = config.get("sensitivePropsKey") or _token(24)

    return client.V1Secret(
        metadata=client.V1ObjectMeta(
            name=f"{name}-credentials",
            namespace=namespace,
            labels={
                "app": "nifi",
                "cluster": name,
                "app.kubernetes.io/managed-by": "nifi-operator",
            },
        ),
        type="Opaque",
        string_data={
            "sensitive-props-key": sensitive_key,
            "keystore-password": _token(18),
        },
    )
