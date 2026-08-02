"""
NiFi 클러스터용 Kubernetes Service 빌더.

Headless Service: 클러스터 노드 간 통신 및 DNS 기반 디스커버리
UI Service: NiFi 웹 UI 외부 접근용
"""

from kubernetes import client


def build_headless_service(name: str, namespace: str) -> client.V1Service:
    """NiFi 노드 디스커버리를 위한 Headless Service를 생성합니다.

    Headless Service (ClusterIP: None)는 각 Pod에 대해
    개별 DNS 레코드를 생성합니다.
    예: my-nifi-nifi-0.my-nifi-headless.nifi.svc.cluster.local

    NiFi 2.x는 이 DNS를 통해 다른 노드를 발견하고 클러스터를 구성합니다.
    publishNotReadyAddresses=True로 설정하여 Pod이 Ready가 아닌
    상태에서도 DNS에 등록되도록 합니다 (부트스트랩 시 필요).

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스

    Returns:
        V1Service 객체
    """
    return client.V1Service(
        metadata=client.V1ObjectMeta(
            name=f"{name}-headless",
            namespace=namespace,
            labels={
                "app": "nifi",
                "cluster": name,
                "app.kubernetes.io/managed-by": "nifi-operator",
            },
        ),
        spec=client.V1ServiceSpec(
            cluster_ip="None",  # Headless Service
            selector={"app": "nifi", "cluster": name},
            ports=[
                # NiFi 웹 UI (HTTPS)
                client.V1ServicePort(name="https", port=8443, target_port=8443),
                # 클러스터 노드 간 통신 프로토콜
                client.V1ServicePort(name="cluster", port=11443, target_port=11443),
                # FlowFile 로드 밸런싱
                client.V1ServicePort(name="load-balance", port=6342, target_port=6342),
            ],
            # Pod이 Ready가 아니어도 DNS에 등록 (클러스터 부트스트랩 시 필요)
            publish_not_ready_addresses=True,
        ),
    )


def build_ui_service(name: str, namespace: str, spec: dict) -> client.V1Service:
    """NiFi 웹 UI 접근을 위한 ClusterIP Service를 생성합니다.

    kubectl port-forward 또는 Ingress를 통해 접근할 수 있습니다.

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스
        spec: NiFiCluster CR의 spec

    Returns:
        V1Service 객체
    """
    return client.V1Service(
        metadata=client.V1ObjectMeta(
            name=f"{name}-ui",
            namespace=namespace,
            labels={
                "app": "nifi",
                "cluster": name,
                "app.kubernetes.io/managed-by": "nifi-operator",
            },
        ),
        spec=client.V1ServiceSpec(
            type="ClusterIP",
            selector={"app": "nifi", "cluster": name},
            ports=[
                # NiFi 웹 UI (HTTPS)
                client.V1ServicePort(name="https", port=8443, target_port=8443),
            ],
        ),
    )
