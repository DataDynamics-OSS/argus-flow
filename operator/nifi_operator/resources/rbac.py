"""
NiFi Pod용 RBAC 및 가용성 리소스 빌더.

NiFi 2.x의 Kubernetes 클러스터링(KubernetesLeaderElectionManager +
KubernetesConfigMapStateProvider)은 NiFi 프로세스가 자신의 네임스페이스에서
Lease와 ConfigMap을 직접 만들고 갱신할 수 있어야 한다. 따라서 오퍼레이터가
관리하는 오퍼레이터 자신의 RBAC와 별개로, **NiFi Pod 전용 ServiceAccount/Role/
RoleBinding**을 클러스터마다 생성한다.
"""

from kubernetes import client

_NODE_SA_SUFFIX = "-nifi"


def node_service_account_name(name: str) -> str:
    return f"{name}{_NODE_SA_SUFFIX}"


def _labels(name: str) -> dict:
    return {
        "app": "nifi",
        "cluster": name,
        "app.kubernetes.io/managed-by": "nifi-operator",
    }


def build_node_service_account(name: str, namespace: str) -> client.V1ServiceAccount:
    """NiFi Pod가 사용할 ServiceAccount."""
    return client.V1ServiceAccount(
        metadata=client.V1ObjectMeta(
            name=node_service_account_name(name),
            namespace=namespace,
            labels=_labels(name),
        ),
    )


def build_node_role(name: str, namespace: str) -> client.V1Role:
    """리더 선출(Lease)과 클러스터 상태(ConfigMap)를 위한 네임스페이스 Role."""
    return client.V1Role(
        metadata=client.V1ObjectMeta(
            name=f"{name}-nifi-role",
            namespace=namespace,
            labels=_labels(name),
        ),
        rules=[
            # KubernetesLeaderElectionManager
            client.V1PolicyRule(
                api_groups=["coordination.k8s.io"],
                resources=["leases"],
                verbs=["get", "list", "watch", "create", "update", "patch", "delete"],
            ),
            # KubernetesConfigMapStateProvider
            client.V1PolicyRule(
                api_groups=[""],
                resources=["configmaps"],
                verbs=["get", "list", "watch", "create", "update", "patch", "delete"],
            ),
        ],
    )


def build_node_role_binding(name: str, namespace: str) -> client.V1RoleBinding:
    """Node ServiceAccount에 Node Role을 바인딩한다."""
    return client.V1RoleBinding(
        metadata=client.V1ObjectMeta(
            name=f"{name}-nifi-rolebinding",
            namespace=namespace,
            labels=_labels(name),
        ),
        subjects=[
            client.RbacV1Subject(
                kind="ServiceAccount",
                name=node_service_account_name(name),
                namespace=namespace,
            )
        ],
        role_ref=client.V1RoleRef(
            api_group="rbac.authorization.k8s.io",
            kind="Role",
            name=f"{name}-nifi-role",
        ),
    )


def build_pod_disruption_budget(name: str, namespace: str, spec: dict):
    """클러스터 노드용 PodDisruptionBudget.

    멀티노드 클러스터에서 voluntary disruption(노드 드레인 등) 시 최소 과반이
    유지되도록 minAvailable을 과반으로 설정한다. 단일 노드에서는 생성하지 않는다
    (main.py에서 replicas<=1이면 건너뜀).
    """
    replicas = spec.get("replicas", 1)
    min_available = replicas // 2 + 1  # 과반
    return client.V1PodDisruptionBudget(
        metadata=client.V1ObjectMeta(
            name=f"{name}-nifi-pdb",
            namespace=namespace,
            labels=_labels(name),
        ),
        spec=client.V1PodDisruptionBudgetSpec(
            min_available=min_available,
            selector=client.V1LabelSelector(
                match_labels={"app": "nifi", "cluster": name}
            ),
        ),
    )
