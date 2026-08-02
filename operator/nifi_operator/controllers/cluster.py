"""
NiFi 2.x 클러스터 상태 모니터링 및 자동 복구.

주기적으로 클러스터 상태를 확인하고, 연결이 끊긴 노드를
자동으로 재연결하는 기능을 제공합니다.
"""

import asyncio
import logging

from nifi_operator.nifi_api.client import build_nifi_client, COORDINATOR_ROLES

logger = logging.getLogger(__name__)


async def check_cluster_health(name: str, namespace: str, spec: dict, core_v1=None) -> dict:
    """NiFi 클러스터의 건강 상태를 확인합니다.

    NiFi REST API를 통해 클러스터 정보를 조회하고,
    연결된 노드 수, 코디네이터 정보 등을 반환합니다.

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스

    Returns:
        상태 정보 딕셔너리:
        - readyNodes: 연결된 노드 수
        - totalNodes: 전체 노드 수
        - clusterCoordinator: 코디네이터 주소
        - phase: 현재 상태 (Running, Degraded, Error)
        - message: 상태 메시지
    """
    # 항상 첫 번째 노드(nifi-0)를 통해 API 호출
    nifi = build_nifi_client(name, namespace, spec, core_v1)

    cluster_info = await nifi.get_cluster_info()
    if not cluster_info:
        return {
            "readyNodes": 0,
            "phase": "Error",
            "message": "NiFi API 연결 실패",
        }

    nodes = cluster_info.get("cluster", {}).get("nodes", [])
    connected = sum(1 for n in nodes if n.get("status") == "CONNECTED")
    total = len(nodes)

    # 코디네이터 노드 찾기 ("Cluster Coordinator"/"Primary Node" role)
    coordinator = None
    for node in nodes:
        if COORDINATOR_ROLES.intersection(node.get("roles", [])):
            coordinator = node.get("address")
            break

    return {
        "readyNodes": connected,
        "totalNodes": total,
        "clusterCoordinator": coordinator or "unknown",
        "phase": "Running" if connected == total else "Degraded",
        "message": f"{connected}/{total} 노드 연결됨",
    }


async def auto_heal_disconnected_nodes(
    name: str, namespace: str, spec: dict, core_v1=None, protected_addresses=None
):
    """연결이 끊긴 노드를 감지하고 자동으로 재연결을 시도합니다.

    kopf 타이머(main.py)에 의해 주기적으로 호출됩니다.
    DISCONNECTED 상태의 노드를 발견하면 CONNECTING 상태로 변경하여
    클러스터에 다시 조인하도록 합니다.

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스
        spec: NiFiCluster spec
        core_v1: CoreV1Api (TLS 인증서 로드용)
        protected_addresses: 자동 복구에서 제외할 노드 주소 집합
            (스케일다운 중 의도적으로 DISCONNECT된 노드를 되살리지 않도록 함)
    """
    protected = protected_addresses or set()
    nifi = build_nifi_client(name, namespace, spec, core_v1)

    cluster_info = await nifi.get_cluster_info()
    if not cluster_info:
        return

    nodes = cluster_info.get("cluster", {}).get("nodes", [])
    for node in nodes:
        if node.get("address") in protected:
            continue
        if node.get("status") == "DISCONNECTED":
            node_id = node["nodeIdentifier"]["id"]
            node_addr = node.get("address", "unknown")
            logger.warning(f"연결 끊긴 노드 감지: {node_addr} ({node_id})")

            # CONNECTING 상태로 변경하여 재연결 시도
            success = await nifi.set_node_status(node_id, "CONNECTING")
            if success:
                logger.info(f"노드 {node_addr} 재연결 시작")
            else:
                logger.error(f"노드 {node_addr} 재연결 실패")


async def wait_for_cluster_ready(
    name: str, namespace: str, replicas: int, spec: dict, core_v1=None,
    timeout_seconds: int = 600,
) -> bool:
    """클러스터가 완전히 구성될 때까지 대기합니다.

    모든 노드가 CONNECTED 상태가 될 때까지 주기적으로 확인합니다.
    클러스터 초기 생성이나 스케일 업 후에 사용합니다.

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스
        replicas: 기대하는 노드 수
        timeout_seconds: 최대 대기 시간 (초)

    Returns:
        모든 노드가 연결되면 True, 타임아웃 시 False.
    """
    nifi = build_nifi_client(name, namespace, spec, core_v1)

    attempts = timeout_seconds // 10
    for attempt in range(attempts):
        connected = await nifi.get_connected_node_count()
        logger.info(
            f"클러스터 '{name}': {connected}/{replicas} 노드 연결됨 "
            f"(시도 {attempt + 1}/{attempts})"
        )
        if connected >= replicas:
            return True
        await asyncio.sleep(10)

    logger.warning(f"클러스터 '{name}'가 {timeout_seconds}초 내에 {replicas}개 노드에 도달하지 못했습니다")
    return False
