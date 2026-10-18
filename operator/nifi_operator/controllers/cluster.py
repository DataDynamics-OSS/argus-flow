"""
NiFi 2.x 클러스터 상태 모니터링 및 자동 복구.

주기적으로 클러스터 상태를 확인하고, 연결이 끊긴 노드를
자동으로 재연결하는 기능을 제공합니다.
"""

import asyncio
import datetime
import logging
import re
from typing import Iterable, Optional

from nifi_operator.resources.configmap import node_identity
from nifi_operator.nifi_api.client import (
    NiFiAPIError,
    NiFiClientConfigError,
    build_nifi_client,
    find_coordinator_address,
    node_id_of,
)

logger = logging.getLogger(__name__)


def node_address(name: str, namespace: str, ordinal: int) -> str:
    """StatefulSet Pod의 NiFi 노드 주소 (nifi.cluster.node.address와 동일)."""
    return f"{name}-nifi-{ordinal}.{name}-headless.{namespace}.svc.cluster.local"


def node_ordinal(name: str, namespace: str, address: str) -> Optional[int]:
    """노드 주소에서 StatefulSet ordinal을 추출한다. 이 클러스터의 Pod 주소가 아니면 None."""
    m = re.fullmatch(
        rf"{re.escape(name)}-nifi-(\d+)\.{re.escape(name)}-headless\.{re.escape(namespace)}"
        r"\.svc\.cluster\.local",
        address or "",
    )
    return int(m.group(1)) if m else None


async def connect_cluster(
    name: str, namespace: str, spec: dict, core_v1=None, ordinals: Optional[Iterable[int]] = None
):
    """응답하는 첫 노드를 찾아 (NiFiClient, cluster_info)를 반환한다.

    nifi-0 하나에만 의존하면 그 Pod가 재시작/장애일 때 헬스체크·자동 복구·스케일링이
    모두 멈춘다. 따라서 ordinals(기본: 0..replicas-1) 순서로 시도한다.
    모든 노드가 응답하지 않으면 (마지막 클라이언트, None).

    Raises:
        NiFiClientConfigError: TLS 클라이언트 구성 불가(인증서 Secret 미발급 등)
    """
    if ordinals is None:
        ordinals = range(max(1, spec.get("replicas", 1)))
    nifi = None
    for ordinal in ordinals:
        nifi = build_nifi_client(name, namespace, spec, core_v1, ordinal=ordinal)
        info = await nifi.get_cluster_info()
        if info:
            return nifi, info
    return nifi, None


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
        - totalNodes: 클러스터가 인지하는 전체 노드 수
        - clusterCoordinator: 코디네이터 주소
        - phase: 현재 상태 (Running, Degraded, Error)
        - message: 상태 메시지
    """
    replicas = spec.get("replicas", 1)
    _, cluster_info = await connect_cluster(name, namespace, spec, core_v1)
    if not cluster_info:
        return {
            "readyNodes": 0,
            "phase": "Error",
            "message": "NiFi API 연결 실패",
        }

    nodes = cluster_info.get("cluster", {}).get("nodes", [])
    connected = sum(1 for n in nodes if n.get("status") == "CONNECTED")
    total = len(nodes)
    coordinator = find_coordinator_address(nodes)

    # 기대 노드 수(spec.replicas)가 모두 CONNECTED여야 Running
    healthy = connected == replicas and total == replicas
    not_connected = [
        f"{n.get('address')}={n.get('status')}" for n in nodes if n.get("status") != "CONNECTED"
    ]
    message = f"{connected}/{replicas} 노드 연결됨"
    if not_connected:
        message += f" ({', '.join(not_connected)})"
    return {
        "readyNodes": connected,
        "totalNodes": total,
        "clusterCoordinator": coordinator or "unknown",
        "phase": "Running" if healthy else "Degraded",
        "message": message,
    }


async def ensure_root_policies(name: str, namespace: str, spec: dict, core_v1=None) -> list:
    """TLS 클러스터의 root Process Group 정책을 Initial Admin(오퍼레이터)과 노드에 부여한다.

    NiFi는 Initial Admin에게 root PG 컴포넌트 정책을 "최초 기동 시 flow가 이미 있을 때만"
    시드한다. 새 클러스터는 flow가 없는 상태로 시작하므로 오퍼레이터(관리자)도 root PG에
    컴포넌트를 만들 수 없다(403 "No applicable policies"). NiFiFlow 배포와 관리자 사용을
    위해 다음 정책을 보장한다:
      - 관리자: read/write /process-groups/{root}, read/write /data/process-groups/{root},
        write /operation/process-groups/{root}
      - 노드 identity: read/write /data/process-groups/{root} (노드가 프록시하는 큐 조회/
        비우기 등 data 요청은 프록시 체인 전체에 data 권한이 필요)

    Returns:
        변경한 정책 목록 (이미 모두 있으면 빈 목록)
    """
    nifi, info = await connect_cluster(name, namespace, spec, core_v1)
    if info is None:
        raise NiFiAPIError("NiFi 클러스터 API에 연결할 수 없습니다")
    root = await nifi.get_root_process_group_id()
    admin = spec.get("tls", {}).get("initialAdminIdentity") or f"CN={name}-operator"
    changed = await nifi.ensure_policies([admin], [
        ("read", f"/process-groups/{root}"),
        ("write", f"/process-groups/{root}"),
        ("read", f"/data/process-groups/{root}"),
        ("write", f"/data/process-groups/{root}"),
        ("write", f"/operation/process-groups/{root}"),
    ])
    changed += await nifi.ensure_policies([node_identity(name, namespace)], [
        ("read", f"/data/process-groups/{root}"),
        ("write", f"/data/process-groups/{root}"),
    ])
    return changed


def _event_time(event: dict) -> datetime.datetime:
    """NiFi 노드 이벤트 timestamp("MM/dd/yyyy HH:mm:ss UTC")를 datetime으로. 실패 시 최소값."""
    try:
        return datetime.datetime.strptime(event.get("timestamp", ""), "%m/%d/%Y %H:%M:%S %Z")
    except ValueError:
        return datetime.datetime.min


def _is_user_disconnected(node: dict) -> bool:
    """사용자(관리자)가 UI/API로 직접 연결 해제한 노드인지 판별한다.

    NiFi 2.x는 노드 이벤트에 연결 해제 사유를 남긴다. 사용자 요청이면
    "Node disconnected due to User <DN> requested that node be disconnected from cluster"
    형태다(k3s 통합 테스트에서 확인). 가장 최근 이벤트가 이 형태면 자동 복구하지 않는다.
    하트비트 누락("Have not received a heartbeat ...")이나 노드 종료로 끊긴 경우는 대상이다.
    """
    events = node.get("events") or []
    if not events:
        return False
    latest = max(events, key=_event_time)
    msg = latest.get("message") or ""
    return "requested that node be disconnected" in msg and "User " in msg


async def auto_heal_disconnected_nodes(
    name: str, namespace: str, spec: dict, core_v1=None, protected_addresses=None
) -> list:
    """연결이 끊긴 노드를 감지하고 자동으로 재연결을 시도합니다.

    kopf 타이머(main.py)에 의해 주기적으로 호출됩니다. 다음 노드는 건드리지 않습니다:
      - DISCONNECTED가 아닌 노드 (OFFLOADING/OFFLOADED 포함 — 스케일 다운/오프로드 중)
      - StatefulSet ordinal ≥ spec.replicas (축소 대상/잔여 노드) 또는 이 클러스터 Pod가 아닌 주소
      - 관리자가 직접 연결 해제한 노드 (_is_user_disconnected)
      - protected_addresses에 포함된 노드

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스
        spec: NiFiCluster spec
        core_v1: CoreV1Api (TLS 인증서 로드용)
        protected_addresses: 자동 복구에서 제외할 노드 주소 집합

    Returns:
        재연결을 요청한 노드 주소 목록.
    """
    protected = protected_addresses or set()
    replicas = spec.get("replicas", 1)
    nifi, cluster_info = await connect_cluster(name, namespace, spec, core_v1)
    if not cluster_info:
        return []

    reconnected = []
    for node in cluster_info.get("cluster", {}).get("nodes", []):
        node_addr = node.get("address", "unknown")
        if node.get("status") != "DISCONNECTED" or node_addr in protected:
            continue
        ordinal = node_ordinal(name, namespace, node_addr)
        if ordinal is None or ordinal >= replicas:
            logger.info(f"노드 {node_addr}: 관리 대상 ordinal 범위 밖 — 자동 복구 건너뜀")
            continue
        if _is_user_disconnected(node):
            logger.info(f"노드 {node_addr}: 관리자가 연결 해제한 노드 — 자동 복구 건너뜀")
            continue

        node_id = node_id_of(node)
        logger.warning(f"연결 끊긴 노드 감지: {node_addr} ({node_id})")
        # CONNECTING 상태로 변경하여 재연결 시도
        if await nifi.set_node_status(node_id, "CONNECTING"):
            logger.info(f"노드 {node_addr} 재연결 시작")
            reconnected.append(node_addr)
        else:
            logger.error(f"노드 {node_addr} 재연결 실패")
    return reconnected


async def wait_for_cluster_ready(
    name: str, namespace: str, replicas: int, spec: dict, core_v1=None,
    timeout_seconds: int = 600, stable_checks: int = 1,
) -> bool:
    """클러스터가 완전히 구성될 때까지 대기합니다.

    모든 노드가 CONNECTED 상태가 될 때까지 주기적으로 확인합니다.
    스케일 업 후에 사용합니다.

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스
        replicas: 기대하는 노드 수
        timeout_seconds: 최대 대기 시간 (초)
        stable_checks: 연속으로 이 횟수(10초 간격)만큼 모두 연결되어야 성공으로 본다.
            코디네이터 노드 재시작 직후 새 코디네이터가 하트비트 누락으로 노드를 잠시
            끊는 현상(k3s 테스트에서 관측)을 롤링 재시작이 지나치지 않도록 한다.

    Returns:
        모든 노드가 연결되면 True, 타임아웃 시 False.
    """
    attempts = max(1, timeout_seconds // 10)
    streak = 0
    for attempt in range(attempts):
        connected = 0
        try:
            _, info = await connect_cluster(name, namespace, spec, core_v1)
            nodes = (info or {}).get("cluster", {}).get("nodes", [])
            connected = sum(1 for n in nodes if n.get("status") == "CONNECTED")
        except NiFiClientConfigError as e:
            logger.info(f"클러스터 '{name}': 클라이언트 준비 대기 ({e})")
        logger.info(
            f"클러스터 '{name}': {connected}/{replicas} 노드 연결됨 "
            f"(시도 {attempt + 1}/{attempts})"
        )
        streak = streak + 1 if connected >= replicas else 0
        if streak >= stable_checks:
            return True
        await asyncio.sleep(10)

    logger.warning(f"클러스터 '{name}'가 {timeout_seconds}초 내에 {replicas}개 노드에 도달하지 못했습니다")
    return False
