"""
NiFi 클러스터 스케일링 로직.

안전한 Scale Up / Scale Down을 수행합니다.
Scale Down 시에는 NiFi가 요구하는 순서인 Disconnect → (DISCONNECTED 확인) → Offload →
(OFFLOADED 확인) → Remove를 지켜 데이터 유실을 방지합니다. 어느 단계든 실패하면
해당 노드의 Pod를 줄이지 않고 ScaleDownError로 중단합니다(데이터가 남은 Pod 삭제 방지).
"""

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from nifi_operator.nifi_api.client import node_id_of
from nifi_operator.controllers.cluster import connect_cluster, node_address

logger = logging.getLogger(__name__)

# 상태 전이 대기 (폴링 간격/최대 대기)
POLL_SECONDS = 5
DISCONNECT_TIMEOUT_SECONDS = 120
OFFLOAD_TIMEOUT_SECONDS = 1800


class ScaleDownError(Exception):
    """스케일 다운 중단 — 해당 노드의 Pod는 제거하지 않았다."""


async def scale_up(name: str, namespace: str, old_replicas: int, new_replicas: int, spec: dict):
    """클러스터를 확장합니다 (Scale Up).

    NiFi 2.x 노드는 기동 시 Kubernetes Lease로 코디네이터를 찾아 스스로 클러스터에
    조인한다. 서버 인증서는 headless 와일드카드 SAN, 노드 identity는 공유 인증서 DN
    하나이므로(certificate.py/configmap.py) 기존 노드 재시작이나 인증서 재발급이
    필요 없다. StatefulSet replicas 증가는 main.py의 on_replicas_change가 수행한다.
    """
    logger.info(f"클러스터 '{name}' 확장: {old_replicas} → {new_replicas}")


async def _wait_status(nifi, node_id: str, wanted: str, timeout: int) -> Optional[str]:
    """노드 상태가 wanted가 될 때까지 대기. 마지막으로 관측한 상태를 반환한다."""
    last = None
    for _ in range(max(1, timeout // POLL_SECONDS)):
        info = await nifi.get_node_status(node_id)
        last = (info or {}).get("node", {}).get("status")
        if last == wanted:
            return last
        await asyncio.sleep(POLL_SECONDS)
    return last


async def scale_down(
    name: str,
    namespace: str,
    old_replicas: int,
    new_replicas: int,
    spec: dict,
    core_v1=None,
    set_replicas: Optional[Callable[[int], Awaitable[None]]] = None,
):
    """클러스터를 축소합니다 (Scale Down).

    가장 높은 인덱스부터 한 노드씩 다음 순서로 제거한다:
        1. DISCONNECTING 요청 → DISCONNECTED 대기 (NiFi는 연결된 노드의 오프로드를 거부)
        2. OFFLOADING 요청 → OFFLOADED 대기 — FlowFile을 남은 노드로 이전
        3. DELETE — 클러스터 멤버십에서 제거
        4. set_replicas(ordinal) — StatefulSet을 해당 노드만큼 축소 (Pod/데이터 정리)

    노드가 클러스터 목록에 없으면(이미 제거됨/조인한 적 없음) 3단계까지 건너뛰고 Pod만
    줄인다. 1~3단계 중 하나라도 실패/타임아웃이면 ScaleDownError를 던지고 그 노드의 Pod는
    줄이지 않는다(이미 제거 완료한 상위 노드는 반영됨).

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스
        old_replicas: 변경 전 노드 수
        new_replicas: 변경 후 노드 수
        spec: NiFiCluster spec
        core_v1: CoreV1Api (TLS 인증서 로드용)
        set_replicas: StatefulSet replicas를 바꾸는 콜백 (main.py가 전달)

    Raises:
        ScaleDownError: 단계 실패 — 호출자는 replicas를 더 줄이지 말고 재시도해야 한다.
    """
    for ordinal in range(old_replicas - 1, new_replicas - 1, -1):
        address = node_address(name, namespace, ordinal)
        logger.info(f"노드 {ordinal} 제거 시작: {address}")

        # 제거 대상이 아닌 노드(0..new-1)를 통해 클러스터 API 호출
        nifi, cluster_info = await connect_cluster(
            name, namespace, spec, core_v1, ordinals=range(new_replicas)
        )
        if cluster_info is None:
            raise ScaleDownError("NiFi 클러스터 API에 연결할 수 없어 스케일 다운을 중단합니다")

        node = next(
            (n for n in cluster_info.get("cluster", {}).get("nodes", [])
             if n.get("address") == address),
            None,
        )
        if node is None:
            logger.warning(f"노드 {address}가 클러스터 목록에 없습니다 — Pod만 축소합니다.")
        else:
            node_id = node_id_of(node)
            status = node.get("status")

            # 1단계: DISCONNECT (이미 DISCONNECTED/OFFLOAD 진행/완료 상태면 건너뜀)
            if status not in ("DISCONNECTED", "OFFLOADING", "OFFLOADED"):
                logger.info(f"노드 {ordinal} ({node_id}) 연결 해제 요청 (현재 {status})")
                if not await nifi.set_node_status(node_id, "DISCONNECTING"):
                    raise ScaleDownError(f"노드 {ordinal} 연결 해제 요청 실패")
                status = await _wait_status(
                    nifi, node_id, "DISCONNECTED", DISCONNECT_TIMEOUT_SECONDS
                )
                if status != "DISCONNECTED":
                    raise ScaleDownError(
                        f"노드 {ordinal}가 {DISCONNECT_TIMEOUT_SECONDS}초 내에 DISCONNECTED가 "
                        f"되지 않았습니다 (현재 {status})"
                    )

            # 2단계: OFFLOAD — FlowFile을 다른 노드로 이전
            if status != "OFFLOADED":
                if status != "OFFLOADING":
                    logger.info(f"노드 {ordinal} 오프로드 요청")
                    if not await nifi.set_node_status(node_id, "OFFLOADING"):
                        raise ScaleDownError(f"노드 {ordinal} 오프로드 요청 실패")
                status = await _wait_status(nifi, node_id, "OFFLOADED", OFFLOAD_TIMEOUT_SECONDS)
                if status != "OFFLOADED":
                    raise ScaleDownError(
                        f"노드 {ordinal} 오프로드가 {OFFLOAD_TIMEOUT_SECONDS}초 내에 끝나지 "
                        f"않았습니다 (현재 {status}) — 데이터 보호를 위해 중단"
                    )
            logger.info(f"노드 {ordinal} 오프로드 완료")

            # 3단계: 클러스터에서 제거
            if not await nifi.remove_node(node_id):
                raise ScaleDownError(f"노드 {ordinal} 클러스터 제거 실패")
            logger.info(f"노드 {ordinal} 클러스터에서 제거됨")

        # 4단계: StatefulSet 축소 (이 노드의 Pod 종료)
        if set_replicas is not None:
            await set_replicas(ordinal)
        logger.info(f"노드 {ordinal} 제거 완료")
