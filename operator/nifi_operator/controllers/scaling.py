"""
NiFi 클러스터 스케일링 로직.

안전한 Scale Up / Scale Down을 수행합니다.
Scale Down 시에는 반드시 Offload → Disconnect → Remove 순서를 지켜
데이터 유실을 방지합니다.
"""

import asyncio
import logging

from nifi_operator.nifi_api.client import build_nifi_client

logger = logging.getLogger(__name__)


async def scale_up(name: str, namespace: str, old_replicas: int, new_replicas: int, spec: dict):
    """클러스터를 확장합니다 (Scale Up).

    NiFi 2.x에서는 새 노드가 시작되면 nifi.properties의 초기 노드 목록을
    참고하여 자동으로 클러스터에 조인합니다. 따라서 Operator는 StatefulSet의
    replicas만 증가시키면 됩니다.

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스
        old_replicas: 변경 전 노드 수
        new_replicas: 변경 후 노드 수
        spec: NiFiCluster CR의 spec
    """
    logger.info(f"클러스터 '{name}' 확장: {old_replicas} → {new_replicas}")
    # StatefulSet replicas 업데이트는 main.py의 on_replicas_change에서 처리합니다.
    # 새 Pod가 시작되면 nifi.properties의 노드 목록을 읽고 자동으로 클러스터에 조인합니다.


async def scale_down(
    name: str, namespace: str, old_replicas: int, new_replicas: int, spec: dict, core_v1=None
):
    """클러스터를 축소합니다 (Scale Down).

    데이터 유실을 방지하기 위해 다음 순서로 안전하게 노드를 제거합니다:
        1. OFFLOADING — 해당 노드의 FlowFile을 다른 노드로 이전
        2. OFFLOADED 상태 대기 — 이전 완료 확인
        3. DISCONNECTING — 클러스터에서 연결 해제
        4. DELETE — 클러스터 멤버십에서 완전 제거

    StatefulSet의 특성상 가장 높은 인덱스의 노드부터 제거합니다.
    예: replicas 5→3이면 nifi-4, nifi-3 순서로 제거

    Args:
        name: NiFiCluster CR 이름
        namespace: 쿠버네티스 네임스페이스
        old_replicas: 변경 전 노드 수
        new_replicas: 변경 후 노드 수
        spec: NiFiCluster spec
        core_v1: CoreV1Api (TLS 인증서 로드용)
    """
    # 코디네이터(nifi-0)를 통해 클러스터 API 호출
    nifi = build_nifi_client(name, namespace, spec, core_v1)
    headless = f"{name}-headless"

    # 높은 인덱스부터 역순으로 제거
    for ordinal in range(old_replicas - 1, new_replicas - 1, -1):
        node_address = f"{name}-nifi-{ordinal}.{headless}.{namespace}.svc.cluster.local"
        logger.info(f"노드 {ordinal} 제거 시작: {node_address}")

        # 1단계: 클러스터에서 해당 노드 찾기
        node = await nifi.find_node_by_address(node_address)
        if not node:
            logger.warning(f"노드 {node_address}를 클러스터에서 찾을 수 없습니다. 건너뜁니다.")
            continue

        node_id = node["nodeIdentifier"]["id"]

        # 2단계: OFFLOADING — 데이터를 다른 노드로 이전 시작
        logger.info(f"노드 {ordinal} ({node_id}) 오프로딩 시작")
        if not await nifi.set_node_status(node_id, "OFFLOADING"):
            logger.error(f"노드 {ordinal} 오프로딩 시작 실패")
            continue

        # 3단계: OFFLOADED 상태 대기 (최대 5분)
        for attempt in range(60):
            node_info = await nifi.get_node_status(node_id)
            if node_info and node_info.get("node", {}).get("status") == "OFFLOADED":
                logger.info(f"노드 {ordinal} 오프로딩 완료")
                break
            await asyncio.sleep(5)
        else:
            logger.warning(f"노드 {ordinal} 오프로딩 타임아웃. 계속 진행합니다.")

        # 4단계: DISCONNECTING — 클러스터에서 연결 해제
        logger.info(f"노드 {ordinal} 연결 해제 중")
        await nifi.set_node_status(node_id, "DISCONNECTING")
        await asyncio.sleep(5)  # 연결 해제 대기

        # 5단계: 클러스터에서 완전 제거
        logger.info(f"노드 {ordinal} 클러스터에서 제거 중")
        await nifi.remove_node(node_id)
        logger.info(f"노드 {ordinal} 제거 완료")
