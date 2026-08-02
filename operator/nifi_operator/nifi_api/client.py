"""
NiFi 2.x REST API 클라이언트.

NiFi 클러스터의 상태 조회, 노드 관리, Flow 배포 등을 수행하는
REST API 클라이언트입니다. 비동기(async) 기반으로 동작합니다.
"""

import httpx
import logging
import os
from typing import Optional, Union

logger = logging.getLogger(__name__)

# 코디네이터/프라이머리 판별에 쓰는 NiFi 노드 role 표시 문자열.
# NiFi REST는 role을 "Cluster Coordinator" / "Primary Node" 형식으로 반환한다.
COORDINATOR_ROLES = {"Cluster Coordinator", "Primary Node"}


class NiFiClient:
    """NiFi 2.x REST API와 통신하는 클라이언트.

    보안 클러스터(2.x 기본)는 익명 요청에 401을 반환하므로, TLS 클러스터에서는
    오퍼레이터 클라이언트 인증서로 mTLS 인증해야 한다(client_cert/ca_cert).

    Args:
        base_url: NiFi 서버 기본 URL (예: https://my-nifi-nifi-0.my-nifi-headless:8443)
        verify: SSL 검증 — CA 번들 파일 경로(str) 또는 bool. TLS 클러스터에서는
                cert-manager CA 경로를 전달해 검증을 켜는 것을 권장.
        client_cert: mTLS 클라이언트 인증서 (certfile, keyfile) 경로 튜플 또는 None.
        timeout: API 타임아웃(초)
    """

    def __init__(
        self,
        base_url: str,
        verify: Union[bool, str] = False,
        client_cert: Optional[tuple] = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.verify = verify
        self.client_cert = client_cert
        self.timeout = timeout

    def _client(self) -> httpx.AsyncClient:
        """내부 HTTP 클라이언트를 생성합니다."""
        return httpx.AsyncClient(
            base_url=self.base_url,
            verify=self.verify,
            cert=self.client_cert,
            timeout=self.timeout,
        )

    # =========================================================================
    # 클러스터 상태 조회
    # =========================================================================

    async def get_cluster_info(self) -> Optional[dict]:
        """클러스터 전체 정보를 조회합니다.

        Returns:
            클러스터 정보 딕셔너리 또는 실패 시 None.
            응답 예시: {"cluster": {"nodes": [{"nodeIdentifier": {...}, "status": "CONNECTED"}]}}
        """
        async with self._client() as client:
            try:
                resp = await client.get("/nifi-api/controller/cluster")
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"클러스터 정보 조회 실패: {e}")
                return None

    async def get_system_diagnostics(self) -> Optional[dict]:
        """시스템 진단 정보를 조회합니다 (CPU, 메모리, 디스크 등).

        Returns:
            시스템 진단 딕셔너리 또는 실패 시 None.
        """
        async with self._client() as client:
            try:
                resp = await client.get("/nifi-api/system-diagnostics")
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"시스템 진단 조회 실패: {e}")
                return None

    async def get_connected_node_count(self) -> int:
        """클러스터에 연결된(CONNECTED) 노드 수를 반환합니다.

        Returns:
            CONNECTED 상태인 노드 수. 조회 실패 시 0.
        """
        cluster_info = await self.get_cluster_info()
        if not cluster_info:
            return 0
        nodes = cluster_info.get("cluster", {}).get("nodes", [])
        return sum(1 for n in nodes if n.get("status") == "CONNECTED")

    async def get_cluster_coordinator(self) -> Optional[str]:
        """현재 클러스터 코디네이터의 주소를 반환합니다.

        클러스터 코디네이터는 클러스터 전체를 관리하는 리더 노드입니다.

        Returns:
            코디네이터 노드 주소 또는 None.
        """
        cluster_info = await self.get_cluster_info()
        if not cluster_info:
            return None
        nodes = cluster_info.get("cluster", {}).get("nodes", [])
        for node in nodes:
            roles = node.get("roles", [])
            # "Cluster Coordinator"/"Primary Node" role을 가진 노드가 코디네이터
            if COORDINATOR_ROLES.intersection(roles):
                return node.get("address")
        return None

    # =========================================================================
    # 노드 관리 (스케일링, 장애 복구에 사용)
    # =========================================================================

    async def get_node_status(self, node_id: str) -> Optional[dict]:
        """특정 노드의 상태를 조회합니다.

        Args:
            node_id: NiFi 노드 고유 식별자 (UUID 형식)

        Returns:
            노드 상태 딕셔너리 또는 실패 시 None.
        """
        async with self._client() as client:
            try:
                resp = await client.get(f"/nifi-api/controller/cluster/nodes/{node_id}")
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"노드 {node_id} 상태 조회 실패: {e}")
                return None

    async def set_node_status(self, node_id: str, status: str) -> bool:
        """노드의 상태를 변경합니다.

        안전한 스케일 다운 순서:
            1. OFFLOADING — 해당 노드의 데이터를 다른 노드로 이전
            2. DISCONNECTING — 클러스터에서 연결 해제
            3. remove_node() 호출로 클러스터에서 완전 제거

        장애 복구 시:
            - CONNECTING — 연결 끊긴 노드를 다시 클러스터에 연결

        Args:
            node_id: NiFi 노드 고유 식별자
            status: 변경할 상태 (CONNECTING, DISCONNECTING, OFFLOADING)

        Returns:
            성공 여부
        """
        async with self._client() as client:
            try:
                resp = await client.put(
                    f"/nifi-api/controller/cluster/nodes/{node_id}",
                    json={
                        "node": {
                            "nodeIdentifier": {"id": node_id},
                            "status": status,
                        }
                    },
                )
                resp.raise_for_status()
                return True
            except Exception as e:
                logger.error(f"노드 {node_id} 상태 변경 실패 ({status}): {e}")
                return False

    async def remove_node(self, node_id: str) -> bool:
        """노드를 클러스터에서 완전히 제거합니다.

        주의: 반드시 OFFLOADING → DISCONNECTING 이후에 호출해야 합니다.
              데이터 이전 없이 제거하면 FlowFile 유실이 발생할 수 있습니다.

        Args:
            node_id: 제거할 노드의 고유 식별자

        Returns:
            성공 여부
        """
        async with self._client() as client:
            try:
                resp = await client.delete(
                    f"/nifi-api/controller/cluster/nodes/{node_id}"
                )
                resp.raise_for_status()
                return True
            except Exception as e:
                logger.error(f"노드 {node_id} 제거 실패: {e}")
                return False

    async def find_node_by_address(self, address: str) -> Optional[dict]:
        """호스트 주소로 클러스터 노드를 검색합니다.

        K8s StatefulSet의 Pod DNS 이름으로 노드를 찾을 때 사용합니다.
        예: "my-nifi-nifi-2.my-nifi-headless.nifi.svc.cluster.local"

        Args:
            address: 검색할 노드 주소 (부분 일치 검색)

        Returns:
            일치하는 노드 정보 딕셔너리 또는 None.
        """
        cluster_info = await self.get_cluster_info()
        if not cluster_info:
            return None
        nodes = cluster_info.get("cluster", {}).get("nodes", [])
        for node in nodes:
            if address in node.get("address", ""):
                return node
        return None

    # =========================================================================
    # Flow 배포 (NiFi Registry 연동)
    # =========================================================================

    async def get_registry_clients(self) -> Optional[list]:
        """등록된 NiFi Registry 클라이언트 목록을 조회합니다.

        NiFi Registry는 Flow의 버전 관리를 담당하는 별도 서비스입니다.

        Returns:
            Registry 클라이언트 리스트 또는 실패 시 None.
        """
        async with self._client() as client:
            try:
                resp = await client.get("/nifi-api/controller/registry-clients")
                resp.raise_for_status()
                return resp.json().get("registries", [])
            except Exception as e:
                logger.warning(f"Registry 클라이언트 조회 실패: {e}")
                return None

    async def import_flow_from_registry(
        self,
        process_group_id: str,
        registry_id: str,
        bucket_id: str,
        flow_id: str,
        flow_version: int,
    ) -> Optional[dict]:
        """NiFi Registry에서 버전 관리된 Flow를 가져와 배포합니다.

        NiFiFlow CR이 생성되면 이 메서드를 호출하여 Flow를 자동 배포합니다.

        Args:
            process_group_id: 배포 대상 Process Group ID ("root" 또는 UUID)
            registry_id: NiFi Registry 클라이언트 ID
            bucket_id: Registry 버킷 ID
            flow_id: Flow ID
            flow_version: 배포할 Flow 버전 번호

        Returns:
            배포된 Process Group 정보 딕셔너리 또는 실패 시 None.
        """
        async with self._client() as client:
            try:
                resp = await client.post(
                    f"/nifi-api/process-groups/{process_group_id}/process-groups",
                    json={
                        "revision": {"version": 0},
                        "component": {
                            "position": {"x": 0, "y": 0},
                            "versionControlInformation": {
                                "registryId": registry_id,
                                "bucketId": bucket_id,
                                "flowId": flow_id,
                                "version": flow_version,
                            },
                        },
                    },
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.error(f"Flow 배포 실패: {e}")
                return None

    async def set_process_group_state(self, process_group_id: str, state: str) -> bool:
        """Process Group의 실행 상태를 변경합니다 (시작/중지).

        Args:
            process_group_id: Process Group ID
            state: RUNNING (시작) 또는 STOPPED (중지)

        Returns:
            성공 여부
        """
        async with self._client() as client:
            try:
                resp = await client.put(
                    f"/nifi-api/flow/process-groups/{process_group_id}",
                    json={"id": process_group_id, "state": state},
                )
                resp.raise_for_status()
                return True
            except Exception as e:
                logger.error(f"Process Group {process_group_id} 상태 변경 실패 ({state}): {e}")
                return False

    async def get_version_control_information(self, process_group_id: str) -> Optional[dict]:
        """Process Group의 버전 제어 정보(현재 버전, revision 등)를 조회합니다."""
        async with self._client() as client:
            try:
                resp = await client.get(
                    f"/nifi-api/versions/process-groups/{process_group_id}"
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"버전 정보 조회 실패 ({process_group_id}): {e}")
                return None

    async def change_flow_version(
        self,
        process_group_id: str,
        registry_id: str,
        bucket_id: str,
        flow_id: str,
        flow_version: int,
    ) -> bool:
        """배포된 Flow를 지정 버전으로 변경합니다 (NiFiFlow.flowVersion 업데이트 시).

        NiFi의 버전 변경은 비동기 update-request로 수행됩니다. 현재 revision을 조회한 뒤
        업데이트를 요청합니다.

        Returns:
            요청 성공 여부. (실제 반영 완료는 비동기이며 monitor가 상태를 추적)
        """
        vci = await self.get_version_control_information(process_group_id)
        if not vci:
            return False
        revision = vci.get("processGroupRevision", {"version": 0})

        async with self._client() as client:
            try:
                resp = await client.post(
                    f"/nifi-api/versions/update-requests/process-groups/{process_group_id}",
                    json={
                        "processGroupRevision": revision,
                        "versionControlInformation": {
                            "groupId": process_group_id,
                            "registryId": registry_id,
                            "bucketId": bucket_id,
                            "flowId": flow_id,
                            "version": flow_version,
                        },
                    },
                )
                resp.raise_for_status()
                return True
            except Exception as e:
                logger.error(
                    f"Flow 버전 변경 실패 ({process_group_id} → v{flow_version}): {e}"
                )
                return False


# =============================================================================
# 클라이언트 팩토리 — TLS 여부에 따라 URL/인증을 구성
# =============================================================================

_OPERATOR_CERT_DIR = "/tmp/nifi-operator-certs"


def _materialize_operator_cert(name: str, namespace: str, core_v1) -> Optional[tuple]:
    """오퍼레이터 클라이언트 인증서 Secret을 파일로 전개하고 (cert, key, ca) 경로를 반환.

    cluster-scoped 오퍼레이터는 클러스터별 Secret을 동적으로 마운트할 수 없으므로,
    K8s API로 읽어 임시 파일로 쓴 뒤 httpx mTLS에 사용한다.
    """
    import base64

    try:
        secret = core_v1.read_namespaced_secret(f"{name}-operator-tls", namespace)
    except Exception as e:  # noqa: BLE001 - Secret 아직 미발급 등
        logger.warning(f"오퍼레이터 인증서 Secret 로드 실패 ({name}): {e}")
        return None

    data = secret.data or {}
    if not {"tls.crt", "tls.key"}.issubset(data):
        return None

    cluster_dir = os.path.join(_OPERATOR_CERT_DIR, namespace, name)
    os.makedirs(cluster_dir, mode=0o700, exist_ok=True)
    paths = {}
    for src, fname in (("tls.crt", "tls.crt"), ("tls.key", "tls.key"), ("ca.crt", "ca.crt")):
        if src in data:
            p = os.path.join(cluster_dir, fname)
            with open(os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "wb") as f:
                f.write(base64.b64decode(data[src]))
            paths[src] = p
    return (paths["tls.crt"], paths["tls.key"], paths.get("ca.crt"))


def build_nifi_client(
    name: str, namespace: str, spec: dict, core_v1=None, ordinal: int = 0
) -> NiFiClient:
    """NiFiCluster spec에 맞는 NiFiClient를 생성한다.

    TLS 활성화 시 https(8443) + 오퍼레이터 클라이언트 인증서(mTLS)를,
    비활성화 시 http(8080)를 사용한다.

    Args:
        name: NiFiCluster 이름
        namespace: 네임스페이스
        spec: NiFiCluster spec
        core_v1: CoreV1Api (TLS 시 인증서 Secret 로드에 필요)
        ordinal: 접속할 노드 인덱스 (기본 0 — nifi-0)
    """
    headless = f"{name}-headless"
    host = f"{name}-nifi-{ordinal}.{headless}.{namespace}.svc.cluster.local"
    tls_enabled = spec.get("tls", {}).get("enabled", False)

    if not tls_enabled:
        return NiFiClient(f"http://{host}:8080")

    client_cert = None
    verify: Union[bool, str] = False
    if core_v1 is not None:
        materialized = _materialize_operator_cert(name, namespace, core_v1)
        if materialized:
            cert, key, ca = materialized
            client_cert = (cert, key)
            verify = ca if ca else False
    return NiFiClient(f"https://{host}:8443", verify=verify, client_cert=client_cert)
