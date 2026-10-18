"""
NiFi 2.x REST API 클라이언트.

NiFi 클러스터의 상태 조회, 노드 관리, Flow 배포 등을 수행하는
REST API 클라이언트입니다. 비동기(async) 기반으로 동작합니다.
"""

import asyncio
import base64
import logging
import os
import ssl
import uuid
from typing import Optional, Union

import httpx

logger = logging.getLogger(__name__)

# 코디네이터/프라이머리 판별에 쓰는 NiFi 노드 role 표시 문자열.
# NiFi REST는 role을 "Cluster Coordinator" / "Primary Node" 형식으로 반환한다.
COORDINATOR_ROLE = "Cluster Coordinator"
PRIMARY_ROLE = "Primary Node"
COORDINATOR_ROLES = {COORDINATOR_ROLE, PRIMARY_ROLE}


class NiFiClientConfigError(Exception):
    """TLS 클러스터에 접속할 클라이언트 구성을 만들 수 없음(인증서 Secret 미발급 등).

    검증 없는 TLS(verify=False)나 클라이언트 인증서 없는 요청으로 조용히 대체하지 않고
    호출자(핸들러/타이머)가 재시도하도록 예외로 알린다.
    """


class NiFiAPIError(Exception):
    """NiFi REST 호출 실패 (응답 본문의 사유를 담는다)."""


def node_id_of(node: dict) -> Optional[str]:
    """/controller/cluster 응답의 NodeDTO에서 노드 ID를 꺼낸다 (NiFi 2.x: ``nodeId``)."""
    return node.get("nodeId") or (node.get("nodeIdentifier") or {}).get("id")


class NiFiClient:
    """NiFi 2.x REST API와 통신하는 클라이언트.

    보안 클러스터(2.x 기본)는 익명 요청에 401을 반환하므로, TLS 클러스터에서는
    오퍼레이터 클라이언트 인증서로 mTLS 인증해야 한다(client_cert/ca_cert).

    Args:
        base_url: NiFi 서버 기본 URL (예: https://my-nifi-nifi-0.my-nifi-headless:8443)
        verify: SSL 검증 — ssl.SSLContext(권장: CA + 클라이언트 인증서 로드됨) 또는 bool.
                http(비-TLS) 클러스터에서는 사용되지 않는다.
        timeout: API 타임아웃(초)
    """

    def __init__(
        self,
        base_url: str,
        verify: Union[bool, ssl.SSLContext] = True,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.verify = verify
        self.timeout = timeout
        # NiFi revision 갱신에 쓰는 clientId (요청 간 동일하게 유지)
        self.client_id = str(uuid.uuid4())

    def _client(self) -> httpx.AsyncClient:
        """내부 HTTP 클라이언트를 생성합니다."""
        return httpx.AsyncClient(
            base_url=self.base_url,
            verify=self.verify,
            timeout=self.timeout,
        )

    @staticmethod
    def _reason(e: Exception) -> str:
        """HTTP 오류 응답이면 NiFi가 돌려준 본문(사유)을 포함한 메시지를 만든다."""
        if isinstance(e, httpx.HTTPStatusError):
            body = (e.response.text or "").strip().replace("\n", " ")
            return f"HTTP {e.response.status_code}: {body[:300]}"
        return f"{type(e).__name__}: {e}"

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
        return find_coordinator_address(nodes)

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

        안전한 스케일 다운 순서 (NiFi는 DISCONNECTED 노드만 오프로드할 수 있다):
            1. DISCONNECTING — 클러스터에서 연결 해제 → DISCONNECTED 대기
            2. OFFLOADING — 해당 노드의 FlowFile을 다른 노드로 이전 → OFFLOADED 대기
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
                    json={"node": {"nodeId": node_id, "status": status}},
                )
                resp.raise_for_status()
                return True
            except Exception as e:
                logger.error(f"노드 {node_id} 상태 변경 실패 ({status}): {self._reason(e)}")
                return False

    async def remove_node(self, node_id: str) -> bool:
        """노드를 클러스터에서 완전히 제거합니다.

        주의: 반드시 DISCONNECTED → OFFLOADED 이후에 호출해야 합니다.
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
                logger.error(f"노드 {node_id} 제거 실패: {self._reason(e)}")
                return False

    async def find_node_by_address(self, address: str) -> Optional[dict]:
        """호스트 주소로 클러스터 노드를 검색합니다.

        K8s StatefulSet의 Pod DNS 이름으로 노드를 찾을 때 사용합니다.
        예: "my-nifi-nifi-2.my-nifi-headless.nifi.svc.cluster.local"

        Args:
            address: 검색할 노드 주소. 정확히 일치하거나, 호스트명 레이블 경계에서
                접두어로 일치해야 한다(예: "x-nifi-1"은 "x-nifi-1.x-headless..."와 일치,
                "x-nifi-10..."과는 불일치).

        Returns:
            일치하는 노드 정보 딕셔너리 또는 None.
        """
        cluster_info = await self.get_cluster_info()
        if not cluster_info:
            return None
        nodes = cluster_info.get("cluster", {}).get("nodes", [])
        for node in nodes:
            node_addr = node.get("address", "")
            if node_addr == address or node_addr.startswith(address + "."):
                return node
        return None

    # =========================================================================
    # Flow 배포 (NiFi Registry 연동)
    # =========================================================================

    async def get_registry_clients(self) -> Optional[list]:
        """등록된 NiFi Registry 클라이언트 목록을 조회합니다.

        NiFi Registry는 Flow의 버전 관리를 담당하는 별도 서비스입니다.

        Returns:
            Registry 클라이언트 엔티티 리스트 또는 실패 시 None.
        """
        async with self._client() as client:
            try:
                resp = await client.get("/nifi-api/controller/registry-clients")
                resp.raise_for_status()
                return resp.json().get("registries", [])
            except Exception as e:
                logger.warning(f"Registry 클라이언트 조회 실패: {self._reason(e)}")
                return None

    async def get_process_group(self, process_group_id: str) -> Optional[dict]:
        """Process Group 엔티티를 조회합니다. 없으면(404) None."""
        async with self._client() as client:
            resp = await client.get(f"/nifi-api/process-groups/{process_group_id}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()

    async def list_child_process_groups(self, process_group_id: str) -> list:
        """하위 Process Group 엔티티 목록 (NiFiFlow 재시도 시 기존 배포 PG 재사용 판별용)."""
        async with self._client() as client:
            resp = await client.get(
                f"/nifi-api/process-groups/{process_group_id}/process-groups"
            )
            resp.raise_for_status()
            return resp.json().get("processGroups", [])

    async def import_flow_from_registry(
        self,
        process_group_id: str,
        registry_id: str,
        bucket_id: str,
        flow_id: str,
        flow_version: Union[int, str],
    ) -> dict:
        """NiFi Registry에서 버전 관리된 Flow를 가져와 새 하위 Process Group으로 배포합니다.

        NiFi 2.x의 VersionControlInformation.version은 문자열이다.

        Args:
            process_group_id: 배포 대상(부모) Process Group ID ("root" 또는 UUID)
            registry_id: NiFi Registry 클라이언트 ID
            bucket_id: Registry 버킷 ID
            flow_id: Flow ID
            flow_version: 배포할 Flow 버전

        Returns:
            배포된 Process Group 엔티티.

        Raises:
            NiFiAPIError: 배포 실패 (NiFi 응답 사유 포함)
        """
        async with self._client() as client:
            try:
                resp = await client.post(
                    f"/nifi-api/process-groups/{process_group_id}/process-groups",
                    json={
                        "revision": {"version": 0, "clientId": self.client_id},
                        "component": {
                            "position": {"x": 0, "y": 0},
                            "versionControlInformation": {
                                "registryId": registry_id,
                                "bucketId": bucket_id,
                                "flowId": flow_id,
                                "version": str(flow_version),
                            },
                        },
                    },
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                raise NiFiAPIError(f"Flow 배포 실패: {self._reason(e)}") from e

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
                logger.error(
                    f"Process Group {process_group_id} 상태 변경 실패 ({state}): {self._reason(e)}"
                )
                return False

    async def set_controller_services_state(self, process_group_id: str, state: str) -> bool:
        """Process Group 하위 Controller Service를 일괄 ENABLED/DISABLED 합니다."""
        async with self._client() as client:
            try:
                resp = await client.put(
                    f"/nifi-api/flow/process-groups/{process_group_id}/controller-services",
                    json={"id": process_group_id, "state": state},
                )
                resp.raise_for_status()
                return True
            except Exception as e:
                logger.error(
                    f"Process Group {process_group_id} Controller Service {state} 실패: "
                    f"{self._reason(e)}"
                )
                return False

    async def delete_process_group(self, process_group_id: str) -> None:
        """Process Group을 삭제합니다 (이미 없으면 성공으로 간주).

        호출 전 컴포넌트 중지/Controller Service 비활성화가 필요하다. 큐에 FlowFile이
        남아 있으면 NiFi가 거부(409)한다 — 데이터 보호를 위해 큐를 임의로 비우지 않는다.

        Raises:
            NiFiAPIError: 삭제 실패 (NiFi 응답 사유 포함)
        """
        pg = await self.get_process_group(process_group_id)
        if pg is None:
            return
        version = pg.get("revision", {}).get("version", 0)
        async with self._client() as client:
            try:
                resp = await client.delete(
                    f"/nifi-api/process-groups/{process_group_id}",
                    params={"version": version, "clientId": self.client_id},
                )
                if resp.status_code == 404:
                    return
                resp.raise_for_status()
            except Exception as e:
                raise NiFiAPIError(f"Process Group 삭제 실패: {self._reason(e)}") from e

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
                logger.warning(f"버전 정보 조회 실패 ({process_group_id}): {self._reason(e)}")
                return None

    async def change_flow_version(
        self,
        process_group_id: str,
        registry_id: str,
        bucket_id: str,
        flow_id: str,
        flow_version: Union[int, str],
        timeout_seconds: int = 300,
        poll_interval: float = 2.0,
    ) -> None:
        """배포된 Flow를 지정 버전으로 변경하고 완료될 때까지 기다립니다.

        NiFi의 버전 변경은 비동기 update-request로 수행된다. 요청을 만든 뒤
        ``complete``가 될 때까지 폴링하고, ``failureReason``이 있으면 실패로 본다.
        끝나면 update-request를 삭제(정리)한다.

        Raises:
            NiFiAPIError: 요청/진행 실패 또는 타임아웃
        """
        vci = await self.get_version_control_information(process_group_id)
        if not vci:
            raise NiFiAPIError(f"Process Group {process_group_id} 버전 정보 조회 실패")
        revision = dict(vci.get("processGroupRevision", {"version": 0}))
        revision["clientId"] = self.client_id

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
                            "version": str(flow_version),
                        },
                    },
                )
                resp.raise_for_status()
                request = resp.json().get("request", {})
            except Exception as e:
                raise NiFiAPIError(
                    f"Flow 버전 변경 요청 실패 ({process_group_id} → v{flow_version}): "
                    f"{self._reason(e)}"
                ) from e

            request_id = request.get("requestId")
            try:
                waited = 0.0
                while not request.get("complete"):
                    if waited >= timeout_seconds:
                        raise NiFiAPIError(
                            f"Flow 버전 변경 타임아웃 ({timeout_seconds}s, "
                            f"{request.get('percentCompleted')}% {request.get('state')})"
                        )
                    await asyncio.sleep(poll_interval)
                    waited += poll_interval
                    resp = await client.get(f"/nifi-api/versions/update-requests/{request_id}")
                    resp.raise_for_status()
                    request = resp.json().get("request", {})
                if request.get("failureReason"):
                    raise NiFiAPIError(f"Flow 버전 변경 실패: {request['failureReason']}")
            finally:
                if request_id:
                    try:
                        await client.delete(f"/nifi-api/versions/update-requests/{request_id}")
                    except Exception:  # noqa: BLE001 - 정리 실패는 무시
                        pass

    # =========================================================================
    # 접근 정책 (TLS 클러스터 초기 권한 부여)
    # =========================================================================

    async def get_root_process_group_id(self) -> str:
        """root Process Group ID (읽기 권한이 없어도 ID는 응답에 포함된다)."""
        async with self._client() as client:
            resp = await client.get("/nifi-api/flow/process-groups/root")
            resp.raise_for_status()
            return resp.json()["processGroupFlow"]["id"]

    async def ensure_policies(self, identities: list, grants: list) -> list:
        """identities(사용자 DN)가 grants의 (action, resource) 정책에 포함되도록 보장한다.

        이미 그 resource에 직접 정의된 정책이 있으면 사용자만 추가하고, 없으면(404 또는
        상위 resource에서 상속된 정책) 새로 만든다. 사용자는 authorizers.xml이 시드한
        users 목록에서 identity로 찾는다.

        Returns:
            변경(생성/갱신)한 "action resource" 목록.

        Raises:
            NiFiAPIError: 사용자 미존재 또는 정책 API 실패
        """
        changed = []
        async with self._client() as client:
            try:
                resp = await client.get("/nifi-api/tenants/users")
                resp.raise_for_status()
                by_identity = {
                    u["component"]["identity"]: u["id"] for u in resp.json().get("users", [])
                }
                missing = [i for i in identities if i not in by_identity]
                if missing:
                    raise NiFiAPIError(f"NiFi 사용자 없음: {missing}")
                user_ids = [by_identity[i] for i in identities]

                for action, resource in grants:
                    resp = await client.get(f"/nifi-api/policies/{action}{resource}")
                    policy = resp.json() if resp.status_code == 200 else None
                    if resp.status_code not in (200, 404):
                        resp.raise_for_status()
                    if policy is None or policy["component"].get("resource") != resource:
                        resp = await client.post("/nifi-api/policies", json={
                            "revision": {"version": 0, "clientId": self.client_id},
                            "component": {
                                "action": action,
                                "resource": resource,
                                "users": [{"id": uid} for uid in user_ids],
                            },
                        })
                        resp.raise_for_status()
                        changed.append(f"{action} {resource}")
                        continue
                    current = {u["id"] for u in policy["component"].get("users", [])}
                    if set(user_ids) <= current:
                        continue
                    comp = policy["component"]
                    comp["users"] = [{"id": uid} for uid in sorted(current | set(user_ids))]
                    revision = dict(policy["revision"])
                    revision["clientId"] = self.client_id
                    resp = await client.put(
                        f"/nifi-api/policies/{policy['id']}",
                        json={"revision": revision, "component": comp},
                    )
                    resp.raise_for_status()
                    changed.append(f"{action} {resource}")
            except NiFiAPIError:
                raise
            except Exception as e:
                raise NiFiAPIError(f"정책 설정 실패: {self._reason(e)}") from e
        return changed


def find_coordinator_address(nodes: list) -> Optional[str]:
    """노드 목록에서 Cluster Coordinator 주소를 찾는다 (없으면 Primary Node, 그래도 없으면 None)."""
    for role in (COORDINATOR_ROLE, PRIMARY_ROLE):
        for node in nodes:
            if role in node.get("roles", []):
                return node.get("address")
    return None


# =============================================================================
# 클라이언트 팩토리 — TLS 여부에 따라 URL/인증을 구성
# =============================================================================

_OPERATOR_CERT_DIR = "/tmp/nifi-operator-certs"


def _materialize_operator_cert(name: str, namespace: str, core_v1) -> tuple:
    """오퍼레이터 클라이언트 인증서 Secret을 파일로 전개하고 (cert, key, ca) 경로를 반환.

    cluster-scoped 오퍼레이터는 클러스터별 Secret을 동적으로 마운트할 수 없으므로,
    K8s API로 읽어 임시 파일로 쓴 뒤 httpx mTLS에 사용한다.

    Raises:
        NiFiClientConfigError: Secret이 없거나 tls.crt/tls.key/ca.crt가 아직 없음
    """
    secret_name = f"{name}-operator-tls"
    try:
        secret = core_v1.read_namespaced_secret(secret_name, namespace)
    except Exception as e:  # noqa: BLE001 - Secret 아직 미발급 등
        raise NiFiClientConfigError(
            f"오퍼레이터 인증서 Secret '{secret_name}' 로드 실패: {e}"
        ) from e

    data = secret.data or {}
    missing = {"tls.crt", "tls.key", "ca.crt"} - set(data)
    if missing:
        raise NiFiClientConfigError(
            f"오퍼레이터 인증서 Secret '{secret_name}'에 {sorted(missing)} 없음 "
            "(cert-manager 발급 대기)"
        )

    cluster_dir = os.path.join(_OPERATOR_CERT_DIR, namespace, name)
    os.makedirs(cluster_dir, mode=0o700, exist_ok=True)
    paths = {}
    for key in ("tls.crt", "tls.key", "ca.crt"):
        p = os.path.join(cluster_dir, key)
        with open(os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "wb") as f:
            f.write(base64.b64decode(data[key]))
        paths[key] = p
    return (paths["tls.crt"], paths["tls.key"], paths["ca.crt"])


def build_nifi_client(
    name: str, namespace: str, spec: dict, core_v1=None, ordinal: int = 0
) -> NiFiClient:
    """NiFiCluster spec에 맞는 NiFiClient를 생성한다.

    TLS 활성화 시 https(8443) + CA 검증 + 오퍼레이터 클라이언트 인증서(mTLS)를,
    비활성화 시 http(8080)를 사용한다. TLS인데 인증서를 준비할 수 없으면 검증 없는
    TLS나 익명 요청으로 대체하지 않고 NiFiClientConfigError를 던진다.

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

    if core_v1 is None:
        raise NiFiClientConfigError("TLS 클러스터 접속에는 CoreV1Api(인증서 Secret 로드)가 필요합니다")
    cert, key, ca = _materialize_operator_cert(name, namespace, core_v1)
    ctx = ssl.create_default_context(cafile=ca)
    ctx.load_cert_chain(cert, key)
    return NiFiClient(f"https://{host}:8443", verify=ctx)
