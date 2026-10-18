"""NiFiClient REST API 클라이언트 단위 테스트."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import respx
import httpx

from nifi_operator.nifi_api.client import (
    NiFiAPIError,
    NiFiClient,
    NiFiClientConfigError,
    build_nifi_client,
    node_id_of,
)

BASE_URL = "https://nifi.test:8443"


@pytest.fixture
def nifi_client():
    """테스트용 NiFiClient 인스턴스 생성."""
    return NiFiClient(base_url=BASE_URL)


@pytest.fixture
def cluster_response(sample_cluster_info):
    """클러스터 응답 데이터를 conftest에서 가져옴."""
    return sample_cluster_info


# ---- get_cluster_info 테스트 ----


@respx.mock
async def test_get_cluster_info_성공(nifi_client, cluster_response):
    """정상적으로 클러스터 정보를 가져오는 경우를 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(200, json=cluster_response)
    )
    result = await nifi_client.get_cluster_info()
    assert result is not None
    assert "cluster" in result
    assert len(result["cluster"]["nodes"]) == 3


@respx.mock
async def test_get_cluster_info_실패(nifi_client):
    """API 호출이 실패하면 None을 반환하는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    result = await nifi_client.get_cluster_info()
    assert result is None


@respx.mock
async def test_get_cluster_info_네트워크_오류(nifi_client):
    """네트워크 오류 시 None을 반환하는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        side_effect=httpx.ConnectError("Connection refused")
    )
    result = await nifi_client.get_cluster_info()
    assert result is None


# ---- get_connected_node_count 테스트 ----


@respx.mock
async def test_get_connected_node_count_전체_연결(nifi_client, cluster_response):
    """모든 노드가 CONNECTED일 때 정확한 수를 반환하는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(200, json=cluster_response)
    )
    count = await nifi_client.get_connected_node_count()
    assert count == 3


@respx.mock
async def test_get_connected_node_count_일부_연결끊김(nifi_client, disconnected_cluster_info):
    """일부 노드가 DISCONNECTED일 때 CONNECTED 노드만 세는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(200, json=disconnected_cluster_info)
    )
    count = await nifi_client.get_connected_node_count()
    assert count == 1


@respx.mock
async def test_get_connected_node_count_API_실패시_0(nifi_client):
    """API 실패 시 0을 반환하는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(500)
    )
    count = await nifi_client.get_connected_node_count()
    assert count == 0


# ---- find_node_by_address 테스트 ----


@respx.mock
async def test_find_node_by_address_노드_발견(nifi_client, cluster_response):
    """주소로 노드를 찾을 수 있는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(200, json=cluster_response)
    )
    node = await nifi_client.find_node_by_address("test-nifi-1")
    assert node is not None
    assert node["nodeId"] == "node-2-id"


@respx.mock
async def test_find_node_by_address_노드_없음(nifi_client, cluster_response):
    """존재하지 않는 주소로 검색 시 None을 반환하는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(200, json=cluster_response)
    )
    node = await nifi_client.find_node_by_address("nonexistent-node")
    assert node is None


@respx.mock
async def test_find_node_by_address_API_실패(nifi_client):
    """API 실패 시 None을 반환하는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(500)
    )
    node = await nifi_client.find_node_by_address("test-nifi-0")
    assert node is None


# ---- set_node_status 테스트 ----


@respx.mock
async def test_set_node_status_성공(nifi_client):
    """노드 상태 변경이 성공하면 True를 반환하는지 테스트."""
    respx.put(f"{BASE_URL}/nifi-api/controller/cluster/nodes/node-1-id").mock(
        return_value=httpx.Response(200, json={})
    )
    result = await nifi_client.set_node_status("node-1-id", "OFFLOADING")
    assert result is True


@respx.mock
async def test_set_node_status_실패(nifi_client):
    """노드 상태 변경이 실패하면 False를 반환하는지 테스트."""
    respx.put(f"{BASE_URL}/nifi-api/controller/cluster/nodes/node-1-id").mock(
        return_value=httpx.Response(409, text="Conflict")
    )
    result = await nifi_client.set_node_status("node-1-id", "OFFLOADING")
    assert result is False


# ---- remove_node 테스트 ----


@respx.mock
async def test_remove_node_성공(nifi_client):
    """노드 제거가 성공하면 True를 반환하는지 테스트."""
    respx.delete(f"{BASE_URL}/nifi-api/controller/cluster/nodes/node-1-id").mock(
        return_value=httpx.Response(200)
    )
    result = await nifi_client.remove_node("node-1-id")
    assert result is True


@respx.mock
async def test_remove_node_실패(nifi_client):
    """노드 제거가 실패하면 False를 반환하는지 테스트."""
    respx.delete(f"{BASE_URL}/nifi-api/controller/cluster/nodes/node-1-id").mock(
        return_value=httpx.Response(404)
    )
    result = await nifi_client.remove_node("node-1-id")
    assert result is False


# ---- get_cluster_coordinator 테스트 ----


@respx.mock
async def test_get_cluster_coordinator_조정자_존재(nifi_client, cluster_response):
    """클러스터 코디네이터 주소를 올바르게 반환하는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(200, json=cluster_response)
    )
    coordinator = await nifi_client.get_cluster_coordinator()
    assert coordinator == "test-nifi-0.test-headless.default.svc.cluster.local"


@respx.mock
async def test_get_cluster_coordinator_조정자_없음(nifi_client):
    """코디네이터 역할이 없는 경우 None을 반환하는지 테스트."""
    no_coordinator = {
        "cluster": {
            "nodes": [
                {
                    "nodeIdentifier": {"id": "node-1-id"},
                    "address": "test-nifi-0.test-headless.default.svc.cluster.local",
                    "status": "CONNECTED",
                    "roles": [],
                }
            ]
        }
    }
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(200, json=no_coordinator)
    )
    coordinator = await nifi_client.get_cluster_coordinator()
    assert coordinator is None


@respx.mock
async def test_get_cluster_coordinator_API_실패(nifi_client):
    """API 실패 시 None을 반환하는지 테스트."""
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(500)
    )
    coordinator = await nifi_client.get_cluster_coordinator()
    assert coordinator is None


@respx.mock
async def test_find_node_by_address_레이블_경계(nifi_client):
    """'x-nifi-1'이 'x-nifi-10...'과 일치하면 안 된다 (부분 문자열 오탐 방지)."""
    info = {"cluster": {"nodes": [
        {"nodeId": "n10", "address": "test-nifi-10.test-headless.default.svc.cluster.local"},
        {"nodeId": "n1", "address": "test-nifi-1.test-headless.default.svc.cluster.local"},
    ]}}
    respx.get(f"{BASE_URL}/nifi-api/controller/cluster").mock(
        return_value=httpx.Response(200, json=info)
    )
    node = await nifi_client.find_node_by_address("test-nifi-1")
    assert node["nodeId"] == "n1"


@respx.mock
async def test_set_node_status_요청본문은_nodeId(nifi_client):
    """NiFi 2.x NodeDTO 필드는 nodeId다 (nodeIdentifier 아님)."""
    route = respx.put(f"{BASE_URL}/nifi-api/controller/cluster/nodes/node-1-id").mock(
        return_value=httpx.Response(200, json={})
    )
    await nifi_client.set_node_status("node-1-id", "DISCONNECTING")
    body = json.loads(route.calls.last.request.content)
    assert body == {"node": {"nodeId": "node-1-id", "status": "DISCONNECTING"}}


def test_node_id_of():
    assert node_id_of({"nodeId": "a"}) == "a"
    assert node_id_of({"nodeIdentifier": {"id": "b"}}) == "b"
    assert node_id_of({}) is None


# ---- build_nifi_client (TLS 구성) ----


class _Secret:
    def __init__(self, data):
        self.data = data


def test_build_client_비TLS는_http():
    c = build_nifi_client("t", "ns", {"tls": {"enabled": False}})
    assert c.base_url == "http://t-nifi-0.t-headless.ns.svc.cluster.local:8080"


def test_build_client_TLS_core_v1_없으면_오류():
    """검증 없는 TLS/익명 요청으로 조용히 대체하지 않아야 한다."""
    with pytest.raises(NiFiClientConfigError):
        build_nifi_client("t", "ns", {"tls": {"enabled": True}})


def test_build_client_TLS_Secret_미발급이면_오류():
    core = MagicMock()
    core.read_namespaced_secret.side_effect = Exception("404 Not Found")
    with pytest.raises(NiFiClientConfigError, match="t-operator-tls"):
        build_nifi_client("t", "ns", {"tls": {"enabled": True}}, core)


def test_build_client_TLS_CA_없으면_오류():
    core = MagicMock()
    core.read_namespaced_secret.return_value = _Secret({"tls.crt": "eA==", "tls.key": "eA=="})
    with pytest.raises(NiFiClientConfigError, match="ca.crt"):
        build_nifi_client("t", "ns", {"tls": {"enabled": True}}, core)


def test_build_client_TLS_mTLS_컨텍스트(tmp_path, monkeypatch):
    """CA 검증 + 클라이언트 인증서를 로드한 SSLContext를 쓴다."""
    import nifi_operator.nifi_api.client as mod

    monkeypatch.setattr(mod, "_OPERATOR_CERT_DIR", str(tmp_path))
    core = MagicMock()
    core.read_namespaced_secret.return_value = _Secret(
        {"tls.crt": "Y2VydA==", "tls.key": "a2V5", "ca.crt": "Y2E="}
    )
    ctx = MagicMock()
    with patch.object(mod.ssl, "create_default_context", return_value=ctx) as cdc:
        c = build_nifi_client("t", "ns", {"tls": {"enabled": True}}, core, ordinal=2)
    assert c.base_url == "https://t-nifi-2.t-headless.ns.svc.cluster.local:8443"
    assert c.verify is ctx
    cdc.assert_called_once_with(cafile=str(tmp_path / "ns" / "t" / "ca.crt"))
    ctx.load_cert_chain.assert_called_once_with(
        str(tmp_path / "ns" / "t" / "tls.crt"), str(tmp_path / "ns" / "t" / "tls.key")
    )
    assert (tmp_path / "ns" / "t" / "tls.key").read_bytes() == b"key"


# ---- Flow / 정책 API ----


@respx.mock
async def test_import_flow_버전은_문자열(nifi_client):
    """NiFi 2.x VersionControlInformation.version은 문자열이다."""
    route = respx.post(f"{BASE_URL}/nifi-api/process-groups/root/process-groups").mock(
        return_value=httpx.Response(201, json={"id": "pg-1"})
    )
    result = await nifi_client.import_flow_from_registry("root", "r", "b", "f", 3)
    assert result["id"] == "pg-1"
    vci = json.loads(route.calls.last.request.content)["component"]["versionControlInformation"]
    assert vci == {"registryId": "r", "bucketId": "b", "flowId": "f", "version": "3"}


@respx.mock
async def test_import_flow_실패시_예외(nifi_client):
    respx.post(f"{BASE_URL}/nifi-api/process-groups/root/process-groups").mock(
        return_value=httpx.Response(409, text="bucket not found")
    )
    with pytest.raises(NiFiAPIError, match="bucket not found"):
        await nifi_client.import_flow_from_registry("root", "r", "b", "f", 1)


@respx.mock
async def test_change_flow_version_완료까지_대기(nifi_client, monkeypatch):
    """update-request가 complete가 될 때까지 폴링하고 끝나면 요청을 삭제한다."""
    monkeypatch.setattr("nifi_operator.nifi_api.client.asyncio.sleep", AsyncMock())
    respx.get(f"{BASE_URL}/nifi-api/versions/process-groups/pg").mock(
        return_value=httpx.Response(200, json={"processGroupRevision": {"version": 4}})
    )
    respx.post(f"{BASE_URL}/nifi-api/versions/update-requests/process-groups/pg").mock(
        return_value=httpx.Response(200, json={"request": {"requestId": "req", "complete": False}})
    )
    poll = respx.get(f"{BASE_URL}/nifi-api/versions/update-requests/req").mock(side_effect=[
        httpx.Response(200, json={"request": {"requestId": "req", "complete": False}}),
        httpx.Response(200, json={"request": {"requestId": "req", "complete": True}}),
    ])
    cleanup = respx.delete(f"{BASE_URL}/nifi-api/versions/update-requests/req").mock(
        return_value=httpx.Response(200, json={})
    )
    await nifi_client.change_flow_version("pg", "r", "b", "f", 2)
    assert poll.call_count == 2
    assert cleanup.called


@respx.mock
async def test_change_flow_version_failureReason이면_예외(nifi_client):
    respx.get(f"{BASE_URL}/nifi-api/versions/process-groups/pg").mock(
        return_value=httpx.Response(200, json={"processGroupRevision": {"version": 1}})
    )
    respx.post(f"{BASE_URL}/nifi-api/versions/update-requests/process-groups/pg").mock(
        return_value=httpx.Response(200, json={"request": {
            "requestId": "req", "complete": True, "failureReason": "component is running"}})
    )
    respx.delete(f"{BASE_URL}/nifi-api/versions/update-requests/req").mock(
        return_value=httpx.Response(200, json={})
    )
    with pytest.raises(NiFiAPIError, match="component is running"):
        await nifi_client.change_flow_version("pg", "r", "b", "f", 2)


@respx.mock
async def test_delete_process_group_revision_사용(nifi_client):
    respx.get(f"{BASE_URL}/nifi-api/process-groups/pg").mock(
        return_value=httpx.Response(200, json={"id": "pg", "revision": {"version": 7}})
    )
    route = respx.delete(f"{BASE_URL}/nifi-api/process-groups/pg").mock(
        return_value=httpx.Response(200, json={})
    )
    await nifi_client.delete_process_group("pg")
    assert route.calls.last.request.url.params["version"] == "7"


@respx.mock
async def test_delete_process_group_없으면_성공(nifi_client):
    respx.get(f"{BASE_URL}/nifi-api/process-groups/pg").mock(return_value=httpx.Response(404))
    await nifi_client.delete_process_group("pg")


@respx.mock
async def test_delete_process_group_큐_데이터면_예외(nifi_client):
    respx.get(f"{BASE_URL}/nifi-api/process-groups/pg").mock(
        return_value=httpx.Response(200, json={"id": "pg", "revision": {"version": 1}})
    )
    respx.delete(f"{BASE_URL}/nifi-api/process-groups/pg").mock(
        return_value=httpx.Response(409, text="Cannot delete Process Group because it contains 5 FlowFiles")
    )
    with pytest.raises(NiFiAPIError, match="FlowFiles"):
        await nifi_client.delete_process_group("pg")


@respx.mock
async def test_ensure_policies_생성_갱신_유지(nifi_client):
    """없는 정책은 생성, 사용자 빠진 정책은 갱신, 상속 정책은 해당 resource로 새로 생성."""
    respx.get(f"{BASE_URL}/nifi-api/tenants/users").mock(return_value=httpx.Response(200, json={
        "users": [{"id": "u1", "component": {"identity": "CN=admin"}}]}))
    respx.get(f"{BASE_URL}/nifi-api/policies/read/process-groups/root-id").mock(
        return_value=httpx.Response(404))
    respx.get(f"{BASE_URL}/nifi-api/policies/write/process-groups/root-id").mock(
        return_value=httpx.Response(200, json={
            "id": "p2", "revision": {"version": 3},
            "component": {"id": "p2", "resource": "/process-groups/root-id", "action": "write", "users": []}}))
    respx.get(f"{BASE_URL}/nifi-api/policies/read/data/process-groups/root-id").mock(
        return_value=httpx.Response(200, json={
            "id": "p3", "revision": {"version": 1},
            "component": {"id": "p3", "resource": "/data/process-groups/root-id", "action": "read",
                          "users": [{"id": "u1"}]}}))
    respx.get(f"{BASE_URL}/nifi-api/policies/write/data/process-groups/root-id").mock(
        return_value=httpx.Response(200, json={
            "id": "p0", "revision": {"version": 1},
            "component": {"id": "p0", "resource": "/data", "action": "write", "users": []}}))
    create = respx.post(f"{BASE_URL}/nifi-api/policies").mock(return_value=httpx.Response(201, json={}))
    update = respx.put(f"{BASE_URL}/nifi-api/policies/p2").mock(return_value=httpx.Response(200, json={}))

    changed = await nifi_client.ensure_policies(["CN=admin"], [
        ("read", "/process-groups/root-id"),
        ("write", "/process-groups/root-id"),
        ("read", "/data/process-groups/root-id"),
        ("write", "/data/process-groups/root-id"),
    ])
    assert changed == [
        "read /process-groups/root-id",
        "write /process-groups/root-id",
        "write /data/process-groups/root-id",
    ]
    assert create.call_count == 2
    assert json.loads(update.calls.last.request.content)["component"]["users"] == [{"id": "u1"}]


@respx.mock
async def test_ensure_policies_사용자_없으면_예외(nifi_client):
    respx.get(f"{BASE_URL}/nifi-api/tenants/users").mock(
        return_value=httpx.Response(200, json={"users": []}))
    with pytest.raises(NiFiAPIError, match="CN=admin"):
        await nifi_client.ensure_policies(["CN=admin"], [("read", "/x")])
