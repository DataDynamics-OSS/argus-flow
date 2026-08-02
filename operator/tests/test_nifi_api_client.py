"""NiFiClient REST API 클라이언트 단위 테스트."""

import pytest
import respx
import httpx

from nifi_operator.nifi_api.client import NiFiClient

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
    assert node["nodeIdentifier"]["id"] == "node-2-id"


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
