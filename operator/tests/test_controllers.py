"""클러스터 컨트롤러 및 스케일링 로직 단위 테스트.

NiFiClient를 AsyncMock으로 대체하여 실제 NiFi 클러스터 없이 테스트.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from nifi_operator.controllers.cluster import (
    check_cluster_health,
    auto_heal_disconnected_nodes,
)
from nifi_operator.controllers.scaling import scale_down


# ---- check_cluster_health 테스트 ----


class TestCheckClusterHealth:
    """클러스터 헬스 체크 로직 테스트 클래스."""

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_정상_클러스터_상태(self, MockClient, sample_cluster_info):
        """모든 노드가 연결된 경우 Running 상태를 반환하는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.get_cluster_info = AsyncMock(return_value=sample_cluster_info)
        MockClient.return_value = mock_instance

        result = await check_cluster_health("test", "default", {})
        assert result["readyNodes"] == 3
        assert result["totalNodes"] == 3
        assert result["phase"] == "Running"
        assert "3/3" in result["message"]

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_열화_클러스터_상태(self, MockClient, disconnected_cluster_info):
        """일부 노드가 DISCONNECTED인 경우 Degraded 상태를 반환하는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.get_cluster_info = AsyncMock(return_value=disconnected_cluster_info)
        MockClient.return_value = mock_instance

        result = await check_cluster_health("test", "default", {})
        assert result["readyNodes"] == 1
        assert result["totalNodes"] == 2
        assert result["phase"] == "Degraded"

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_API_연결_실패(self, MockClient):
        """API 연결 실패 시 Error 상태를 반환하는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.get_cluster_info = AsyncMock(return_value=None)
        MockClient.return_value = mock_instance

        result = await check_cluster_health("test", "default", {})
        assert result["readyNodes"] == 0
        assert result["phase"] == "Error"

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_코디네이터_주소_반환(self, MockClient, sample_cluster_info):
        """클러스터 코디네이터 주소가 올바르게 포함되는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.get_cluster_info = AsyncMock(return_value=sample_cluster_info)
        MockClient.return_value = mock_instance

        result = await check_cluster_health("test", "default", {})
        assert result["clusterCoordinator"] == "test-nifi-0.test-headless.default.svc.cluster.local"


# ---- auto_heal_disconnected_nodes 테스트 ----


class TestAutoHealDisconnectedNodes:
    """자동 복구 로직 테스트 클래스."""

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_연결끊긴_노드_재연결_시도(self, MockClient, disconnected_cluster_info):
        """DISCONNECTED 노드에 대해 CONNECTING 상태 변경을 시도하는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.get_cluster_info = AsyncMock(return_value=disconnected_cluster_info)
        mock_instance.set_node_status = AsyncMock(return_value=True)
        MockClient.return_value = mock_instance

        await auto_heal_disconnected_nodes("test", "default", {})

        # DISCONNECTED 노드(node-2-id)에 대해 CONNECTING 호출이 있어야 함
        mock_instance.set_node_status.assert_called_once_with("node-2-id", "CONNECTING")

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_모든_노드_연결됨_재연결_불필요(self, MockClient, sample_cluster_info):
        """모든 노드가 CONNECTED면 set_node_status가 호출되지 않는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.get_cluster_info = AsyncMock(return_value=sample_cluster_info)
        mock_instance.set_node_status = AsyncMock()
        MockClient.return_value = mock_instance

        await auto_heal_disconnected_nodes("test", "default", {})

        mock_instance.set_node_status.assert_not_called()

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_API_실패시_에러_없이_종료(self, MockClient):
        """클러스터 정보 조회 실패 시 예외 없이 종료되는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.get_cluster_info = AsyncMock(return_value=None)
        MockClient.return_value = mock_instance

        # 예외가 발생하지 않아야 함
        await auto_heal_disconnected_nodes("test", "default", {})

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_재연결_실패_처리(self, MockClient, disconnected_cluster_info):
        """재연결 시도가 실패해도 예외 없이 처리되는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.get_cluster_info = AsyncMock(return_value=disconnected_cluster_info)
        mock_instance.set_node_status = AsyncMock(return_value=False)
        MockClient.return_value = mock_instance

        # 예외가 발생하지 않아야 함
        await auto_heal_disconnected_nodes("test", "default", {})
        mock_instance.set_node_status.assert_called_once()


# ---- scale_down 테스트 ----


class TestScaleDown:
    """스케일 다운 로직 테스트 클래스."""

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.scaling.build_nifi_client")
    async def test_스케일다운_오프로드_디스커넥트_제거(self, MockClient, mock_sleep):
        """스케일 다운 시 오프로드 -> 디스커넥트 -> 제거 순서로 수행되는지 테스트."""
        mock_instance = MagicMock()
        # find_node_by_address로 노드를 찾음
        mock_instance.find_node_by_address = AsyncMock(return_value={
            "nodeIdentifier": {"id": "node-3-id"},
            "address": "test-nifi-2.test-headless.default.svc.cluster.local",
            "status": "CONNECTED",
        })
        # set_node_status 항상 성공
        mock_instance.set_node_status = AsyncMock(return_value=True)
        # get_node_status에서 OFFLOADED 상태 반환 (즉시 완료)
        mock_instance.get_node_status = AsyncMock(return_value={
            "node": {"status": "OFFLOADED"}
        })
        # remove_node 성공
        mock_instance.remove_node = AsyncMock(return_value=True)
        MockClient.return_value = mock_instance

        await scale_down("test", "default", old_replicas=3, new_replicas=2, spec={})

        # OFFLOADING 상태 설정 호출 확인
        mock_instance.set_node_status.assert_any_call("node-3-id", "OFFLOADING")
        # DISCONNECTING 상태 설정 호출 확인
        mock_instance.set_node_status.assert_any_call("node-3-id", "DISCONNECTING")
        # 노드 제거 호출 확인
        mock_instance.remove_node.assert_called_once_with("node-3-id")

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.scaling.build_nifi_client")
    async def test_스케일다운_노드_미발견시_건너뜀(self, MockClient, mock_sleep):
        """노드를 찾을 수 없으면 해당 노드를 건너뛰는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.find_node_by_address = AsyncMock(return_value=None)
        mock_instance.set_node_status = AsyncMock()
        mock_instance.remove_node = AsyncMock()
        MockClient.return_value = mock_instance

        await scale_down("test", "default", old_replicas=3, new_replicas=2, spec={})

        # 노드를 찾지 못했으므로 상태 변경이나 제거가 호출되지 않아야 함
        mock_instance.set_node_status.assert_not_called()
        mock_instance.remove_node.assert_not_called()

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.scaling.build_nifi_client")
    async def test_스케일다운_오프로드_실패시_계속(self, MockClient, mock_sleep):
        """오프로드 실패 시 해당 노드를 건너뛰고 다음으로 진행하는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.find_node_by_address = AsyncMock(return_value={
            "nodeIdentifier": {"id": "node-3-id"},
            "address": "test-nifi-2.test-headless.default.svc.cluster.local",
            "status": "CONNECTED",
        })
        # 오프로드 실패
        mock_instance.set_node_status = AsyncMock(return_value=False)
        mock_instance.remove_node = AsyncMock()
        MockClient.return_value = mock_instance

        await scale_down("test", "default", old_replicas=3, new_replicas=2, spec={})

        # 오프로드 실패했으므로 remove_node는 호출되지 않아야 함
        mock_instance.remove_node.assert_not_called()

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.scaling.build_nifi_client")
    async def test_스케일다운_여러_노드(self, MockClient, mock_sleep):
        """여러 노드를 한번에 스케일 다운할 때 높은 ordinal부터 제거하는지 테스트."""
        call_order = []

        mock_instance = MagicMock()

        async def mock_find(address):
            """주소로 노드를 찾는 모의 함수."""
            if "nifi-2" in address:
                return {
                    "nodeIdentifier": {"id": "node-3-id"},
                    "address": address,
                    "status": "CONNECTED",
                }
            elif "nifi-1" in address:
                return {
                    "nodeIdentifier": {"id": "node-2-id"},
                    "address": address,
                    "status": "CONNECTED",
                }
            return None

        mock_instance.find_node_by_address = AsyncMock(side_effect=mock_find)

        async def mock_set_status(node_id, status):
            """노드 상태 설정 모의 함수 - 호출 순서 기록."""
            call_order.append((node_id, status))
            return True

        mock_instance.set_node_status = AsyncMock(side_effect=mock_set_status)
        mock_instance.get_node_status = AsyncMock(return_value={
            "node": {"status": "OFFLOADED"}
        })
        mock_instance.remove_node = AsyncMock(return_value=True)
        MockClient.return_value = mock_instance

        # 3 -> 1로 스케일 다운 (노드 2, 1 제거)
        await scale_down("test", "default", old_replicas=3, new_replicas=1, spec={})

        # 높은 ordinal(node-3-id)이 먼저 처리됨
        assert call_order[0] == ("node-3-id", "OFFLOADING")
        # remove_node가 두 번 호출되어야 함
        assert mock_instance.remove_node.call_count == 2

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.scaling.build_nifi_client")
    async def test_스케일다운_오프로드_타임아웃(self, MockClient, mock_sleep):
        """오프로드가 타임아웃되어도 disconnect와 remove가 계속 진행되는지 테스트."""
        mock_instance = MagicMock()
        mock_instance.find_node_by_address = AsyncMock(return_value={
            "nodeIdentifier": {"id": "node-3-id"},
            "address": "test-nifi-2.test-headless.default.svc.cluster.local",
            "status": "CONNECTED",
        })
        mock_instance.set_node_status = AsyncMock(return_value=True)
        # get_node_status가 계속 OFFLOADING 반환 (오프로드 완료되지 않음)
        mock_instance.get_node_status = AsyncMock(return_value={
            "node": {"status": "OFFLOADING"}
        })
        mock_instance.remove_node = AsyncMock(return_value=True)
        MockClient.return_value = mock_instance

        await scale_down("test", "default", old_replicas=3, new_replicas=2, spec={})

        # 타임아웃 후에도 DISCONNECTING과 remove_node가 호출되어야 함
        mock_instance.set_node_status.assert_any_call("node-3-id", "DISCONNECTING")
        mock_instance.remove_node.assert_called_once_with("node-3-id")
