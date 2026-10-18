"""클러스터 컨트롤러 및 스케일링 로직 단위 테스트.

NiFiClient를 AsyncMock으로 대체하여 실제 NiFi 클러스터 없이 테스트.
노드 응답 형식(nodeId, events 문구)은 NiFi 2.10 k3s 통합 테스트에서 관측한 값을 따른다.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from nifi_operator.controllers.cluster import (
    check_cluster_health,
    auto_heal_disconnected_nodes,
    connect_cluster,
    node_ordinal,
    _is_user_disconnected,
)
from nifi_operator.controllers.scaling import scale_down, ScaleDownError

SPEC3 = {"replicas": 3}
ADDR = "test-nifi-{}.test-headless.default.svc.cluster.local"


def _mock_client(cluster_info):
    m = MagicMock()
    m.get_cluster_info = AsyncMock(return_value=cluster_info)
    m.set_node_status = AsyncMock(return_value=True)
    return m


# ---- connect_cluster / node_ordinal ----


class TestConnectCluster:
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_nifi0_장애시_다음_노드로_조회(self, MockClient, sample_cluster_info):
        """nifi-0이 응답하지 않으면 nifi-1로 조회해야 한다 (헬스체크가 nifi-0에 묶이지 않음)."""
        down, up = _mock_client(None), _mock_client(sample_cluster_info)
        MockClient.side_effect = lambda *a, ordinal=0, **k: down if ordinal == 0 else up

        nifi, info = await connect_cluster("test", "default", SPEC3)
        assert nifi is up
        assert info == sample_cluster_info

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_모두_장애면_None(self, MockClient):
        MockClient.return_value = _mock_client(None)
        _, info = await connect_cluster("test", "default", SPEC3)
        assert info is None
        assert MockClient.call_count == 3

    def test_node_ordinal(self):
        assert node_ordinal("test", "default", ADDR.format(12)) == 12
        assert node_ordinal("test", "default", "other-nifi-1.other-headless.default.svc.cluster.local") is None
        assert node_ordinal("test", "default", "0.0.0.0") is None


# ---- check_cluster_health 테스트 ----


class TestCheckClusterHealth:
    """클러스터 헬스 체크 로직 테스트 클래스."""

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_정상_클러스터_상태(self, MockClient, sample_cluster_info):
        """모든 노드가 연결된 경우 Running 상태를 반환하는지 테스트."""
        MockClient.return_value = _mock_client(sample_cluster_info)

        result = await check_cluster_health("test", "default", SPEC3)
        assert result["readyNodes"] == 3
        assert result["totalNodes"] == 3
        assert result["phase"] == "Running"
        assert "3/3" in result["message"]

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_열화_클러스터_상태(self, MockClient, disconnected_cluster_info):
        """일부 노드가 DISCONNECTED인 경우 Degraded 상태와 해당 노드를 메시지에 포함."""
        MockClient.return_value = _mock_client(disconnected_cluster_info)

        result = await check_cluster_health("test", "default", {"replicas": 2})
        assert result["readyNodes"] == 1
        assert result["totalNodes"] == 2
        assert result["phase"] == "Degraded"
        assert "DISCONNECTED" in result["message"]

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_노드수_부족시_Degraded(self, MockClient, sample_cluster_info):
        """spec.replicas보다 조인한 노드가 적으면 모두 CONNECTED여도 Running이 아니다."""
        MockClient.return_value = _mock_client(sample_cluster_info)
        result = await check_cluster_health("test", "default", {"replicas": 4})
        assert result["phase"] == "Degraded"
        assert "3/4" in result["message"]

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_API_연결_실패(self, MockClient):
        """API 연결 실패 시 Error 상태를 반환하는지 테스트."""
        MockClient.return_value = _mock_client(None)

        result = await check_cluster_health("test", "default", SPEC3)
        assert result["readyNodes"] == 0
        assert result["phase"] == "Error"

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_코디네이터_주소_반환(self, MockClient, sample_cluster_info):
        """클러스터 코디네이터 주소가 올바르게 포함되는지 테스트."""
        MockClient.return_value = _mock_client(sample_cluster_info)

        result = await check_cluster_health("test", "default", SPEC3)
        assert result["clusterCoordinator"] == ADDR.format(0)

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_코디네이터는_Primary보다_우선(self, MockClient):
        """Primary Node와 Cluster Coordinator가 다른 노드면 Coordinator를 보고한다."""
        info = {"cluster": {"nodes": [
            {"nodeId": "a", "address": ADDR.format(0), "status": "CONNECTED", "roles": ["Primary Node"]},
            {"nodeId": "b", "address": ADDR.format(1), "status": "CONNECTED", "roles": ["Cluster Coordinator"]},
        ]}}
        MockClient.return_value = _mock_client(info)
        result = await check_cluster_health("test", "default", {"replicas": 2})
        assert result["clusterCoordinator"] == ADDR.format(1)


# ---- auto_heal_disconnected_nodes 테스트 ----


class TestAutoHealDisconnectedNodes:
    """자동 복구 로직 테스트 클래스."""

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_연결끊긴_노드_재연결_시도(self, MockClient, disconnected_cluster_info):
        """DISCONNECTED 노드에 대해 CONNECTING 상태 변경을 시도하는지 테스트."""
        mock_instance = _mock_client(disconnected_cluster_info)
        MockClient.return_value = mock_instance

        result = await auto_heal_disconnected_nodes("test", "default", {"replicas": 2})

        # DISCONNECTED 노드(node-2-id)에 대해 CONNECTING 호출이 있어야 함
        mock_instance.set_node_status.assert_called_once_with("node-2-id", "CONNECTING")
        assert result == [ADDR.format(1)]

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_모든_노드_연결됨_재연결_불필요(self, MockClient, sample_cluster_info):
        """모든 노드가 CONNECTED면 set_node_status가 호출되지 않는지 테스트."""
        mock_instance = _mock_client(sample_cluster_info)
        MockClient.return_value = mock_instance

        await auto_heal_disconnected_nodes("test", "default", SPEC3)

        mock_instance.set_node_status.assert_not_called()

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_API_실패시_에러_없이_종료(self, MockClient):
        """클러스터 정보 조회 실패 시 예외 없이 종료되는지 테스트."""
        MockClient.return_value = _mock_client(None)

        # 예외가 발생하지 않아야 함
        assert await auto_heal_disconnected_nodes("test", "default", SPEC3) == []

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_재연결_실패_처리(self, MockClient, disconnected_cluster_info):
        """재연결 시도가 실패해도 예외 없이 처리되는지 테스트."""
        mock_instance = _mock_client(disconnected_cluster_info)
        mock_instance.set_node_status = AsyncMock(return_value=False)
        MockClient.return_value = mock_instance

        # 예외가 발생하지 않아야 함
        assert await auto_heal_disconnected_nodes("test", "default", {"replicas": 2}) == []
        mock_instance.set_node_status.assert_called_once()

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_관리자_연결해제_노드는_건너뜀(self, MockClient, user_disconnected_cluster_info):
        """관리자가 UI/API로 연결 해제한 노드는 다시 연결하지 않는다."""
        mock_instance = _mock_client(user_disconnected_cluster_info)
        MockClient.return_value = mock_instance

        await auto_heal_disconnected_nodes("test", "default", {"replicas": 2})
        mock_instance.set_node_status.assert_not_called()

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_축소대상_ordinal은_건너뜀(self, MockClient, disconnected_cluster_info):
        """ordinal ≥ spec.replicas (스케일 다운 대상) 노드는 다시 연결하지 않는다."""
        mock_instance = _mock_client(disconnected_cluster_info)
        MockClient.return_value = mock_instance

        # nifi-1이 DISCONNECTED지만 replicas=1 → 축소 대상
        await auto_heal_disconnected_nodes("test", "default", {"replicas": 1})
        mock_instance.set_node_status.assert_not_called()

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_오프로드_노드는_건너뜀(self, MockClient, disconnected_cluster_info):
        disconnected_cluster_info["cluster"]["nodes"][1]["status"] = "OFFLOADED"
        mock_instance = _mock_client(disconnected_cluster_info)
        MockClient.return_value = mock_instance

        await auto_heal_disconnected_nodes("test", "default", {"replicas": 2})
        mock_instance.set_node_status.assert_not_called()

    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_protected_주소_건너뜀(self, MockClient, disconnected_cluster_info):
        mock_instance = _mock_client(disconnected_cluster_info)
        MockClient.return_value = mock_instance

        await auto_heal_disconnected_nodes(
            "test", "default", {"replicas": 2}, protected_addresses={ADDR.format(1)}
        )
        mock_instance.set_node_status.assert_not_called()

    def test_사용자_연결해제_판별(self):
        heartbeat = {"events": [{
            "timestamp": "10/18/2026 12:00:30 UTC",
            "message": "Node disconnected from cluster due to Have not received a heartbeat from node in 44 seconds",
        }]}
        assert _is_user_disconnected(heartbeat) is False
        assert _is_user_disconnected({}) is False
        # 가장 최근 이벤트 기준 (사용자 해제 후 하트비트 누락으로 다시 끊긴 경우는 복구 대상)
        mixed = {"events": [
            {"timestamp": "10/18/2026 12:00:30 UTC", "message": "Node disconnected from cluster due to Have not received a heartbeat"},
            {"timestamp": "10/18/2026 11:53:45 UTC", "message": "Node disconnected due to User CN=x requested that node be disconnected from cluster"},
        ]}
        assert _is_user_disconnected(mixed) is False


# ---- scale_down 테스트 ----


def _scale_client(node_status="CONNECTED", status_sequence=None, set_ok=True, remove_ok=True):
    """ordinal 2 노드(node-3-id)를 가진 클러스터를 흉내내는 모의 클라이언트."""
    info = {"cluster": {"nodes": [
        {"nodeId": "node-1-id", "address": ADDR.format(0), "status": "CONNECTED"},
        {"nodeId": "node-2-id", "address": ADDR.format(1), "status": "CONNECTED"},
        {"nodeId": "node-3-id", "address": ADDR.format(2), "status": node_status},
    ]}}
    m = _mock_client(info)
    m.calls = []

    async def set_status(node_id, status):
        m.calls.append((node_id, status))
        return set_ok

    m.set_node_status = AsyncMock(side_effect=set_status)
    seq = list(status_sequence or [])

    async def get_status(node_id):
        if seq:
            return {"node": {"status": seq.pop(0) if len(seq) > 1 else seq[0]}}
        return {"node": {"status": None}}

    m.get_node_status = AsyncMock(side_effect=get_status)
    m.remove_node = AsyncMock(return_value=remove_ok)
    return m


class TestScaleDown:
    """스케일 다운 로직 테스트 클래스."""

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_스케일다운_디스커넥트_오프로드_제거_순서(self, MockClient, mock_sleep):
        """NiFi 요구 순서: DISCONNECTING → DISCONNECTED → OFFLOADING → OFFLOADED → DELETE → Pod 축소."""
        m = _scale_client(status_sequence=["DISCONNECTED", "OFFLOADING", "OFFLOADED"])
        MockClient.return_value = m
        set_replicas = AsyncMock()

        await scale_down("test", "default", 3, 2, SPEC3, set_replicas=set_replicas)

        assert m.calls == [("node-3-id", "DISCONNECTING"), ("node-3-id", "OFFLOADING")]
        m.remove_node.assert_called_once_with("node-3-id")
        set_replicas.assert_awaited_once_with(2)

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_제거대상이_아닌_노드로_API_호출(self, MockClient, mock_sleep):
        """클러스터 API는 남는 노드(ordinal < new)로만 호출한다."""
        MockClient.return_value = _scale_client(status_sequence=["DISCONNECTED", "OFFLOADED"])
        await scale_down("test", "default", 3, 2, SPEC3, set_replicas=AsyncMock())
        ordinals = {c.kwargs.get("ordinal") for c in MockClient.call_args_list}
        assert ordinals <= {0, 1}

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_노드_미발견시_Pod만_축소(self, MockClient, mock_sleep, sample_cluster_info):
        """클러스터에 없는 노드(이미 제거됨)는 상태 변경 없이 Pod만 줄인다 (재시도 멱등)."""
        m = _mock_client(sample_cluster_info)  # nifi-3 없음
        m.remove_node = AsyncMock()
        MockClient.return_value = m
        set_replicas = AsyncMock()

        await scale_down("test", "default", 4, 3, {"replicas": 4}, set_replicas=set_replicas)

        m.set_node_status.assert_not_called()
        m.remove_node.assert_not_called()
        set_replicas.assert_awaited_once_with(3)

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_디스커넥트_요청_실패시_중단(self, MockClient, mock_sleep):
        """연결 해제 요청이 실패하면 Pod를 줄이지 않고 ScaleDownError."""
        m = _scale_client(set_ok=False)
        MockClient.return_value = m
        set_replicas = AsyncMock()

        with pytest.raises(ScaleDownError):
            await scale_down("test", "default", 3, 2, SPEC3, set_replicas=set_replicas)
        m.remove_node.assert_not_called()
        set_replicas.assert_not_called()

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_오프로드_타임아웃시_중단(self, MockClient, mock_sleep):
        """오프로드가 끝나지 않으면 제거/Pod 축소 없이 중단한다 (데이터 유실 방지)."""
        m = _scale_client(status_sequence=["DISCONNECTED", "OFFLOADING"])
        MockClient.return_value = m
        set_replicas = AsyncMock()

        with pytest.raises(ScaleDownError, match="오프로드"):
            await scale_down("test", "default", 3, 2, SPEC3, set_replicas=set_replicas)
        m.remove_node.assert_not_called()
        set_replicas.assert_not_called()

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_제거_실패시_중단(self, MockClient, mock_sleep):
        m = _scale_client(status_sequence=["DISCONNECTED", "OFFLOADED"], remove_ok=False)
        MockClient.return_value = m
        set_replicas = AsyncMock()

        with pytest.raises(ScaleDownError, match="제거"):
            await scale_down("test", "default", 3, 2, SPEC3, set_replicas=set_replicas)
        set_replicas.assert_not_called()

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_이미_오프로드된_노드는_삭제만(self, MockClient, mock_sleep):
        """재시도 시 이미 OFFLOADED인 노드는 상태 변경 없이 제거한다."""
        m = _scale_client(node_status="OFFLOADED")
        MockClient.return_value = m
        await scale_down("test", "default", 3, 2, SPEC3, set_replicas=AsyncMock())
        assert m.calls == []
        m.remove_node.assert_called_once_with("node-3-id")

    @patch("nifi_operator.controllers.scaling.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_여러_노드_높은_ordinal부터(self, MockClient, mock_sleep):
        """3 → 1: nifi-2, nifi-1 순서로 제거하고 단계마다 Pod를 줄인다."""
        info = {"cluster": {"nodes": [
            {"nodeId": f"node-{i}", "address": ADDR.format(i), "status": "CONNECTED"} for i in range(3)
        ]}}
        m = _mock_client(info)
        order = []

        async def set_status(node_id, status):
            order.append((node_id, status))
            return True

        m.set_node_status = AsyncMock(side_effect=set_status)

        async def get_status(node_id):
            last = [s for n, s in order if n == node_id][-1]
            return {"node": {"status": {"DISCONNECTING": "DISCONNECTED", "OFFLOADING": "OFFLOADED"}[last]}}

        m.get_node_status = AsyncMock(side_effect=get_status)
        m.remove_node = AsyncMock(return_value=True)
        MockClient.return_value = m
        set_replicas = AsyncMock()

        await scale_down("test", "default", 3, 1, SPEC3, set_replicas=set_replicas)

        assert order[0] == ("node-2", "DISCONNECTING")
        assert [c.args[0] for c in m.remove_node.call_args_list] == ["node-2", "node-1"]
        assert [c.args[0] for c in set_replicas.call_args_list] == [2, 1]


class TestWaitForClusterReady:
    @patch("nifi_operator.controllers.cluster.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_연속_안정_확인(self, MockClient, mock_sleep, sample_cluster_info, disconnected_cluster_info):
        """stable_checks회 연속 전체 연결이어야 성공 (중간에 끊기면 다시 셈)."""
        from nifi_operator.controllers.cluster import wait_for_cluster_ready

        seq = [sample_cluster_info, disconnected_cluster_info, sample_cluster_info,
               sample_cluster_info, sample_cluster_info]
        m = MagicMock()
        m.get_cluster_info = AsyncMock(side_effect=seq)
        MockClient.return_value = m
        ok = await wait_for_cluster_ready("test", "default", 3, SPEC3, stable_checks=3,
                                          timeout_seconds=100)
        assert ok is True
        assert m.get_cluster_info.await_count == 5

    @patch("nifi_operator.controllers.cluster.asyncio.sleep", new_callable=AsyncMock)
    @patch("nifi_operator.controllers.cluster.build_nifi_client")
    async def test_타임아웃(self, MockClient, mock_sleep, disconnected_cluster_info):
        from nifi_operator.controllers.cluster import wait_for_cluster_ready

        MockClient.return_value = _mock_client(disconnected_cluster_info)
        assert await wait_for_cluster_ready("test", "default", 2, {"replicas": 2},
                                            timeout_seconds=30) is False
