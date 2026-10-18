"""main.py 핸들러/헬퍼 단위 테스트 (Kubernetes/NiFi API는 모의 객체로 대체)."""

import logging
from unittest.mock import AsyncMock, MagicMock

import kopf
import kubernetes
import pytest

from nifi_operator import main

LOG = logging.getLogger("test")
OWNER = {
    "apiVersion": "datadynamics.io/v1alpha1", "kind": "NiFiCluster",
    "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
}


class Patch:
    """kopf patch 객체 흉내 (status dict만 사용)."""

    def __init__(self):
        self.status = {}


def _api_exc(status):
    return kubernetes.client.exceptions.ApiException(status=status)


@pytest.fixture
def k8s(monkeypatch):
    """main 모듈의 K8s API 전역을 모의 객체로 교체."""
    apis = {n: MagicMock() for n in ("apps_v1", "core_v1", "rbac_v1", "policy_v1", "custom_api")}
    for n, m in apis.items():
        monkeypatch.setattr(main, n, m, raising=False)
    monkeypatch.setattr(main, "_busy", set())
    return apis


# ---- _effective_spec ----


def test_effective_spec_불변필드는_적용값_유지():
    spec = {"replicas": 3, "version": "2.10.0", "tls": {"enabled": False}, "storage": {"size": "9Gi"}}
    status = {"applied": {"tls": {"enabled": True, "issuerRef": {"name": "ca"}}, "storage": {"size": "5Gi"}}}
    eff = main._effective_spec(spec, status)
    assert eff["tls"]["enabled"] is True
    assert eff["storage"]["size"] == "5Gi"
    assert eff["version"] == "2.10.0"
    assert spec["tls"]["enabled"] is False  # 원본 불변


def test_effective_spec_status_없으면_spec_그대로():
    spec = {"replicas": 1, "version": "2.10.0"}
    assert main._effective_spec(spec, None) == spec


# ---- _select_registry_client ----


def _reg(rid, url):
    return {"id": rid, "component": {"properties": {"url": url}}}


def test_registry_url_일치_선택():
    regs = [_reg("a", "http://r1:18080"), _reg("b", "http://r2:18080/")]
    assert main._select_registry_client(regs, "http://r2:18080") == "b"


def test_registry_여러개인데_url없으면_오류():
    """registries[0] 임의 선택 금지."""
    with pytest.raises(kopf.TemporaryError):
        main._select_registry_client([_reg("a", "x"), _reg("b", "y")], "")


def test_registry_하나면_url없어도_선택():
    assert main._select_registry_client([_reg("a", "x")], "") == "a"


def test_registry_없음_또는_불일치는_재시도():
    with pytest.raises(kopf.TemporaryError):
        main._select_registry_client([], "")
    with pytest.raises(kopf.TemporaryError):
        main._select_registry_client(None, "")
    with pytest.raises(kopf.TemporaryError):
        main._select_registry_client([_reg("a", "http://r1")], "http://other")


# ---- PDB 조정 ----


async def test_pdb_멀티노드는_생성(k8s):
    await main._reconcile_pdb("test", "default", {"replicas": 3}, owner=OWNER)
    pdb = k8s["policy_v1"].create_namespaced_pod_disruption_budget.call_args.args[1]
    assert pdb.spec.min_available == 2


async def test_pdb_이미있으면_minAvailable_갱신(k8s):
    k8s["policy_v1"].create_namespaced_pod_disruption_budget.side_effect = _api_exc(409)
    await main._reconcile_pdb("test", "default", {"replicas": 5}, owner=OWNER)
    k8s["policy_v1"].patch_namespaced_pod_disruption_budget.assert_called_once_with(
        "test-nifi-pdb", "default", {"spec": {"minAvailable": 3}}
    )


async def test_pdb_단일노드면_삭제(k8s):
    """replicas=1에 PDB가 남으면 노드 드레인이 막힌다."""
    await main._reconcile_pdb("test", "default", {"replicas": 1})
    k8s["policy_v1"].delete_namespaced_pod_disruption_budget.assert_called_once_with(
        "test-nifi-pdb", "default"
    )


async def test_pdb_삭제시_없으면_무시(k8s):
    k8s["policy_v1"].delete_namespaced_pod_disruption_budget.side_effect = _api_exc(404)
    await main._reconcile_pdb("test", "default", {"replicas": 1})


# ---- on_replicas_change ----


SPEC = {"replicas": 2, "version": "2.10.0", "tls": {"enabled": True, "issuerRef": {"name": "ca"}}}


async def test_스케일다운_실패시_Error기록_및_재시도(k8s, monkeypatch):
    monkeypatch.setattr(main, "scale_down", AsyncMock(side_effect=main.ScaleDownError("오프로드 타임아웃")))
    patch = Patch()
    with pytest.raises(kopf.TemporaryError):
        await main.on_replicas_change(
            spec=SPEC, status={}, body=OWNER, name="test", namespace="default",
            old=3, new=2, logger=LOG, patch=patch,
        )
    statuses = [c.args[-1]["status"] for c in
                k8s["custom_api"].patch_namespaced_custom_object_status.call_args_list]
    assert statuses[0]["phase"] == "Scaling"
    assert statuses[-1]["phase"] == "Error"
    # StatefulSet replicas를 new로 줄이지 않았다
    k8s["apps_v1"].patch_namespaced_stateful_set.assert_not_called()
    assert main._busy == set()


async def test_스케일업은_sts_PDB_조정(k8s, monkeypatch):
    monkeypatch.setattr(main, "wait_for_cluster_ready", AsyncMock(return_value=True))
    patch = Patch()
    spec = {**SPEC, "replicas": 4}
    await main.on_replicas_change(
        spec=spec, status={}, body=OWNER, name="test", namespace="default",
        old=3, new=4, logger=LOG, patch=patch,
    )
    k8s["apps_v1"].patch_namespaced_stateful_set.assert_called_once_with(
        "test-nifi", "default", {"spec": {"replicas": 4}}
    )
    assert k8s["policy_v1"].create_namespaced_pod_disruption_budget.called
    assert patch.status["phase"] == "Running"


async def test_비TLS_멀티노드_스케일업_거부(k8s):
    with pytest.raises(kopf.PermanentError):
        await main.on_replicas_change(
            spec={"replicas": 3, "version": "2.10.0"}, status={}, body=OWNER, name="test",
            namespace="default", old=1, new=3, logger=LOG, patch=Patch(),
        )


# ---- on_spec_change ----


async def test_불변필드_변경은_적용안함(k8s, monkeypatch):
    warn = MagicMock()
    monkeypatch.setattr(main.kopf, "warn", warn)
    patch = Patch()
    old = {**SPEC, "storage": {"size": "5Gi"}}
    new = {**SPEC, "storage": {"size": "9Gi"}}
    await main.on_spec_change(
        old=old, new=new, status={}, body=OWNER, name="test", namespace="default",
        logger=LOG, patch=patch,
    )
    assert "spec.storage" in patch.status["message"]
    warn.assert_called_once()
    k8s["apps_v1"].patch_namespaced_stateful_set.assert_not_called()


async def test_replicas만_바뀌면_무시(k8s):
    await main.on_spec_change(
        old={**SPEC, "replicas": 3}, new={**SPEC, "replicas": 4}, status={}, body=OWNER,
        name="test", namespace="default", logger=LOG, patch=Patch(),
    )
    k8s["apps_v1"].patch_namespaced_stateful_set.assert_not_called()


async def test_버전변경은_롤링재시작(k8s, monkeypatch):
    rolling = AsyncMock(return_value="")
    monkeypatch.setattr(main, "_rolling_restart", rolling)
    patch = Patch()
    new = {**SPEC, "version": "2.11.0"}
    await main.on_spec_change(
        old=SPEC, new=new, status={}, body=OWNER, name="test", namespace="default",
        logger=LOG, patch=patch,
    )
    template = rolling.call_args.args[3]
    assert template["spec"]["containers"][0]["image"] == "apache/nifi:2.11.0"
    assert patch.status["version"] == "2.11.0"
    assert patch.status["phase"] == "Running"
    k8s["core_v1"].patch_namespaced_config_map.assert_called_once()


# ---- monitor_cluster ----


async def test_monitor_스케일링중_건너뜀(k8s, monkeypatch):
    health = AsyncMock()
    monkeypatch.setattr(main, "check_cluster_health", health)
    patch = Patch()
    await main.monitor_cluster(
        spec=SPEC, name="test", namespace="default", status={"phase": "Scaling"},
        patch=patch, logger=LOG,
    )
    health.assert_not_called()
    assert patch.status == {}


async def test_monitor_생성중_실패면_Creating_유지_알림없음(k8s, monkeypatch):
    monkeypatch.setattr(main, "check_cluster_health", AsyncMock(return_value={
        "phase": "Error", "message": "NiFi API 연결 실패", "readyNodes": 0}))
    hook = AsyncMock()
    monkeypatch.setattr(main, "_send_webhook", hook)
    patch = Patch()
    await main.monitor_cluster(
        spec=SPEC, name="test", namespace="default", status={"phase": "Creating"},
        patch=patch, logger=LOG,
    )
    assert "phase" not in patch.status
    hook.assert_not_called()


async def test_monitor_Running전이시_root정책_1회(k8s, monkeypatch):
    monkeypatch.setattr(main, "check_cluster_health", AsyncMock(return_value={
        "phase": "Running", "message": "2/2", "readyNodes": 2, "totalNodes": 2}))
    ensure = AsyncMock(return_value=["read /process-groups/x"])
    monkeypatch.setattr(main, "ensure_root_policies", ensure)
    patch = Patch()
    await main.monitor_cluster(
        spec=SPEC, name="test", namespace="default", status={"phase": "Creating"},
        patch=patch, logger=LOG,
    )
    assert patch.status["phase"] == "Running"
    assert patch.status["rootPoliciesApplied"] is True
    ensure.assert_awaited_once()

    ensure.reset_mock()
    await main.monitor_cluster(
        spec=SPEC, name="test", namespace="default",
        status={"phase": "Running", "rootPoliciesApplied": True}, patch=Patch(), logger=LOG,
    )
    ensure.assert_not_called()


async def test_monitor_비정상전이시_웹훅(k8s, monkeypatch):
    monkeypatch.setattr(main, "check_cluster_health", AsyncMock(return_value={
        "phase": "Degraded", "message": "1/2", "readyNodes": 1}))
    hook = AsyncMock()
    monkeypatch.setattr(main, "_send_webhook", hook)
    await main.monitor_cluster(
        spec=SPEC, name="test", namespace="default", status={"phase": "Running"},
        patch=Patch(), logger=LOG,
    )
    hook.assert_awaited_once()


# ---- auto_heal 타이머 ----


async def test_auto_heal_어노테이션으로_비활성(k8s, monkeypatch):
    heal = AsyncMock()
    monkeypatch.setattr(main, "auto_heal_disconnected_nodes", heal)
    await main.auto_heal(
        spec=SPEC, name="test", namespace="default", status={"phase": "Running"},
        meta={"annotations": {main.AUTO_HEAL_ANNOTATION: "false"}}, logger=LOG,
    )
    heal.assert_not_called()


async def test_auto_heal_영속된_Scaling이면_건너뜀(k8s, monkeypatch):
    """오퍼레이터 재시작으로 _busy가 비어도 status.phase로 진행 중임을 안다."""
    heal = AsyncMock()
    monkeypatch.setattr(main, "auto_heal_disconnected_nodes", heal)
    await main.auto_heal(
        spec=SPEC, name="test", namespace="default", status={"phase": "Scaling"},
        meta={}, logger=LOG,
    )
    heal.assert_not_called()


# ---- NiFiFlow ----


FLOW_SPEC = {"clusterRef": "test", "bucketId": "b", "flowId": "f", "flowVersion": 2,
             "registryUrl": "http://reg", "processGroupId": "root", "autoStart": True}


def _flow_nifi():
    nifi = MagicMock()
    nifi.get_registry_clients = AsyncMock(return_value=[_reg("r1", "http://reg")])
    nifi.get_process_group = AsyncMock(return_value=None)
    nifi.list_child_process_groups = AsyncMock(return_value=[])
    nifi.import_flow_from_registry = AsyncMock(return_value={"id": "new-pg"})
    nifi.set_process_group_state = AsyncMock(return_value=True)
    return nifi


async def test_flow_create_신규배포(k8s, monkeypatch):
    nifi = _flow_nifi()
    monkeypatch.setattr(main, "_flow_client", AsyncMock(return_value=nifi))
    k8s["custom_api"].list_namespaced_custom_object.return_value = {"items": []}
    patch = Patch()
    await main.on_flow_create(spec=FLOW_SPEC, name="fl", namespace="default", status={},
                              logger=LOG, patch=patch)
    nifi.import_flow_from_registry.assert_awaited_once()
    assert patch.status["deployedProcessGroupId"] == "new-pg"
    assert patch.status["phase"] == "Running"


async def test_flow_create_재시도시_기존PG_재사용(k8s, monkeypatch):
    """이전 시도에서 PG는 만들어졌지만 status 기록 전에 실패한 경우 중복 PG를 만들지 않는다."""
    nifi = _flow_nifi()
    nifi.list_child_process_groups = AsyncMock(return_value=[
        {"id": "pg-other", "component": {"versionControlInformation": {
            "registryId": "r1", "bucketId": "b", "flowId": "f"}}},
        {"id": "pg-mine", "component": {"versionControlInformation": {
            "registryId": "r1", "bucketId": "b", "flowId": "f"}}},
    ])
    monkeypatch.setattr(main, "_flow_client", AsyncMock(return_value=nifi))
    # pg-other는 다른 NiFiFlow가 관리 중
    k8s["custom_api"].list_namespaced_custom_object.return_value = {"items": [
        {"metadata": {"name": "other"}, "status": {"deployedProcessGroupId": "pg-other"}},
    ]}
    patch = Patch()
    await main.on_flow_create(spec=FLOW_SPEC, name="fl", namespace="default", status={},
                              logger=LOG, patch=patch)
    nifi.import_flow_from_registry.assert_not_called()
    assert patch.status["deployedProcessGroupId"] == "pg-mine"


async def test_flow_create_status의_PG가_있으면_재배포안함(k8s, monkeypatch):
    nifi = _flow_nifi()
    nifi.get_process_group = AsyncMock(return_value={"id": "pg-1"})
    monkeypatch.setattr(main, "_flow_client", AsyncMock(return_value=nifi))
    patch = Patch()
    await main.on_flow_create(spec=FLOW_SPEC, name="fl", namespace="default",
                              status={"deployedProcessGroupId": "pg-1"}, logger=LOG, patch=patch)
    nifi.import_flow_from_registry.assert_not_called()
    assert patch.status["deployedProcessGroupId"] == "pg-1"


async def test_flow_create_실패는_status기록후_재시도(k8s, monkeypatch):
    nifi = _flow_nifi()
    nifi.import_flow_from_registry = AsyncMock(side_effect=main.NiFiAPIError("Flow 배포 실패: HTTP 409"))
    monkeypatch.setattr(main, "_flow_client", AsyncMock(return_value=nifi))
    k8s["custom_api"].list_namespaced_custom_object.return_value = {"items": []}
    patch = Patch()
    with pytest.raises(kopf.TemporaryError):
        await main.on_flow_create(spec=FLOW_SPEC, name="fl", namespace="default", status={},
                                  logger=LOG, patch=patch)
    assert patch.status["phase"] == "Error"
    assert "409" in patch.status["message"]


async def test_flow_version_이미_목표버전이면_무시(k8s, monkeypatch):
    nifi = _flow_nifi()
    nifi.get_version_control_information = AsyncMock(return_value={
        "versionControlInformation": {"version": "2"}})
    nifi.change_flow_version = AsyncMock()
    monkeypatch.setattr(main, "_flow_client", AsyncMock(return_value=nifi))
    patch = Patch()
    await main.on_flow_version_change(spec=FLOW_SPEC, name="fl", namespace="default", old=1, new=2,
                                      status={"deployedProcessGroupId": "pg"}, logger=LOG, patch=patch)
    nifi.change_flow_version.assert_not_called()
    assert patch.status["deployedVersion"] == 2


async def test_flow_version_변경_완료대기(k8s, monkeypatch):
    nifi = _flow_nifi()
    nifi.get_version_control_information = AsyncMock(return_value={
        "versionControlInformation": {"version": "1"}})
    nifi.change_flow_version = AsyncMock()
    monkeypatch.setattr(main, "_flow_client", AsyncMock(return_value=nifi))
    patch = Patch()
    await main.on_flow_version_change(spec=FLOW_SPEC, name="fl", namespace="default", old=1, new=2,
                                      status={"deployedProcessGroupId": "pg"}, logger=LOG, patch=patch)
    nifi.change_flow_version.assert_awaited_once()
    assert patch.status["deployedVersion"] == 2


async def test_flow_delete_중지후_삭제(k8s, monkeypatch):
    nifi = _flow_nifi()
    nifi.get_process_group = AsyncMock(return_value={"id": "pg"})
    nifi.set_controller_services_state = AsyncMock(return_value=True)
    nifi.delete_process_group = AsyncMock()
    monkeypatch.setattr(main, "_flow_client", AsyncMock(return_value=nifi))
    await main.on_flow_delete(spec=FLOW_SPEC, name="fl", namespace="default", logger=LOG,
                              status={"deployedProcessGroupId": "pg"})
    nifi.set_process_group_state.assert_awaited_once_with("pg", "STOPPED")
    nifi.delete_process_group.assert_awaited_once_with("pg")


async def test_flow_delete_삭제거부면_재시도(k8s, monkeypatch):
    nifi = _flow_nifi()
    nifi.get_process_group = AsyncMock(return_value={"id": "pg"})
    nifi.set_controller_services_state = AsyncMock(return_value=True)
    nifi.delete_process_group = AsyncMock(side_effect=main.NiFiAPIError("contains 5 FlowFiles"))
    monkeypatch.setattr(main, "_flow_client", AsyncMock(return_value=nifi))
    with pytest.raises(kopf.TemporaryError):
        await main.on_flow_delete(spec=FLOW_SPEC, name="fl", namespace="default", logger=LOG,
                                  status={"deployedProcessGroupId": "pg"})


async def test_flow_delete_클러스터_없으면_통과(k8s, monkeypatch):
    monkeypatch.setattr(main, "_flow_client", AsyncMock(side_effect=_api_exc(404)))
    await main.on_flow_delete(spec=FLOW_SPEC, name="fl", namespace="default", logger=LOG,
                              status={"deployedProcessGroupId": "pg"})


# ---- on_cluster_delete ----


def test_cluster_delete_소유자없는_잔여리소스_정리(k8s, monkeypatch):
    coord = MagicMock()
    leases = []
    for n in ("test-cluster-coordinator", "test-primary-node", "other-cluster-coordinator"):
        m = MagicMock()
        m.metadata.name = n
        leases.append(m)
    coord.list_namespaced_lease.return_value.items = leases
    monkeypatch.setattr(main.client, "CoordinationV1Api", lambda: coord)
    cms = []
    for n, owners in (("test-nifi-component-abc", None), ("test-config", [object()]),
                      ("test2-nifi-component-x", None)):
        m = MagicMock()
        m.metadata.name = n
        m.metadata.owner_references = owners
        cms.append(m)
    k8s["core_v1"].list_namespaced_config_map.return_value.items = cms
    k8s["core_v1"].delete_namespaced_secret.side_effect = [None, _api_exc(404)]
    k8s["core_v1"].list_namespaced_pod.return_value.items = []

    main.on_cluster_delete(name="test", namespace="default", logger=LOG)

    # 재생성 원인(Certificate, StatefulSet)을 먼저 지운다
    assert [c.args[4] for c in k8s["custom_api"].delete_namespaced_custom_object.call_args_list] == [
        "test-nifi-tls", "test-operator-tls"]
    k8s["apps_v1"].delete_namespaced_stateful_set.assert_called_once_with("test-nifi", "default")

    assert [c.args[0] for c in k8s["core_v1"].delete_namespaced_secret.call_args_list] == [
        "test-nifi-tls", "test-operator-tls"]
    assert sorted(c.args[0] for c in coord.delete_namespaced_lease.call_args_list) == [
        "test-cluster-coordinator", "test-primary-node"]
    assert [c.args[0] for c in k8s["core_v1"].delete_namespaced_config_map.call_args_list] == [
        "test-nifi-component-abc"]


def test_cluster_delete_Pod_남아있으면_정리_보류(k8s):
    """NiFi Pod가 아직 살아 있으면 Lease/Secret이 다시 생기므로 정리하지 않고 재시도한다."""
    k8s["core_v1"].list_namespaced_pod.return_value.items = [MagicMock()]
    with pytest.raises(kopf.TemporaryError):
        main.on_cluster_delete(name="test", namespace="default", logger=LOG)
    k8s["core_v1"].delete_namespaced_secret.assert_not_called()
    k8s["custom_api"].delete_namespaced_custom_object.assert_called()
