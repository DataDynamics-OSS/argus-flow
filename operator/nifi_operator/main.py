"""
NiFi 2.x Kubernetes Operator — 메인 진입점.

kopf 프레임워크를 사용하여 NiFiCluster와 NiFiFlow 커스텀 리소스를 관리합니다.
- NiFiCluster 생성/삭제/스케일링/설정 변경
- NiFiFlow 배포/버전 변경/삭제 (NiFi Registry 연동)
- 주기적 헬스체크 및 장애 노드 자동 복구
- 클러스터 이상 시 웹훅 알림
"""

import asyncio
import datetime
import logging

import httpx
import kopf
import kubernetes
from kubernetes import client

from nifi_operator.resources.statefulset import build_statefulset
from nifi_operator.resources.service import build_headless_service, build_ui_service
from nifi_operator.resources.configmap import build_nifi_config
from nifi_operator.resources.secret import build_credentials_secret
from nifi_operator.resources.certificate import (
    build_server_certificate,
    build_operator_certificate,
)
from nifi_operator.resources import rbac
from nifi_operator.controllers.scaling import scale_up, scale_down
from nifi_operator.controllers.cluster import (
    check_cluster_health,
    auto_heal_disconnected_nodes,
)
from nifi_operator.nifi_api.client import build_nifi_client

logger = logging.getLogger(__name__)

# Kubernetes API 클라이언트 (startup 시 초기화)
apps_v1: client.AppsV1Api
core_v1: client.CoreV1Api
rbac_v1: client.RbacAuthorizationV1Api
policy_v1: client.PolicyV1Api
custom_api: client.CustomObjectsApi

# 스케일링/설정변경이 진행 중인 클러스터 (namespace, name). monitor/auto_heal 타이머가
# 이 목록의 클러스터는 건드리지 않아 상태 덮어쓰기/재연결 경합을 방지한다.
_busy: set = set()

_HEALTHY_PHASES = {"Running", "Creating", "Scaling", "Updating"}


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **kwargs):
    """Operator 시작 시 초기 설정."""
    global apps_v1, core_v1, rbac_v1, policy_v1, custom_api

    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()

    apps_v1 = client.AppsV1Api()
    core_v1 = client.CoreV1Api()
    rbac_v1 = client.RbacAuthorizationV1Api()
    policy_v1 = client.PolicyV1Api()
    custom_api = client.CustomObjectsApi()

    settings.posting.level = logging.INFO
    settings.persistence.finalizer = "nifi-operator.datadynamics.io/finalizer"
    logger.info("NiFi Operator 시작됨")


# =============================================================================
# 공통 헬퍼
# =============================================================================


def _create_or_ignore(create_fn, *args, resource_desc: str = "") -> bool:
    """리소스를 생성하되 이미 존재(409)하면 무시한다 → 핸들러 멱등성 확보.

    Returns:
        새로 생성했으면 True, 이미 있으면 False.
    """
    try:
        create_fn(*args)
        return True
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 409:
            logger.info(f"{resource_desc} 이미 존재 — 건너뜀")
            return False
        raise


def _get_cluster_spec(cluster_name: str, namespace: str) -> dict:
    """NiFiCluster CR의 spec을 조회한다 (NiFiFlow 핸들러가 TLS 여부 등을 알기 위해)."""
    obj = custom_api.get_namespaced_custom_object(
        group="datadynamics.io",
        version="v1alpha1",
        namespace=namespace,
        plural="nificlusters",
        name=cluster_name,
    )
    return obj.get("spec", {})


async def _send_webhook(spec: dict, payload: dict):
    """설정된 웹훅 URL로 클러스터 이벤트를 POST한다 (§6.1 오퍼레이터 알림)."""
    url = spec.get("notification", {}).get("webhookUrl")
    if not url:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as hc:
            await hc.post(url, json=payload)
    except Exception as e:  # noqa: BLE001 - 알림 실패가 조정 루프를 막지 않도록
        logger.warning(f"웹훅 전송 실패: {e}")


# =============================================================================
# NiFiCluster 핸들러
# =============================================================================


@kopf.on.create("datadynamics.io", "v1alpha1", "nificlusters")
def on_cluster_create(spec, name, namespace, logger, patch, **kwargs):
    """NiFiCluster 생성 시 모든 Kubernetes 리소스를 프로비저닝합니다.

    생성 순서(멱등, 409 허용):
      Secret → Node SA/Role/RoleBinding → ConfigMap → Services
      → (TLS) 서버/오퍼레이터 인증서 → StatefulSet → (멀티노드) PDB
    모든 리소스에 Owner Reference를 설정하여 CR 삭제 시 함께 삭제됩니다.
    """
    replicas = spec["replicas"]
    tls_enabled = spec.get("tls", {}).get("enabled", False)

    # NiFi 2.x 멀티노드 클러스터링은 보안(TLS)이 사실상 필수다.
    if replicas > 1 and not tls_enabled:
        raise kopf.PermanentError(
            "멀티노드 NiFi 2.x 클러스터는 tls.enabled=true가 필요합니다 "
            "(노드 간 보안 프로토콜 필수)."
        )
    if tls_enabled and not spec.get("tls", {}).get("issuerRef", {}).get("name"):
        raise kopf.PermanentError("tls.enabled=true 시 tls.issuerRef.name이 필요합니다.")

    logger.info(f"NiFi 클러스터 '{name}' 생성 중 (노드 수: {replicas}, TLS: {tls_enabled})")
    patch.status["phase"] = "Creating"
    patch.status["readyNodes"] = 0

    # 1. 자격 증명 Secret (한번 생성되면 갱신 안 함)
    secret = build_credentials_secret(name, namespace, spec)
    kopf.adopt(secret)
    _create_or_ignore(
        core_v1.create_namespaced_secret, namespace, secret,
        resource_desc=f"Secret '{name}-credentials'",
    )

    # 2. NiFi Pod용 RBAC (Kubernetes 리더 선출/상태 관리)
    sa = rbac.build_node_service_account(name, namespace)
    role = rbac.build_node_role(name, namespace)
    binding = rbac.build_node_role_binding(name, namespace)
    for obj, create_fn, args, desc in (
        (sa, core_v1.create_namespaced_service_account, (namespace,), "ServiceAccount"),
        (role, rbac_v1.create_namespaced_role, (namespace,), "Role"),
        (binding, rbac_v1.create_namespaced_role_binding, (namespace,), "RoleBinding"),
    ):
        kopf.adopt(obj)
        _create_or_ignore(create_fn, *args, obj, resource_desc=f"{desc} (node)")

    # 3. ConfigMap (nifi-overrides.properties, merge-config.sh, authorizers.xml)
    configmap = build_nifi_config(name, namespace, spec)
    kopf.adopt(configmap)
    _create_or_ignore(
        core_v1.create_namespaced_config_map, namespace, configmap,
        resource_desc=f"ConfigMap '{name}-config'",
    )

    # 4. Services (headless: 노드 디스커버리, ui: 웹 접근)
    for svc in (build_headless_service(name, namespace), build_ui_service(name, namespace, spec)):
        kopf.adopt(svc)
        _create_or_ignore(
            core_v1.create_namespaced_service, namespace, svc,
            resource_desc=f"Service '{svc.metadata.name}'",
        )

    # 5. TLS 인증서 (서버 1개 + 오퍼레이터 클라이언트 1개)
    if tls_enabled:
        for cert in (
            build_server_certificate(name, namespace, spec),
            build_operator_certificate(name, namespace, spec),
        ):
            kopf.adopt(cert)
            _create_or_ignore(
                custom_api.create_namespaced_custom_object,
                "cert-manager.io", "v1", namespace, "certificates", cert,
                resource_desc=f"Certificate '{cert['metadata']['name']}'",
            )

    # 6. StatefulSet
    statefulset = build_statefulset(name, namespace, spec)
    kopf.adopt(statefulset)
    _create_or_ignore(
        apps_v1.create_namespaced_stateful_set, namespace, statefulset,
        resource_desc=f"StatefulSet '{name}-nifi'",
    )

    # 7. PodDisruptionBudget (멀티노드만)
    if replicas > 1:
        pdb = rbac.build_pod_disruption_budget(name, namespace, spec)
        kopf.adopt(pdb)
        _create_or_ignore(
            policy_v1.create_namespaced_pod_disruption_budget, namespace, pdb,
            resource_desc=f"PDB '{name}-nifi-pdb'",
        )

    patch.status["phase"] = "Running"
    patch.status["version"] = spec["version"]
    return {"message": f"NiFi 클러스터 '{name}' 생성 완료"}


@kopf.on.delete("datadynamics.io", "v1alpha1", "nificlusters")
def on_cluster_delete(name, namespace, logger, **kwargs):
    """NiFiCluster 삭제 처리 (하위 리소스는 Owner Reference로 Cascade Delete)."""
    logger.info(f"NiFi 클러스터 '{name}' 삭제 중 (Cascade Delete)")


@kopf.on.update("datadynamics.io", "v1alpha1", "nificlusters", field="spec.replicas")
async def on_replicas_change(spec, name, namespace, old, new, logger, patch, **kwargs):
    """replicas 변경 감지 → 스케일 업/다운 수행."""
    key = (namespace, name)
    logger.info(f"클러스터 '{name}' 스케일링: {old} → {new}")
    patch.status["phase"] = "Scaling"
    _busy.add(key)
    try:
        if new > old:
            await scale_up(name, namespace, old, new, spec)
        elif new < old:
            await scale_down(name, namespace, old, new, spec, core_v1)

        # ConfigMap 갱신 (노드 수 변경 반영) — 동기 호출은 스레드로
        configmap = build_nifi_config(name, namespace, spec)
        await asyncio.to_thread(
            core_v1.patch_namespaced_config_map, f"{name}-config", namespace, configmap
        )
        await asyncio.to_thread(
            apps_v1.patch_namespaced_stateful_set,
            f"{name}-nifi", namespace, {"spec": {"replicas": new}},
        )

        # Scale Up + TLS: 서버 인증서 SAN에 새 노드 추가되도록 인증서 갱신
        if new > old and spec.get("tls", {}).get("enabled", False):
            cert = build_server_certificate(name, namespace, spec)
            await asyncio.to_thread(
                custom_api.patch_namespaced_custom_object,
                "cert-manager.io", "v1", namespace, "certificates",
                f"{name}-nifi-tls", cert,
            )
    finally:
        _busy.discard(key)

    patch.status["phase"] = "Running"
    logger.info(f"클러스터 '{name}' 스케일링 완료: {new}개 노드")


@kopf.on.update("datadynamics.io", "v1alpha1", "nificlusters", field="spec.config")
async def on_config_change(spec, name, namespace, logger, patch, **kwargs):
    """config 변경 감지 → ConfigMap 업데이트 + 롤링 재시작."""
    key = (namespace, name)
    logger.info(f"클러스터 '{name}' 설정 변경 감지, 롤링 재시작 수행")
    patch.status["phase"] = "Updating"
    _busy.add(key)
    try:
        configmap = build_nifi_config(name, namespace, spec)
        await asyncio.to_thread(
            core_v1.patch_namespaced_config_map, f"{name}-config", namespace, configmap
        )
        restart_patch = {
            "spec": {"template": {"metadata": {"annotations": {
                "nifi-operator/restartedAt": _now()
            }}}}
        }
        await asyncio.to_thread(
            apps_v1.patch_namespaced_stateful_set, f"{name}-nifi", namespace, restart_patch
        )
    finally:
        _busy.discard(key)

    patch.status["phase"] = "Running"
    logger.info("롤링 재시작 트리거됨")


# =============================================================================
# 클러스터 헬스 모니터링 (주기적 타이머)
# =============================================================================


@kopf.timer("datadynamics.io", "v1alpha1", "nificlusters", interval=30, initial_delay=120)
async def monitor_cluster(spec, name, namespace, status, patch, logger, **kwargs):
    """30초마다 클러스터 상태를 확인하고 status를 업데이트합니다.

    스케일링/설정변경 진행 중(_busy)에는 상태를 덮어쓰지 않습니다.
    이상(Error/Degraded)으로 전이하면 웹훅 알림을 1회 전송합니다.
    """
    if (namespace, name) in _busy:
        return

    prev_phase = (status or {}).get("phase")
    try:
        health = await check_cluster_health(name, namespace, spec, core_v1)
        patch.status["readyNodes"] = health.get("readyNodes", 0)
        patch.status["totalNodes"] = health.get("totalNodes", 0)
        patch.status["clusterCoordinator"] = health.get("clusterCoordinator", "unknown")
        patch.status["phase"] = health.get("phase", "Unknown")
        patch.status["message"] = health.get("message", "")
        new_phase = health.get("phase")
    except Exception as e:
        logger.warning(f"클러스터 '{name}' 헬스체크 실패: {e}")
        patch.status["phase"] = "Error"
        patch.status["message"] = str(e)
        new_phase = "Error"

    # 정상 → 비정상 전이 시에만 알림 (반복 알림 방지)
    if new_phase in ("Error", "Degraded") and prev_phase not in ("Error", "Degraded"):
        await _send_webhook(spec, {
            "type": "CLUSTER_UNHEALTHY",
            "cluster": name,
            "namespace": namespace,
            "phase": new_phase,
            "message": patch.status.get("message", ""),
            "timestamp": _now(),
        })


@kopf.timer("datadynamics.io", "v1alpha1", "nificlusters", interval=60, initial_delay=180)
async def auto_heal(spec, name, namespace, logger, **kwargs):
    """60초마다 연결 끊긴 노드를 감지하고 자동 재연결을 시도합니다.

    스케일링 중(_busy)에는 동작하지 않습니다 — 의도적으로 DISCONNECT된 노드를
    되살리는 경합을 방지합니다.
    """
    if (namespace, name) in _busy:
        return
    try:
        await auto_heal_disconnected_nodes(name, namespace, spec, core_v1)
    except Exception as e:
        logger.warning(f"클러스터 '{name}' 자동 복구 실패: {e}")


# =============================================================================
# NiFiFlow 핸들러 (Flow 배포 자동화)
# =============================================================================


async def _flow_client(spec, namespace):
    """NiFiFlow의 clusterRef로 대상 클러스터 spec을 조회하고 NiFiClient를 만든다."""
    cluster_name = spec["clusterRef"]
    cluster_spec = await asyncio.to_thread(_get_cluster_spec, cluster_name, namespace)
    return build_nifi_client(cluster_name, namespace, cluster_spec, core_v1)


@kopf.on.create("datadynamics.io", "v1alpha1", "nififlows")
async def on_flow_create(spec, name, namespace, logger, patch, **kwargs):
    """NiFiFlow 생성 시 NiFi Registry에서 Flow를 가져와 배포합니다."""
    nifi = await _flow_client(spec, namespace)
    patch.status["phase"] = "Deploying"

    registries = await nifi.get_registry_clients()
    if not registries:
        patch.status["phase"] = "Error"
        patch.status["message"] = "NiFi Registry 클라이언트가 설정되어 있지 않습니다"
        return

    registry_id = registries[0].get("id")
    result = await nifi.import_flow_from_registry(
        process_group_id=spec.get("processGroupId", "root"),
        registry_id=registry_id,
        bucket_id=spec.get("bucketId", ""),
        flow_id=spec.get("flowId", ""),
        flow_version=spec.get("flowVersion", 1),
    )

    if result:
        pg_id = result.get("id", "")
        patch.status["deployedProcessGroupId"] = pg_id
        patch.status["deployedVersion"] = spec.get("flowVersion", 1)
        if spec.get("autoStart", True):
            await nifi.set_process_group_state(pg_id, "RUNNING")
            logger.info(f"Flow '{name}' 시작됨 (Process Group: {pg_id})")
        patch.status["phase"] = "Running"
        logger.info(f"Flow '{name}' 배포 완료")
    else:
        patch.status["phase"] = "Error"
        patch.status["message"] = "Registry에서 Flow 가져오기 실패"


@kopf.on.update("datadynamics.io", "v1alpha1", "nififlows", field="spec.flowVersion")
async def on_flow_version_change(spec, name, namespace, old, new, status, logger, patch, **kwargs):
    """flowVersion 변경 감지 → 배포된 Flow를 새 버전으로 업데이트합니다 (GitOps)."""
    pg_id = (status or {}).get("deployedProcessGroupId")
    if not pg_id:
        logger.warning(f"Flow '{name}': 배포된 Process Group이 없어 버전 변경을 건너뜁니다")
        return

    logger.info(f"Flow '{name}' 버전 변경: v{old} → v{new}")
    patch.status["phase"] = "Updating"
    nifi = await _flow_client(spec, namespace)

    registries = await nifi.get_registry_clients()
    if not registries:
        patch.status["phase"] = "Error"
        patch.status["message"] = "NiFi Registry 클라이언트가 설정되어 있지 않습니다"
        return

    ok = await nifi.change_flow_version(
        process_group_id=pg_id,
        registry_id=registries[0].get("id"),
        bucket_id=spec.get("bucketId", ""),
        flow_id=spec.get("flowId", ""),
        flow_version=new,
    )
    if ok:
        patch.status["deployedVersion"] = new
        patch.status["phase"] = "Running"
        logger.info(f"Flow '{name}' 버전 변경 완료: v{new}")
    else:
        patch.status["phase"] = "Error"
        patch.status["message"] = f"버전 v{new}로 변경 실패"


@kopf.on.delete("datadynamics.io", "v1alpha1", "nififlows")
async def on_flow_delete(spec, name, namespace, logger, status, **kwargs):
    """NiFiFlow 삭제 시 배포된 Flow를 중지합니다."""
    pg_id = (status or {}).get("deployedProcessGroupId")
    if not pg_id:
        return
    nifi = await _flow_client(spec, namespace)
    await nifi.set_process_group_state(pg_id, "STOPPED")
    logger.info(f"Flow '{name}' 중지됨 (Process Group: {pg_id})")
