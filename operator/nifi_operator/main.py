"""
NiFi 2.x Kubernetes Operator — 메인 진입점.

kopf 프레임워크를 사용하여 NiFiCluster와 NiFiFlow 커스텀 리소스를 관리합니다.
- NiFiCluster 생성/삭제/스케일링/설정 변경
- NiFiFlow 배포/버전 변경/삭제 (NiFi Registry 연동)
- 주기적 헬스체크 및 장애 노드 자동 복구
- 클러스터 이상 시 웹훅 알림
"""

import asyncio
import copy
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
from nifi_operator.controllers.scaling import scale_up, scale_down, ScaleDownError
from nifi_operator.controllers.cluster import (
    check_cluster_health,
    auto_heal_disconnected_nodes,
    ensure_root_policies,
    wait_for_cluster_ready,
)
from nifi_operator.nifi_api.client import (
    NiFiAPIError,
    NiFiClientConfigError,
    build_nifi_client,
)

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

# 핸들러가 진행 중임을 status.phase로 영속화한다(오퍼레이터 재시작 후에도 유지).
# 이 phase에서는 monitor가 phase를 덮어쓰지 않고, auto_heal이 동작하지 않는다.
_IN_PROGRESS_PHASES = {"Scaling", "Updating"}

# CR 어노테이션으로 자동 복구를 끌 수 있다: nifi.datadynamics.io/auto-heal: "false"
AUTO_HEAL_ANNOTATION = "nifi.datadynamics.io/auto-heal"

# 생성 후 변경을 지원하지 않는 spec 필드 (StatefulSet volumeClaimTemplates 불변,
# TLS 전환은 인증서/설정/인가 전체 재구성이 필요). 변경 시 무시하고 경고만 남긴다.
_IMMUTABLE_FIELDS = ("storage", "tls")
# 변경 시 ConfigMap/StatefulSet 템플릿 갱신 + 롤링 재시작으로 반영하는 spec 필드
_ROLLING_FIELDS = ("version", "image", "resources", "config")

ROLLOUT_STEP_TIMEOUT_SECONDS = 900
SCALE_UP_TIMEOUT_SECONDS = 900


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
    # httpx는 요청마다 INFO 로그를 남긴다(30초 monitor마다 수 건) → 경고 이상만
    logging.getLogger("httpx").setLevel(logging.WARNING)
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
    return _effective_spec(obj.get("spec", {}), obj.get("status"))


def _effective_spec(spec: dict, status) -> dict:
    """리소스 생성에 실제로 쓸 spec — 불변 필드(storage/tls)는 생성 시 적용값을 유지한다.

    생성 시 status.applied에 storage/tls를 기록해 두고, 이후 사용자가 이 필드를 바꿔도
    (지원하지 않는 변경) ConfigMap/StatefulSet/클라이언트가 실제 배포와 어긋나지 않게 한다.
    """
    applied = ((status or {}).get("applied") or {})
    eff = copy.deepcopy(dict(spec or {}))
    for field in _IMMUTABLE_FIELDS:
        if field in applied:
            eff[field] = copy.deepcopy(applied[field])
    return eff


async def _patch_status(name: str, namespace: str, status: dict):
    """핸들러 도중 status를 즉시 기록한다 (kopf patch는 핸들러 종료 시에만 반영됨)."""
    await asyncio.to_thread(
        custom_api.patch_namespaced_custom_object_status,
        "datadynamics.io", "v1alpha1", namespace, "nificlusters", name,
        {"status": status},
    )


async def _set_sts_replicas(name: str, namespace: str, replicas: int):
    await asyncio.to_thread(
        apps_v1.patch_namespaced_stateful_set,
        f"{name}-nifi", namespace, {"spec": {"replicas": replicas}},
    )


async def _apply_configmap(name: str, namespace: str, spec: dict):
    configmap = build_nifi_config(name, namespace, spec)
    await asyncio.to_thread(
        core_v1.patch_namespaced_config_map, f"{name}-config", namespace, configmap
    )


async def _reconcile_pdb(name: str, namespace: str, spec: dict, owner: dict = None):
    """replicas에 맞게 PDB를 생성/갱신/삭제한다.

    멀티노드(replicas>1)면 minAvailable=과반으로 생성 또는 갱신하고, 단일 노드면 삭제한다
    (replicas=1에 minAvailable=1 PDB가 남으면 노드 드레인이 영구히 막힌다).
    """
    pdb_name = f"{name}-nifi-pdb"
    if spec.get("replicas", 1) > 1:
        pdb = rbac.build_pod_disruption_budget(name, namespace, spec)
        if owner is not None:
            kopf.adopt(pdb, owner=owner)
        try:
            await asyncio.to_thread(
                policy_v1.create_namespaced_pod_disruption_budget, namespace, pdb
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status != 409:
                raise
            await asyncio.to_thread(
                policy_v1.patch_namespaced_pod_disruption_budget, pdb_name, namespace,
                {"spec": {"minAvailable": pdb.spec.min_available}},
            )
    else:
        try:
            await asyncio.to_thread(
                policy_v1.delete_namespaced_pod_disruption_budget, pdb_name, namespace
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status != 404:
                raise


async def _sts_partition(name: str, namespace: str, partition: int):
    await asyncio.to_thread(
        apps_v1.patch_namespaced_stateful_set, f"{name}-nifi", namespace,
        {"spec": {"updateStrategy": {
            "type": "RollingUpdate", "rollingUpdate": {"partition": partition},
        }}},
    )


async def _wait_pod_updated(name: str, namespace: str, ordinal: int, timeout_seconds: int) -> bool:
    """Pod ordinal이 StatefulSet의 새 revision으로 재생성되어 Ready가 될 때까지 대기."""
    pod_name = f"{name}-nifi-{ordinal}"
    for _ in range(max(1, timeout_seconds // 5)):
        sts = await asyncio.to_thread(
            apps_v1.read_namespaced_stateful_set, f"{name}-nifi", namespace
        )
        # 템플릿 patch 직후에는 컨트롤러가 아직 새 generation을 관측하지 않아
        # status.updateRevision이 이전 값이다 → 관측 전에는 판단하지 않는다.
        observed = (sts.status.observed_generation or 0) >= (sts.metadata.generation or 0)
        target = sts.status.update_revision if observed else None
        try:
            pod = await asyncio.to_thread(core_v1.read_namespaced_pod, pod_name, namespace)
        except kubernetes.client.exceptions.ApiException as e:
            if e.status != 404:
                raise
            pod = None
        if pod is not None and target and \
                (pod.metadata.labels or {}).get("controller-revision-hash") == target:
            conds = {c.type: c.status for c in (pod.status.conditions or [])}
            if conds.get("Ready") == "True" and pod.metadata.deletion_timestamp is None:
                return True
        await asyncio.sleep(5)
    return False


async def _rolling_restart(name: str, namespace: str, spec: dict, template_body: dict) -> str:
    """NiFi 노드를 한 대씩 재시작한다 (StatefulSet partition 단계 제어).

    Pod readiness는 TCP 리스너 기동만 의미하므로(2.x 보안 클러스터는 HTTP 프로브 불가),
    기본 RollingUpdate에 맡기면 이전 노드가 클러스터에 재조인하기 전에 다음 노드를
    재시작해 전체 노드가 동시에 이탈한다(k3s 테스트에서 확인). 따라서
      1. partition=replicas로 템플릿을 갱신(아직 아무 Pod도 재시작 안 함)
      2. 높은 ordinal부터 partition을 낮춰 한 Pod씩 갱신 → Pod Ready + NiFi 전체 노드가
         연속 3회(약 30초) CONNECTED일 때까지 대기 후 다음 Pod 진행
    대기가 제한 시간을 넘으면 partition=0으로 풀어 나머지는 Kubernetes에 맡기고 메시지를 반환한다.

    Returns:
        결과 메시지 (빈 문자열이면 정상 완료)
    """
    sts = await asyncio.to_thread(apps_v1.read_namespaced_stateful_set, f"{name}-nifi", namespace)
    replicas = sts.spec.replicas or 0
    await asyncio.to_thread(
        apps_v1.patch_namespaced_stateful_set, f"{name}-nifi", namespace,
        {"spec": {
            "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": {"partition": replicas}},
            "template": template_body,
        }},
    )
    for ordinal in range(replicas - 1, -1, -1):
        await _sts_partition(name, namespace, ordinal)
        if not await _wait_pod_updated(name, namespace, ordinal, ROLLOUT_STEP_TIMEOUT_SECONDS):
            await _sts_partition(name, namespace, 0)
            return f"노드 {ordinal} Pod가 {ROLLOUT_STEP_TIMEOUT_SECONDS}초 내에 Ready가 되지 않았습니다"
        if not await wait_for_cluster_ready(
            name, namespace, replicas, spec, core_v1,
            timeout_seconds=ROLLOUT_STEP_TIMEOUT_SECONDS, stable_checks=3,
        ):
            await _sts_partition(name, namespace, 0)
            return f"노드 {ordinal} 재시작 후 {ROLLOUT_STEP_TIMEOUT_SECONDS}초 내에 클러스터가 복구되지 않았습니다"
        logger.info(f"클러스터 '{name}': 노드 {ordinal} 재시작/재조인 완료")
    return ""


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


def _validate_cluster_spec(spec: dict):
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


@kopf.on.create("datadynamics.io", "v1alpha1", "nificlusters")
def on_cluster_create(spec, name, namespace, logger, patch, **kwargs):
    """NiFiCluster 생성 시 모든 Kubernetes 리소스를 프로비저닝합니다.

    생성 순서(멱등, 409 허용):
      Secret → Node SA/Role/RoleBinding → ConfigMap → Services
      → (TLS) 서버/오퍼레이터 인증서 → StatefulSet → (멀티노드) PDB
    모든 리소스에 Owner Reference를 설정하여 CR 삭제 시 함께 삭제됩니다.
    phase는 Creating으로 두고, 모든 노드가 CONNECTED가 되면 monitor가 Running으로 바꿉니다.
    """
    _validate_cluster_spec(spec)
    replicas = spec["replicas"]
    tls_enabled = spec.get("tls", {}).get("enabled", False)

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

    # 3. ConfigMap (nifi-overrides.properties, merge-config.sh, start-nifi.sh, authorizers.xml)
    configmap = build_nifi_config(name, namespace, spec)
    kopf.adopt(configmap)
    _create_or_ignore(
        core_v1.create_namespaced_config_map, namespace, configmap,
        resource_desc=f"ConfigMap '{name}-config'",
    )

    # 4. Services (headless: 노드 디스커버리, ui: 웹 접근)
    for svc in (build_headless_service(name, namespace, spec), build_ui_service(name, namespace, spec)):
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

    patch.status["version"] = spec["version"]
    patch.status["message"] = "리소스 생성 완료 — 노드 기동/클러스터 조인 대기"
    # 생성 후 변경 불가 필드의 적용값 (_effective_spec 참조)
    patch.status["applied"] = {f: spec.get(f, {}) for f in _IMMUTABLE_FIELDS}
    return {"message": f"NiFi 클러스터 '{name}' 생성 완료"}


@kopf.on.delete("datadynamics.io", "v1alpha1", "nificlusters")
def on_cluster_delete(name, namespace, logger, **kwargs):
    """NiFiCluster 삭제 처리.

    Owner Reference가 있는 하위 리소스(StatefulSet/Service/ConfigMap/Secret/SA/Role/
    RoleBinding/PDB/Certificate)는 CR이 사라진 뒤 Cascade Delete된다. Owner Reference가
    없는 다음 리소스는 여기서 정리한다:
      - cert-manager가 만든 TLS Secret(`{name}-nifi-tls`, `{name}-operator-tls`):
        남겨 두면 같은 이름으로 재생성 시 이전 keystore 비밀번호의 PKCS12가 재사용된다.
      - NiFi가 만든 리더 선출 Lease(`{name}-cluster-coordinator`, `{name}-primary-node`)와
        클러스터 state ConfigMap(`{name}-nifi-component-*`, KubernetesConfigMapStateProvider)

    이 핸들러는 CR이 아직 있을 때(finalizer) 실행되므로, 그대로 지우면 cert-manager가 Secret을
    다시 만들고 살아 있는 NiFi 리더가 Lease를 다시 만든다(k3s 테스트에서 확인). 따라서 먼저
    Certificate와 StatefulSet을 지우고 NiFi Pod가 모두 종료된 뒤에 정리한다(종료 대기는 재시도).
    PVC(data-{name}-nifi-N)는 데이터 보호를 위해 삭제하지 않는다(README 참조).
    """
    logger.info(f"NiFi 클러스터 '{name}' 삭제 중 (Cascade Delete)")

    def ignore_404(fn, *args):
        try:
            fn(*args)
            return True
        except kubernetes.client.exceptions.ApiException as e:
            if e.status != 404:
                raise
            return False

    # 1) 재생성 원인 제거: Certificate(→ Secret), StatefulSet(→ Lease/state 갱신)
    for cert_name in (f"{name}-nifi-tls", f"{name}-operator-tls"):
        ignore_404(
            custom_api.delete_namespaced_custom_object,
            "cert-manager.io", "v1", namespace, "certificates", cert_name,
        )
    ignore_404(apps_v1.delete_namespaced_stateful_set, f"{name}-nifi", namespace)
    pods = core_v1.list_namespaced_pod(
        namespace, label_selector=f"app=nifi,cluster={name}"
    ).items
    if pods:
        raise kopf.TemporaryError(f"NiFi Pod {len(pods)}개 종료 대기 중", delay=10)

    # 2) Owner Reference 없는 잔여 리소스 정리
    for secret_name in (f"{name}-nifi-tls", f"{name}-operator-tls"):
        if ignore_404(core_v1.delete_namespaced_secret, secret_name, namespace):
            logger.info(f"TLS Secret '{secret_name}' 삭제")

    coordination_v1 = client.CoordinationV1Api()
    for lease in coordination_v1.list_namespaced_lease(namespace).items:
        if lease.metadata.name in _nifi_lease_names(name):
            ignore_404(coordination_v1.delete_namespaced_lease, lease.metadata.name, namespace)
            logger.info(f"Lease '{lease.metadata.name}' 삭제")
    state_prefix = state_configmap_prefix(name)
    for cm in core_v1.list_namespaced_config_map(namespace).items:
        if cm.metadata.name.startswith(state_prefix) and not cm.metadata.owner_references:
            ignore_404(core_v1.delete_namespaced_config_map, cm.metadata.name, namespace)
            logger.info(f"state ConfigMap '{cm.metadata.name}' 삭제")


def state_configmap_prefix(name: str) -> str:
    """KubernetesConfigMapStateProvider(ConfigMap Name Prefix={name})가 만드는 ConfigMap 이름 접두어."""
    return f"{name}-nifi-component-"


def _nifi_lease_names(name: str) -> set:
    """KubernetesLeaderElectionManager가 lease.prefix={name}으로 만드는 Lease 이름."""
    return {f"{name}-cluster-coordinator", f"{name}-primary-node"}


@kopf.on.update("datadynamics.io", "v1alpha1", "nificlusters", field="spec.replicas")
async def on_replicas_change(spec, status, body, name, namespace, old, new, logger, patch, **kwargs):
    """replicas 변경 감지 → 스케일 업/다운 수행.

    진행 중에는 status.phase=Scaling을 즉시 기록(영속)하여 monitor/auto_heal이
    오퍼레이터 재시작 후에도 개입하지 않게 한다. 스케일 다운 단계가 실패하면
    해당 노드의 Pod는 줄이지 않고 phase=Error로 기록한 뒤 TemporaryError로 재시도한다.
    """
    if old is None or new is None or old == new:
        return
    spec = _effective_spec(spec, status)
    _validate_cluster_spec(spec)
    key = (namespace, name)
    logger.info(f"클러스터 '{name}' 스케일링: {old} → {new}")
    _busy.add(key)
    try:
        await _patch_status(name, namespace, {
            "phase": "Scaling", "message": f"스케일링 {old} → {new} 진행 중",
        })
        # ConfigMap 갱신 (flow election 후보 수 등 노드 수 반영 — 다음 재시작부터 적용)
        await _apply_configmap(name, namespace, spec)

        if new > old:
            await scale_up(name, namespace, old, new, spec)
            await _set_sts_replicas(name, namespace, new)
            await _reconcile_pdb(name, namespace, spec, owner=body)
            joined = await wait_for_cluster_ready(
                name, namespace, new, spec, core_v1, timeout_seconds=SCALE_UP_TIMEOUT_SECONDS
            )
            if not joined:
                logger.warning(f"클러스터 '{name}': 새 노드가 제한 시간 내에 조인하지 않았습니다")
        else:
            async def set_replicas(n: int):
                await _set_sts_replicas(name, namespace, n)

            try:
                await scale_down(
                    name, namespace, old, new, spec, core_v1, set_replicas=set_replicas
                )
            except (ScaleDownError, NiFiClientConfigError) as e:
                msg = f"스케일 다운 중단(재시도 예정): {e}"
                logger.error(msg)
                await _patch_status(name, namespace, {"phase": "Error", "message": msg})
                raise kopf.TemporaryError(msg, delay=60)
            await _set_sts_replicas(name, namespace, new)
            await _reconcile_pdb(name, namespace, spec, owner=body)
    finally:
        _busy.discard(key)

    patch.status["phase"] = "Running"
    patch.status["message"] = f"스케일링 완료: {new}개 노드"
    logger.info(f"클러스터 '{name}' 스케일링 완료: {new}개 노드")


@kopf.on.update("datadynamics.io", "v1alpha1", "nificlusters", field="spec")
async def on_spec_change(old, new, status, body, name, namespace, logger, patch, **kwargs):
    """spec 변경 감지 → 지원 필드는 롤링 재시작으로 반영, 미지원 필드는 경고.

    - version/image/resources/config: ConfigMap + StatefulSet Pod 템플릿(설정 해시 포함)을
      갱신하고, 높은 ordinal부터 한 노드씩 재시작하며 각 노드의 재조인을 기다린다
      (_rolling_restart). 템플릿이 그대로면(설정 동일) 재시작하지 않는다.
    - storage/tls: 생성 후 변경 미지원 — 적용하지 않고 status.message/Event로 알린다.
    - replicas: on_replicas_change가 처리한다.
    """
    old = dict(old or {})
    new = dict(new or {})
    changed = {k for k in set(old) | set(new) if old.get(k) != new.get(k)}
    immutable = sorted(changed & set(_IMMUTABLE_FIELDS))
    rolling = sorted(changed & set(_ROLLING_FIELDS))

    if immutable:
        msg = (
            f"spec.{', spec.'.join(immutable)} 변경은 지원하지 않아 적용하지 않았습니다 "
            "(클러스터 재생성 필요)"
        )
        logger.warning(msg)
        kopf.warn(body, reason="UnsupportedChange", message=msg)
        patch.status["message"] = msg
    if not rolling:
        return

    spec = _effective_spec(new, status)
    key = (namespace, name)
    logger.info(f"클러스터 '{name}' 변경 감지({', '.join(rolling)}) — 롤링 재시작 수행")
    _busy.add(key)
    try:
        await _patch_status(name, namespace, {
            "phase": "Updating", "message": f"{', '.join(rolling)} 변경 롤링 재시작 중",
        })
        await _apply_configmap(name, namespace, spec)

        template = build_statefulset(name, namespace, spec).spec.template
        template_body = client.ApiClient().sanitize_for_serialization(template)
        problem = await _rolling_restart(name, namespace, spec, template_body)
    finally:
        _busy.discard(key)

    patch.status["phase"] = "Running"
    patch.status["version"] = spec["version"]
    if not problem:
        patch.status["message"] = f"{', '.join(rolling)} 변경 반영 완료"
        logger.info("롤링 재시작 완료")
    else:
        patch.status["message"] = f"롤링 재시작 경고: {problem}"
        logger.warning(patch.status["message"])


# =============================================================================
# 클러스터 헬스 모니터링 (주기적 타이머)
# =============================================================================


@kopf.timer("datadynamics.io", "v1alpha1", "nificlusters", interval=30, initial_delay=60)
async def monitor_cluster(spec, name, namespace, status, patch, logger, **kwargs):
    """30초마다 클러스터 상태를 확인하고 status를 업데이트합니다.

    스케일링/설정변경 진행 중(_busy 또는 영속된 phase Scaling/Updating)에는 상태를
    덮어쓰지 않습니다. 생성 중(Creating)에는 아직 연결되지 않아도 Creating을 유지하고
    알림을 보내지 않습니다. 이상(Error/Degraded)으로 전이하면 웹훅 알림을 1회 전송합니다.
    """
    prev_phase = (status or {}).get("phase")
    if (namespace, name) in _busy or prev_phase in _IN_PROGRESS_PHASES:
        return
    spec = _effective_spec(spec, status)

    try:
        health = await check_cluster_health(name, namespace, spec, core_v1)
        new_phase = health.get("phase", "Error")
        message = health.get("message", "")
        patch.status["readyNodes"] = health.get("readyNodes", 0)
        patch.status["totalNodes"] = health.get("totalNodes", 0)
        patch.status["clusterCoordinator"] = health.get("clusterCoordinator", "unknown")
    except NiFiClientConfigError as e:
        new_phase, message = "Error", str(e)
    except Exception as e:
        logger.warning(f"클러스터 '{name}' 헬스체크 실패: {e}")
        new_phase, message = "Error", str(e)

    if prev_phase == "Creating" and new_phase != "Running":
        # 생성 직후 기동/조인 대기 중 — Creating 유지, 알림 없음
        patch.status["message"] = f"기동 대기: {message}"
        return

    patch.status["phase"] = new_phase
    patch.status["message"] = message

    # TLS 클러스터가 처음 정상화되면 root PG 정책을 1회 부여한다 (ensure_root_policies 참조)
    if (
        new_phase == "Running"
        and spec.get("tls", {}).get("enabled", False)
        and not (status or {}).get("rootPoliciesApplied")
    ):
        try:
            changed = await ensure_root_policies(name, namespace, spec, core_v1)
            patch.status["rootPoliciesApplied"] = True
            if changed:
                logger.info(f"클러스터 '{name}' root PG 정책 부여: {changed}")
        except Exception as e:  # noqa: BLE001 - 다음 주기에 재시도
            logger.warning(f"클러스터 '{name}' root PG 정책 부여 실패(재시도 예정): {e}")

    # 정상 → 비정상 전이 시에만 알림 (반복 알림 방지)
    if new_phase in ("Error", "Degraded") and prev_phase not in ("Error", "Degraded"):
        await _send_webhook(spec, {
            "type": "CLUSTER_UNHEALTHY",
            "cluster": name,
            "namespace": namespace,
            "phase": new_phase,
            "message": message,
            "timestamp": _now(),
        })


@kopf.timer("datadynamics.io", "v1alpha1", "nificlusters", interval=60, initial_delay=180)
async def auto_heal(spec, name, namespace, status, meta, logger, **kwargs):
    """60초마다 연결 끊긴 노드를 감지하고 자동 재연결을 시도합니다.

    다음 경우에는 동작하지 않습니다:
      - 스케일링/롤링 재시작 진행 중(_busy 또는 영속된 phase Scaling/Updating) 또는 Creating
      - CR 어노테이션 nifi.datadynamics.io/auto-heal: "false"
    노드 단위 제외 규칙(관리자가 직접 연결 해제한 노드, 축소 대상 ordinal,
    OFFLOADING/OFFLOADED)은 auto_heal_disconnected_nodes가 적용합니다.
    """
    phase = (status or {}).get("phase")
    if (namespace, name) in _busy or phase in _IN_PROGRESS_PHASES or phase == "Creating":
        return
    if (meta.get("annotations") or {}).get(AUTO_HEAL_ANNOTATION, "").lower() == "false":
        return
    try:
        await auto_heal_disconnected_nodes(
            name, namespace, _effective_spec(spec, status), core_v1
        )
    except Exception as e:
        logger.warning(f"클러스터 '{name}' 자동 복구 실패: {e}")


# =============================================================================
# NiFiFlow 핸들러 (Flow 배포 자동화)
# =============================================================================


async def _flow_client(spec, namespace):
    """NiFiFlow의 clusterRef로 대상 클러스터 spec을 조회하고 NiFiClient를 만든다.

    클러스터 클라이언트를 아직 만들 수 없으면(TLS 인증서 미발급 등) TemporaryError로 재시도.
    """
    cluster_name = spec["clusterRef"]
    cluster_spec = await asyncio.to_thread(_get_cluster_spec, cluster_name, namespace)
    try:
        return build_nifi_client(cluster_name, namespace, cluster_spec, core_v1)
    except NiFiClientConfigError as e:
        raise kopf.TemporaryError(f"NiFi 클라이언트 준비 안 됨: {e}", delay=30)


def _select_registry_client(registries, registry_url: str) -> str:
    """사용할 NiFi Registry 클라이언트 ID를 고른다.

    - spec.registryUrl이 있으면 url 속성이 일치하는 클라이언트를 사용한다.
    - 없으면 클라이언트가 정확히 하나일 때만 그것을 사용한다(임의 선택 금지).
    """
    if registries is None:
        raise kopf.TemporaryError("NiFi Registry 클라이언트 목록 조회 실패", delay=30)
    if not registries:
        raise kopf.TemporaryError("NiFi Registry 클라이언트가 설정되어 있지 않습니다", delay=60)

    def url_of(r):
        return ((r.get("component") or {}).get("properties") or {}).get("url", "") or ""

    if registry_url:
        want = registry_url.rstrip("/")
        matches = [r for r in registries if url_of(r).rstrip("/") == want]
        if len(matches) == 1:
            return matches[0]["id"]
        raise kopf.TemporaryError(
            f"registryUrl '{registry_url}'과 일치하는 Registry 클라이언트가 "
            f"{len(matches)}개입니다 (1개여야 함)", delay=60,
        )
    if len(registries) == 1:
        return registries[0]["id"]
    raise kopf.TemporaryError(
        f"Registry 클라이언트가 {len(registries)}개입니다 — spec.registryUrl로 지정하세요",
        delay=60,
    )


def _claimed_process_groups(namespace: str, exclude: str) -> set:
    """같은 네임스페이스의 다른 NiFiFlow가 이미 관리 중인 Process Group ID 집합."""
    flows = custom_api.list_namespaced_custom_object(
        "datadynamics.io", "v1alpha1", namespace, "nififlows"
    )
    return {
        (f.get("status") or {}).get("deployedProcessGroupId")
        for f in flows.get("items", [])
        if f["metadata"]["name"] != exclude
    } - {None, ""}


def _record_flow_failure(patch, e: Exception):
    """재시도할 실패를 NiFiFlow status에 남긴다 (kopf는 실패 시에도 patch를 반영한다)."""
    patch.status["phase"] = "Error"
    patch.status["message"] = str(e)


@kopf.on.create("datadynamics.io", "v1alpha1", "nififlows")
async def on_flow_create(spec, name, namespace, status, logger, patch, **kwargs):
    """NiFiFlow 생성 핸들러 — 실패 사유를 status에 기록한 뒤 재시도한다."""
    try:
        await _deploy_flow(spec, name, namespace, status, logger, patch)
    except kopf.TemporaryError as e:
        _record_flow_failure(patch, e)
        raise


async def _deploy_flow(spec, name, namespace, status, logger, patch):
    """NiFiFlow 생성 시 NiFi Registry에서 Flow를 가져와 배포합니다.

    재시도에 안전(멱등)하다: 이미 배포한 PG(status) 또는 같은 Registry/bucket/flow로
    버전 관리되는 미관리 하위 PG가 있으면 새로 만들지 않고 재사용한다.
    실패는 TemporaryError로 재시도한다.
    """
    nifi = await _flow_client(spec, namespace)
    parent = spec.get("processGroupId") or "root"
    bucket_id = spec.get("bucketId", "")
    flow_id = spec.get("flowId", "")
    flow_version = spec.get("flowVersion", 1)

    registry_id = _select_registry_client(
        await nifi.get_registry_clients(), spec.get("registryUrl", "")
    )

    try:
        pg_id = (status or {}).get("deployedProcessGroupId")
        if pg_id and await nifi.get_process_group(pg_id) is None:
            pg_id = None
        if not pg_id:
            claimed = await asyncio.to_thread(_claimed_process_groups, namespace, name)
            for pg in await nifi.list_child_process_groups(parent):
                vci = (pg.get("component") or {}).get("versionControlInformation") or {}
                if (
                    pg["id"] not in claimed
                    and vci.get("registryId") == registry_id
                    and vci.get("bucketId") == bucket_id
                    and vci.get("flowId") == flow_id
                ):
                    pg_id = pg["id"]
                    logger.info(f"Flow '{name}': 기존 Process Group {pg_id} 재사용 (재시도)")
                    break
        if not pg_id:
            result = await nifi.import_flow_from_registry(
                process_group_id=parent,
                registry_id=registry_id,
                bucket_id=bucket_id,
                flow_id=flow_id,
                flow_version=flow_version,
            )
            pg_id = result["id"]
            logger.info(f"Flow '{name}' 배포됨 (Process Group: {pg_id})")
    except (NiFiAPIError, httpx.HTTPError) as e:
        raise kopf.TemporaryError(str(e), delay=60)

    patch.status["deployedProcessGroupId"] = pg_id
    patch.status["deployedVersion"] = flow_version
    if spec.get("autoStart", True):
        if not await nifi.set_process_group_state(pg_id, "RUNNING"):
            raise kopf.TemporaryError(f"Process Group {pg_id} 시작 실패", delay=60)
        logger.info(f"Flow '{name}' 시작됨 (Process Group: {pg_id})")
        patch.status["phase"] = "Running"
    else:
        patch.status["phase"] = "Stopped"
    patch.status["message"] = ""
    logger.info(f"Flow '{name}' 배포 완료")


@kopf.on.update("datadynamics.io", "v1alpha1", "nififlows", field="spec.flowVersion")
async def on_flow_version_change(spec, name, namespace, old, new, status, logger, patch, **kwargs):
    """flowVersion 변경 핸들러 — 실패 사유를 status에 기록한 뒤 재시도한다."""
    try:
        await _change_flow_version(spec, name, namespace, old, new, status, logger, patch)
    except kopf.TemporaryError as e:
        _record_flow_failure(patch, e)
        raise


async def _change_flow_version(spec, name, namespace, old, new, status, logger, patch):
    """flowVersion 변경 감지 → 배포된 Flow를 새 버전으로 업데이트합니다 (GitOps).

    NiFi의 비동기 update-request가 완료될 때까지 기다리고, 실패하면 TemporaryError로
    재시도한다. 이미 목표 버전이면(재시도) 아무것도 하지 않는다.
    """
    pg_id = (status or {}).get("deployedProcessGroupId")
    if not pg_id:
        raise kopf.TemporaryError(
            f"Flow '{name}': 배포된 Process Group이 아직 없어 버전 변경을 대기합니다", delay=60
        )

    logger.info(f"Flow '{name}' 버전 변경: v{old} → v{new}")
    nifi = await _flow_client(spec, namespace)
    vci = await nifi.get_version_control_information(pg_id)
    current = ((vci or {}).get("versionControlInformation") or {}).get("version")
    if current is not None and str(current) == str(new):
        patch.status["deployedVersion"] = new
        patch.status["phase"] = "Running" if spec.get("autoStart", True) else "Stopped"
        return

    registry_id = _select_registry_client(
        await nifi.get_registry_clients(), spec.get("registryUrl", "")
    )
    try:
        await nifi.change_flow_version(
            process_group_id=pg_id,
            registry_id=registry_id,
            bucket_id=spec.get("bucketId", ""),
            flow_id=spec.get("flowId", ""),
            flow_version=new,
        )
    except (NiFiAPIError, httpx.HTTPError) as e:
        raise kopf.TemporaryError(f"버전 v{new}로 변경 실패: {e}", delay=60)

    patch.status["deployedVersion"] = new
    patch.status["phase"] = "Running" if spec.get("autoStart", True) else "Stopped"
    patch.status["message"] = ""
    logger.info(f"Flow '{name}' 버전 변경 완료: v{new}")


@kopf.on.delete("datadynamics.io", "v1alpha1", "nififlows")
async def on_flow_delete(spec, name, namespace, logger, status, **kwargs):
    """NiFiFlow 삭제 시 배포된 Process Group을 중지 후 삭제합니다.

    큐에 FlowFile이 남아 있으면 NiFi가 삭제를 거부한다 — 데이터를 임의로 버리지 않고
    TemporaryError로 재시도한다(큐를 비우면 삭제가 완료됨). 대상 NiFiCluster가 이미
    없으면 정리할 것이 없으므로 그대로 삭제를 허용한다.
    """
    pg_id = (status or {}).get("deployedProcessGroupId")
    if not pg_id:
        return
    try:
        nifi = await _flow_client(spec, namespace)
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.info(f"Flow '{name}': 대상 클러스터가 없어 정리를 건너뜁니다")
            return
        raise
    try:
        if await nifi.get_process_group(pg_id) is None:
            return
        await nifi.set_process_group_state(pg_id, "STOPPED")
        await nifi.set_controller_services_state(pg_id, "DISABLED")
        await nifi.delete_process_group(pg_id)
    except (NiFiAPIError, httpx.HTTPError) as e:
        raise kopf.TemporaryError(f"Flow '{name}' Process Group 삭제 실패: {e}", delay=60)
    logger.info(f"Flow '{name}' Process Group {pg_id} 삭제됨")
