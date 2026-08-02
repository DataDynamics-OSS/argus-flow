"""
NiFi 2.x 클러스터 노드용 StatefulSet 빌더.

StatefulSet은 각 NiFi 노드에 안정적인 DNS(Pod FQDN)와 전용 PVC를 제공한다.

설정은 init 컨테이너가 **공식 NiFi 이미지의 기본 conf 위에 override를 병합**하여
생성한다(configmap.py의 merge-config.sh 참조). 이렇게 하면 nifi.properties의 필수
기본값을 유지하면서 클러스터/보안 관련 키만 덮어쓸 수 있다.
"""

from kubernetes import client

from nifi_operator.resources.rbac import node_service_account_name

# TLS 인증서 Secret 마운트 경로 (configmap.py의 TLS_MOUNT과 일치해야 함)
TLS_MOUNT = "/opt/nifi/tls"
NIFI_UID = 1000


def _labels(name: str) -> dict:
    return {
        "app": "nifi",
        "cluster": name,
        "app.kubernetes.io/managed-by": "nifi-operator",
    }


def _pod_field_env(env_name: str, field_path: str) -> client.V1EnvVar:
    return client.V1EnvVar(
        name=env_name,
        value_from=client.V1EnvVarSource(
            field_ref=client.V1ObjectFieldSelector(field_path=field_path)
        ),
    )


def _secret_env(env_name: str, secret_name: str, key: str) -> client.V1EnvVar:
    return client.V1EnvVar(
        name=env_name,
        value_from=client.V1EnvVarSource(
            secret_key_ref=client.V1SecretKeySelector(name=secret_name, key=key)
        ),
    )


def build_statefulset(name: str, namespace: str, spec: dict) -> client.V1StatefulSet:
    """NiFi 클러스터를 위한 StatefulSet을 생성한다.

    Args:
        name: NiFiCluster CR 이름
        namespace: 네임스페이스
        spec: NiFiCluster spec

    Returns:
        V1StatefulSet 객체
    """
    replicas = spec.get("replicas", 1)
    version = spec["version"]
    image = spec.get("image", "apache/nifi")
    image_ref = f"{image}:{version}"
    resources_spec = spec.get("resources", {})
    storage = spec.get("storage", {})
    config = spec.get("config", {})
    tls_enabled = spec.get("tls", {}).get("enabled", False)
    headless = f"{name}-headless"
    secret_name = f"{name}-credentials"

    labels = _labels(name)
    web_port = 8443 if tls_enabled else 8080

    # =========================================================================
    # init 컨테이너 env: Downward API + Secret. merge-config.sh가 사용한다.
    # =========================================================================
    init_env = [
        _pod_field_env("NIFI_POD_NAME", "metadata.name"),
        _pod_field_env("NIFI_POD_NAMESPACE", "metadata.namespace"),
        client.V1EnvVar(name="HEADLESS_SERVICE", value=headless),
        _secret_env("SENSITIVE_PROPS_KEY", secret_name, "sensitive-props-key"),
    ]
    if tls_enabled:
        init_env += [
            _secret_env("KEYSTORE_PASSWORD", secret_name, "keystore-password"),
            _secret_env("TRUSTSTORE_PASSWORD", secret_name, "keystore-password"),
        ]

    init_container = client.V1Container(
        name="merge-config",
        image=image_ref,  # 기본 conf를 얻기 위해 NiFi 이미지 자체를 사용
        command=["sh", "/scripts/merge-config.sh"],
        env=init_env,
        volume_mounts=[
            client.V1VolumeMount(name="overrides", mount_path="/overrides"),
            client.V1VolumeMount(name="scripts", mount_path="/scripts"),
            client.V1VolumeMount(name="config-rendered", mount_path="/work-conf"),
        ],
    )

    # =========================================================================
    # 메인 NiFi 컨테이너
    # =========================================================================
    ports = [
        client.V1ContainerPort(container_port=web_port, name="web"),
        client.V1ContainerPort(container_port=11443, name="cluster"),
        client.V1ContainerPort(container_port=6342, name="load-balance"),
    ]

    volume_mounts = [
        client.V1VolumeMount(name="data", mount_path="/opt/nifi/nifi-current/data"),
        # 병합된 conf 디렉터리 전체를 덮어쓴다 (개별 subPath 아님 → 필수 파일 보존)
        client.V1VolumeMount(
            name="config-rendered", mount_path="/opt/nifi/nifi-current/conf"
        ),
    ]
    if tls_enabled:
        volume_mounts.append(
            client.V1VolumeMount(name="tls", mount_path=TLS_MOUNT, read_only=True)
        )

    # 프로브: 인증이 필요한 REST API 대신 TCP 소켓으로 리스너 기동을 확인한다
    # (2.x 보안 클러스터에서 REST는 401을 반환하므로 HTTP 프로브는 항상 실패).
    # 실제 클러스터 조인 상태는 오퍼레이터의 monitor 타이머가 인증된 API로 확인한다.
    tcp_probe = client.V1TCPSocketAction(port=web_port)
    container = client.V1Container(
        name="nifi",
        image=image_ref,
        ports=ports,
        env=[
            client.V1EnvVar(name="NIFI_JVM_HEAP_INIT", value=config.get("jvmHeapSize", "1g")),
            client.V1EnvVar(name="NIFI_JVM_HEAP_MAX", value=config.get("jvmHeapSize", "1g")),
        ],
        resources=client.V1ResourceRequirements(
            requests={
                "cpu": resources_spec.get("requests", {}).get("cpu", "1"),
                "memory": resources_spec.get("requests", {}).get("memory", "2Gi"),
            },
            limits={
                "cpu": resources_spec.get("limits", {}).get("cpu", "2"),
                "memory": resources_spec.get("limits", {}).get("memory", "4Gi"),
            },
        ),
        volume_mounts=volume_mounts,
        # startup 프로브: 느린 부팅 허용 (최대 10분). 성공 전까지 liveness 미적용.
        startup_probe=client.V1Probe(
            tcp_socket=tcp_probe,
            period_seconds=10,
            failure_threshold=60,
        ),
        readiness_probe=client.V1Probe(
            tcp_socket=tcp_probe,
            period_seconds=15,
            failure_threshold=4,
        ),
        liveness_probe=client.V1Probe(
            tcp_socket=tcp_probe,
            period_seconds=30,
            failure_threshold=5,
        ),
    )

    # =========================================================================
    # 볼륨
    # =========================================================================
    volumes = [
        client.V1Volume(
            name="overrides",
            config_map=client.V1ConfigMapVolumeSource(name=f"{name}-config"),
        ),
        client.V1Volume(
            name="scripts",
            config_map=client.V1ConfigMapVolumeSource(
                name=f"{name}-config",
                items=[client.V1KeyToPath(key="merge-config.sh", path="merge-config.sh")],
            ),
        ),
        client.V1Volume(
            name="config-rendered", empty_dir=client.V1EmptyDirVolumeSource()
        ),
    ]
    if tls_enabled:
        volumes.append(
            client.V1Volume(
                name="tls",
                secret=client.V1SecretVolumeSource(secret_name=f"{name}-nifi-tls"),
            )
        )

    # =========================================================================
    # PVC 템플릿
    # =========================================================================
    pvc_template = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name="data"),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            storage_class_name=storage.get("storageClassName"),
            resources=client.V1VolumeResourceRequirements(
                requests={"storage": storage.get("size", "10Gi")}
            ),
        ),
    )

    # 노드가 서로 다른 K8s 노드에 분산되도록 선호(soft) anti-affinity
    affinity = client.V1Affinity(
        pod_anti_affinity=client.V1PodAntiAffinity(
            preferred_during_scheduling_ignored_during_execution=[
                client.V1WeightedPodAffinityTerm(
                    weight=100,
                    pod_affinity_term=client.V1PodAffinityTerm(
                        topology_key="kubernetes.io/hostname",
                        label_selector=client.V1LabelSelector(
                            match_labels={"app": "nifi", "cluster": name}
                        ),
                    ),
                )
            ]
        )
    )

    return client.V1StatefulSet(
        metadata=client.V1ObjectMeta(
            name=f"{name}-nifi", namespace=namespace, labels=labels
        ),
        spec=client.V1StatefulSetSpec(
            replicas=replicas,
            service_name=headless,
            pod_management_policy="OrderedReady",
            selector=client.V1LabelSelector(
                match_labels={"app": "nifi", "cluster": name}
            ),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=labels),
                spec=client.V1PodSpec(
                    service_account_name=node_service_account_name(name),
                    security_context=client.V1PodSecurityContext(
                        run_as_user=NIFI_UID,
                        run_as_group=NIFI_UID,
                        fs_group=NIFI_UID,  # PVC/emptyDir를 nifi uid가 쓸 수 있게
                        run_as_non_root=True,
                    ),
                    affinity=affinity,
                    init_containers=[init_container],
                    containers=[container],
                    volumes=volumes,
                    termination_grace_period_seconds=60,
                ),
            ),
            volume_claim_templates=[pvc_template],
        ),
    )
