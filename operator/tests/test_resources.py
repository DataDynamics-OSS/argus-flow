"""쿠버네티스 리소스 빌더 단위 테스트.

실제 K8s 클러스터 없이 순수 단위 테스트로 리소스 객체를 검증함.
"""

import pytest

from nifi_operator.resources.configmap import build_nifi_config
from nifi_operator.resources.service import build_headless_service, build_ui_service
from nifi_operator.resources.statefulset import (
    build_statefulset,
    config_hash,
    CONFIG_HASH_ANNOTATION,
)
from nifi_operator.resources.secret import build_credentials_secret
from nifi_operator.resources.certificate import (
    build_server_certificate,
    build_operator_certificate,
)
from nifi_operator.resources import rbac

CLUSTER_NAME = "test"
NAMESPACE = "default"


# ---- build_nifi_config (override 병합 방식) ----


class TestBuildNifiConfig:
    def test_configmap_이름_네임스페이스_레이블(self, sample_spec):
        cm = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert cm.metadata.name == "test-config"
        assert cm.metadata.namespace == NAMESPACE
        assert cm.metadata.labels["app"] == "nifi"
        assert cm.metadata.labels["cluster"] == CLUSTER_NAME

    def test_데이터_키(self, sample_spec):
        """override properties와 merge 스크립트가 포함되어야 한다."""
        cm = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert "nifi-overrides.properties" in cm.data
        assert "merge-config.sh" in cm.data

    def test_클러스터_k8s_리더선출_설정(self, sample_spec):
        """NiFi 2.x Kubernetes 클러스터링 프로퍼티가 포함되어야 한다."""
        props = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec).data[
            "nifi-overrides.properties"
        ]
        assert "nifi.cluster.is.node=true" in props
        assert (
            "nifi.cluster.leader.election.implementation=KubernetesLeaderElectionManager"
            in props
        )
        assert "nifi.state.management.provider.cluster=kubernetes-provider" in props
        # 구버전의 초기 노드 목록 프로퍼티는 존재하면 안 된다 (2.x에 없음)
        assert "nifi.cluster.node.1=" not in props

    def test_민감키_플레이스홀더(self, sample_spec):
        """민감키는 평문이 아니라 플레이스홀더여야 한다 (init이 Secret에서 치환)."""
        props = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec).data[
            "nifi-overrides.properties"
        ]
        assert "nifi.sensitive.props.key=@SENSITIVE_PROPS_KEY@" in props
        assert "test-secret-key" not in props  # 평문 유출 없음

    def test_tls_시_authorizers_및_keystore(self, sample_spec):
        cm = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert "authorizers.xml" in cm.data
        props = cm.data["nifi-overrides.properties"]
        assert "nifi.security.keystore=/opt/nifi/tls/keystore.p12" in props
        assert "nifi.web.https.port=8443" in props

    def test_비TLS_단일노드는_http(self, minimal_spec):
        cm = build_nifi_config(CLUSTER_NAME, NAMESPACE, minimal_spec)
        assert "authorizers.xml" not in cm.data
        props = cm.data["nifi-overrides.properties"]
        assert "nifi.web.http.port=8080" in props
        # https 커넥터를 끄고, 이미지 기본 keystore 경로도 비워야 HTTP 모드로 기동된다
        assert "nifi.web.https.port=\n" in props
        assert "nifi.security.keystore=\n" in props
        assert "nifi.security.truststore=\n" in props

    def test_영속_경로는_PVC_아래(self, sample_spec):
        """flow/리포지토리/DB가 PVC(data) 아래에 있어야 재시작 후 보존된다."""
        props = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec).data[
            "nifi-overrides.properties"
        ]
        data = "/opt/nifi/nifi-current/data"
        for key in (
            "nifi.flow.configuration.file",
            "nifi.flowfile.repository.directory",
            "nifi.content.repository.directory.default",
            "nifi.provenance.repository.directory.default",
            "nifi.database.directory",
        ):
            line = next(l for l in props.splitlines() if l.startswith(key + "="))
            assert line.split("=", 1)[1].startswith(data + "/"), line

    def test_merge_스크립트_state_및_heap(self, sample_spec):
        script = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec).data["merge-config.sh"]
        assert 'cp -a "$SRC"' not in script  # emptyDir 루트 시간 보존 실패 방지
        assert 'cp -R "$SRC"/. "$DST"/' in script
        assert "./state/local" in script and "${DATA}/state/local" in script
        assert "ConfigMap Name Prefix" in script
        assert "java.arg.3=-Xmx" in script

    def test_start_스크립트는_start_sh를_쓰지_않음(self, sample_spec):
        """이미지 start.sh는 병합된 nifi.properties를 덮어쓰므로 nifi.sh run을 직접 실행한다."""
        script = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec).data["start-nifi.sh"]
        assert "bin/nifi.sh run" in script
        assert "start.sh" not in script
        assert "bin/nifi.sh stop" in script  # SIGTERM → graceful shutdown
        assert "nifi-app.log" in script  # 로그를 stdout으로

    def test_tls_인가_설정(self, sample_spec):
        props = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec).data[
            "nifi-overrides.properties"
        ]
        assert "nifi.security.user.authorizer=managed-authorizer" in props
        assert "nifi.security.user.login.identity.provider=\n" in props
        # port-forward(localhost) 접속을 위해 인터페이스 단위 바인딩
        assert "nifi.web.https.network.interface.lo=lo" in props
        # 외부 프록시 호스트 반영
        assert "nifi.example.com" in props

    def test_authorizers_노드identity는_Initial_User로_시드(self, sample_spec):
        """FileUserGroupProvider는 Initial User Identity만 사용자로 만든다 → 노드 DN도 여기 있어야 함."""
        xml = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec).data["authorizers.xml"]
        node_dn = "CN=test-nifi.default.svc.cluster.local"
        ugp = xml.split("<userGroupProvider>")[1].split("</userGroupProvider>")[0]
        app = xml.split("<accessPolicyProvider>")[1].split("</accessPolicyProvider>")[0]
        assert f'<property name="Initial User Identity nodes">{node_dn}</property>' in ugp
        assert f'<property name="Node Identity 1">{node_dn}</property>' in app
        assert "Node Identity" not in ugp
        assert "/opt/nifi/nifi-current/data/conf/users.xml" in xml
        assert "/opt/nifi/nifi-current/data/conf/authorizations.xml" in xml

    def test_authorizers는_노드수와_무관(self, sample_spec):
        """공유 서버 인증서 → 스케일 업 시 authorizers.xml이 바뀌지 않아야 한다."""
        a = build_nifi_config(CLUSTER_NAME, NAMESPACE, sample_spec).data["authorizers.xml"]
        b = build_nifi_config(CLUSTER_NAME, NAMESPACE, {**sample_spec, "replicas": 5}).data["authorizers.xml"]
        assert a == b


# ---- build_credentials_secret ----


class TestCredentialsSecret:
    def test_secret_키(self, sample_spec):
        s = build_credentials_secret(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert s.metadata.name == "test-credentials"
        assert "sensitive-props-key" in s.string_data
        assert "keystore-password" in s.string_data

    def test_spec_민감키_우선(self, sample_spec):
        s = build_credentials_secret(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert s.string_data["sensitive-props-key"] == "test-secret-key"

    def test_민감키_없으면_생성(self, minimal_spec):
        s = build_credentials_secret(CLUSTER_NAME, NAMESPACE, minimal_spec)
        assert len(s.string_data["sensitive-props-key"]) >= 16


# ---- Services ----


class TestServices:
    def test_headless_clusterip_none_및_포트(self, sample_spec):
        svc = build_headless_service(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert svc.spec.cluster_ip == "None"
        assert svc.spec.publish_not_ready_addresses is True
        names = {p.name for p in svc.spec.ports}
        assert {"https", "cluster", "load-balance"} <= names

    def test_ui_clusterip(self, sample_spec):
        svc = build_ui_service(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert svc.spec.type == "ClusterIP"
        assert svc.metadata.name == "test-ui"
        assert svc.spec.ports[0].port == 8443

    def test_비TLS_서비스는_8080(self, minimal_spec):
        """비-TLS NiFi는 8080(http)만 리슨하므로 Service도 8080이어야 한다."""
        for svc in (
            build_ui_service(CLUSTER_NAME, NAMESPACE, minimal_spec),
            build_headless_service(CLUSTER_NAME, NAMESPACE, minimal_spec),
        ):
            web = svc.spec.ports[0]
            assert (web.name, web.port, web.target_port) == ("http", 8080, 8080)


# ---- build_statefulset ----


class TestBuildStatefulSet:
    def test_기본_속성(self, sample_spec):
        sts = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert sts.spec.replicas == 3
        assert sts.spec.service_name == "test-headless"
        assert sts.spec.template.spec.containers[0].image == "apache/nifi:2.0.0"

    def test_노드_서비스어카운트(self, sample_spec):
        sts = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert sts.spec.template.spec.service_account_name == "test-nifi"

    def test_init_컨테이너는_nifi_이미지로_merge(self, sample_spec):
        """init 컨테이너는 기본 conf를 얻기 위해 NiFi 이미지를 쓰고 merge를 실행한다."""
        init = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.spec.init_containers
        assert len(init) == 1
        assert init[0].name == "merge-config"
        assert init[0].image == "apache/nifi:2.0.0"
        assert "merge-config.sh" in " ".join(init[0].command)

    def test_conf_전체디렉터리_마운트(self, sample_spec):
        """subPath가 아니라 conf 디렉터리 전체를 마운트해야 필수 기본값이 보존된다."""
        c = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.spec.containers[0]
        conf_mounts = [m for m in c.volume_mounts if m.mount_path == "/opt/nifi/nifi-current/conf"]
        assert len(conf_mounts) == 1
        assert conf_mounts[0].sub_path is None

    def test_프로브는_TCP소켓(self, sample_spec):
        """인증이 필요한 REST 대신 TCP 소켓 프로브 (401 방지)."""
        c = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.spec.containers[0]
        assert c.readiness_probe.tcp_socket is not None
        assert c.readiness_probe.http_get is None
        assert c.startup_probe is not None
        assert c.startup_probe.tcp_socket.port == 8443

    def test_보안컨텍스트_fsgroup(self, sample_spec):
        pod = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.spec
        assert pod.security_context.fs_group == 1000
        assert pod.security_context.run_as_non_root is True

    def test_tls_시크릿_마운트(self, sample_spec):
        pod = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.spec
        vol_names = {v.name for v in pod.volumes}
        assert "tls" in vol_names
        c = pod.containers[0]
        assert any(m.mount_path == "/opt/nifi/tls" for m in c.volume_mounts)

    def test_비TLS는_8080_포트_및_시크릿없음(self, minimal_spec):
        pod = build_statefulset(CLUSTER_NAME, NAMESPACE, minimal_spec).spec.template.spec
        assert "tls" not in {v.name for v in pod.volumes}
        ports = {p.name: p.container_port for p in pod.containers[0].ports}
        assert ports["web"] == 8080

    def test_anti_affinity(self, sample_spec):
        pod = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.spec
        assert pod.affinity.pod_anti_affinity is not None

    def test_메인컨테이너는_start_sh_대신_스크립트(self, sample_spec):
        c = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.spec.containers[0]
        assert c.command == ["sh", "/scripts/start-nifi.sh"]
        assert any(m.mount_path == "/scripts" for m in c.volume_mounts)

    def test_init은_PVC와_heap_env(self, sample_spec):
        init = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.spec.init_containers[0]
        assert any(m.name == "data" for m in init.volume_mounts)
        env = {e.name: e.value for e in init.env}
        assert env["NIFI_JVM_HEAP_MAX"] == "2g"
        assert env["STATE_CONFIGMAP_PREFIX"] == CLUSTER_NAME

    def test_설정해시_어노테이션(self, sample_spec):
        """ConfigMap 내용이 바뀌면 Pod 템플릿 해시가 바뀌고, 같으면 그대로여야 한다(재시도 멱등)."""
        meta = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.template.metadata
        h = meta.annotations[CONFIG_HASH_ANNOTATION]
        assert h == config_hash(CLUSTER_NAME, NAMESPACE, sample_spec)
        changed = {**sample_spec, "config": {**sample_spec["config"], "maxThreads": 99}}
        assert config_hash(CLUSTER_NAME, NAMESPACE, changed) != h

    def test_pvc_템플릿(self, sample_spec):
        pvc = build_statefulset(CLUSTER_NAME, NAMESPACE, sample_spec).spec.volume_claim_templates[0]
        assert pvc.spec.resources.requests["storage"] == "50Gi"
        assert pvc.spec.storage_class_name == "standard"


# ---- 인증서 ----


class TestCertificates:
    def test_서버인증서_와일드카드_SAN(self, sample_spec):
        """단일 서버 인증서가 headless 와일드카드로 모든(미래 포함) 노드 FQDN을 커버해야 한다."""
        cert = build_server_certificate(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert cert["spec"]["secretName"] == "test-nifi-tls"
        dns = cert["spec"]["dnsNames"]
        assert "*.test-headless.default.svc.cluster.local" in dns
        assert "test-ui.default.svc.cluster.local" in dns

    def test_서버인증서는_노드수와_무관(self, sample_spec):
        """스케일 시 인증서 재발급/노드 재시작이 필요 없도록 spec이 replicas와 무관해야 한다."""
        a = build_server_certificate(CLUSTER_NAME, NAMESPACE, sample_spec)
        b = build_server_certificate(CLUSTER_NAME, NAMESPACE, {**sample_spec, "replicas": 7})
        assert a == b

    def test_서버인증서_CN은_노드identity(self, sample_spec):
        from nifi_operator.resources.configmap import node_identity

        cert = build_server_certificate(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert f"CN={cert['spec']['commonName']}" == node_identity(CLUSTER_NAME, NAMESPACE)

    def test_서버인증서_pkcs12(self, sample_spec):
        """NiFi가 쓰는 PKCS12로 생성하고 비밀번호는 credentials Secret 공유."""
        cert = build_server_certificate(CLUSTER_NAME, NAMESPACE, sample_spec)
        p12 = cert["spec"]["keystores"]["pkcs12"]
        assert p12["create"] is True
        assert p12["passwordSecretRef"]["name"] == "test-credentials"
        assert p12["passwordSecretRef"]["key"] == "keystore-password"

    def test_오퍼레이터_클라이언트인증서(self, sample_spec):
        cert = build_operator_certificate(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert cert["spec"]["secretName"] == "test-operator-tls"
        assert "client auth" in cert["spec"]["usages"]
        assert cert["spec"]["commonName"] == "test-operator"


# ---- RBAC / PDB ----


class TestNodeRbac:
    def test_service_account_이름(self):
        sa = rbac.build_node_service_account(CLUSTER_NAME, NAMESPACE)
        assert sa.metadata.name == "test-nifi"

    def test_role_leases_configmaps(self):
        """리더 선출(leases)과 상태(configmaps) 권한이 있어야 한다."""
        role = rbac.build_node_role(CLUSTER_NAME, NAMESPACE)
        resources = {r for rule in role.rules for r in rule.resources}
        assert "leases" in resources
        assert "configmaps" in resources

    def test_rolebinding_참조(self):
        rb = rbac.build_node_role_binding(CLUSTER_NAME, NAMESPACE)
        assert rb.role_ref.name == "test-nifi-role"
        assert rb.subjects[0].name == "test-nifi"

    def test_pdb_과반(self, sample_spec):
        """replicas=3 → minAvailable=2 (과반)."""
        pdb = rbac.build_pod_disruption_budget(CLUSTER_NAME, NAMESPACE, sample_spec)
        assert pdb.spec.min_available == 2
