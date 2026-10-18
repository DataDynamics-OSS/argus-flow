"""공통 테스트 픽스처 정의."""

import pytest


@pytest.fixture
def sample_spec():
    """NiFiCluster 리소스의 샘플 spec 딕셔너리."""
    return {
        "version": "2.0.0",
        "image": "apache/nifi",
        "replicas": 3,
        "config": {
            "jvmHeapSize": "2g",
            "maxThreads": 20,
            "sensitivePropsKey": "test-secret-key",
            "webProxyHost": "nifi.example.com",
        },
        "resources": {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "2", "memory": "4Gi"},
        },
        "storage": {
            "storageClassName": "standard",
            "size": "50Gi",
        },
        "tls": {
            "enabled": True,
            "issuerRef": {
                "name": "my-ca-issuer",
                "kind": "ClusterIssuer",
            },
        },
    }


@pytest.fixture
def minimal_spec():
    """비-TLS 단일 노드 최소 spec."""
    return {"version": "2.0.0", "replicas": 1}


@pytest.fixture
def sample_cluster_info():
    """NiFi REST API에서 반환되는 클러스터 정보 샘플 응답."""
    return {
        "cluster": {
            "nodes": [
                {
                    "nodeId": "node-1-id",
                    "address": "test-nifi-0.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "CONNECTED",
                    "roles": ["Cluster Coordinator", "Primary Node"],
                },
                {
                    "nodeId": "node-2-id",
                    "address": "test-nifi-1.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "CONNECTED",
                    "roles": [],
                },
                {
                    "nodeId": "node-3-id",
                    "address": "test-nifi-2.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "CONNECTED",
                    "roles": [],
                },
            ]
        }
    }


@pytest.fixture
def user_disconnected_cluster_info():
    """관리자가 직접 연결 해제한 노드(nifi-1)가 있는 클러스터 정보 (NiFi 2.10 실제 이벤트 문구)."""
    return {
        "cluster": {
            "nodes": [
                {
                    "nodeId": "node-1-id",
                    "address": "test-nifi-0.test-headless.default.svc.cluster.local",
                    "status": "CONNECTED",
                    "roles": ["Cluster Coordinator", "Primary Node"],
                },
                {
                    "nodeId": "node-2-id",
                    "address": "test-nifi-1.test-headless.default.svc.cluster.local",
                    "status": "DISCONNECTED",
                    "roles": [],
                    "events": [
                        {
                            "timestamp": "10/18/2026 11:53:45 UTC",
                            "category": "INFO",
                            "message": "Node disconnected due to User CN=test-operator "
                                       "requested that node be disconnected from cluster",
                        },
                        {
                            "timestamp": "10/18/2026 11:40:00 UTC",
                            "category": "INFO",
                            "message": "Received first heartbeat from connecting node. Node connected.",
                        },
                    ],
                },
            ]
        }
    }


@pytest.fixture
def disconnected_cluster_info():
    """일부 노드가 DISCONNECTED 상태인 클러스터 정보 샘플."""
    return {
        "cluster": {
            "nodes": [
                {
                    "nodeId": "node-1-id",
                    "address": "test-nifi-0.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "CONNECTED",
                    "roles": ["Cluster Coordinator", "Primary Node"],
                },
                {
                    "nodeId": "node-2-id",
                    "address": "test-nifi-1.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "DISCONNECTED",
                    "roles": [],
                },
            ]
        }
    }
