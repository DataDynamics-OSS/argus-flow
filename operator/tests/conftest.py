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
                    "nodeIdentifier": {"id": "node-1-id"},
                    "address": "test-nifi-0.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "CONNECTED",
                    "roles": ["Cluster Coordinator", "Primary Node"],
                },
                {
                    "nodeIdentifier": {"id": "node-2-id"},
                    "address": "test-nifi-1.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "CONNECTED",
                    "roles": [],
                },
                {
                    "nodeIdentifier": {"id": "node-3-id"},
                    "address": "test-nifi-2.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "CONNECTED",
                    "roles": [],
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
                    "nodeIdentifier": {"id": "node-1-id"},
                    "address": "test-nifi-0.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "CONNECTED",
                    "roles": ["Cluster Coordinator", "Primary Node"],
                },
                {
                    "nodeIdentifier": {"id": "node-2-id"},
                    "address": "test-nifi-1.test-headless.default.svc.cluster.local",
                    "apiPort": 8443,
                    "status": "DISCONNECTED",
                    "roles": [],
                },
            ]
        }
    }
