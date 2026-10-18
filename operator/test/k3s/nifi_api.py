"""k3s 통합 테스트용 NiFi REST 헬퍼 — 오퍼레이터 Pod 안에서 실행한다.

  kubectl -n nifi-optest exec -i deploy/nifi-operator -- python - <cluster> <tls:0|1> <cmd> [args] < nifi_api.py

TLS 클러스터는 오퍼레이터가 전개한 클라이언트 인증서(/tmp/nifi-operator-certs/<ns>/<name>)로
mTLS 인증한다(Initial Admin = CN=<name>-operator).

명령:
  cluster                       노드 목록 (address/status/roles/queued)
  mkflow                        ListFile(/opt/nifi/nifi-current/lib, 로컬 state) → UpdateAttribute(중지)
                                흐름 생성 후 ListFile을 1회 실행해 큐에 FlowFile을 쌓는다
  flowstat                      root PG 하위 프로세서/연결 큐/ListFile state 요약
  node <ordinal> <STATUS>       노드 상태 변경 (DISCONNECTING/CONNECTING/OFFLOADING)
  regsetup                      NiFi Registry 버킷/클라이언트 + v1/v2 flow 생성 (S11)
  pgs                           root 하위 Process Group 목록 (버전/실행 상태)
  runonce <proc-id>             프로세서 1회 실행 (RUN_ONCE)
  lb <conn-id>                  연결을 Round Robin 로드밸런싱으로 설정
"""

import json
import os
import ssl
import sys
import time

import httpx

NS = "nifi-optest"
name, tls = sys.argv[1], sys.argv[2] == "1"
cmd, args = sys.argv[3], sys.argv[4:]


def base(ordinal=0):
    host = f"{name}-nifi-{ordinal}.{name}-headless.{NS}.svc.cluster.local"
    return f"https://{host}:8443/nifi-api" if tls else f"http://{host}:8080/nifi-api"


def client(ordinal=0):
    verify = True
    if tls:
        d = f"/tmp/nifi-operator-certs/{NS}/{name}"
        verify = ssl.create_default_context(cafile=f"{d}/ca.crt")
        verify.load_cert_chain(f"{d}/tls.crt", f"{d}/tls.key")
    return httpx.Client(base_url=base(ordinal), verify=verify, timeout=30)


ORD = int(os.environ.get("ORD", "0"))
c = client(ORD)
rev = lambda v=0: {"version": v, "clientId": "k3s-test"}


def j(r):
    if r.status_code >= 400:
        print("HTTP", r.status_code, r.text[:500])
        sys.exit(1)
    return r.json() if r.text else {}


def nodes():
    return j(c.get("/controller/cluster"))["cluster"]["nodes"]


if cmd == "cluster":
    for n in nodes():
        print(n["address"], n["status"], ",".join(n.get("roles", [])), "queued=" + n.get("queued", ""),
              "| last:", (n.get("events") or [{}])[0].get("message", ""))

elif cmd == "mkflow":
    root = j(c.get("/flow/process-groups/root"))["processGroupFlow"]["id"]

    def proc(t, x, props=None, auto=None):
        body = {"revision": rev(), "component": {"type": t, "position": {"x": x, "y": 0},
                "config": {"properties": props or {}, "autoTerminatedRelationships": auto or [],
                           "schedulingPeriod": "1 hour"}}}
        return j(c.post(f"/process-groups/{root}/processors", json=body))

    lf = proc("org.apache.nifi.processors.standard.ListFile", 0,
              {"Input Directory": "/opt/nifi/nifi-current/lib", "Recurse Subdirectories": "false",
               "Input Directory Location": "Local"})
    ua = proc("org.apache.nifi.processors.attributes.UpdateAttribute", 600, {}, ["success"])
    conn = j(c.post(f"/process-groups/{root}/connections", json={"revision": rev(), "component": {
        "source": {"id": lf["id"], "groupId": root, "type": "PROCESSOR"},
        "destination": {"id": ua["id"], "groupId": root, "type": "PROCESSOR"},
        "selectedRelationships": ["success"]}}))
    # ListFile 1회 실행 (RUN_ONCE) → 큐 적재, UpdateAttribute는 중지 상태 유지
    lfe = j(c.get(f"/processors/{lf['id']}"))
    j(c.put(f"/processors/{lf['id']}/run-status", json={"revision": lfe["revision"], "state": "RUN_ONCE"}))
    time.sleep(5)
    print(json.dumps({"listfile": lf["id"], "updateattr": ua["id"], "connection": conn["id"]}))

elif cmd == "flowstat":
    flow = j(c.get("/flow/process-groups/root"))["processGroupFlow"]["flow"]
    for p in flow["processors"]:
        comp = p["component"]
        line = f"proc {comp['type'].split('.')[-1]} {p['id']} state={comp['state']}"
        if comp["type"].endswith("ListFile"):
            st = j(c.get(f"/processors/{p['id']}/state"))["componentState"]
            loc = st["localState"]
            line += f" localState.total={loc.get('totalEntryCount')}"
            line += f" clusterState.total={st['clusterState'].get('totalEntryCount')}"
        print(line)
    for cn in flow["connections"]:
        s = cn["status"]["aggregateSnapshot"]
        print(f"conn {cn['id']} queued={s['queued']} lb={cn['component'].get('loadBalanceStrategy')}")
    # 노드별 큐 (클러스터)
    try:
        for n in nodes():
            print("node", n["address"].split(".")[0], n["status"], "queued=" + n.get("queued", ""))
    except SystemExit:
        pass

elif cmd == "node":
    ordinal, status = int(args[0]), args[1]
    addr = f"{name}-nifi-{ordinal}.{name}-headless.{NS}.svc.cluster.local"
    n = next(n for n in nodes() if n["address"] == addr)
    r = c.put(f"/controller/cluster/nodes/{n['nodeId']}",
              json={"node": {"nodeId": n["nodeId"], "status": status}})
    print(r.status_code, r.text[:300])

elif cmd == "runonce":
    pid = args[0]
    pe = j(c.get(f"/processors/{pid}"))
    j(c.put(f"/processors/{pid}/run-status", json={"revision": pe["revision"], "state": "RUN_ONCE"}))
    time.sleep(5)
    print("ok")

elif cmd == "regsetup":
    # NiFi Registry 버킷 + NiFi Registry 클라이언트 + 버전 2개(v1, v2)짜리 flow를 만든다
    reg = "http://nifi-registry.nifi-optest.svc.cluster.local:18080"
    rc_ = httpx.Client(base_url=reg + "/nifi-registry-api", timeout=30)
    bucket = j(rc_.post("/buckets", json={"name": "optest"}))["identifier"]
    rcl = j(c.post("/controller/registry-clients", json={"revision": rev(), "component": {
        "name": "optest-registry",
        "type": "org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient",
        "properties": {"url": reg}}}))["id"]
    root = j(c.get("/flow/process-groups/root"))["processGroupFlow"]["id"]
    pg = j(c.post(f"/process-groups/{root}/process-groups", json={"revision": rev(), "component": {
        "name": "optest-src", "position": {"x": 0, "y": 400}}}))
    gen = j(c.post(f"/process-groups/{pg['id']}/processors", json={"revision": rev(), "component": {
        "type": "org.apache.nifi.processors.standard.GenerateFlowFile", "position": {"x": 0, "y": 0},
        "config": {"schedulingPeriod": "1 min", "autoTerminatedRelationships": ["success"],
                   "properties": {"File Size": "0B"}}}}))
    pge = j(c.get(f"/process-groups/{pg['id']}"))
    v1 = j(c.post(f"/versions/process-groups/{pg['id']}", json={
        "processGroupRevision": pge["revision"],
        "versionedFlow": {"registryId": rcl, "bucketId": bucket, "flowName": "optest-flow",
                          "comments": "v1", "action": "COMMIT"}}))
    flow_id = v1["versionControlInformation"]["flowId"]
    gene = j(c.get(f"/processors/{gen['id']}"))
    j(c.put(f"/processors/{gen['id']}", json={"revision": gene["revision"], "component": {
        "id": gen["id"], "config": {"properties": {"File Size": "1 KB"}}}}))
    pge = j(c.get(f"/process-groups/{pg['id']}"))
    v2 = j(c.post(f"/versions/process-groups/{pg['id']}", json={
        "processGroupRevision": pge["revision"],
        "versionedFlow": {"registryId": rcl, "bucketId": bucket, "flowId": flow_id,
                          "comments": "v2", "action": "COMMIT"}}))
    pge = j(c.get(f"/process-groups/{pg['id']}"))
    j(c.delete(f"/process-groups/{pg['id']}", params={"version": pge["revision"]["version"],
                                                     "clientId": "k3s-test"}))
    print(json.dumps({"registryUrl": reg, "registryClientId": rcl, "bucketId": bucket,
                      "flowId": flow_id, "versions": [v1["versionControlInformation"]["version"],
                                                      v2["versionControlInformation"]["version"]]}))

elif cmd == "pgs":
    # root 하위 Process Group과 버전 정보/프로세서 상태
    root = j(c.get("/flow/process-groups/root"))["processGroupFlow"]["id"]
    for g in j(c.get(f"/process-groups/{root}/process-groups"))["processGroups"]:
        vci = g["component"].get("versionControlInformation") or {}
        print("pg", g["id"], g["component"]["name"], "version=" + str(vci.get("version")),
              "state=" + str(vci.get("state")), "running=" + str(g.get("runningCount")),
              "stopped=" + str(g.get("stoppedCount")))

elif cmd == "lb":
    cid = args[0]
    ce = j(c.get(f"/connections/{cid}"))
    j(c.put(f"/connections/{cid}", json={"revision": ce["revision"], "component": {
        "id": cid, "loadBalanceStrategy": "ROUND_ROBIN"}}))
    print("ok")
