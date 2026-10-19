#!/usr/bin/env python3
# Copyright 2026 Data Dynamics Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""CycloneDX JSON SBOM 여러 개를 제품 단위 SBOM 하나로 합친다.

사용법:
  merge-sbom.py --name argus-flow --version 0.5.0 --nifi-version 2.10.0 -o out.cdx.json \\
      maven-sbom.json operator-sbom.json nifi-config-sbom.json

- 각 입력의 metadata.component(그 SBOM 의 루트: Maven 루트 프로젝트, operator, 설정 도구)를 component 로
  옮기고, 새 루트(제품 argus-flow)가 그것들에 의존하게 한다.
- component 는 bom-ref 기준으로 중복을 없애고, 같은 bom-ref 의 dependsOn 은 합집합으로 합친다.
- 타임스탬프는 SOURCE_DATE_EPOCH 를 쓴다(재현성). serialNumber 는 입력과 버전으로 정해지는 UUIDv5.

cyclonedx-cli 의 merge 와 같은 일을 하지만 .NET 바이너리를 따로 받지 않으려고 직접 구현했다.
표준 라이브러리만 쓴다. cyclonedx-python-lib 이 있으면 결과를 스키마로 검증한다(--validate).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import uuid


def load(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        bom = json.load(f)
    if bom.get("bomFormat") != "CycloneDX":
        raise SystemExit(f"{path}: CycloneDX JSON 이 아닙니다")
    return bom


def walk_components(components: list[dict]):
    for c in components or []:
        yield c
        yield from walk_components(c.get("components", []))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", required=True)
    ap.add_argument("--version", required=True)
    ap.add_argument("--nifi-version", required=True)
    ap.add_argument("--spec-version", default="1.6")
    ap.add_argument("--validate", action="store_true", help="cyclonedx-python-lib 로 스키마 검증")
    ap.add_argument("-o", "--output", required=True)
    ap.add_argument("inputs", nargs="+")
    a = ap.parse_args()

    root_ref = f"pkg:generic/datadynamics/{a.name}@{a.version}"
    components: dict[str, dict] = {}
    depends: dict[str, list[str]] = {}
    roots: list[str] = []
    tools: list[dict] = []
    digest = hashlib.sha256()

    def add_dep(ref: str, on: list[str]) -> None:
        cur = depends.setdefault(ref, [])
        for d in on:
            if d not in cur:
                cur.append(d)

    for path in a.inputs:
        bom = load(path)
        with open(path, "rb") as f:
            digest.update(f.read())
        meta = bom.get("metadata", {})
        t = meta.get("tools")
        if isinstance(t, dict):
            tools.extend(t.get("components", []))
        elif isinstance(t, list):
            tools.extend({"type": "application", "name": x.get("name"), "version": x.get("version"),
                          **({"group": x["vendor"]} if x.get("vendor") else {})} for x in t)
        root = meta.get("component")
        remap: dict[str, str] = {}
        if root:
            root = dict(root)
            root.pop("components", None)
            ref = root.get("bom-ref") or ""
            # cyclonedx-py 는 루트 bom-ref 를 'root-component' 로 고정해 입력끼리 충돌한다. 이름@버전으로 바꾼다.
            if not ref or ref == "root-component" or ref in components:
                new_ref = root.get("purl") or f"{root.get('group', '') + '/' if root.get('group') else ''}{root.get('name')}@{root.get('version')}"
                if ref:
                    remap[ref] = new_ref
                ref = new_ref
            root["bom-ref"] = ref
            components.setdefault(ref, root)
            roots.append(ref)
        for c in walk_components(bom.get("components", [])):
            c = dict(c)
            c.pop("components", None)  # 평탄화 (계층은 dependencies 로 표현)
            ref = c.get("bom-ref") or c.get("purl") or f"{c.get('group', '')}/{c.get('name')}@{c.get('version')}"
            c["bom-ref"] = ref
            components.setdefault(ref, c)
        for d in bom.get("dependencies", []):
            add_dep(remap.get(d["ref"], d["ref"]), [remap.get(x, x) for x in d.get("dependsOn", [])])

    # 도구 중복 제거
    seen = set()
    uniq_tools = []
    for t in tools:
        key = (t.get("group"), t.get("name"), t.get("version"))
        if key not in seen:
            seen.add(key)
            uniq_tools.append(t)
    uniq_tools.append({"type": "application", "name": "merge-sbom.py", "group": "io.datadynamics",
                       "version": a.version})

    add_dep(root_ref, roots)
    # 의존성 그래프가 component 에 없는 ref 를 가리키면 스키마상 유효해도 도구가 경고한다. 걸러낸다.
    known = set(components) | {root_ref}
    deps = [{"ref": r, "dependsOn": [d for d in on if d in known]} for r, on in depends.items() if r in known]

    sde = os.environ.get("SOURCE_DATE_EPOCH")
    ts = dt.datetime.fromtimestamp(int(sde), dt.timezone.utc) if sde else dt.datetime.now(dt.timezone.utc)
    serial = uuid.uuid5(uuid.NAMESPACE_URL, f"urn:argus-flow-sbom:{a.version}:{digest.hexdigest()}")

    out = {
        "$schema": f"http://cyclonedx.org/schema/bom-{a.spec_version}.schema.json",
        "bomFormat": "CycloneDX",
        "specVersion": a.spec_version,
        "serialNumber": f"urn:uuid:{serial}",
        "version": 1,
        "metadata": {
            "timestamp": ts.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tools": {"components": uniq_tools},
            "component": {
                "type": "application",
                "bom-ref": root_ref,
                "group": "io.datadynamics",
                "name": a.name,
                "version": a.version,
                "purl": root_ref,
                "description": f"Argus Flow {a.version} (Apache NiFi {a.nifi_version} based distribution)",
                "licenses": [{"license": {"id": "Apache-2.0"}}],
                "properties": [{"name": "io.datadynamics.nifi.version", "value": a.nifi_version}],
            },
            "supplier": {"name": "Data Dynamics Inc.", "url": ["https://www.data-dynamics.io/"]},
        },
        "components": sorted(components.values(), key=lambda c: c["bom-ref"]),
        "dependencies": sorted(deps, key=lambda d: d["ref"]),
    }
    with open(a.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"[merge-sbom] {a.output}: components {len(components)}, roots {roots}")

    if a.validate:
        try:
            from cyclonedx.schema import SchemaVersion
            from cyclonedx.validation.json import JsonStrictValidator
        except ImportError:
            print("[merge-sbom] 경고: cyclonedx-python-lib 이 없어 스키마 검증을 건너뜁니다", file=sys.stderr)
            return 0
        sv = SchemaVersion.from_version(a.spec_version)
        with open(a.output, encoding="utf-8") as f:
            err = JsonStrictValidator(sv).validate_str(f.read())
        if err:
            print(f"[merge-sbom] 스키마 검증 실패: {err}", file=sys.stderr)
            return 1
        print(f"[merge-sbom] CycloneDX {a.spec_version} 스키마 검증 OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
