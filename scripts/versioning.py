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
"""Argus Flow 제품 버전 일괄 변경·검사 도구.

scripts/set-version.sh, scripts/check-version.sh 가 이 스크립트를 부른다.

버전 체계
  - 제품 버전은 SemVer(`0.5.0`, `0.6.0-SNAPSHOT`, `0.6.0-rc.1`). 모든 구성요소(Maven 모듈·NAR,
    Helm 차트, operator 이미지, 설정 도구, RPM)가 같은 버전을 쓴다.
  - 기반 Apache NiFi 버전은 루트 pom 의 `nifi.version` 속성이 정본이며 제품 버전과 따로 움직인다.
    이 도구는 NiFi 버전을 바꾸지 않고, 그 값이 쓰이는 곳끼리 일치하는지만 검사한다.
  - Python 패키지 버전은 PEP 440 형식으로 바꿔 쓴다(`0.6.0-SNAPSHOT` → `0.6.0.dev0`).
  - RPM 은 nfpm 이 SemVer 프리릴리스를 `~` 로 바꿔 준다(`0.6.0~SNAPSHOT`). Epoch 는 1 고정.

표준 라이브러리만 쓴다(CI 에서 setup-python 직후 바로 실행).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

GROUP_ID = "io.datadynamics.nifi"
OPERATOR_IMAGE = "datadynamics/argus-flow-operator"
NIFI_IMAGE = "datadynamics/argus-flow-nifi"
RPM_EPOCH = "1"
NIFI_VERSION_ANNOTATION = "argus-flow.datadynamics.io/nifi-version"

SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z][0-9A-Za-z.-]*))?$")
# 문서 속 버전 표기. 프리릴리스는 rc/alpha/beta 만 버전의 일부로 본다 —
# `argus-flow-nifi:0.5.0-nifi2.10.0` 같은 태그에서 `-nifi2.10.0` 을 먹지 않기 위해서다.
DOC_VER = r"\d+\.\d+\.\d+(?:-(?:rc|alpha|beta)[.0-9]*)?"

# 루트 2개: 저장소 루트(argus-flow) 와 nifi-extensions(부모가 org.apache.nifi 라 루트를 상속하지 않음)
ROOT_POMS = ["pom.xml", "nifi-extensions/pom.xml"]

# 현재 버전을 설명하는 문서. 릴리스 버전(SNAPSHOT 아님)일 때만 바꾸고 검사한다 —
# 문서가 아직 받을 수 없는 SNAPSHOT 이미지를 안내하면 안 되기 때문이다.
DOC_FILES = [
    "README.md",
    "CONTRIBUTING.md",
    "operator/README.md",
    "operator/INSTALL.md",
    "operator/examples/nifi-cluster-simple.yaml",
    "operator/examples/nifi-cluster-tls.yaml",
    "nifi-extensions/README.md",
    "tools/nifi-config/README.md",
    ".github/ISSUE_TEMPLATE/bug_report.yml",
]
DOC_PATTERNS = [
    # (설명, 정규식 — 그룹 1: 앞부분, 그룹 2: 버전)
    # 사내 레지스트리 예시(registry.example.com/.../argus-flow-operator:<버전>)도 포함
    ("operator 이미지", re.compile(r"(/argus-flow-operator:)(" + DOC_VER + r")")),
    ("NiFi 이미지", re.compile(r"(/argus-flow-nifi:)(" + DOC_VER + r")")),
    ("NiFi 이미지 기반 버전 태그", re.compile(r"((?:`|태그 ))(" + DOC_VER + r")(?=-nifi\d)")),
    ("현재 버전 안내", re.compile(r"((?:현재 |태그는 릴리스 버전과 같습니다\()`)(" + DOC_VER + r")(?=`)")),
    ("현재 버전 안내", re.compile(r"(`)(" + DOC_VER + r")(?=` 기준|`이 출력되면)")),
    ("tar.gz 파일명", re.compile(r"(argus-flow-)(" + DOC_VER + r")(?=-nifi-)")),
    ("RPM 파일명", re.compile(r"(argus-flow-nifi-)(" + DOC_VER + r")(?=-\d+\.x86_64\.rpm)")),
    ("릴리스 파일명", re.compile(
        r"((?:argus-flow-nars|argus-config|argus-user-cli|argus-flow-db-iaa-sql|argus-flow-operator-image"
        r"|argus-flow-operator|argus-flow)-)(" + DOC_VER + r")(?=\.(?:zip|pyz|jar|tar|yaml|tgz)\b|-sbom)")),
    ("릴리스 태그", re.compile(r"(argus-flow[\w.-]*/releases/(?:download|tag)/v|git tag -a v|checkout v)(" + DOC_VER + r")")),
    ("Helm 이미지 태그", re.compile(r"(operator\.image\.tag=)(" + DOC_VER + r")")),
    ("NiFiCluster 이미지 버전", re.compile(r"(version: \")(" + DOC_VER + r")(\"\s+# Argus Flow)")),
    ("이슈 템플릿 예시", re.compile(r"(placeholder: \")(" + DOC_VER + r")(\")")),
    ("pyz 빌드 예시", re.compile(r"(build-config-pyz\.sh \S+ )(\d[\w.-]*)")),
]


# ── 버전 문자열 변환 ─────────────────────────────────────────────────────────

def parse(version: str) -> re.Match:
    m = SEMVER.match(version)
    if not m:
        raise SystemExit(f"버전 형식 오류: {version!r} (SemVer MAJOR.MINOR.PATCH[-PRERELEASE])")
    return m


def is_release(version: str) -> bool:
    """SNAPSHOT 이 아닌 버전(정식·rc 포함)."""
    return not version.endswith("-SNAPSHOT")


def pep440(version: str) -> str:
    m = parse(version)
    base = ".".join(m.group(i) for i in (1, 2, 3))
    pre = m.group(4)
    if pre is None:
        return base
    if pre == "SNAPSHOT":
        return base + ".dev0"
    pm = re.fullmatch(r"(rc|alpha|beta)\.?(\d+)", pre)
    if not pm:
        raise SystemExit(f"Python 버전으로 바꿀 수 없는 프리릴리스: {pre!r} (SNAPSHOT, rc.N, alpha.N, beta.N 만 지원)")
    return base + {"rc": "rc", "alpha": "a", "beta": "b"}[pm.group(1)] + pm.group(2)


# ── 파일 유틸 ───────────────────────────────────────────────────────────────

def read(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


def write(rel: str, text: str, changed: list[str]) -> None:
    p = REPO / rel
    if p.read_text(encoding="utf-8") != text:
        p.write_text(text, encoding="utf-8")
        changed.append(rel)


def sub_one(rel: str, text: str, pattern: str, repl: str, flags: int = re.M) -> str:
    new, n = re.subn(pattern, repl, text, count=1, flags=flags)
    if n != 1:
        raise SystemExit(f"{rel}: 패턴을 찾지 못했습니다: {pattern}")
    return new


def find_one(rel: str, pattern: str, flags: int = re.M) -> str:
    m = re.search(pattern, read(rel), flags)
    if not m:
        raise SystemExit(f"{rel}: 패턴을 찾지 못했습니다: {pattern}")
    return m.group(1)


def tracked_poms() -> list[str]:
    try:
        out = subprocess.run(["git", "ls-files", "--", "pom.xml", "*/pom.xml"], cwd=REPO,
                             check=True, capture_output=True, text=True).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        out = [str(p.relative_to(REPO)) for p in REPO.rglob("pom.xml")
               if not ({"target", ".upstream", "node_modules"} & set(p.parts))]
    return sorted(p for p in out if not p.startswith(".upstream/"))


# ── POM ────────────────────────────────────────────────────────────────────

NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def pom_info(rel: str) -> dict:
    root = ET.parse(REPO / rel).getroot()
    g = lambda path: (root.findtext(path, namespaces=NS) or "").strip() or None  # noqa: E731
    return {
        "groupId": g("m:groupId") or g("m:parent/m:groupId"),
        "version": g("m:version"),
        "parentGroupId": g("m:parent/m:groupId"),
        "parentVersion": g("m:parent/m:version"),
        "nifiVersion": g("m:properties/m:nifi.version"),
        "outputTimestamp": g("m:properties/m:project.build.outputTimestamp"),
    }


def set_pom(rel: str, version: str, timestamp: str | None, changed: list[str]) -> None:
    text = read(rel)
    info = pom_info(rel)
    # <parent> 블록의 version (부모가 우리 groupId 일 때만)
    if info["parentGroupId"] == GROUP_ID:
        text = sub_one(rel, text,
                       r"(<parent>\s*<groupId>" + re.escape(GROUP_ID) + r"</groupId>\s*<artifactId>[^<]+</artifactId>\s*<version>)[^<]+(</version>)",
                       r"\g<1>" + version + r"\g<2>", flags=re.S)
    # 프로젝트 자신의 version (명시된 경우만; 들여쓰기 4칸 = 최상위 요소)
    if info["version"] is not None and info["groupId"] == GROUP_ID:
        head, sep, tail = text.partition("</parent>")
        body = tail if sep else text
        body = sub_one(rel, body, r"^(    <version>)[^<]+(</version>)", r"\g<1>" + version + r"\g<2>")
        text = head + sep + body if sep else body
    if timestamp and rel in ROOT_POMS:
        if info["outputTimestamp"] is None:
            raise SystemExit(f"{rel}: <project.build.outputTimestamp> 속성이 없습니다")
        text = sub_one(rel, text,
                       r"(<project\.build\.outputTimestamp>)[^<]+(</project\.build\.outputTimestamp>)",
                       r"\g<1>" + timestamp + r"\g<2>")
    write(rel, text, changed)


# ── set ────────────────────────────────────────────────────────────────────

def nifi_version() -> str:
    v = pom_info("pom.xml")["nifiVersion"]
    if not v:
        raise SystemExit("pom.xml 에 <nifi.version> 속성이 없습니다")
    return v


def cmd_set(version: str) -> int:
    parse(version)
    nifi = nifi_version()
    py = pep440(version)
    ts = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    changed: list[str] = []

    for rel in tracked_poms():
        set_pom(rel, version, ts, changed)

    rel = "Makefile"
    write(rel, sub_one(rel, read(rel), r"^(VERSION\s*\?=\s*).*$", r"\g<1>" + version), changed)

    rel = "charts/argus-flow/Chart.yaml"
    t = read(rel)
    t = sub_one(rel, t, r"^(version:\s*).*$", r"\g<1>" + version)
    t = sub_one(rel, t, r"^(appVersion:\s*).*$", r'\g<1>"' + version + '"')
    t = sub_one(rel, t, r"^(\s+" + re.escape(NIFI_VERSION_ANNOTATION) + r":\s*).*$", r'\g<1>"' + nifi + '"')
    write(rel, t, changed)

    rel = "charts/argus-flow/values.yaml"
    t = read(rel)
    t = sub_one(rel, t, r"^(\s+repository:\s*).*$", r"\g<1>" + OPERATOR_IMAGE)
    t = sub_one(rel, t, r"^(\s+tag:\s*).*$", r'\g<1>"' + version + '"')
    write(rel, t, changed)

    rel = "operator/deploy/operator-deployment.yaml"
    write(rel, sub_one(rel, read(rel), r"^(\s+image:\s*).*$", r"\g<1>" + OPERATOR_IMAGE + ":" + version), changed)

    rel = "tools/nifi-config/pyproject.toml"
    write(rel, sub_one(rel, read(rel), r'^(version\s*=\s*")[^"]*(")', r"\g<1>" + py + r"\g<2>"), changed)
    rel = "tools/nifi-config/argus_nifi_config/__init__.py"
    write(rel, sub_one(rel, read(rel), r'^(__version__\s*=\s*")[^"]*(")', r"\g<1>" + py + r"\g<2>"), changed)

    rel = "distribution/rpm/nfpm.yaml"
    t = read(rel)
    t = sub_one(rel, t, r"^(version:\s*).*$", r"\g<1>" + version)
    t = sub_one(rel, t, r"^(release:\s*).*$", r"\g<1>1")
    t = sub_one(rel, t, r"^(epoch:\s*).*$", r'\g<1>"' + RPM_EPOCH + '"')
    t = sub_one(rel, t, r"(Apache NiFi )\d+\.\d+\.\d+(?= )", r"\g<1>" + nifi)
    write(rel, t, changed)

    if is_release(version):
        for rel in DOC_FILES:
            if not (REPO / rel).exists():
                continue
            t = read(rel)
            for _, rx in DOC_PATTERNS:
                t = rx.sub(lambda m: m.group(1) + version + (m.group(3) if m.re.groups >= 3 else ""), t)
            write(rel, t, changed)
    else:
        print(f"[set-version] {version} 은 SNAPSHOT 이므로 문서의 버전 표기는 바꾸지 않습니다.")

    for c in changed:
        print(f"[set-version] 변경: {c}")
    print(f"[set-version] 제품 버전 {version} (Python {py}, RPM {RPM_EPOCH}:{version.replace('-', '~')}-1), "
          f"기반 NiFi {nifi}, outputTimestamp {ts}")
    print("[set-version] CHANGELOG.md 의 절 제목은 직접 고치세요 (`## [<버전>] - YYYY-MM-DD`).")
    return cmd_check(None, quiet=True, changelog_required=False)


# ── check ──────────────────────────────────────────────────────────────────

def cmd_check(tag: str | None, quiet: bool = False, changelog_required: bool = True) -> int:
    errors: list[str] = []
    found: list[tuple[str, str]] = []

    root = pom_info("pom.xml")
    version = root["version"]
    if not version:
        raise SystemExit("pom.xml 에 <version> 이 없습니다")
    parse(version)
    nifi = root["nifiVersion"]

    def expect(where: str, actual: str | None, wanted: str) -> None:
        found.append((where, actual or "(없음)"))
        if actual != wanted:
            errors.append(f"{where}: {actual!r} != {wanted!r}")

    for rel in tracked_poms():
        info = pom_info(rel)
        if info["parentGroupId"] == GROUP_ID:
            expect(f"{rel} <parent><version>", info["parentVersion"], version)
        if info["version"] is not None and info["groupId"] == GROUP_ID:
            expect(f"{rel} <version>", info["version"], version)
        if rel in ROOT_POMS:
            expect(f"{rel} <nifi.version>", info["nifiVersion"], nifi)
            if not info["outputTimestamp"]:
                errors.append(f"{rel}: <project.build.outputTimestamp> 속성이 없습니다")
    ext = pom_info("nifi-extensions/pom.xml")
    expect("nifi-extensions/pom.xml <parent> (org.apache.nifi)", ext["parentVersion"], nifi)

    expect("Makefile VERSION", find_one("Makefile", r"^VERSION\s*\?=\s*(\S+)"), version)
    expect("Makefile NIFI_VERSION", find_one("Makefile", r"^NIFI_VERSION\s*\?=\s*(\S+)"), nifi)

    rel = "charts/argus-flow/Chart.yaml"
    expect(f"{rel} version", find_one(rel, r"^version:\s*\"?([^\"\s]+)"), version)
    expect(f"{rel} appVersion", find_one(rel, r"^appVersion:\s*\"?([^\"\s]+)"), version)
    expect(f"{rel} {NIFI_VERSION_ANNOTATION}",
           find_one(rel, r"^\s+" + re.escape(NIFI_VERSION_ANNOTATION) + r":\s*\"?([^\"\s]+)"), nifi)

    rel = "charts/argus-flow/values.yaml"
    expect(f"{rel} operator.image.repository", find_one(rel, r"^\s+repository:\s*(\S+)"), OPERATOR_IMAGE)
    expect(f"{rel} operator.image.tag", find_one(rel, r"^\s+tag:\s*\"?([^\"\s]+)"), version)

    rel = "operator/deploy/operator-deployment.yaml"
    expect(f"{rel} image", find_one(rel, r"^\s+image:\s*(\S+)"), f"{OPERATOR_IMAGE}:{version}")

    py = pep440(version)
    expect("tools/nifi-config/pyproject.toml version",
           find_one("tools/nifi-config/pyproject.toml", r'^version\s*=\s*"([^"]*)"'), py)
    expect("tools/nifi-config/argus_nifi_config/__init__.py __version__",
           find_one("tools/nifi-config/argus_nifi_config/__init__.py", r'^__version__\s*=\s*"([^"]*)"'), py)

    rel = "distribution/rpm/nfpm.yaml"
    expect(f"{rel} version", find_one(rel, r"^version:\s*(\S+)"), version)
    expect(f"{rel} release", find_one(rel, r"^release:\s*\"?([^\"\s]+)"), "1")
    expect(f"{rel} epoch", find_one(rel, r"^epoch:\s*\"?([^\"\s]+)"), RPM_EPOCH)
    expect(f"{rel} description NiFi", find_one(rel, r"Apache NiFi (\d+\.\d+\.\d+) "), nifi)

    expect("docker/Dockerfile ARG NIFI_VERSION", find_one("docker/Dockerfile", r"^ARG NIFI_VERSION=(\S+)"), nifi)

    if is_release(version):
        for rel in DOC_FILES:
            if not (REPO / rel).exists():
                continue
            text = read(rel)
            for desc, rx in DOC_PATTERNS:
                for m in rx.finditer(text):
                    line = text.count("\n", 0, m.start()) + 1
                    expect(f"{rel}:{line} {desc}", m.group(2), version)
        changelog = read("CHANGELOG.md")
        has = re.search(r"^## \[" + re.escape(version) + r"\]", changelog, re.M) is not None
        found.append(("CHANGELOG.md ## [" + version + "]", "있음" if has else "없음"))
        if not has:
            if changelog_required:
                errors.append(f"CHANGELOG.md: '## [{version}]' 절이 없습니다")
            else:
                print(f"[check-version] 경고: CHANGELOG.md 에 '## [{version}]' 절이 아직 없습니다", file=sys.stderr)

    if tag is not None:
        found.append(("git 태그", tag))
        if tag != f"v{version}":
            errors.append(f"태그 {tag!r} != 'v{version}'")
        if not is_release(version):
            errors.append(f"SNAPSHOT 버전({version})은 태그로 릴리스할 수 없습니다")

    if not quiet:
        width = max(len(w) for w, _ in found)
        for where, actual in found:
            print(f"  {where:<{width}}  {actual}")
    if errors:
        print(f"\n[check-version] 불일치 {len(errors)}건 (기준: pom.xml {version}, NiFi {nifi}):", file=sys.stderr)
        for e in errors:
            print(f"  ✗ {e}", file=sys.stderr)
        return 1
    print(f"[check-version] OK — 제품 버전 {version}, 기반 NiFi {nifi}, 검사 {len(found)}곳"
          + (f", 태그 {tag}" if tag else ""))
    return 0


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("set", help="모든 위치의 제품 버전을 바꾼다")
    s.add_argument("version")
    c = sub.add_parser("check", help="모든 위치의 버전이 일치하는지 검사한다")
    c.add_argument("tag", nargs="?", help="릴리스 태그(v<버전>). 주면 태그와 버전 일치·CHANGELOG 절도 검사")
    sub.add_parser("get", help="현재 제품 버전을 출력한다")
    sub.add_parser("get-nifi", help="기반 NiFi 버전을 출력한다")
    a = ap.parse_args(argv)
    if a.cmd == "set":
        return cmd_set(a.version)
    if a.cmd == "check":
        return cmd_check(a.tag)
    if a.cmd == "get":
        print(pom_info("pom.xml")["version"])
        return 0
    if a.cmd == "get-nifi":
        print(nifi_version())
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
