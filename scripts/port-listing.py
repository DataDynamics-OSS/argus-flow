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
"""nifi-argus-listing-bundle 포크 원본 생성기.

업스트림 NiFi 소스를 복사하면서 패키지·클래스명만 바꾼다(동작 변경 없음).
설계: design/listing-processors-port.md §7

  1) 최초 이관 — 결과물이 "커밋 ①(원문 복사 + 이름 변경)"이 된다.
       scripts/fetch-upstream.sh 2.10.0
       scripts/port-listing.py
  2) 업스트림 업그레이드 — 새 태그로 "이름만 바꾼 원문"을 별도 디렉터리에 만들고,
     이전 원문(base)·현재 코드(ours)와 3-way 병합한다.
       scripts/port-listing.py --upstream .upstream/nifi-2.11.0 --version 2.11.0 --out /tmp/renamed-2.11.0
       git merge-file <ours> <base> <theirs>
"""
import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BUNDLE = "nifi-extensions/nifi-argus-listing-bundle"

STD = "nifi-extension-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard"
AWS = "nifi-extension-bundles/nifi-aws-bundle/nifi-aws-processors/src/main/java/org/apache/nifi/processors/aws/s3"
HDFS = "nifi-extension-bundles/nifi-hadoop-bundle/nifi-hdfs-processors/src/main/java/org/apache/nifi/processors/hadoop"
LISTED = "nifi-extension-bundles/nifi-extension-utils/nifi-listed-entity/src/main/java/org/apache/nifi/processor/util/list"
FTRANS = "nifi-extension-bundles/nifi-extension-utils/nifi-file-transfer/src/main/java/org/apache/nifi/processor/util/file/transfer"

ARGUS = "io.datadynamics.nifi"
P_LIST = f"{ARGUS}.processor.util.list"
P_FILE = f"{ARGUS}.processors.listing.file"
P_XFER = f"{ARGUS}.processors.listing.transfer"
P_S3 = f"{ARGUS}.processors.listing.s3"
P_HDFS = f"{ARGUS}.processors.listing.hdfs"

COMMON = f"{BUNDLE}/nifi-argus-listing-common"
PROCS = f"{BUNDLE}/nifi-argus-listing-processors"
AWSP = f"{BUNDLE}/nifi-argus-listing-aws-processors"
HDFSP = f"{BUNDLE}/nifi-argus-listing-hadoop-processors"

# (업스트림 경로, 대상 모듈, 대상 패키지, 대상 클래스명)
FILES = [
    (f"{LISTED}/AbstractListProcessor.java", COMMON, P_LIST, "AbstractTriggeredListProcessor"),
    (f"{FTRANS}/ListFileTransfer.java", PROCS, P_XFER, "AbstractTriggeredListFileTransfer"),
    (f"{STD}/ListFile.java", PROCS, P_FILE, "ListFile"),
    (f"{STD}/ListSFTP.java", PROCS, P_XFER, "ListSFTP"),
    (f"{STD}/ListFTP.java", PROCS, P_XFER, "ListFTP"),
    (f"{STD}/util/SFTPTransfer.java", PROCS, P_XFER, "SFTPTransfer"),
    (f"{STD}/util/FTPTransfer.java", PROCS, P_XFER, "FTPTransfer"),
    (f"{STD}/ssh/SshClientProvider.java", PROCS, f"{P_XFER}.ssh", "SshClientProvider"),
    (f"{STD}/ssh/StandardSshClientProvider.java", PROCS, f"{P_XFER}.ssh", "StandardSshClientProvider"),
    (f"{STD}/ssh/netty/StandardNettyIoConnector.java", PROCS, f"{P_XFER}.ssh.netty", "StandardNettyIoConnector"),
    (f"{STD}/ssh/netty/StandardNettyIoServiceFactory.java", PROCS, f"{P_XFER}.ssh.netty", "StandardNettyIoServiceFactory"),
    (f"{STD}/ssh/netty/StandardNettyIoServiceFactoryFactory.java", PROCS, f"{P_XFER}.ssh.netty", "StandardNettyIoServiceFactoryFactory"),
    (f"{STD}/ssh/netty/StandardNettyIoSession.java", PROCS, f"{P_XFER}.ssh.netty", "StandardNettyIoSession"),
    (f"{STD}/ftp/FTPClientProvider.java", PROCS, f"{P_XFER}.ftp", "FTPClientProvider"),
    (f"{STD}/ftp/StandardFTPClientProvider.java", PROCS, f"{P_XFER}.ftp", "StandardFTPClientProvider"),
    (f"{STD}/socket/ClientAuthenticationException.java", PROCS, f"{P_XFER}.socket", "ClientAuthenticationException"),
    (f"{STD}/socket/ClientConfigurationException.java", PROCS, f"{P_XFER}.socket", "ClientConfigurationException"),
    (f"{STD}/socket/ClientConnectException.java", PROCS, f"{P_XFER}.socket", "ClientConnectException"),
    (f"{STD}/socket/ProxySocketFactory.java", PROCS, f"{P_XFER}.socket", "ProxySocketFactory"),
    (f"{STD}/socket/SocketFactoryProvider.java", PROCS, f"{P_XFER}.socket", "SocketFactoryProvider"),
    (f"{STD}/socket/StandardSocketFactoryProvider.java", PROCS, f"{P_XFER}.socket", "StandardSocketFactoryProvider"),
    (f"{AWS}/ListS3.java", AWSP, P_S3, "ListS3"),
    (f"{AWS}/util/S3Util.java", AWSP, f"{P_S3}.util", "S3Util"),
    (f"{AWS}/util/Expiration.java", AWSP, f"{P_S3}.util", "Expiration"),
    (f"{HDFS}/ListHDFS.java", HDFSP, P_HDFS, "ListHDFS"),
    (f"{HDFS}/util/FileStatusIterable.java", HDFSP, f"{P_HDFS}.util", "FileStatusIterable"),
    (f"{HDFS}/util/FileStatusManager.java", HDFSP, f"{P_HDFS}.util", "FileStatusManager"),
    (f"{HDFS}/util/FilterMode.java", HDFSP, f"{P_HDFS}.util", "FilterMode"),
    (f"{HDFS}/util/writer/HadoopFileStatusWriter.java", HDFSP, f"{P_HDFS}.util.writer", "HadoopFileStatusWriter"),
    (f"{HDFS}/util/writer/FlowFileHadoopFileStatusWriter.java", HDFSP, f"{P_HDFS}.util.writer", "FlowFileHadoopFileStatusWriter"),
    (f"{HDFS}/util/writer/RecordHadoopFileStatusWriter.java", HDFSP, f"{P_HDFS}.util.writer", "RecordHadoopFileStatusWriter"),
]

# 패키지 이동: 이관하는 클래스마다 "업스트림 FQCN → 새 패키지.원래 단순명" (단순명은 CLASS_RENAMES 가 바꾼다).
# 긴 것부터 적용해야 접두어가 겹치는 패키지가 잘못 바뀌지 않는다.
PACKAGE_MOVES = sorted(
    [(rel.split("src/main/java/")[1].removesuffix(".java").replace("/", "."), f"{pkg}.{Path(rel).stem}")
     for rel, _module, pkg, _cls in FILES],
    key=lambda m: -len(m[0]),
)

# 클래스명 변경 (단어 경계 기준). 프로세서는 업스트림 단순명을 유지하고 패키지로 구분한다.
CLASS_RENAMES = [
    ("AbstractListProcessor", "AbstractTriggeredListProcessor"),
    ("ListFileTransfer", "AbstractTriggeredListFileTransfer"),
]

# @SeeAlso 의 업스트림 클래스 참조 → classNames 문자열 (클래스가 이 번들의 클래스패스에 없다).
# 목록에 없으면 원본 파일의 패키지로 간주한다.
SEE_ALSO_FQCN = {
    "GetFile": "org.apache.nifi.processors.standard.GetFile",
    "PutFile": "org.apache.nifi.processors.standard.PutFile",
    "FetchFile": "org.apache.nifi.processors.standard.FetchFile",
    "FetchSFTP": "org.apache.nifi.processors.standard.FetchSFTP",
    "GetSFTP": "org.apache.nifi.processors.standard.GetSFTP",
    "PutSFTP": "org.apache.nifi.processors.standard.PutSFTP",
    "FetchFTP": "org.apache.nifi.processors.standard.FetchFTP",
    "GetFTP": "org.apache.nifi.processors.standard.GetFTP",
    "PutFTP": "org.apache.nifi.processors.standard.PutFTP",
    "FetchS3Object": "org.apache.nifi.processors.aws.s3.FetchS3Object",
    "PutS3Object": "org.apache.nifi.processors.aws.s3.PutS3Object",
    "DeleteS3Object": "org.apache.nifi.processors.aws.s3.DeleteS3Object",
    "CopyS3Object": "org.apache.nifi.processors.aws.s3.CopyS3Object",
    "GetS3ObjectMetadata": "org.apache.nifi.processors.aws.s3.GetS3ObjectMetadata",
    "TagS3Object": "org.apache.nifi.processors.aws.s3.TagS3Object",
    "GetHDFS": "org.apache.nifi.processors.hadoop.GetHDFS",
    "FetchHDFS": "org.apache.nifi.processors.hadoop.FetchHDFS",
    "PutHDFS": "org.apache.nifi.processors.hadoop.PutHDFS",
}

def header(version: str, upstream_rel: str) -> str:
    return (
        "/*\n"
        " * Modifications Copyright 2026 Data Dynamics Inc.\n"
        " *\n"
        f" * Ported from Apache NiFi {version} (rel/nifi-{version}) for FlowFile-triggered listing\n"
        " * (nifi-argus-listing-bundle, design/listing-processors-port.md):\n"
        f" *   {upstream_rel}\n"
        " */\n"
    )


def convert_see_also(src: str, upstream_package: str) -> str:
    def repl(m):
        names = [n.strip().removesuffix(".class") for n in m.group(1).split(",") if n.strip()]
        fqcns = ", ".join(f'"{SEE_ALSO_FQCN.get(n, upstream_package + "." + n)}"' for n in names)
        return f"@SeeAlso(classNames = {{{fqcns}}})"
    return re.sub(r"@SeeAlso\(\{([^}]*)\}\)", repl, src)


# 업스트림에서 패키지를 나눠 쓰는(split package) 다른 jar 의 클래스 — 디렉터리 탐색으로 찾을 수 없다
SPLIT_PACKAGE_IMPORTS = {
    "ListS3": ["org.apache.nifi.processors.aws.s3.AbstractS3Processor"],
    "ListHDFS": ["org.apache.nifi.processors.hadoop.AbstractHadoopProcessor"],
}


def same_package_imports(src: str, src_path: Path, package: str) -> list:
    """원본과 같은 패키지에 있던 클래스 중 이관하지 않는 것은, 패키지가 바뀌면 명시 import 가 필요하다."""
    upstream_package = re.search(r"^package ([\w.]+);", src, flags=re.M).group(1)
    ported = {Path(rel).stem: (pkg, cls) for rel, _module, pkg, cls in FILES}
    body = re.sub(r"^(package|import) .*$", "", src, flags=re.M)
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)
    body = re.sub(r"//.*$", "", body, flags=re.M)
    body = re.sub(r"@SeeAlso\(\{[^}]*\}\)", "", body)
    body = re.sub(r'"(?:\\.|[^"\\])*"', '""', body)
    imports = list(SPLIT_PACKAGE_IMPORTS.get(src_path.stem, []))
    for sibling in sorted(src_path.parent.glob("*.java")):
        name = sibling.stem
        if name == src_path.stem or not re.search(r"\b" + re.escape(name) + r"\b", body):
            continue
        if name not in ported:
            imports.append(f"{upstream_package}.{name}")
        elif ported[name][0] != package:
            # 함께 이관하지만 다른 패키지로 간 클래스 (예: ListFile → listing.file, ListSFTP → listing.transfer)
            imports.append(f"{ported[name][0]}.{ported[name][1]}")
    return imports


def transform(src: str, package: str, cls: str, version: str, upstream_rel: str, src_path: Path) -> str:
    upstream_package = re.search(r"^package ([\w.]+);", src, flags=re.M).group(1)
    extra = same_package_imports(src, src_path, package)
    for old, new in PACKAGE_MOVES:
        src = re.sub(re.escape(old) + r"\b", new, src)
    for old, new in CLASS_RENAMES:
        src = re.sub(r"\b" + old + r"\b", new, src)
    src = re.sub(r"^package [\w.]+;", f"package {package};", src, count=1, flags=re.M)
    src = convert_see_also(src, upstream_package)
    if extra:
        first_import = re.search(r"^import ", src, flags=re.M)
        lines = "".join(f"import {e};\n" for e in extra)
        src = src[:first_import.start()] + lines + src[first_import.start():]
    # 같은 패키지가 된 클래스의 불필요 import 제거
    src = re.sub(rf"^import {re.escape(package)}\.[A-Z]\w*;\n", "", src, flags=re.M)
    return header(version, upstream_rel) + src


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--upstream", default=str(REPO / ".upstream/nifi-2.10.0"))
    ap.add_argument("--version", default="2.10.0")
    ap.add_argument("--out", default=str(REPO), help="출력 루트 (기본: 저장소 루트)")
    ap.add_argument("--force", action="store_true", help="이미 있는 파일도 덮어쓴다")
    args = ap.parse_args()

    upstream = Path(args.upstream)
    out = Path(args.out)
    for rel, module, package, cls in FILES:
        src_path = upstream / rel
        if not src_path.is_file():
            print(f"ERROR: missing upstream file {src_path}", file=sys.stderr)
            return 1
        dst = out / module / "src/main/java" / package.replace(".", "/") / f"{cls}.java"
        if dst.exists() and not args.force:
            print(f"skip (exists): {dst.relative_to(out)}")
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(transform(src_path.read_text(), package, cls, args.version, rel, src_path))
        print(f"wrote: {dst.relative_to(out)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
