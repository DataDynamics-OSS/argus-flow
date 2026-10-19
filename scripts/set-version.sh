#!/usr/bin/env bash
# Argus Flow 제품 버전 일괄 변경.
#
# 사용법:
#   scripts/set-version.sh <버전>        예) 0.5.0, 0.6.0-SNAPSHOT, 0.6.0-rc.1
#
# 바꾸는 곳(자세한 규칙은 scripts/versioning.py):
#   - 모든 pom.xml 의 프로젝트·부모 버전(io.datadynamics.nifi) — 루트, nifi-extensions, distribution
#     그리고 project.build.outputTimestamp(현재 UTC)
#   - Makefile VERSION
#   - charts/argus-flow/Chart.yaml version·appVersion·NiFi 버전 annotation
#   - charts/argus-flow/values.yaml, operator/deploy/operator-deployment.yaml 의 operator 이미지
#   - tools/nifi-config/pyproject.toml, argus_nifi_config/__init__.py (PEP 440 으로 변환)
#   - distribution/rpm/nfpm.yaml version·release(1)·epoch(1)·설명의 NiFi 버전
#   - (SNAPSHOT 이 아닐 때만) 현재 버전을 안내하는 문서의 이미지 태그·파일명
#
# 기반 NiFi 버전(루트 pom nifi.version)은 바꾸지 않는다. CHANGELOG.md 의 절 제목은 직접 고친다.
# 끝나면 scripts/check-version.sh 와 같은 검사를 돌린다.
set -euo pipefail
[ $# -eq 1 ] || { echo "usage: $0 <version>" >&2; exit 2; }
exec python3 "$(dirname "${BASH_SOURCE[0]}")/versioning.py" set "$1"
