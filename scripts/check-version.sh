#!/usr/bin/env bash
# 모든 위치의 제품 버전·기반 NiFi 버전이 일치하는지 검사한다 (불일치면 종료 코드 1).
#
# 사용법:
#   scripts/check-version.sh            # 위치 간 일치만 검사
#   scripts/check-version.sh v0.5.0     # + 태그 == v<버전>, SNAPSHOT 아님
#
# SNAPSHOT 이 아닌 버전이면 CHANGELOG.md 의 `## [<버전>]` 절과 문서 속 버전 표기도 검사한다.
# 릴리스 워크플로(.github/workflows/release.yml)의 첫 단계다.
set -euo pipefail
[ $# -le 1 ] || { echo "usage: $0 [<tag>]" >&2; exit 2; }
exec python3 "$(dirname "${BASH_SOURCE[0]}")/versioning.py" check "$@"
