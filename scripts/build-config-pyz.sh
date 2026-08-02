#!/usr/bin/env bash
# 설정 TUI(argus-nifi-config)를 단일 실행 파일(zipapp)로 빌드한다.
#
# 배포본의 tools/argus-config/argus-config.pyz 가 되고, bin/argus-config.sh 가 실행한다.
# pip 설치 없이 동작하므로 tar.gz/RPM 만 받은 운영자도 쓸 수 있다.
#
# 사용법:
#   scripts/build-config-pyz.sh <출력 .pyz 경로> [배포본 버전]
#
# Python 이 없거나 3.10 미만이면 **경고만 남기고 종료 코드 0 으로 끝낸다.** 파이썬 없이도
# NAR·tar.gz 를 빌드할 수 있어야 하기 때문이다. 이 경우 .pyz 가 만들어지지 않고,
# bin/argus-config.sh 가 그 사실을 사용자에게 알린다.
set -euo pipefail

OUT="${1:?usage: build-config-pyz.sh <out.pyz> [version]}"
DIST_VERSION="${2:-unknown}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${REPO}/tools/nifi-config"

warn_skip() {
  echo "[build-config-pyz] 경고: $1" >&2
  echo "[build-config-pyz] 설정 TUI 를 배포본에 포함하지 않고 계속합니다." >&2
  exit 0
}

PY="${ARGUS_PYTHON:-$(command -v python3 || true)}"
[ -n "$PY" ] || warn_skip "python3 를 찾을 수 없습니다."

# zipapp 은 3.5+ 지만 이 도구는 3.10 문법(match/union type)을 쓴다.
"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)' \
  || warn_skip "python3 3.10 이상이 필요합니다 (현재: $("$PY" -V 2>&1))."
"$PY" -c 'import pip' 2>/dev/null || warn_skip "pip 를 사용할 수 없습니다."

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

echo "[build-config-pyz] 의존성 설치 (핀 고정)"
# --require-hashes: requirements-pyz.txt 에 없는 전이 의존성이 끼어들면 실패한다.
"$PY" -m pip install --quiet --disable-pip-version-check \
  --target "$STAGE" --no-compile --require-hashes \
  -r "${SRC}/requirements-pyz.txt" \
  || warn_skip "의존성 설치에 실패했습니다(네트워크 또는 해시 불일치)."

echo "[build-config-pyz] 소스 설치"
"$PY" -m pip install --quiet --disable-pip-version-check \
  --target "$STAGE" --no-compile --no-deps "$SRC" \
  || warn_skip "argus-nifi-config 설치에 실패했습니다."

# 배포본 버전을 심어 지원 요청 시 어느 빌드인지 알 수 있게 한다 (--version 이 함께 출력).
cat > "${STAGE}/argus_nifi_config/_build_info.py" <<EOF
"""빌드 시 생성됨. 직접 편집하지 말 것 (scripts/build-config-pyz.sh)."""

DISTRIBUTION_VERSION = "${DIST_VERSION}"
EOF

# 실행에 불필요한 메타데이터 제거 (크기 절감)
find "$STAGE" -maxdepth 1 -name '*.dist-info' -exec rm -rf {} + 2>/dev/null || true
find "$STAGE" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true

cat > "${STAGE}/__main__.py" <<'EOF'
from argus_nifi_config.cli import main

raise SystemExit(main())
EOF

mkdir -p "$(dirname "$OUT")"
"$PY" -m zipapp "$STAGE" -o "$OUT" -p '/usr/bin/env python3' -c
chmod 0755 "$OUT"

echo "[build-config-pyz] 생성 완료: $OUT ($(du -h "$OUT" | cut -f1))"

# 만들어진 산출물이 실제로 실행되는지 확인한다. 조립 이후에 발견하면 원인 추적이 어렵다.
"$PY" "$OUT" --version >/dev/null || {
  echo "[build-config-pyz] 오류: 생성된 .pyz 가 실행되지 않습니다." >&2
  exit 1
}
