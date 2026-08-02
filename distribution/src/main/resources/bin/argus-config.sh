#!/usr/bin/env bash
# Argus Flow — NiFi conf/ 설정 도구.
#
# nifi.properties·bootstrap.conf·authorizers.xml 등을 대화형(TUI)으로 편집한다.
# 인자 없이 실행하면 메뉴가 뜨고, --set 등으로 비대화형 사용도 된다.
#
#   ./bin/argus-config.sh                          대화형
#   ./bin/argus-config.sh --set nifi.web.https.port=9443
#   ./bin/argus-config.sh --list
#   ./bin/argus-config.sh --help
#
# NIFI_HOME 은 이 스크립트 위치에서 자동으로 정한다. 다른 설치본을 대상으로 하려면
# --nifi-home 또는 --conf-dir 을 인자로 주면 된다.
#
# 환경변수(선택):
#   ARGUS_PYTHON   사용할 python3 경로. 시스템 python3 가 3.10 미만인 경우에 지정한다.
set -euo pipefail

NIFI_HOME="$(cd "$(dirname "$0")/.." && pwd)"
PYZ="${NIFI_HOME}/tools/argus-config/argus-config.pyz"

if [ ! -f "${PYZ}" ]; then
  cat >&2 <<EOF
설정 도구가 이 배포본에 포함되어 있지 않습니다: ${PYZ}

빌드 환경에 python3(3.10 이상)가 없으면 설정 도구가 빠진 채로 패키징됩니다.
conf/ 파일을 직접 편집하거나, 소스 저장소의 tools/nifi-config 를 사용하십시오.
EOF
  exit 1
fi

PY="${ARGUS_PYTHON:-$(command -v python3 || true)}"
if [ -z "${PY}" ]; then
  cat >&2 <<EOF
python3 를 찾을 수 없습니다. 설정 도구를 쓰려면 python3 3.10 이상이 필요합니다.
NiFi 자체 동작에는 영향이 없습니다.

다른 경로의 python 을 쓰려면:  ARGUS_PYTHON=/path/to/python3 \$0
EOF
  exit 1
fi

if ! "${PY}" -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)'; then
  echo "python3 3.10 이상이 필요합니다 (현재: $("${PY}" -V 2>&1))." >&2
  echo "ARGUS_PYTHON 으로 다른 python 경로를 지정할 수 있습니다." >&2
  exit 1
fi

# 사용자가 대상을 직접 지정했으면 건드리지 않는다.
#
# 무조건 --nifi-home 을 붙이면 안 된다 — 도구는 nifi_home 을 conf_dir 보다 먼저 보므로,
# 사용자가 --conf-dir 로 다른 디렉터리를 지정해도 이 스크립트가 넣은 --nifi-home 이 이겨
# 조용히 무시된다.
TARGET_GIVEN=0
for arg in "$@"; do
  case "${arg}" in
    --nifi-home|--nifi-home=*|--conf-dir|--conf-dir=*) TARGET_GIVEN=1; break ;;
  esac
done

if [ "${TARGET_GIVEN}" -eq 1 ]; then
  exec "${PY}" "${PYZ}" "$@"
fi
exec "${PY}" "${PYZ}" --nifi-home "${NIFI_HOME}" "$@"
