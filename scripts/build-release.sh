#!/usr/bin/env bash
# Argus Flow 릴리스 산출물 빌드 — GitHub Release 첨부 파일 전부를 한 디렉터리에 만든다.
#
# 사용법:
#   scripts/build-release.sh [단계...]
#
#   단계 (생략하면 all = maven package images sbom checksums notes 순서):
#     maven      ./mvnw -Pdist package (단위·통합 테스트 포함) + CycloneDX Maven 집계 SBOM
#     package    tar.gz·NAR zip·DDL zip·도구 복사, RPM(nfpm), Helm 차트, operator 매니페스트
#     images     operator·NiFi 이미지 빌드(로컬), operator 이미지 docker save
#     sbom       Python(operator, nifi-config) SBOM 생성 후 Maven SBOM 과 병합
#     checksums  SHA256SUMS 생성, 파일당 2 GiB 미만 검사
#     notes      릴리스 노트 본문(distribution/target/release-notes.md)
#
# 결과: distribution/target/release/ (첨부 파일만), distribution/target/release-images.txt
#       (push 대상 이미지 참조 목록 — push 는 이 스크립트가 하지 않는다)
#
# 환경변수:
#   MVN            Maven 실행 파일 (기본 ./mvnw)
#   MAVEN_ARGS     Maven 추가 인자 (예: -DskipTests — 릴리스에서는 쓰지 말 것)
#   NFPM, HELM     nfpm·helm 실행 파일 (기본 PATH). 버전은 아래 *_EXPECTED 와 달라도 경고만 한다
#   PYTHON         Python 3.10+ (기본 python3)
#   SKIP_NIFI_IMAGE=1   NiFi 이미지(~3.4GB) 빌드를 건너뜀 (로컬 확인용)
#   ARGUS_SOURCE_URL    OCI 라벨 org.opencontainers.image.source (기본 공개 저장소 https://github.com/DataDynamics-OSS/argus-flow)
#
# 재현성: SOURCE_DATE_EPOCH(기본 HEAD 커밋 시각)를 Maven outputTimestamp·nfpm·SBOM 타임스탬프에 쓴다.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# 고정 도구 버전 (release.yml 과 같게 유지)
NFPM_EXPECTED="2.48.0"
HELM_EXPECTED="4.3.1"
CYCLONEDX_PY_VERSION="7.5.0"

MVN="${MVN:-./mvnw}"
NFPM="${NFPM:-nfpm}"
HELM="${HELM:-helm}"
PYTHON="${PYTHON:-python3}"
MAVEN_ARGS="${MAVEN_ARGS:-}"

log()  { printf '\033[1;34m[build-release]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[build-release] 경고:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[build-release] 오류:\033[0m %s\n' "$*" >&2; exit 1; }

VERSION="$("$PYTHON" scripts/versioning.py get)"
NIFI_VERSION="$("$PYTHON" scripts/versioning.py get-nifi)"
REVISION="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct 2>/dev/null || date +%s)}"
CREATED="$(date -u -d "@${SOURCE_DATE_EPOCH}" +%Y-%m-%dT%H:%M:%SZ)"
# 이미지는 Docker Hub 에 공개되므로 source 라벨은 공개 저장소를 가리킨다(비공개 저장소 주소를 노출하지 않음).
ARGUS_SOURCE_URL="${ARGUS_SOURCE_URL:-https://github.com/DataDynamics-OSS/argus-flow}"

NIFI_IMAGE="datadynamics/argus-flow-nifi"
OPERATOR_IMAGE="datadynamics/argus-flow-operator"
DIST_NAME="argus-flow-${VERSION}-nifi-${NIFI_VERSION}"
TARGET="${REPO}/distribution/target"
OUT="${TARGET}/release"
WORK="${TARGET}/release-work"
IMAGES_FILE="${TARGET}/release-images.txt"

# 프리릴리스(0.6.0-rc.1, SNAPSHOT)에는 MINOR 별칭 태그(0.6)를 붙이지 않는다
MINOR_ALIAS=""
if [[ "$VERSION" =~ ^([0-9]+)\.([0-9]+)\.[0-9]+$ ]]; then
  MINOR_ALIAS="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
fi

tool_version_check() {  # <이름> <명령> <기대 버전>
  local got
  got="$("$2" version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
  [ -n "$got" ] || got="$("$2" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
  if [ "$got" != "$3" ]; then
    warn "$1 버전 ${got:-?} (릴리스 고정 버전 $3 과 다름)"
  fi
}

# ── maven ──────────────────────────────────────────────────────────────────
step_maven() {
  log "Maven 빌드 (-Pdist package, 테스트 포함) + 집계 SBOM — 버전 ${VERSION}, NiFi ${NIFI_VERSION}, outputTimestamp ${CREATED}"
  # shellcheck disable=SC2086
  "$MVN" -B -ntp -Pdist -Dproject.build.outputTimestamp="${CREATED}" $MAVEN_ARGS \
    package org.cyclonedx:cyclonedx-maven-plugin:makeAggregateBom
}

# ── package ────────────────────────────────────────────────────────────────
step_package() {
  rm -rf "$OUT" && mkdir -p "$OUT" "$WORK"

  local tgz="${TARGET}/${DIST_NAME}-bin.tar.gz"
  [ -f "$tgz" ] || die "$tgz 가 없습니다. 먼저 maven 단계를 실행하세요"
  log "tar.gz·NAR zip·DDL zip·도구 복사"
  cp "$tgz" "$OUT/"
  cp "${TARGET}/argus-flow-nars-${VERSION}.zip" "$OUT/"
  cp "${TARGET}/argus-flow-db-iaa-sql-${VERSION}.zip" "$OUT/"
  [ -f "${TARGET}/tools/argus-config/argus-config.pyz" ] \
    || die "argus-config.pyz 가 없습니다(빌드 시 python3 3.10+ 와 pip 가 필요). 릴리스에는 반드시 포함돼야 합니다"
  cp "${TARGET}/tools/argus-config/argus-config.pyz" "$OUT/argus-config-${VERSION}.pyz"
  cp "${TARGET}/tools/argus-user/argus-user-cli.jar" "$OUT/argus-user-cli-${VERSION}.jar"

  log "RPM (nfpm)"
  tool_version_check nfpm "$NFPM" "$NFPM_EXPECTED"
  rm -rf "${TARGET}/rpmroot" && mkdir -p "${TARGET}/rpmroot"
  tar xzf "$tgz" -C "${TARGET}/rpmroot"
  mv "${TARGET}/rpmroot/${DIST_NAME}" "${TARGET}/rpmroot/dist"
  # config|noreplace 파일은 디렉터리 entry 와 겹치지 않게 따로 둔다 (nfpm.yaml 주석 참고)
  mv "${TARGET}/rpmroot/dist/conf" "${TARGET}/rpmroot/config"
  (cd distribution/rpm && "$NFPM" pkg --packager rpm --target "$OUT/")
  rm -rf "${TARGET}/rpmroot"
  local rpm="$OUT/argus-flow-nifi-${VERSION//-/\~}-1.x86_64.rpm"
  [ -f "$rpm" ] || die "예상한 RPM 파일이 없습니다: $rpm ($(ls "$OUT"/*.rpm 2>/dev/null))"

  log "Helm 차트"
  tool_version_check helm "$HELM" "$HELM_EXPECTED"
  "$HELM" lint charts/argus-flow
  "$HELM" package charts/argus-flow -d "$OUT/" >/dev/null
  [ -f "$OUT/argus-flow-${VERSION}.tgz" ] || die "차트 패키지가 없습니다"

  log "operator 매니페스트 (helm template + CRD + Namespace)"
  local manifest="$OUT/argus-flow-operator-${VERSION}.yaml"
  {
    cat <<EOF
# Argus Flow NiFi operator ${VERSION} — CRD + Namespace + RBAC + Deployment
# 설치: kubectl apply -f argus-flow-operator-${VERSION}.yaml
# 이미지: docker.io/${OPERATOR_IMAGE}:${VERSION} (기반 NiFi ${NIFI_VERSION})
# Helm 차트 charts/argus-flow 를 \`helm template --include-crds\` 로 렌더링한 결과다. 직접 고치지 말 것.
---
apiVersion: v1
kind: Namespace
metadata:
  name: nifi-operator-system
  labels:
    app.kubernetes.io/name: argus-flow
    app.kubernetes.io/version: "${VERSION}"
EOF
    "$HELM" template argus-flow charts/argus-flow --namespace nifi-operator-system --include-crds
  } > "$manifest"
  grep -q "image: \"${OPERATOR_IMAGE}:${VERSION}\"" "$manifest" \
    || die "매니페스트의 operator 이미지가 ${OPERATOR_IMAGE}:${VERSION} 이 아닙니다"
}

# ── images ─────────────────────────────────────────────────────────────────
step_images() {
  mkdir -p "$OUT"
  local args=(--build-arg "VERSION=${VERSION}" --build-arg "REVISION=${REVISION}"
              --build-arg "SOURCE=${ARGUS_SOURCE_URL}" --build-arg "CREATED=${CREATED}"
              --build-arg "NIFI_VERSION=${NIFI_VERSION}")
  : > "$IMAGES_FILE"

  log "operator 이미지 ${OPERATOR_IMAGE}:${VERSION}"
  local op_tags=(-t "${OPERATOR_IMAGE}:${VERSION}")
  [ -n "$MINOR_ALIAS" ] && op_tags+=(-t "${OPERATOR_IMAGE}:${MINOR_ALIAS}")
  docker build --pull "${args[@]}" "${op_tags[@]}" operator
  echo "${OPERATOR_IMAGE}:${VERSION}" >> "$IMAGES_FILE"
  [ -n "$MINOR_ALIAS" ] && echo "${OPERATOR_IMAGE}:${MINOR_ALIAS}" >> "$IMAGES_FILE"
  docker save -o "$OUT/argus-flow-operator-image-${VERSION}.tar" "${OPERATOR_IMAGE}:${VERSION}"

  if [ "${SKIP_NIFI_IMAGE:-0}" = "1" ]; then
    warn "SKIP_NIFI_IMAGE=1 — NiFi 이미지를 만들지 않습니다"
    return
  fi
  log "NiFi 이미지 ${NIFI_IMAGE}:${VERSION} (apache/nifi:${NIFI_VERSION} + NAR)"
  [ -d "${TARGET}/nars" ] || die "${TARGET}/nars 가 없습니다. 먼저 maven 단계를 실행하세요"
  rm -rf docker/nars && mkdir -p docker/nars
  cp "${TARGET}"/nars/*.nar docker/nars/
  local nifi_tags=(-t "${NIFI_IMAGE}:${VERSION}" -t "${NIFI_IMAGE}:${VERSION}-nifi${NIFI_VERSION}")
  [ -n "$MINOR_ALIAS" ] && nifi_tags+=(-t "${NIFI_IMAGE}:${MINOR_ALIAS}")
  docker build --pull "${args[@]}" "${nifi_tags[@]}" docker
  rm -rf docker/nars
  printf '%s\n' "${NIFI_IMAGE}:${VERSION}" "${NIFI_IMAGE}:${VERSION}-nifi${NIFI_VERSION}" >> "$IMAGES_FILE"
  [ -n "$MINOR_ALIAS" ] && echo "${NIFI_IMAGE}:${MINOR_ALIAS}" >> "$IMAGES_FILE"
  log "이미지 목록: $(tr '\n' ' ' < "$IMAGES_FILE")"
}

# ── sbom ───────────────────────────────────────────────────────────────────
step_sbom() {
  mkdir -p "$OUT" "$WORK"
  local maven_bom="${REPO}/target/argus-flow-maven-sbom.json"
  [ -f "$maven_bom" ] || die "$maven_bom 가 없습니다. maven 단계(makeAggregateBom)를 먼저 실행하세요"
  local py_ver
  py_ver="$("$PYTHON" -c 'import sys; print(sys.version.split()[0])')"
  log "Python SBOM (cyclonedx-bom ${CYCLONEDX_PY_VERSION}, Python ${py_ver})"

  rm -rf "$WORK/sbom" && mkdir -p "$WORK/sbom"
  local tool="$WORK/sbom/tool" op="$WORK/sbom/operator" cfg="$WORK/sbom/nifi-config"
  "$PYTHON" -m venv "$tool" && "$tool/bin/pip" install -q --disable-pip-version-check "cyclonedx-bom==${CYCLONEDX_PY_VERSION}"

  # operator: 이미지에 들어가는 런타임 의존성(requirements.txt)을 실제로 설치해 해석된 버전을 기록한다.
  # cyclonedx-py 가 루트 컴포넌트·직접 의존성을 알 수 있도록 최소 pyproject 를 만든다.
  "$PYTHON" -m venv "$op" && "$op/bin/pip" install -q --disable-pip-version-check -r operator/requirements.txt
  {
    echo '[project]'
    echo 'name = "argus-flow-operator"'
    echo "version = \"$("$PYTHON" -c 'import sys; sys.path.insert(0, "scripts"); import versioning; print(versioning.pep440(sys.argv[1]))' "$VERSION")\""
    echo 'description = "Argus Flow NiFi operator (kopf)"'
    echo 'license = { text = "Apache-2.0" }'
    echo 'dependencies = ['
    grep -vE '^\s*(#|$|-)' operator/requirements.txt | sed -E 's/^\s*/  "/; s/\s*$/",/'
    echo ']'
  } > "$WORK/sbom/operator-pyproject.toml"
  "$tool/bin/cyclonedx-py" environment --pyproject "$WORK/sbom/operator-pyproject.toml" \
    --of JSON --sv 1.6 -o "$WORK/sbom/operator.cdx.json" "$op/bin/python"

  # nifi-config: 배포본 .pyz 와 같은 핀(requirements-pyz.txt, 해시 고정)
  "$PYTHON" -m venv "$cfg"
  "$cfg/bin/pip" install -q --disable-pip-version-check --require-hashes -r tools/nifi-config/requirements-pyz.txt
  "$cfg/bin/pip" install -q --disable-pip-version-check --no-deps ./tools/nifi-config
  "$tool/bin/cyclonedx-py" environment --pyproject tools/nifi-config/pyproject.toml \
    --of JSON --sv 1.6 -o "$WORK/sbom/nifi-config.cdx.json" "$cfg/bin/python"

  "$tool/bin/python" scripts/merge-sbom.py --validate \
    --name argus-flow --version "$VERSION" --nifi-version "$NIFI_VERSION" \
    -o "$OUT/argus-flow-${VERSION}-sbom.cdx.json" \
    "$maven_bom" "$WORK/sbom/operator.cdx.json" "$WORK/sbom/nifi-config.cdx.json"
  rm -rf "$WORK/sbom"
}

# ── checksums ──────────────────────────────────────────────────────────────
step_checksums() {
  [ -d "$OUT" ] || die "$OUT 가 없습니다"
  mkdir -p "$WORK"
  log "SHA256SUMS"
  local limit=$((2 * 1024 * 1024 * 1024)) bad=0 f size
  for f in "$OUT"/*; do
    [ "$(basename "$f")" = SHA256SUMS ] && continue
    size="$(stat -c %s "$f")"
    if [ "$size" -ge "$limit" ]; then
      echo "  ✗ $(basename "$f"): ${size} bytes — GitHub Release 첨부 한도(2 GiB) 초과" >&2
      bad=1
    fi
  done
  [ "$bad" = 0 ] || die "2 GiB 이상인 파일이 있습니다"
  rm -f "$OUT/SHA256SUMS"
  (cd "$OUT" && find . -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort | xargs sha256sum > "$WORK/SHA256SUMS")
  mv "$WORK/SHA256SUMS" "$OUT/SHA256SUMS"
  (cd "$OUT" && sha256sum -c --quiet SHA256SUMS)
  log "결과 (${OUT}):"
  (cd "$OUT" && find . -maxdepth 1 -type f -printf '%s\t%f\n' | LC_ALL=C sort -k2 \
     | awk -F'\t' '{printf "  %14d bytes  %8.1f MiB  %s\n", $1, $1/1048576, $2}')
}

# ── notes ──────────────────────────────────────────────────────────────────
step_notes() {
  scripts/release-notes.sh "$VERSION" > "${TARGET}/release-notes.md"
  log "릴리스 노트: ${TARGET}/release-notes.md ($(wc -l < "${TARGET}/release-notes.md") 줄)"
}

steps=("$@")
[ ${#steps[@]} -gt 0 ] || steps=(all)
log "Argus Flow ${VERSION} (NiFi ${NIFI_VERSION}), revision ${REVISION:0:12}, source ${ARGUS_SOURCE_URL}"
for s in "${steps[@]}"; do
  case "$s" in
    all)       step_maven; step_package; step_images; step_sbom; step_checksums; step_notes ;;
    maven)     step_maven ;;
    package)   step_package ;;
    images)    step_images ;;
    sbom)      step_sbom ;;
    checksums) step_checksums ;;
    notes)     step_notes ;;
    *) die "알 수 없는 단계: $s (all|maven|package|images|sbom|checksums|notes)" ;;
  esac
done
