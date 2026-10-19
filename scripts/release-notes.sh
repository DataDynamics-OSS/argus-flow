#!/usr/bin/env bash
# GitHub Release 본문 생성 — "설치 방법별 받을 파일" 표 + 체크섬 확인 방법 + CHANGELOG 의 해당 절.
#
# 사용법:
#   scripts/release-notes.sh <버전> > release-notes.md
#
# 환경변수 (선택, GitHub Actions 에서는 자동으로 있음):
#   GITHUB_SERVER_URL, GITHUB_REPOSITORY
#     CHANGELOG 의 상대 링크를 태그 기준 절대 링크로 바꾸고, `gh release download -R` 예시에 쓴다.
#     없으면 git remote origin 에서 추정한다.
set -euo pipefail

VERSION="${1:?usage: release-notes.sh <version>}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
NIFI_VERSION="$(python3 scripts/versioning.py get-nifi)"
TAG="v${VERSION}"

SERVER="${GITHUB_SERVER_URL:-https://github.com}"
REPOSITORY="${GITHUB_REPOSITORY:-}"
if [ -z "$REPOSITORY" ]; then
  REPOSITORY="$(git remote get-url origin 2>/dev/null | sed -E 's#^(git@[^:]+:|https://[^/]+/)##; s#\.git$##' || true)"
fi

# CHANGELOG 의 `## [<버전>]` 절 본문 (다음 `## [` 직전까지, 제목 줄 제외)
SECTION="$(awk -v v="$VERSION" '
  index($0, "## [" v "]") == 1 { found = 1; next }
  found && /^## \[/ { exit }
  found && /^\[[^]]+\]: / { next }
  found { print }
' CHANGELOG.md)"
[ -n "$(tr -d '[:space:]' <<<"$SECTION")" ] || { echo "CHANGELOG.md 에 '## [${VERSION}]' 절이 없거나 비어 있습니다" >&2; exit 1; }

# 상대 링크 → 태그 기준 절대 링크 (Release 본문에서는 상대 링크가 깨진다)
if [ -n "$REPOSITORY" ]; then
  BASE="${SERVER}/${REPOSITORY}/blob/${TAG}/"
  SECTION="$(sed -E "s#\]\(([^):#][^):]*)\)#](${BASE}\1)#g" <<<"$SECTION")"
fi
DATE_LINE="$(grep -m1 -E "^## \[${VERSION//./\\.}\]" CHANGELOG.md | sed -E 's/^## \[[^]]+\] *-? *//')"
GH_R=""; [ -n "$REPOSITORY" ] && GH_R=" -R ${REPOSITORY}"
# 정식 버전에만 MINOR 별칭 태그(0.5)가 붙는다 (scripts/build-release.sh 와 같은 규칙)
ALIAS=""
if [[ "$VERSION" =~ ^([0-9]+\.[0-9]+)\.[0-9]+$ ]]; then ALIAS=", \`${BASH_REMATCH[1]}\`"; fi

cat <<EOF
Argus Flow **${VERSION}** — 기반 Apache NiFi **${NIFI_VERSION}**${DATE_LINE:+ · ${DATE_LINE}}

## 설치 방법별 받을 파일

| 설치 방법 | 받을 파일 | 비고 |
|---|---|---|
| Linux 서버에 직접 설치 | \`argus-flow-${VERSION}-nifi-${NIFI_VERSION}-bin.tar.gz\` | NiFi ${NIFI_VERSION} + Argus NAR + conf 오버레이 + \`bin/argus-*.sh\`. JDK 21 필요 |
| RHEL/Rocky 계열 | \`argus-flow-nifi-${VERSION}-1.x86_64.rpm\` | \`/opt/argus-flow/nifi\`, systemd 유닛. Epoch 1 — \`2.10.0-1\` 에서 \`dnf upgrade\` 가능 |
| 운영 중인 NiFi ${NIFI_VERSION} 에 확장만 추가 | \`argus-flow-nars-${VERSION}.zip\` | \`nar_extensions/\` 에 그대로 풀기 |
| Kubernetes (Helm) | \`argus-flow-${VERSION}.tgz\` | \`helm install argus-flow ./argus-flow-${VERSION}.tgz -n nifi-operator-system --create-namespace\` |
| Kubernetes (kubectl) | \`argus-flow-operator-${VERSION}.yaml\` | CRD + Namespace + RBAC + Deployment. \`kubectl apply -f\` 한 번 |
| 폐쇄망 Kubernetes | \`argus-flow-operator-image-${VERSION}.tar\` | operator 이미지 \`docker save\`. \`docker load -i\` 또는 \`k3s ctr images import\` |
| conf 편집 도구 | \`argus-config-${VERSION}.pyz\` | python3 3.10+ 단일 실행 파일 |
| DB 사용자 관리 | \`argus-user-cli-${VERSION}.jar\`, \`argus-flow-db-iaa-sql-${VERSION}.zip\` | db-iaa 프로바이더 CLI, 스키마 DDL |
| 보안 점검 | \`argus-flow-${VERSION}-sbom.cdx.json\` | CycloneDX SBOM (Maven 전 모듈 + operator·설정 도구 Python 의존성) |

### 컨테이너 이미지 (Docker Hub, 공개)

| 이미지 | 태그 |
|---|---|
| \`docker.io/datadynamics/argus-flow-nifi\` | \`${VERSION}\`, \`${VERSION}-nifi${NIFI_VERSION}\`${ALIAS} |
| \`docker.io/datadynamics/argus-flow-operator\` | \`${VERSION}\`${ALIAS} |

NiFi 이미지는 2 GiB 를 넘어 Release 에 첨부하지 않습니다. 폐쇄망에서는 tar.gz·RPM 을 쓰거나, 연결된 환경에서
\`docker pull\` 후 \`docker save\` 로 옮기세요. \`latest\` 태그는 없습니다.

## 무결성 확인

\`\`\`bash
gh release download ${TAG}${GH_R} -p SHA256SUMS -p 'argus-flow-${VERSION}-nifi-*-bin.tar.gz'
sha256sum -c SHA256SUMS --ignore-missing     # 받은 파일만 검사, 모두 OK 여야 함
\`\`\`

## 변경 사항

${SECTION}
EOF
