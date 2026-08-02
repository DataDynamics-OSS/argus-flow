#!/usr/bin/env bash
# Apache NiFi 1.28.0 업스트림 원본 확보.
#
# 모든 포팅 소스는 이 스크립트로 받은 apache/nifi 원본(Apache-2.0)에서만 가져온다.
# 벤더 배포판 저장소의 코드는 복사·참조를 금지한다. 자세한 규약은 CONTRIBUTING.md 참조.
set -euo pipefail

TAG="rel/nifi-1.28.0"
# 검증된 태그 커밋 (2026-07-19 확인)
EXPECTED_COMMIT="8ecf23e77c8ca828a77f3b84554ed3347d8f7fa2"
DEST="${1:-.upstream/nifi-1.28.0}"

BUNDLES=(
  # 포팅 대상
  nifi-nar-bundles/nifi-iceberg-bundle
  nifi-nar-bundles/nifi-cassandra-bundle
  nifi-nar-bundles/nifi-solr-bundle
  nifi-nar-bundles/nifi-grpc-bundle
  nifi-nar-bundles/nifi-beats-bundle
  nifi-nar-bundles/nifi-standard-services

  # 라이선스 헤더 대조 대상 — 구 저장소에서 이관된 번들의 파생 원본을 확인하기 위해
  # 받는다. 포팅 소스로 쓰는 것이 아니라 대조용이다.
  nifi-nar-bundles/nifi-hive-bundle
  nifi-nar-bundles/nifi-kudu-bundle
  nifi-nar-bundles/nifi-standard-bundle
  nifi-nar-bundles/nifi-parquet-bundle
  nifi-nar-bundles/nifi-record-serialization-services-bundle
  nifi-nar-bundles/nifi-extension-utils
  nifi-commons/nifi-record
)

if [[ ! -d "$DEST/.git" ]]; then
  git clone --filter=blob:none --sparse --depth 1 --branch "$TAG" \
    https://github.com/apache/nifi.git "$DEST"
fi

cd "$DEST"
ACTUAL=$(git rev-parse HEAD)
if [[ "$ACTUAL" != "$EXPECTED_COMMIT" ]]; then
  echo "ERROR: upstream commit mismatch: expected $EXPECTED_COMMIT, got $ACTUAL" >&2
  exit 1
fi
git sparse-checkout set "${BUNDLES[@]}"
echo "OK: $TAG @ $ACTUAL -> $DEST"
