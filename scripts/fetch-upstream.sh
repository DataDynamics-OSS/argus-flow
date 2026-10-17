#!/usr/bin/env bash
# Apache NiFi 업스트림 원본 확보.
#
# 모든 포팅 소스는 이 스크립트로 받은 apache/nifi 원본(Apache-2.0)에서만 가져온다.
# 벤더 배포판 저장소의 코드는 복사·참조를 금지한다. 자세한 규약은 CONTRIBUTING.md 참조.
#
# 사용법:
#   scripts/fetch-upstream.sh [VERSION] [DEST]
#     VERSION  1.28.0 (기본) | 2.10.0
#     DEST     기본 .upstream/nifi-<VERSION>
#   하위 호환: 첫 인자가 버전이 아니면 1.28.0 의 DEST 로 해석한다.
set -euo pipefail

VERSION="1.28.0"
if [[ $# -gt 0 && "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  VERSION="$1"
  shift
fi
DEST="${1:-.upstream/nifi-$VERSION}"

case "$VERSION" in
  1.28.0)
    # 검증된 태그 커밋 (2026-07-19 확인)
    EXPECTED_COMMIT="8ecf23e77c8ca828a77f3b84554ed3347d8f7fa2"
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
    ;;
  2.10.0)
    # 검증된 태그 커밋 (2026-10-14 확인)
    EXPECTED_COMMIT="2baff691128fa782983fb79bb1087adff027a4c1"
    BUNDLES=(
      # nifi-argus-listing-bundle 포크 원본 (design/listing-processors-port.md)
      nifi-extension-bundles/nifi-extension-utils/nifi-listed-entity
      nifi-extension-bundles/nifi-extension-utils/nifi-file-transfer
      nifi-extension-bundles/nifi-standard-bundle/nifi-standard-processors
      nifi-extension-bundles/nifi-standard-bundle/nifi-standard-nar
      nifi-extension-bundles/nifi-aws-bundle/nifi-aws-processors
      nifi-extension-bundles/nifi-aws-bundle/nifi-aws-nar
      nifi-extension-bundles/nifi-hadoop-bundle/nifi-hdfs-processors
      nifi-extension-bundles/nifi-hadoop-bundle/nifi-hadoop-nar
    )
    ;;
  *)
    echo "ERROR: unsupported version: $VERSION (1.28.0 | 2.10.0)" >&2
    exit 1
    ;;
esac
TAG="rel/nifi-$VERSION"

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
