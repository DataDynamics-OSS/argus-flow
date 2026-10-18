#!/bin/sh
# 오퍼레이터 이미지를 빌드해 k3s containerd로 가져오고 테스트 배포를 갱신한다.
#   사용: test/k3s/redeploy.sh <tag>
set -eu
TAG=${1:?tag}
cd "$(dirname "$0")/../.."
export KUBECONFIG=${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}
docker build -q -t "nifi-operator:${TAG}" . >/dev/null
docker save "nifi-operator:${TAG}" | sudo k3s ctr images import - >/dev/null
sed "s#nifi-operator:test#nifi-operator:${TAG}#" test/k3s/10-operator.yaml | kubectl apply -f - >/dev/null
kubectl -n nifi-optest rollout status deploy/nifi-operator --timeout=120s
