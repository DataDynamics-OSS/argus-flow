# K8s 배포용 NiFi 이미지: upstream 공식 이미지 + Argus NAR 전부(+ nifi-hadoop-libraries-nar)
# 빌드는 `make docker-image` 또는 scripts/build-release.sh 사용
# (NAR 을 docker/nars/ 에 스테이징한 뒤 빌드). 이미지 이름: datadynamics/argus-flow-nifi
#
# 기반 NiFi 버전. 루트 pom 의 nifi.version 과 같아야 한다(scripts/check-version.sh 가 검사).
ARG NIFI_VERSION=2.10.0
FROM apache/nifi:${NIFI_VERSION}

ARG NIFI_VERSION
ARG VERSION=0.0.0-dev
ARG REVISION=unknown
ARG SOURCE=https://github.com/DataDynamics-OSS/argus-flow
ARG CREATED=1970-01-01T00:00:00Z

LABEL org.opencontainers.image.title="Argus Flow NiFi" \
      org.opencontainers.image.description="Apache NiFi ${NIFI_VERSION} with Argus Flow extensions" \
      org.opencontainers.image.vendor="Data Dynamics Inc." \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.base.name="docker.io/apache/nifi:${NIFI_VERSION}" \
      io.datadynamics.nifi.version="${NIFI_VERSION}"

COPY --chown=nifi:nifi nars/*.nar /opt/nifi/nifi-current/extensions/
