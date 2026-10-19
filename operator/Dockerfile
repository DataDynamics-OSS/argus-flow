FROM python:3.12-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

FROM python:3.12-slim

# 이미지 이름: datadynamics/argus-flow-operator. 버전·커밋은 빌드 인자로 받는다
# (scripts/build-release.sh, .github/workflows/release.yml).
ARG VERSION=0.0.0-dev
ARG REVISION=unknown
ARG SOURCE=https://github.com/DataDynamics-OSS/argus-flow
ARG CREATED=1970-01-01T00:00:00Z
# operator 가 기본으로 다루는 NiFi 버전(루트 pom 의 nifi.version). 표시용 라벨이다.
ARG NIFI_VERSION=unknown

LABEL org.opencontainers.image.title="Argus Flow NiFi Operator" \
      org.opencontainers.image.description="Kubernetes operator for Apache NiFi 2.x clusters (Argus Flow)" \
      org.opencontainers.image.vendor="Data Dynamics Inc." \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.created="${CREATED}" \
      io.datadynamics.nifi.version="${NIFI_VERSION}"

WORKDIR /app

COPY --from=builder /app/deps /app/deps
ENV PYTHONPATH="/app/deps:${PYTHONPATH}"

COPY nifi_operator/ ./nifi_operator/

# kopf는 기본 식별자 생성에 getpass.getuser()(→ pwd 조회)를 쓰므로 uid 1000이
# /etc/passwd에 있어야 한다. 없으면 "getpwuid(): uid not found: 1000"으로 기동 실패.
RUN groupadd -g 1000 nifiop && useradd -u 1000 -g 1000 -M -d /app -s /usr/sbin/nologin nifiop

USER 1000:1000

# --liveness: kopf가 /healthz(8080)를 서빙 → Deployment의 프로브가 실제로 응답받음
# 감시 범위는 CMD(기본 --all-namespaces)로 두어 Deployment args로 바꿀 수 있게 한다
# (예: args: ["--namespace=nifi", "--standalone"]).
ENTRYPOINT ["python", "-m", "kopf", "run", "nifi_operator/main.py", \
            "--liveness=http://0.0.0.0:8080/healthz"]
CMD ["--all-namespaces"]
