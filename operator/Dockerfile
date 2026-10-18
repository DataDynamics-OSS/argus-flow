FROM python:3.12-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

FROM python:3.12-slim

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
