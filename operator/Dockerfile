FROM python:3.12-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

FROM python:3.12-slim

WORKDIR /app

COPY --from=builder /app/deps /app/deps
ENV PYTHONPATH="/app/deps:${PYTHONPATH}"

COPY nifi_operator/ ./nifi_operator/

USER 1000:1000

# --liveness: kopf가 /healthz(8080)를 서빙 → Deployment의 프로브가 실제로 응답받음
ENTRYPOINT ["python", "-m", "kopf", "run", "nifi_operator/main.py", \
            "--all-namespaces", \
            "--liveness=http://0.0.0.0:8080/healthz"]
