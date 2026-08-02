# K8s 배포용 NiFi 이미지: upstream 공식 이미지 + Argus NAR 4종
# 빌드는 `make docker-image` 사용 (NAR을 docker/nars/ 에 스테이징한 뒤 빌드)
FROM apache/nifi:2.10.0

COPY --chown=nifi:nifi nars/*.nar /opt/nifi/nifi-current/extensions/
