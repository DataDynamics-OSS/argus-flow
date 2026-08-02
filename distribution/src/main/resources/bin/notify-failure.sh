#!/bin/bash
# NiFi 프로세스 비정상 종료 알림 (systemd OnFailure= 에서 호출)
# HttpNotificationReportingTask와 동일한 웹훅으로 NIFI_DIED 이벤트를 전송한다.
# 설정: /etc/sysconfig/argus-flow-nifi 의 ARGUS_NOTIFY_URL
set -u

[ -f /etc/sysconfig/argus-flow-nifi ] && . /etc/sysconfig/argus-flow-nifi

if [ -z "${ARGUS_NOTIFY_URL:-}" ]; then
    echo "ARGUS_NOTIFY_URL not set; skipping failure notification"
    exit 0
fi

curl -sf -m 10 -X POST -H "Content-Type: application/json" \
    -d "{\"type\":\"NIFI_DIED\",\"subject\":\"NiFi service failed\",\"hostname\":\"$(hostname -f 2>/dev/null || hostname)\",\"timestamp\":\"$(date -Iseconds)\"}" \
    "$ARGUS_NOTIFY_URL" || echo "failure notification POST failed"
