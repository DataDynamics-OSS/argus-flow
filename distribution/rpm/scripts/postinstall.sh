#!/bin/bash
set -e
if ! id nifi >/dev/null 2>&1; then
    useradd --system --home-dir /opt/argus-flow/nifi --shell /sbin/nologin nifi
fi
chown -R nifi:nifi /opt/argus-flow/nifi
systemctl daemon-reload

# 설정 도구는 python3 3.10+ 를 쓴다. NiFi 자체에는 필요 없으므로 RPM 의존성으로 선언하지
# 않고, 없으면 안내만 한다.
if [ -f /opt/argus-flow/nifi/tools/argus-config/argus-config.pyz ]; then
    if ! command -v python3 >/dev/null 2>&1 \
       || ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)' 2>/dev/null; then
        echo "알림: 설정 도구(bin/argus-config.sh)를 쓰려면 python3 3.10 이상이 필요합니다."
        echo "      NiFi 자체 동작에는 영향이 없습니다."
    fi
fi
