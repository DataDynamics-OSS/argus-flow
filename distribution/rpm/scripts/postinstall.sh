#!/bin/bash
set -e
if ! id nifi >/dev/null 2>&1; then
    useradd --system --home-dir /opt/argus-flow/nifi --shell /sbin/nologin nifi
fi
chown -R nifi:nifi /opt/argus-flow/nifi
systemctl daemon-reload
