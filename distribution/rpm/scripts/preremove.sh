#!/bin/bash
set -e
if systemctl is-active --quiet nifi; then
    systemctl stop nifi
fi
