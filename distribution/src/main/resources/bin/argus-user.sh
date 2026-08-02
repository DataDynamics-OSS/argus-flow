#!/usr/bin/env bash
# Argus Flow — DB 기반 인증·인가의 사용자 관리 CLI.
#
# 접속 정보는 conf/authorizers.xml 의 DbUserGroupProvider 블록에서 읽는다.
# 사용법은 `argus-user.sh --help` 참조.
#
# NiFi 가 떠 있는 상태에서 써도 된다 — 프로바이더가 DB 를 조회하므로 변경이 반영된다.
# 예외는 schema-init 으로, DDL 중 프로바이더 조회가 실패할 수 있어 NiFi 정지 상태를 권한다.
#
# 환경변수(선택):
#   JAVA_HOME            사용할 JDK. 없으면 PATH 의 java
#   ARGUS_JDBC_DRIVER    번들되지 않은 JDBC 드라이버 jar 경로. MariaDB 드라이버는
#                        LGPL-2.1 이라 배포에 포함하지 않으므로 직접 받아 지정한다.
#                        지정하지 않으면 authorizers.xml 의 Database Driver Location 을 쓴다.
set -euo pipefail

NIFI_HOME="$(cd "$(dirname "$0")/.." && pwd)"
CLI_JAR="${NIFI_HOME}/tools/argus-user/argus-user-cli.jar"

if [ ! -f "${CLI_JAR}" ]; then
  echo "CLI jar 를 찾을 수 없습니다: ${CLI_JAR}" >&2
  exit 1
fi

if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
  JAVA="${JAVA_HOME}/bin/java"
else
  JAVA="$(command -v java || true)"
fi
if [ -z "${JAVA}" ]; then
  echo "java 를 찾을 수 없습니다. JAVA_HOME 을 설정하십시오." >&2
  exit 1
fi

CLASSPATH="${CLI_JAR}"
if [ -n "${ARGUS_JDBC_DRIVER:-}" ]; then
  CLASSPATH="${CLASSPATH}:${ARGUS_JDBC_DRIVER}"
fi

exec "${JAVA}" -cp "${CLASSPATH}" \
  "-Dnifi.home=${NIFI_HOME}" \
  io.datadynamics.nifi.iaa.db.cli.ArgusUserCommand "$@"
