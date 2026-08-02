#!/usr/bin/env bash
# NiFi TLS Toolkit standalone 모드로 CA·키스토어·트러스트스토어를 생성한다.
#
# 모든 설정은 환경변수로 덮어쓸 수 있다. 기본값은 예시이므로 실제 환경에 맞게 지정할 것.
#   NIFI_TOOLKIT_HOME  tls-toolkit.sh 가 있는 NiFi Toolkit 설치 경로
#   NIFI_SSL_HOME      생성물 출력 디렉터리 (실행 시 내용이 모두 삭제된다)
#   CA_HOSTNAME        CA 인증서의 CN
#   HOSTS              인증서를 발급할 호스트 목록 (쉼표 구분)
#   NIFI_SSL_PASSWORD  키/키스토어/트러스트스토어 비밀번호 — 운영 환경에서는 반드시 지정
#   NIFI_SSL_FORCE=1   출력 디렉터리 삭제 확인 프롬프트를 건너뛴다
#
# 예: NIFI_SSL_PASSWORD='...' HOSTS=nifi1.example.com,nifi2.example.com ./ssl-generate.sh
#
# 생성 후 확인:
#   keytool -list -v -keystore "${NIFI_SSL_HOME}/${CA_HOSTNAME}/truststore.jks" -storepass "<비밀번호>"
set -euo pipefail

NIFI_TOOLKIT_HOME="${NIFI_TOOLKIT_HOME:-/opt/nifi-toolkit}"
NIFI_SSL_HOME="${NIFI_SSL_HOME:-/opt/nifi/security}"

CA_HOSTNAME="${CA_HOSTNAME:-nifi1.example.com}"
PASSWD="${NIFI_SSL_PASSWORD:-ChangeMe}"
HOSTS="${HOSTS:-nifi1.example.com}"

if [ "${PASSWD}" = "ChangeMe" ]; then
  echo "WARNING: 기본 비밀번호(ChangeMe)를 사용 중입니다. 운영 환경에서는 NIFI_SSL_PASSWORD 로 지정하십시오." >&2
fi

if [ ! -d "${NIFI_SSL_HOME}" ]; then
  echo "${NIFI_SSL_HOME} does not exist."
  mkdir -p "${NIFI_SSL_HOME}"
fi

# 출력 디렉터리를 통째로 비운다 — 기존 인증서가 있으면 되돌릴 수 없다.
if [ "${NIFI_SSL_FORCE:-0}" != "1" ]; then
  printf '%s 의 내용을 모두 삭제합니다. 계속하시겠습니까? [y/N] ' "${NIFI_SSL_HOME}"
  read -r REPLY
  case "${REPLY}" in
    [yY]) ;;
    *) echo "중단했습니다."; exit 1 ;;
  esac
fi
rm -rf "${NIFI_SSL_HOME:?}"/*

HOST_STRING=""
for HOST in ${HOSTS//,/ }; do
    HOST_STRING="${HOST_STRING} -n ${HOST}"
done

echo ${HOST_STRING}

# HOST_STRING 은 "-n host -n host" 형태라 단어 분리가 필요하므로 인용하지 않는다.
sh "${NIFI_TOOLKIT_HOME}/bin/tls-toolkit.sh" standalone \
        ${HOST_STRING} \
        --days 3650 \
        -c "${CA_HOSTNAME}" \
        --keyPassword "${PASSWD}" \
        --trustStorePassword "${PASSWD}" \
        --keyStorePassword "${PASSWD}" \
        -C 'CN=admin' \
        --clientCertPassword "${PASSWD}" \
        -o "${NIFI_SSL_HOME}"

chown -R nifi:nifi "${NIFI_SSL_HOME}"/*
chmod -R 640 "${NIFI_SSL_HOME}"/*

echo "Directory: ${NIFI_SSL_HOME}"

ls -lsa "${NIFI_SSL_HOME}"
