#!/usr/bin/env bash
# NiFi 서버 인증서 생성 (openssl) — SAN 에 DNS 이름과 IP 주소를 명시 지정한다.
#
# tls-toolkit 없이 NiFi 가 요구하는 PKCS12 keystore/truststore 를 만든다.
# Jetty 는 요청 호스트(SNI 또는 Host 헤더)를 인증서 SAN 과 대조하므로,
# 접속에 사용할 모든 이름과 IP 가 SAN 에 들어 있어야 한다.
#
#   CA_HOSTNAME        CA 인증서 CN (기본: nifi-ca)
#   HOSTS              서버 인증서 SAN 의 DNS 이름 목록 (쉼표 구분)
#   IPS                서버 인증서 SAN 의 IP 주소 목록 (쉼표 구분, 선택)
#   NIFI_SSL_PASSWORD  keystore/truststore 비밀번호
#   NIFI_SSL_HOME      출력 디렉터리
#   NIFI_SSL_CA_DAYS   CA 인증서 유효기간 (기본: 36500 = 100년)
#   NIFI_SSL_DAYS      서버 인증서 유효기간 (기본: 3650 = 10년)
#
# CA 를 길게 잡는 이유: CA 가 만료되면 그 CA 가 발급한 모든 서버 인증서가 한꺼번에
# 무효가 되고, 클라이언트에 배포한 신뢰 인증서도 전부 교체해야 한다. 서버 인증서는
# 더 짧게 두고 CA 로 재발급하는 편이 운영 부담이 작다.
set -euo pipefail

CA_HOSTNAME="${CA_HOSTNAME:-nifi-ca}"
HOSTS="${HOSTS:?set HOSTS (예: nifi1.example.com)}"
IPS="${IPS:-}"
PASSWD="${NIFI_SSL_PASSWORD:-ChangeMe}"
OUT="${NIFI_SSL_HOME:-/opt/nifi/security}"
CA_DAYS="${NIFI_SSL_CA_DAYS:-36500}"
DAYS="${NIFI_SSL_DAYS:-3650}"

PRIMARY="${HOSTS%%,*}"
mkdir -p "$OUT"

# SAN 문자열 조립: DNS:a,DNS:b,IP:1.2.3.4
SAN=""
IFS=',' read -ra _h <<< "$HOSTS"
for h in "${_h[@]}"; do [ -n "$h" ] && SAN="${SAN}${SAN:+,}DNS:${h// /}"; done
if [ -n "$IPS" ]; then
  IFS=',' read -ra _i <<< "$IPS"
  for i in "${_i[@]}"; do [ -n "$i" ] && SAN="${SAN}${SAN:+,}IP:${i// /}"; done
fi
echo "SAN = $SAN"

# 1. CA
openssl req -x509 -newkey rsa:4096 -sha256 -days "$CA_DAYS" -nodes \
  -keyout "$OUT/ca.key" -out "$OUT/ca.crt" \
  -subj "/CN=${CA_HOSTNAME}" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

# 2. 서버 키 + CSR
openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout "$OUT/server.key" -out "$OUT/server.csr" \
  -subj "/CN=${PRIMARY}" 2>/dev/null

# 3. CA 서명 (SAN 포함)
openssl x509 -req -in "$OUT/server.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" \
  -CAcreateserial -days "$DAYS" -sha256 -out "$OUT/server.crt" \
  -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\n' "$SAN") 2>/dev/null

# 4. keystore.p12 (서버 키 + 인증서 체인)
openssl pkcs12 -export -in "$OUT/server.crt" -inkey "$OUT/server.key" \
  -certfile "$OUT/ca.crt" -name nifi-key \
  -out "$OUT/keystore.p12" -passout "pass:${PASSWD}"

# 5. truststore.p12 (CA)
rm -f "$OUT/truststore.p12"
keytool -importcert -noprompt -alias nifi-ca -file "$OUT/ca.crt" \
  -keystore "$OUT/truststore.p12" -storetype PKCS12 -storepass "$PASSWD" >/dev/null

rm -f "$OUT/server.csr" "$OUT/ca.srl"
chmod 600 "$OUT"/*.key "$OUT"/*.p12
echo "생성 완료: $OUT/{keystore.p12,truststore.p12,ca.crt}"
