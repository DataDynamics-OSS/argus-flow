"""설정 점검(진단).

설정을 바꾸지 않고 현재 상태만 본다. 항목은 이 저장소가 실제로 겪은 실패에서 왔다 —
SAN 불일치로 인한 `Invalid SNI`, IP 접속 실패, 자동 생성 인증서의 짧은 만료, 두 XML 의
DB 설정 불일치.

인증서 파싱에 `cryptography` 를 쓰지 않는다. 네이티브 확장이 있어 zipapp 단일 파일 배포가
성립하지 않기 때문이다(design/config-tool-packaging.md §2.1). 대신 openssl 을 호출하고,
없으면 해당 항목만 건너뛴다.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .session import Session

OK = "ok"
WARN = "warn"
FAIL = "fail"
SKIP = "skip"


@dataclass
class Result:
    level: str          # OK / WARN / FAIL / SKIP
    title: str
    detail: str = ""
    hint: str = ""


def _openssl() -> str | None:
    return shutil.which("openssl")


def _run(argv: list[str]) -> tuple[int, str]:
    proc = subprocess.run(argv, capture_output=True, text=True)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _resolve(conf_dir: Path, value: str) -> Path:
    """nifi.properties 의 경로를 푼다.

    NiFi 는 NIFI_HOME 에서 실행되므로 상대 경로는 그 기준이다(업스트림 기본값도
    ./conf/keystore.p12 형태다).
    """
    p = Path(value)
    return p if p.is_absolute() else (conf_dir.parent / value).resolve()


def _cert_pem_from_keystore(path: Path, password: str) -> str | None:
    openssl = _openssl()
    if openssl is None:
        return None
    rc, out = _run([openssl, "pkcs12", "-in", str(path), "-clcerts", "-nokeys",
                    "-passin", f"pass:{password}"])
    return out if rc == 0 and "BEGIN CERTIFICATE" in out else None


def _cert_field(pem: str, *args: str) -> str:
    openssl = _openssl()
    if openssl is None:
        return ""
    proc = subprocess.run([openssl, "x509", "-noout", *args],
                          input=pem, capture_output=True, text=True)
    return proc.stdout.strip()


def run_checks(session: Session) -> list[Result]:
    props = session.props("nifi.properties")
    conf = session.conf_dir
    results: list[Result] = []

    keystore_value = props.get("nifi.security.keystore") or ""
    password = props.get("nifi.security.keystorePasswd") or ""
    https_host = props.get("nifi.web.https.host") or ""

    # ── 자동 생성 인증서를 그대로 쓰고 있는가 ──────────────────────────
    # NiFi 는 keystore 가 없으면 기동 시 자체 서명 인증서를 만들고 32자리 hex 비밀번호를
    # nifi.properties 에 써 넣는다. SAN 이 localhost 와 호스트명뿐이고 유효기간이 60일이다.
    if re.fullmatch(r"[0-9a-f]{32}", password or ""):
        results.append(Result(
            WARN, "NiFi 자동 생성 인증서를 사용 중입니다",
            "keystorePasswd 가 32자리 hex 입니다 — NiFi 가 기동 시 만든 값입니다.",
            "유효기간이 60일이고 SAN 에 IP 가 없습니다. bin/argus-config.sh --recipe tls:generate "
            "로 직접 관리하는 인증서를 만드십시오."))

    # ── keystore 존재와 비밀번호 ────────────────────────────────────────
    if not keystore_value:
        results.append(Result(FAIL, "keystore 가 설정되지 않았습니다",
                              hint="nifi.security.keystore 를 설정하십시오."))
        return results

    keystore = _resolve(conf, keystore_value)
    if not keystore.is_file():
        results.append(Result(
            FAIL, "keystore 파일이 없습니다", f"{keystore_value} → {keystore}",
            "경로가 맞는지, 인증서를 생성했는지 확인하십시오. NiFi 는 NIFI_HOME 에서 "
            "실행되므로 상대 경로는 그 기준입니다."))
        return results
    results.append(Result(OK, "keystore 파일 존재", str(keystore)))

    if _openssl() is None:
        results.append(Result(SKIP, "인증서 내용 점검을 건너뜁니다",
                              "openssl 을 찾을 수 없습니다."))
        return results

    pem = _cert_pem_from_keystore(keystore, password)
    if pem is None:
        results.append(Result(
            FAIL, "keystore 를 열 수 없습니다",
            "비밀번호가 맞지 않거나 파일이 손상되었습니다.",
            "nifi.security.keystorePasswd 를 확인하십시오."))
        return results
    results.append(Result(OK, "keystore 비밀번호 일치"))

    # ── SAN 과 접속 주소 ────────────────────────────────────────────────
    san = _cert_field(pem, "-ext", "subjectAltName")
    dns = re.findall(r"DNS:([^,\s]+)", san)
    ips = re.findall(r"IP Address:([^,\s]+)", san)
    results.append(Result(OK, "인증서 SAN",
                          f"DNS={', '.join(dns) or '없음'} / IP={', '.join(ips) or '없음'}"))

    if not ips:
        results.append(Result(
            WARN, "SAN 에 IP 주소가 없습니다",
            "IP 로 접속하면 Jetty 가 400 Invalid SNI 로 거절합니다.",
            "IP 접속이 필요하면 tls:generate 의 ips 파라미터로 재발급하십시오."))

    # localhost 를 먼저 본다. SAN 불일치보다 구체적인 진단이고 조치도 다르기 때문이다
    # (재발급이 아니라 바인드 주소를 바꿔야 한다).
    if https_host in ("localhost", "127.0.0.1"):
        results.append(Result(
            WARN, "nifi.web.https.host 가 localhost 입니다",
            "이 노드 밖에서는 접속할 수 없습니다.",
            "외부 접속이 필요하면 실제 주소나 빈 값(모든 인터페이스)으로 바꾸십시오. "
            "그 이름이 인증서 SAN 에도 있어야 합니다."))
    elif https_host and https_host != "0.0.0.0":
        if https_host in dns or https_host in ips:
            results.append(Result(OK, "nifi.web.https.host 가 SAN 에 포함됨", https_host))
        else:
            results.append(Result(
                FAIL, "nifi.web.https.host 가 인증서 SAN 에 없습니다",
                f"host={https_host}, SAN={', '.join(dns + ips) or '없음'}",
                "이 상태로 접속하면 Invalid SNI 로 거절됩니다. SAN 에 이 이름을 넣어 "
                "재발급하거나 https.host 를 SAN 의 이름으로 바꾸십시오."))

    # ── 만료 ────────────────────────────────────────────────────────────
    end = _cert_field(pem, "-enddate").replace("notAfter=", "")
    try:
        expiry = datetime.strptime(end, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
        days = (expiry - datetime.now(timezone.utc)).days
        if days < 0:
            results.append(Result(FAIL, "인증서가 만료되었습니다", end))
        elif days < 30:
            results.append(Result(WARN, f"인증서 만료가 {days}일 남았습니다", end))
        else:
            results.append(Result(OK, "인증서 유효기간", f"{end} ({days}일 남음)"))
    except ValueError:
        results.append(Result(SKIP, "만료일을 해석할 수 없습니다", end))

    results += _check_providers(session)
    return results


def _check_providers(session: Session) -> list[Result]:
    """로그인 프로바이더 설정이 XML 과 맞는지, DB 를 쓴다면 두 파일이 일치하는지."""
    out: list[Result] = []
    props = session.props("nifi.properties")
    provider_id = props.get("nifi.security.user.login.identity.provider") or ""

    try:
        login = session.xml("login-identity-providers.xml")
    except FileNotFoundError:
        return out

    active = [p.identifier for p in login.providers()]
    if provider_id and provider_id not in active:
        out.append(Result(
            FAIL, "로그인 프로바이더를 찾을 수 없습니다",
            f"nifi.properties={provider_id}, 활성={', '.join(active) or '없음'}",
            "오타이거나 해당 블록이 주석 상태입니다. 주석이면 NiFi 가 기동하지 않습니다."))
    elif provider_id:
        out.append(Result(OK, "로그인 프로바이더", provider_id))

    if provider_id != "db-provider":
        return out

    # DB 인증을 쓰는 경우, 인가 쪽 설정과 값이 같아야 한다. 어긋나면 로그인은 되는데
    # 권한이 없는 상태가 되고, 원인이 로그에 잘 드러나지 않는다.
    try:
        authz = session.xml("authorizers.xml")
    except FileNotFoundError:
        return out

    if "db-user-group-provider" not in [p.identifier for p in authz.providers()]:
        out.append(Result(
            FAIL, "인증은 DB 인데 인가는 아닙니다",
            "authorizers.xml 의 db-user-group-provider 가 활성이 아닙니다.",
            "로그인은 되지만 사용자를 알 수 없어 권한이 없습니다. "
            "--recipe login:db 로 함께 설정하십시오."))
        return out

    mismatched = [
        name for name in ("Database URL", "Database User")
        if login.get_property("db-provider", name) != authz.get_property("db-user-group-provider", name)
    ]
    if mismatched:
        out.append(Result(
            FAIL, "두 XML 의 DB 설정이 다릅니다", ", ".join(mismatched),
            "인증과 인가가 서로 다른 DB 를 봅니다. --recipe login:db 로 다시 맞추십시오."))
    else:
        out.append(Result(OK, "인증·인가가 같은 DB 를 가리킵니다",
                          login.get_property("db-provider", "Database URL") or ""))
    return out
