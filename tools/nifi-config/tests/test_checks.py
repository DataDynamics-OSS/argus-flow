"""설정 점검(checks).

각 항목은 이 저장소가 실제로 겪은 실패에 대응한다. 테스트는 "그 상황을 만들면 잡히는가"를
확인한다. 인증서는 openssl 로 실제로 만든다 — 픽스처를 손으로 박아 두면 파싱 경로가
검증되지 않는다.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from argus_nifi_config.checks import FAIL, OK, WARN, run_checks
from argus_nifi_config.session import Session

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None,
                                reason="openssl 이 필요합니다")

DIST_CONF = Path(__file__).resolve().parents[3] / "distribution/src/main/resources/conf"


def make_cert(directory: Path, password: str, san: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "3650",
         "-nodes", "-keyout", str(directory / "k.key"), "-out", str(directory / "k.crt"),
         "-subj", "/CN=test", "-addext", f"subjectAltName={san}"],
        check=True, capture_output=True)
    subprocess.run(
        ["openssl", "pkcs12", "-export", "-in", str(directory / "k.crt"),
         "-inkey", str(directory / "k.key"), "-out", str(directory / "keystore.p12"),
         "-passout", f"pass:{password}"],
        check=True, capture_output=True)


@pytest.fixture
def home(tmp_path):
    """NIFI_HOME 모양의 디렉터리. conf 는 배포본 실물을 쓴다."""
    conf = tmp_path / "conf"
    conf.mkdir()
    for name in ("authorizers.xml", "login-identity-providers.xml"):
        shutil.copy(DIST_CONF / name, conf / name)
    return tmp_path


def write_props(home: Path, **kv) -> Session:
    lines = [f"{k.replace('__', '.')}={v}" for k, v in kv.items()]
    (home / "conf" / "nifi.properties").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return Session(home / "conf", home / "conf")


def levels(results, title_contains):
    return [r.level for r in results if title_contains in r.title]


def test_keystore_가_없으면_FAIL(home):
    session = write_props(home, nifi__security__keystore="./conf/keystore.p12")
    assert FAIL in levels(run_checks(session), "keystore 파일이 없습니다")


def test_비밀번호가_틀리면_FAIL(home):
    make_cert(home / "security", "correct-password", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="wrong-password")
    assert FAIL in levels(run_checks(session), "keystore 를 열 수 없습니다")


def test_상대_경로는_NIFI_HOME_기준으로_해석된다(home):
    """업스트림이 ./conf/keystore.p12 를 쓰고 NiFi 는 NIFI_HOME 에서 실행된다."""
    make_cert(home / "security", "pw", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw")
    assert OK in levels(run_checks(session), "keystore 파일 존재")


def test_SAN_에_IP_가_없으면_경고(home):
    """IP 로 접속하면 Jetty 가 Invalid SNI 로 거절한다."""
    make_cert(home / "security", "pw", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw")
    assert WARN in levels(run_checks(session), "SAN 에 IP 주소가 없습니다")


def test_IP_가_있으면_경고하지_않는다(home):
    make_cert(home / "security", "pw", "DNS:nifi1.example.com,IP:10.0.0.5")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw")
    assert not levels(run_checks(session), "SAN 에 IP 주소가 없습니다")


def test_https_host_가_SAN_에_없으면_FAIL(home):
    """Invalid SNI 의 직접 원인. 이 조합을 미리 잡는 것이 이 점검의 핵심이다."""
    make_cert(home / "security", "pw", "DNS:other.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw",
                          nifi__web__https__host="nifi1.example.com")
    assert FAIL in levels(run_checks(session), "SAN 에 없습니다")


def test_https_host_가_SAN_에_있으면_정상(home):
    make_cert(home / "security", "pw", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw",
                          nifi__web__https__host="nifi1.example.com")
    assert OK in levels(run_checks(session), "SAN 에 포함됨")


def test_localhost_는_SAN_불일치가_아니라_전용_안내(home):
    """조치가 다르다 — 재발급이 아니라 바인드 주소를 바꿔야 한다."""
    make_cert(home / "security", "pw", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw",
                          nifi__web__https__host="localhost")
    results = run_checks(session)
    assert WARN in levels(results, "localhost")
    assert not levels(results, "SAN 에 없습니다")


def test_자동_생성_인증서를_쓰면_경고(home):
    """NiFi 는 기동 시 32자리 hex 비밀번호를 써 넣는다. 유효기간이 60일이다."""
    make_cert(home / "security", "0123456789abcdef0123456789abcdef", "DNS:localhost")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="0123456789abcdef0123456789abcdef")
    assert WARN in levels(run_checks(session), "자동 생성 인증서")


def test_로그인_프로바이더가_주석이면_FAIL(home):
    """배포본은 db-provider 를 주석으로 내보낸다. 지정만 하고 주석을 안 풀면 기동 실패."""
    make_cert(home / "security", "pw", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw",
                          nifi__security__user__login__identity__provider="db-provider")
    assert FAIL in levels(run_checks(session), "로그인 프로바이더를 찾을 수 없습니다")


def test_인증만_DB_이고_인가가_아니면_FAIL(home):
    """로그인은 되는데 권한이 없는 상태. 로그에 원인이 잘 드러나지 않는다."""
    make_cert(home / "security", "pw", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw",
                          nifi__security__user__login__identity__provider="db-provider")
    session.xml("login-identity-providers.xml").activate_provider("db-provider")
    assert FAIL in levels(run_checks(session), "인증은 DB 인데 인가는 아닙니다")


def test_두_XML_의_DB_설정이_다르면_FAIL(home):
    make_cert(home / "security", "pw", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw",
                          nifi__security__user__login__identity__provider="db-provider")
    login = session.xml("login-identity-providers.xml")
    authz = session.xml("authorizers.xml")
    login.activate_provider("db-provider")
    authz.activate_provider("db-user-group-provider")
    login.set_property("db-provider", "Database URL", "jdbc:postgresql://a:5432/nifi")
    authz.set_property("db-user-group-provider", "Database URL", "jdbc:postgresql://b:5432/nifi")

    assert FAIL in levels(run_checks(session), "두 XML 의 DB 설정이 다릅니다")


def test_두_XML_이_일치하면_정상(home):
    make_cert(home / "security", "pw", "DNS:nifi1.example.com")
    session = write_props(home,
                          nifi__security__keystore="./security/keystore.p12",
                          nifi__security__keystorePasswd="pw",
                          nifi__security__user__login__identity__provider="db-provider")
    login = session.xml("login-identity-providers.xml")
    authz = session.xml("authorizers.xml")
    login.activate_provider("db-provider")
    authz.activate_provider("db-user-group-provider")
    for cfg, ident in ((login, "db-provider"), (authz, "db-user-group-provider")):
        cfg.set_property(ident, "Database URL", "jdbc:postgresql://db:5432/nifi")
        cfg.set_property(ident, "Database User", "argus")

    assert OK in levels(run_checks(session), "같은 DB 를 가리킵니다")
