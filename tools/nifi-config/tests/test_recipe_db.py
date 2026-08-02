"""DB 기반 인증·인가 레시피 (login:db).

이 레시피의 존재 이유는 프롬프트가 아니라 **파일 간 정합성**이다. NiFi 는 인증과 인가를
다른 SPI 로 분리하므로 한쪽만 켜면 "비밀번호는 맞는데 권한이 없는" 상태가 된다. 테스트는
두 파일이 같은 값으로, 스키마가 요구하는 순서를 지켜 설정되는지에 집중한다.
"""

import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from argus_nifi_config.catalog.xml_recipes import BY_ID
from argus_nifi_config.session import Session

DIST_CONF = Path(__file__).resolve().parents[3] / "distribution/src/main/resources/conf"


@pytest.fixture
def session(tmp_path):
    """배포본의 실제 conf 를 복사해 쓴다. 합성 픽스처는 실물과 다를 수 있다."""
    conf = tmp_path / "conf"
    conf.mkdir()
    for name in ("authorizers.xml", "login-identity-providers.xml"):
        shutil.copy(DIST_CONF / name, conf / name)
    (conf / "nifi.properties").write_text(
        "nifi.security.user.login.identity.provider=single-user-provider\n"
        "nifi.security.user.authorizer=single-user-authorizer\n",
        encoding="utf-8",
    )
    return Session(conf, conf)


PARAMS = {
    "db_url": "jdbc:postgresql://db.example.com:5432/nifi",
    "db_user": "argus",
    "db_password": "${ARGUS_DB_IAA_PASSWORD}",
    "driver_class": "org.postgresql.Driver",
    "initial_admin": "admin",
    "cache_duration": "1 min",
    "expiration": "8 hours",
    "max_failed": "3",
    "lockout": "30 mins",
}


def apply_recipe(session, params=None):
    recipe = BY_ID["login:db"]
    xml = session.xml(recipe.file)
    return recipe.apply(xml, params or PARAMS, session.props("nifi.properties"), session)


def test_두_파일의_DB_설정이_일치한다(session):
    """어긋나면 인증은 되는데 인가가 사용자를 모르는 상태가 된다."""
    apply_recipe(session)

    login = session.xml("login-identity-providers.xml")
    authz = session.xml("authorizers.xml")
    for name, param in (("Database URL", "db_url"), ("Database User", "db_user"),
                        ("Database Password", "db_password")):
        assert login.get_property("db-provider", name) == PARAMS[param]
        assert authz.get_property("db-user-group-provider", name) == PARAMS[param]


def test_주석이_해제되어_provider_로_인식된다(session):
    apply_recipe(session)
    assert "db-provider" in [p.identifier for p in session.xml("login-identity-providers.xml").providers()]
    assert "db-user-group-provider" in [p.identifier for p in session.xml("authorizers.xml").providers()]


def test_authorizers_의_요소_순서가_스키마를_만족한다(session, tmp_path):
    """userGroupProvider 는 accessPolicyProvider·authorizer 보다 앞에 와야 한다."""
    apply_recipe(session)
    session.save_all()

    tags = [c.tag for c in ET.parse(tmp_path / "conf/authorizers.xml").getroot()
            if isinstance(c.tag, str)]
    last_ugp = max(i for i, t in enumerate(tags) if t == "userGroupProvider")
    first_other = min(i for i, t in enumerate(tags)
                      if t in ("accessPolicyProvider", "authorizer"))
    assert last_ugp < first_other, tags


def test_저장한_XML_이_well_formed(session, tmp_path):
    apply_recipe(session)
    session.save_all()
    for name in ("authorizers.xml", "login-identity-providers.xml"):
        ET.parse(tmp_path / "conf" / name)


def test_인가는_사용자만_DB_로_옮기고_정책은_파일에_둔다(session):
    apply_recipe(session)
    authz = session.xml("authorizers.xml")
    assert authz.get_property("file-access-policy-provider", "User Group Provider") == "db-user-group-provider"
    assert authz.get_property("file-access-policy-provider", "Initial Admin Identity") == "admin"


def test_nifi_properties_가_함께_설정된다(session):
    apply_recipe(session)
    props = session.props("nifi.properties")
    assert props.get("nifi.security.user.login.identity.provider") == "db-provider"
    assert props.get("nifi.security.user.authorizer") == "managed-authorizer"


def test_인증_전용_설정은_login_쪽에만_간다(session):
    apply_recipe(session)
    login = session.xml("login-identity-providers.xml")
    authz = session.xml("authorizers.xml")
    assert login.get_property("db-provider", "Max Failed Attempts") == "3"
    assert login.get_property("db-provider", "Lockout Duration") == "30 mins"
    assert login.get_property("db-provider", "Authentication Expiration") == "8 hours"
    # 인가 쪽에는 없어야 한다
    assert authz.get_property("db-user-group-provider", "Max Failed Attempts") is None
    assert authz.get_property("db-user-group-provider", "Cache Duration") == "1 min"


def test_환경변수_참조를_쓰면_안내가_나온다(session):
    notes = apply_recipe(session)
    assert any("환경변수" in n for n in notes), notes


def test_후속_명령을_순서대로_안내한다(session):
    notes = apply_recipe(session)
    joined = "\n".join(notes)
    # argus-user.sh 는 authorizers.xml 을 읽으므로 저장 뒤에 실행해야 한다
    assert "schema-init" in joined
    assert "argus-user.sh add admin" in joined


def test_MariaDB_드라이버_경로를_주면_미포함_사실을_알린다(session):
    params = dict(PARAMS, driver_class="org.mariadb.jdbc.Driver",
                  driver_path="/opt/nifi/drivers/mariadb.jar")
    notes = apply_recipe(session, params)
    assert any("LGPL" in n or "포함되지 않" in n for n in notes), notes
    assert session.xml("login-identity-providers.xml").get_property(
        "db-provider", "Database Driver Location") == "/opt/nifi/drivers/mariadb.jar"


def test_두_번_적용해도_안전하다(session):
    apply_recipe(session)
    apply_recipe(session)                      # 이미 활성 — 주석 해제는 no-op
    authz = session.xml("authorizers.xml")
    assert authz.get_property("db-user-group-provider", "Database User") == "argus"
    ids = [p.identifier for p in authz.providers()]
    assert ids.count("db-user-group-provider") == 1
