"""주석 처리된 provider 활성화/비활성화 (xmlconf).

배포본은 DB 인증·인가 provider 를 주석 상태로 내보내므로, 레시피가 성립하려면 주석을
벗겨 살아 있는 요소로 만들 수 있어야 한다. 위치 보존이 핵심이다 — authorizers.xml 의
스키마는 userGroupProvider 가 accessPolicyProvider·authorizer 보다 앞에 오도록 강제한다.
"""

import xml.etree.ElementTree as ET

import pytest

from argus_nifi_config.xmlconf import XmlConfig

AUTHORIZERS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<authorizers>
    <userGroupProvider>
        <identifier>file-user-group-provider</identifier>
        <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
        <property name="Users File">./conf/users.xml</property>
    </userGroupProvider>
    <!-- 설명 주석은 활성화 후에도 남아야 한다 -->
    <!--
    <userGroupProvider>
        <identifier>db-user-group-provider</identifier>
        <class>io.datadynamics.nifi.iaa.db.DbUserGroupProvider</class>
        <property name="Database URL">jdbc:postgresql://localhost:5432/nifi</property>
        <property name="Database User">nifi</property>
    </userGroupProvider>
    -->
    <accessPolicyProvider>
        <identifier>file-access-policy-provider</identifier>
        <class>org.apache.nifi.authorization.FileAccessPolicyProvider</class>
        <property name="User Group Provider">file-user-group-provider</property>
    </accessPolicyProvider>
    <authorizer>
        <identifier>managed-authorizer</identifier>
        <class>org.apache.nifi.authorization.StandardManagedAuthorizer</class>
    </authorizer>
</authorizers>
"""


def load(tmp_path, text=AUTHORIZERS):
    path = tmp_path / "authorizers.xml"
    path.write_text(text, encoding="utf-8")
    return XmlConfig.load(path)


def order_of(xml: XmlConfig) -> list[str]:
    """살아 있는 요소의 태그 순서."""
    return [c.tag for c in list(xml._root) if isinstance(c.tag, str)]


def test_주석_상태를_인식한다(tmp_path):
    xml = load(tmp_path)
    assert xml.is_commented("db-user-group-provider")
    assert not xml.is_commented("file-user-group-provider")
    # 주석 안에 있으므로 provider 목록에는 보이지 않는다
    assert "db-user-group-provider" not in [p.identifier for p in xml.providers()]


def test_활성화하면_provider_로_보이고_편집할_수_있다(tmp_path):
    xml = load(tmp_path)

    assert xml.activate_provider("db-user-group-provider") is True

    ids = [p.identifier for p in xml.providers()]
    assert "db-user-group-provider" in ids
    assert xml.get_property("db-user-group-provider", "Database User") == "nifi"

    # 활성화 전에는 불가능했던 편집이 가능해야 한다
    xml.set_property("db-user-group-provider", "Database User", "argus")
    assert xml.get_property("db-user-group-provider", "Database User") == "argus"


def test_활성화해도_요소_순서가_유지된다(tmp_path):
    """authorizers.xml 스키마는 userGroupProvider 를 앞에 두도록 강제한다.

    끝에 덧붙이면 주석을 해제하는 순간 cvc-complex-type.2.4.a 로 기동이 실패한다.
    """
    xml = load(tmp_path)
    xml.activate_provider("db-user-group-provider")

    tags = order_of(xml)
    assert tags == [
        "userGroupProvider",        # file-user-group-provider
        "userGroupProvider",        # db-user-group-provider (주석이 있던 자리)
        "accessPolicyProvider",
        "authorizer",
    ], tags
    assert tags.index("accessPolicyProvider") > 1


def test_활성화_후에도_설명_주석이_남는다(tmp_path):
    xml = load(tmp_path)
    xml.activate_provider("db-user-group-provider")
    assert "설명 주석은 활성화 후에도 남아야 한다" in xml.dumps()


def test_저장한_결과가_다시_읽힌다(tmp_path):
    xml = load(tmp_path)
    xml.activate_provider("db-user-group-provider")
    xml.set_property("db-user-group-provider", "Database URL", "jdbc:postgresql://db:5432/x")
    out = tmp_path / "out.xml"
    xml.save(out)

    ET.parse(out)                                    # well-formed 여야 한다
    again = XmlConfig.load(out)
    assert again.get_property("db-user-group-provider", "Database URL") == "jdbc:postgresql://db:5432/x"
    assert order_of(again).index("accessPolicyProvider") == 2


def test_이미_활성이면_아무것도_하지_않는다(tmp_path):
    xml = load(tmp_path)
    assert xml.activate_provider("file-user-group-provider") is False


def test_없는_provider_는_KeyError(tmp_path):
    xml = load(tmp_path)
    with pytest.raises(KeyError):
        xml.activate_provider("없는-프로바이더")


def test_주석에_provider_가_여러개면_거부한다(tmp_path):
    """모호한 대상을 임의로 고르면 사용자가 의도하지 않은 블록이 켜진다."""
    text = AUTHORIZERS.replace(
        "    </userGroupProvider>\n    -->",
        "    </userGroupProvider>\n"
        "    <userGroupProvider>\n"
        "        <identifier>other-provider</identifier>\n"
        "        <class>X</class>\n"
        "    </userGroupProvider>\n    -->",
    )
    xml = load(tmp_path, text)
    with pytest.raises(ValueError):
        xml.activate_provider("db-user-group-provider")


def test_비활성화로_되돌릴_수_있다(tmp_path):
    xml = load(tmp_path)
    xml.activate_provider("db-user-group-provider")

    assert xml.deactivate_provider("db-user-group-provider") is True
    assert "db-user-group-provider" not in [p.identifier for p in xml.providers()]
    assert xml.is_commented("db-user-group-provider")

    # 되돌린 뒤에도 다시 켤 수 있어야 한다
    assert xml.activate_provider("db-user-group-provider") is True
    assert xml.get_property("db-user-group-provider", "Database User") == "nifi"


def test_활성이_아닌_provider_비활성화는_False(tmp_path):
    xml = load(tmp_path)
    assert xml.deactivate_provider("db-user-group-provider") is False
