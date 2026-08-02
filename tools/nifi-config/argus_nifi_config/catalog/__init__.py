"""
conf 파일 레지스트리.

파일명 → (종류, 카탈로그) 매핑. 종류가 'properties'면 key=value 편집,
'xml'이면 provider/property 편집 + 레시피.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..model import Setting
from . import bootstrap, nifi_properties, xml_recipes


@dataclass
class ConfFile:
    filename: str
    kind: str                     # "properties" | "xml"
    title: str
    settings: list[Setting]       # properties용 큐레이션 (xml은 빈 리스트)
    groups: list[str]


PROPERTIES_FILES = [
    ConfFile("nifi.properties", "properties", "NiFi 핵심 설정",
             nifi_properties.SETTINGS, nifi_properties.GROUPS),
    ConfFile("bootstrap.conf", "properties", "부트스트랩/JVM",
             bootstrap.SETTINGS, bootstrap.GROUPS),
]

XML_FILES = [
    ConfFile("authorizers.xml", "xml", "인가(Authorizers)", [], []),
    ConfFile("login-identity-providers.xml", "xml", "로그인 아이덴티티", [], []),
    ConfFile("state-management.xml", "xml", "상태 관리", [], []),
]

ALL_FILES = PROPERTIES_FILES + XML_FILES
BY_FILENAME = {c.filename: c for c in ALL_FILES}


def recipes_for(filename: str):
    return [r for r in xml_recipes.RECIPES if r.file == filename]
