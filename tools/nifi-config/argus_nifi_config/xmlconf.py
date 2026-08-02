"""
NiFi XML 설정 파일(authorizers.xml / login-identity-providers.xml /
state-management.xml) 편집기.

세 파일 모두 "provider 블록"의 반복 구조다:

    <root>
      <providerTag>
        <identifier>id</identifier>   (state-management.xml은 <id>)
        <class>...</class>
        <property name="X">value</property>
        ...
      </providerTag>
    </root>

이 편집기는 provider를 식별자로 찾아 `<property>` 값만 제자리에서 바꾼다. 주석은
insert_comments 파서로 보존하고, 저장 시 ET.indent로 4칸 재정렬한다. 새 파일을
통째로 생성하지 않으므로 upstream의 설명 주석이 유지된다.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_ID_TAGS = ("identifier", "id")
_XML_DECL = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'


@dataclass
class Provider:
    tag: str                 # userGroupProvider / provider / cluster-provider ...
    identifier: str
    clazz: str
    properties: dict[str, str] = field(default_factory=dict)


class XmlConfig:
    def __init__(self, tree: ET.ElementTree, path: Optional[Path] = None):
        self._tree = tree
        self._root = tree.getroot()
        self.path = path
        self._dirty = False

    @classmethod
    def load(cls, path: Path) -> "XmlConfig":
        parser = ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
        tree = ET.parse(path, parser=parser)
        return cls(tree, path=path)

    # ---- provider 탐색 ------------------------------------------------
    def _provider_elements(self) -> list[ET.Element]:
        out = []
        for child in list(self._root):
            if not isinstance(child.tag, str):  # Comment/PI
                continue
            if any(child.find(t) is not None for t in _ID_TAGS):
                out.append(child)
        return out

    @staticmethod
    def _identifier(el: ET.Element) -> str:
        for t in _ID_TAGS:
            node = el.find(t)
            if node is not None and node.text:
                return node.text.strip()
        return ""

    def providers(self) -> list[Provider]:
        result = []
        for el in self._provider_elements():
            clazz = el.find("class")
            props = {
                p.get("name"): (p.text or "")
                for p in el.findall("property")
                if p.get("name")
            }
            result.append(
                Provider(
                    tag=el.tag,
                    identifier=self._identifier(el),
                    clazz=(clazz.text or "").strip() if clazz is not None else "",
                    properties=props,
                )
            )
        return result

    def _find_provider_el(self, identifier: str) -> Optional[ET.Element]:
        for el in self._provider_elements():
            if self._identifier(el) == identifier:
                return el
        return None

    # ---- 프로퍼티 읽기/쓰기 ------------------------------------------
    def get_property(self, identifier: str, name: str) -> Optional[str]:
        el = self._find_provider_el(identifier)
        if el is None:
            return None
        for p in el.findall("property"):
            if p.get("name") == name:
                return p.text or ""
        return None

    def set_property(self, identifier: str, name: str, value: str) -> None:
        """provider의 property 값을 설정한다. property가 없으면 새로 추가한다."""
        el = self._find_provider_el(identifier)
        if el is None:
            raise KeyError(f"provider '{identifier}' 를 찾을 수 없습니다")
        for p in el.findall("property"):
            if p.get("name") == name:
                p.text = value
                self._dirty = True
                return
        new = ET.SubElement(el, "property")
        new.set("name", name)
        new.text = value
        self._dirty = True

    @property
    def dirty(self) -> bool:
        return self._dirty

    # ---- 직렬화 --------------------------------------------------------
    def dumps(self) -> str:
        ET.indent(self._tree, space="    ")
        body = ET.tostring(self._root, encoding="unicode")
        return _XML_DECL + body + "\n"

    def save(self, path: Optional[Path] = None) -> None:
        target = path or self.path
        if target is None:
            raise ValueError("저장 경로가 없습니다")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.dumps(), encoding="utf-8")
