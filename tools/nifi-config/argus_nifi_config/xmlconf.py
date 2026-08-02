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

    # ---- 주석 처리된 provider 활성화/비활성화 --------------------------
    #
    # 배포본은 DB 인증·인가 provider 를 **주석 상태로** 내보낸다. 그래야 DB 가 준비되지
    # 않은 설치에서도 NiFi 가 기동한다. 주석 안의 provider 는 Comment 노드라
    # _provider_elements() 에 잡히지 않으므로 set_property() 로 손댈 수 없다.
    #
    # 활성화는 **원래 주석이 있던 자리에 그대로** 요소를 끼워 넣는다. 위치가 중요하다 —
    # authorizers.xml 의 스키마는 모든 userGroupProvider 가 accessPolicyProvider·
    # authorizer 보다 앞에 오도록 강제하고, 어기면 NiFi 가 기동하지 않는다.

    def _commented_provider_index(self, identifier: str) -> Optional[int]:
        """`<identifier>` 가 일치하는 provider 를 담은 주석 노드의 인덱스."""
        needle = f"<identifier>{identifier}</identifier>"
        for i, child in enumerate(list(self._root)):
            if isinstance(child.tag, str):          # 일반 요소
                continue
            text = child.text or ""
            if needle in text.replace(" ", "").replace("\n", "") or needle in text:
                return i
        return None

    def is_commented(self, identifier: str) -> bool:
        """해당 provider 가 주석 상태인지."""
        return self._commented_provider_index(identifier) is not None

    def activate_provider(self, identifier: str) -> bool:
        """주석 안의 provider 블록을 살아 있는 요소로 바꾼다.

        :return: 활성화했으면 True, 이미 활성이면 False
        :raises KeyError: 주석에도 요소에도 없는 경우
        :raises ValueError: 주석 하나에 provider 가 여러 개라 대상이 모호한 경우
        """
        if self._find_provider_el(identifier) is not None:
            return False                            # 이미 활성 — no-op

        index = self._commented_provider_index(identifier)
        if index is None:
            raise KeyError(f"provider '{identifier}' 를 찾을 수 없습니다(주석 포함)")

        comment = list(self._root)[index]
        fragment = (comment.text or "").strip()
        try:
            element = ET.fromstring(fragment)
        except ET.ParseError as e:
            # provider 가 여러 개면 루트가 둘 이상이라 파싱이 실패한다.
            raise ValueError(
                f"주석 안의 provider '{identifier}' 를 해석할 수 없습니다. "
                f"주석에 provider 가 하나만 들어 있어야 합니다: {e}"
            ) from e

        self._root.remove(comment)
        self._root.insert(index, element)           # 같은 자리에 넣는다
        self._dirty = True
        return True

    def deactivate_provider(self, identifier: str) -> bool:
        """활성 provider 를 다시 주석으로 되돌린다.

        :return: 비활성화했으면 True, 이미 주석이거나 없으면 False
        """
        el = self._find_provider_el(identifier)
        if el is None:
            return False
        children = list(self._root)
        index = children.index(el)
        # 되돌렸을 때 다시 활성화할 수 있도록 들여쓰기를 정돈해 둔다.
        ET.indent(el, space="    ", level=1)
        fragment = ET.tostring(el, encoding="unicode").rstrip()
        self._root.remove(el)
        self._root.insert(index, ET.Comment(f"\n{fragment}\n    "))
        self._dirty = True
        return True

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
