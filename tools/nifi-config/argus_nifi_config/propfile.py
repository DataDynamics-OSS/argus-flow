"""
주석·순서·공백을 보존하는 .properties 파일 편집기.

NiFi의 nifi.properties / bootstrap.conf는 200줄이 넘고 대부분 주석과 기본값이다.
파일을 통째로 재생성하면 upstream 기본 주석이 사라지므로, 이 편집기는 원본 라인을
그대로 보존하고 **바뀐 키의 값만 제자리에서 치환**한다. 카탈로그에 있지만 파일에
없는 키를 새로 넣으면 파일 끝의 관리 섹션에 추가한다.

지원 형식: `key=value`, `#`/`!` 주석, 빈 줄. Java properties의 백슬래시 줄바꿈
연속(line continuation)은 NiFi conf에서 쓰이지 않으므로 다루지 않고 원본 그대로 보존한다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_MANAGED_HEADER = "# ==== argus-nifi-config 관리 섹션 (아래는 도구가 추가한 키) ===="
# key = value  (구분자 앞뒤 공백 허용, 주석/빈 줄 제외)
_KV_RE = re.compile(r"^(?P<key>[^#!\s][^=:]*?)\s*[=:]\s*(?P<val>.*)$")


@dataclass
class _Line:
    raw: str
    key: Optional[str] = None
    value: Optional[str] = None


class PropertiesFile:
    """순서/주석 보존 properties 편집 모델."""

    def __init__(self, lines: list[_Line], newline: str = "\n"):
        self._lines = lines
        self._newline = newline
        self._original: dict[str, str] = {
            ln.key: ln.value for ln in lines if ln.key is not None
        }

    # ---- 로딩/저장 -----------------------------------------------------
    @classmethod
    def load(cls, path: Path) -> "PropertiesFile":
        text = path.read_text(encoding="utf-8")
        newline = "\r\n" if "\r\n" in text else "\n"
        parsed: list[_Line] = []
        for raw in text.split("\n"):
            raw = raw.rstrip("\r")
            stripped = raw.lstrip()
            if not stripped or stripped[0] in "#!":
                parsed.append(_Line(raw=raw))
                continue
            m = _KV_RE.match(raw)
            if m:
                parsed.append(
                    _Line(raw=raw, key=m.group("key").strip(), value=m.group("val"))
                )
            else:
                parsed.append(_Line(raw=raw))
        # split 으로 생긴 마지막 빈 요소 제거 (파일이 개행으로 끝날 때)
        if parsed and parsed[-1].raw == "":
            parsed.pop()
        return cls(parsed, newline=newline)

    @classmethod
    def empty(cls) -> "PropertiesFile":
        return cls([])

    def dumps(self) -> str:
        out = self._newline.join(ln.raw for ln in self._lines)
        return out + self._newline  # 파일은 개행으로 끝난다

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.dumps(), encoding="utf-8")

    # ---- 조회/수정 -----------------------------------------------------
    def get(self, key: str) -> Optional[str]:
        for ln in self._lines:
            if ln.key == key:
                return ln.value
        return None

    def keys(self) -> list[str]:
        return [ln.key for ln in self._lines if ln.key is not None]

    def items(self) -> list[tuple[str, str]]:
        return [(ln.key, ln.value) for ln in self._lines if ln.key is not None]

    def set(self, key: str, value: str) -> None:
        """키 값을 치환한다. 없으면 관리 섹션에 추가한다."""
        for ln in self._lines:
            if ln.key == key:
                ln.value = value
                ln.raw = f"{key}={value}"
                return
        self._append_managed(key, value)

    def unset(self, key: str) -> bool:
        """키 라인을 제거한다. 제거했으면 True."""
        for i, ln in enumerate(self._lines):
            if ln.key == key:
                del self._lines[i]
                return True
        return False

    def _append_managed(self, key: str, value: str) -> None:
        if not any(ln.raw == _MANAGED_HEADER for ln in self._lines):
            if self._lines and self._lines[-1].raw != "":
                self._lines.append(_Line(raw=""))
            self._lines.append(_Line(raw=_MANAGED_HEADER))
        self._lines.append(_Line(raw=f"{key}={value}", key=key, value=value))

    # ---- diff ----------------------------------------------------------
    def changes(self) -> list[tuple[str, Optional[str], str]]:
        """load 이후 변경된 (key, old, new) 목록. old=None 이면 신규 추가."""
        result: list[tuple[str, Optional[str], str]] = []
        for key, new in self.items():
            old = self._original.get(key)
            if old != new:
                result.append((key, old, new))
        return result
