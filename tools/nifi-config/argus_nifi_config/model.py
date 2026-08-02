"""
설정 항목(Setting) 모델과 값 타입/검증.

카탈로그(catalog/*)는 이 모듈의 Setting 리스트로 표현되며, TUI와 CLI가 공통으로
사용한다. 검증은 실패 시 사람이 읽는 메시지(str)를 반환하고, 통과하면 None을 반환한다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional


class ValueType(str, Enum):
    STRING = "string"
    INT = "int"
    BOOL = "bool"        # true / false
    ENUM = "enum"        # choices 중 하나
    DURATION = "duration"  # "30 sec", "1 min", "30 days"
    DATASIZE = "datasize"  # "1 GB", "500 MB"
    PATH = "path"        # 파일/디렉터리 경로 (형식만 검사, 존재 검사 안 함)
    LIST = "list"        # 쉼표 구분 목록
    PASSWORD = "password"  # 민감값 (표시 시 마스킹)


_DURATION_RE = re.compile(
    r"^\s*\d+(\.\d+)?\s*(nanos|millis|ms|msec|secs?|sec|s|mins?|min|m|hours?|hrs?|h|days?|d|weeks?)\s*$",
    re.IGNORECASE,
)
_DATASIZE_RE = re.compile(r"^\s*\d+(\.\d+)?\s*(B|KB|MB|GB|TB|PB)\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class Setting:
    """단일 설정 항목의 메타데이터."""

    key: str
    label: str
    group: str
    help: str = ""
    type: ValueType = ValueType.STRING
    choices: tuple[str, ...] = ()
    default: str = ""
    sensitive: bool = False
    # 비대화형(--param)에서 생략해도 되는 항목. 기본값도 없고 민감값도 아니지만
    # 정말로 선택인 경우에 쓴다(예: 번들되지 않은 JDBC 드라이버 경로).
    optional: bool = False
    # 커스텀 검증기: (value) -> 오류메시지 or None
    validator: Optional[Callable[[str], Optional[str]]] = field(
        default=None, repr=False
    )

    def validate(self, value: str) -> Optional[str]:
        """값을 검증한다. 통과 시 None, 실패 시 오류 메시지."""
        v = (value or "").strip()
        # 빈 값은 항상 허용 (키를 upstream 기본값/미설정으로 두는 것을 의미)
        if v == "":
            return None

        if self.type == ValueType.INT:
            if not re.fullmatch(r"-?\d+", v):
                return f"정수여야 합니다 (입력: {value!r})"
        elif self.type == ValueType.BOOL:
            if v.lower() not in ("true", "false"):
                return "true 또는 false 여야 합니다"
        elif self.type == ValueType.ENUM:
            if self.choices and v not in self.choices:
                return f"허용값: {', '.join(self.choices)}"
        elif self.type == ValueType.DURATION:
            if not _DURATION_RE.match(v):
                return "기간 형식이어야 합니다 (예: '30 sec', '1 min', '30 days')"
        elif self.type == ValueType.DATASIZE:
            if not _DATASIZE_RE.match(v):
                return "용량 형식이어야 합니다 (예: '1 GB', '500 MB')"

        if self.validator is not None:
            return self.validator(v)
        return None

    def display_value(self, value: str) -> str:
        """표시용 값. 민감값은 마스킹."""
        if self.sensitive and value:
            return "•" * 8
        return value
