"""
편집 세션: 소스 conf 디렉터리에서 파일을 읽고, 변경분을 모아 출력 디렉터리에 저장.

- conf_dir: 읽기 원본 (실제 설치본의 conf/ 또는 upstream 기준 conf/)
- out_dir : 쓰기 대상 (기본은 conf_dir = in-place; --out 지정 시 오버레이 작성)

파일은 지연 로딩되어 캐시되므로, 여러 액션이 같은 파일을 공유 편집한다.
"""

from __future__ import annotations

from pathlib import Path
from typing import Union

from .propfile import PropertiesFile
from .xmlconf import XmlConfig


class Session:
    def __init__(self, conf_dir: Path, out_dir: Path | None = None):
        self.conf_dir = conf_dir
        self.out_dir = out_dir or conf_dir
        self._props: dict[str, PropertiesFile] = {}
        self._xml: dict[str, XmlConfig] = {}
        # 레시피가 남기는 후속 실행 명령. (설명, argv) 목록.
        #
        # 설정 파일 편집과 외부 프로세스 실행은 성격이 다르다. 인증서 생성은 되돌리기
        # 어려우므로 레시피가 직접 실행하지 않고 여기에 쌓아 두고, 호출자가 정한다:
        # 대화형은 명령을 보여주고 확인받아 실행하고, 비대화형(--recipe)은 출력만 한다.
        # CI 가 인증서를 의도치 않게 재발급하는 사고를 막기 위해서다.
        self.pending_commands: list[tuple[str, list[str]]] = []

    def props(self, filename: str) -> PropertiesFile:
        if filename not in self._props:
            src = self.conf_dir / filename
            self._props[filename] = (
                PropertiesFile.load(src) if src.exists() else PropertiesFile.empty()
            )
        return self._props[filename]

    def xml(self, filename: str) -> XmlConfig:
        if filename not in self._xml:
            src = self.conf_dir / filename
            if not src.exists():
                raise FileNotFoundError(f"{src} 가 없습니다 (XML은 원본 파일이 필요합니다)")
            self._xml[filename] = XmlConfig.load(src)
        return self._xml[filename]

    def touched(self) -> list[str]:
        """변경분이 있는 파일명 목록."""
        out = []
        for fn, pf in self._props.items():
            if pf.changes():
                out.append(fn)
        for fn, xc in self._xml.items():
            if xc.dirty:
                out.append(fn)
        return out

    def save_all(self, dry_run: bool = False) -> list[tuple[str, Path]]:
        """변경된 파일을 out_dir에 저장. (filename, written_path) 목록 반환."""
        written = []
        for fn in self.touched():
            dst = self.out_dir / fn
            obj: Union[PropertiesFile, XmlConfig] = (
                self._props.get(fn) or self._xml[fn]
            )
            if not dry_run:
                obj.save(dst)
            written.append((fn, dst))
        return written
