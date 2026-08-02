"""
대화형 TUI (questionary + rich).

메인 메뉴 → 가이드 마법사 / 전체 키 검색 / 파일별 편집 / XML 레시피 / 저장.
값 입력은 타입별 편집기(bool 확인, enum 선택, 검증 텍스트)로 처리한다.
"""

from __future__ import annotations

from pathlib import Path

import questionary
from questionary import Choice
from rich.console import Console
from rich.table import Table

from .catalog import (
    ALL_FILES, PROPERTIES_FILES, XML_FILES, BY_FILENAME, recipes_for,
)
from .catalog import nifi_properties, bootstrap
from .model import Setting, ValueType
from .session import Session

console = Console()
_CATALOGS = {"nifi.properties": nifi_properties.BY_KEY, "bootstrap.conf": bootstrap.BY_KEY}


# ---- 값 편집기 ----------------------------------------------------------
def edit_value(setting: Setting, current: str | None) -> str | None:
    """설정 하나의 값을 입력받는다. 취소 시 None."""
    cur = current if current is not None else setting.default
    console.print(f"[bold cyan]{setting.label}[/]  [dim]({setting.key})[/]")
    if setting.help:
        console.print(f"  [dim]{setting.help}[/]")
    if current is not None:
        console.print(f"  [dim]현재값: {setting.display_value(current)}[/]")

    if setting.type == ValueType.BOOL:
        ans = questionary.select(
            "값", choices=["true", "false", "(변경 안 함)"],
            default=cur if cur in ("true", "false") else "false",
        ).ask()
    elif setting.type == ValueType.ENUM and setting.choices:
        ans = questionary.select(
            "값", choices=list(setting.choices) + ["(변경 안 함)"],
            default=cur if cur in setting.choices else setting.choices[0],
        ).ask()
    elif setting.sensitive:
        ans = questionary.password("값 (비우면 변경 안 함)").ask()
        if ans == "":
            return None
    else:
        ans = questionary.text(
            "값", default=cur or "",
            validate=lambda v: (setting.validate(v) or True),
        ).ask()

    if ans is None or ans == "(변경 안 함)":
        return None
    return ans


# ---- 마법사 / 편집 흐름 --------------------------------------------------
def wizard(session: Session) -> None:
    cf = _pick_properties_file()
    if cf is None:
        return
    group = questionary.select(
        f"{cf.filename} — 그룹 선택", choices=cf.groups + ["← 뒤로"]
    ).ask()
    if group in (None, "← 뒤로"):
        return
    pf = session.props(cf.filename)
    for s in (x for x in cf.settings if x.group == group):
        new = edit_value(s, pf.get(s.key))
        if new is not None:
            pf.set(s.key, new)
            console.print(f"  [green]✓ {s.key} = {s.display_value(new)}[/]\n")


def search_edit(session: Session) -> None:
    cf = _pick_properties_file()
    if cf is None:
        return
    pf = session.props(cf.filename)
    term = questionary.text("검색어 (키 일부)").ask()
    if not term:
        return
    matches = [k for k in pf.keys() if term.lower() in k.lower()]
    # 카탈로그에만 있고 파일엔 없는 키도 후보에 포함
    for k in _CATALOGS.get(cf.filename, {}):
        if term.lower() in k.lower() and k not in matches:
            matches.append(k)
    if not matches:
        console.print("[yellow]일치하는 키가 없습니다.[/]")
        return
    key = questionary.select("편집할 키", choices=matches[:40] + ["← 취소"]).ask()
    if key in (None, "← 취소"):
        return
    s = _CATALOGS.get(cf.filename, {}).get(key) or Setting(key, key, "기타")
    new = edit_value(s, pf.get(key))
    if new is not None:
        pf.set(key, new)
        console.print(f"  [green]✓ {key} = {s.display_value(new)}[/]")


def xml_recipe_flow(session: Session) -> None:
    all_recipes = [r for cf in XML_FILES for r in recipes_for(cf.filename)]
    choice = questionary.select(
        "적용할 레시피",
        choices=[Choice(f"{r.title}  [{r.file}]", value=r.id) for r in all_recipes]
        + ["← 뒤로"],
    ).ask()
    if choice in (None, "← 뒤로"):
        return
    from .catalog.xml_recipes import BY_ID
    recipe = BY_ID[choice]
    console.print(f"[dim]{recipe.help}[/]")
    values = {}
    for p in recipe.params:
        v = edit_value(p, None)
        if v is not None:
            values[p.key] = v
    try:
        xml = session.xml(recipe.file)
    except FileNotFoundError as e:
        console.print(f"[red]{e}[/]")
        return
    notes = recipe.apply(xml, values, session.props("nifi.properties"))
    for n in notes:
        console.print(f"  [yellow]* {n}[/]")


def xml_property_edit(session: Session) -> None:
    cf = questionary.select(
        "XML 파일", choices=[Choice(f"{c.filename} — {c.title}", value=c.filename)
                            for c in XML_FILES] + ["← 뒤로"]
    ).ask()
    if cf in (None, "← 뒤로"):
        return
    try:
        xml = session.xml(cf)
    except FileNotFoundError as e:
        console.print(f"[red]{e}[/]")
        return
    provs = xml.providers()
    pid = questionary.select(
        "provider", choices=[Choice(f"{p.identifier}  [{p.tag}]", value=p.identifier)
                             for p in provs] + ["← 뒤로"]
    ).ask()
    if pid in (None, "← 뒤로"):
        return
    prov = next(p for p in provs if p.identifier == pid)
    if not prov.properties:
        console.print("[yellow]이 provider에는 편집 가능한 property가 없습니다.[/]")
        return
    pname = questionary.select(
        "property", choices=list(prov.properties.keys()) + ["← 뒤로"]
    ).ask()
    if pname in (None, "← 뒤로"):
        return
    s = Setting(pname, pname, "xml")
    new = edit_value(s, prov.properties.get(pname))
    if new is not None:
        xml.set_property(pid, pname, new)
        console.print(f"  [green]✓ {pid} / {pname} = {new}[/]")


# ---- 저장/미리보기 ------------------------------------------------------
def preview_and_save(session: Session) -> None:
    touched = session.touched()
    if not touched:
        console.print("[yellow]변경사항이 없습니다.[/]")
        return
    table = Table(title="변경 미리보기", show_lines=False)
    table.add_column("파일", style="cyan")
    table.add_column("키")
    table.add_column("이전", style="dim")
    table.add_column("이후", style="green")
    for fn in touched:
        pf = session._props.get(fn)
        if pf is not None:
            for key, old, new in pf.changes():
                s = _CATALOGS.get(fn, {}).get(key)
                shown_old = "(없음)" if old is None else (s.display_value(old) if s else old)
                shown_new = s.display_value(new) if s else new
                table.add_row(fn, key, shown_old, shown_new)
        else:
            table.add_row(fn, "(XML provider/property)", "", "변경됨")
    console.print(table)
    console.print(f"[dim]출력 위치: {session.out_dir}[/]")
    if questionary.confirm("저장할까요?", default=True).ask():
        written = session.save_all()
        for fn, path in written:
            console.print(f"  [green]저장됨: {path}[/]")


# ---- helper -------------------------------------------------------------
def _pick_properties_file():
    choice = questionary.select(
        "파일 선택",
        choices=[Choice(f"{c.filename} — {c.title}", value=c.filename)
                 for c in PROPERTIES_FILES] + ["← 뒤로"],
    ).ask()
    if choice in (None, "← 뒤로"):
        return None
    return BY_FILENAME[choice]


def _resolve_conf_dir_interactive(args) -> Path | None:
    if args.nifi_home:
        return Path(args.nifi_home) / "conf"
    if args.conf_dir:
        return Path(args.conf_dir)
    ans = questionary.text("conf 디렉터리 경로 (또는 NiFi 설치 루트/conf)").ask()
    return Path(ans) if ans else None


def run_tui(args) -> int:
    conf_dir = _resolve_conf_dir_interactive(args)
    if conf_dir is None:
        return 1
    if not conf_dir.exists():
        console.print(f"[red]conf 디렉터리가 없습니다: {conf_dir}[/]")
        return 1
    out_dir = Path(args.out) if args.out else conf_dir
    session = Session(conf_dir, out_dir)

    console.print(f"[bold]argus-nifi-config[/] · 원본: {conf_dir} · 출력: {out_dir}\n")
    actions = {
        "🧭 가이드 마법사 (그룹별 추천 설정)": wizard,
        "🔎 전체 키 검색/편집": search_edit,
        "🧩 XML 레시피 적용": xml_recipe_flow,
        "🔧 XML provider/property 직접 편집": xml_property_edit,
        "💾 변경사항 저장 (미리보기)": preview_and_save,
    }
    while True:
        pending = len(session.touched())
        suffix = f"  [미저장 {pending}개]" if pending else ""
        choice = questionary.select(
            f"무엇을 할까요?{suffix}",
            choices=list(actions.keys()) + ["❌ 종료"],
        ).ask()
        if choice in (None, "❌ 종료"):
            if session.touched() and not questionary.confirm(
                "미저장 변경이 있습니다. 저장 없이 종료할까요?", default=False
            ).ask():
                continue
            return 0
        actions[choice](session)
