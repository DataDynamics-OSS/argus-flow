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
    from .catalog.xml_recipes import RECIPES
    all_recipes = list(RECIPES)          # file=None(인증서 등) 레시피도 포함한다
    choice = questionary.select(
        "적용할 레시피",
        choices=[Choice(f"{r.title}" + (f"  [{r.file}]" if r.file else ""), value=r.id)
                 for r in all_recipes]
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
        xml = session.xml(recipe.file) if recipe.file else None
    except FileNotFoundError as e:
        console.print(f"[red]{e}[/]")
        return
    notes = recipe.apply(xml, values, session.props("nifi.properties"), session)
    for n in notes:
        console.print(f"  [yellow]* {n}[/]")
    _run_pending(session)


def _run_pending(session: Session) -> None:
    """레시피가 남긴 외부 명령을 보여주고 확인받아 실행한다.

    설정 파일 편집과 달리 되돌리기 어려운 작업(인증서 재발급 등)이므로, 무엇이 실행되는지
    전문을 보여주고 기본값을 '아니오'로 둔다.
    """
    import subprocess

    while session.pending_commands:
        desc, argv = session.pending_commands.pop(0)
        console.print(f"\n[bold]{desc}[/] — 실행할 명령:")
        console.print(f"  [cyan]{' '.join(argv)}[/]")
        if not questionary.confirm("지금 실행할까요?", default=False).ask():
            console.print("  [dim]실행하지 않았습니다. 위 명령을 직접 실행하십시오.[/]")
            continue
        # 비밀번호가 자리표시자로 남아 있으면 여기서 받아 환경변수로 넘긴다.
        env_overrides = {}
        for i, token in enumerate(argv):
            if token.endswith("=<비밀번호>"):
                key = token.split("=", 1)[0]
                pw = questionary.password(f"{key}").ask()
                if not pw:
                    console.print("  [red]비밀번호가 없어 실행하지 않았습니다.[/]")
                    break
                env_overrides[i] = f"{key}={pw}"
        else:
            final = [env_overrides.get(i, tok) for i, tok in enumerate(argv)]
            rc = subprocess.call(final)
            console.print("  [green]완료[/]" if rc == 0 else f"  [red]실패 (종료 코드 {rc})[/]")


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
def check_flow(session: Session) -> None:
    """설정을 바꾸지 않고 현재 상태만 진단한다."""
    from .checks import FAIL, OK, SKIP, WARN, run_checks

    color = {OK: "green", WARN: "yellow", FAIL: "red", SKIP: "dim"}
    label = {OK: "정상", WARN: "주의", FAIL: "문제", SKIP: "건너뜀"}
    for r in run_checks(session):
        console.print(f"[{color[r.level]}]{label[r.level]:>4}[/] {r.title}")
        if r.detail:
            console.print(f"       [dim]{r.detail}[/]")
        if r.hint:
            console.print(f"       [cyan]→ {r.hint}[/]")


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


# ---- 처음 설정하기 (묶음 마법사) ---------------------------------------
def first_run(session: Session) -> None:
    """TLS → 로그인 → 사용자 순서를 한 번에 안내한다.

    개별 레시피를 따로 돌려도 결과는 같지만, 접속 주소를 한 번만 받아 인증서 SAN 에
    그대로 쓰는 것이 이 흐름의 핵심이다. 두 곳에 따로 입력하면 반드시 어긋나고, 그것이
    'Invalid SNI' 의 원인이다.
    """
    from .catalog.xml_recipes import BY_ID

    console.print("\n[bold]처음 설정하기[/] — 접속 주소 → TLS → 로그인 순서로 진행합니다.")
    console.print("[dim]언제든 Ctrl-C 로 빠져나올 수 있습니다. 저장 전에는 파일이 바뀌지 않습니다.[/]\n")

    # 1. 접속 주소 — 이후 단계가 모두 이 값을 쓴다
    hosts = questionary.text(
        "NiFi 에 접속할 호스트명 (쉼표 구분)",
        default=_default_hostname(),
    ).ask()
    if not hosts:
        return
    ips = questionary.text(
        "IP 로도 접속합니까? 그렇다면 IP 주소 (쉼표 구분, 없으면 비움)"
    ).ask() or ""

    props = session.props("nifi.properties")
    bind = questionary.select(
        "HTTPS 바인드 주소",
        choices=[
            Choice("모든 인터페이스 (권장)", value=""),
            Choice(f"{hosts.split(',')[0].strip()} 만", value=hosts.split(",")[0].strip()),
            Choice("localhost (이 노드에서만)", value="localhost"),
        ],
    ).ask()
    if bind is None:
        return
    props.set("nifi.web.https.host", bind)
    port = questionary.text("HTTPS 포트", default=props.get("nifi.web.https.port") or "8443").ask()
    if port:
        props.set("nifi.web.https.port", port)

    # 2. TLS — 1단계 값을 그대로 넘긴다
    if questionary.confirm("TLS 인증서를 만들까요?", default=True).ask():
        recipe = BY_ID["tls:generate"]
        values = {"hosts": hosts, "ips": ips}
        for p in recipe.params:
            if p.key in values:
                continue
            v = edit_value(p, None)
            if v is not None:
                values[p.key] = v
        for n in recipe.apply(None, values, props, session):
            console.print(f"  [yellow]* {n}[/]")

    # 3. 로그인 방식
    login = questionary.select(
        "로그인 방식",
        choices=[
            Choice("단일 사용자 (개발·단일 노드)", value="login:single-user"),
            Choice("DB 기반 (사용자를 DB 에서 관리)", value="login:db"),
            Choice("LDAP", value="login:ldap"),
            Choice("나중에 정한다", value=None),
        ],
    ).ask()
    if login:
        recipe = BY_ID[login]
        values = {}
        for p in recipe.params:
            v = edit_value(p, None)
            if v is not None:
                values[p.key] = v
        try:
            xml = session.xml(recipe.file) if recipe.file else None
        except FileNotFoundError as e:
            console.print(f"[red]{e}[/]")
            return
        for n in recipe.apply(xml, values, props, session):
            console.print(f"  [yellow]* {n}[/]")

    # 4~6. 저장 → 후속 명령 → 점검
    console.print()
    preview_and_save(session)
    _run_pending(session)
    if questionary.confirm("설정을 점검할까요?", default=True).ask():
        console.print()
        check_flow(session)


def _default_hostname() -> str:
    import socket

    try:
        return socket.getfqdn() or socket.gethostname()
    except OSError:
        return "localhost"


# ---- 사용자 관리 (argus-user.sh 위임) ----------------------------------
def user_admin(session: Session) -> None:
    """DB 인증을 쓸 때의 사용자 관리. bin/argus-user.sh 에 위임한다.

    같은 기능을 여기에 다시 구현하지 않는다 — 해시 방식이나 스키마 접근이 갈리면
    "이 도구로 만든 사용자로 로그인이 안 되는" 사고가 난다.
    """
    import subprocess

    script = _argus_user_script(session)
    if script is None:
        console.print("[yellow]bin/argus-user.sh 를 찾을 수 없습니다 "
                      "(배포본에서 실행할 때 사용할 수 있습니다).[/]")
        return

    while True:
        action = questionary.select(
            "사용자 관리",
            choices=[
                Choice("목록", value=["list"]),
                Choice("상세 보기", value=["show"]),
                Choice("사용자 추가", value=["add"]),
                Choice("비밀번호 변경", value=["passwd"]),
                Choice("잠금 해제", value=["unlock"]),
                Choice("삭제", value=["delete"]),
                Choice("스키마 적용 (NiFi 정지 상태에서)", value=["schema-init"]),
                "← 뒤로",
            ],
        ).ask()
        if action in (None, "← 뒤로"):
            return

        argv = [str(script)] + action
        needs_identity = action[0] in ("show", "add", "passwd", "unlock", "delete")
        if needs_identity:
            identity = questionary.text("identity").ask()
            if not identity:
                continue
            argv.append(identity)

        # 비밀번호는 인자로 넘기지 않는다 — ps 에 노출된다. stdin 으로 파이프한다.
        if action[0] in ("add", "passwd"):
            pw = questionary.password("비밀번호 (12자 이상)").ask()
            if not pw:
                continue
            argv.append("--password-stdin")
            proc = subprocess.run(argv, input=pw + "\n", text=True)
            rc = proc.returncode
        else:
            rc = subprocess.call(argv)
        if rc != 0:
            console.print(f"  [red]실패 (종료 코드 {rc})[/]")


def _argus_user_script(session: Session) -> Path | None:
    candidate = Path(session.conf_dir).parent / "bin" / "argus-user.sh"
    return candidate if candidate.is_file() else None


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
        "🚀 처음 설정하기 (접속 주소 → TLS → 로그인)": first_run,
        "🧭 가이드 마법사 (그룹별 추천 설정)": wizard,
        "🔎 전체 키 검색/편집": search_edit,
        "🧩 XML 레시피 적용": xml_recipe_flow,
        "🔧 XML provider/property 직접 편집": xml_property_edit,
        "🩺 설정 점검 (TLS·로그인 정합성)": check_flow,
        "👤 사용자 관리 (DB 인증)": user_admin,
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
