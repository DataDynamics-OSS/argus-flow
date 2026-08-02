"""
명령줄 진입점.

대상 지정 옵션 없이 실행하면 대화형 TUI를 띄운다. --list/--get/--set/--recipe 등
비대화형 액션을 주면 스크립트/자동화/테스트 용도로 동작한다.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import version_string
from .catalog import BY_FILENAME, PROPERTIES_FILES
from .catalog import nifi_properties, bootstrap
from .catalog.xml_recipes import BY_ID as RECIPES_BY_ID
from .session import Session

_CATALOGS = {"nifi.properties": nifi_properties.BY_KEY, "bootstrap.conf": bootstrap.BY_KEY}


def _resolve_conf_dir(args) -> Path:
    if args.nifi_home:
        return Path(args.nifi_home) / "conf"
    if args.conf_dir:
        return Path(args.conf_dir)
    raise SystemExit("오류: --nifi-home 또는 --conf-dir 로 conf 위치를 지정하세요.")


def _make_session(args) -> Session:
    conf_dir = _resolve_conf_dir(args)
    if not conf_dir.exists():
        raise SystemExit(f"오류: conf 디렉터리가 없습니다: {conf_dir}")
    out_dir = Path(args.out) if args.out else conf_dir
    return Session(conf_dir, out_dir)


def _print_diff(session: Session) -> None:
    for fn in session.touched():
        pf = session._props.get(fn)
        print(f"\n# {fn}")
        if pf is not None:
            for key, old, new in pf.changes():
                old_s = "(없음)" if old is None else repr(old)
                print(f"  {key}: {old_s} -> {new!r}")
        else:
            print("  (XML provider/property 변경됨)")


# ---- 액션 ---------------------------------------------------------------
def _cmd_list(args) -> int:
    files = [BY_FILENAME[args.file]] if args.file else PROPERTIES_FILES
    session = None
    try:
        session = _make_session(args)
    except SystemExit:
        pass  # conf 미지정 시 카탈로그만 출력
    for cf in files:
        if cf.kind != "properties":
            continue
        print(f"\n=== {cf.filename} — {cf.title} ===")
        for group in cf.groups:
            print(f"\n[{group}]")
            for s in (x for x in cf.settings if x.group == group):
                cur = ""
                if session is not None:
                    val = session.props(cf.filename).get(s.key)
                    if val is not None:
                        cur = f"  (현재: {s.display_value(val)})"
                print(f"  {s.key}\n      {s.label} · {s.type.value}{cur}")
    return 0


def _cmd_describe(args) -> int:
    for cat in _CATALOGS.values():
        s = cat.get(args.describe)
        if s:
            print(f"{s.key}\n  이름: {s.label}\n  그룹: {s.group}\n  타입: {s.type.value}")
            if s.choices:
                print(f"  허용값: {', '.join(s.choices)}")
            if s.default:
                print(f"  기본값: {s.default}")
            if s.help:
                print(f"  설명: {s.help}")
            return 0
    print(f"카탈로그에 '{args.describe}' 키가 없습니다.", file=sys.stderr)
    return 1


def _cmd_get(args) -> int:
    session = _make_session(args)
    val = session.props(args.file or "nifi.properties").get(args.get)
    if val is None:
        print(f"(미설정: {args.get})")
        return 1
    print(val)
    return 0


def _cmd_search(args) -> int:
    session = _make_session(args)
    term = args.search.lower()
    pf = session.props(args.file or "nifi.properties")
    for key, val in pf.items():
        if term in key.lower():
            print(f"{key}={val}")
    return 0


def _cmd_set(args) -> int:
    session = _make_session(args)
    filename = args.file or "nifi.properties"
    cf = BY_FILENAME.get(filename)
    if cf is None or cf.kind != "properties":
        raise SystemExit(f"--set 은 properties 파일에만 사용합니다 (대상: {filename})")
    pf = session.props(filename)
    cat = _CATALOGS.get(filename, {})
    for pair in args.set:
        if "=" not in pair:
            raise SystemExit(f"--set 형식은 key=value 입니다: {pair!r}")
        key, value = pair.split("=", 1)
        key = key.strip()
        s = cat.get(key)
        if s is not None:
            err = s.validate(value)
            if err:
                raise SystemExit(f"'{key}' 값 오류: {err}")
        pf.set(key, value)
    _print_diff(session)
    written = session.save_all(dry_run=args.dry_run)
    _report_written(written, args.dry_run)
    return 0


def _cmd_recipe(args) -> int:
    recipe = RECIPES_BY_ID.get(args.recipe)
    if recipe is None:
        raise SystemExit(f"알 수 없는 레시피: {args.recipe}\n"
                         f"사용 가능: {', '.join(RECIPES_BY_ID)}")
    session = _make_session(args)
    values = {}
    for pair in args.param or []:
        k, _, v = pair.partition("=")
        values[k.strip()] = v
    for p in recipe.params:
        if p.key not in values:
            if p.default:
                values[p.key] = p.default
            elif not p.sensitive and not p.optional:
                raise SystemExit(
                    f"레시피 '{recipe.id}' 에 --param {p.key}=<값> 이 필요합니다"
                )
        err = p.validate(values.get(p.key, ""))
        if err:
            raise SystemExit(f"'{p.key}' 값 오류: {err}")
    xml = session.xml(recipe.file) if recipe.file else None
    props = session.props("nifi.properties")
    notes = recipe.apply(xml, values, props, session)
    _print_diff(session)
    for n in notes:
        print(f"  * {n}")
    # 비대화형에서는 외부 명령을 실행하지 않는다. CI 가 인증서를 의도치 않게 재발급하는
    # 사고를 막기 위해서다. 실행이 필요하면 사용자가 이 명령을 직접 스크립트에 넣는다.
    for desc, argv in session.pending_commands:
        print(f"\n  다음 명령을 직접 실행하십시오 — {desc}:")
        print("    " + " ".join(argv))
    written = session.save_all(dry_run=args.dry_run)
    _report_written(written, args.dry_run)
    return 0


def _report_written(written, dry_run: bool) -> None:
    if not written:
        print("\n변경사항이 없습니다.")
        return
    verb = "저장 예정" if dry_run else "저장됨"
    print(f"\n{verb}:")
    for fn, path in written:
        print(f"  {fn} -> {path}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="argus-nifi-config",
        description="NiFi conf/ 디렉터리 대화형/비대화형 설정 도구",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {version_string()}")
    # 대상
    p.add_argument("--nifi-home", help="NiFi 설치 루트 (conf 는 <home>/conf)")
    p.add_argument("--conf-dir", help="conf 디렉터리 직접 지정")
    p.add_argument("--out", help="출력 디렉터리 (기본: in-place 편집)")
    p.add_argument("--file", help="대상 파일명 (기본 액션별 상이)")
    p.add_argument("--dry-run", action="store_true", help="저장하지 않고 diff만 출력")
    # 액션 (택1; 없으면 TUI)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--list", action="store_true", help="카탈로그(+현재값) 나열")
    g.add_argument("--describe", metavar="KEY", help="키 메타데이터 설명")
    g.add_argument("--get", metavar="KEY", help="현재 값 출력")
    g.add_argument("--search", metavar="TERM", help="키 검색(파일 내)")
    g.add_argument("--set", metavar="KEY=VALUE", action="append", help="값 설정(반복 가능)")
    g.add_argument("--recipe", metavar="ID", help="XML 레시피 적용")
    g.add_argument("--check", action="store_true",
                   help="설정 점검(TLS·로그인 프로바이더 정합성). 변경하지 않는다")
    p.add_argument("--param", metavar="K=V", action="append", help="레시피 파라미터(반복)")
    return p



def _cmd_check(args) -> int:
    """설정 점검. 변경하지 않는다.

    종료 코드: 0 = 문제 없음, 1 = FAIL 항목 있음. 경고만 있으면 0 이다 —
    스크립트가 경고 때문에 실패하면 무시하게 되기 때문이다.
    """
    from .checks import FAIL, OK, SKIP, WARN, run_checks

    session = _make_session(args)
    mark = {OK: "[ok]  ", WARN: "[warn]", FAIL: "[FAIL]", SKIP: "[skip]"}
    failed = 0
    for r in run_checks(session):
        if r.level == FAIL:
            failed += 1
        print(f"{mark[r.level]} {r.title}")
        if r.detail:
            print(f"        {r.detail}")
        if r.hint:
            print(f"        → {r.hint}")
    print()
    print("문제 없음" if failed == 0 else f"확인이 필요한 항목 {failed}개")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.list:
        return _cmd_list(args)
    if args.describe:
        return _cmd_describe(args)
    if args.get:
        return _cmd_get(args)
    if args.search:
        return _cmd_search(args)
    if args.set:
        return _cmd_set(args)
    if args.check:
        return _cmd_check(args)
    if args.recipe:
        return _cmd_recipe(args)
    # 액션 없음 → 대화형 TUI
    from .tui import run_tui
    return run_tui(args)


if __name__ == "__main__":
    raise SystemExit(main())
