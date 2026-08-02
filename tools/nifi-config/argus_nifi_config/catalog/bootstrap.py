"""
bootstrap.conf 큐레이션 카탈로그.

JVM 힙(java.arg.2/3)은 값에 -Xms/-Xmx 접두가 포함된 형태로 저장되므로, 입력 시
접두까지 포함해 적는다(예: -Xmx4g). 헬프에 명시한다.
"""

from __future__ import annotations

from ..model import Setting, ValueType

GROUPS = ["JVM/메모리", "프로세스"]


def _jvm_arg(v: str) -> str | None:
    if v and not v.startswith("-"):
        return "JVM 인자는 '-'로 시작해야 합니다 (예: -Xmx4g)"
    return None


SETTINGS: list[Setting] = [
    Setting("java.arg.2", "JVM 최소 힙 (-Xms)", "JVM/메모리", type=ValueType.STRING,
            default="-Xms1g", validator=_jvm_arg,
            help="초기 힙 크기. -Xms 접두 포함해서 입력 (예: -Xms2g)."),
    Setting("java.arg.3", "JVM 최대 힙 (-Xmx)", "JVM/메모리", type=ValueType.STRING,
            default="-Xmx1g", validator=_jvm_arg,
            help="최대 힙 크기. -Xmx 접두 포함해서 입력 (예: -Xmx4g). 보통 -Xms와 동일하게."),
    Setting("graceful.shutdown.seconds", "그레이스풀 종료 대기(초)", "프로세스",
            type=ValueType.INT, default="20",
            help="이 시간 내 종료되지 않으면 강제 종료."),
    Setting("preserve.environment", "환경변수 전달", "프로세스", type=ValueType.BOOL,
            default="false", help="true면 부트스트랩 환경변수를 NiFi JVM에 전달."),
    Setting("run.as", "실행 사용자", "프로세스", type=ValueType.STRING,
            help="NiFi를 실행할 OS 사용자(비우면 부트스트랩 실행 사용자). Linux 전용."),
]

BY_KEY = {s.key: s for s in SETTINGS}
