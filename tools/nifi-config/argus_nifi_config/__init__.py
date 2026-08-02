"""argus-nifi-config — NiFi conf/ 디렉터리 대화형 설정 도구."""

__version__ = "0.1.0"


def version_string() -> str:
    """`--version` 이 출력할 문자열.

    배포본(tar.gz/RPM)에 zipapp 으로 들어간 경우 빌드 스크립트가 _build_info.py 를
    심어 두므로 Argus Flow 배포본 버전을 함께 보여준다. 지원 요청 때 도구 버전만으로는
    어느 배포본인지 알 수 없다. 소스에서 pip 로 설치했다면 이 모듈이 없다.
    """
    try:
        from ._build_info import DISTRIBUTION_VERSION
    except ImportError:
        return __version__
    return f"{__version__} (Argus Flow {DISTRIBUTION_VERSION})"
