"""
XML 설정 레시피.

자주 쓰는 인증/인가/상태관리 조합을 가이드로 제공한다. 각 레시피는 파라미터를
받아 해당 XML의 provider property를 설정하고, 연동되는 nifi.properties 키까지 함께
설정한다(파일 간 정합성 유지). apply()는 사용자에게 보여줄 주의 메시지 목록을 반환한다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

from ..model import Setting, ValueType
from ..propfile import PropertiesFile
from ..xmlconf import XmlConfig

if TYPE_CHECKING:                       # 순환 임포트 방지 — 타입 힌트에만 쓴다
    from ..session import Session


@dataclass
class Recipe:
    id: str
    # 대상 XML 파일명. None 이면 XML 을 건드리지 않는 레시피(예: 인증서 생성).
    file: str | None
    title: str
    help: str
    params: list[Setting]           # 수집할 파라미터 (key=파라미터명)
    # session 을 받는 이유: DB 인증 레시피처럼 XML 두 개를 함께 고쳐야 하는 경우가 있다.
    # 파일마다 레시피를 나누면 사용자가 같은 값을 두 번 입력하게 되고, 두 파일의 값이
    # 어긋나면 "인증은 되는데 권한이 없는" 상태가 된다 — 막으려던 실패다.
    apply: Callable[[XmlConfig, dict, PropertiesFile, "Session"], list[str]] = field(repr=False)


def _p(key: str, label: str, **kw) -> Setting:
    return Setting(key, label, group="recipe", **kw)


# ---- authorizers.xml : 파일 기반 + Initial Admin --------------------------
def _apply_file_admin(xml: XmlConfig, v: dict, props: PropertiesFile, session) -> list[str]:
    admin = v["admin_identity"]
    xml.set_property("file-user-group-provider", "Initial User Identity 1", admin)
    xml.set_property("file-access-policy-provider", "Initial Admin Identity", admin)
    props.set("nifi.security.user.authorizer", "managed-authorizer")
    return [
        "nifi.security.user.authorizer=managed-authorizer 로 설정했습니다.",
        "Initial Admin 은 users.xml/authorizations.xml 가 없는 최초 기동 시에만 반영됩니다 "
        "(이미 초기화된 경우 UI에서 정책을 추가하세요).",
    ]


# ---- login-identity-providers.xml : 단일 사용자 --------------------------
def _apply_single_user(xml: XmlConfig, v: dict, props: PropertiesFile, session) -> list[str]:
    xml.set_property("single-user-provider", "Username", v["username"])
    props.set("nifi.security.user.login.identity.provider", "single-user-provider")
    return [
        "nifi.security.user.login.identity.provider=single-user-provider 로 설정했습니다.",
        "비밀번호는 이 도구가 아니라 다음 명령으로 설정해야 합니다(bcrypt 해시 필요):",
        f"  bin/nifi.sh set-single-user-credentials {v['username']} <password>",
    ]


# ---- login-identity-providers.xml : LDAP ---------------------------------
def _apply_ldap(xml: XmlConfig, v: dict, props: PropertiesFile, session) -> list[str]:
    mapping = {
        "Url": "url",
        "Manager DN": "manager_dn",
        "Manager Password": "manager_password",
        "User Search Base": "user_search_base",
        "User Search Filter": "user_search_filter",
        "Authentication Strategy": "auth_strategy",
    }
    for prop_name, param in mapping.items():
        if v.get(param):
            xml.set_property("ldap-provider", prop_name, v[param])
    props.set("nifi.security.user.login.identity.provider", "ldap-provider")
    return ["nifi.security.user.login.identity.provider=ldap-provider 로 설정했습니다."]


# ---- state-management.xml : 외부 ZooKeeper -------------------------------
def _apply_state_zk(xml: XmlConfig, v: dict, props: PropertiesFile, session) -> list[str]:
    xml.set_property("zk-provider", "Connect String", v["connect_string"])
    xml.set_property("zk-provider", "Root Node", v.get("root_node") or "/nifi")
    props.set("nifi.state.management.provider.cluster", "zk-provider")
    props.set("nifi.zookeeper.connect.string", v["connect_string"])
    props.set("nifi.zookeeper.root.node", v.get("root_node") or "/nifi")
    props.set("nifi.state.management.embedded.zookeeper.start", "false")
    return ["클러스터 상태 프로바이더를 외부 ZooKeeper(zk-provider)로 설정했습니다."]


# ---- state-management.xml : Kubernetes -----------------------------------
def _apply_state_k8s(xml: XmlConfig, v: dict, props: PropertiesFile, session) -> list[str]:
    if v.get("prefix"):
        xml.set_property("kubernetes-provider", "ConfigMap Name Prefix", v["prefix"])
    props.set("nifi.state.management.provider.cluster", "kubernetes-provider")
    return [
        "클러스터 상태 프로바이더를 kubernetes-provider로 설정했습니다.",
        "이 방식은 K8s 오퍼레이터 배포용입니다(Pod에 ConfigMap 권한 RBAC 필요).",
    ]


# ---- login-identity-providers.xml + authorizers.xml : DB 기반 인증·인가 ----
#
# 두 파일을 함께 고친다. NiFi 는 인증과 인가를 다른 SPI 로 분리하므로 한쪽만 켜면
# "비밀번호는 맞는데 권한이 없는" 상태가 된다. 값을 한 번만 받아 양쪽에 같이 써야
# 두 파일의 Database URL 이 어긋나지 않는다.
_DB_PROPS = {
    "Database URL": "db_url",
    "Database Driver Class Name": "driver_class",
    "Database Driver Location": "driver_path",
    "Database User": "db_user",
    "Database Password": "db_password",
}


def _apply_db_iaa(xml: XmlConfig, v: dict, props: PropertiesFile, session) -> list[str]:
    notes: list[str] = []

    # 배포본은 두 provider 를 주석 상태로 내보낸다(DB 미설정 환경에서도 NiFi 가 뜨도록).
    # 주석을 벗기면 원래 자리에 그대로 들어가므로 authorizers.xml 의 요소 순서 제약
    # (userGroupProvider 가 accessPolicyProvider 보다 앞)이 자동으로 지켜진다.
    try:
        authz = session.xml("authorizers.xml")
    except FileNotFoundError as e:
        raise SystemExit(f"authorizers.xml 을 찾을 수 없습니다: {e}")

    for cfg, ident in ((xml, "db-provider"), (authz, "db-user-group-provider")):
        try:
            if cfg.activate_provider(ident):
                notes.append(f"{ident} 주석을 해제했습니다.")
        except KeyError:
            raise SystemExit(
                f"'{ident}' 블록을 찾을 수 없습니다. Argus Flow 배포본의 conf/ 인지 "
                f"확인하십시오(업스트림 NiFi 에는 이 블록이 없습니다)."
            )
        for prop_name, param in _DB_PROPS.items():
            if v.get(param):
                cfg.set_property(ident, prop_name, v[param])

    # 인증 전용 설정
    for prop_name, param in (("Authentication Expiration", "expiration"),
                             ("Max Failed Attempts", "max_failed"),
                             ("Lockout Duration", "lockout")):
        if v.get(param):
            xml.set_property("db-provider", prop_name, v[param])

    # 인가 전용 설정
    if v.get("cache_duration"):
        authz.set_property("db-user-group-provider", "Cache Duration", v["cache_duration"])
    if v.get("initial_admin"):
        authz.set_property("db-user-group-provider", "Initial User Identity 1", v["initial_admin"])

    # 정책은 파일에 두고 사용자만 DB 로 옮긴다.
    authz.set_property("file-access-policy-provider", "User Group Provider",
                       "db-user-group-provider")
    if v.get("initial_admin"):
        authz.set_property("file-access-policy-provider", "Initial Admin Identity",
                           v["initial_admin"])

    props.set("nifi.security.user.login.identity.provider", "db-provider")
    props.set("nifi.security.user.authorizer", "managed-authorizer")

    notes += [
        "인증=db-provider, 인가=managed-authorizer(사용자는 db-user-group-provider)로 설정했습니다.",
        "접근 정책은 file-access-policy-provider 에 그대로 둡니다.",
        "저장한 뒤 아래 순서로 실행하십시오(도구가 authorizers.xml 을 읽습니다):",
        "  bin/argus-user.sh schema-init      # NiFi 정지 상태에서",
        f"  bin/argus-user.sh add {v.get('initial_admin') or '<관리자>'}",
    ]
    if (v.get("db_password") or "").startswith("${"):
        notes.append(
            "비밀번호를 환경변수 참조로 저장했습니다. NiFi 프로세스 환경에 해당 변수가 "
            "없으면 기동에 실패합니다(conf/bootstrap.conf 또는 systemd unit 에 설정)."
        )
    if v.get("driver_path"):
        notes.append(
            "MariaDB 드라이버는 배포본에 포함되지 않습니다(LGPL-2.1). 지정한 경로에 "
            "jar 이 있는지 확인하십시오."
        )
    return notes


# ---- TLS 인증서 생성 + nifi.properties 연동 (XML 없음) --------------------
#
# 인증서 생성 로직을 여기에 다시 구현하지 않는다. bin/argus-ssl.sh 를 그대로 호출한다 —
# 로직이 두 곳에 생기면 반드시 갈라진다. 실행은 session.pending_commands 에 쌓아 두고
# 호출자가 결정한다(대화형은 확인 후 실행, 비대화형은 출력만).
def _apply_tls_generate(xml, v: dict, props: PropertiesFile, session) -> list[str]:
    import os
    from pathlib import Path as _Path

    hosts = (v.get("hosts") or "").strip()
    ips = (v.get("ips") or "").strip()
    password = v.get("password") or ""

    # nifi.properties 는 ${NIFI_HOME} 같은 변수를 확장하지 않는다. 업스트림이
    # ./conf/keystore.p12 처럼 상대 경로를 쓰고 NiFi 가 NIFI_HOME 에서 실행되므로,
    # 기본값도 상대 경로로 둔다. 절대 경로를 받으면 그대로 쓴다.
    nifi_home = os.environ.get("NIFI_HOME") or str(_Path(session.conf_dir).parent)
    ssl_home = (v.get("ssl_home") or "").strip() or "./security"
    if _Path(ssl_home).is_absolute():
        ssl_home_abs = ssl_home
    else:
        ssl_home_abs = str((_Path(nifi_home) / ssl_home).resolve())

    notes: list[str] = []

    # SAN 에 IP 가 없으면 IP 로 접속할 때 Jetty 가 400 Invalid SNI 로 거절한다.
    # 브라우저는 RFC 6066 에 따라 IP 리터럴을 SNI 로 보내지 않으므로, Host 헤더가
    # 인증서 SAN 과 대조되기 때문이다. 미리 알려주지 않으면 배포 후에 겪는다.
    if not ips:
        notes.append(
            "SAN 에 IP 를 넣지 않았습니다. IP 주소로 접속하면 'Invalid SNI' 로 거절됩니다 — "
            "호스트명으로만 접속할 계획이면 그대로 두십시오."
        )

    keystore = f"{ssl_home}/keystore.p12"
    truststore = f"{ssl_home}/truststore.p12"
    props.set("nifi.security.keystore", keystore)
    props.set("nifi.security.keystoreType", "PKCS12")
    props.set("nifi.security.truststore", truststore)
    props.set("nifi.security.truststoreType", "PKCS12")
    if password:
        props.set("nifi.security.keystorePasswd", password)
        props.set("nifi.security.keyPasswd", password)
        props.set("nifi.security.truststorePasswd", password)

    # 바인드 호스트가 localhost 면 외부에서 접속할 수 없다. 인증서를 아무리 잘 만들어도
    # 연결 자체가 되지 않으므로 함께 짚어 준다.
    if (props.get("nifi.web.https.host") or "localhost") in ("localhost", "127.0.0.1"):
        notes.append(
            "nifi.web.https.host 가 localhost 입니다. 외부에서 접속하려면 실제 주소나 "
            "빈 값(모든 인터페이스)으로 바꾸십시오."
        )

    # 스크립트에는 절대 경로를 넘긴다 — 셸은 NIFI_HOME 에서 실행된다는 보장이 없다.
    env = [f"HOSTS={hosts}"]
    if ips:
        env.append(f"IPS={ips}")
    env += [
        f"NIFI_SSL_HOME={ssl_home_abs}",
        "NIFI_SSL_PASSWORD=<비밀번호>",
        f"NIFI_SSL_CA_DAYS={v.get('ca_days') or '36500'}",
        f"NIFI_SSL_DAYS={v.get('cert_days') or '3650'}",
    ]
    session.pending_commands.append((
        "TLS 인증서 생성",
        ["env"] + env + [str(_Path(nifi_home) / "bin/argus-ssl.sh")],
    ))

    notes += [
        f"keystore/truststore 경로를 {ssl_home} (= {ssl_home_abs}) 로 설정했습니다.",
        "인증서는 아직 만들어지지 않았습니다 — 저장 후 아래 명령을 실행하십시오.",
    ]
    if (_Path(ssl_home_abs) / "ca.key").exists():
        notes.append(
            "출력 디렉터리에 기존 ca.key 가 있습니다. 덮어쓰면 같은 CA 로 재발급할 수 없어 "
            "클라이언트에 배포한 신뢰 설정을 모두 다시 배포해야 합니다."
        )
    return notes


RECIPES: list[Recipe] = [
    Recipe(
        id="authorizers:file-admin",
        file="authorizers.xml",
        title="파일 기반 인가 + Initial Admin 지정",
        help="managed-authorizer(파일 기반)로 최초 관리자 아이덴티티를 지정한다.",
        params=[_p("admin_identity", "Initial Admin 아이덴티티", type=ValueType.STRING,
                   help="클라이언트 인증서 DN 또는 로그인 사용자명 (예: CN=admin, OU=NiFi).")],
        apply=_apply_file_admin,
    ),
    Recipe(
        id="tls:generate",
        file=None,
        title="TLS 인증서 생성 + keystore 설정",
        help=("SAN 에 DNS 이름과 IP 를 넣어 인증서를 만들고 nifi.properties 의 "
              "keystore/truststore 키를 맞춘다. 실제 생성은 확인 후 실행한다."),
        params=[
            _p("hosts", "인증서 SAN 의 DNS 이름", type=ValueType.STRING,
               help="쉼표 구분. 접속에 쓸 모든 이름을 넣는다. 예: nifi1.example.com,nifi1"),
            _p("ips", "SAN 의 IP 주소(선택)", type=ValueType.STRING, optional=True,
               help="IP 로 접속할 계획이면 반드시 넣는다. 없으면 Invalid SNI 로 거절된다."),
            _p("ssl_home", "출력 디렉터리", type=ValueType.STRING, optional=True,
               help="비우면 <NIFI_HOME>/security"),
            _p("password", "keystore 비밀번호", type=ValueType.PASSWORD, sensitive=True),
            _p("ca_days", "CA 유효기간(일)", type=ValueType.INT, default="36500"),
            _p("cert_days", "서버 인증서 유효기간(일)", type=ValueType.INT, default="3650"),
        ],
        apply=_apply_tls_generate,
    ),
    Recipe(
        id="login:single-user",
        file="login-identity-providers.xml",
        title="단일 사용자 로그인",
        help="개발/단일 노드용. 사용자명만 설정하고 비밀번호는 nifi.sh로 별도 설정.",
        params=[_p("username", "사용자명", type=ValueType.STRING)],
        apply=_apply_single_user,
    ),
    Recipe(
        id="login:ldap",
        file="login-identity-providers.xml",
        title="LDAP 로그인",
        help="LDAP 서버로 사용자 인증.",
        params=[
            _p("url", "LDAP URL", type=ValueType.STRING, help="예: ldaps://ldap.example.com:636"),
            _p("auth_strategy", "인증 방식", type=ValueType.ENUM, default="LDAPS",
               choices=("ANONYMOUS", "SIMPLE", "LDAPS", "START_TLS")),
            _p("manager_dn", "Manager DN", type=ValueType.STRING),
            _p("manager_password", "Manager 비밀번호", type=ValueType.PASSWORD, sensitive=True),
            _p("user_search_base", "User Search Base", type=ValueType.STRING,
               help="예: ou=users,dc=example,dc=com"),
            _p("user_search_filter", "User Search Filter", type=ValueType.STRING,
               help="예: (uid={0})"),
        ],
        apply=_apply_ldap,
    ),
    Recipe(
        id="login:db",
        file="login-identity-providers.xml",
        title="DB 기반 로그인 (사용자·비밀번호를 DB에서 관리)",
        help=("login-identity-providers.xml 과 authorizers.xml 을 함께 설정한다. "
              "LDAP·외부 IdP 를 쓸 수 없을 때. 사용자 관리는 bin/argus-user.sh."),
        params=[
            _p("db_url", "Database URL", type=ValueType.STRING,
               default="jdbc:postgresql://localhost:5432/nifi",
               help="PostgreSQL 또는 MariaDB. 예: jdbc:postgresql://db:5432/nifi"),
            _p("db_user", "DB 사용자", type=ValueType.STRING, default="nifi"),
            _p("db_password", "DB 비밀번호", type=ValueType.PASSWORD, sensitive=True,
               help="${ARGUS_DB_IAA_PASSWORD} 처럼 환경변수 참조를 넣으면 XML 에 평문이 남지 않는다."),
            _p("driver_class", "드라이버 클래스", type=ValueType.STRING,
               default="org.postgresql.Driver",
               help="MariaDB 는 org.mariadb.jdbc.Driver"),
            _p("driver_path", "드라이버 jar 경로(선택)", type=ValueType.STRING, optional=True,
               help="PostgreSQL 은 번들되어 있어 비워 둔다. MariaDB 드라이버는 미포함이라 직접 지정."),
            _p("initial_admin", "최초 관리자 identity", type=ValueType.STRING, default="admin"),
            _p("cache_duration", "인가 조회 캐시", type=ValueType.STRING, default="1 min",
               help="0 secs 면 캐시하지 않는다. 인증은 캐시하지 않으므로 비밀번호 변경은 즉시 반영."),
            _p("expiration", "인증 만료", type=ValueType.STRING, default="12 hours"),
            _p("max_failed", "계정 잠금까지 실패 횟수", type=ValueType.INT, default="5"),
            _p("lockout", "잠금 유지 시간", type=ValueType.STRING, default="15 mins"),
        ],
        apply=_apply_db_iaa,
    ),
    Recipe(
        id="state:zookeeper",
        file="state-management.xml",
        title="외부 ZooKeeper 클러스터 상태",
        help="독립(비 K8s) 클러스터의 표준. 외부 ZK 앙상블을 상태 저장소로 사용.",
        params=[
            _p("connect_string", "ZK 접속 문자열", type=ValueType.STRING,
               help="host1:2181,host2:2181,host3:2181"),
            _p("root_node", "루트 노드", type=ValueType.STRING, default="/nifi"),
        ],
        apply=_apply_state_zk,
    ),
    Recipe(
        id="state:kubernetes",
        file="state-management.xml",
        title="Kubernetes ConfigMap 상태",
        help="K8s 오퍼레이터 배포용. ZooKeeper 없이 ConfigMap으로 클러스터 상태 관리.",
        params=[_p("prefix", "ConfigMap 이름 접두(선택)", type=ValueType.STRING, optional=True)],
        apply=_apply_state_k8s,
    ),
]

BY_ID = {r.id: r for r in RECIPES}
