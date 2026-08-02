"""
XML 설정 레시피.

자주 쓰는 인증/인가/상태관리 조합을 가이드로 제공한다. 각 레시피는 파라미터를
받아 해당 XML의 provider property를 설정하고, 연동되는 nifi.properties 키까지 함께
설정한다(파일 간 정합성 유지). apply()는 사용자에게 보여줄 주의 메시지 목록을 반환한다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from ..model import Setting, ValueType
from ..propfile import PropertiesFile
from ..xmlconf import XmlConfig


@dataclass
class Recipe:
    id: str
    file: str                       # 대상 XML 파일명
    title: str
    help: str
    params: list[Setting]           # 수집할 파라미터 (key=파라미터명)
    apply: Callable[[XmlConfig, dict, PropertiesFile], list[str]] = field(repr=False)


def _p(key: str, label: str, **kw) -> Setting:
    return Setting(key, label, group="recipe", **kw)


# ---- authorizers.xml : 파일 기반 + Initial Admin --------------------------
def _apply_file_admin(xml: XmlConfig, v: dict, props: PropertiesFile) -> list[str]:
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
def _apply_single_user(xml: XmlConfig, v: dict, props: PropertiesFile) -> list[str]:
    xml.set_property("single-user-provider", "Username", v["username"])
    props.set("nifi.security.user.login.identity.provider", "single-user-provider")
    return [
        "nifi.security.user.login.identity.provider=single-user-provider 로 설정했습니다.",
        "비밀번호는 이 도구가 아니라 다음 명령으로 설정해야 합니다(bcrypt 해시 필요):",
        f"  bin/nifi.sh set-single-user-credentials {v['username']} <password>",
    ]


# ---- login-identity-providers.xml : LDAP ---------------------------------
def _apply_ldap(xml: XmlConfig, v: dict, props: PropertiesFile) -> list[str]:
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
def _apply_state_zk(xml: XmlConfig, v: dict, props: PropertiesFile) -> list[str]:
    xml.set_property("zk-provider", "Connect String", v["connect_string"])
    xml.set_property("zk-provider", "Root Node", v.get("root_node") or "/nifi")
    props.set("nifi.state.management.provider.cluster", "zk-provider")
    props.set("nifi.zookeeper.connect.string", v["connect_string"])
    props.set("nifi.zookeeper.root.node", v.get("root_node") or "/nifi")
    props.set("nifi.state.management.embedded.zookeeper.start", "false")
    return ["클러스터 상태 프로바이더를 외부 ZooKeeper(zk-provider)로 설정했습니다."]


# ---- state-management.xml : Kubernetes -----------------------------------
def _apply_state_k8s(xml: XmlConfig, v: dict, props: PropertiesFile) -> list[str]:
    if v.get("prefix"):
        xml.set_property("kubernetes-provider", "ConfigMap Name Prefix", v["prefix"])
    props.set("nifi.state.management.provider.cluster", "kubernetes-provider")
    return [
        "클러스터 상태 프로바이더를 kubernetes-provider로 설정했습니다.",
        "이 방식은 K8s 오퍼레이터 배포용입니다(Pod에 ConfigMap 권한 RBAC 필요).",
    ]


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
        params=[_p("prefix", "ConfigMap 이름 접두(선택)", type=ValueType.STRING)],
        apply=_apply_state_k8s,
    ),
]

BY_ID = {r.id: r for r in RECIPES}
