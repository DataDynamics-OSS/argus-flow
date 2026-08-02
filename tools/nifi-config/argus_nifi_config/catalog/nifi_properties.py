"""
nifi.properties 큐레이션 카탈로그.

전체 202개 키 중 운영에서 자주 바꾸는 항목을 그룹별로 정리한다. 카탈로그에 없는
키도 TUI의 "전체 키 검색"에서 원본 값 그대로 편집할 수 있다(카탈로그는 안내용 UI일 뿐,
파일의 어떤 키든 편집 가능하다).
"""

from __future__ import annotations

from ..model import Setting, ValueType

# 그룹 표시 순서
GROUPS = [
    "웹/HTTP",
    "보안/TLS",
    "클러스터링",
    "상태관리/ZooKeeper",
    "저장소/리포지토리",
    "플로우/백프레셔",
    "프로버넌스",
]

_KEYSTORE_TYPES = ("PKCS12", "JKS", "BCFKS")

SETTINGS: list[Setting] = [
    # ---- 웹/HTTP ----
    Setting("nifi.web.https.host", "HTTPS 바인드 호스트", "웹/HTTP", type=ValueType.STRING,
            help="HTTPS 리스너가 바인드할 호스트. 모든 인터페이스는 비워두거나 0.0.0.0."),
    Setting("nifi.web.https.port", "HTTPS 포트", "웹/HTTP", type=ValueType.INT, default="8443",
            help="보안(HTTPS) 웹 UI 포트. TLS 활성 배포의 표준 접속 포트."),
    Setting("nifi.web.http.host", "HTTP 바인드 호스트", "웹/HTTP", type=ValueType.STRING,
            help="비보안 HTTP 리스너 호스트. TLS를 쓰면 http.host/port는 비워 둔다."),
    Setting("nifi.web.http.port", "HTTP 포트", "웹/HTTP", type=ValueType.INT,
            help="비보안 HTTP 포트. HTTPS를 쓰면 반드시 비워 비활성화한다."),
    Setting("nifi.web.proxy.host", "프록시 허용 호스트", "웹/HTTP", type=ValueType.LIST,
            help="리버스 프록시/로드밸런서 뒤에 둘 때 허용할 Host 헤더 목록(쉼표 구분). "
                 "없으면 프록시 경유 요청이 400으로 거부된다."),
    Setting("nifi.web.proxy.context.path", "프록시 컨텍스트 경로", "웹/HTTP", type=ValueType.STRING,
            help="프록시가 하위 경로로 마운트할 때(예: /nifi) 허용할 컨텍스트 경로."),
    Setting("nifi.web.max.header.size", "최대 헤더 크기", "웹/HTTP", type=ValueType.DATASIZE, default="16 KB",
            help="HTTP 요청 헤더 최대 크기. SSO 토큰이 크면 상향."),
    Setting("nifi.web.request.timeout", "웹 요청 타임아웃", "웹/HTTP", type=ValueType.DURATION, default="60 secs"),

    # ---- 보안/TLS ----
    Setting("nifi.security.keystore", "keystore 경로", "보안/TLS", type=ValueType.PATH,
            help="서버 인증서 keystore 경로(예: ./conf/keystore.p12)."),
    Setting("nifi.security.keystoreType", "keystore 타입", "보안/TLS", type=ValueType.ENUM,
            choices=_KEYSTORE_TYPES, default="PKCS12"),
    Setting("nifi.security.keystorePasswd", "keystore 비밀번호", "보안/TLS",
            type=ValueType.PASSWORD, sensitive=True),
    Setting("nifi.security.keyPasswd", "key 비밀번호", "보안/TLS", type=ValueType.PASSWORD,
            sensitive=True, help="keystore 내 개인키 비밀번호. PKCS12는 보통 keystore 비밀번호와 동일."),
    Setting("nifi.security.truststore", "truststore 경로", "보안/TLS", type=ValueType.PATH),
    Setting("nifi.security.truststoreType", "truststore 타입", "보안/TLS", type=ValueType.ENUM,
            choices=_KEYSTORE_TYPES, default="PKCS12"),
    Setting("nifi.security.truststorePasswd", "truststore 비밀번호", "보안/TLS",
            type=ValueType.PASSWORD, sensitive=True),
    Setting("nifi.sensitive.props.key", "민감 프로퍼티 암호화 키", "보안/TLS",
            type=ValueType.PASSWORD, sensitive=True,
            help="플로우 내 민감값 암호화 키(12자 이상 권장). 한번 정하면 절대 바꾸지 말 것 "
                 "— 바꾸면 기존 암호화 값 복호화 불가.",
            validator=lambda v: None if len(v) >= 12 else "12자 이상을 권장합니다"),
    Setting("nifi.sensitive.props.algorithm", "민감 프로퍼티 알고리즘", "보안/TLS",
            type=ValueType.ENUM, default="NIFI_PBKDF2_AES_GCM_256",
            choices=("NIFI_PBKDF2_AES_GCM_256", "NIFI_ARGON2_AES_GCM_256",
                     "NIFI_BCRYPT_AES_GCM_256", "NIFI_SCRYPT_AES_GCM_256")),
    Setting("nifi.security.allow.anonymous.authentication", "익명 접근 허용", "보안/TLS",
            type=ValueType.BOOL, default="false",
            help="true면 인증 없이 접근 허용. 운영에서는 반드시 false."),
    Setting("nifi.security.user.login.identity.provider", "로그인 아이덴티티 프로바이더", "보안/TLS",
            type=ValueType.STRING,
            help="login-identity-providers.xml의 identifier 참조(예: single-user-provider)."),
    Setting("nifi.security.user.authorizer", "인가자(authorizer)", "보안/TLS", type=ValueType.STRING,
            help="authorizers.xml의 authorizer identifier 참조(예: managed-authorizer)."),

    # ---- 클러스터링 ----
    Setting("nifi.cluster.is.node", "클러스터 노드 여부", "클러스터링", type=ValueType.BOOL,
            default="false", help="true면 이 인스턴스가 클러스터 노드로 동작한다."),
    Setting("nifi.cluster.node.address", "노드 주소", "클러스터링", type=ValueType.STRING,
            help="다른 노드가 접속할 이 노드의 FQDN/호스트명."),
    Setting("nifi.cluster.node.protocol.port", "노드 프로토콜 포트", "클러스터링", type=ValueType.INT,
            help="노드 간 클러스터 프로토콜 통신 포트(예: 11443)."),
    Setting("nifi.cluster.leader.election.implementation", "리더 선출 구현", "클러스터링",
            type=ValueType.ENUM, default="CuratorLeaderElectionManager",
            choices=("CuratorLeaderElectionManager", "KubernetesLeaderElectionManager"),
            help="독립 배포는 Curator(ZooKeeper), K8s 오퍼레이터 배포는 Kubernetes."),
    Setting("nifi.cluster.flow.election.max.wait.time", "플로우 선출 최대 대기", "클러스터링",
            type=ValueType.DURATION, default="5 mins"),
    Setting("nifi.cluster.flow.election.max.candidates", "플로우 선출 후보 수", "클러스터링",
            type=ValueType.INT, help="이 수만큼 노드가 투표하면 즉시 플로우 확정(대기 단축)."),
    Setting("nifi.cluster.load.balance.host", "부하분산 호스트", "클러스터링", type=ValueType.STRING),
    Setting("nifi.cluster.load.balance.port", "부하분산 포트", "클러스터링", type=ValueType.INT,
            default="6342"),

    # ---- 상태관리/ZooKeeper ----
    Setting("nifi.state.management.provider.cluster", "클러스터 상태 프로바이더", "상태관리/ZooKeeper",
            type=ValueType.STRING, default="zk-provider",
            help="state-management.xml의 cluster provider id(zk-provider 또는 kubernetes-provider)."),
    Setting("nifi.state.management.embedded.zookeeper.start", "내장 ZooKeeper 시작", "상태관리/ZooKeeper",
            type=ValueType.BOOL, default="false",
            help="true면 NiFi가 내장 ZK를 기동. 운영에서는 외부 ZK 권장(false)."),
    Setting("nifi.zookeeper.connect.string", "ZooKeeper 접속 문자열", "상태관리/ZooKeeper",
            type=ValueType.STRING, help="host1:2181,host2:2181,host3:2181 형식."),
    Setting("nifi.zookeeper.root.node", "ZooKeeper 루트 노드", "상태관리/ZooKeeper",
            type=ValueType.STRING, default="/nifi"),
    Setting("nifi.zookeeper.client.secure", "ZooKeeper TLS", "상태관리/ZooKeeper",
            type=ValueType.BOOL, default="false"),

    # ---- 저장소/리포지토리 ----
    Setting("nifi.database.directory", "데이터베이스 디렉터리", "저장소/리포지토리", type=ValueType.PATH,
            default="./database_repository"),
    Setting("nifi.flowfile.repository.directory", "FlowFile 리포지토리 경로", "저장소/리포지토리",
            type=ValueType.PATH, default="./flowfile_repository"),
    Setting("nifi.content.repository.directory.default", "Content 리포지토리 경로", "저장소/리포지토리",
            type=ValueType.PATH, default="./content_repository"),
    Setting("nifi.provenance.repository.directory.default", "Provenance 리포지토리 경로",
            "저장소/리포지토리", type=ValueType.PATH, default="./provenance_repository"),
    Setting("nifi.content.repository.archive.enabled", "Content 아카이브 사용", "저장소/리포지토리",
            type=ValueType.BOOL, default="true"),
    Setting("nifi.content.repository.archive.max.usage.percentage", "Content 아카이브 최대 사용률",
            "저장소/리포지토리", type=ValueType.STRING, default="50%",
            help="디스크 사용률이 이 값을 넘으면 오래된 아카이브부터 삭제(예: 50%)."),
    Setting("nifi.content.repository.archive.max.retention.period", "Content 아카이브 보존기간",
            "저장소/리포지토리", type=ValueType.DURATION, default="7 days"),

    # ---- 플로우/백프레셔 ----
    Setting("nifi.queue.backpressure.count", "백프레셔 객체 수 임계", "플로우/백프레셔",
            type=ValueType.INT, default="10000"),
    Setting("nifi.queue.backpressure.size", "백프레셔 용량 임계", "플로우/백프레셔",
            type=ValueType.DATASIZE, default="1 GB"),
    Setting("nifi.flow.configuration.archive.enabled", "플로우 설정 아카이브", "플로우/백프레셔",
            type=ValueType.BOOL, default="true"),
    Setting("nifi.flow.configuration.archive.max.storage", "플로우 아카이브 최대 용량",
            "플로우/백프레셔", type=ValueType.DATASIZE, default="500 MB"),
    Setting("nifi.flowcontroller.graceful.shutdown.period", "그레이스풀 종료 대기", "플로우/백프레셔",
            type=ValueType.DURATION, default="10 sec"),

    # ---- 프로버넌스 ----
    Setting("nifi.provenance.repository.max.storage.time", "Provenance 최대 보존기간",
            "프로버넌스", type=ValueType.DURATION, default="30 days"),
    Setting("nifi.provenance.repository.max.storage.size", "Provenance 최대 용량",
            "프로버넌스", type=ValueType.DATASIZE, default="10 GB"),
    Setting("nifi.provenance.repository.index.threads", "Provenance 인덱스 스레드",
            "프로버넌스", type=ValueType.INT, default="2"),
    Setting("nifi.provenance.repository.query.threads", "Provenance 쿼리 스레드",
            "프로버넌스", type=ValueType.INT, default="2"),
]

BY_KEY = {s.key: s for s in SETTINGS}
