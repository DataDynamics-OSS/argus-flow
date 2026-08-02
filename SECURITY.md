# 보안 정책

## 취약점 제보

**취약점은 공개 이슈로 올리지 말아 주십시오.**

GitHub의 비공개 취약점 제보 기능을 이용해 주십시오:
저장소의 **Security** 탭 → **Report a vulnerability**.

제보에 다음을 포함해 주시면 확인이 빨라집니다.

- 영향받는 구성요소(확장 번들, 오퍼레이터, 배포 패키지, 차트 등)와 버전
- 재현 절차 또는 개념 증명
- 예상되는 영향 범위

접수 후 영업일 기준 5일 이내에 회신하며, 확인된 취약점은 수정 릴리스와 함께 공개합니다.

## 지원 버전

최신 릴리스에 대해서만 보안 수정을 제공합니다. Apache NiFi 자체의 취약점은
[Apache NiFi 보안 페이지](https://nifi.apache.org/security.html)를 따르며, 해당 수정이 포함된
NiFi 버전으로 기반을 올리는 방식으로 대응합니다.

## 운영 배포 시 반드시 교체해야 하는 항목

이 저장소의 기본값은 **개발·시연용**입니다. 운영 환경에 배포하기 전에 다음을 반드시
교체하십시오.

| 항목 | 위치 | 조치 |
|---|---|---|
| TLS 키스토어/트러스트스토어 비밀번호 | `scripts/ssl/ssl-generate.sh` | 기본값이 `ChangeMe`입니다. `NIFI_SSL_PASSWORD` 환경변수로 강한 비밀번호를 지정하십시오 |
| CA·인증서 호스트명 | `scripts/ssl/ssl-generate.sh` | 기본값 `nifi1.example.com`을 실제 호스트로 바꾸십시오 |
| **NiFi 관리자 자격증명** | `conf/login-identity-providers.xml` | 배포 패키지는 single-user 계정 **`admin`**을 기본값으로 담고 있습니다. **모든 설치본이 동일하고 해시가 공개되어 있으므로 반드시 교체하십시오.** NiFi를 정지한 뒤 `./bin/nifi.sh set-single-user-credentials <사용자> <비밀번호>` (비밀번호 12자 이상). 운영에서는 single-user 대신 OIDC/LDAP/mTLS 구성을 권장합니다 |
| 민감 속성 암호화 키 | `nifi.sensitive.props.key` (`nifi.properties`) | 설치마다 고유한 값으로 설정하십시오. 기본값 사용 시 플로우에 저장된 비밀번호가 보호되지 않습니다 |
| 오퍼레이터 RBAC 범위 | `operator/deploy/rbac.yaml`, `charts/argus-flow/templates/rbac.yaml` | 필요한 네임스페이스로 제한하십시오 |
| 데이터소스 자격증명 | NiFi 컨트롤러 서비스 | 플로우 정의에 평문으로 넣지 말고 NiFi 파라미터 컨텍스트의 민감 파라미터를 쓰십시오 |

## 추가 권고

- NiFi 웹 UI와 클러스터 내부 통신에 TLS를 활성화하십시오. 평문 HTTP로 노출하지 마십시오.
- `PutORC`·`PutParquet` 등 파일시스템 접근 프로세서는 NiFi의 **restricted** 컴포넌트입니다.
  해당 권한은 필요한 사용자에게만 부여하십시오.
- 벤더 JDBC 드라이버는 이 저장소에 포함되어 있지 않습니다. 직접 받아서 설치할 때 배포처와
  무결성을 확인하십시오.
