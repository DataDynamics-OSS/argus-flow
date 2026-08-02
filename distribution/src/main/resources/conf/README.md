# conf 오버레이

이 디렉터리의 파일은 tar.gz 조립 시 upstream NiFi의 `conf/` 위에 덮어쓰인다.

- 여기에 두는 파일(예: `nifi.properties`)은 배포 표준 설정이다.
- 파일을 추가하면 `src/main/assembly/dist.xml`의 upstream fileSet `excludes`에
  같은 경로를 추가하여 중복을 방지하고, RPM에서 업그레이드 시 보존이 필요하면
  `rpm/nfpm.yaml`에 해당 파일의 `config|noreplace` entry를 추가할 것.
## 현재 오버레이 파일

| 파일 | 내용 |
|---|---|
| `login-identity-providers.xml` | single-user 인증의 기본 자격증명(`admin`). **운영에서는 반드시 교체할 것** — 아래 참조 |

`nifi.properties` 등 나머지는 upstream 기본값이 그대로 패키징된다(표준값은 운영 요구사항
확정 후 추가).

## ⚠️ 기본 자격증명

`login-identity-providers.xml`의 `admin` 계정은 **공개된 기본값**이다. 설치본마다 동일하고
해시가 공개되어 있으므로 그대로 두면 누구나 로그인할 수 있다. 설치 후 NiFi를 정지한
상태에서 교체할 것:

```bash
./bin/nifi.sh set-single-user-credentials <사용자> <비밀번호>   # 비밀번호 12자 이상
```

RPM에서는 이 파일이 `config|noreplace`로 지정되어 있어 업그레이드 시 교체한 값이 보존된다.
