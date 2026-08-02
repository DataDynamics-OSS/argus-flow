# nifi-argus-hive-bundle

Apache Hive 3.x 연동 확장 번들. Hive/ORC 프로세서와 Hive DBCP 커넥션 풀 컨트롤러 서비스를 제공한다.

## 구성 요소

| 유형 | 이름 | 기능 |
|---|---|---|
| 프로세서 | `SelectHive3QL` / `PutHive3QL` | Hive3 SELECT / DDL·DML 실행 |
| 프로세서 | `PutHive3Streaming` | Hive3 Streaming API 적재 |
| 프로세서 | `UpdateHive3Table` | Hive3 테이블 스키마 동기화 |
| 프로세서 | `TriggerHiveMetaStoreEvent` | Hive Metastore 이벤트 트리거 |
| 프로세서 | `PutORC` | ORC 파일 기록 |
| 컨트롤러 서비스 | `Hive3ConnectionPool` | Hive3 JDBC 커넥션 풀 |
| 컨트롤러 서비스 | `VendorHive3ConnectionPool` | 벤더 Hive JDBC 드라이버용 커넥션 풀 (드라이버 별도 제공) |

---

## `VendorHive3ConnectionPool` — JDBC 드라이버 수동 적용

`VendorHive3ConnectionPool`은 벤더 Hive JDBC 드라이버(`com.cloudera.hive.jdbc.HS2Driver`)로
CDP Hive에 연결한다. **이 드라이버는 라이선스 문제로 번들·배포판에 포함하지 않는다.** CDP를
사용하는 사용자는 라이선스 문제가 없으므로 드라이버를 직접 내려받아 적용한다.

### 1. 드라이버 다운로드

- Cloudera 다운로드 포털에서 **"Cloudera JDBC Connector for Apache Hive"** 를 받는다(CDP 이용자 라이선스 무상).
- zip 압축을 풀면 `HiveJDBC42.jar`(2.6.x 최신은 단일 fat jar)가 나온다. 구버전(2.5.x)은 의존 jar가
  여러 개 동봉되므로 **전부** 사용한다.
- 드라이버 클래스명 `com.cloudera.hive.jdbc.HS2Driver` 는 서비스에 하드코딩되어 있어 별도 지정이 필요 없다.

### 2. 드라이버 적용 — 두 가지 방법

#### 방법 A — "드라이버 위치(들)" 프로퍼티 (권장)

서비스별로 드라이버를 격리 로드하며, **NiFi 재기동이 필요 없고** `lib/` 를 건드리지 않는다.

1. 내려받은 드라이버 jar를 NiFi가 접근 가능한 경로에 둔다(예: `/opt/argus-flow/drivers/HiveJDBC42.jar`).
   - 클러스터라면 **모든 노드의 동일 경로**에 두거나, 공유 스토리지/URL을 사용한다.
2. NiFi UI에서 `VendorHive3ConnectionPool` 컨트롤러 서비스의 **드라이버 위치(들)** 프로퍼티에
   jar 파일/디렉터리 경로 또는 URL을 지정한다(쉼표로 여러 개 지정 가능, 디렉터리 지정 시 하위 jar 포함).
3. 서비스를 **Enable** 한다. 지정한 jar가 이 서비스 전용 클래스로더에 로드된다.

> 동작 원리: 이 프로퍼티는 `dynamicallyModifiesClasspath` + `@RequiresInstanceClassLoading` 조합으로,
> 지정한 jar를 서비스 인스턴스 전용 클래스로더에 주입한다. 프로퍼티만 바꿔 Enable/Disable 하면 되므로
> 재기동이 필요 없다.

#### 방법 B — NiFi `lib/` 에 배치 (전역·재기동 필요)

```bash
cp HiveJDBC42.jar  $NIFI_HOME/lib/     # 구버전이면 동봉 jar 전부 복사 · 클러스터는 전 노드
$NIFI_HOME/bin/nifi.sh restart          # lib/ 는 기동 시에만 로드됨
```

`lib/` 의 jar는 시스템 클래스로더가 로드하여 모든 확장에서 보인다. 방법 A를 쓰기 어려운
환경(예: 자동화된 이미지 빌드 단계에서 드라이버를 미리 심는 경우)에 적합하다.

### 3. 커넥션 풀 설정 (NiFi UI)

- **데이터베이스 연결 URL** — CDP 배포 형태별로 상이:
  - 온프레(Kerberos): `jdbc:hive2://<host>:10000/default;AuthMech=1;KrbServiceName=hive;KrbHostFQDN=<host>;KrbRealm=<REALM>`
  - CDP Public Cloud(HTTP/SSL): `jdbc:hive2://<host>:443/default;ssl=1;transportMode=http;httpPath=cliservice`
- **데이터베이스 사용자 / 비밀번호** — 기본 인증 시.
- **Kerberos 사용자 서비스** — 커버로스 환경이면 `KerberosUserService` 컨트롤러 서비스를 연결한다
  (`hive-site.xml` 등은 **Hive 설정 리소스** 프로퍼티로 지정).

### 4. 문제 해결

| 증상 | 원인 / 조치 |
|---|---|
| `ClassNotFoundException: com.cloudera.hive.jdbc.HS2Driver` | 드라이버 jar 미적용. 방법 A는 경로/URL 확인, 방법 B는 `lib/` 배치 후 **재기동** 여부 확인 |
| 클러스터 일부 노드만 실패 | 해당 노드에 드라이버 jar 누락. 모든 노드에 동일 적용 |
| Kerberos 인증 실패 | `KerberosUserService` 미설정 또는 `hive-site.xml`의 보안 속성 누락 |
