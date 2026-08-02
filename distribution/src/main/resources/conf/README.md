# conf 오버레이

이 디렉터리의 파일은 tar.gz 조립 시 upstream NiFi의 `conf/` 위에 덮어쓰인다.

- 여기에 두는 파일(예: `nifi.properties`)은 배포 표준 설정이다.
- 파일을 추가하면 `src/main/assembly/dist.xml`의 upstream fileSet `excludes`에
  같은 경로를 추가하여 중복을 방지하고, RPM에서 업그레이드 시 보존이 필요하면
  `rpm/nfpm.yaml`에 해당 파일의 `config|noreplace` entry를 추가할 것.
- 현재는 오버레이 파일이 없으며 upstream 기본 conf가 그대로 패키징된다
  (표준값은 운영 요구사항 확정 후 추가).

Phase 4에서 `nifi.properties` 등 표준 설정 템플릿을 확정한다.
