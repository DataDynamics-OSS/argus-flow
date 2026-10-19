MVN     ?= ./mvnw
# 제품 버전(SemVer). scripts/set-version.sh 로만 바꾼다 (scripts/check-version.sh 가 pom 과 일치 검사)
VERSION ?= 0.5.1
# 기반 Apache NiFi 버전 (루트 pom 의 nifi.version 과 같아야 함)
NIFI_VERSION ?= 2.10.0
NFPM    ?= nfpm

NIFI_IMAGE     ?= datadynamics/argus-flow-nifi
OPERATOR_IMAGE ?= datadynamics/argus-flow-operator
DIST_NAME      := argus-flow-$(VERSION)-nifi-$(NIFI_VERSION)
GIT_REVISION   := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)

.PHONY: all extensions dist rpm docker-image operator-test operator-image \
	nifi-config nifi-config-test release-artifacts check-version clean

all: extensions

## 확장 NAR 빌드 (14개 번들 → Argus NAR 24개. distribution 은 nifi-hadoop-libraries-nar 를 더해 25개 수집)
extensions:
	$(MVN) -B package

## NiFi 재패키징 tar.gz (upstream 바이너리 다운로드 + SHA-512 검증 포함) + NAR zip + DDL zip
dist:
	$(MVN) -B -Pdist package

## RPM (nfpm 필요: https://nfpm.goreleaser.com — 릴리스는 scripts/build-release.sh 가 고정 버전을 받음)
rpm: dist
	rm -rf distribution/target/rpmroot
	mkdir -p distribution/target/rpmroot
	tar xzf distribution/target/$(DIST_NAME)-bin.tar.gz -C distribution/target/rpmroot
	mv distribution/target/rpmroot/$(DIST_NAME) distribution/target/rpmroot/dist
	mv distribution/target/rpmroot/dist/conf distribution/target/rpmroot/config
	cd distribution/rpm && $(NFPM) pkg --packager rpm --target ../target/

## NAR 포함 NiFi 컨테이너 이미지 (Argus NAR 전부 + hadoop-libraries NAR)
docker-image: extensions
	rm -rf docker/nars && mkdir -p docker/nars
	cp distribution/target/nars/*.nar docker/nars/
	docker build -t $(NIFI_IMAGE):$(VERSION) \
		--build-arg NIFI_VERSION=$(NIFI_VERSION) --build-arg VERSION=$(VERSION) \
		--build-arg REVISION=$(GIT_REVISION) docker

operator-test:
	cd operator && python3 -m venv .venv && .venv/bin/pip install -q -r requirements-dev.txt && .venv/bin/python -m pytest

operator-image:
	docker build -t $(OPERATOR_IMAGE):$(VERSION) \
		--build-arg NIFI_VERSION=$(NIFI_VERSION) --build-arg VERSION=$(VERSION) \
		--build-arg REVISION=$(GIT_REVISION) operator

## 릴리스 첨부 파일 전부를 distribution/target/release/ 에 생성 (scripts/build-release.sh 참고)
release-artifacts:
	scripts/build-release.sh

## 모든 위치의 버전 일치 검사 (TAG=v0.5.0 을 주면 태그·CHANGELOG 도 검사)
check-version:
	scripts/check-version.sh $(TAG)

## NiFi conf/ 대화형 설정 CLI 실행 (CONF=<conf 디렉터리> 또는 NIFI_HOME=<설치루트>)
nifi-config:
	cd tools/nifi-config && python3 -m venv .venv && .venv/bin/pip install -q -e .
	@cd tools/nifi-config && .venv/bin/argus-nifi-config \
		$(if $(NIFI_HOME),--nifi-home $(NIFI_HOME),) $(if $(CONF),--conf-dir $(CONF),)

nifi-config-test:
	cd tools/nifi-config && python3 -m venv .venv && .venv/bin/pip install -q -e '.[dev]' && .venv/bin/python -m pytest

clean:
	$(MVN) -B clean
	rm -rf docker/nars
