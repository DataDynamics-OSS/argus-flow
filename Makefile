MVN     ?= ./mvnw
NIFI_VERSION ?= 2.10.0
REVISION ?= 1
VERSION ?= $(NIFI_VERSION)-$(REVISION)

.PHONY: all extensions dist rpm docker-image operator-test operator-image \
	nifi-config nifi-config-test clean

all: extensions

## 확장 NAR 빌드 (11개 번들 → 17개 NAR)
extensions:
	$(MVN) -B package

## NiFi 재패키징 tar.gz (upstream 바이너리 다운로드 포함)
dist:
	$(MVN) -B -Pdist package

## RPM (nfpm 필요: https://nfpm.goreleaser.com)
rpm: dist
	rm -rf distribution/target/rpmroot
	mkdir -p distribution/target/rpmroot
	tar xzf distribution/target/argus-flow-nifi-$(VERSION).tar.gz -C distribution/target/rpmroot
	mv distribution/target/rpmroot/argus-flow-nifi-$(VERSION) distribution/target/rpmroot/dist
	cd distribution/rpm && NIFI_VERSION=$(NIFI_VERSION) RELEASE=$(REVISION) \
		nfpm pkg --packager rpm --target ../target/

## NAR 포함 NiFi 컨테이너 이미지 (argus NAR + hadoop-libraries NAR)
docker-image: extensions
	rm -rf docker/nars && mkdir -p docker/nars
	cp distribution/target/nars/*.nar docker/nars/
	docker build -t argus-flow/nifi:$(VERSION) docker

operator-test:
	cd operator && python3 -m venv .venv && .venv/bin/pip install -q -r requirements-dev.txt && .venv/bin/python -m pytest

operator-image:
	docker build -t argus-flow/nifi2-operator:latest operator

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
