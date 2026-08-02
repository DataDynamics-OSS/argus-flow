IMAGE_NAME ?= datadynamics/nifi-operator
IMAGE_TAG ?= latest
NAMESPACE ?= nifi-operator-system

.PHONY: help install-crds run build push deploy undeploy clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install-crds: ## Install CRDs into the cluster
	kubectl apply -f deploy/crds/

run: ## Run operator locally (development)
	kopf run nifi_operator/main.py --verbose

build: ## Build Docker image
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

push: build ## Build and push Docker image
	docker push $(IMAGE_NAME):$(IMAGE_TAG)

deploy: install-crds ## Deploy operator to cluster
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f deploy/rbac.yaml
	kubectl apply -f deploy/operator-deployment.yaml

undeploy: ## Remove operator from cluster
	kubectl delete -f deploy/operator-deployment.yaml --ignore-not-found
	kubectl delete -f deploy/rbac.yaml --ignore-not-found
	kubectl delete -f deploy/crds/ --ignore-not-found

example: ## Deploy example NiFi cluster
	kubectl create namespace nifi --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f examples/nifi-cluster-simple.yaml

clean: ## Clean up example resources
	kubectl delete -f examples/ --ignore-not-found
	kubectl delete namespace nifi --ignore-not-found

lint: ## Compile-check all operator modules
	python -m compileall -q nifi_operator/
	@echo "All files compiled successfully"
