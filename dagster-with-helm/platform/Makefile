.PHONY: deploy deploy/* workspace

SHELL := bash

PROJECT := dagster

deploy: deploy/prepare
	helm upgrade --install $(PROJECT) dagster/dagster -f values.yaml

deploy/prepare:
	helm repo add dagster https://dagster-io.github.io/helm
	helm repo update dagster
	helm search repo dagster

values.yaml:
	helm show values dagster/dagster > $@

define tpl
{'  - grpc_server:\n      host: '}{@.metadata.name}{'\n      port: 3030\n      location_name: '}{@.metadata.name}{'\n'}
endef

define patch
{"data": {"workspace.yaml": "REPLACE"}}
endef

workspace:
	@kubectl get services \
		--namespace dagster \
		-l "app.kubernetes.io/name=dagster-user-deployments,component=user-deployments" \
		-o jsonpath="{'load_from:\n'}{range .items[*]}$(tpl){end}" | awk '{printf "%s\\n", $$0}' > workspace.yaml
	@kubectl patch configmap dagster-workspace-yaml --patch '{"data": {"workspace.yaml": "$(shell cat workspace.yaml)"}}'
