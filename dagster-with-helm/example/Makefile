.PHONY: image deploy deploy/*

SHELL := bash

PROJECT    := dagster-example
VERSION    := $(shell python -c "import __version__; print(__version__.__version__)")
REPOSITORY ?= pawaclawczyk
IMAGE      := $(REPOSITORY)/$(PROJECT):$(VERSION)

var/envs/work: requirements.txt
	python -m venv $@
	source $@/bin/activate && python -m pip install --requirement $^
	touch $@

image: Dockerfile
	docker build --build-arg PROJECT=$(PROJECT) -t $(IMAGE) .
	docker push $(IMAGE)

deploy: deploy/prepare
	helm upgrade --install $(PROJECT) dagster/dagster-user-deployments -f values.yaml

deploy/prepare:
	helm repo add dagster https://dagster-io.github.io/helm
	helm repo update dagster
	helm search repo dagster

values.yaml:
	helm show values dagster/dagster-user-deployments > $@
