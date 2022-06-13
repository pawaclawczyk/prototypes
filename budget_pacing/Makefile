.PHONY:  help develop dagster/* dagit clean clean/*
.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

# User settings

## Python binary
PYTHON ?= python

## Directory where the Python virtual environments will be located
ENVS_DIR ?= var/envs

## Actual environment name to use; default: dev
ENV ?= dev

## Dagster environments
DAGSTER_HOME = var/dagster

# Project settings

## Python environments
ENV_PROD_DEPS = requirements.txt setup.py setup.cfg
ENV_DEV_DEPS  = $(ENV_PROD_DEPS) requirements.dev.txt

## Dagster
DAGSTER_REPOSITORIES = src/pacing_simulation/workflow/repository.py
DAGSTER_ENVS         = DAGSTER_HOME=$(abspath $(DAGSTER_HOME))

# Helpers

## Environment activation prefix
__env = source $(ENVS_DIR)/$(ENV)/bin/activate &&

# Main targets

help: ## print this help
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

develop: $(ENVS_DIR)/dev ## set up development environment

dagster/run: $(ENVS_DIR)/$(ENV) $(DAGSTER_HOME) ## run workflow
	$(__env) $(DAGSTER_ENVS) $(PYTHON) -m dagster job execute -f src/pacing_simulation/workflow/ops.py -c run_config.yaml

dagit: $(ENVS_DIR)/$(ENV) $(DAGSTER_HOME) ## run Dagit
	$(__env) $(DAGSTER_ENVS) $(PYTHON) -m dagit -w workspace.yaml

clean: clean/build clean/pyc clean/runtime ## remove all build, test, coverage and Python artifacts

# Helper targets

$(ENVS_DIR):
	@mkdir -p $@

$(ENVS_DIR)/prod: $(ENVS_DIR) $(ENV_PROD_DEPS)
	@$(PYTHON) -m venv $@
	$(__env) $(PYTHON) -m pip install --editable '.'
	touch $@

$(ENVS_DIR)/dev: $(ENVS_DIR) $(ENV_DEV_DEPS)
	@$(PYTHON) -m venv $@
	$(__env) $(PYTHON) -m pip install --editable '.[dev]'
	touch $@

$(ENVS_DIR)/%: $(ENVS_DIR)
	@$(PYTHON) -m venv $@

$(DAGSTER_HOME): dagster.yaml
	mkdir -p $@
	cp $^ $@
	touch $@

clean/build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean/pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean/runtime:
	rm -fr var/
	rm -fr tmp/
