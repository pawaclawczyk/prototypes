.PHONY: help conda.yaml/* install install/* mlflow/* test clean
.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-].*):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

SHELL := bash

PROJECT  := dummy
SRC_DIR  := $(PROJECT)
TEST_DIR := $(SRC_DIR)

BUILD_ENV_PREFIX  ?= var/envs/build
PROD_ENV_PREFIX   ?= var/envs/prod
DEV_ENV_LOCK_FILE ?= var/lock/envs/dev

__conda_bin := $(shell which conda)
ifeq ($(findstring '.asdf/shims/conda', $(__conda_bin)),)
	__conda_bin := $(shell asdf which conda)
endif
__conda_home   = $(abspath $(dir $(__conda_bin))/../)
__source_conda = source $(__conda_home)/etc/profile.d/conda.sh &&
__conda_env    = $(__source_conda) conda activate $(PROD_ENV_PREFIX)/ &&

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

conda.yaml: etc/conda.yaml etc/requirements.txt ## dump conda.yaml
	conda env create --force --prefix $(BUILD_ENV_PREFIX) --file $(firstword $^)
	conda env export --prefix $(BUILD_ENV_PREFIX) | yq '.name|="$(PROJECT)"' | yq 'del(.prefix)' > $@

conda.yaml/update: conda.yaml ## update dependencies and dump conda.yaml
	conda update $(foreach ch,$(shell yq '.channels[]' conda.yaml),--channel $(ch)) --override-channels --update-all --yes --prefix $(BUILD_ENV_PREFIX)
	conda env export --prefix $(BUILD_ENV_PREFIX) | yq '.name|="$(PROJECT)"' | yq 'del(.channels[] | select(. == "defaults"))' | yq 'del(.prefix)' > $(@D)

$(PROD_ENV_PREFIX): conda.yaml
	conda env create --force --file conda.yaml --prefix $@
	mkdir -p $(@D) && touch $@

$(DEV_ENV_LOCK_FILE): $(PROD_ENV_PREFIX) etc/requirements.dev.txt
	$(__conda_env) pip install --requirement $(lastword $^)
	mkdir -p $(@D) && touch $@

install: $(PROD_ENV_PREFIX) ## install production dependencies

install/dev: $(DEV_ENV_LOCK_FILE) ## install development dependencies (patch production environment)

mlflow/run: install ## run MLFlow project
	$(__conda_env) mlflow run .

mlflow/ui: install/dev ## run MLFlow UI
	$(__conda_env) mlflow ui

test: install/dev ## run all tests
	$(__conda_env) python -m pytest -v $(TEST_DIR)

clean: ## remove all artifacts
	rm -fr .pytest_cache/
	rm -fr $(SRC_DIR)/__pycache__
	rm -fr mlruns/
	rm -fr var/
