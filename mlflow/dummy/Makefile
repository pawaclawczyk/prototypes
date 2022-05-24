.PHONY: help .env/* mlflow/* test clean
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

SHELL := bash

PROJECT := dummy
SRC_DIR := $(PROJECT)

__conda_bin := $(shell which conda)
ifeq ($(findstring '.asdf/shims/conda', $(__conda_bin)),)
	__conda_bin := $(shell asdf which conda)
endif
__conda_home   = $(abspath $(dir $(__conda_bin))/../)
__source_conda = source $(__conda_home)/etc/profile.d/conda.sh &&
__conda_env    = $(__source_conda) conda activate .venv/ &&

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

.venv: etc/conda.yaml etc/requirements.txt ## install dependencies
	conda env create --force --file $(word 1,$^) --prefix $@
	touch $@

.venv/dev: .venv etc/requirements.dev.txt ## install development dependencies
	$(__conda_env) python -m pip install -U -r $(word 2,$^)
	touch $(word 1,$^)

conda.yaml: .venv etc/conda.yaml etc/requirements.txt ## dump conda.yaml environment configuration file
	#conda env create --force --file $(word 2,$^) --prefix $(word 1,$^)
	$(__conda_env) python -m pip uninstall --yes -r etc/requirements.dev.txt
	conda env export --prefix $(word 1,$^) | yq '.name|="$(PROJECT)"' | yq 'del(.prefix)' > $@

mlflow/run: conda.yaml ## run MLFlow project
	$(__conda_env) mlflow run .

mlflow/ui: .venv/dev ## run MLFlow UI
	$(__conda_env) mlflow ui

test: .venv/dev ## run all tests
	$(__conda_env) python -m pytest -v $(SRC_DIR)

clean: ## remove all artifacts
	rm -fr .venv/
	rm -fr mlruns/
