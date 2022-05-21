.PHONY: help var/envs/*/update clean envs update run ui
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
SRC_DIR  = $(PROJECT)

RUNTIME_ENV     ?= runtime
DEVELOPMENT_ENV ?= development

CONDA  := $(shell which conda)
IN_ENV  = source $(dir $(CONDA))../etc/profile.d/conda.sh \
          && conda activate var/envs/$(RUNTIME_ENV) 1>&2 \
          && conda activate var/envs/$(DEVELOPMENT_ENV) --stack 1>&2 \
          &&

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

var/envs/%: etc/envs/%.yaml ## create environment
	@rm -fr $@
	@$(CONDA) env create --quiet --prefix $@ --file $?

var/envs/%/update: etc/envs/%.yaml ## update environment
	@$(CONDA) env update --quiet --prefix $(@D) --file $^ --prune

conda.yaml: var/envs/$(RUNTIME_ENV) ## create distribution environment configuration
	@$(CONDA) env export --quiet --prefix $? | sed 's/name: .*/name: ${PROJECT}/g' | sed '/prefix/d' > $@

clean: ## remove all objects
	@rm -fr mlruns/
	@rm -fr var/

envs: var/envs/$(RUNTIME_ENV) var/envs/$(DEVELOPMENT_ENV) ## create environments

update: var/envs/$(RUNTIME_ENV)/update var/envs/$(DEVELOPMENT_ENV)/update ## update environments

run: envs conda.yaml ## run MLFlow project
	$(IN_ENV) mlflow run $(PWD)

ui: envs ## run MLFlow UI
	@$(IN_ENV) mlflow ui
