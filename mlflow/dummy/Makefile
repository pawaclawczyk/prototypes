.PHONY: help var/envs/*/update clean all
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
SRC_DIR = $(PROJECT)

RUNTIME_ENV ?= runtime

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

var/envs/%: etc/envs/%.yaml ## create environment
	@rm -fr $@
	@conda env create --quiet --prefix $@ --file $?

var/envs/%/update: etc/envs/%.yaml ## update environment
	@conda env update --quiet --prefix $(@D) --file $^ --prune

conda.yaml: var/envs/$(RUNTIME_ENV) ## create distribution environment configuration
	@conda env export --quiet --prefix $? | sed 's/name: .*/name: ${PROJECT}/g' | sed '/^prefix/d' > $@

clean: ## remove all objects
	@rm -fr var/

all: clean conda.yaml ## run all targets in the distribution workflow
