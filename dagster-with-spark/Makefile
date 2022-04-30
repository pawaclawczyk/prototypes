.PHONY: help clean clean-build clean-pyc clean-data dependencies-all develop dagit
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

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-data ## remove all artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-data: ## remove created data sets
	rm -rf data/ingested

dependencies-all: ## install and upgrade all dependencies
	python -m pip install -U -r requirements/dev.txt
	python -m pip install -U -r requirements/dagster.txt
	python -m pip install -U -r requirements/spark.txt

develop: dependencies-all ## install package in editable mode for development
	python -m pip install -e .

dagit: ## run Dagit instance
	python -m dagit -f src/workflows/repositories.py
