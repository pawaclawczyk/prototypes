dependencies-all:
	python -m pip install -U -r requirements/dev.txt
	python -m pip install -U -r requirements/dagster.txt
	python -m pip install -U -r requirements/spark.txt

develop: dependencies-all
	python -m pip install -e .
