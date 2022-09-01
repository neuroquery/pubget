.PHONY: test_all test test_plugin test_coverage test_coverage_strict test_mypy \
        test_flake8 test_pylint run_full_pipeline run_full_pipeline_neurosynth \
        compare_query_vs_pmcid_list doc format clean clean_all

test_all: test_mypy test_flake8 test_coverage_strict test test_plugin test_pylint

test:
	tox

test_coverage_strict:
	pytest --cov=nqdc --cov-report=xml --cov-report=term --cov-fail-under=100 tests
	coverage html

test_coverage:
	pytest --cov=nqdc --cov-report=xml --cov-report=term tests
	coverage html

test_mypy:
	mypy ./src/nqdc/*.py

test_flake8:
	flake8 ./src/nqdc/*.py
	flake8 tests/

test_pylint:
	pylint ./src

test_plugin:
	tox -e run_plugin
	tox -c docs/example_plugin/tox.ini

run_full_pipeline:
	python tests/run_full_pipeline.py -o /tmp/

run_full_pipeline_neurosynth:
	python tests/run_full_pipeline.py --fit_neurosynth -o /tmp/

compare_query_vs_pmcid_list:
	python tests/compare_query_vs_pmcid_list.py

doc:
	pdoc --no-search --no-show-source -d numpy -o doc_build ./src/nqdc

format:
	isort .
	black .

clean:
	rm -rf doc_build build dist htmlcov .coverage .coverage.*

clean_all: clean
	rm -rf .mypy_cache .pytest_cache .tox
