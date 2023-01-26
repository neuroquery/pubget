pubget_version := $(shell cat src/pubget/_data/VERSION)

.PHONY: test_all test test_plugin test_coverage test_coverage_strict test_mypy \
        test_flake8 test_pylint run_full_pipeline run_full_pipeline_neurosynth \
        compare_query_vs_pmcid_list doc format clean clean_all

test_all: test_mypy test_flake8 test_coverage_strict test test_plugin test_pylint

test:
	tox

test_coverage_strict:
	pytest --cov=pubget --cov-report=xml --cov-report=term --cov-fail-under=100 tests
	coverage html

test_coverage:
	pytest --cov=pubget --cov-report=xml --cov-report=term tests
	coverage html

test_mypy:
	mypy ./src/pubget/*.py

test_flake8:
	flake8 ./src/pubget/*.py
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
	rm -rf doc_build/*
	PUBGET_VERSION=$$(cat src/pubget/_data/VERSION) pdoc --no-search --no-show-source -d numpy -o doc_build ./src/pubget -t docs/
	@sed --in-place 's/^\(.*pubget.*is a command-line tool for collecting.*\)$$'\
'/<p><b>This document describes pubget version $(pubget_version)<\/b><\/p>\n\1/' \
doc_build/pubget.html
	@sed --in-place '/<h1 id="pubget">/d' doc_build/pubget.html
	@sed --in-place '/<li><a href="#pubget">pubget<\/a><\/li>/d' doc_build/pubget.html
	cp pubget.svg doc_build

format:
	isort .
	black .

clean:
	rm -rf doc_build build dist htmlcov .coverage .coverage.*

clean_all: clean
	rm -rf .mypy_cache .pytest_cache .tox
