.PHONY: test_all test test_mypy test_flake8 run_full_pipeline doc clean

test_all: test_mypy test_flake8 test_coverage_strict test test_pylint

test:
	tox

test_coverage_strict:
	pytest --cov=nqdc --cov-report=xml --cov-report=term --cov-fail-under=100
	coverage html

test_coverage:
	pytest --cov=nqdc --cov-report=xml --cov-report=term
	coverage html

test_mypy:
	mypy ./src/nqdc/*.py

test_flake8:
	flake8 ./src/nqdc/*.py
	flake8 tests/

test_pylint:
	pylint ./src

run_full_pipeline:
	python tests/run_full_pipeline.py -o /tmp/

doc:
	pdoc --no-search --no-show-source -d numpy -o doc_build ./src/nqdc

black:
	black src tests

clean:
	rm -rf doc_build build dist htmlcov .coverage .coverage.*
