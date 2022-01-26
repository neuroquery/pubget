.PHONY: test_all test test_mypy test_flake8 test_no_tox run_full_pipeline doc clean

test_all: test_mypy test_flake8 test_coverage test

test:
	tox

test_coverage:
	pytest --cov=nqdc --cov-fail-under=100
	coverage html

test_mypy:
	mypy ./src/nqdc/*.py

test_flake8:
	flake8 ./src/nqdc/*.py
	flake8 tests/

test_no_tox:
	pytest --cov=nqdc tests/

run_full_pipeline:
	python tests/run_full_pipeline -o /tmp/

doc:
	pdoc --no-search --no-show-source -d numpy -o doc_build ./src/nqdc

clean:
	rm -rf doc_build build dist htmlcov .coverage .coverage.*
