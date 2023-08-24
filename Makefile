codespell:
	codespell -w $(shell git ls-files)

codespell-check:
	codespell $(shell git ls-files)

coverage.xml: testcov
	coverage xml

coverage-report: testcov
	coverage report

flake8:
	flake8 streamflow_postgresql tests

format:
	black streamflow_postgresql tests

format-check:
	black --diff --check streamflow_postgresql tests

pyupgrade:
	pyupgrade --py3-only --py38-plus $(shell git ls-files | grep .py)

test:
	python -m pytest -rs ${PYTEST_EXTRA}

testcov:
	python -m pytest -rs --cov --cov-report= ${PYTEST_EXTRA}