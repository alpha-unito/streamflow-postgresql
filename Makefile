codespell:
	codespell -w $(shell git ls-files)

codespell-check:
	codespell $(shell git ls-files)

coverage.xml: testcov
	coverage xml

coverage-report: testcov
	coverage report

flake8:
	flake8 streamflow tests

format:
	isort streamflow tests
	black streamflow tests

format-check:
	isort --check-only streamflow tests
	black --diff --check streamflow tests

pyupgrade:
	pyupgrade --py3-only --py310-plus $(shell git ls-files | grep .py)

test:
	python -m pytest -rs ${PYTEST_EXTRA}

testcov:
	python -m pytest -rs --cov --cov-report= ${PYTEST_EXTRA}