codespell:
	codespell -w $(shell git ls-files)

codespell-check:
	codespell $(shell git ls-files)

coverage.xml: testcov
	coverage xml

coverage-report: testcov
	coverage report

format:
	ruff check --fix streamflow tests
	black --target-version py310 streamflow tests

format-check:
	ruff check streamflow tests
	black --target-version py310 --diff --check streamflow tests

pyupgrade:
	pyupgrade --py3-only --py310-plus $(shell git ls-files | grep .py)

test:
	python -m pytest -rs ${PYTEST_EXTRA}

testcov:
	python -m pytest -rs --cov --cov-report= ${PYTEST_EXTRA}