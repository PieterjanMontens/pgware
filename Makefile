clean: clean-build clean-pyc clean-test

clean-build:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -rf {} +

clean-test:
	rm -rf .pytest_cache
	rm -f .coverage
	rm -rf htmlcov/

version:
	pipenv run python setup.py --version

tests: flake8 pylint unittests behavetests coverage-html

flake8:
	pipenv run flake8

pylint:
	find . -name "*.py" -not -path '*/\.*' -exec pipenv run pylint --rcfile=setup.cfg '{}' +

unittests:
	PYTHONPATH=. pipenv run py.test --junitxml=./test_report.xml --cov=pgware --cov-report=term-missing -v

behavetests:
	pipenv run behave tests/behave

coverage-html:
	pipenv run coverage html

build_package:
	pipenv run python setup.py sdist bdist_wheel

push_package: build_package
	pipenv run twine upload dist/*

pdoc-docs:
	pipenv run pdoc --html pgware --html-dir docs/build
