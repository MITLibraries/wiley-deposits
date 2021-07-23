all: bandit black flake8 isort pylama

bandit:
	pipenv run bandit -r . --skip B101

black:
	pipenv run black --check --diff .

flake8:
	pipenv run flake8 .

isort:
	pipenv run isort . --diff

pylama:
	pipenv run pylama .
