lint: bandit black flake8 isort

bandit:
	pipenv run bandit -r awd

black:
	pipenv run black --check --diff .
	
coveralls: test
	pipenv run coveralls

flake8:
	pipenv run flake8 .

isort:
	pipenv run isort . --diff
	
test:
	pipenv run pytest --cov=awd
