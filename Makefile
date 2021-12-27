flake8-check:
	flake8 --config=.flake8

black-check:
	black . --config pyproject.toml --check

isort-check:
	isort . --diff --check

black:
	black . --config pyproject.toml

isort:
	isort .
linters: isort black flake8-check

up_local_compose:
	docker-compose up -d

run_script:
	python postgres_to_es/load_pg_to_es.py
