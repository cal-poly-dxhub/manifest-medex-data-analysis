.PHONY: install format lint typecheck test synth validate

install:
	uv sync --all-groups --frozen

format:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff format --check .
	uv run ruff check .

typecheck:
	uv run mypy

test:
	uv run pytest

synth:
	uv run cdk synth --quiet

validate: lint typecheck test synth
