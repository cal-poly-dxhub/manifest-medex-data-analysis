HL7_QUERY_DIR ?= ../customer_delivery/PrismInfoFor AWS-HL7 DSG Dashboard

.PHONY: install format lint typecheck test web-install web-typecheck web-build synth validate hl7-dictionary hl7-parity

install:
	uv sync --all-groups --frozen
	npm ci --prefix web

hl7-dictionary:
	uv run python tools/generate_hl7_dictionary.py

hl7-parity:
	uv run python tools/validate_against_customer_queries.py --query-dir "$(HL7_QUERY_DIR)"

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

web-install:
	npm ci --prefix web

web-typecheck:
	npm run typecheck --prefix web

web-build:
	npm run build --prefix web

synth: web-build
	uv run cdk synth --quiet

validate: lint typecheck test web-typecheck synth
