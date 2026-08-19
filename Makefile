HL7_QUERY_DIR ?= ../customer_delivery/PrismInfoFor AWS-HL7 DSG Dashboard

.PHONY: install format lint typecheck test synth validate hl7-dictionary hl7-parity

install:
	uv sync --all-groups --frozen

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

synth:
	uv run cdk synth --quiet

validate: lint typecheck test synth
