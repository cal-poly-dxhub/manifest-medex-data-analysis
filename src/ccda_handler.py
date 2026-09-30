"""AWS Lambda entry point for the dedicated CCDA ingestion lane."""

from __future__ import annotations

from typing import Any

from src.document_handler import runtime_handler


def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    """Process only CCDA objects from the dedicated CCDA SQS lane."""
    return runtime_handler(event, context, expected_format="ccda")
