from __future__ import annotations

from typing import Any

from src.document_handler import runtime_handler


def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    """Process only HL7 v2 objects from the dedicated HL7 SQS lane."""
    return runtime_handler(event, context, expected_format="hl7-v2")
