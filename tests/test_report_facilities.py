import json
from typing import Any

import pytest
from src.report_facilities import (
    FacilityDirectory,
    FacilityDirectoryError,
)

HL7_INDEX = "hl7-messages-v1"
CCDA_INDEX = "ccda-documents-v1"


class FakeTransport:
    def __init__(
        self,
        *,
        status: int = 200,
        response: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._status = status
        self._response = response or {}
        self._error = error
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        if self._error is not None:
            raise self._error
        return self._status, self._response


def _aggregation(keys: list[Any]) -> dict[str, Any]:
    return {"aggregations": {"facilities": {"buckets": [{"key": key} for key in keys]}}}


def _directory(transport: FakeTransport, **kwargs: Any) -> FacilityDirectory:
    return FacilityDirectory(
        transport,
        hl7_index=HL7_INDEX,
        ccda_index=CCDA_INDEX,
        **kwargs,
    )


def test_list_facilities_queries_both_indexes_with_terms_aggregation() -> None:
    transport = FakeTransport(response=_aggregation(["FAC-B", "FAC-A"]))

    facilities = _directory(transport).list_facilities()

    assert facilities == ["FAC-A", "FAC-B"]
    method, path, body = transport.calls[0]
    assert method == "POST"
    assert path == "/hl7-messages-v1,ccda-documents-v1/_search"
    assert body is not None
    payload = json.loads(body)
    assert payload["size"] == 0
    assert payload["aggs"]["facilities"]["terms"]["field"] == "sourceFacilityId"
    assert payload["aggs"]["facilities"]["terms"]["size"] == 10_000


def test_list_facilities_dedupes_and_sorts_across_indexes() -> None:
    transport = FakeTransport(response=_aggregation(["FAC-2", "FAC-1", "FAC-2", "FAC-1"]))

    assert _directory(transport).list_facilities() == ["FAC-1", "FAC-2"]


def test_list_facilities_ignores_non_string_and_empty_keys() -> None:
    transport = FakeTransport(response=_aggregation(["FAC-1", 42, "", None, "FAC-2"]))

    assert _directory(transport).list_facilities() == ["FAC-1", "FAC-2"]


def test_list_facilities_honors_configured_field_and_size() -> None:
    transport = FakeTransport(response=_aggregation([]))

    _directory(transport, field="sourceFacilityId", max_terms=25).list_facilities()

    payload = json.loads(transport.calls[0][2] or b"{}")
    assert payload["aggs"]["facilities"]["terms"]["size"] == 25


def test_list_facilities_non_2xx_status_is_sanitized() -> None:
    transport = FakeTransport(status=503, response={"error": {"type": "cluster_block_exception"}})

    with pytest.raises(FacilityDirectoryError, match="lookup failed"):
        _directory(transport).list_facilities()


def test_list_facilities_transport_error_is_sanitized() -> None:
    transport = FakeTransport(error=RuntimeError("connection reset with sensitive detail"))

    with pytest.raises(FacilityDirectoryError, match="lookup failed") as captured:
        _directory(transport).list_facilities()

    assert "sensitive" not in str(captured.value)


@pytest.mark.parametrize(
    "response",
    [
        {},
        {"aggregations": []},
        {"aggregations": {"facilities": []}},
        {"aggregations": {"facilities": {"buckets": "nope"}}},
        {"aggregations": {"facilities": {"buckets": ["not-a-dict"]}}},
    ],
)
def test_list_facilities_malformed_aggregation_is_sanitized(response: dict[str, Any]) -> None:
    transport = FakeTransport(response=response)

    with pytest.raises(FacilityDirectoryError, match="lookup failed"):
        _directory(transport).list_facilities()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"hl7_index": "Bad Index"},
        {"ccda_index": ""},
        {"field": "bad field"},
        {"field": "9startsWithDigit"},
        {"max_terms": 0},
    ],
)
def test_constructor_rejects_invalid_configuration(kwargs: dict[str, Any]) -> None:
    base: dict[str, Any] = {"hl7_index": HL7_INDEX, "ccda_index": CCDA_INDEX}
    base.update(kwargs)
    with pytest.raises(ValueError, match="configuration is invalid"):
        FacilityDirectory(FakeTransport(), **base)
