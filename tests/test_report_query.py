import json
from typing import Any

import pytest
from src.report_query import (
    COUNT_RESPONSE_INVALID,
    PLACEHOLDER_QUERY,
    QUERY_TEST_FAILED,
    QueryCountError,
    QueryTester,
    QueryTestError,
    QueryTestRequestError,
    build_count_body,
    read_total_hits,
)

HL7_INDEX = "hl7-messages-v1"
_QUERY = {"term": {"ROOT.PID._present": "1"}}


class FakeTransport:
    def __init__(
        self,
        *,
        status: int = 200,
        response: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.status = status
        self.response = response if response is not None else {"hits": {"total": {"value": 3}}}
        self.error = error
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        if self.error is not None:
            raise self.error
        return self.status, self.response


class FakeFacilities:
    def __init__(self, facilities: list[str] | None = None) -> None:
        self._facilities = facilities if facilities is not None else ["facility-a", "facility-b"]

    def list_facilities(self) -> list[str]:
        return list(self._facilities)


def test_build_count_body_wraps_non_bool_query_without_mutating_source() -> None:
    source = {"term": {"a": "b"}}
    body = build_count_body(source)

    assert body == {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"filter": [{"term": {"a": "b"}}]}},
    }
    # The stored query is never mutated by injection.
    assert source == {"term": {"a": "b"}}


def test_build_count_body_preserves_existing_list_filters_and_injects() -> None:
    source = {"bool": {"filter": [{"term": {"existing": "1"}}]}}
    body = build_count_body(
        source,
        partition_field="sourceFacilityId",
        partition_value="facility-a",
        time_field="messageTime",
        from_time="2026-08-01T00:00:00Z",
        to_time="2026-08-31T00:00:00Z",
    )

    filters = body["query"]["bool"]["filter"]
    assert {"term": {"existing": "1"}} in filters
    assert {"term": {"sourceFacilityId": "facility-a"}} in filters
    assert {
        "range": {"messageTime": {"gte": "2026-08-01T00:00:00Z", "lt": "2026-08-31T00:00:00Z"}}
    } in filters


def test_build_count_body_normalizes_missing_and_scalar_filter() -> None:
    empty = build_count_body({"bool": {}})
    assert empty["query"]["bool"]["filter"] == []

    scalar = build_count_body(
        {"bool": {"filter": {"term": {"only": "1"}}}},
        partition_field="sourceFacilityId",
        partition_value="facility-a",
    )
    assert scalar["query"]["bool"]["filter"] == [
        {"term": {"only": "1"}},
        {"term": {"sourceFacilityId": "facility-a"}},
    ]


def test_build_count_body_omits_filters_when_bounds_incomplete() -> None:
    body = build_count_body(
        _QUERY,
        partition_field="sourceFacilityId",
        partition_value=None,
        time_field="messageTime",
        from_time="2026-08-01T00:00:00Z",
        to_time=None,
    )
    filters = body["query"]["bool"]["filter"]
    assert filters == [_QUERY]


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"hits": {"total": {"value": 12}}}, 12),
        ({"hits": {"total": 5}}, 5),
    ],
)
def test_read_total_hits_accepts_int_and_object_totals(
    payload: dict[str, Any], expected: int
) -> None:
    assert read_total_hits(payload) == expected


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"hits": {}},
        {"hits": {"total": True}},
        {"hits": {"total": "5"}},
        {"hits": {"total": {"value": -1}}},
        {"hits": {"total": {"value": True}}},
    ],
)
def test_read_total_hits_rejects_malformed_totals(payload: dict[str, Any]) -> None:
    with pytest.raises(QueryCountError, match=COUNT_RESPONSE_INVALID):
        read_total_hits(payload)


def test_query_tester_counts_with_shared_size_zero_search() -> None:
    transport = FakeTransport(response={"hits": {"total": {"value": 8}}})
    tester = QueryTester(transport, FakeFacilities())

    result = tester.count(
        index=HL7_INDEX,
        query=_QUERY,
        facility="facility-a",
        from_time="2026-08-01T00:00:00Z",
        to_time="2026-08-31T00:00:00Z",
    )

    assert result == {"count": 8}
    method, path, body = transport.calls[0]
    assert (method, path) == ("POST", "/hl7-messages-v1/_search")
    assert body is not None
    sent = json.loads(body.decode())
    assert sent["size"] == 0
    assert sent["track_total_hits"] is True
    filters = sent["query"]["bool"]["filter"]
    assert {"term": {"sourceFacilityId": "facility-a"}} in filters
    assert {
        "range": {"messageTime": {"gte": "2026-08-01T00:00:00Z", "lt": "2026-08-31T00:00:00Z"}}
    } in filters


def test_query_tester_allows_no_optional_filters() -> None:
    transport = FakeTransport(response={"hits": {"total": {"value": 0}}})
    tester = QueryTester(transport, FakeFacilities())

    result = tester.count(index=HL7_INDEX, query=_QUERY)

    assert result == {"count": 0}
    _method, _path, body = transport.calls[0]
    assert body is not None
    filters = json.loads(body.decode())["query"]["bool"]["filter"]
    assert filters == [_QUERY]


@pytest.mark.parametrize(
    ("kwargs", "code"),
    [
        ({"index": HL7_INDEX, "query": "not-a-dict"}, "invalid_query"),
        ({"index": 123, "query": _QUERY}, "invalid_query"),
        ({"index": "unknown-index", "query": _QUERY}, "invalid_query"),
        ({"index": HL7_INDEX, "query": {}}, "invalid_query"),
        ({"index": HL7_INDEX, "query": {"term": {"sourceFacilityId": "x"}}}, "invalid_query"),
        ({"index": HL7_INDEX, "query": {"term": {"messageTime": "x"}}}, "invalid_query"),
        (
            {"index": HL7_INDEX, "query": _QUERY, "from_time": "2026-08-01T00:00:00Z"},
            "invalid_time_range",
        ),
        (
            {
                "index": HL7_INDEX,
                "query": _QUERY,
                "from_time": "2026-08-31T00:00:00Z",
                "to_time": "2026-08-01T00:00:00Z",
            },
            "invalid_time_range",
        ),
        (
            {
                "index": HL7_INDEX,
                "query": _QUERY,
                "from_time": "no-tz",
                "to_time": "2026-08-01T00:00:00Z",
            },
            "invalid_time",
        ),
        (
            {
                "index": HL7_INDEX,
                "query": _QUERY,
                "from_time": "2026-08-01T00:00:00",
                "to_time": "2026-08-02T00:00:00+00:00",
            },
            "invalid_time",
        ),
        (
            {
                "index": HL7_INDEX,
                "query": _QUERY,
                "from_time": "2026-08-01T00:00:00+00:00" + "0" * 64,
                "to_time": "2026-08-02T00:00:00+00:00",
            },
            "invalid_time",
        ),
        ({"index": HL7_INDEX, "query": _QUERY, "facility": ""}, "invalid_facility"),
        ({"index": HL7_INDEX, "query": _QUERY, "facility": 7}, "invalid_facility"),
        ({"index": HL7_INDEX, "query": _QUERY, "facility": "ghost"}, "unknown_facility"),
    ],
)
def test_query_tester_rejects_invalid_requests(kwargs: dict[str, Any], code: str) -> None:
    transport = FakeTransport()
    tester = QueryTester(transport, FakeFacilities())

    with pytest.raises(QueryTestRequestError) as captured:
        tester.count(**kwargs)

    assert captured.value.code == code
    assert captured.value.status_code == 400
    # An invalid request must never reach the search backend.
    assert transport.calls == []


def test_query_tester_rejects_placeholder_null_query_before_transport() -> None:
    transport = FakeTransport()
    tester = QueryTester(transport, FakeFacilities())

    with pytest.raises(QueryTestRequestError) as captured:
        tester.count(index=HL7_INDEX, query=None)

    # A placeholder (null) query is rejected with its own clear, sanitized code, and the
    # rejection happens before any request reaches the search backend.
    assert captured.value.code == PLACEHOLDER_QUERY
    assert captured.value.code == "placeholder_query_not_implemented"
    assert captured.value.status_code == 400
    assert transport.calls == []


def test_query_tester_sanitizes_transport_error() -> None:
    transport = FakeTransport(error=RuntimeError("sensitive backend detail"))
    tester = QueryTester(transport, FakeFacilities())

    with pytest.raises(QueryTestError, match=QUERY_TEST_FAILED) as captured:
        tester.count(index=HL7_INDEX, query=_QUERY)

    assert not isinstance(captured.value, QueryTestRequestError)
    assert captured.value.__cause__ is None
    assert "sensitive" not in str(captured.value)


def test_query_tester_sanitizes_non_success_status_and_bad_body() -> None:
    rejected = QueryTester(
        FakeTransport(status=403, response={"error": "denied"}), FakeFacilities()
    )
    with pytest.raises(QueryTestError, match=QUERY_TEST_FAILED):
        rejected.count(index=HL7_INDEX, query=_QUERY)

    malformed = QueryTester(FakeTransport(response={"hits": {}}), FakeFacilities())
    with pytest.raises(QueryTestError, match=QUERY_TEST_FAILED):
        malformed.count(index=HL7_INDEX, query=_QUERY)
