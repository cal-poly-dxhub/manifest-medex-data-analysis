import base64
import json
import logging
from typing import Any, cast

import pytest
from src.message_search import (
    FIELD_CAPS_FAILED,
    FIELD_CAPS_INVALID,
    INVALID_CALLER,
    INVALID_CURSOR,
    INVALID_FACILITY,
    INVALID_FIELD,
    INVALID_FILTER,
    INVALID_INDEX,
    INVALID_LIMIT,
    INVALID_OPERATOR,
    INVALID_TIME,
    INVALID_TIME_RANGE,
    INVALID_VALUE,
    SEARCH_EVENT,
    SEARCH_FAILED,
    TOO_MANY_FILTERS,
    UNKNOWN_FIELD,
    VALUE_TOO_SHORT,
    VALUE_UNSAFE,
    SearchError,
    SearchRequestError,
    SearchService,
)

LOGGER_NAME = "src.message_search"


class FakeTransport:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return cast("tuple[int, dict[str, Any]]", item)


def _hl7_caps() -> tuple[int, dict[str, Any]]:
    return (
        200,
        {
            "fields": {
                "documentId": {"keyword": {"type": "keyword", "searchable": True}},
                "messageType": {"keyword": {"type": "keyword", "searchable": True}},
                "triggerEvent": {"keyword": {"type": "keyword", "searchable": True}},
                "sourceFacilityId": {"keyword": {"type": "keyword", "searchable": True}},
                "messageControlId": {"keyword": {"type": "keyword", "searchable": True}},
                "messageTime": {"date": {"type": "date", "searchable": True}},
                "ingestTime": {"date": {"type": "date", "searchable": True}},
                "rawObject": {"object": {"type": "object", "searchable": False}},
                "disabledField": {"keyword": {"type": "keyword", "searchable": False}},
            }
        },
    )


def _ccda_caps() -> tuple[int, dict[str, Any]]:
    return (
        200,
        {
            "fields": {
                "documentId": {"keyword": {"type": "keyword", "searchable": True}},
                "sourceFacilityId": {"keyword": {"type": "keyword", "searchable": True}},
                "documentTime": {"date": {"type": "date", "searchable": True}},
                "ingestTime": {"date": {"type": "date", "searchable": True}},
                "narrativeText": {"text": {"type": "text", "searchable": True}},
                "narrativeText.keyword": {"keyword": {"type": "keyword", "searchable": True}},
                "unindexedText": {"text": {"type": "text", "searchable": True}},
                "rawObject": {"object": {"type": "object", "searchable": False}},
            }
        },
    )


def _search_response(hits: list[dict[str, Any]], total: int) -> tuple[int, dict[str, Any]]:
    return (200, {"hits": {"total": {"value": total, "relation": "eq"}, "hits": hits}})


def _post_bodies(transport: FakeTransport) -> list[dict[str, Any]]:
    return [
        json.loads(body)
        for method, path, body in transport.calls
        if method == "POST" and path.endswith("/_search") and body is not None
    ]


def _search_body(transport: FakeTransport) -> dict[str, Any]:
    bodies = _post_bodies(transport)
    assert len(bodies) == 1
    return bodies[0]


@pytest.mark.parametrize(
    ("operator", "value", "expected"),
    [
        ("equals", "ORU", {"term": {"messageType": "ORU"}}),
        ("exists", None, {"exists": {"field": "messageType"}}),
        ("contains", "ORU", {"wildcard": {"messageType": {"value": "*ORU*"}}}),
        ("prefix", "OR", {"prefix": {"messageType": {"value": "OR"}}}),
    ],
)
def test_each_operator_builds_expected_clause(
    operator: str,
    value: str | None,
    expected: dict[str, Any],
) -> None:
    transport = FakeTransport([_hl7_caps(), _search_response([], 0)])
    service = SearchService(transport)
    request_filter: dict[str, Any] = {"field": "messageType", "operator": operator}
    if value is not None:
        request_filter["value"] = value

    service.search(index="hl7-messages-v1", caller_sub="sub", filters=[request_filter])

    body = _search_body(transport)
    assert body["query"]["bool"]["filter"] == [expected]


def test_equals_on_ccda_text_field_uses_keyword_subfield() -> None:
    transport = FakeTransport([_ccda_caps(), _search_response([], 0)])
    service = SearchService(transport)

    service.search(
        index="ccda-documents-v1",
        caller_sub="sub",
        filters=[{"field": "narrativeText", "operator": "equals", "value": "Smith"}],
    )

    body = _search_body(transport)
    assert body["query"]["bool"]["filter"] == [{"term": {"narrativeText.keyword": "Smith"}}]


def test_equals_on_text_field_without_keyword_subfield_uses_base_field() -> None:
    transport = FakeTransport([_ccda_caps(), _search_response([], 0)])
    service = SearchService(transport)

    service.search(
        index="ccda-documents-v1",
        caller_sub="sub",
        filters=[{"field": "unindexedText", "operator": "equals", "value": "value"}],
    )

    body = _search_body(transport)
    assert body["query"]["bool"]["filter"] == [{"term": {"unindexedText": "value"}}]


def test_equals_on_hl7_native_keyword_field_is_unchanged() -> None:
    transport = FakeTransport([_hl7_caps(), _search_response([], 0)])
    service = SearchService(transport)

    service.search(
        index="hl7-messages-v1",
        caller_sub="sub",
        filters=[{"field": "messageType", "operator": "equals", "value": "ADT"}],
    )

    body = _search_body(transport)
    assert body["query"]["bool"]["filter"] == [{"term": {"messageType": "ADT"}}]


def test_contains_escapes_wildcard_metacharacters_and_trims() -> None:
    transport = FakeTransport([_ccda_caps(), _search_response([], 0)])
    service = SearchService(transport)

    service.search(
        index="ccda-documents-v1",
        caller_sub="sub",
        filters=[{"field": "narrativeText", "operator": "contains", "value": " a*b?c\\d "}],
    )

    body = _search_body(transport)
    assert body["query"]["bool"]["filter"] == [
        {"wildcard": {"narrativeText": {"value": "*a\\*b\\?c\\\\d*"}}}
    ]


@pytest.mark.parametrize(
    ("value", "code"),
    [
        ("ab", VALUE_TOO_SHORT),
        ("  a  ", VALUE_TOO_SHORT),
        ("***", VALUE_UNSAFE),
        ("*?*", VALUE_UNSAFE),
        ("x" * 257, VALUE_UNSAFE),
    ],
)
def test_contains_guard_rejects_unsafe_values(value: str, code: str) -> None:
    transport = FakeTransport([_ccda_caps()])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(
            index="ccda-documents-v1",
            caller_sub="sub",
            filters=[{"field": "narrativeText", "operator": "contains", "value": value}],
        )

    assert captured.value.code == code
    assert captured.value.status == 400
    assert all(not call[1].endswith("/_search") for call in transport.calls)


def test_unknown_field_is_rejected_before_any_search() -> None:
    transport = FakeTransport([_hl7_caps()])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(
            index="hl7-messages-v1",
            caller_sub="sub",
            filters=[{"field": "patientName", "operator": "equals", "value": "x"}],
        )

    assert captured.value.code == UNKNOWN_FIELD
    assert [call[1] for call in transport.calls] == ["/hl7-messages-v1/_field_caps?fields=*"]


@pytest.mark.parametrize(
    ("bad_filter", "code"),
    [
        ("not-a-mapping", INVALID_FILTER),
        ({"field": "messageType", "operator": "exists", "extra": 1}, INVALID_FILTER),
        ({"operator": "exists"}, INVALID_FIELD),
        ({"field": "bad field", "operator": "exists"}, INVALID_FIELD),
        ({"field": "rawObject", "operator": "exists"}, UNKNOWN_FIELD),
        ({"field": "disabledField", "operator": "exists"}, UNKNOWN_FIELD),
        ({"field": "messageType"}, INVALID_OPERATOR),
        ({"field": "messageType", "operator": "regexp", "value": "x"}, INVALID_OPERATOR),
        ({"field": "messageType", "operator": "exists", "value": "x"}, INVALID_VALUE),
        ({"field": "messageType", "operator": "equals"}, INVALID_VALUE),
        ({"field": "messageType", "operator": "equals", "value": ""}, INVALID_VALUE),
        ({"field": "messageType", "operator": "equals", "value": 123}, INVALID_VALUE),
    ],
)
def test_filter_validation_rejects_malformed_requests(bad_filter: Any, code: str) -> None:
    transport = FakeTransport([_hl7_caps()])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=[bad_filter])

    assert captured.value.code == code
    assert all(not call[1].endswith("/_search") for call in transport.calls)


def test_filters_must_be_a_list() -> None:
    transport = FakeTransport([_hl7_caps()])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters="nope")

    assert captured.value.code == INVALID_FILTER


def test_too_many_filters_are_rejected() -> None:
    transport = FakeTransport([_hl7_caps()])
    service = SearchService(transport)
    filters = [{"field": "messageType", "operator": "exists"}] * 21

    with pytest.raises(SearchRequestError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=filters)

    assert captured.value.code == TOO_MANY_FILTERS


def test_field_capabilities_are_cached_per_instance() -> None:
    transport = FakeTransport([_hl7_caps(), _search_response([], 0), _search_response([], 0)])
    service = SearchService(transport)

    filters = [{"field": "messageType", "operator": "exists"}]
    service.search(index="hl7-messages-v1", caller_sub="sub", filters=filters)
    service.search(index="hl7-messages-v1", caller_sub="sub", filters=filters)

    assert sum(1 for call in transport.calls if "/_field_caps" in call[1]) == 1
    assert sum(1 for call in transport.calls if call[1].endswith("/_search")) == 2


def test_empty_filters_use_match_all_without_field_capabilities() -> None:
    transport = FakeTransport([_search_response([], 0)])
    service = SearchService(transport)

    result = service.search(index="hl7-messages-v1", caller_sub="sub", filters=[])

    assert result == {"items": [], "total": 0, "nextCursor": None}
    assert [call[0] for call in transport.calls] == ["POST"]
    assert _search_body(transport)["query"] == {"match_all": {}}


def test_facility_and_time_window_are_added_as_filters() -> None:
    transport = FakeTransport([_hl7_caps(), _search_response([], 0)])
    service = SearchService(transport)

    service.search(
        index="hl7-messages-v1",
        caller_sub="sub",
        filters=[{"field": "messageType", "operator": "exists"}],
        facility="FAC-1",
        from_time="2026-01-01T00:00:00+00:00",
        to_time="2026-02-01T00:00:00+00:00",
    )

    clauses = _search_body(transport)["query"]["bool"]["filter"]
    assert {"term": {"sourceFacilityId": "FAC-1"}} in clauses
    assert {
        "range": {
            "messageTime": {"gte": "2026-01-01T00:00:00+00:00", "lt": "2026-02-01T00:00:00+00:00"}
        }
    } in clauses


def test_time_window_uses_configured_time_field_for_ccda() -> None:
    transport = FakeTransport([_search_response([], 0)])
    service = SearchService(transport)

    service.search(
        index="ccda-documents-v1",
        caller_sub="sub",
        filters=[],
        from_time="2026-01-01T00:00:00+00:00",
        to_time="2026-02-01T00:00:00+00:00",
    )

    clauses = _search_body(transport)["query"]["bool"]["filter"]
    assert clauses == [
        {
            "range": {
                "documentTime": {
                    "gte": "2026-01-01T00:00:00+00:00",
                    "lt": "2026-02-01T00:00:00+00:00",
                }
            }
        }
    ]


@pytest.mark.parametrize(
    ("from_time", "to_time", "code"),
    [
        ("2026-01-01T00:00:00+00:00", None, INVALID_TIME_RANGE),
        (None, "2026-01-01T00:00:00+00:00", INVALID_TIME_RANGE),
        ("2026-02-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00", INVALID_TIME_RANGE),
        ("2026-01-01T00:00:00", "2026-02-01T00:00:00+00:00", INVALID_TIME),
    ],
)
def test_invalid_time_windows_are_rejected(
    from_time: Any,
    to_time: Any,
    code: str,
) -> None:
    transport = FakeTransport([_hl7_caps()])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(
            index="hl7-messages-v1",
            caller_sub="sub",
            filters=[],
            from_time=from_time,
            to_time=to_time,
        )

    assert captured.value.code == code
    assert all(not call[1].endswith("/_search") for call in transport.calls)


@pytest.mark.parametrize("facility", ["", 123, "x" * 257])
def test_invalid_facility_is_rejected(facility: Any) -> None:
    transport = FakeTransport([_hl7_caps()])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], facility=facility)

    assert captured.value.code == INVALID_FACILITY


def test_default_limit_and_pagination_controls() -> None:
    transport = FakeTransport([_search_response([], 0)])
    service = SearchService(transport)

    service.search(index="hl7-messages-v1", caller_sub="sub", filters=[])

    body = _search_body(transport)
    assert body["size"] == 25
    assert body["track_total_hits"] == 10_000
    assert body["sort"] == [
        {"messageTime": {"order": "desc"}},
        {"documentId": {"order": "desc"}},
    ]
    assert body["_source"] == {
        "includes": [
            "documentId",
            "sourceFormat",
            "sourceFacilityId",
            "messageType",
            "triggerEvent",
            "messageTime",
            "ingestTime",
        ]
    }
    assert "from" not in body
    assert "search_after" not in body


def test_limit_maximum_is_accepted() -> None:
    transport = FakeTransport([_search_response([], 0)])
    service = SearchService(transport)

    service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], limit=100)

    assert _search_body(transport)["size"] == 100


@pytest.mark.parametrize("limit", [0, -1, 101, True, 1.5, "25"])
def test_invalid_limit_is_rejected(limit: Any) -> None:
    transport = FakeTransport([_hl7_caps()])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], limit=limit)

    assert captured.value.code == INVALID_LIMIT


def test_full_page_returns_cursor_that_drives_search_after() -> None:
    hits = [
        {"_source": {"documentId": "d1"}, "sort": [1000, "d1"]},
        {"_source": {"documentId": "d2"}, "sort": [900, "d2"]},
    ]
    transport = FakeTransport([_search_response(hits, 5), _search_response([], 0)])
    service = SearchService(transport)

    first = service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], limit=2)
    cursor = first["nextCursor"]
    assert cursor is not None

    service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], limit=2, cursor=cursor)

    second_body = _post_bodies(transport)[1]
    assert second_body["search_after"] == [900, "d2"]
    assert "from" not in second_body


def test_short_page_has_no_cursor() -> None:
    hits = [{"_source": {"documentId": "d1"}, "sort": [1000, "d1"]}]
    transport = FakeTransport([_search_response(hits, 1)])
    service = SearchService(transport)

    result = service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], limit=2)

    assert result["nextCursor"] is None


def test_cursor_is_bound_to_its_index() -> None:
    hits = [
        {"_source": {"documentId": "d1"}, "sort": [1000, "d1"]},
        {"_source": {"documentId": "d2"}, "sort": [900, "d2"]},
    ]
    transport = FakeTransport([_search_response(hits, 5)])
    service = SearchService(transport)
    cursor = service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], limit=2)[
        "nextCursor"
    ]

    other = FakeTransport([])
    other_service = SearchService(other)
    with pytest.raises(SearchRequestError) as captured:
        other_service.search(
            index="ccda-documents-v1", caller_sub="sub", filters=[], limit=2, cursor=cursor
        )

    assert captured.value.code == INVALID_CURSOR
    assert other.calls == []


@pytest.mark.parametrize(
    "cursor",
    [
        "",
        "!!!not-base64!!!",
        "x" * 3000,
        base64.urlsafe_b64encode(
            json.dumps({"index": "hl7-messages-v1", "searchAfter": [1], "version": 1}).encode()
        )
        .decode()
        .rstrip("="),
        base64.urlsafe_b64encode(
            json.dumps({"index": "hl7-messages-v1", "searchAfter": [1, "d"], "version": 2}).encode()
        )
        .decode()
        .rstrip("="),
    ],
)
def test_malformed_cursor_is_rejected(cursor: str) -> None:
    transport = FakeTransport([])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], cursor=cursor)

    assert captured.value.code == INVALID_CURSOR
    assert transport.calls == []


def test_response_projects_only_allowlisted_metadata() -> None:
    hits = [
        {
            "_source": {
                "documentId": "d1",
                "sourceFormat": "hl7-v2",
                "sourceFacilityId": "FAC",
                "messageType": "ORU",
                "triggerEvent": "R01",
                "messageTime": "2026-01-01T00:00:00Z",
                "ingestTime": "2026-01-02T00:00:00Z",
                "narrativeText": "PATIENT SSN 123-45-6789",
                "rawObject": {"segment": "PID"},
            },
            "sort": [1, "d1"],
        }
    ]
    transport = FakeTransport([_search_response(hits, 1)])
    service = SearchService(transport)

    result = service.search(index="hl7-messages-v1", caller_sub="sub", filters=[])

    item = result["items"][0]
    assert set(item) == {
        "documentId",
        "sourceFormat",
        "sourceFacilityId",
        "messageType",
        "triggerEvent",
        "messageTime",
        "ingestTime",
    }
    assert "narrativeText" not in item
    assert "rawObject" not in item
    assert result["total"] == 1


def test_audit_logs_only_field_names_and_no_values(caplog: pytest.LogCaptureFixture) -> None:
    hits = [{"_source": {"documentId": "d1", "messageType": "ORU"}, "sort": [1, "d1"]}]
    transport = FakeTransport([_hl7_caps(), _search_response(hits, 1)])
    service = SearchService(transport)

    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        service.search(
            index="hl7-messages-v1",
            caller_sub="user-123",
            filters=[{"field": "messageType", "operator": "equals", "value": "ORU"}],
            facility="FAC-9",
            from_time="2026-01-01T00:00:00+00:00",
            to_time="2026-02-01T00:00:00+00:00",
        )

    records = [record for record in caplog.records if record.name == LOGGER_NAME]
    assert len(records) == 1
    message = records[0].getMessage()
    payload = json.loads(message)
    assert set(payload) == {"callerSub", "event", "fields", "index", "timestamp"}
    assert payload["callerSub"] == "user-123"
    assert payload["event"] == SEARCH_EVENT
    assert payload["fields"] == ["messageType"]
    assert payload["index"] == "hl7-messages-v1"
    # No searched value, facility, or date content is ever logged. The timestamp is the
    # only date present, so the specific searched window bounds must be absent.
    assert "ORU" not in message
    assert "FAC-9" not in message
    assert "2026-01-01T00:00:00+00:00" not in message
    assert "2026-02-01T00:00:00+00:00" not in message


def test_audit_field_names_are_sorted_and_distinct(caplog: pytest.LogCaptureFixture) -> None:
    transport = FakeTransport([_hl7_caps(), _search_response([], 0)])
    service = SearchService(transport)

    with caplog.at_level(logging.INFO, logger=LOGGER_NAME):
        service.search(
            index="hl7-messages-v1",
            caller_sub="sub",
            filters=[
                {"field": "triggerEvent", "operator": "exists"},
                {"field": "messageType", "operator": "exists"},
                {"field": "messageType", "operator": "equals", "value": "ORU"},
            ],
        )

    payload = json.loads(
        next(record for record in caplog.records if record.name == LOGGER_NAME).getMessage()
    )
    assert payload["fields"] == ["messageType", "triggerEvent"]


def test_no_audit_line_on_backend_failure(caplog: pytest.LogCaptureFixture) -> None:
    transport = FakeTransport([(503, {"error": {"type": "cluster_block_exception"}})])
    service = SearchService(transport)

    with caplog.at_level(logging.INFO, logger=LOGGER_NAME), pytest.raises(SearchError):
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=[])

    assert [record for record in caplog.records if record.name == LOGGER_NAME] == []


@pytest.mark.parametrize("index", ["", "other-index", 123, None])
def test_invalid_index_is_rejected(index: Any) -> None:
    transport = FakeTransport([])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(index=index, caller_sub="sub", filters=[])

    assert captured.value.code == INVALID_INDEX
    assert transport.calls == []


@pytest.mark.parametrize("caller", ["", None, 123, "x" * 129])
def test_invalid_caller_is_rejected(caller: Any) -> None:
    transport = FakeTransport([])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.search(index="hl7-messages-v1", caller_sub=caller, filters=[])

    assert captured.value.code == INVALID_CALLER
    assert transport.calls == []


def test_field_caps_http_error_is_sanitized() -> None:
    transport = FakeTransport(
        [(500, {"error": {"type": "secret-index", "reason": "PHI in reason"}})]
    )
    service = SearchService(transport)

    with pytest.raises(SearchError) as captured:
        service.search(
            index="hl7-messages-v1",
            caller_sub="sub",
            filters=[{"field": "messageType", "operator": "exists"}],
        )

    assert str(captured.value) == FIELD_CAPS_FAILED
    assert "PHI" not in str(captured.value)
    assert not isinstance(captured.value, SearchRequestError)


def test_field_caps_malformed_response_is_sanitized() -> None:
    transport = FakeTransport([(200, {"unexpected": True})])
    service = SearchService(transport)

    with pytest.raises(SearchError) as captured:
        service.search(
            index="hl7-messages-v1",
            caller_sub="sub",
            filters=[{"field": "messageType", "operator": "exists"}],
        )

    assert str(captured.value) == FIELD_CAPS_INVALID


def test_field_caps_transport_exception_is_sanitized() -> None:
    transport = FakeTransport([RuntimeError("dns failure leaking PHI")])
    service = SearchService(transport)

    with pytest.raises(SearchError) as captured:
        service.search(
            index="hl7-messages-v1",
            caller_sub="sub",
            filters=[{"field": "messageType", "operator": "exists"}],
        )

    assert str(captured.value) == FIELD_CAPS_FAILED
    assert "PHI" not in str(captured.value)


def test_search_http_error_is_sanitized() -> None:
    transport = FakeTransport([(429, {"error": {"reason": "clinical detail"}})])
    service = SearchService(transport)

    with pytest.raises(SearchError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=[])

    assert str(captured.value) == SEARCH_FAILED
    assert "clinical" not in str(captured.value)


def test_search_transport_exception_is_sanitized() -> None:
    transport = FakeTransport([RuntimeError("socket blew up with PHI")])
    service = SearchService(transport)

    with pytest.raises(SearchError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=[])

    assert str(captured.value) == SEARCH_FAILED
    assert "PHI" not in str(captured.value)


@pytest.mark.parametrize(
    "response_body",
    [
        {"hits": {"total": {"value": 1}, "hits": "not-a-list"}},
        {"hits": {"total": {"value": 1}, "hits": ["not-a-mapping"]}},
        {"hits": {"total": {"value": 1}, "hits": [{"_source": "not-a-mapping"}]}},
        {"hits": {"total": "lots", "hits": []}},
        {"hits": {"total": {"value": -1}, "hits": []}},
        {"unexpected": True},
    ],
)
def test_malformed_search_response_is_sanitized(response_body: dict[str, Any]) -> None:
    transport = FakeTransport([(200, response_body)])
    service = SearchService(transport)

    with pytest.raises(SearchError) as captured:
        service.search(index="hl7-messages-v1", caller_sub="sub", filters=[])

    assert str(captured.value) == SEARCH_FAILED


def test_full_page_without_sort_values_yields_no_cursor() -> None:
    hits: list[dict[str, Any]] = [
        {"_source": {"documentId": "d1"}},
        {"_source": {"documentId": "d2"}, "sort": [1, 2, 3]},
    ]
    transport = FakeTransport([_search_response(hits, 9)])
    service = SearchService(transport)

    result = service.search(index="hl7-messages-v1", caller_sub="sub", filters=[], limit=2)

    assert result["nextCursor"] is None


def test_field_catalog_exposes_sorted_fields_and_relationships() -> None:
    transport = FakeTransport([_ccda_caps()])
    service = SearchService(transport)

    catalog = service.field_catalog("ccda-documents-v1")

    assert catalog.sorted_fields == [
        "documentId",
        "documentTime",
        "ingestTime",
        "narrativeText",
        "narrativeText.keyword",
        "sourceFacilityId",
        "unindexedText",
    ]
    assert "rawObject" not in catalog.present
    assert catalog.text_fields == frozenset({"narrativeText", "unindexedText"})
    assert catalog.keyword_subfields == frozenset({"narrativeText.keyword"})
    assert catalog.resolve_equals_field("narrativeText") == "narrativeText.keyword"
    assert catalog.resolve_equals_field("unindexedText") == "unindexedText"


def test_field_catalog_rejects_unknown_index() -> None:
    transport = FakeTransport([])
    service = SearchService(transport)

    with pytest.raises(SearchRequestError) as captured:
        service.field_catalog("nope")

    assert captured.value.code == INVALID_INDEX
