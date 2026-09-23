import copy
import hashlib
import io
import json
import sys
import urllib.error
import zipfile
from collections.abc import Mapping
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from src.report_catalog import ReportCatalog
from src.report_definition import ReportDefinition, load_report_definition
from src.report_runner import (
    ARCHIVE_TOO_LARGE,
    DEFINITION_LOAD_FAILED,
    DEFINITION_MISMATCH,
    MAX_PARTITION_VALUES,
    MAX_ROW_QUERIES_PER_BATCH,
    MSEARCH_COUNT_MISMATCH,
    MSEARCH_PATH,
    MSEARCH_QUERY_REJECTED,
    MSEARCH_REQUEST_FAILED,
    MSEARCH_RESPONSE_INVALID,
    REPORT_UPLOAD_FAILED,
    RUN_PROGRESS_UPDATE_FAILED,
    DynamoRunProgressStore,
    LoggingRunProgressStore,
    ReportOutput,
    ReportRequestError,
    ReportRunError,
    ReportRunner,
    S3ReportSink,
    SignedMsearchTransport,
    _catalog_definition_provider,
    handler,
    parse_report_request,
)

FROM_TIME = "2026-01-01T00:00:00+00:00"
TO_TIME = "2026-02-01T00:00:00+00:00"
RUN_ID = "run-123"

_BASE_QUERY = {"bool": {"filter": [{"term": {"ROOT.MSH.MSH_9_Message_Type.MSG_1": "ADT"}}]}}


def _make_definition(
    report_id: str,
    sections: list[tuple[str, list[str]]],
) -> ReportDefinition:
    return load_report_definition(
        {
            "report_id": report_id,
            "name": "Test Report",
            "description": "test",
            "partition_field": "sourceFacilityId",
            "time_field": "messageTime",
            "sections": [
                {
                    "seq": section_index + 1,
                    "name": name,
                    "rows": [
                        {
                            "seq": row_index + 1,
                            "label": label,
                            "description": "row",
                            "index": "hl7-messages-v1",
                            "query": copy.deepcopy(_BASE_QUERY),
                        }
                        for row_index, label in enumerate(labels)
                    ],
                }
                for section_index, (name, labels) in enumerate(sections)
            ],
        }
    )


def _event(
    partition_values: list[str],
    report_id: str = "rpt",
    run_id: str = RUN_ID,
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "run_id": run_id,
        "from": FROM_TIME,
        "to": TO_TIME,
        "partition_values": partition_values,
    }


def _msearch_response(counts: list[int]) -> dict[str, Any]:
    return {
        "responses": [
            {"status": 200, "hits": {"total": {"value": count, "relation": "eq"}}}
            for count in counts
        ]
    }


class FakeTransport:
    def __init__(self, responses: list[tuple[int, dict[str, Any]]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        return self.responses.pop(0)


class FakeSink:
    def __init__(self, version: str = "ver-1") -> None:
        self.uploads: list[tuple[str, bytes]] = []
        self._version = version

    def put_report(self, key: str, body: bytes) -> str:
        self.uploads.append((key, body))
        return self._version


class FakeProgress:
    def __init__(self) -> None:
        self.progress: list[tuple[str, int, int]] = []
        self.successes: list[tuple[str, str, str, dict[str, int | None]]] = []
        self.summaries: list[tuple[str, int, int]] = []
        self.failures: list[tuple[str, str]] = []

    def record_progress(self, run_id: str, completed: int, total: int) -> None:
        self.progress.append((run_id, completed, total))

    def record_success(
        self,
        run_id: str,
        zip_s3_key: str,
        version: str,
        row_counts: Mapping[str, int | None],
        rows_executed: int,
        placeholders_skipped: int,
    ) -> None:
        self.successes.append((run_id, zip_s3_key, version, dict(row_counts)))
        self.summaries.append((run_id, rows_executed, placeholders_skipped))

    def record_failure(self, run_id: str, failing_partition: str) -> None:
        self.failures.append((run_id, failing_partition))


def _runner(
    definition: ReportDefinition,
    transport: FakeTransport,
    sink: FakeSink | None = None,
    progress: FakeProgress | None = None,
) -> tuple[ReportRunner, FakeSink, FakeProgress]:
    sink = sink or FakeSink()
    progress = progress or FakeProgress()
    runner = ReportRunner(transport, sink, progress, lambda _report_id: definition)
    return runner, sink, progress


def _read_csv(archive_bytes: bytes, name: str) -> list[list[str]]:
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        text = archive.read(name).decode("utf-8")
    return [line.split(",") for line in text.split("\r\n") if line]


# --------------------------------------------------------------------------- #
# Request validation
# --------------------------------------------------------------------------- #


def test_parse_report_request_accepts_valid_event() -> None:
    request = parse_report_request(_event(["facility-a", "facility-b"]))

    assert request.report_id == "rpt"
    assert request.run_id == RUN_ID
    assert request.from_time == FROM_TIME
    assert request.to_time == TO_TIME
    assert request.partition_values == ("facility-a", "facility-b")


def test_parse_report_request_accepts_maximum_partition_count() -> None:
    values = [f"facility-{index:03d}" for index in range(MAX_PARTITION_VALUES)]
    assert len(values) == 200

    request = parse_report_request(_event(values))

    assert len(request.partition_values) == 200


def test_parse_report_request_rejects_one_over_maximum_partition_count() -> None:
    values = [f"facility-{index:03d}" for index in range(MAX_PARTITION_VALUES + 1)]
    assert len(values) == 201

    with pytest.raises(ReportRequestError) as captured:
        parse_report_request(_event(values))

    assert captured.value.code == "invalid_partition_values"


@pytest.mark.parametrize(
    ("event", "code"),
    [
        ("not-a-mapping", "invalid_event"),
        (
            {
                "report_id": "",
                "run_id": RUN_ID,
                "from": FROM_TIME,
                "to": TO_TIME,
                "partition_values": ["a"],
            },
            "invalid_report_id",
        ),
        (
            {
                "report_id": "rpt",
                "from": FROM_TIME,
                "to": TO_TIME,
                "partition_values": ["a"],
            },
            "invalid_run_id",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": "bad id",
                "from": FROM_TIME,
                "to": TO_TIME,
                "partition_values": ["a"],
            },
            "invalid_run_id",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": RUN_ID,
                "from": "2026-01-01",
                "to": TO_TIME,
                "partition_values": ["a"],
            },
            "invalid_from",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": RUN_ID,
                "from": FROM_TIME,
                "to": "2026-02-01",
                "partition_values": ["a"],
            },
            "invalid_to",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": RUN_ID,
                "from": TO_TIME,
                "to": FROM_TIME,
                "partition_values": ["a"],
            },
            "invalid_time_range",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": RUN_ID,
                "from": FROM_TIME,
                "to": TO_TIME,
                "partition_values": [],
            },
            "invalid_partition_values",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": RUN_ID,
                "from": FROM_TIME,
                "to": TO_TIME,
                "partition_values": ["a"] * 201,
            },
            "invalid_partition_values",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": RUN_ID,
                "from": FROM_TIME,
                "to": TO_TIME,
                "partition_values": ["", "b"],
            },
            "invalid_partition_values",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": RUN_ID,
                "from": FROM_TIME,
                "to": TO_TIME,
                "partition_values": "a",
            },
            "invalid_partition_values",
        ),
        (
            {
                "report_id": "rpt",
                "run_id": RUN_ID,
                "from": FROM_TIME,
                "to": TO_TIME,
                "partition_values": ["a", "a"],
            },
            "duplicate_partition_values",
        ),
    ],
)
def test_parse_report_request_rejects_invalid_events(event: Any, code: str) -> None:
    with pytest.raises(ReportRequestError) as captured:
        parse_report_request(event)
    assert captured.value.code == code


def test_parse_report_request_rejects_naive_timestamp() -> None:
    event = _event(["a"])
    event["from"] = "2026-01-01T00:00:00"
    with pytest.raises(ReportRequestError, match="invalid_from"):
        parse_report_request(event)


def test_parse_report_request_rejects_non_string_timestamp() -> None:
    event = _event(["a"])
    event["from"] = 20260101
    with pytest.raises(ReportRequestError, match="invalid_from"):
        parse_report_request(event)


def test_parse_report_request_rejects_malformed_timestamp() -> None:
    event = _event(["a"])
    event["to"] = "not-a-date"
    with pytest.raises(ReportRequestError, match="invalid_to"):
        parse_report_request(event)


# --------------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------------- #


def test_run_produces_one_csv_per_partition_and_uploads_once() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1", "L2"]), ("SecB", ["L3"])])
    transport = FakeTransport(
        [
            (200, _msearch_response([5, 3, 7])),
            (200, _msearch_response([1, 0, 2])),
        ]
    )
    runner, sink, progress = _runner(definition, transport)

    output = runner.run(_event(["facility-a", "facility-b"]))

    assert isinstance(output, ReportOutput)
    assert output.partition_count == 2
    assert output.run_id == RUN_ID
    assert output.version == "ver-1"
    # One msearch per partition (three rows fit in a single batch); one upload after both.
    assert [call[:2] for call in transport.calls] == [
        ("POST", MSEARCH_PATH),
        ("POST", MSEARCH_PATH),
    ]
    assert len(sink.uploads) == 1
    key, archive_bytes = sink.uploads[0]
    assert output.key == key
    # The archive is keyed by report id and run id under the outputs/ prefix.
    assert key == "outputs/rpt/run-123.zip"

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        assert archive.namelist() == ["facility-a.csv", "facility-b.csv"]

    grid_a = _read_csv(archive_bytes, "facility-a.csv")
    assert grid_a[0] == ["SecA", "", "SecB", ""]
    assert grid_a[1] == ["L1", "5", "L3", "7"]
    assert grid_a[2] == ["L2", "3", "", ""]
    grid_b = _read_csv(archive_bytes, "facility-b.csv")
    assert grid_b[1] == ["L1", "1", "L3", "2"]

    assert progress.progress == [(RUN_ID, 1, 2), (RUN_ID, 2, 2)]
    assert progress.successes == [
        (RUN_ID, output.key, "ver-1", {"S1:R1": 6, "S1:R2": 3, "S2:R1": 9})
    ]
    assert progress.failures == []


def test_msearch_bodies_use_size_zero_and_track_total_hits_with_injected_filters() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport([(200, _msearch_response([9]))])
    runner, _sink, _progress = _runner(definition, transport)

    runner.run(_event(["facility-a"]))

    body = transport.calls[0][2]
    assert body is not None
    lines = body.decode().split("\n")
    assert lines[-1] == ""  # trailing newline
    header = json.loads(lines[0])
    search = json.loads(lines[1])
    assert header == {"index": "hl7-messages-v1"}
    assert search["size"] == 0
    assert search["track_total_hits"] is True
    filters = search["query"]["bool"]["filter"]
    assert {"term": {"sourceFacilityId": "facility-a"}} in filters
    assert {"range": {"messageTime": {"gte": FROM_TIME, "lt": TO_TIME}}} in filters
    # The original stored term survives injection.
    assert {"term": {"ROOT.MSH.MSH_9_Message_Type.MSG_1": "ADT"}} in filters


def test_zero_counts_render_as_zero() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1", "L2"])])
    transport = FakeTransport([(200, _msearch_response([0, 0]))])
    runner, sink, _progress = _runner(definition, transport)

    runner.run(_event(["facility-a"]))

    grid = _read_csv(sink.uploads[0][1], "facility-a.csv")
    assert grid[1] == ["L1", "0"]
    assert grid[2] == ["L2", "0"]


def test_aggregate_row_counts_sum_each_label_across_partitions() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1", "L2"]), ("SecB", ["L3"])])
    transport = FakeTransport(
        [
            (200, _msearch_response([5, 3, 7])),
            (200, _msearch_response([1, 0, 2])),
            (200, _msearch_response([4, 6, 8])),
        ]
    )
    runner, _sink, progress = _runner(definition, transport)

    runner.run(_event(["facility-a", "facility-b", "facility-c"]))

    # Each row is summed across every partition into one aggregate mapping keyed by identity.
    assert progress.successes == [
        (RUN_ID, "outputs/rpt/run-123.zip", "ver-1", {"S1:R1": 10, "S1:R2": 9, "S2:R1": 17})
    ]


def test_aggregate_row_counts_key_duplicate_labels_by_identity() -> None:
    # Both sections reuse the label "Total"; identity keys keep the two counts distinct.
    definition = _make_definition("rpt", [("SecA", ["Total"]), ("SecB", ["Total"])])
    transport = FakeTransport(
        [
            (200, _msearch_response([11, 22])),
            (200, _msearch_response([3, 4])),
        ]
    )
    runner, sink, progress = _runner(definition, transport)

    runner.run(_event(["facility-a", "facility-b"]))

    # The reused label does not collapse: S1:R1 and S2:R1 sum independently.
    assert progress.successes == [
        (RUN_ID, "outputs/rpt/run-123.zip", "ver-1", {"S1:R1": 14, "S2:R1": 26})
    ]
    # Both CSV columns still render the shared label with each section's own count.
    grid = _read_csv(sink.uploads[0][1], "facility-a.csv")
    assert grid[0] == ["SecA", "", "SecB", ""]
    assert grid[1] == ["Total", "11", "Total", "22"]


def test_aggregate_row_counts_are_all_zero_when_every_partition_counts_zero() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1", "L2"])])
    transport = FakeTransport(
        [
            (200, _msearch_response([0, 0])),
            (200, _msearch_response([0, 0])),
        ]
    )
    runner, _sink, progress = _runner(definition, transport)

    runner.run(_event(["facility-a", "facility-b"]))

    assert progress.successes == [
        (RUN_ID, "outputs/rpt/run-123.zip", "ver-1", {"S1:R1": 0, "S1:R2": 0})
    ]


def _mixed_definition(
    report_id: str,
    rows: list[tuple[str, bool]],
) -> ReportDefinition:
    """Build a one-section definition where each row is implemented or a placeholder.

    ``rows`` is a list of ``(label, has_query)`` pairs; a row with ``has_query`` False stores
    a null query (a placeholder), and one with True stores the shared base query.
    """
    return load_report_definition(
        {
            "report_id": report_id,
            "name": "Test Report",
            "description": "test",
            "partition_field": "sourceFacilityId",
            "time_field": "messageTime",
            "sections": [
                {
                    "seq": 1,
                    "name": "SecA",
                    "rows": [
                        {
                            "seq": index + 1,
                            "label": label,
                            "description": "row",
                            "index": "hl7-messages-v1",
                            "query": copy.deepcopy(_BASE_QUERY) if has_query else None,
                        }
                        for index, (label, has_query) in enumerate(rows)
                    ],
                }
            ],
        }
    )


def test_placeholder_rows_are_skipped_and_render_blank() -> None:
    definition = _mixed_definition("rpt", [("L1", True), ("L2", False), ("L3", True)])
    # Only the two implemented rows are counted, so a single msearch carries two queries.
    transport = FakeTransport([(200, _msearch_response([5, 7]))])
    runner, sink, progress = _runner(definition, transport)

    runner.run(_event(["facility-a"]))

    # Exactly one msearch, and its NDJSON holds only the two implemented row queries.
    assert len(transport.calls) == 1
    body = transport.calls[0][2]
    assert body is not None
    # Two implemented rows -> 4 NDJSON lines plus the trailing newline.
    assert len(body.decode().split("\n")) == 5

    grid = _read_csv(sink.uploads[0][1], "facility-a.csv")
    assert grid[1] == ["L1", "5"]
    # The placeholder keeps its label but renders a blank count.
    assert grid[2] == ["L2", ""]
    assert grid[3] == ["L3", "7"]

    # The aggregate keeps the placeholder as None and sums only implemented rows.
    assert progress.successes == [
        (RUN_ID, "outputs/rpt/run-123.zip", "ver-1", {"S1:R1": 5, "S1:R2": None, "S1:R3": 7})
    ]
    # The summary tallies unique implemented vs. placeholder rows.
    assert progress.summaries == [(RUN_ID, 2, 1)]


def test_all_placeholder_definition_issues_no_msearch() -> None:
    definition = _mixed_definition("rpt", [("L1", False), ("L2", False)])
    # No implemented rows means no msearch request is ever issued.
    transport = FakeTransport([])
    runner, sink, progress = _runner(definition, transport)

    output = runner.run(_event(["facility-a"]))

    assert transport.calls == []
    # The archive is still produced and uploaded, with every count blank.
    assert output.partition_count == 1
    grid = _read_csv(sink.uploads[0][1], "facility-a.csv")
    assert grid[1] == ["L1", ""]
    assert grid[2] == ["L2", ""]
    assert progress.successes == [
        (RUN_ID, "outputs/rpt/run-123.zip", "ver-1", {"S1:R1": None, "S1:R2": None})
    ]
    assert progress.summaries == [(RUN_ID, 0, 2)]


def test_placeholders_are_excluded_from_batching_across_multiple_batches() -> None:
    # 21 implemented rows require two batches (20 + 1); the 4 placeholders are never sent.
    rows: list[tuple[str, bool]] = [(f"L{index:02d}", True) for index in range(21)]
    rows += [(f"P{index:02d}", False) for index in range(4)]
    definition = _mixed_definition("rpt", rows)
    transport = FakeTransport(
        [
            (200, _msearch_response([1] * MAX_ROW_QUERIES_PER_BATCH)),
            (200, _msearch_response([1])),
        ]
    )
    runner, _sink, progress = _runner(definition, transport)

    runner.run(_event(["facility-a"]))

    # Two msearch requests for the 21 implemented rows; the placeholders add no queries.
    assert len(transport.calls) == 2
    first_body = transport.calls[0][2]
    second_body = transport.calls[1][2]
    assert first_body is not None
    assert second_body is not None
    assert len(first_body.decode().split("\n")) == 41
    assert len(second_body.decode().split("\n")) == 3
    assert progress.summaries == [(RUN_ID, 21, 4)]


def test_int_total_shorthand_is_parsed() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport([(200, {"responses": [{"status": 200, "hits": {"total": 42}}]})])
    runner, sink, _progress = _runner(definition, transport)

    runner.run(_event(["facility-a"]))

    grid = _read_csv(sink.uploads[0][1], "facility-a.csv")
    assert grid[1] == ["L1", "42"]


# --------------------------------------------------------------------------- #
# Batching
# --------------------------------------------------------------------------- #


def test_more_than_twenty_rows_are_split_into_batches() -> None:
    labels = [f"L{index:02d}" for index in range(21)]
    definition = _make_definition("rpt", [("SecA", labels)])
    transport = FakeTransport(
        [
            (200, _msearch_response([1] * MAX_ROW_QUERIES_PER_BATCH)),
            (200, _msearch_response([1])),
        ]
    )
    runner, sink, _progress = _runner(definition, transport)

    runner.run(_event(["facility-a"]))

    # Twenty-one rows for one partition require two _msearch requests.
    assert len(transport.calls) == 2
    first_body = transport.calls[0][2]
    second_body = transport.calls[1][2]
    assert first_body is not None
    assert second_body is not None
    # First batch: 20 row queries -> 40 NDJSON lines plus the trailing newline.
    assert len(first_body.decode().split("\n")) == 41
    assert len(second_body.decode().split("\n")) == 3
    assert len(sink.uploads) == 1


# --------------------------------------------------------------------------- #
# Failures
# --------------------------------------------------------------------------- #


def test_partition_failure_records_failing_partition_and_skips_upload() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport(
        [
            (200, _msearch_response([4])),
            (503, {"error": {"type": "cluster_block_exception"}}),
        ]
    )
    runner, sink, progress = _runner(definition, transport)

    with pytest.raises(ReportRunError, match=MSEARCH_REQUEST_FAILED) as captured:
        runner.run(_event(["facility-a", "facility-b"]))

    assert captured.value.http_status == 503
    assert captured.value.backend_error_type == "cluster_block_exception"
    assert sink.uploads == []
    assert progress.progress == [(RUN_ID, 1, 2)]
    assert progress.failures == [(RUN_ID, "facility-b")]
    assert progress.successes == []


def test_per_query_rejection_sanitizes_unknown_backend_error() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport(
        [
            (
                200,
                {"responses": [{"status": 400, "error": {"type": "patient-sensitive-detail"}}]},
            )
        ]
    )
    runner, sink, progress = _runner(definition, transport)

    with pytest.raises(ReportRunError, match=MSEARCH_QUERY_REJECTED) as captured:
        runner.run(_event(["facility-a"]))

    assert captured.value.http_status == 400
    assert captured.value.backend_error_type == "other"
    assert sink.uploads == []
    assert progress.failures == [(RUN_ID, "facility-a")]


@pytest.mark.parametrize(
    "response",
    [
        {"responses": [{"status": 200}]},
        {"responses": [{"status": 200, "hits": {"total": {"value": -1}}}]},
        {"responses": [{"status": 200, "hits": {"total": {"value": True}}}]},
        {"responses": [{"status": 200, "hits": {"total": "many"}}]},
        {"responses": ["not-a-dict"]},
    ],
)
def test_malformed_msearch_responses_are_rejected(response: dict[str, Any]) -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport([(200, response)])
    runner, _sink, _progress = _runner(definition, transport)

    with pytest.raises(ReportRunError, match=MSEARCH_RESPONSE_INVALID):
        runner.run(_event(["facility-a"]))


def test_msearch_response_count_mismatch_is_rejected() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1", "L2"])])
    transport = FakeTransport([(200, _msearch_response([1]))])
    runner, _sink, _progress = _runner(definition, transport)

    with pytest.raises(ReportRunError, match=MSEARCH_COUNT_MISMATCH):
        runner.run(_event(["facility-a"]))


def test_definition_mismatch_is_rejected() -> None:
    definition = _make_definition("other", [("SecA", ["L1"])])
    transport = FakeTransport([])
    runner, _sink, _progress = _runner(definition, transport)

    with pytest.raises(ReportRunError, match=DEFINITION_MISMATCH):
        runner.run(_event(["facility-a"], report_id="rpt"))


def test_definition_provider_failure_is_sanitized() -> None:
    def failing_provider(_report_id: str) -> ReportDefinition:
        raise RuntimeError("detail")

    runner = ReportRunner(FakeTransport([]), FakeSink(), FakeProgress(), failing_provider)

    with pytest.raises(ReportRunError, match=DEFINITION_LOAD_FAILED):
        runner.run(_event(["facility-a"]))


def test_unexpected_partition_error_is_sanitized_and_recorded() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])

    class ExplodingTransport:
        def request(
            self,
            _method: str,
            _path: str,
            _body: bytes | None = None,
        ) -> tuple[int, dict[str, Any]]:
            raise RuntimeError("detail")

    progress = FakeProgress()
    runner = ReportRunner(ExplodingTransport(), FakeSink(), progress, lambda _r: definition)

    with pytest.raises(ReportRunError, match=MSEARCH_REQUEST_FAILED):
        runner.run(_event(["facility-a"]))
    assert progress.failures == [(RUN_ID, "facility-a")]


def test_archive_size_bound_is_enforced(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport([(200, _msearch_response([1]))])
    runner, sink, _progress = _runner(definition, transport)
    monkeypatch.setattr("src.report_runner.MAX_ARCHIVE_BYTES", 1)

    with pytest.raises(ReportRunError, match=ARCHIVE_TOO_LARGE):
        runner.run(_event(["facility-a"]))
    assert sink.uploads == []


# --------------------------------------------------------------------------- #
# No mutation of the stored definition query
# --------------------------------------------------------------------------- #


def test_stored_query_is_not_mutated_by_injection() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    original_query = copy.deepcopy(definition.sections[0].rows[0].query)
    transport = FakeTransport(
        [
            (200, _msearch_response([1])),
            (200, _msearch_response([1])),
        ]
    )
    runner, _sink, _progress = _runner(definition, transport)

    runner.run(_event(["facility-a", "facility-b"]))

    assert definition.sections[0].rows[0].query == original_query


def test_non_bool_query_is_wrapped_without_mutation() -> None:
    definition = load_report_definition(
        {
            "report_id": "rpt",
            "name": "Test",
            "description": "test",
            "partition_field": "sourceFacilityId",
            "time_field": "messageTime",
            "sections": [
                {
                    "seq": 1,
                    "name": "SecA",
                    "rows": [
                        {
                            "seq": 1,
                            "label": "L1",
                            "description": "row",
                            "index": "hl7-messages-v1",
                            "query": {"match_all": {}},
                        }
                    ],
                }
            ],
        }
    )
    original_query = copy.deepcopy(definition.sections[0].rows[0].query)
    transport = FakeTransport([(200, _msearch_response([1]))])
    runner, _sink, _progress = _runner(definition, transport)

    runner.run(_event(["facility-a"]))

    body = transport.calls[0][2]
    assert body is not None
    search = json.loads(body.decode().split("\n")[1])
    filters = search["query"]["bool"]["filter"]
    assert {"match_all": {}} in filters
    assert definition.sections[0].rows[0].query == original_query


def test_bool_filter_single_clause_is_normalized_to_list() -> None:
    definition = load_report_definition(
        {
            "report_id": "rpt",
            "name": "Test",
            "description": "test",
            "partition_field": "sourceFacilityId",
            "time_field": "messageTime",
            "sections": [
                {
                    "seq": 1,
                    "name": "SecA",
                    "rows": [
                        {
                            "seq": 1,
                            "label": "L1",
                            "description": "row",
                            "index": "hl7-messages-v1",
                            "query": {"bool": {"filter": {"term": {"ROOT.PID._present": "1"}}}},
                        }
                    ],
                }
            ],
        }
    )
    transport = FakeTransport([(200, _msearch_response([1]))])
    runner, _sink, _progress = _runner(definition, transport)

    runner.run(_event(["facility-a"]))

    body = transport.calls[0][2]
    assert body is not None
    filters = json.loads(body.decode().split("\n")[1])["query"]["bool"]["filter"]
    assert {"term": {"ROOT.PID._present": "1"}} in filters
    assert {"term": {"sourceFacilityId": "facility-a"}} in filters


def test_sanitized_filename_collisions_are_disambiguated() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport(
        [
            (200, _msearch_response([1])),
            (200, _msearch_response([2])),
        ]
    )
    runner, sink, _progress = _runner(definition, transport)

    runner.run(_event(["a/b", "a_b"]))

    with zipfile.ZipFile(io.BytesIO(sink.uploads[0][1])) as archive:
        assert archive.namelist() == ["a_b.csv", "a_b-1.csv"]


def test_missing_responses_key_is_rejected() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport([(200, {})])
    runner, _sink, _progress = _runner(definition, transport)

    with pytest.raises(ReportRunError, match=MSEARCH_RESPONSE_INVALID):
        runner.run(_event(["facility-a"]))


def test_request_failure_without_error_type_has_no_backend_type() -> None:
    definition = _make_definition("rpt", [("SecA", ["L1"])])
    transport = FakeTransport([(500, {})])
    runner, _sink, _progress = _runner(definition, transport)

    with pytest.raises(ReportRunError, match=MSEARCH_REQUEST_FAILED) as captured:
        runner.run(_event(["facility-a"]))
    assert captured.value.http_status == 500
    assert captured.value.backend_error_type is None


# --------------------------------------------------------------------------- #
# Sinks, progress store, definition provider, transport, handler
# --------------------------------------------------------------------------- #


def test_s3_report_sink_uploads_and_returns_version() -> None:
    class FakeS3:
        def __init__(self) -> None:
            self.kwargs: dict[str, Any] = {}

        def put_object(self, **kwargs: Any) -> dict[str, Any]:
            self.kwargs = kwargs
            return {"VersionId": "v9"}

    client = FakeS3()
    sink = S3ReportSink(client, bucket="report-bucket")

    version = sink.put_report("outputs/rpt/run-1.zip", b"payload")

    assert version == "v9"
    assert client.kwargs["Bucket"] == "report-bucket"
    assert client.kwargs["Key"] == "outputs/rpt/run-1.zip"
    assert client.kwargs["Body"] == b"payload"
    assert client.kwargs["ContentType"] == "application/zip"


def test_s3_report_sink_returns_empty_version_when_unversioned() -> None:
    class FakeS3:
        def put_object(self, **_kwargs: Any) -> dict[str, Any]:
            return {}

    sink = S3ReportSink(FakeS3(), bucket="report-bucket")

    assert sink.put_report("outputs/rpt/run-1.zip", b"payload") == ""


def test_s3_report_sink_sanitizes_upload_failure() -> None:
    class ExplodingS3:
        def put_object(self, **_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("detail")

    sink = S3ReportSink(ExplodingS3(), bucket="report-bucket")

    with pytest.raises(ReportRunError, match=REPORT_UPLOAD_FAILED):
        sink.put_report("key", b"payload")


def test_logging_progress_store_emits_sanitized_events(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = LoggingRunProgressStore()
    with caplog.at_level("INFO", logger="src.report_runner"):
        store.record_progress(RUN_ID, 1, 3)
        store.record_success(RUN_ID, "outputs/rpt/run-123.zip", "v9", {"L1": 7, "L2": 11}, 2, 0)
    with caplog.at_level("ERROR", logger="src.report_runner"):
        store.record_failure(RUN_ID, "facility/../a")

    messages = [record.getMessage() for record in caplog.records]
    assert any('"event":"report_run_progress"' in message for message in messages)

    failure = next(message for message in messages if '"status":"failed"' in message)
    failure_payload = json.loads(failure)
    assert failure_payload["runId"] == RUN_ID
    # The failing partition (a facility identifier) is never written to logs.
    assert "failingPartition" not in failure_payload

    success = next(message for message in messages if '"status":"complete"' in message)
    # The archive key, version, and aggregate row counts never appear in logs.
    assert "outputs/rpt/run-123.zip" not in success
    assert "v9" not in success
    assert "rowCounts" not in success
    assert "L1" not in success
    assert "L2" not in success

    # No facility identifier leaks into any emitted log line.
    assert all("facility" not in message for message in messages)


class _FakeUpdateDynamo:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._error = error

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return {}


def test_dynamo_progress_store_increments_completed_partitions() -> None:
    dynamo = _FakeUpdateDynamo()
    store = DynamoRunProgressStore(dynamo, table_name="runs")

    store.record_progress(RUN_ID, 2, 5)

    call = dynamo.calls[0]
    assert call["TableName"] == "runs"
    assert call["Key"] == {"runId": {"S": RUN_ID}}
    assert call["ConditionExpression"] == "attribute_exists(runId)"
    assert "completedPartitions = :completed" in call["UpdateExpression"]
    assert call["ExpressionAttributeValues"][":completed"] == {"N": "2"}
    assert call["ExpressionAttributeValues"][":total"] == {"N": "5"}


def test_dynamo_progress_store_marks_complete_with_key_and_version() -> None:
    dynamo = _FakeUpdateDynamo()
    store = DynamoRunProgressStore(dynamo, table_name="runs")

    store.record_success(RUN_ID, "outputs/rpt/run-123.zip", "v9", {"L1": 6, "L2": 0}, 2, 0)

    call = dynamo.calls[0]
    values = call["ExpressionAttributeValues"]
    assert values[":status"] == {"S": "complete"}
    assert values[":zipS3Key"] == {"S": "outputs/rpt/run-123.zip"}
    assert values[":version"] == {"S": "v9"}
    assert ":finishedAt" in values
    # Aggregate row counts are stored as a DynamoDB map of numeric (N) values.
    assert values[":rowCounts"] == {"M": {"L1": {"N": "6"}, "L2": {"N": "0"}}}
    assert "rowCounts = :rowCounts" in call["UpdateExpression"]
    # Unique-row tallies are stored alongside the counts.
    assert values[":rowsExecuted"] == {"N": "2"}
    assert values[":placeholdersSkipped"] == {"N": "0"}
    assert "rowsExecuted = :rowsExecuted" in call["UpdateExpression"]
    assert "placeholdersSkipped = :placeholdersSkipped" in call["UpdateExpression"]
    # "version" is a DynamoDB reserved word, so it is bound through a name alias.
    assert call["ExpressionAttributeNames"]["#version"] == "version"
    assert call["ExpressionAttributeNames"]["#status"] == "status"


def test_dynamo_progress_store_success_omits_version_when_absent() -> None:
    dynamo = _FakeUpdateDynamo()
    store = DynamoRunProgressStore(dynamo, table_name="runs")

    store.record_success(RUN_ID, "outputs/rpt/run-123.zip", "", {"L1": 3}, 1, 0)

    call = dynamo.calls[0]
    assert ":version" not in call["ExpressionAttributeValues"]
    assert "#version" not in call["ExpressionAttributeNames"]
    # Row counts are still persisted even when the object is unversioned.
    assert call["ExpressionAttributeValues"][":rowCounts"] == {"M": {"L1": {"N": "3"}}}


def test_dynamo_progress_store_success_encodes_empty_row_counts() -> None:
    dynamo = _FakeUpdateDynamo()
    store = DynamoRunProgressStore(dynamo, table_name="runs")

    store.record_success(RUN_ID, "outputs/rpt/run-123.zip", "v9", {}, 0, 0)

    call = dynamo.calls[0]
    assert call["ExpressionAttributeValues"][":rowCounts"] == {"M": {}}
    assert "rowCounts = :rowCounts" in call["UpdateExpression"]


def test_dynamo_progress_store_marks_failed_with_partition() -> None:
    dynamo = _FakeUpdateDynamo()
    store = DynamoRunProgressStore(dynamo, table_name="runs")

    store.record_failure(RUN_ID, "facility-b")

    call = dynamo.calls[0]
    values = call["ExpressionAttributeValues"]
    assert values[":status"] == {"S": "failed"}
    # The failing partition is persisted for operators (never logged).
    assert values[":failingPartition"] == {"S": "facility-b"}
    assert ":finishedAt" in values


def test_dynamo_progress_store_failure_log_excludes_facility(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dynamo = _FakeUpdateDynamo()
    store = DynamoRunProgressStore(dynamo, table_name="runs")

    with caplog.at_level("ERROR", logger="src.report_runner"):
        store.record_failure(RUN_ID, "facility-secret")

    messages = [record.getMessage() for record in caplog.records]
    assert all("facility-secret" not in message for message in messages)


def test_dynamo_progress_store_sanitizes_backend_error() -> None:
    dynamo = _FakeUpdateDynamo(error=RuntimeError("detail"))
    store = DynamoRunProgressStore(dynamo, table_name="runs")

    with pytest.raises(ReportRunError, match=RUN_PROGRESS_UPDATE_FAILED):
        store.record_progress(RUN_ID, 1, 2)


def _catalog_definition_dict(report_id: str = "rpt") -> dict[str, Any]:
    return {
        "report_id": report_id,
        "name": "Test",
        "description": "test",
        "partition_field": "sourceFacilityId",
        "time_field": "messageTime",
        "sections": [
            {
                "seq": 1,
                "name": "SecA",
                "rows": [
                    {
                        "seq": 1,
                        "label": "L1",
                        "description": "row",
                        "index": "hl7-messages-v1",
                        "query": copy.deepcopy(_BASE_QUERY),
                    }
                ],
            }
        ],
    }


class _CatalogDynamoDouble:
    """Minimal in-memory single-table double for the row-granular Reports catalog.

    It supports exactly the writes an import issues (``put_item`` for the META item,
    ``batch_write_item`` for the section and row body, and a ``transact_write_items`` to
    publish the draft and record the import audit) and the single partition-key ``query``
    that assembles a report. Every low-level method name is
    recorded so a test can prove the catalog reads a whole definition with one ``Query``
    and never issues any other operation (in particular no S3 ``get_object``).
    """

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.query_calls: list[dict[str, Any]] = []
        self.method_calls: list[str] = []

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("put_item")
        item = kwargs["Item"]
        self.items[(item["PK"]["S"], item["SK"]["S"])] = dict(item)
        return {}

    def batch_write_item(self, **kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("batch_write_item")
        for requests in kwargs["RequestItems"].values():
            for request in requests:
                item = request["PutRequest"]["Item"]
                self.items[(item["PK"]["S"], item["SK"]["S"])] = dict(item)
        return {"UnprocessedItems": {}}

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("update_item")
        key = (kwargs["Key"]["PK"]["S"], kwargs["Key"]["SK"]["S"])
        item = self.items.get(key, {"PK": kwargs["Key"]["PK"], "SK": kwargs["Key"]["SK"]})
        names = kwargs.get("ExpressionAttributeNames", {})
        values = kwargs.get("ExpressionAttributeValues", {})
        body = kwargs["UpdateExpression"][len("SET ") :]
        for assignment in body.split(","):
            lhs, rhs = (part.strip() for part in assignment.split("="))
            attribute = names.get(lhs, lhs) if lhs.startswith("#") else lhs
            item[attribute] = values[rhs]
        self.items[key] = item
        return {}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("query")
        self.query_calls.append(kwargs)
        partition = kwargs["ExpressionAttributeValues"][":pk"]["S"]
        matches = [item for (pk, _), item in self.items.items() if pk == partition]
        matches.sort(key=lambda item: item["SK"]["S"])
        return {"Items": matches}

    def delete_item(self, **kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("delete_item")
        key = (kwargs["Key"]["PK"]["S"], kwargs["Key"]["SK"]["S"])
        self.items.pop(key, None)
        return {}

    def scan(self, **_kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("scan")
        return {"Items": [dict(item) for item in self.items.values()]}

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("get_item")
        key = (kwargs["Key"]["PK"]["S"], kwargs["Key"]["SK"]["S"])
        existing = self.items.get(key)
        return {"Item": dict(existing)} if existing is not None else {}

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.method_calls.append("transact_write_items")
        for entry in kwargs["TransactItems"]:
            op, spec = next(iter(entry.items()))
            if op == "Put":
                item = spec["Item"]
                self.items[(item["PK"]["S"], item["SK"]["S"])] = dict(item)
            elif op == "Update":
                key = (spec["Key"]["PK"]["S"], spec["Key"]["SK"]["S"])
                item = self.items.get(key, {"PK": spec["Key"]["PK"], "SK": spec["Key"]["SK"]})
                names = spec.get("ExpressionAttributeNames", {})
                values = spec.get("ExpressionAttributeValues", {})
                body = spec["UpdateExpression"][len("SET ") :]
                for assignment in body.split(","):
                    lhs, rhs = (part.strip() for part in assignment.split("="))
                    attribute = names.get(lhs, lhs) if lhs.startswith("#") else lhs
                    item[attribute] = values[rhs]
                self.items[key] = item
            elif op == "Delete":
                self.items.pop((spec["Key"]["PK"]["S"], spec["Key"]["SK"]["S"]), None)
        return {}


def _seed_catalog(report_id: str = "rpt") -> _CatalogDynamoDouble:
    dynamo = _CatalogDynamoDouble()
    ReportCatalog(dynamo, table_name="report-catalog").import_report(
        json.dumps(_catalog_definition_dict(report_id)),
        updated_by="auth0|editor",
    )
    return dynamo


def test_catalog_definition_provider_loads_definition() -> None:
    dynamo = _seed_catalog("rpt")
    dynamo.method_calls.clear()
    provider = _catalog_definition_provider(dynamo, table_name="report-catalog")

    definition = provider("rpt")

    assert definition.report_id == "rpt"
    assert definition.sections[0].rows[0].label == "L1"
    # The whole definition is assembled with a single DynamoDB Query and nothing else --
    # in particular, no S3 get_object is issued to read the definition.
    assert dynamo.method_calls == ["query"]
    assert len(dynamo.query_calls) == 1


def test_catalog_definition_provider_sanitizes_read_failure() -> None:
    class ExplodingDynamo:
        def query(self, **_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("detail")

    provider = _catalog_definition_provider(ExplodingDynamo(), table_name="report-catalog")

    with pytest.raises(ReportRunError, match=DEFINITION_LOAD_FAILED):
        provider("rpt")


def test_catalog_definition_provider_sanitizes_missing_report() -> None:
    dynamo = _CatalogDynamoDouble()
    provider = _catalog_definition_provider(dynamo, table_name="report-catalog")

    with pytest.raises(ReportRunError, match=DEFINITION_LOAD_FAILED):
        provider("rpt")


def test_runner_loads_definition_with_one_query_and_no_s3_definition_read() -> None:
    dynamo = _seed_catalog("rpt")
    dynamo.method_calls.clear()
    dynamo.query_calls.clear()
    provider = _catalog_definition_provider(dynamo, table_name="report-catalog")
    transport = FakeTransport([(200, _msearch_response([5]))])
    sink = FakeSink()
    progress = FakeProgress()
    runner = ReportRunner(transport, sink, progress, provider)

    output = runner.run(_event(["facility-a"], report_id="rpt"))

    # The definition is assembled from DynamoDB with exactly one Query for the whole run.
    assert dynamo.method_calls == ["query"]
    assert len(dynamo.query_calls) == 1
    # S3 is touched only to upload the finished archive, never to read the definition.
    assert len(sink.uploads) == 1
    assert output.partition_count == 1


# --- Signed transport (fake botocore, mirroring the search_store test style) --- #


class _FakeCredentials:
    def get_frozen_credentials(self) -> object:
        return object()


class _FakeSession:
    credentials: ClassVar[_FakeCredentials | None] = _FakeCredentials()

    def get_credentials(self) -> _FakeCredentials | None:
        return self.credentials


class _FakeAwsRequest:
    def __init__(
        self,
        *,
        method: str,
        url: str,
        data: bytes | None,
        headers: dict[str, str],
    ) -> None:
        self.method = method
        self.url = url
        self.data = data
        self.headers = headers

    def prepare(self) -> SimpleNamespace:
        return SimpleNamespace(headers=self.headers)


class _FakeSigV4Auth:
    signed_headers: ClassVar[list[dict[str, str]]] = []

    def __init__(self, _credentials: object, service: str, region: str) -> None:
        self.service = service
        self.region = region

    def add_auth(self, request: _FakeAwsRequest) -> None:
        self.signed_headers.append(dict(request.headers))
        request.headers["Authorization"] = "synthetic-signature"


class _FakeHttpResponse:
    status = 200

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return b'{"responses":[]}'


def _install_fake_botocore(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeSigV4Auth.signed_headers.clear()
    _FakeSession.credentials = _FakeCredentials()
    package = ModuleType("botocore")
    package.__path__ = []
    auth = ModuleType("botocore.auth")
    auth.SigV4Auth = _FakeSigV4Auth  # type: ignore[attr-defined]
    awsrequest = ModuleType("botocore.awsrequest")
    awsrequest.AWSRequest = _FakeAwsRequest  # type: ignore[attr-defined]
    session = ModuleType("botocore.session")
    session.Session = _FakeSession  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "botocore", package)
    monkeypatch.setitem(sys.modules, "botocore.auth", auth)
    monkeypatch.setitem(sys.modules, "botocore.awsrequest", awsrequest)
    monkeypatch.setitem(sys.modules, "botocore.session", session)


def test_signed_transport_uses_ndjson_content_type_for_msearch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_botocore(monkeypatch)
    monkeypatch.setattr(
        "src.report_runner.urllib.request.urlopen",
        lambda *_args, **_kwargs: _FakeHttpResponse(),
    )
    transport = SignedMsearchTransport(
        endpoint="search.example", region="us-west-2", service="aoss"
    )

    status, response = transport.request("POST", MSEARCH_PATH, b"{}\n{}\n")

    assert status == 200
    assert response == {"responses": []}
    assert transport.endpoint == "https://search.example"
    assert _FakeSigV4Auth.signed_headers == [
        {
            "Content-Type": "application/x-ndjson",
            "x-amz-content-sha256": hashlib.sha256(b"{}\n{}\n").hexdigest(),
        }
    ]


def test_signed_transport_returns_http_error_without_logging_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_botocore(monkeypatch)
    http_error = urllib.error.HTTPError(
        "https://search.example/_msearch",
        429,
        "rejected",
        hdrs=cast(Any, None),
        fp=io.BytesIO(b'{"error":{"type":"too_many_requests_exception"}}'),
    )

    def raise_http_error(*_args: object, **_kwargs: object) -> _FakeHttpResponse:
        raise http_error

    monkeypatch.setattr("src.report_runner.urllib.request.urlopen", raise_http_error)
    transport = SignedMsearchTransport(
        endpoint="https://search.example/", region="us-west-2", service="aoss"
    )

    status, response = transport.request("POST", MSEARCH_PATH, b"{}\n")

    assert status == 429
    assert response == {"error": {"type": "too_many_requests_exception"}}


def test_signed_transport_rejects_http_and_missing_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="must use HTTPS"):
        SignedMsearchTransport(endpoint="http://search.example", region="us-west-2", service="aoss")

    _install_fake_botocore(monkeypatch)
    _FakeSession.credentials = None
    transport = SignedMsearchTransport(
        endpoint="search.example", region="us-west-2", service="aoss"
    )
    with pytest.raises(ReportRunError, match="credentials are unavailable"):
        transport.request("POST", MSEARCH_PATH, b"{}\n")


# --- Handler --- #


class _FakeRunner:
    def __init__(self, result: Any) -> None:
        self._result = result

    def run(self, _event: dict[str, Any]) -> ReportOutput:
        if isinstance(self._result, Exception):
            raise self._result
        return cast(ReportOutput, self._result)


def test_handler_returns_success_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    output = ReportOutput(
        report_id="rpt",
        run_id=RUN_ID,
        key="outputs/rpt/run-123.zip",
        version="v9",
        partition_count=2,
        byte_size=321,
    )
    monkeypatch.setattr("src.report_runner._runtime_runner", lambda: _FakeRunner(output))

    result = handler(_event(["a", "b"]), None)

    assert result == {
        "status": "succeeded",
        "reportId": "rpt",
        "runId": RUN_ID,
        "zipS3Key": "outputs/rpt/run-123.zip",
        "version": "v9",
        "partitionCount": 2,
        "byteSize": 321,
    }


def test_handler_returns_invalid_for_request_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.report_runner._runtime_runner",
        lambda: _FakeRunner(ReportRequestError("invalid_report_id")),
    )

    result = handler({}, None)

    assert result == {"status": "invalid", "error": "invalid_report_id"}


def test_handler_returns_failed_for_run_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.report_runner._runtime_runner",
        lambda: _FakeRunner(ReportRunError(MSEARCH_REQUEST_FAILED, http_status=503)),
    )

    result = handler(_event(["a"]), None)

    assert result == {"status": "failed", "error": "report_run_failed"}


def test_runtime_runner_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.report_runner as module

    monkeypatch.setattr(module, "_RUNTIME_RUNNER", None)
    monkeypatch.setenv("OPENSEARCH_ENDPOINT", "search.example")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("REPORT_BUCKET", "report-bucket")
    monkeypatch.setenv("REPORT_CATALOG_TABLE", "report-catalog")
    monkeypatch.setenv("RUNS_TABLE", "runs-table")
    monkeypatch.setattr(module, "_aws_client", lambda _service: object())

    first = module._runtime_runner()
    second = module._runtime_runner()

    assert first is second
