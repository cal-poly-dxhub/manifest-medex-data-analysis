import json
import logging
import threading
import time
from typing import Any

import pytest
from worker import worker


class FakeSqs:
    def __init__(self, batches: list[list[dict[str, Any]]]) -> None:
        self.batches = batches
        self.receive_calls: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.visibility_calls: list[dict[str, Any]] = []

    def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        self.receive_calls.append(kwargs)
        if not self.batches:
            return {}
        return {"Messages": self.batches.pop(0)}

    def delete_message_batch(self, **kwargs: Any) -> dict[str, Any]:
        self.deleted.append(kwargs)
        return {"Successful": [{"Id": entry["Id"]} for entry in kwargs["Entries"]]}

    def change_message_visibility_batch(self, **kwargs: Any) -> dict[str, Any]:
        self.visibility_calls.append(kwargs)
        return {"Successful": [{"Id": entry["Id"]} for entry in kwargs["Entries"]]}


def _message(index: int) -> dict[str, Any]:
    return {
        "MessageId": f"m{index}",
        "ReceiptHandle": f"r{index}",
        "Body": json.dumps({"secret": "PHI-BODY"}),
        "Attributes": {"ApproximateReceiveCount": "1"},
    }


def _ok_handler(event: dict[str, Any], _context: Any) -> dict[str, list[dict[str, str]]]:
    assert all({"messageId", "receiptHandle", "body"} <= set(r) for r in event["Records"])
    return {"batchItemFailures": []}


def test_read_settings_applies_defaults_and_types() -> None:
    settings = worker.read_settings({"QUEUE_URL": "https://q", "FORMAT": "hl7"})
    assert settings == {"queue_url": "https://q", "format": "hl7", "workers": 4, "batch_size": 10}

    settings = worker.read_settings(
        {"QUEUE_URL": "https://q", "FORMAT": "ccda", "WORKERS": "2", "RECEIVE_BATCH_SIZE": "1"}
    )
    assert (settings["workers"], settings["batch_size"]) == (2, 1)


@pytest.mark.parametrize(
    ("environ", "message"),
    [
        ({"FORMAT": "hl7"}, "QUEUE_URL is required"),
        ({"QUEUE_URL": "q", "FORMAT": "fhir"}, "FORMAT must be one of"),
        ({"QUEUE_URL": "q", "FORMAT": "hl7", "WORKERS": "x"}, "must be integers"),
        ({"QUEUE_URL": "q", "FORMAT": "hl7", "WORKERS": "0"}, "at least 1"),
        ({"QUEUE_URL": "q", "FORMAT": "hl7", "RECEIVE_BATCH_SIZE": "11"}, "between 1 and 10"),
    ],
)
def test_read_settings_rejects_invalid_environment(environ: dict[str, str], message: str) -> None:
    with pytest.raises(worker.WorkerConfigError, match=message):
        worker.read_settings(environ)


def test_process_once_long_polls_and_deletes_every_success() -> None:
    sqs = FakeSqs([[_message(1), _message(2)]])
    assert worker.process_once(sqs, _ok_handler, queue_url="https://q", batch_size=10) == (2, 0)
    assert sqs.receive_calls[0]["WaitTimeSeconds"] == 20
    assert sqs.receive_calls[0]["MaxNumberOfMessages"] == 10
    assert sqs.deleted[0]["Entries"] == [
        {"Id": "0", "ReceiptHandle": "r1"},
        {"Id": "1", "ReceiptHandle": "r2"},
    ]


def test_process_once_keeps_reported_failures_on_the_queue() -> None:
    def partial(_event: dict[str, Any], _context: Any) -> dict[str, list[dict[str, str]]]:
        return {"batchItemFailures": [{"itemIdentifier": "m2"}]}

    sqs = FakeSqs([[_message(1), _message(2), _message(3)]])
    assert worker.process_once(sqs, partial, queue_url="q", batch_size=10) == (2, 1)
    receipts = [entry["ReceiptHandle"] for entry in sqs.deleted[0]["Entries"]]
    assert receipts == ["r1", "r3"]


def test_process_once_does_not_delete_when_every_message_fails() -> None:
    def failing(event: dict[str, Any], _context: Any) -> dict[str, list[dict[str, str]]]:
        return {"batchItemFailures": [{"itemIdentifier": r["messageId"]} for r in event["Records"]]}

    sqs = FakeSqs([[_message(1)]])
    assert worker.process_once(sqs, failing, queue_url="q", batch_size=1) == (0, 1)
    assert sqs.deleted == []


def test_handler_exception_leaves_batch_and_logs_no_message_content(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def explode(_event: dict[str, Any], _context: Any) -> dict[str, list[dict[str, str]]]:
        msg = "PHI-BODY leaked into exception text"
        raise RuntimeError(msg)

    sqs = FakeSqs([[_message(1)]])
    with caplog.at_level(logging.INFO, logger="ingestion_worker"):
        assert worker.process_once(sqs, explode, queue_url="q", batch_size=10) == (0, 1)
    assert sqs.deleted == []
    assert "batch_failed" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "PHI-BODY" not in caplog.text


def test_empty_receive_processes_nothing() -> None:
    sqs = FakeSqs([])
    assert worker.process_once(sqs, _ok_handler, queue_url="q", batch_size=10) == (0, 0)
    assert sqs.deleted == []


def test_run_loop_finishes_the_held_batch_then_stops() -> None:
    stop = threading.Event()

    def stop_after(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
        # SIGTERM arrives mid-batch: the batch still completes and is deleted.
        stop.set()
        return _ok_handler(event, context)

    sqs = FakeSqs([[_message(1)], [_message(2)]])
    worker.run_loop(sqs, stop_after, queue_url="q", batch_size=10, stop=stop)  # type: ignore[arg-type]
    assert len(sqs.receive_calls) == 1
    assert [entry["ReceiptHandle"] for entry in sqs.deleted[0]["Entries"]] == ["r1"]


def test_run_loop_backs_off_on_receive_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    stop = threading.Event()
    waits: list[float] = []

    class FlakySqs(FakeSqs):
        def receive_message(self, **_kwargs: Any) -> dict[str, Any]:
            msg = "endpoint unreachable"
            raise ConnectionError(msg)

    original_wait = stop.wait

    def record_wait(timeout: float | None = None) -> bool:
        waits.append(timeout or 0)
        stop.set()
        return original_wait(0)

    monkeypatch.setattr(stop, "wait", record_wait)
    worker.run_loop(FlakySqs([]), _ok_handler, queue_url="q", batch_size=10, stop=stop)  # type: ignore[arg-type]
    assert waits == [worker.ERROR_BACKOFF_SECONDS]


def test_main_rejects_invalid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QUEUE_URL", raising=False)
    monkeypatch.setenv("FORMAT", "hl7")
    assert worker.main() == 2


def test_load_handler_returns_the_format_handler() -> None:
    from src import ccda_handler, hl7_handler

    assert worker.load_handler("hl7") is hl7_handler.handler
    assert worker.load_handler("ccda") is ccda_handler.handler


def test_heartbeat_extends_visibility_while_a_long_batch_runs() -> None:
    sqs = FakeSqs([])
    with worker._VisibilityHeartbeat(sqs, "q", [_message(1), _message(2)], interval=0.02) as beat:
        time.sleep(0.15)
    calls_after_exit = len(sqs.visibility_calls)
    time.sleep(0.06)

    assert beat.extensions >= 3
    assert len(sqs.visibility_calls) == calls_after_exit  # stops when the batch finishes
    entries = sqs.visibility_calls[0]["Entries"]
    assert [entry["ReceiptHandle"] for entry in entries] == ["r1", "r2"]
    assert {entry["VisibilityTimeout"] for entry in entries} == {
        worker.VISIBILITY_EXTENSION_SECONDS
    }


def test_heartbeat_gives_up_after_the_processing_cap(caplog: pytest.LogCaptureFixture) -> None:
    sqs = FakeSqs([])
    with (
        caplog.at_level(logging.INFO, logger="ingestion_worker"),
        worker._VisibilityHeartbeat(sqs, "q", [_message(1)], interval=0.02, max_seconds=0.05),
    ):
        time.sleep(0.2)

    assert len(sqs.visibility_calls) <= 3
    assert "visibility_heartbeat_stopped" in caplog.text


def test_heartbeat_failures_do_not_stop_processing(caplog: pytest.LogCaptureFixture) -> None:
    class BrokenVisibility(FakeSqs):
        def change_message_visibility_batch(self, **_kwargs: Any) -> dict[str, Any]:
            msg = "throttled with receipt r1"
            raise ConnectionError(msg)

    with (
        caplog.at_level(logging.INFO, logger="ingestion_worker"),
        worker._VisibilityHeartbeat(BrokenVisibility([]), "q", [_message(1)], interval=0.02),
    ):
        time.sleep(0.08)

    assert "visibility_extend_failed" in caplog.text
    assert "ConnectionError" in caplog.text
    assert "r1" not in caplog.text


def test_fast_batch_never_extends_visibility() -> None:
    sqs = FakeSqs([[_message(1)]])

    assert worker.process_once(sqs, _ok_handler, queue_url="q", batch_size=10) == (1, 0)
    assert sqs.visibility_calls == []
