import json
import logging
import threading
from typing import Any

import pytest
from worker import worker


class FakeSqs:
    def __init__(self, batches: list[list[dict[str, Any]]]) -> None:
        self.batches = batches
        self.receive_calls: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []

    def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        self.receive_calls.append(kwargs)
        if not self.batches:
            return {}
        return {"Messages": self.batches.pop(0)}

    def delete_message_batch(self, **kwargs: Any) -> dict[str, Any]:
        self.deleted.append(kwargs)
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
