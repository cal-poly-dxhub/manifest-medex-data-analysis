"""Poll one format's ingestion queue and process batches with the existing handlers.

Each ECS task runs ``WORKERS`` processes. Ingestion is I/O-bound (S3, OpenSearch, Aurora),
so several processes share one vCPU and overlap their waits. Every process long-polls the
queue, wraps the received messages in the same ``{"Records": [...]}`` shape SQS delivers
to Lambda, calls the format handler, and deletes only the messages the handler reports as
processed; failed ones return to the queue after the visibility timeout and are retried
until the queue's redrive policy moves them to the dead-letter queue.

Graceful shutdown: ECS sends SIGTERM before stopping a task (scale-in or deployment) and
SIGKILL after the container stop timeout. On SIGTERM every process stops polling, finishes
the batch it already holds, deletes what succeeded, and exits, so stopping a task does not
leave messages waiting for their visibility timeout.

Environment:
    QUEUE_URL           queue to poll (required)
    FORMAT              hl7 | ccda (required); must match the handler's SOURCE_FORMAT
    WORKERS             processes per task (default 4)
    RECEIVE_BATCH_SIZE  messages per receive, 1-10 (default 10)

Logs contain counts and error types only, never message bodies, keys, or exception text.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import signal
import sys
import time
from collections.abc import Callable
from multiprocessing.synchronize import Event as EventType
from typing import Any, Protocol

LOGGER = logging.getLogger("ingestion_worker")
WAIT_SECONDS = 20
ERROR_BACKOFF_SECONDS = 5
SUPERVISOR_POLL_SECONDS = 2
FORMATS = frozenset({"hl7", "ccda"})

Handler = Callable[[dict[str, Any], Any], dict[str, list[dict[str, str]]]]


class _SqsClient(Protocol):
    def receive_message(self, **kwargs: Any) -> dict[str, Any]: ...

    def delete_message_batch(self, **kwargs: Any) -> dict[str, Any]: ...


class WorkerConfigError(ValueError):
    """Raised when the worker environment is incomplete or invalid."""


def load_handler(source_format: str) -> Handler:
    """Import the format handler lazily so each process builds its own clients."""
    if source_format == "ccda":
        from src.ccda_handler import handler as ccda_handler

        return ccda_handler
    from src.hl7_handler import handler as hl7_handler

    return hl7_handler


def read_settings(environ: dict[str, str]) -> dict[str, Any]:
    """Validate the worker environment and return typed settings."""
    queue_url = environ.get("QUEUE_URL", "")
    source_format = environ.get("FORMAT", "")
    if not queue_url:
        msg = "QUEUE_URL is required"
        raise WorkerConfigError(msg)
    if source_format not in FORMATS:
        msg = f"FORMAT must be one of {sorted(FORMATS)}"
        raise WorkerConfigError(msg)
    try:
        workers = int(environ.get("WORKERS", "4"))
        batch_size = int(environ.get("RECEIVE_BATCH_SIZE", "10"))
    except ValueError:
        msg = "WORKERS and RECEIVE_BATCH_SIZE must be integers"
        raise WorkerConfigError(msg) from None
    if workers < 1:
        msg = "WORKERS must be at least 1"
        raise WorkerConfigError(msg)
    if not 1 <= batch_size <= 10:
        msg = "RECEIVE_BATCH_SIZE must be between 1 and 10"
        raise WorkerConfigError(msg)
    return {
        "queue_url": queue_url,
        "format": source_format,
        "workers": workers,
        "batch_size": batch_size,
    }


def _log(event: str, **fields: Any) -> None:
    LOGGER.info(json.dumps({"event": event, **fields}, separators=(",", ":"), sort_keys=True))


def _to_event(messages: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "Records": [
            {
                "messageId": message["MessageId"],
                "receiptHandle": message["ReceiptHandle"],
                "body": message["Body"],
                "attributes": message.get("Attributes", {}),
            }
            for message in messages
        ]
    }


def process_once(
    sqs: _SqsClient,
    handler: Handler,
    *,
    queue_url: str,
    batch_size: int,
) -> tuple[int, int]:
    """Receive one batch, process it, and delete the successes; return (ok, failed)."""
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=batch_size,
        WaitTimeSeconds=WAIT_SECONDS,
        AttributeNames=["ApproximateReceiveCount"],
    )
    messages: list[dict[str, Any]] = response.get("Messages", [])
    if not messages:
        return 0, 0
    try:
        result = handler(_to_event(messages), None)
    except Exception as error:
        # Leave the whole batch on the queue; it is retried after the visibility timeout.
        _log("batch_failed", errorType=type(error).__name__, messages=len(messages))
        return 0, len(messages)
    failed_ids = {item["itemIdentifier"] for item in result.get("batchItemFailures", [])}
    done = [
        {"Id": str(index), "ReceiptHandle": message["ReceiptHandle"]}
        for index, message in enumerate(messages)
        if message["MessageId"] not in failed_ids
    ]
    if done:
        sqs.delete_message_batch(QueueUrl=queue_url, Entries=done)
    return len(done), len(messages) - len(done)


def run_loop(
    sqs: _SqsClient,
    handler: Handler,
    *,
    queue_url: str,
    batch_size: int,
    stop: EventType,
) -> None:
    """Poll until ``stop`` is set; a batch already received always finishes first."""
    processed = 0
    while not stop.is_set():
        try:
            ok, failed = process_once(sqs, handler, queue_url=queue_url, batch_size=batch_size)
        except Exception as error:
            # SQS or network errors: back off and keep polling rather than exiting.
            _log("receive_failed", errorType=type(error).__name__)
            stop.wait(ERROR_BACKOFF_SECONDS)
            continue
        if ok or failed:
            processed += ok
            _log("batch_processed", ok=ok, failed=failed, total=processed)
    _log("worker_stopped", total=processed)


def _worker_main(settings: dict[str, Any], stop: EventType) -> None:
    # The supervisor owns SIGTERM; workers only watch the shared stop event.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    _configure_logging()
    import boto3  # type: ignore[import-not-found]

    run_loop(
        boto3.client("sqs"),
        load_handler(settings["format"]),
        queue_url=settings["queue_url"],
        batch_size=settings["batch_size"],
        stop=stop,
    )


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(processName)s %(message)s", force=True)


def supervise(settings: dict[str, Any]) -> int:
    """Start the worker processes and keep them alive until SIGTERM; return an exit code."""
    stop = multiprocessing.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        _log("shutdown_requested")
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    processes = [
        multiprocessing.Process(target=_worker_main, args=(settings, stop), name=f"w{index}")
        for index in range(settings["workers"])
    ]
    for process in processes:
        process.start()
    _log("worker_started", format=settings["format"], workers=settings["workers"])
    while not stop.is_set():
        if any(not process.is_alive() for process in processes):
            # A crashed process means the task is unhealthy; exit so ECS replaces it.
            _log("worker_process_exited")
            stop.set()
            for process in processes:
                process.join(timeout=WAIT_SECONDS + 5)
            return 1
        time.sleep(SUPERVISOR_POLL_SECONDS)
    for process in processes:
        process.join()
    return 0


def main() -> int:
    _configure_logging()
    try:
        settings = read_settings(dict(os.environ))
    except WorkerConfigError as error:
        _log("worker_config_invalid", reason=str(error))
        return 2
    return supervise(settings)


if __name__ == "__main__":
    sys.exit(main())
