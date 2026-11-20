"""Accept Prism Clinical Message Intake requests and hand the message to the S3 pipeline.

Senders (an integration engine such as Rhapsody) keep posting the same JSON they sent to
Prism (``POST /prism/ods/clinical-message``): an envelope of message metadata plus one or
more base64-encoded HL7 v2 / CCDA ``contents``. For each request this service:

1. authenticates the sender (``Authorization: ApiKey base64(id:secret)`` or ``Basic``);
2. validates the envelope and decodes the chosen content (PROCESSED preferred, else RAW);
3. archives the exact request body (gzip) in the private archive bucket;
4. writes the envelope (without the content) as a KMS-encrypted sidecar under
   ``envelopes/...``, then the decoded message under ``incoming/...``. The message object
   carries the sidecar's version id, so the worker merges exactly that envelope;
5. answers 200 with Prism's ``importStatus`` only after S3 confirmed every write.

Network exposure: the service listens on all interfaces *inside its task*. The task has no
public IP and its security group admits only the internal load balancer, which itself admits
only the configured sender networks over HTTPS. Logs never contain request bodies, envelope
fields, or credentials.

Authentication note: the per-sender API key is a deliberate, documented decision made at the
customer's request so existing Prism senders need no change. Mutual TLS is the recommended
upgrade. Without a configured key the service rejects every request (fail closed).
"""

from __future__ import annotations

import base64
import binascii
import gzip
import hashlib
import hmac
import json
import logging
import os
import re
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol

LOGGER = logging.getLogger("clinical_message_intake")
INTAKE_PATHS = frozenset({"/prism/ods/clinical-message", "/clinical-message"})
STATUS_PATHS = frozenset({"/prism/ods/status", "/status"})
HEALTH_PATH = "/healthz"
MAX_OBJECT_BYTES_DEFAULT = 50 * 1024 * 1024
MESSAGE_TYPES = {"HL7V2": ("hl7", "hl7"), "CCDA": ("ccda", "xml")}
REQUIRED_FIELDS = (
    "sourceMessageId",
    "messageDate",
    "clinicalMessageType",
    "sourceFacilityId",
    "patientId",
    "patientIdAa",
)
OPTIONAL_DATE_FIELDS = ("sourceLastUpdatedTime",)
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_WHITESPACE = re.compile(rb"\s+")
REALM = 'ApiKey realm="clinical-message-intake"'


class S3Writer(Protocol):
    """Minimal S3 operations the intake service needs."""

    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...


class IntakeError(Exception):
    """A request rejected with an HTTP status and a non-sensitive message."""

    def __init__(self, status: HTTPStatus, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True)
class IntakeSettings:
    raw_bucket: str
    archive_bucket: str
    sender_id: str
    sender_secret: str
    max_request_bytes: int
    max_hl7_object_bytes: int = MAX_OBJECT_BYTES_DEFAULT
    max_ccda_object_bytes: int = MAX_OBJECT_BYTES_DEFAULT


class ByteBudget:
    """Bound the total request bytes held in memory across concurrent requests.

    A count of requests is not enough when sizes range from kilobytes to 150 MB: a few
    large CCDs at once must queue instead of exhausting the task's memory.
    """

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._available = capacity
        self._condition = threading.Condition()

    def acquire(self, amount: int, timeout: float) -> bool:
        # A request bigger than the whole budget still runs, but only on its own.
        amount = min(amount, self.capacity)
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._available < amount:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            self._available -= amount
            return True

    def release(self, amount: int) -> None:
        with self._condition:
            self._available = min(self.capacity, self._available + min(amount, self.capacity))
            self._condition.notify_all()


@dataclass(frozen=True)
class AcceptedMessage:
    raw_key: str
    envelope_key: str
    content: bytes
    content_index: int
    envelope: dict[str, Any]
    message_ref: str


def authenticate(header: str | None, settings: IntakeSettings) -> str | None:
    """Return the sender id for a valid ApiKey/Basic credential, else None (constant time)."""
    if not header or not settings.sender_secret:
        return None
    scheme, _, token = header.strip().partition(" ")
    if scheme.lower() not in ("apikey", "basic") or not token:
        return None
    try:
        decoded = base64.b64decode(token.strip(), validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return None
    sender, separator, secret = decoded.partition(":")
    if not separator:
        return None
    sender_ok = hmac.compare_digest(sender.encode(), settings.sender_id.encode())
    secret_ok = hmac.compare_digest(secret.encode(), settings.sender_secret.encode())
    return settings.sender_id if sender_ok and secret_ok else None


def _require_string(envelope: dict[str, Any], name: str) -> str:
    value = envelope.get(name)
    if not isinstance(value, str) or not value.strip():
        raise IntakeError(HTTPStatus.BAD_REQUEST, f"Missing or empty required field: {name}")
    return value.strip()


def _parse_datetime(value: str, name: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise IntakeError(HTTPStatus.BAD_REQUEST, f"{name} must be an ISO 8601 date-time") from None


def _segment(value: str) -> str:
    """Use a value in an S3 key as-is when it is safe, otherwise a stable hash of it."""
    if _SAFE_SEGMENT.fullmatch(value):
        return value
    return "x-" + hashlib.sha256(value.encode()).hexdigest()[:32]


def _choose_content(contents: object) -> tuple[int, dict[str, Any]]:
    if not isinstance(contents, list) or not contents:
        raise IntakeError(HTTPStatus.BAD_REQUEST, "contents must be a non-empty array")
    candidates: dict[str, tuple[int, dict[str, Any]]] = {}
    for index, item in enumerate(contents):
        if not isinstance(item, dict):
            raise IntakeError(HTTPStatus.BAD_REQUEST, "contents entries must be objects")
        level = item.get("processedLevel")
        if level not in ("RAW", "PROCESSED"):
            raise IntakeError(
                HTTPStatus.BAD_REQUEST, "contents[].processedLevel must be RAW or PROCESSED"
            )
        candidates.setdefault(level, (index, item))
    # Prism's guidance: PROCESSED is the recommended default when both are supplied.
    return candidates.get("PROCESSED") or candidates["RAW"]


def _decode_content(item: dict[str, Any], max_bytes: int) -> bytes:
    encoded = item.get("content")
    if not isinstance(encoded, str) or not encoded:
        raise IntakeError(HTTPStatus.BAD_REQUEST, "contents[].content must be base64 text")
    # MIME-style line breaks are common in base64 payloads; strip whitespace, then decode
    # strictly so non-base64 input is rejected instead of silently truncated.
    compact = _WHITESPACE.sub(b"", encoded.encode("ascii", errors="replace"))
    if len(compact) * 3 // 4 > max_bytes:
        raise IntakeError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Decoded content is too large")
    try:
        content = base64.b64decode(compact, validate=True)
    except (binascii.Error, ValueError):
        raise IntakeError(
            HTTPStatus.BAD_REQUEST, "contents[].content is not valid base64"
        ) from None
    if not content:
        raise IntakeError(HTTPStatus.BAD_REQUEST, "contents[].content is empty")
    return content


def parse_request(
    body: bytes, *, settings: IntakeSettings, received_at: datetime
) -> AcceptedMessage:
    """Validate one intake request and return what to store; raise IntakeError otherwise."""
    try:
        envelope = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise IntakeError(HTTPStatus.BAD_REQUEST, "Request body is not valid JSON") from None
    if not isinstance(envelope, dict):
        raise IntakeError(HTTPStatus.BAD_REQUEST, "Request body must be a JSON object")
    values = {name: _require_string(envelope, name) for name in REQUIRED_FIELDS}
    message_type = values["clinicalMessageType"].upper()
    if message_type not in MESSAGE_TYPES:
        raise IntakeError(HTTPStatus.BAD_REQUEST, "clinicalMessageType must be HL7V2 or CCDA")
    message_date = _parse_datetime(values["messageDate"], "messageDate")
    for name in OPTIONAL_DATE_FIELDS:
        optional = envelope.get(name)
        if optional not in (None, ""):
            if not isinstance(optional, str):
                raise IntakeError(HTTPStatus.BAD_REQUEST, f"{name} must be a string")
            _parse_datetime(optional, name)
    other = envelope.get("otherAttributes")
    if other is not None and not isinstance(other, dict):
        raise IntakeError(HTTPStatus.BAD_REQUEST, "otherAttributes must be a JSON object")

    index, item = _choose_content(envelope.get("contents"))
    lane, extension = MESSAGE_TYPES[message_type]
    limit = settings.max_ccda_object_bytes if lane == "ccda" else settings.max_hl7_object_bytes
    content = _decode_content(item, limit)
    # Keys derive only from stable envelope values, so a re-sent message (same facility,
    # message date, and sourceMessageId) overwrites the same objects: Prism's update semantics.
    relative = (
        f"{lane}/participant={_segment(values['sourceFacilityId'])}/"
        f"{message_date:%Y/%m/%d}/{_segment(values['sourceMessageId'])}"
    )
    sidecar = {key: value for key, value in envelope.items() if key != "contents"}
    sidecar["clinicalMessageType"] = message_type
    sidecar["contents"] = [
        {k: v for k, v in entry.items() if k != "content"}
        for entry in envelope["contents"]
        if isinstance(entry, dict)
    ]
    sidecar["selectedContentIndex"] = index
    sidecar["intakeReceivedAt"] = received_at.isoformat().replace("+00:00", "Z")
    sidecar["intakeSender"] = settings.sender_id
    return AcceptedMessage(
        raw_key=f"incoming/{relative}.{extension}",
        envelope_key=f"envelopes/{relative}.json",
        content=content,
        content_index=index,
        envelope=sidecar,
        message_ref=hashlib.sha256(values["sourceMessageId"].encode()).hexdigest()[:16],
    )


def store(
    s3: S3Writer,
    body: bytes,
    message: AcceptedMessage,
    *,
    settings: IntakeSettings,
    received_at: datetime,
    request_id: str,
) -> None:
    """Archive the request, write the envelope sidecar, then the message (order matters)."""
    s3.put_object(
        Bucket=settings.archive_bucket,
        Key=f"requests/{received_at:%Y/%m/%d}/{request_id}.json.gz",
        Body=gzip.compress(body),
        ContentType="application/gzip",
    )
    sidecar = s3.put_object(
        Bucket=settings.raw_bucket,
        Key=message.envelope_key,
        Body=json.dumps(message.envelope, separators=(",", ":"), sort_keys=True).encode(),
        ContentType="application/json",
    )
    metadata = {"intake-sender": settings.sender_id}
    if sidecar.get("VersionId"):
        metadata["envelope-version-id"] = str(sidecar["VersionId"])
    else:
        metadata["envelope-key-only"] = "true"
    s3.put_object(
        Bucket=settings.raw_bucket,
        Key=message.raw_key,
        Body=message.content,
        Metadata=metadata,
    )


def _log(event: str, **fields: Any) -> None:
    LOGGER.info(json.dumps({"event": event, **fields}, separators=(",", ":"), sort_keys=True))


def _response_body(status: str, message: str, *, index: int = 0, request_id: str = "") -> bytes:
    payload = {
        "importStatus": {
            "contentIndex": index,
            "status": status,
            "message": message,
            "Id": request_id,
        }
    }
    return json.dumps(payload, separators=(",", ":")).encode()


def make_handler(settings: IntakeSettings, s3: S3Writer, budget: ByteBudget) -> type:
    class IntakeHandler(BaseHTTPRequestHandler):
        server_version = "ClinicalMessageIntake"
        sys_version = ""
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args: Any) -> None:
            # The default access log would print paths and client details; use _log instead.
            return

        def _send(
            self, status: HTTPStatus, body: bytes, *, extra: dict[str, str] | None = None
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            for name, value in (extra or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

        def _reject(self, status: HTTPStatus, message: str, *, close: bool = False) -> None:
            extra = {"WWW-Authenticate": REALM} if status == HTTPStatus.UNAUTHORIZED else {}
            if close:
                # The body was not read; close so leftover bytes cannot be parsed as a request.
                self.close_connection = True
                extra["Connection"] = "close"
            self._send(status, _response_body("FAILED", message), extra=extra)

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == HEALTH_PATH:
                # Load-balancer health check: unauthenticated, reveals nothing.
                self._send(HTTPStatus.OK, b'{"status":"ok"}')
                return
            if path in STATUS_PATHS:
                if authenticate(self.headers.get("Authorization"), settings) is None:
                    self._reject(HTTPStatus.UNAUTHORIZED, "Authentication required")
                    return
                self._send(HTTPStatus.OK, b'{"status":"ok"}')
                return
            self._reject(HTTPStatus.NOT_FOUND, "Not found")

        def do_POST(self) -> None:
            started = time.monotonic()
            path = self.path.split("?", 1)[0]
            if path not in INTAKE_PATHS:
                self._reject(HTTPStatus.NOT_FOUND, "Not found", close=True)
                return
            # Authenticate before reading the body, so unauthenticated callers cannot make
            # the service buffer large payloads.
            sender = authenticate(self.headers.get("Authorization"), settings)
            if sender is None:
                _log("intake_rejected", status=401, reason="authentication")
                self._reject(HTTPStatus.UNAUTHORIZED, "Authentication required", close=True)
                return
            length_header = self.headers.get("Content-Length")
            if length_header is None or not length_header.isdigit():
                self._reject(HTTPStatus.LENGTH_REQUIRED, "Content-Length is required", close=True)
                return
            length = int(length_header)
            if length > settings.max_request_bytes:
                _log("intake_rejected", status=413, bytes=length, sender=sender)
                self._reject(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request is too large", close=True
                )
                return
            # Parsing, decoding, and archiving hold about three copies of the body at once.
            reserved = length * 3
            if not budget.acquire(reserved, timeout=30):
                self._reject(HTTPStatus.SERVICE_UNAVAILABLE, "Busy, retry later", close=True)
                return
            try:
                self._handle_message(sender, length, started)
            finally:
                budget.release(reserved)

        def _handle_message(self, sender: str, length: int, started: float) -> None:
            body = self.rfile.read(length)
            received_at = datetime.now(UTC)
            request_id = str(uuid.uuid4())
            try:
                message = parse_request(body, settings=settings, received_at=received_at)
            except IntakeError as error:
                _log("intake_rejected", status=int(error.status), bytes=length, sender=sender)
                self._reject(error.status, error.message)
                return
            try:
                store(
                    s3,
                    body,
                    message,
                    settings=settings,
                    received_at=received_at,
                    request_id=request_id,
                )
            except Exception as error:
                # Storage failures are retryable by the sender; nothing was acknowledged.
                _log(
                    "intake_store_failed",
                    errorType=type(error).__name__,
                    messageRef=message.message_ref,
                )
                self._send(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    _response_body("FAILED", "Temporarily unable to store the message"),
                    extra={"Retry-After": "5"},
                )
                return
            _log(
                "intake_accepted",
                bytes=length,
                durationMs=int((time.monotonic() - started) * 1000),
                messageRef=message.message_ref,
                sender=sender,
                status=200,
            )
            self._send(
                HTTPStatus.OK,
                _response_body("SUCCESS", "", index=message.content_index, request_id=request_id),
            )

    return IntakeHandler


def settings_from_environment(environ: dict[str, str]) -> IntakeSettings:
    secret_payload = environ.get("INTAKE_API_SECRET", "")
    return IntakeSettings(
        raw_bucket=environ["RAW_BUCKET"],
        archive_bucket=environ["ARCHIVE_BUCKET"],
        sender_id=environ["INTAKE_SENDER_ID"],
        sender_secret=secret_payload,
        max_request_bytes=int(environ.get("MAX_REQUEST_BYTES", str(75 * 1024 * 1024))),
        max_hl7_object_bytes=int(
            environ.get("MAX_HL7_OBJECT_BYTES", str(MAX_OBJECT_BYTES_DEFAULT))
        ),
        max_ccda_object_bytes=int(
            environ.get("MAX_CCDA_OBJECT_BYTES", str(MAX_OBJECT_BYTES_DEFAULT))
        ),
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    settings = settings_from_environment(dict(os.environ))
    if not settings.sender_secret:
        # Fail closed: without a key the service would accept nobody, so refuse to start.
        _log("intake_config_invalid", reason="INTAKE_API_SECRET is not set")
        return 2
    import boto3  # type: ignore[import-not-found]

    s3 = boto3.client("s3")
    budget = ByteBudget(int(os.environ.get("MAX_INFLIGHT_BYTES", str(1024 * 1024 * 1024))))
    # Binding all interfaces is confined to the task's own network namespace: the task has no
    # public IP and its security group admits only the internal load balancer.
    bind_address = "0.0.0.0"  # noqa: S104
    handler = make_handler(settings, s3, budget)
    server = ThreadingHTTPServer((bind_address, int(os.environ.get("PORT", "8080"))), handler)
    server.daemon_threads = False

    def stop(_signum: int, _frame: object) -> None:
        _log("shutdown_requested")
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    _log("intake_started", sender=settings.sender_id)
    server.serve_forever()
    server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
