import base64
import gzip
import http.client
import json
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from typing import Any

import pytest
from intake import server
from intake.server import IntakeError, IntakeSettings

SECRET = "s3cr3t-value-with-enough-entropy-0123456789ab"  # noqa: S105 - test fixture
HL7 = (
    b"MSH|^~\\&|SYNTH|FAC|DEST|DEST|20260810120000||ADT^A08|MSG-1|P|2.5.1\r"
    b"PID|1||12345^^^KHS||DOE^JANE\r"
)
CCDA = b"<ClinicalDocument xmlns='urn:hl7-org:v3'/>"
RECEIVED = datetime(2026, 11, 18, 17, 0, tzinfo=UTC)


def _settings(**overrides: Any) -> IntakeSettings:
    values: dict[str, Any] = {
        "raw_bucket": "raw-bucket",
        "archive_bucket": "archive-bucket",
        "sender_id": "rhapsody",
        "sender_secret": SECRET,
        "max_request_bytes": 1024 * 1024,
        "max_hl7_object_bytes": 512 * 1024,
        "max_ccda_object_bytes": 1024 * 1024,
    }
    values.update(overrides)
    return IntakeSettings(**values)


def _envelope(**overrides: Any) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "sourceMessageId": "MSG-1",
        "messageDate": "2026-11-18T09:30:00-08:00",
        "clinicalMessageType": "HL7V2",
        "eventType": "ADT_A08",
        "sourceFacilityId": "ARR_CA_ARMC_H",
        "sourceFacility": "Synthetic Facility",
        "patientId": "12345",
        "patientIdAa": "KHS",
        "patientMpiId": "MPI-1",
        "otherAttributes": {"isPartTwo": True},
        "contents": [{"processedLevel": "RAW", "content": base64.b64encode(HL7).decode()}],
    }
    envelope.update(overrides)
    return envelope


def _token(scheme: str, sender: str = "rhapsody", secret: str = SECRET) -> str:
    return f"{scheme} " + base64.b64encode(f"{sender}:{secret}".encode()).decode()


class FakeS3:
    def __init__(self, *, fail: bool = False) -> None:
        self.puts: list[dict[str, Any]] = []
        self.fail = fail

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        if self.fail:
            msg = "s3 unavailable"
            raise ConnectionError(msg)
        self.puts.append(kwargs)
        return {"VersionId": f"v{len(self.puts)}"}


# ---- authentication -------------------------------------------------------------------


@pytest.mark.parametrize("scheme", ["ApiKey", "Basic", "apikey", "BASIC"])
def test_valid_apikey_and_basic_credentials_authenticate(scheme: str) -> None:
    assert server.authenticate(_token(scheme), _settings()) == "rhapsody"


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        _token("ApiKey", secret="wrong"),  # noqa: S106
        _token("ApiKey", sender="someone-else"),
        _token("Bearer"),
        "ApiKey not-base64!!",
        "ApiKey " + base64.b64encode(b"no-separator").decode(),
        "ApiKey",
    ],
)
def test_invalid_credentials_are_rejected(header: str | None) -> None:
    assert server.authenticate(header, _settings()) is None


def test_missing_configured_secret_rejects_everyone() -> None:
    settings = _settings(sender_secret="")
    assert server.authenticate(_token("ApiKey", secret=""), settings) is None


# ---- validation and key layout --------------------------------------------------------


def test_hl7_request_maps_to_stable_keys_and_sidecar_without_content() -> None:
    message = server.parse_request(
        json.dumps(_envelope()).encode(), settings=_settings(), received_at=RECEIVED
    )

    assert message.raw_key == "incoming/hl7/participant=ARR_CA_ARMC_H/2026/11/18/MSG-1.hl7"
    assert message.envelope_key == "envelopes/hl7/participant=ARR_CA_ARMC_H/2026/11/18/MSG-1.json"
    assert message.content == HL7
    assert message.envelope["patientMpiId"] == "MPI-1"
    assert message.envelope["intakeReceivedAt"] == "2026-11-18T17:00:00Z"
    assert message.envelope["intakeSender"] == "rhapsody"
    assert message.envelope["selectedContentIndex"] == 0
    assert "content" not in json.dumps(message.envelope["contents"])
    assert "MSG-1" not in message.message_ref


def test_ccda_request_routes_to_ccda_lane() -> None:
    envelope = _envelope(
        clinicalMessageType="CCDA",
        contents=[{"processedLevel": "PROCESSED", "content": base64.b64encode(CCDA).decode()}],
    )
    message = server.parse_request(
        json.dumps(envelope).encode(), settings=_settings(), received_at=RECEIVED
    )
    assert message.raw_key.startswith("incoming/ccda/participant=ARR_CA_ARMC_H/")
    assert message.raw_key.endswith(".xml")
    assert message.content == CCDA


def test_processed_content_is_preferred_over_raw() -> None:
    envelope = _envelope(
        contents=[
            {"processedLevel": "RAW", "content": base64.b64encode(b"MSH|raw").decode()},
            {"processedLevel": "PROCESSED", "content": base64.b64encode(HL7).decode()},
        ]
    )
    message = server.parse_request(
        json.dumps(envelope).encode(), settings=_settings(), received_at=RECEIVED
    )
    assert message.content_index == 1
    assert message.content == HL7


def test_mime_wrapped_base64_is_accepted() -> None:
    encoded = base64.encodebytes(HL7).decode()  # 76-character lines
    envelope = _envelope(contents=[{"processedLevel": "RAW", "content": encoded}])
    message = server.parse_request(
        json.dumps(envelope).encode(), settings=_settings(), received_at=RECEIVED
    )
    assert message.content == HL7


def test_unsafe_identifiers_are_hashed_into_keys() -> None:
    envelope = _envelope(sourceMessageId="../../etc/passwd", sourceFacilityId="A B/C")
    message = server.parse_request(
        json.dumps(envelope).encode(), settings=_settings(), received_at=RECEIVED
    )
    assert ".." not in message.raw_key
    assert message.raw_key.startswith("incoming/hl7/participant=x-")
    assert message.raw_key.count("/") == 6


@pytest.mark.parametrize(
    ("change", "status", "text"),
    [
        ({"patientId": None}, HTTPStatus.BAD_REQUEST, "patientId"),
        ({"patientIdAa": ""}, HTTPStatus.BAD_REQUEST, "patientIdAa"),
        ({"sourceFacilityId": None}, HTTPStatus.BAD_REQUEST, "sourceFacilityId"),
        ({"clinicalMessageType": "OTHER"}, HTTPStatus.BAD_REQUEST, "HL7V2 or CCDA"),
        ({"messageDate": "yesterday"}, HTTPStatus.BAD_REQUEST, "messageDate"),
        ({"sourceLastUpdatedTime": "soon"}, HTTPStatus.BAD_REQUEST, "sourceLastUpdatedTime"),
        ({"otherAttributes": ["x"]}, HTTPStatus.BAD_REQUEST, "otherAttributes"),
        ({"contents": []}, HTTPStatus.BAD_REQUEST, "contents"),
        (
            {"contents": [{"processedLevel": "X", "content": "QQ=="}]},
            HTTPStatus.BAD_REQUEST,
            "processedLevel",
        ),
        (
            {"contents": [{"processedLevel": "RAW", "content": "@@@"}]},
            HTTPStatus.BAD_REQUEST,
            "base64",
        ),
        (
            {"contents": [{"processedLevel": "RAW", "content": ""}]},
            HTTPStatus.BAD_REQUEST,
            "base64",
        ),
    ],
)
def test_invalid_envelopes_are_rejected(
    change: dict[str, Any], status: HTTPStatus, text: str
) -> None:
    with pytest.raises(IntakeError) as raised:
        server.parse_request(
            json.dumps(_envelope(**change)).encode(), settings=_settings(), received_at=RECEIVED
        )
    assert raised.value.status == status
    assert text in raised.value.message


def test_oversized_decoded_content_is_rejected_with_413() -> None:
    big = base64.b64encode(b"M" * 600 * 1024).decode()
    envelope = _envelope(contents=[{"processedLevel": "RAW", "content": big}])
    with pytest.raises(IntakeError) as raised:
        server.parse_request(
            json.dumps(envelope).encode(), settings=_settings(), received_at=RECEIVED
        )
    assert raised.value.status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE


def test_non_json_body_is_rejected() -> None:
    with pytest.raises(IntakeError) as raised:
        server.parse_request(b"not json", settings=_settings(), received_at=RECEIVED)
    assert raised.value.status == HTTPStatus.BAD_REQUEST


def test_store_archives_then_writes_sidecar_then_message_with_version_link() -> None:
    s3 = FakeS3()
    body = json.dumps(_envelope()).encode()
    message = server.parse_request(body, settings=_settings(), received_at=RECEIVED)

    server.store(s3, body, message, settings=_settings(), received_at=RECEIVED, request_id="req-1")

    archive, sidecar, raw = s3.puts
    assert archive["Bucket"] == "archive-bucket"
    assert archive["Key"] == "requests/2026/11/18/req-1.json.gz"
    assert gzip.decompress(archive["Body"]) == body
    assert sidecar["Key"] == message.envelope_key
    assert json.loads(sidecar["Body"])["patientId"] == "12345"
    assert raw["Key"] == message.raw_key
    assert raw["Body"] == HL7
    assert raw["Metadata"] == {"intake-sender": "rhapsody", "envelope-version-id": "v2"}


# ---- HTTP behavior (real server on loopback) --------------------------------------------


@pytest.fixture
def running() -> Iterator[tuple[int, FakeS3, list[IntakeSettings]]]:
    s3 = FakeS3()
    settings = [_settings()]
    handler = server.make_handler(settings[0], s3, server.ByteBudget(64 * 1024 * 1024))
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield httpd.server_address[1], s3, settings
    finally:
        httpd.shutdown()
        httpd.server_close()


def _request(
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], dict[str, Any]]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    connection.request(method, path, body=body, headers=headers or {})
    response = connection.getresponse()
    payload = json.loads(response.read() or b"{}")
    result = (response.status, {k.lower(): v for k, v in response.getheaders()}, payload)
    connection.close()
    return result


def test_unauthenticated_post_is_denied_by_the_gate_before_any_write(running: Any) -> None:
    port, s3, _ = running
    status, headers, payload = _request(
        port,
        "POST",
        "/prism/ods/clinical-message",
        json.dumps(_envelope()).encode(),
        {"Content-Type": "application/json"},
    )
    assert status == 401
    assert headers["www-authenticate"] == server.REALM
    assert payload["importStatus"]["status"] == "FAILED"
    assert s3.puts == []


def test_wrong_key_is_denied(running: Any) -> None:
    port, s3, _ = running
    status, _, _ = _request(
        port,
        "POST",
        "/prism/ods/clinical-message",
        json.dumps(_envelope()).encode(),
        {"Authorization": _token("ApiKey", secret="nope")},  # noqa: S106
    )
    assert status == 401
    assert s3.puts == []


def test_authenticated_post_is_stored_and_acknowledged(running: Any) -> None:
    port, s3, _ = running
    status, _, payload = _request(
        port,
        "POST",
        "/prism/ods/clinical-message",
        json.dumps(_envelope()).encode(),
        {"Authorization": _token("ApiKey"), "Content-Type": "application/json"},
    )
    assert status == 200
    assert payload["importStatus"]["status"] == "SUCCESS"
    assert payload["importStatus"]["contentIndex"] == 0
    assert payload["importStatus"]["Id"]
    assert [put["Bucket"] for put in s3.puts] == ["archive-bucket", "raw-bucket", "raw-bucket"]


def test_short_path_alias_and_basic_auth_work(running: Any) -> None:
    port, s3, _ = running
    status, _, _ = _request(
        port,
        "POST",
        "/clinical-message",
        json.dumps(_envelope()).encode(),
        {"Authorization": _token("Basic")},
    )
    assert status == 200
    assert len(s3.puts) == 3


def test_request_over_the_size_cap_is_refused_without_reading(running: Any) -> None:
    port, s3, _ = running
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    connection.putrequest("POST", "/prism/ods/clinical-message")
    connection.putheader("Authorization", _token("ApiKey"))
    connection.putheader("Content-Length", str(5 * 1024 * 1024))
    connection.endheaders()
    response = connection.getresponse()
    assert response.status == 413
    connection.close()
    assert s3.puts == []


def test_invalid_envelope_returns_400_and_stores_nothing(running: Any) -> None:
    port, s3, _ = running
    status, _, payload = _request(
        port,
        "POST",
        "/prism/ods/clinical-message",
        json.dumps(_envelope(patientId=None)).encode(),
        {"Authorization": _token("ApiKey")},
    )
    assert status == 400
    assert "patientId" in payload["importStatus"]["message"]
    assert s3.puts == []


def test_health_is_open_but_status_requires_auth(running: Any) -> None:
    port, _, _ = running
    assert _request(port, "GET", "/healthz")[0] == 200
    assert _request(port, "GET", "/status")[0] == 401
    assert _request(port, "GET", "/status", headers={"Authorization": _token("ApiKey")})[0] == 200
    assert _request(port, "GET", "/anything")[0] == 404


def test_storage_failure_returns_retryable_503() -> None:
    s3 = FakeS3(fail=True)
    httpd = ThreadingHTTPServer(
        ("127.0.0.1", 0), server.make_handler(_settings(), s3, server.ByteBudget(64 * 1024 * 1024))
    )
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        status, headers, _ = _request(
            httpd.server_address[1],
            "POST",
            "/prism/ods/clinical-message",
            json.dumps(_envelope()).encode(),
            {"Authorization": _token("ApiKey")},
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
    assert status == 503
    assert headers["retry-after"] == "5"


def test_settings_from_environment_and_fail_closed_main(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = server.settings_from_environment(
        {
            "RAW_BUCKET": "r",
            "ARCHIVE_BUCKET": "a",
            "INTAKE_SENDER_ID": "rhapsody",
            "MAX_REQUEST_BYTES": "10",
        }
    )
    assert settings.max_request_bytes == 10
    assert settings.sender_secret == ""
    for name, value in {
        "RAW_BUCKET": "r",
        "ARCHIVE_BUCKET": "a",
        "INTAKE_SENDER_ID": "rhapsody",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("INTAKE_API_SECRET", raising=False)
    assert server.main() == 2


def test_ccda_limit_is_separate_from_hl7_limit() -> None:
    doc = base64.b64encode(
        b"<ClinicalDocument>" + b"x" * 700 * 1024 + b"</ClinicalDocument>"
    ).decode()
    ccda = _envelope(
        clinicalMessageType="CCDA", contents=[{"processedLevel": "RAW", "content": doc}]
    )
    message = server.parse_request(
        json.dumps(ccda).encode(), settings=_settings(), received_at=RECEIVED
    )
    assert len(message.content) > 512 * 1024  # over the HL7 limit, under the CCDA limit
    hl7 = _envelope(contents=[{"processedLevel": "RAW", "content": doc}])
    with pytest.raises(IntakeError) as raised:
        server.parse_request(json.dumps(hl7).encode(), settings=_settings(), received_at=RECEIVED)
    assert raised.value.status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE


def test_byte_budget_queues_large_requests_and_times_out() -> None:
    budget = server.ByteBudget(100)
    assert budget.acquire(80, timeout=0.01)
    assert not budget.acquire(30, timeout=0.05)  # would exceed the budget
    released = threading.Timer(0.05, budget.release, args=(80,))
    released.start()
    assert budget.acquire(30, timeout=2)  # granted once the first request finishes
    budget.release(30)
    assert budget.acquire(500, timeout=0.01)  # oversized request runs alone
    budget.release(500)
    assert budget.acquire(100, timeout=0.01)
