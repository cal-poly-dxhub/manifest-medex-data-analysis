"""Parse HL7 v2 batches and project fields required by customer dashboard queries."""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import hl7
from hl7.containers import Message, Segment
from hl7.exceptions import HL7Exception

PARSER_VERSION = "0.4.0"
INVALID_UTF8 = "Raw object is not valid UTF-8"
CONTENT_BEFORE_MSH = "Content appeared before the first MSH segment"
MISSING_MSH = "No HL7 MSH segment was found"
INVALID_MSH = "HL7 message has an invalid MSH segment"


class ParseError(ValueError):
    """Raised when a source object cannot be parsed into an HL7 message."""


@dataclass(frozen=True)
class SourceReference:
    """Location and identity of an authoritative raw S3 object."""

    bucket: str
    key: str
    version_id: str | None = None
    etag: str | None = None


@dataclass(frozen=True)
class _SegmentView:
    """Typed application adapter over python-hl7's untyped segment container."""

    container: Segment
    name: str
    field_separator: str
    component_separator: str
    repetition_separator: str
    escape_character: str
    subcomponent_separator: str

    def field(self, number: int) -> str:
        try:
            raw = str(self.container[number])
        except IndexError:
            return ""
        return _decode_escapes(raw, self)

    def component(self, number: int, component: int) -> str:
        try:
            first_repetition = self.container[number][0]
        except IndexError:
            return ""
        if isinstance(first_repetition, str):
            raw = first_repetition if component == 1 else ""
        else:
            try:
                raw = str(first_repetition[component - 1])
            except IndexError:
                return ""
        return _decode_escapes(raw, self)


def parse_hl7_file(
    payload: bytes,
    source: SourceReference,
    *,
    ingested_at: str | None = None,
) -> list[dict[str, Any]]:
    """Split, deduplicate, and parse HL7 messages from one raw object."""
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ParseError(INVALID_UTF8) from error

    messages = split_hl7_messages(text)
    raw_checksum = hashlib.sha256(payload).hexdigest()
    documents: list[dict[str, Any]] = []
    seen_messages: set[str] = set()
    for ordinal, message in enumerate(messages):
        # Identity is based on normalized message content; retain only its first occurrence.
        if message in seen_messages:
            continue
        seen_messages.add(message)
        message_checksum = hashlib.sha256(message.encode()).hexdigest()
        documents.append(
            _parse_message(
                message,
                source,
                raw_checksum=raw_checksum,
                message_checksum=message_checksum,
                ordinal=ordinal,
                ingested_at=ingested_at,
            )
        )
    return documents


def split_hl7_messages(text: str) -> list[str]:
    """Split a batch file on MSH segments while tolerating common line endings and MLLP."""
    # Remove MLLP framing and normalize common line endings before locating MSH boundaries.
    normalized = text.replace("\x0b", "").replace("\x1c", "")
    normalized = normalized.replace("\r\n", "\r").replace("\n", "\r")
    segments = [segment for segment in normalized.split("\r") if segment]

    messages: list[list[str]] = []
    current: list[str] = []
    for segment in segments:
        if segment.startswith("MSH"):
            if current:
                messages.append(current)
            current = [segment]
        elif current:
            current.append(segment)
        else:
            raise ParseError(CONTENT_BEFORE_MSH)
    if current:
        messages.append(current)
    if not messages:
        raise ParseError(MISSING_MSH)
    return ["\r".join(message) for message in messages]


def _parse_message(
    message: str,
    source: SourceReference,
    *,
    raw_checksum: str,
    message_checksum: str,
    ordinal: int,
    ingested_at: str | None,
) -> dict[str, Any]:
    msh = message.partition("\r")[0]
    if len(msh) < 9 or not msh.startswith("MSH") or msh[8] != msh[3]:
        raise ParseError(INVALID_MSH)

    parsed = _parse_with_library(message)
    # python-hl7 exposes separators in library order; _SegmentView assigns semantic names.
    separators = parsed.separators
    if len(separators) != 5:
        raise ParseError(INVALID_MSH)

    counters: Counter[str] = Counter()
    root: dict[str, Any] = {}
    parsed_segments: list[_SegmentView] = []

    for container in parsed:
        name = str(container[0])[:3]
        if len(name) < 3:
            continue
        counters[name] += 1
        segment = _SegmentView(
            container=container,
            name=name,
            field_separator=separators[1],
            component_separator=separators[3],
            repetition_separator=separators[2],
            escape_character=parsed.esc,
            subcomponent_separator=separators[4],
        )
        parsed_segments.append(segment)
        # Only query-backed fields enter ROOT; segmentCounts records every segment type.
        mapped = _legacy_projection(segment)
        if mapped is not None:
            existing = root.get(name)
            root[name] = mapped if existing is None else _merge_objects(existing, mapped)

    if not parsed_segments or parsed_segments[0].name != "MSH":
        raise ParseError(INVALID_MSH)

    header = parsed_segments[0]
    message_type = header.component(9, 1)
    trigger_event = header.component(9, 2)
    message_control_id = header.field(10)
    facility = header.component(4, 1)
    message_time_raw = header.field(7)
    message_time = _normalize_hl7_timestamp(message_time_raw)
    participant = _participant_from_key(source.key) or facility or None
    # The same normalized message under one S3 key always resolves to one logical document.
    document_id = hashlib.sha256(
        f"{source.bucket}\0{source.key}\0{message_checksum}".encode()
    ).hexdigest()

    document: dict[str, Any] = {
        "documentId": document_id,
        "parserVersion": PARSER_VERSION,
        "sourceFormat": "hl7-v2",
        "sourceFacilityId": facility or participant,
        "participantId": participant,
        "messageType": message_type,
        "triggerEvent": trigger_event,
        "messageControlId": message_control_id,
        "messageOrdinal": ordinal,
        "messageTime": message_time,
        "messageTimeRaw": message_time_raw,
        "segmentCounts": dict(sorted(counters.items())),
        "ROOT": root,
        "rawObject": {
            "bucket": source.bucket,
            "key": source.key,
            "versionId": source.version_id,
            "etag": source.etag,
            "sha256": raw_checksum,
        },
    }
    if ingested_at is not None:
        document["ingestTime"] = ingested_at
    return document


def _parse_with_library(message: str) -> Message:
    """Parse one message without allowing library errors to expose clinical content."""
    # python-hl7 strips the complete input. A disposable final segment keeps meaningful
    # spaces in the source message's final field away from that outer strip operation.
    try:
        parsed = hl7.parse(f"{message}\rZ99")
        parsed.pop()
    except (AssertionError, HL7Exception, IndexError, ValueError) as error:
        raise ParseError(INVALID_MSH) from error
    return parsed


def _legacy_projection(segment: _SegmentView) -> dict[str, Any] | None:
    """Project fields used by supplied dashboard queries without claiming Prism parity."""
    mapping_factories: dict[str, Callable[[], dict[str, Any]]] = {
        "MSH": lambda: {
            "MSH_1_Field_Separator": segment.field(1),
            "MSH_4_Sending_Facility": {
                "HD_1": segment.component(4, 1),
                "HD_2": segment.component(4, 2),
            },
            "MSH_7_Date-Time_of_Message": segment.field(7),
            "MSH_9_Message_Type": {
                "MSG_1": segment.component(9, 1),
                "MSG_2": segment.component(9, 2),
            },
            "MSH_10_Message_Control_ID": segment.field(10),
        },
        "PID": lambda: {
            "PID_3_Patient_Identifier_List": {"CX_1": segment.component(3, 1)},
            "PID_5_Patient_Name": {
                "XPN_1": {"FN_1": segment.component(5, 1)},
                "XPN_2": segment.component(5, 2),
            },
            "PID_7_Date-Time_of_Birth": segment.field(7),
            "PID_8_Administrative_Sex": segment.field(8),
        },
        "NK1": lambda: {
            # Referenced by the supplied ADT report query rows (next-of-kin relationship).
            "NK1_3_Relationship": {
                "CWE_1": segment.component(3, 1),
                "CWE_2": segment.component(3, 2),
            }
        },
        "PV1": lambda: {
            "PV1_2_Patient_Class": segment.field(2),
            "PV1_7_Attending_Doctor": {"XCN_1": segment.component(7, 1)},
            "PV1_8_Referring_Doctor": {"XCN_1": segment.component(8, 1)},
            "PV1_9_Consulting_Doctor": {"XCN_1": segment.component(9, 1)},
            "PV1_17_Admitting_Doctor": {"XCN_1": segment.component(17, 1)},
            "PV1_44_Admit_Date-Time": segment.field(44),
            "PV1_45_Discharge_Date-Time": segment.field(45),
        },
        "PV2": lambda: {
            "PV2_3_Admit_Reason": {
                "CWE_1": segment.component(3, 1),
                "CWE_2": segment.component(3, 2),
            }
        },
        "OBR": lambda: {
            "OBR_4_Universal_Service_Identifier": {
                "CWE_1": segment.component(4, 1),
                "CWE_3": segment.component(4, 3),
            },
            "OBR_22_Results_Rpt-Status_Chng_-_Date-Time_": segment.field(22),
            "OBR_24_Diagnostic_Serv_Sect_ID": segment.field(24),
            "OBR_25_Result_Status_": segment.field(25),
        },
        "OBX": lambda: {
            "OBX_1_Set_ID_-_OBX": segment.field(1),
            "OBX_2_Value_Type": segment.field(2),
            "OBX_3_Observation_Identifier": {
                "CWE_1": segment.component(3, 1),
                "CWE_3": segment.component(3, 3),
            },
            "OBX_11_Observation_Result_Status": segment.field(11),
        },
    }
    factory = mapping_factories.get(segment.name)
    if factory is None:
        return None
    mapped = factory()
    # Object fields are not searchable for existence, so each projected segment gets a marker.
    mapped["_present"] = True
    return _without_empty_values(mapped)


def _without_empty_values(value: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, dict):
            nested = _without_empty_values(item)
            if nested:
                cleaned[key] = nested
        elif item != "":
            cleaned[key] = item
    return cleaned


def _merge_objects(existing: Any, incoming: Any) -> Any:
    # Repeated segments/components become arrays while first occurrences retain scalar objects.
    if isinstance(existing, dict) and isinstance(incoming, dict):
        merged = dict(existing)
        for key, value in incoming.items():
            merged[key] = value if key not in merged else _merge_objects(merged[key], value)
        return merged
    if isinstance(existing, list):
        return [*existing, incoming]
    return [existing, incoming]


def _decode_escapes(value: str, segment: _SegmentView) -> str:
    # Decode only standard delimiter escapes; leave application-specific escape content untouched.
    replacements = {
        "F": segment.field_separator,
        "S": segment.component_separator,
        "R": segment.repetition_separator,
        "E": segment.escape_character,
        "T": segment.subcomponent_separator,
    }
    pattern = re.compile(
        re.escape(segment.escape_character) + "([FSRET])" + re.escape(segment.escape_character)
    )
    return pattern.sub(lambda match: replacements[match.group(1)], value)


def _normalize_hl7_timestamp(value: str) -> str | None:
    match = re.fullmatch(
        r"(?P<date>\d{4}(?:\d{2}){0,5})(?:\.(?P<fraction>\d+))?(?P<offset>[+-]\d{4})?",
        value,
    )
    if match is None:
        return None
    digits = match.group("date")
    if len(digits) not in {4, 6, 8, 10, 12, 14}:
        return None
    # HL7 permits reduced precision; fill omitted components with the earliest valid instant.
    padded = digits + "0101000000"[len(digits) - 4 :]
    try:
        parsed = datetime.strptime(padded, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None
    fraction = match.group("fraction")
    if fraction:
        parsed = parsed.replace(microsecond=int((fraction + "000000")[:6]))
    offset = match.group("offset")
    if offset:
        sign = 1 if offset[0] == "+" else -1
        minutes = sign * (int(offset[1:3]) * 60 + int(offset[3:5]))
        parsed = (parsed.replace(tzinfo=None) - timedelta(minutes=minutes)).replace(tzinfo=UTC)
    return parsed.isoformat().replace("+00:00", "Z")


def _participant_from_key(key: str) -> str | None:
    match = re.search(r"(?:^|/)participant=([^/]+)", key)
    return match.group(1) if match else None
