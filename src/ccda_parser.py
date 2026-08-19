"""Safely project structured CCDA XML into the customer dashboard document shape."""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import ParseError as XmlParseError

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException
from src.parser import ParseError, SourceReference

PARSER_VERSION = "0.1.0"
INVALID_XML = "Raw object is not a safe, well-formed XML document"
INVALID_DOCUMENT = "XML root is not an HL7 ClinicalDocument"
XML_TOO_COMPLEX = "XML document exceeds the configured structural complexity limit"
MAX_MARKUP_TOKENS = 2_000_000
MAX_XML_DEPTH = 128

# Standard LOINC section codes map to the legacy keys used by customer queries.
_SECTION_NAMES = {
    "10160-0": "medications-section",
    "11369-6": "immunizations-section",
    "11450-4": "problem-section",
    "47519-4": "procedures-section",
    "30954-2": "results-section",
    "29762-2": "socialHistory-section",
    "8716-3": "vitalSigns-section",
}


def parse_ccda_document(
    payload: bytes,
    source: SourceReference,
    *,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Parse one CCDA document into the supplied dashboard compatibility shape."""
    # Bound obvious markup amplification before allocating the XML tree.
    if payload.count(b"<") > MAX_MARKUP_TOKENS:
        raise ParseError(XML_TOO_COMPLEX)
    try:
        root = ElementTree.fromstring(
            payload,
            forbid_dtd=True,
            forbid_entities=True,
            forbid_external=True,
        )
    except (DefusedXmlException, XmlParseError, UnicodeError, ValueError) as error:
        raise ParseError(INVALID_XML) from error
    if _local_name(root.tag) != "ClinicalDocument":
        raise ParseError(INVALID_DOCUMENT)
    _require_safe_depth(root)

    checksum = hashlib.sha256(payload).hexdigest()
    # Prefer immutable S3 identity, with content hash as a deterministic fallback.
    source_identity = source.version_id or source.etag or checksum
    document_id = hashlib.sha256(
        f"{source.bucket}\0{source.key}\0{source_identity}\0ccda".encode()
    ).hexdigest()
    participant = _participant_from_key(source.key)
    document_time_raw = _attribute(_child(root, "effectiveTime"), "value")

    # Project structured entries only; narrative section text is intentionally excluded.
    projection: dict[str, Any] = {
        "recordTarget": _record_target(_child(root, "recordTarget")),
        "custodian": _custodian(_child(root, "custodian")),
        "body": _body(root),
    }
    projection = _compact_dict(projection)

    document: dict[str, Any] = {
        "documentId": document_id,
        "parserVersion": PARSER_VERSION,
        "sourceFormat": "ccda",
        "sourceFacilityId": participant,
        "participantId": participant,
        "documentTime": _normalize_cda_timestamp(document_time_raw),
        "documentTimeRaw": document_time_raw,
        "sectionCounts": _section_counts(root),
        "CD": projection,
        "rawObject": {
            "bucket": source.bucket,
            "key": source.key,
            "versionId": source.version_id,
            "etag": source.etag,
            "sha256": checksum,
        },
    }
    if ingested_at is not None:
        document["ingestTime"] = ingested_at
    return document


def _record_target(record_target: Element | None) -> dict[str, Any] | None:
    patient_role = _child(record_target, "patientRole")
    if patient_role is None:
        return None
    patient = _child(patient_role, "patient")
    identifiers = [_identifier(item) for item in _children(patient_role, "id")]
    addresses = [_address(item) for item in _children(patient_role, "addr")]
    telecoms = [
        value for item in _children(patient_role, "telecom") if (value := item.get("value"))
    ]
    projected_role = _compact_dict(
        {
            "_present": True,
            "id": _collapse(identifiers),
            "addr": _collapse(addresses),
            "telecom": _collapse(telecoms),
            "patient": _patient(patient),
        }
    )
    return {"patientRole": projected_role}


def _patient(patient: Element | None) -> dict[str, Any] | None:
    if patient is None:
        return None
    names = [_person_name(item) for item in _children(patient, "name")]
    gender = _child(patient, "administrativeGenderCode")
    ethnicity = _child(patient, "ethnicGroupCode")
    return _compact_dict(
        {
            "name": _collapse(names),
            "administrativeGenderCode": _attributes(gender, "code"),
            "birthTime": _attribute(_child(patient, "birthTime"), "value"),
            "raceCode": _attribute(_child(patient, "raceCode"), "code"),
            "ethnicGroupCode": _attributes(ethnicity, "code"),
        }
    )


def _custodian(custodian: Element | None) -> dict[str, Any] | None:
    assigned = _child(custodian, "assignedCustodian")
    organization = _child(assigned, "representedCustodianOrganization")
    if assigned is None or organization is None:
        return None
    identifiers = [_identifier(item) for item in _children(organization, "id")]
    return {
        "assignedCustodian": _compact_dict(
            {
                "_present": True,
                "representedCustodianOrganization": _compact_dict(
                    {
                        "id": _collapse(identifiers),
                        "name": _text(_child(organization, "name")),
                    }
                ),
            }
        )
    }


def _body(root: Element) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for section in root.findall(".//{*}structuredBody/{*}component/{*}section"):
        code = _attribute(_child(section, "code"), "code")
        section_name = _SECTION_NAMES.get(code or "")
        if section_name is None:
            # Unknown sections still count, but do not enter the compatibility projection.
            continue
        projected = _section(section, section_name)
        existing = body.get(section_name)
        body[section_name] = projected if existing is None else _merge(existing, projected)
    return body


def _section(section: Element, section_name: str) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for entry in _children(section, "entry"):
        projected = _entry(entry, section_name)
        if projected:
            entries.append(projected)
    return _compact_dict(
        {
            "_present": True,
            "code": _code(_child(section, "code")),
            "entry": _collapse(entries),
        }
    )


def _entry(entry: Element, section_name: str) -> dict[str, Any]:
    if section_name in {"medications-section", "immunizations-section"}:
        administration = _child(entry, "substanceAdministration")
        return _compact_dict({"substanceAdministration": _substance_administration(administration)})
    if section_name in {"results-section", "vitalSigns-section"}:
        return _compact_dict({"organizer": _organizer(_child(entry, "organizer"))})
    if section_name == "problem-section":
        return _compact_dict({"act": _act(_child(entry, "act"))})
    if section_name == "procedures-section":
        return _compact_dict({"procedure": _procedure(_child(entry, "procedure"))})
    if section_name == "socialHistory-section":
        activity = _child(entry, "act")
        if activity is None:
            activity = _child(entry, "observation")
        return _compact_dict({"act": _act(activity)})
    return {}


def _substance_administration(element: Element | None) -> dict[str, Any] | None:
    if element is None:
        return None
    identifiers = [_identifier(item) for item in _children(element, "id")]
    times = [_effective_time(item) for item in _children(element, "effectiveTime")]
    relationships = [_entry_relationship(item) for item in _children(element, "entryRelationship")]
    return _compact_dict(
        {
            "id": _collapse(identifiers),
            "statusCode": _attributes(_child(element, "statusCode"), "code"),
            "effectiveTime": _collapse(times),
            "doseQuantity": _attributes(_child(element, "doseQuantity"), "value", "unit"),
            "routeCode": _code(_child(element, "routeCode")),
            "consumable": _consumable(_child(element, "consumable")),
            "performer": _performers(element),
            "entryRelationship": _collapse(relationships),
            "code": _code(_child(element, "code")),
        }
    )


def _consumable(element: Element | None) -> dict[str, Any] | None:
    product = _child(element, "manufacturedProduct")
    material = _child(product, "manufacturedMaterial")
    if material is None:
        return None
    return {
        "manufacturedProduct": {
            "manufacturedMaterial": _compact_dict(
                {
                    "code": _code(_child(material, "code")),
                    "name": _text(_child(material, "name")),
                }
            )
        }
    }


def _organizer(element: Element | None) -> dict[str, Any] | None:
    if element is None:
        return None
    identifiers = [_identifier(item) for item in _children(element, "id")]
    components = [
        _compact_dict({"observation": _observation(_child(item, "observation"))})
        for item in _children(element, "component")
    ]
    return _compact_dict(
        {
            "id": _collapse(identifiers),
            "code": _code(_child(element, "code")),
            "statusCode": _attributes(_child(element, "statusCode"), "code"),
            "effectiveTime": _effective_time(_child(element, "effectiveTime")),
            "component": _collapse(components),
            "performer": _performers(element),
        }
    )


def _observation(element: Element | None) -> dict[str, Any] | None:
    if element is None:
        return None
    interpretations = [_code(item) for item in _children(element, "interpretationCode")]
    relationships = [_entry_relationship(item) for item in _children(element, "entryRelationship")]
    ranges = [_reference_range(item) for item in _children(element, "referenceRange")]
    return _compact_dict(
        {
            "code": _code(_child(element, "code")),
            "text": _reference_text(_child(element, "text")),
            "statusCode": _attributes(_child(element, "statusCode"), "code"),
            "effectiveTime": _effective_time(_child(element, "effectiveTime")),
            "value": _value(_child(element, "value")),
            "interpretationCode": _collapse(interpretations),
            "referenceRange": _collapse(ranges),
            "entryRelationship": _collapse(relationships),
        }
    )


def _reference_range(element: Element) -> dict[str, Any]:
    observation_range = _child(element, "observationRange")
    return _compact_dict(
        {"observationRange": _compact_dict({"value": _value(_child(observation_range, "value"))})}
    )


def _act(element: Element | None) -> dict[str, Any] | None:
    if element is None:
        return None
    relationships = [_entry_relationship(item) for item in _children(element, "entryRelationship")]
    return _compact_dict(
        {
            "code": _code(_child(element, "code")),
            "text": _reference_text(_child(element, "text")),
            "statusCode": _attributes(_child(element, "statusCode"), "code"),
            "effectiveTime": _effective_time(_child(element, "effectiveTime")),
            "entryRelationship": _collapse(relationships),
        }
    )


def _entry_relationship(element: Element) -> dict[str, Any]:
    return _compact_dict(
        {
            "typeCode": element.get("typeCode"),
            "act": _act(_child(element, "act")),
            "observation": _observation(_child(element, "observation")),
            "substanceAdministration": _substance_administration(
                _child(element, "substanceAdministration")
            ),
        }
    )


def _procedure(element: Element | None) -> dict[str, Any] | None:
    if element is None:
        return None
    identifiers = [_identifier(item) for item in _children(element, "id")]
    return _compact_dict(
        {
            "id": _collapse(identifiers),
            "code": _code(_child(element, "code")),
            "effectiveTime": _effective_time(_child(element, "effectiveTime")),
            "performer": _performers(element, content_names=True),
        }
    )


def _performers(parent: Element, *, content_names: bool = False) -> Any:
    performers: list[dict[str, Any]] = []
    for performer in _children(parent, "performer"):
        assigned = _child(performer, "assignedEntity")
        if assigned is None:
            continue
        identifiers = [_identifier(item) for item in _children(assigned, "id")]
        person = _child(assigned, "assignedPerson")
        names = [
            _person_name(item, content_wrapped=content_names) for item in _children(person, "name")
        ]
        performers.append(
            {
                "assignedEntity": _compact_dict(
                    {
                        "id": _collapse(identifiers),
                        "assignedPerson": _compact_dict({"name": _collapse(names)}),
                    }
                )
            }
        )
    return _collapse(performers)


def _person_name(element: Element, *, content_wrapped: bool = False) -> dict[str, Any]:
    def values(name: str) -> Any:
        items = [_text(item) for item in _children(element, name)]
        cleaned = [item for item in items if item]
        if content_wrapped:
            return _collapse([{"content": item} for item in cleaned])
        return _collapse(cleaned)

    return _compact_dict({"given": values("given"), "family": values("family")})


def _address(element: Element) -> dict[str, Any]:
    return _compact_dict(
        {
            "streetAddressLine": _texts(element, "streetAddressLine"),
            "city": _texts(element, "city"),
            "state": _texts(element, "state"),
            "postalCode": _texts(element, "postalCode"),
        }
    )


def _identifier(element: Element) -> dict[str, Any]:
    return _attributes(element, "root", "extension") or {"_present": True}


def _code(element: Element | None) -> dict[str, Any] | None:
    if element is None:
        return None
    translations = [_code(item) for item in _children(element, "translation")]
    return _compact_dict(
        {
            **_attributes(
                element,
                "code",
                "codeSystem",
                "codeSystemName",
                "displayName",
                "nullFlavor",
            ),
            "originalText": _reference_text(_child(element, "originalText")),
            "translation": _collapse(translations),
        }
    )


def _value(element: Element | None) -> dict[str, Any] | None:
    if element is None:
        return None
    return _compact_dict(
        {
            **_attributes(
                element,
                "value",
                "unit",
                "code",
                "codeSystem",
                "codeSystemName",
                "displayName",
                "nullFlavor",
            ),
            "low": _attributes(_child(element, "low"), "value", "unit"),
            "high": _attributes(_child(element, "high"), "value", "unit"),
            "originalText": _reference_text(_child(element, "originalText")),
        }
    )


def _effective_time(element: Element | None) -> dict[str, Any] | None:
    if element is None:
        return None
    return _compact_dict(
        {
            **_attributes(element, "value", "nullFlavor"),
            "low": _attributes(_child(element, "low"), "value"),
            "high": _attributes(_child(element, "high"), "value"),
        }
    )


def _reference_text(element: Element | None) -> dict[str, Any] | str | None:
    if element is None:
        return None
    reference = _attributes(_child(element, "reference"), "value")
    content = _text(element)
    projected = _compact_dict({"content": content, "reference": reference})
    return projected or None


def _attributes(element: Element | None, *names: str) -> dict[str, Any]:
    if element is None:
        return {}
    return _compact_dict({name: element.get(name) for name in names})


def _attribute(element: Element | None, name: str) -> str | None:
    return element.get(name) if element is not None else None


def _child(element: Element | None, name: str) -> Element | None:
    return element.find(f"./{{*}}{name}") if element is not None else None


def _children(element: Element | None, name: str) -> list[Element]:
    return list(element.findall(f"./{{*}}{name}")) if element is not None else []


def _text(element: Element | None) -> str | None:
    if element is None:
        return None
    text = "".join(element.itertext()).strip()
    return text or None


def _texts(element: Element, child_name: str) -> Any:
    return _collapse([value for item in _children(element, child_name) if (value := _text(item))])


def _collapse(values: list[Any]) -> Any:
    # Preserve the legacy scalar-for-one/list-for-many shape expected by saved queries.
    cleaned = [value for value in values if value is not None and value != ""]
    if not cleaned:
        return None
    return cleaned[0] if len(cleaned) == 1 else cleaned


def _compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        if item is None or item == "" or item == {} or item == []:
            continue
        result[key] = item
    return result


def _merge(existing: Any, incoming: Any) -> Any:
    if isinstance(existing, list):
        return [*existing, incoming]
    return [existing, incoming]


def _section_counts(root: Element) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for section in root.findall(".//{*}structuredBody/{*}component/{*}section"):
        code = _attribute(_child(section, "code"), "code")
        if code:
            counts[code] += 1
    return dict(sorted(counts.items()))


def _require_safe_depth(root: Element) -> None:
    # Use an explicit stack so the safety check itself cannot overflow Python recursion.
    stack = [(root, 1)]
    while stack:
        element, depth = stack.pop()
        if depth > MAX_XML_DEPTH:
            raise ParseError(XML_TOO_COMPLEX)
        stack.extend((child, depth + 1) for child in element)


def _normalize_cda_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    match = re.fullmatch(
        r"(?P<date>\d{4}(?:\d{2}){0,5})(?:\.(?P<fraction>\d+))?(?P<offset>[+-]\d{4})?",
        value,
    )
    if match is None:
        return None
    digits = match.group("date")
    if len(digits) not in {4, 6, 8, 10, 12, 14}:
        return None
    # CDA permits reduced precision; fill omitted components with the earliest valid instant.
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


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
