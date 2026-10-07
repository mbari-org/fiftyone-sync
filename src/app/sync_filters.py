# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync_filters.py
# Description: Tator section, query, and include_classes filter helpers for sync.
"""Tator section, encoded_search, and include_classes query filter helpers for sync."""

from __future__ import annotations

import base64
import hashlib
import json
import os

# Tator localization attribute used as the Voxel51 ground-truth label.
LABEL_ATTR = "Label"


def parse_include_classes(value: str | list[str] | None) -> list[str]:
    """Parse a comma-separated string or list of label names.

    Empty/whitespace entries are dropped. Order is preserved; duplicates are
    removed. Used by the applet Labels field and the include_classes config key.
    """
    if value is None:
        return []
    parts = value.split(",") if isinstance(value, str) else list(value)
    seen: set[str] = set()
    out: list[str] = []
    for part in parts:
        name = str(part).strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def include_classes_slug(include_classes: list[str] | None) -> str:
    """Stable cache-path slug for a label set (order-independent)."""
    classes = parse_include_classes(include_classes)
    if not classes:
        return ""
    key = "\0".join(sorted(classes))
    return "l" + hashlib.sha256(key.encode()).hexdigest()[:12]


def version_slug(version_id: int | None) -> str:
    return f"v{version_id}" if version_id is not None else "v_all"


def filter_slug(
    section_id: int | None = None,
    query: str | None = None,
    localization_type_id: int | None = None,
    include_classes: list[str] | None = None,
) -> str:
    """Slug for section/query/box-type/label filters in on-disk cache paths."""
    parts: list[str] = []
    if section_id is not None:
        parts.append(f"s{section_id}")
    q = (query or "").strip()
    if q:
        parts.append("q" + hashlib.sha256(q.encode()).hexdigest()[:12])
    if localization_type_id is not None:
        parts.append(f"t{localization_type_id}")
    class_slug = include_classes_slug(include_classes)
    if class_slug:
        parts.append(class_slug)
    return "_".join(parts)


def label_attribute_search(include_classes: list[str] | None) -> dict | None:
    """AttributeOperationSpec for LocalizationSpec.attributes[Label].

    One label is an equality filter. Several labels are an `or` combinator, because
    Tator attribute equality is a conjunction and a localization has one Label.
    """
    names = parse_include_classes(include_classes)
    if not names:
        return None
    if len(names) == 1:
        return {"attribute": LABEL_ATTR, "operation": "eq", "value": names[0]}
    return {
        "method": "or",
        "operations": [
            {"attribute": LABEL_ATTR, "operation": "eq", "value": name} for name in names
        ],
    }


def encode_object_search(spec: dict) -> str:
    """Base64 JSON for Tator encoded_search / encoded_related_search."""
    payload = json.dumps(spec, separators=(",", ":"), sort_keys=True).encode()
    return base64.b64encode(payload).decode("ascii")


def _decode_object_search(value: str) -> dict | None:
    raw = (value or "").strip()
    if not raw:
        return None
    padded = raw + ("=" * (-len(raw) % 4))
    try:
        parsed = json.loads(base64.b64decode(padded))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def merge_object_search(existing_b64: str | None, spec: dict) -> str:
    """AND a new AttributeOperationSpec into an existing encoded search, if it decodes."""
    existing = _decode_object_search(existing_b64 or "")
    if existing is None:
        if (existing_b64 or "").strip():
            return existing_b64.strip()
        return encode_object_search(spec)
    return encode_object_search({"method": "and", "operations": [existing, spec]})


def localization_fetch_kwargs(
    *,
    version_id: int | None = None,
    section_id: int | None = None,
    query: str | None = None,
    localization_type_id: int | None = None,
    verified_only: bool = False,
    include_classes: list[str] | None = None,
) -> dict:
    """Tator kwargs for localization list/count (version, section, encoded_search, type).

    When verified_only is True, adds an `attribute` filter for the localization's
    own `verified::true` attribute so Tator excludes unverified localizations
    server-side, instead of downloading everything and filtering client-side.

    When include_classes is set, Label is applied as an encoded_search
    AttributeOperationSpec against the localization's own attributes
    (LocalizationSpec.attributes). Multiple names are an `or` combinator in that
    one search. An existing encoded_search query is ANDed with the label search.
    """
    kw: dict = {}
    if version_id is not None:
        kw["version"] = [version_id]
    if section_id is not None:
        kw["section"] = section_id
    q = (query or "").strip()
    label_spec = label_attribute_search(include_classes)
    if label_spec:
        kw["encoded_search"] = merge_object_search(q or None, label_spec)
    elif q:
        kw["encoded_search"] = q
    if localization_type_id is not None:
        kw["type"] = [localization_type_id]
    if verified_only:
        kw["attribute"] = ["verified::true"]
    return kw


def localization_id_query(
    *,
    query: str | None = None,
    include_classes: list[str] | None = None,
    media_ids: list[int] | None = None,
) -> tuple[dict | None, str | None]:
    """LocalizationIdQuery body for a label-scoped localization list/count.

    Returns (body, leftover_query). The Label AttributeOperationSpec goes in the
    PUT body as `object_search`, so a long label list never lengthens the URL
    (nginx caps the request line near 4 KB). A decodable encoded_search query is
    ANDed into the same object_search; an undecodable one is returned as
    leftover_query for the encoded_search query param. media_ids also travel in
    the body, so they need no request-line batching. body is None when there is
    no label filter.
    """
    label_spec = label_attribute_search(include_classes)
    if not label_spec:
        return None, (query or "").strip() or None
    q = (query or "").strip()
    leftover: str | None = None
    spec = label_spec
    if q:
        existing = _decode_object_search(q)
        if existing is None:
            leftover = q
        else:
            spec = {"method": "and", "operations": [existing, label_spec]}
    body: dict = {"object_search": spec}
    if media_ids:
        body["media_ids"] = list(media_ids)
    return body, leftover


def media_fetch_kwargs(
    *,
    version_id: int | None = None,
    section_id: int | None = None,
    verified_only: bool = False,
    include_classes: list[str] | None = None,
    media_labels: bool = False,
) -> dict:
    """Tator kwargs for media list (version via related_attribute, section).

    media_labels=True is for classification media, whose Label and verified are
    the media's own attributes: labels go in encoded_search and verified in
    `attribute`, while version still filters on related metadata.

    When verified_only is True, adds `verified::true` to the `related_attribute`
    filter so Tator only returns media with at least one verified localization,
    instead of downloading all media and filtering client-side.

    When include_classes is set, media are refined with encoded_related_search.
    That search runs against related localization attributes
    (LocalizationSpec.attributes), which is how GetMediaList selects media that
    contain those labels. related_attribute is reserved for built-in related
    fields such as $version and verified.
    """
    kw: dict = {}
    related_attribute: list[str] = []
    if version_id is not None:
        related_attribute.append(f"$version::{version_id}")
    if verified_only:
        if media_labels:
            kw["attribute"] = ["verified::true"]
        else:
            related_attribute.append("verified::true")
    if related_attribute:
        kw["related_attribute"] = related_attribute
    label_spec = label_attribute_search(include_classes)
    if label_spec:
        key = "encoded_search" if media_labels else "encoded_related_search"
        kw[key] = encode_object_search(label_spec)
    if section_id is not None:
        kw["section"] = section_id
    return kw


def scoped_data_dir(
    sync_base: str,
    project_id: int,
    version_id: int | None,
    *,
    section_id: int | None = None,
    query: str | None = None,
    localization_type_id: int | None = None,
    include_classes: list[str] | None = None,
) -> str:
    """Per-project+version directory, with optional filter subdir."""
    path = os.path.join(sync_base, "data", str(project_id), version_slug(version_id))
    filt = filter_slug(
        section_id, query, localization_type_id, include_classes=include_classes
    )
    if filt:
        path = os.path.join(path, filt)
    os.makedirs(path, exist_ok=True)
    return path
