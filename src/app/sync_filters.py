# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync_filters.py
# Description: Tator section, query, and include_classes filter helpers for sync.
"""Tator section, encoded_search, and include_classes query filter helpers for sync."""

from __future__ import annotations

import hashlib
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


def localization_fetch_kwargs(
    *,
    version_id: int | None = None,
    section_id: int | None = None,
    query: str | None = None,
    localization_type_id: int | None = None,
    verified_only: bool = False,
    include_class: str | None = None,
) -> dict:
    """Tator kwargs for localization list/count (version, section, encoded_search, type).

    When verified_only is True, adds an `attribute` filter for the localization's
    own `verified::true` attribute so Tator excludes unverified localizations
    server-side, instead of downloading everything and filtering client-side.

    When include_class is set, adds `Label::{name}` so Tator returns only that
    label. Multiple labels are fetched as separate requests (OR) by the caller.
    """
    kw: dict = {}
    if version_id is not None:
        kw["version"] = [version_id]
    if section_id is not None:
        kw["section"] = section_id
    q = (query or "").strip()
    if q:
        kw["encoded_search"] = q
    if localization_type_id is not None:
        kw["type"] = [localization_type_id]
    attribute: list[str] = []
    if verified_only:
        attribute.append("verified::true")
    class_name = (include_class or "").strip()
    if class_name:
        attribute.append(f"{LABEL_ATTR}::{class_name}")
    if attribute:
        kw["attribute"] = attribute
    return kw


def media_fetch_kwargs(
    *,
    version_id: int | None = None,
    section_id: int | None = None,
    verified_only: bool = False,
) -> dict:
    """Tator kwargs for media list (version via related_attribute, section).

    When verified_only is True, adds `verified::true` to the `related_attribute`
    filter so Tator only returns media with at least one verified localization,
    instead of downloading all media and filtering client-side.
    """
    kw: dict = {}
    related_attribute: list[str] = []
    if version_id is not None:
        related_attribute.append(f"$version::{version_id}")
    if verified_only:
        related_attribute.append("verified::true")
    if related_attribute:
        kw["related_attribute"] = related_attribute
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
