# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync_filters.py
# Description: Tator section and encoded_search query filter helpers for sync.
"""Tator section and encoded_search query filter helpers for sync."""

from __future__ import annotations

import hashlib
import os


def version_slug(version_id: int | None) -> str:
    return f"v{version_id}" if version_id is not None else "v_all"


def filter_slug(
    section_id: int | None = None,
    query: str | None = None,
    localization_type_id: int | None = None,
) -> str:
    """Slug for section/query/box-type filters in on-disk cache paths."""
    parts: list[str] = []
    if section_id is not None:
        parts.append(f"s{section_id}")
    q = (query or "").strip()
    if q:
        parts.append("q" + hashlib.sha256(q.encode()).hexdigest()[:12])
    if localization_type_id is not None:
        parts.append(f"t{localization_type_id}")
    return "_".join(parts)


def localization_fetch_kwargs(
    *,
    version_id: int | None = None,
    section_id: int | None = None,
    query: str | None = None,
    localization_type_id: int | None = None,
) -> dict:
    """Tator kwargs for localization list/count (version, section, encoded_search, type)."""
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
    return kw


def media_fetch_kwargs(
    *,
    version_id: int | None = None,
    section_id: int | None = None,
) -> dict:
    """Tator kwargs for media list (version via related_attribute, section)."""
    kw: dict = {}
    if version_id is not None:
        kw["related_attribute"] = [f"$version::{version_id}"]
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
) -> str:
    """Per-project+version directory, with optional filter subdir."""
    path = os.path.join(sync_base, "data", str(project_id), version_slug(version_id))
    filt = filter_slug(section_id, query, localization_type_id)
    if filt:
        path = os.path.join(path, filt)
    os.makedirs(path, exist_ok=True)
    return path
