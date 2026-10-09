# fiftyone-sync, Apache-2.0 license
# Filename: src/app/media_mounts.py
# Description: Resolve Tator image media URLs to files on locally mounted storage (config `mounts`).
"""
Map image media URLs served by an nginx host onto a locally mounted filesystem.

Configured with a global ``mounts`` list in the sync config (FIFTYONE_SYNC_CONFIG_PATH)::

    mounts:
      - name: "image"
        path: "/mnt/DeepSea-AI"
        host: "https://cortex.shore.mbari.org"
        nginx_root: "/DeepSea-AI"

An image whose source URL is ``https://cortex.shore.mbari.org/DeepSea-AI/a/b.png``
resolves to ``/mnt/DeepSea-AI/a/b.png``. When that file exists the crop pipeline
reads it in place instead of downloading a copy; otherwise it falls back to the
normal download. Only image media are resolved -- videos always download.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

import yaml

logger = logging.getLogger(__name__)

_DEFAULT_PORTS = {"http": 80, "https": 443}
# Mirrors sync.VIDEO_EXTENSIONS (kept local to avoid a circular import).
_VIDEO_EXTENSIONS = (".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v")


@dataclass(frozen=True)
class MediaMount:
    name: str
    path: str
    host: str
    nginx_root: str


def _netloc(host_or_url: str) -> str:
    """Lower-cased host[:port] with the scheme's default port dropped."""
    s = (host_or_url or "").strip()
    if "://" not in s:
        s = "//" + s
    parsed = urlparse(s)
    hostname = (parsed.hostname or "").lower()
    try:
        port = parsed.port
    except ValueError:
        port = None
    if port is None or port == _DEFAULT_PORTS.get(parsed.scheme):
        return hostname
    return f"{hostname}:{port}"


def _normalize_root(root: str) -> str:
    root = "/" + (root or "").strip().strip("/")
    return root if root != "/" else ""


def parse_mounts(config: dict[str, Any] | None) -> list[MediaMount]:
    """Parse the global ``mounts`` config list. Invalid entries are skipped with a warning."""
    raw = (config or {}).get("mounts") or []
    if not isinstance(raw, list):
        logger.warning("Config 'mounts' must be a list; ignoring")
        return []
    mounts: list[MediaMount] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            logger.warning("Config mounts[%s] is not a mapping; ignoring", i)
            continue
        path = str(entry.get("path") or "").strip()
        host = str(entry.get("host") or "").strip()
        if not path or not host or not _netloc(host):
            logger.warning("Config mounts[%s] needs both 'path' and 'host'; ignoring", i)
            continue
        mounts.append(
            MediaMount(
                name=str(entry.get("name") or f"mount{i}"),
                path=os.path.normpath(path),
                host=_netloc(host),
                nginx_root=_normalize_root(str(entry.get("nginx_root") or "")),
            )
        )
    return mounts


_cache: dict[str, tuple[float, list[MediaMount]]] = {}


def load_mounts() -> list[MediaMount]:
    """Mounts from the file at FIFTYONE_SYNC_CONFIG_PATH (re-read when the file changes)."""
    path = os.getenv("FIFTYONE_SYNC_CONFIG_PATH")
    if not path or not os.path.isfile(path):
        return []
    try:
        mtime = os.path.getmtime(path)
        cached = _cache.get(path)
        if cached and cached[0] == mtime:
            return cached[1]
        with open(path) as f:
            mounts = parse_mounts(yaml.safe_load(f) or {})
        _cache[path] = (mtime, mounts)
        if mounts:
            logger.info(
                "Media mounts: %s",
                ", ".join(f"{m.name}: {m.host}{m.nginx_root} -> {m.path}" for m in mounts),
            )
        return mounts
    except Exception as e:
        logger.warning("Could not load media mounts from %s: %s", path, e)
        return []


def resolve_url_to_local_path(url: str, mounts: list[MediaMount]) -> str | None:
    """Absolute path of an existing local file for ``url``, or None if no mount matches."""
    if not url or not mounts or "://" not in url:
        return None
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    netloc = _netloc(url)
    url_path = unquote(parsed.path or "")
    for m in mounts:
        if netloc != m.host:
            continue
        root = m.nginx_root
        if root and not (url_path == root or url_path.startswith(root + "/")):
            continue
        rel = url_path[len(root):].lstrip("/")
        if not rel:
            continue
        candidate = os.path.normpath(os.path.join(m.path, rel))
        # Reject `..` segments that would escape the mount.
        if os.path.commonpath([candidate, m.path]) != m.path:
            continue
        if os.path.isfile(candidate):
            return os.path.abspath(candidate)
    return None


def _media_source_urls(media: Any) -> list[str]:
    """Candidate source URLs for an image media: media_files.image paths, then a
    ``source_url`` field/attribute if present."""
    urls: list[str] = []
    files = getattr(media, "media_files", None)
    images = getattr(files, "image", None) if files is not None else None
    if images is None and isinstance(files, dict):
        images = files.get("image")
    for img in images or []:
        p = img.get("path") if isinstance(img, dict) else getattr(img, "path", None)
        if p:
            urls.append(str(p))
    src = getattr(media, "source_url", None)
    attrs = getattr(media, "attributes", None)
    if not src and isinstance(attrs, dict):
        src = attrs.get("source_url")
    if src:
        urls.append(str(src))
    return urls


def resolve_media_local_path(
    media: Any, mounts: list[MediaMount] | None = None
) -> str | None:
    """Absolute local path for an image media via configured mounts, else None.

    ``mounts`` defaults to the global config. Video media always return None.
    """
    if media is None:
        return None
    if mounts is None:
        mounts = load_mounts()
    if not mounts:
        return None
    if (getattr(media, "name", "") or "").lower().endswith(_VIDEO_EXTENSIONS):
        return None
    for url in _media_source_urls(media):
        local = resolve_url_to_local_path(url, mounts)
        if local:
            return local
    return None
