# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync.py
# Description: Tator to FiftyOne sync: fetch media and localizations, build dataset, launch app.
"""
Tator to FiftyOne sync: fetch media + localizations, build FiftyOne dataset, launch app.
Phase 2 implementation. Requires fiftyone, tator, PyYAML and MongoDB. Cropping uses PIL.
"""

from __future__ import annotations

from collections import defaultdict
import glob
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any

import fiftyone as fo
import tator
import yaml
from PIL import Image
from src.app.database_uri_config import database_name_from_uri
from src.app.database_manager import (
    get_database_entry_or_enterprise_default,
    get_database_name,
    get_database_uri,
    get_is_enterprise,
    get_port_for_project,
    get_s3_config,
)
from src.app.sync_filters import (
    filter_slug as _filter_slug,
    localization_fetch_kwargs as _localization_fetch_kwargs,
    media_fetch_kwargs as _media_fetch_kwargs,
    scoped_data_dir,
    version_slug as _version_slug_from_filters,
)
from src.app.sync_lock import (
    get_sync_lock_key,
    get_sync_to_tator_lock_key,
    release_sync_lock,
    try_acquire_sync_lock,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# logger.info to console
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Fallback batch sizes when not set in config (see config.yml: media_id_batch_size, localization_batch_size)
_DEFAULT_MEDIA_ID_BATCH_SIZE = 200
_DEFAULT_LOCALIZATION_BATCH_SIZE = 5000
# Max media IDs per request so URL stays under nginx request line limit (e.g. 4094 bytes).
# Each media_id in query string is ~17 bytes; base URL + version ~500; 150 * 17 + 500 < 4094.
_MAX_SAFE_MEDIA_ID_BATCH_SIZE = 150

# sync_edits_to_tator: batch LocalizationList PUT/PATCH (elemental_ids / bulk update).
# Chunk sizes are tunable via env vars so production can throttle bursts that would
# otherwise overwhelm the Tator API (defaults reduced from 500 to 100/100).
# FIFTYONE_SYNC_TO_TATOR_FETCH_CHUNK: elemental-id resolve chunk size (PUT by ids).
# FIFTYONE_SYNC_TO_TATOR_PATCH_CHUNK: bulk PATCH chunk size (update_localization_list).
# FIFTYONE_SYNC_TO_TATOR_CHUNK_DELAY_MS: optional sleep between PATCH chunks (smooths writes).


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    """Parse positive int env var; fall back to default on missing/invalid."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        val = int(raw)
    except ValueError:
        return default
    return max(minimum, val)


def _sync_to_tator_fetch_chunk() -> int:
    return _env_int("FIFTYONE_SYNC_TO_TATOR_FETCH_CHUNK", 100, minimum=1)


def _sync_to_tator_patch_chunk() -> int:
    return _env_int("FIFTYONE_SYNC_TO_TATOR_PATCH_CHUNK", 100, minimum=1)


def _sync_to_tator_chunk_delay_seconds() -> float:
    """Inter-chunk sleep in seconds for bulk PATCH (0 disables)."""
    raw = os.environ.get("FIFTYONE_SYNC_TO_TATOR_CHUNK_DELAY_MS", "").strip()
    if not raw:
        return 0.0
    try:
        ms = float(raw)
    except ValueError:
        return 0.0
    return max(0.0, ms) / 1000.0


_SYNC_TO_TATOR_ELEMENTAL_FETCH_CHUNK = 100
_SYNC_TO_TATOR_BULK_PATCH_CHUNK = 100

# Sample field storing Tator localization modified time (FiftyOne's last_modified_at is read-only)
TATOR_MODIFIED_AT_FIELD = "tator_modified_at"

# Max log lines stored in RQ job metadata for applet progress display
_JOB_LOG_LINES_CAP = 4000
_JOB_LOG_LOG_FLUSH_EVERY_N_LINES = 5


class _JobMetaLogHandler(logging.Handler):
    """Logging handler that appends lines to RQ job.meta['log_lines'] for applet progress display."""

    def __init__(
        self,
        job: Any,
        cap: int = _JOB_LOG_LINES_CAP,
        flush_every: int = _JOB_LOG_LOG_FLUSH_EVERY_N_LINES,
    ) -> None:
        super().__init__()
        self._job = job
        self._cap = cap
        self._flush_every = flush_every
        self._lines: list[str] = []
        self._formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self._formatter.format(record)
            self._lines.append(msg)
            if len(self._lines) > self._cap:
                self._lines = self._lines[-self._cap :]
            if len(self._lines) % self._flush_every == 0:
                self._flush()
        except Exception:  # avoid breaking the worker on log errors
            pass

    def _flush(self) -> None:
        try:
            self._job.meta["log_lines"] = list(self._lines)
            self._job.save_meta()
        except Exception:  # e.g. Redis unavailable
            pass

    def close(self) -> None:
        self._flush()
        super().close()


def _test_mongodb_connection(database_uri: str, timeout_ms: int = 5000) -> None:
    """Verify MongoDB is reachable before doing expensive Tator API work.

    Raises ConnectionError if the server cannot be reached within *timeout_ms*.
    """
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

    client = MongoClient(database_uri, serverSelectionTimeoutMS=timeout_ms)
    try:
        client.admin.command("ping")
    except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
        raise ConnectionError(
            f"Cannot connect to MongoDB at {database_uri}: {exc}"
        ) from exc
    finally:
        client.close()


def _test_fiftyone_connection() -> None:
    """Verify FiftyOne backend is reachable via a basic API call (e.g. list datasets).

    Used when is_enterprise=True instead of direct MongoDB check.
    Raises ConnectionError if the backend cannot be reached.
    """
    try:
        fo.list_datasets()
    except Exception as exc:
        raise ConnectionError(
            f"FiftyOne connection check failed (list_datasets): {exc}"
        ) from exc


def _json_serial(obj: Any) -> Any:
    """Convert datetime/date to epoch seconds (float) for JSON serialization."""
    if isinstance(obj, datetime):
        return obj.timestamp()
    if isinstance(obj, date):
        return datetime.combine(obj, datetime.min.time()).timestamp()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


_SYNC_BASE = os.environ.get("FIFTYONE_SYNC_BASE", "/tmp/fiftyone_sync")


def _version_slug(version_id: int | None) -> str:
    return _version_slug_from_filters(version_id)


def _download_dir(project_id: int) -> str:
    """Ephemeral media-download directory, isolated from JSONL and crops."""
    path = os.path.join(_SYNC_BASE, "downloads", str(project_id))
    os.makedirs(path, exist_ok=True)
    return path


def _data_dir(
    project_id: int,
    version_id: int | None,
    *,
    section_id: int | None = None,
    query: str | None = None,
) -> str:
    """Per-project+version directory for JSONL, crops, and manifest."""
    return scoped_data_dir(
        _SYNC_BASE,
        project_id,
        version_id,
        section_id=section_id,
        query=query,
    )


def _crops_dir(
    project_id: int,
    version_id: int | None,
    *,
    section_id: int | None = None,
    query: str | None = None,
) -> str:
    """Per-project+version crops directory."""
    path = os.path.join(
        _data_dir(project_id, version_id, section_id=section_id, query=query), "crops"
    )
    os.makedirs(path, exist_ok=True)
    return path


def _localizations_jsonl_path(
    project_id: int,
    version_id: int | None,
    *,
    section_id: int | None = None,
    query: str | None = None,
) -> str:
    """Per-project+version JSONL path (optional section/query filter scope)."""
    return os.path.join(
        _data_dir(project_id, version_id, section_id=section_id, query=query),
        "localizations.jsonl",
    )


def _file_newer_than_days(filepath: str, days: float = 1.0) -> bool:
    """True if file exists and was modified within the last *days* days."""
    if not filepath or not os.path.isfile(filepath):
        return False
    try:
        mtime = os.path.getmtime(filepath)
        return (time.time() - mtime) <= (days * 24 * 3600)
    except OSError:
        return False


def _localizations_jsonl_line_count_and_media_ids(path: str) -> tuple[int, list[int]]:
    """
    Stream JSONL and return (line count, distinct media IDs from "media" field).
    Returns (0, []) if file is missing or unreadable.
    """
    if not path or not os.path.isfile(path):
        return (0, [])
    line_count = 0
    media_ids: set[int] = set()
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                line_count += 1
                try:
                    obj = json.loads(line)
                    mid = obj.get("media")
                    if mid is not None:
                        media_ids.add(int(mid))
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
    except OSError:
        return (0, [])
    return (line_count, sorted(media_ids))


def _get_localization_count_from_api(
    api: Any,
    project_id: int,
    version_id: int | None,
    media_ids: list[int] | None,
    media_id_batch_size: int,
    *,
    section_id: int | None = None,
    query: str | None = None,
) -> int | None:
    """
    Return total localization count from Tator API (same batching as fetch_and_save_localizations).
    Returns None on error.
    """
    mid_batch = min(media_id_batch_size, _MAX_SAFE_MEDIA_ID_BATCH_SIZE)
    media_id_batches: list[list[int] | None] = (
        [
            media_ids[i : i + mid_batch]
            for i in range(0, len(media_ids or []), mid_batch)
        ]
        if media_ids
        else [None]
    )

    filter_kw = _localization_fetch_kwargs(
        version_id=version_id, section_id=section_id, query=query
    )

    try:
        loc_count = 0
        for batch in media_id_batches:
            kw = dict(filter_kw)
            if batch:
                kw["media_id"] = batch
            loc_count += api.get_localization_count(project_id, **kw)
        return loc_count
    except Exception as e:
        logger.debug(f"_get_localization_count_from_api failed: {e}")
        return None


def fetch_project_media_ids(
    api_url: str,
    token: str,
    project_id: int,
    media_ids_filter: list[int] | None = None,
    version_id: int | None = None,
    section_id: int | None = None,
) -> list[int]:
    """
    Fetch all media in the project. Returns list of media ids.
    If media_ids_filter is set, only those media are returned (and must exist in the project).
    If version_id is set, filters media by that version via related_attribute.
    If section_id is set, filters media to that Tator section.
    """
    logger.info(
        f"fetch_project_media_ids: project_id={project_id} filter={media_ids_filter} "
        f"version_id={version_id} section_id={section_id}"
    )
    host = api_url.rstrip("/")
    api = tator.get_api(host, token)
    kwargs = _media_fetch_kwargs(version_id=version_id, section_id=section_id)
    if media_ids_filter:
        # Chunk filter to avoid "Request Line is too large" from nginx (e.g. 4094 bytes).
        chunk = _MAX_SAFE_MEDIA_ID_BATCH_SIZE
        media_list = []
        for i in range(0, len(media_ids_filter), chunk):
            kw = {**kwargs, "media_id": media_ids_filter[i : i + chunk]}
            media_list.extend(api.get_media_list(project_id, **kw))
        media_ids = [m.id for m in media_list]
    else:
        media_list = api.get_media_list(project_id, **kwargs)
        media_ids = [m.id for m in media_list]
    logger.info(f"Project {project_id} media count: {len(media_ids)}")
    return media_ids


def get_media_chunked(
    api: Any,
    project_id: int,
    media_ids: list[int],
    media_id_batch_size: int | None = None,
) -> list[Any]:
    """
    Get media objects in chunks. Uses get_media_list_by_id for reliable Media objects.
    Filters out non-Media responses (API quirk). Returns list of tator.models.Media.
    """
    chunk_size = (
        media_id_batch_size
        if media_id_batch_size is not None
        else _DEFAULT_MEDIA_ID_BATCH_SIZE
    )
    chunk_size = min(chunk_size, _MAX_SAFE_MEDIA_ID_BATCH_SIZE)
    logger.info(
        f"get_media_chunked: project_id={project_id} num_ids={len(media_ids)} chunk_size={chunk_size}"
    )
    if not media_ids:
        logger.info("get_media_chunked: no ids, returning []")
        return []
    all_media = []
    for start in range(0, len(media_ids), chunk_size):
        chunk_ids = media_ids[start : start + chunk_size]
        media = api.get_media_list_by_id(project_id, {"ids": chunk_ids})
        new_media = [m for m in media if isinstance(m, tator.models.Media)]
        all_media += new_media
        logger.info(
            f"get_media_chunked: start={start} chunk_len={len(new_media)} total_media={len(all_media)}"
        )
    logger.info(f"get_media_chunked: done, {len(all_media)} Media objects")
    logger.info(
        f"get_media_chunked: {len(media_ids)} ids -> {len(all_media)} Media objects"
    )
    return all_media


def _get_image_media_type_and_attr_names(
    api: Any, project_id: int
) -> tuple[int | None, list[str]]:
    """
    Get the Image media type id and its attribute names from the project.
    Image media will always have the name "Image". Returns (image_type_id, attr_names) or (None, []).
    """
    try:
        media_types = api.get_media_type_list(project_id)
    except Exception as e:
        logger.info(f"get_media_type_list failed: {e}")
        return (None, [])
    for mt in media_types or []:
        name = getattr(mt, "name", None) or (
            mt.get("name") if isinstance(mt, dict) else None
        )
        if name != "Image":
            continue
        type_id = getattr(mt, "id", None) or (
            mt.get("id") if isinstance(mt, dict) else None
        )
        attr_types = (
            getattr(mt, "attribute_types", None)
            or (mt.get("attribute_types") if isinstance(mt, dict) else None)
            or []
        )
        attr_names = []
        for at in attr_types:
            n = (
                getattr(at, "name", None)
                if not isinstance(at, dict)
                else at.get("name")
            )
            if n:
                attr_names.append(str(n))
        logger.info(f"Image media type id={type_id} attribute_names={attr_names}")
        return (type_id, attr_names)
    logger.info("No media type named 'Image' in project; skipping media attributes")
    return (None, [])


def _build_media_attributes_map(
    api: Any,
    project_id: int,
    localizations_path: str,
    media_id_batch_size: int | None = None,
) -> dict[int, dict[str, Any]]:
    """
    Build media_id -> {attr_name: value} for Image media only, using project Image type schema.
    Returns empty dict if no Image type or no media.
    """
    image_type_id, attr_names = _get_image_media_type_and_attr_names(api, project_id)
    if image_type_id is None or not attr_names:
        return {}
    _, media_ids = _localizations_jsonl_line_count_and_media_ids(localizations_path)
    if not media_ids:
        return {}
    all_media = get_media_chunked(
        api, project_id, media_ids, media_id_batch_size=media_id_batch_size
    )
    result: dict[int, dict[str, Any]] = {}
    for m in all_media:
        if getattr(m, "type", None) != image_type_id:
            continue
        mid = getattr(m, "id", None)
        if mid is None:
            continue
        attrs = getattr(m, "attributes", None) or {}
        result[mid] = {
            k: attrs[k] for k in attr_names if k in attrs and attrs[k] is not None
        }
    logger.info(f"Media attributes map: {len(result)} Image media with attributes")
    return result


# Name of the Image media attribute that marks a media as a classification sample.
CLASSIFICATION_LABEL_ATTR = "Label"


def is_classification_project(api: Any, project_id: int) -> bool:
    """
    True if the project's Image media type defines a "Label" attribute.

    A project may be BOTH classification and detection: when this attribute
    exists, labeled whole images are synced as classification samples (the media
    image is the crop) in addition to any localizations (which are cropped as
    usual). The two kinds of samples coexist, each identified by its elemental_id.
    """
    _, attr_names = _get_image_media_type_and_attr_names(api, project_id)
    return CLASSIFICATION_LABEL_ATTR in attr_names


def _media_to_classification_loc(m: Any) -> dict[str, Any] | None:
    """
    Build a synthetic full-frame localization for one Image media.

    The label comes from the media's "Label" attribute (copied verbatim into the
    synthetic localization's attributes). Using a full-frame box (x=0, y=0,
    width=1, height=1) and the media's own elemental_id lets the rest of the
    pipeline (manifest, dataset build, reconcile) treat the whole image as one
    classification sample distinct from any real localizations on the same media.
    The "_classification" flag routes cropping to a whole-image resize.
    """
    mid = getattr(m, "id", None)
    if mid is None:
        return None
    attrs = dict(getattr(m, "attributes", None) or {})
    eid = getattr(m, "elemental_id", None)
    eid = str(eid) if eid else f"m{mid}"
    return {
        "id": mid,
        "elemental_id": eid,
        "media": int(mid),
        "frame": None,
        "x": 0.0,
        "y": 0.0,
        "width": 1.0,
        "height": 1.0,
        "attributes": attrs,
        "type": getattr(m, "type", None),
        "version": getattr(m, "version", None),
        "modified_datetime": getattr(m, "modified_datetime", None),
        "created_datetime": getattr(m, "created_datetime", None),
        "_classification": True,
    }


def fetch_and_save_classification_localizations(
    api: Any,
    project_id: int,
    media_objects: list[Any],
    out_path: str | None = None,
    mode: str = "w",
    require_label: bool = True,
    version_id: int | None = None,
    section_id: int | None = None,
    query: str | None = None,
) -> int:
    """
    Write one synthetic full-frame localization per labeled Image media to JSONL.

    For classification samples the label is a media attribute rather than a
    localization. When require_label is True, only media whose "Label" attribute
    has a non-empty value produce a sample (so detection-only images are skipped).
    Use mode="a" to append these alongside detection localizations in the same
    file. Returns the number of classification localizations written.
    """
    if out_path is None:
        out_path = _localizations_jsonl_path(
            project_id, version_id, section_id=section_id, query=query
        )
    image_type_id, _ = _get_image_media_type_and_attr_names(api, project_id)
    logger.info(f"Classification localizations JSONL (mode={mode}): {out_path}")
    total = 0
    with open(out_path, mode) as f:
        for m in media_objects:
            if not isinstance(m, tator.models.Media):
                continue
            if image_type_id is not None and getattr(m, "type", None) != image_type_id:
                continue
            loc = _media_to_classification_loc(m)
            if loc is None:
                continue
            if require_label:
                label = (loc.get("attributes") or {}).get(CLASSIFICATION_LABEL_ATTR)
                if label is None or not str(label).strip():
                    continue
            try:
                f.write(json.dumps(loc, default=_json_serial) + "\n")
                total += 1
            except Exception as e:
                logger.info(f"Skip classification localization serialization: {e}")
    logger.info(f"Wrote {total} classification localizations -> {out_path}")
    return total


def _append_classification_localizations_to_jsonl(
    api: Any,
    *,
    project_id: int,
    api_url: str,
    token: str,
    localizations_path: str,
    media_id_batch_size: int,
    section_id: int | None = None,
) -> int:
    """
    Append synthetic full-frame localizations for labeled Image media to the JSONL.

    Detection localizations are written first (by _resolve_localizations_jsonl);
    this adds one whole-image classification sample per labeled Image media, so a
    project that is both classification and detection yields both kinds of
    samples, each identified by its own elemental_id. Media labels are not
    versioned, so all project (section-scoped) media are considered. Returns the
    number of classification localizations appended.
    """
    media_ids = fetch_project_media_ids(
        api_url, token, project_id, section_id=section_id
    )
    if not media_ids:
        return 0
    media_objects = get_media_chunked(
        api, project_id, media_ids, media_id_batch_size=media_id_batch_size
    )
    return fetch_and_save_classification_localizations(
        api,
        project_id,
        media_objects,
        out_path=localizations_path,
        mode="a",
        require_label=True,
    )


# Video extensions: skip download (not supported); downloads come directly from Tator for images only.
VIDEO_EXTENSIONS = (".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v")


def _is_video_name(name: str) -> bool:
    return any(name.lower().endswith(ext) for ext in VIDEO_EXTENSIONS)


def frame_to_timestamp(media: Any, frame: int) -> str:
    """Convert frame number to timestamp string for ffmpeg -ss (accurate frame indexing)."""
    fps = getattr(media, "fps", None)
    if fps is None or fps <= 0:
        fps = 1.0
    total_seconds = frame / fps
    total_microseconds = int(total_seconds * 1_000_000)
    return f"{total_microseconds}us"


def _ffprobe_dimensions(input_path_or_url: str | Path, _cache: dict[str, tuple[int, int]] | None = None) -> tuple[int, int] | None:
    """
    Return (width, height) for an image or video (local path or HTTP URL). Uses ffprobe.
    Returns None on failure. Results are cached per input string.
    """
    cache: dict[str, tuple[int, int]] = _cache if _cache is not None else {}
    key = str(input_path_or_url)
    if key in cache:
        return cache[key]
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            "-i", key,
        ]
        out = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if out.returncode != 0:
            logger.debug(f"ffprobe failed for {key}: {out.stderr}")
            return None
        line = (out.stdout or "").strip()
        if not line:
            return None
        parts = line.split(",")
        if len(parts) >= 2:
            w, h = int(parts[0]), int(parts[1])
            if w > 0 and h > 0:
                cache[key] = (w, h)
                return (w, h)
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError) as e:
        logger.debug(f"ffprobe error for {key}: {e}")
    return None


_DEFAULT_CROP_TIMEOUT = int(os.environ.get("CROP_TIMEOUT", "300"))
_DEFAULT_VIDEO_WORKERS = int(os.environ.get("CROP_VIDEO_WORKERS", "8"))
_FRAME_BATCH_SIZE = int(os.environ.get("CROP_FRAME_BATCH_SIZE", "20"))

_PIL_RESAMPLE = Image.LANCZOS


def _extract_video_frame(
    input_str: str,
    frame_index: int | None,
    media: Any,
    crop_timeout: int,
) -> Path | None:
    """Extract a single video frame to a temp PNG file using ffmpeg. Returns path or None."""
    use_ss = (
        frame_index is not None
        and media is not None
        and getattr(media, "fps", None) is not None
        and getattr(media, "fps", None) > 0
    )
    fd, tmp_path = tempfile.mkstemp(suffix=".png", prefix="frame_")
    os.close(fd)
    cmd = ["ffmpeg", "-y"]
    if use_ss:
        cmd.extend(["-ss", frame_to_timestamp(media, frame_index)])
    cmd.extend(["-i", input_str])
    if frame_index is not None and not use_ss:
        cmd.extend(["-vf", f"select=eq(n\\,{frame_index})"])
    cmd.extend(["-vframes", "1", "-update", "1", tmp_path])

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=crop_timeout,
        )
        if result.returncode != 0:
            logger.info(f"ffmpeg frame extract failed for {input_str}: {result.stderr[:500]}")
            _safe_unlink(tmp_path)
            return None
        if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) == 0:
            _safe_unlink(tmp_path)
            return None
        return Path(tmp_path)
    except subprocess.TimeoutExpired as e:
        logger.info(f"ffmpeg frame extract timeout ({crop_timeout}s) for {input_str}: {e}")
        _safe_unlink(tmp_path)
        return None
    except FileNotFoundError as e:
        logger.info(f"ffmpeg not found during frame extract for {input_str}: {e}")
        _safe_unlink(tmp_path)
        return None


def _extract_video_frames_batch(
    input_str: str,
    frame_indices: list[int],
    media: Any,
    crop_timeout: int,
    *,
    render_format: str = "png",
) -> dict[int, Path]:
    """
    Extract multiple frames in one ffmpeg invocation.

    Uses one input per frame (`-ss ... -i <input>`) and maps each input video stream
    to a single output image via `-map {idx}:v -frames:v 1`.
    """
    if not frame_indices:
        return {}

    frames = [int(f) for f in frame_indices]
    tmp_paths: dict[int, Path] = {}
    out_files: list[str] = []

    for frame_idx in frames:
        fd, tmp_path = tempfile.mkstemp(
            suffix=f".{render_format}", prefix=f"frame_{frame_idx}_"
        )
        os.close(fd)
        p = Path(tmp_path)
        tmp_paths[frame_idx] = p
        out_files.append(tmp_path)

    args: list[str] = ["ffmpeg", "-y"]

    # Inputs: one per frame, each with its own seek.
    for frame_idx in frames:
        args.extend(["-ss", frame_to_timestamp(media, frame_idx)])
        args.extend(["-i", input_str])

    # Outputs: map each input to exactly one frame.
    for batch_idx, _frame_idx in enumerate(frames):
        args.extend(["-map", f"{batch_idx}:v", "-frames:v", "1"])
        args.append(out_files[batch_idx])

    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=crop_timeout,
        )
        if result.returncode != 0:
            logger.info(
                f"ffmpeg batch extract failed for {input_str}: {(result.stderr or '')[:500]}"
            )
            for p in tmp_paths.values():
                _safe_unlink(p)
            return {}

        ok: dict[int, Path] = {}
        for frame_idx, p in tmp_paths.items():
            if p.exists() and p.stat().st_size > 0:
                ok[frame_idx] = p
            else:
                _safe_unlink(p)
        return ok
    except subprocess.TimeoutExpired as e:
        logger.info(
            f"ffmpeg batch extract timeout ({crop_timeout}s) for {input_str}: {e}"
        )
    except FileNotFoundError as e:
        logger.info(f"ffmpeg not found during batch extract for {input_str}: {e}")
    except Exception as e:
        logger.info(f"ffmpeg batch extract error for {input_str}: {e}")

    for p in tmp_paths.values():
        _safe_unlink(p)
    return {}


def _crop_output_exists(out_path: Path) -> bool:
    """True if crop file is already present and non-empty."""
    try:
        return out_path.is_file() and out_path.stat().st_size > 0
    except OSError:
        return False


def _video_frame_group_fully_cached(
    group: list[tuple[dict, Path]],
) -> bool:
    """True when every localization in this frame already has a crop on disk."""
    if not group:
        return True
    return all(_crop_output_exists(out_path) for _, out_path in group)


def _compute_square_coordinates(
    loc: dict[str, Any], image_width: int, image_height: int
) -> tuple[int, int, int, int] | None:
    """
    Compute in-frame square crop coordinates for a normalized localization box.

    Pads the shorter side to make a square, then shifts/clamps to keep the crop
    fully inside the frame to avoid out-of-frame black bars.
    """
    x = float(loc.get("x", 0))
    y = float(loc.get("y", 0))
    w = float(loc.get("width", 0))
    h = float(loc.get("height", 0))
    if w <= 0 or h <= 0 or image_width <= 0 or image_height <= 0:
        return None

    x1 = int(image_width * x)
    y1 = int(image_height * y)
    x2 = int(image_width * (x + w))
    y2 = int(image_height * (y + h))

    x1 = max(0, min(x1, image_width - 1))
    y1 = max(0, min(y1, image_height - 1))
    x2 = max(x1 + 1, min(x2, image_width))
    y2 = max(y1 + 1, min(y2, image_height))

    width = x2 - x1
    height = y2 - y1
    if width <= 0 or height <= 0:
        return None

    shorter_side = min(height, width)
    longer_side = max(height, width)
    delta = longer_side - shorter_side
    pad_before = delta // 2
    pad_after = delta - pad_before

    if width < height:
        x1 -= pad_before
        x2 += pad_after
    elif height < width:
        y1 -= pad_before
        y2 += pad_after

    if y1 < 0:
        shift = -y1
        y1 = 0
        y2 = min(image_height, y2 + shift)
    if y2 > image_height:
        shift = y2 - image_height
        y2 = image_height
        y1 = max(0, y1 - shift)

    if x1 < 0:
        shift = -x1
        x1 = 0
        x2 = min(image_width, x2 + shift)
    if x2 > image_width:
        shift = x2 - image_width
        x2 = image_width
        x1 = max(0, x1 - shift)

    crop_w = x2 - x1
    crop_h = y2 - y1
    if crop_w <= 0 or crop_h <= 0:
        return None

    # If frame bounds prevented perfect padding, enforce in-frame square by
    # shrinking the longer side to the shorter one around the crop center.
    if crop_w != crop_h:
        side = min(crop_w, crop_h)
        cx = (x1 + x2) // 2
        cy = (y1 + y2) // 2
        x1 = max(0, min(cx - (side // 2), image_width - side))
        y1 = max(0, min(cy - (side // 2), image_height - side))
        x2 = x1 + side
        y2 = y1 + side

    if x2 <= x1 or y2 <= y1:
        return None
    return (x1, y1, x2, y2)


def _crop_image_group(
    image_path: str | Path,
    locs_with_out_paths: list[tuple[dict, Path]],
    *,
    size: int,
) -> tuple[int, int]:
    """Crop a set of localizations from a single image file."""
    if not locs_with_out_paths:
        return (0, 0)
    try:
        img = Image.open(str(image_path))
        img.load()
        width, height = img.size

        total_ok = 0
        total_fail = 0
        for loc, out_path in locs_with_out_paths:
            if _crop_output_exists(out_path):
                total_ok += 1
                continue
            square_coords = _compute_square_coordinates(loc, width, height)
            if square_coords is None:
                total_fail += 1
                continue
            x1, y1, x2, y2 = square_coords

            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                crop = img.crop((x1, y1, x2, y2))
                crop = crop.resize((size, size), _PIL_RESAMPLE)
                crop.save(out_path, format="PNG")
                total_ok += 1
            except Exception as e:
                logger.debug(f"PIL crop failed for {out_path}: {e}")
                total_fail += 1

        img.close()
        return (total_ok, total_fail)

    except Exception as e:
        logger.info(f"Could not open source for cropping ({image_path}): {e}")
        return (0, len(locs_with_out_paths))


def _safe_unlink(path: str | Path) -> None:
    """Remove a file, ignoring errors if it doesn't exist."""
    try:
        os.unlink(path)
    except OSError:
        pass


def _pad_to_square(img: Image.Image) -> Image.Image:
    """
    Pad the shorter side of an image so it becomes square, centering the content.

    Keeps aspect ratio (no distortion) before a later resize to the target size,
    mirroring how localization crops are made square before resizing. Padding is
    black (zeros), the standard letterbox convention.
    """
    width, height = img.size
    if width == height:
        return img
    side = max(width, height)
    mode = img.mode if img.mode in ("RGB", "RGBA", "L") else "RGB"
    if img.mode != mode:
        img = img.convert(mode)
    fill = 0 if mode == "L" else (0, 0, 0) if mode == "RGB" else (0, 0, 0, 0)
    canvas = Image.new(mode, (side, side), fill)
    canvas.paste(img, ((side - width) // 2, (side - height) // 2))
    return canvas


def _resize_whole_image(
    img: Image.Image,
    locs_with_out_paths: list[tuple[dict, Path]],
    size: int,
) -> tuple[int, int]:
    """
    Save the whole image (padded to square, then resized) for each output path.

    Used for classification samples where the media image itself is the crop (no
    localization box). Padding to a square preserves aspect ratio before resizing
    to size x size, matching the square-then-resize behavior of localization crops.
    """
    total_ok = 0
    total_fail = 0
    squared = _pad_to_square(img)
    for _loc, out_path in locs_with_out_paths:
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            squared.resize((size, size), _PIL_RESAMPLE).save(out_path, format="PNG")
            total_ok += 1
        except Exception as e:
            logger.debug(f"PIL resize failed for {out_path}: {e}")
            total_fail += 1
    return (total_ok, total_fail)


def _crop_media_group(
    input_path_or_url: str | Path,
    locs_with_out_paths: list[tuple[dict, Path]],
    frame_index: int | None = None,
    size: int = 224,
    _dim_cache: dict[str, tuple[int, int]] | None = None,
    media: Any = None,
    crop_timeout: int = _DEFAULT_CROP_TIMEOUT,
) -> tuple[int, int]:
    """
    Crop multiple localizations from one image or one video frame using PIL.

    For images the source file is opened directly. For video frames a single
    frame is extracted via ffmpeg to a temp file, cropped with PIL, then the
    temp file is deleted. Localizations flagged with "_classification" (whole-image
    classification samples) are padded to a square and resized to size x size
    instead of cropping a box; only image media use that path. Returns (num_ok, num_fail).
    """
    if not locs_with_out_paths:
        return (0, 0)

    input_str = str(input_path_or_url)
    is_video = frame_index is not None
    frame_path: Path | None = None

    try:
        if is_video:
            frame_path = _extract_video_frame(input_str, frame_index, media, crop_timeout)
            if frame_path is None:
                return (0, len(locs_with_out_paths))
            img = Image.open(frame_path)
        else:
            img = Image.open(input_str)

        img.load()
        width, height = img.size

        # Split into whole-image classification samples and box crops. Classification
        # only applies to still images (video frames always crop the localization box).
        class_items: list[tuple[dict, Path]] = []
        box_items: list[tuple[dict, Path]] = []
        for loc, out_path in locs_with_out_paths:
            if not is_video and loc.get("_classification"):
                class_items.append((loc, out_path))
            else:
                box_items.append((loc, out_path))

        total_ok = 0
        total_fail = 0
        if class_items:
            ok, fail = _resize_whole_image(img, class_items, size)
            total_ok += ok
            total_fail += fail

        for loc, out_path in box_items:
            square_coords = _compute_square_coordinates(loc, width, height)
            if square_coords is None:
                total_fail += 1
                continue
            x1, y1, x2, y2 = square_coords

            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                crop = img.crop((x1, y1, x2, y2))
                crop = crop.resize((size, size), _PIL_RESAMPLE)
                crop.save(out_path, format="PNG")
                total_ok += 1
            except Exception as e:
                logger.debug(f"PIL crop failed for {out_path}: {e}")
                total_fail += 1

        img.close()
        return (total_ok, total_fail)

    except Exception as e:
        logger.info(f"Could not open source for cropping ({input_str}): {e}")
        return (0, len(locs_with_out_paths))
    finally:
        if frame_path is not None:
            _safe_unlink(frame_path)


def _crop_video_media_group(
    video_url: str,
    media: Any,
    frame_groups: list[tuple[int, list[tuple[dict, Path]]]],
    size: int,
    frame_workers: int,
    crop_timeout: int,
) -> tuple[int, int]:
    """
    Crop one video's localizations, grouped by frame.

    This keeps scheduling at media granularity (one task per video) while still
    reducing ffmpeg overhead by extracting multiple frames per ffmpeg invocation.
    """
    if not frame_groups:
        return (0, 0)
    ok_total = 0
    fail_total = 0

    pending_groups = [
        (fidx, grp)
        for fidx, grp in frame_groups
        if not _video_frame_group_fully_cached(grp)
    ]
    skipped_frames = len(frame_groups) - len(pending_groups)
    if skipped_frames:
        logger.info(
            "Skipping ffmpeg for %s frame(s): all crops already on disk",
            skipped_frames,
        )
    if not pending_groups:
        return (ok_total, fail_total)

    # One ffmpeg process per batch of frames, then crop extracted frame images in parallel.
    batch_size = max(1, _FRAME_BATCH_SIZE)
    for start in range(0, len(pending_groups), batch_size):
        batch = pending_groups[start : start + batch_size]
        frame_indices = [int(fidx) for fidx, _ in batch]
        extracted = _extract_video_frames_batch(
            video_url, frame_indices, media, crop_timeout
        )

        # Fan out crop work across CPUs for this batch
        crop_tasks: list[tuple[Path, list[tuple[dict, Path]]]] = []
        for frame_idx, group in batch:
            img_path = extracted.get(int(frame_idx))
            if img_path is None:
                fail_total += len(group)
                continue
            crop_tasks.append((img_path, group))

        if crop_tasks:
            workers = max(1, min(frame_workers or 1, len(crop_tasks)))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [
                    ex.submit(_crop_image_group, img_path, group, size=size)
                    for img_path, group in crop_tasks
                ]
                for fut in as_completed(futures):
                    try:
                        ok, fail = fut.result()
                        ok_total += ok
                        fail_total += fail
                    except Exception as e:
                        # count as 1 failed crop group when unexpected
                        fail_total += 1
                        logger.info(f"Video crop group error: {e}")

        for p in extracted.values():
            _safe_unlink(p)

    return (ok_total, fail_total)


def save_media_to_tmp(
    api: Any,
    project_id: int,
    media_objects: list[Any],
    media_ids_filter: set[int] | None = None,
) -> str:
    """
    Download each media to an isolated download directory.
    Existing non-empty files are skipped.
    When media_ids_filter is provided, only media whose id is in the set are downloaded.
    Retries each download up to 3 times. Returns the download directory path.
    """
    out_dir = _download_dir(project_id)
    valid = [m for m in media_objects if isinstance(m, tator.models.Media)]
    if media_ids_filter is not None:
        valid = [m for m in valid if m.id in media_ids_filter]
    total = len(valid)
    logger.info(f"Processing {total} media -> {out_dir}")
    downloaded = 0
    video_downloaded = 0
    cached_skipped = 0
    failed = 0
    log_interval = max(1, total // 10)
    for idx, m in enumerate(valid, 1):
        safe_name = f"{m.id}_{m.name}"
        out_path = os.path.join(out_dir, safe_name)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            cached_skipped += 1
            continue
        num_tries = 0
        success = False
        while num_tries < 3 and not success:
            try:
                for _ in tator.util.download_media(api, m, out_path):
                    pass
                success = True
                downloaded += 1
                if _is_video_name(m.name):
                    video_downloaded += 1
            except Exception as e:
                logger.debug(
                    f"Download attempt {num_tries + 1}/3 failed for {m.id}: {e}"
                )
                num_tries += 1
        if not success:
            failed += 1
            logger.warning(f"Could not download {m.name} after 3 tries")
        if idx % log_interval == 0 or idx == total:
            logger.info(
                f"Download progress: {idx}/{total} processed "
                f"({downloaded} saved, {video_downloaded} videos saved, {cached_skipped} already cached, {failed} failed)"
            )
    logger.info(
        f"Download complete: {downloaded} saved, {video_downloaded} videos saved, "
        f"{cached_skipped} already cached, {failed} failed -> {out_dir}"
    )
    return out_dir


def fetch_and_save_localizations(
    api: Any,
    project_id: int,
    version_id: int | None = None,
    media_ids: list[int] | None = None,
    localization_batch_size: int | None = None,
    media_id_batch_size: int | None = None,
    section_id: int | None = None,
    query: str | None = None,
) -> str:
    """
    Fetch all current localizations from Tator and write to a JSONL file.
    Overwrites the file (mode "w"), so the JSONL is always reconciled with Tator:
    removed localizations are absent, and the file is the single source of truth for the sync.
    Returns path to the file (e.g. .../localizations.jsonl).
    If media_ids is provided, only localizations for those media are fetched (required when syncing
    a subset of media; avoids empty results when project localizations are scoped to media).

    Batch sizes are from config (media_id_batch_size, localization_batch_size) or fallbacks to avoid
    414 Request-URI Too Large errors from nginx when the project has many media.
    """
    out_path = _localizations_jsonl_path(
        project_id, version_id, section_id=section_id, query=query
    )
    logger.info(f"Localizations JSONL will be saved to: {out_path}")
    loc_batch = (
        localization_batch_size
        if localization_batch_size is not None
        else _DEFAULT_LOCALIZATION_BATCH_SIZE
    )
    mid_batch = (
        media_id_batch_size
        if media_id_batch_size is not None
        else _DEFAULT_MEDIA_ID_BATCH_SIZE
    )
    effective_mid_batch = min(mid_batch, _MAX_SAFE_MEDIA_ID_BATCH_SIZE)
    if effective_mid_batch < mid_batch:
        logger.info(
            f"Media ID batch size capped to {effective_mid_batch} (request line limit)"
        )

    media_id_batches: list[list[int] | None] = (
        [
            media_ids[i : i + effective_mid_batch]
            for i in range(0, len(media_ids), effective_mid_batch)
        ]
        if media_ids
        else [None]
    )
    logger.info(
        f"Media ID batches: {len(media_id_batches)} batch(es) of up to {effective_mid_batch}"
    )

    filter_kw = _localization_fetch_kwargs(
        version_id=version_id, section_id=section_id, query=query
    )

    try:
        loc_count = 0
        for mid_batch in media_id_batches:
            kw = dict(filter_kw)
            if mid_batch:
                kw["media_id"] = mid_batch
            loc_count += api.get_localization_count(project_id, **kw)
        logger.info(
            f"get_localization_count(project_id={project_id}, media_ids={bool(media_ids)}, "
            f"version={version_id}, section_id={section_id}, query={'set' if (query or '').strip() else 'none'}) = {loc_count}"
        )
        if loc_count == 0 and version_id is not None:
            count_no_ver = 0
            for mid_batch in media_id_batches:
                kw: dict = {}
                if mid_batch:
                    kw["media_id"] = mid_batch
                count_no_ver += api.get_localization_count(project_id, **kw)
            if count_no_ver > 0:
                raise ValueError(
                    f"Version {version_id} has 0 localizations but {count_no_ver} exist across other versions; "
                    f"check that the correct version is specified"
                )
    except ValueError:
        raise
    except Exception as e:
        loc_count = None
        logger.exception(f"get_localization_count failed (will still try list): {e}")

    total = 0
    with open(out_path, "w") as f:

        def _fetch_all_locs() -> int:
            """Fetch localizations across all media_id batches, paginating each. Returns count."""
            fetched = 0
            for bidx, mid_batch in enumerate(media_id_batches):
                after_id = None
                while True:
                    kw = {"stop": loc_batch}
                    if mid_batch:
                        kw["media_id"] = mid_batch
                    kw.update(filter_kw)
                    if after_id is not None:
                        kw["after"] = after_id
                    try:
                        locs = api.get_localization_list(project_id, **kw)
                    except Exception as e:
                        logger.info(f"get_localization_list failed: {e}")
                        return fetched
                    if not locs:
                        logger.info(
                            f"Localizations batch empty (media_batch={bidx + 1}, after={after_id}), moving on"
                        )
                        break
                    for loc in locs:
                        try:
                            obj = loc.to_dict() if hasattr(loc, "to_dict") else loc
                            f.write(json.dumps(obj, default=_json_serial) + "\n")
                        except Exception as e:
                            logger.info(f"Skip localization serialization: {e}")
                    fetched += len(locs)
                    after_id = locs[-1].id if locs else None
                    logger.info(
                        f"Localizations batch: count={len(locs)} total_so_far={fetched} last_id={after_id}"
                    )
                    if len(locs) < loc_batch:
                        break
            return fetched

        total = _fetch_all_locs()

    logger.info(f"Fetched {total} localizations -> {out_path}")
    return out_path


def crop_localizations_parallel(
    download_dir: str,
    localizations_jsonl_path: str,
    crops_dir: str,
    size: int = 224,
    max_workers: int | None = None,
    locs_to_crop: list[dict] | None = None,
    media_objects: list[Any] | None = None,
) -> tuple[int, int]:
    """
    Crop localizations from their media in parallel using PIL.

    Frames are downloaded/extracted in batches of _FRAME_BATCH_SIZE to limit disk
    and memory usage. Video frames are extracted via ffmpeg to temp files, cropped
    with PIL, then the temp files are deleted. Image files are opened directly.

    Saves using elemental_id as filestem (e.g. elemental_id.png).
    Image: local files from download_dir, grouped by media_id.
    Video: local downloaded files from download_dir, grouped by (media_id, frame).

    When locs_to_crop is provided, only those localizations are cropped (cache-miss
    optimization). Otherwise falls back to reading all localizations from the JSONL.
    media_objects: Tator Media list for cache-miss media; used for stems/fps metadata.

    Returns (num_cropped, num_failed).
    """
    if not os.path.exists(download_dir) and not media_objects:
        logger.info("Download dir missing and no media_objects; skipping crops")
        return (0, 0)
    if locs_to_crop is None and not os.path.exists(localizations_jsonl_path):
        logger.info(
            "Localizations JSONL missing and no locs_to_crop provided; skipping crops"
        )
        return (0, 0)
    download_path = Path(download_dir)
    crops_path = Path(crops_dir)
    crops_path.mkdir(parents=True, exist_ok=True)

    # Image: media_id -> local path (from download dir)
    media_id_to_image_path: dict[int, Path] = {}
    if download_path.exists():
        for f in download_path.iterdir():
            if f.is_file() and f.suffix.lower() in (
                ".jpg",
                ".jpeg",
                ".png",
                ".webp",
                ".bmp",
            ):
                stem = f.stem
                if "_" in stem:
                    try:
                        mid = int(stem.split("_", 1)[0])
                        media_id_to_image_path[mid] = f
                    except ValueError:
                        pass

    # Video: media_id -> local file path, stem, Media from Tator metadata
    media_id_to_video_path: dict[int, str] = {}
    media_id_to_stem: dict[int, str] = {}
    media_id_to_media: dict[int, Any] = {}
    if download_path.exists():
        for f in download_path.iterdir():
            if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS:
                stem = f.stem
                if "_" in stem:
                    try:
                        mid = int(stem.split("_", 1)[0])
                        media_id_to_video_path[mid] = str(f)
                    except ValueError:
                        pass
    local_video_misses = 0
    for m in (media_objects or []):
        if not isinstance(m, tator.models.Media):
            continue
        mid = getattr(m, "id", None)
        if mid is None:
            continue
        media_id_to_media[mid] = m
        stem = f"{mid}_{getattr(m, 'name', '') or ''}"
        media_id_to_stem[mid] = stem
        name = getattr(m, "name", None) or ""
        if _is_video_name(name):
            local_video = media_id_to_video_path.get(mid)
            if local_video:
                media_id_to_stem[mid] = Path(local_video).stem
            else:
                local_video_misses += 1
        elif mid in media_id_to_image_path:
            media_id_to_stem[mid] = media_id_to_image_path[mid].stem

    if local_video_misses:
        logger.info(
            "Local video file missing for %s Media object(s); video crops may be skipped",
            local_video_misses,
        )

    if locs_to_crop is not None:
        loc_list = locs_to_crop
    else:
        loc_list = []
        with open(localizations_jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    loc_list.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    locs_by_media: dict[int, list[dict]] = {}
    for loc in loc_list:
        media_id = loc.get("media")
        if media_id is None:
            continue
        mid = int(media_id)
        if mid not in locs_by_media:
            locs_by_media[mid] = []
        locs_by_media[mid].append(loc)

    # Image tasks: (image_path, [(loc, out_path), ...]) one per image
    image_tasks: list[tuple[Path, list[tuple[dict, Path]]]] = []
    for mid, locs in locs_by_media.items():
        image_path = media_id_to_image_path.get(mid)
        if image_path is None or not image_path.exists():
            continue
        stem = media_id_to_stem.get(mid) or image_path.stem
        group: list[tuple[dict, Path]] = []
        for loc in locs:
            elemental_id = loc.get("elemental_id") or loc.get("id")
            if elemental_id is None:
                continue
            out_path = crops_path / stem / f"{elemental_id}.png"
            group.append((loc, out_path))
        if group:
            image_tasks.append((image_path, group))

    # Video tasks grouped by media: (video_path, media, [(frame_idx, [(loc, out_path), ...]), ...])
    video_tasks: list[tuple[str, Any, list[tuple[int, list[tuple[dict, Path]]]]]] = []
    skipped_video_media = 0
    for mid, locs in locs_by_media.items():
        if mid not in media_id_to_video_path:
            # Only log/track skips for media that look like video (frame present)
            if any(loc.get("frame") is not None for loc in locs):
                skipped_video_media += 1
            continue
        video_path = media_id_to_video_path[mid]
        media = media_id_to_media.get(mid)
        stem = media_id_to_stem.get(mid) or str(mid)
        by_frame: dict[int, list[tuple[dict, Path]]] = {}
        for loc in locs:
            frame = loc.get("frame")
            if frame is None:
                continue
            try:
                frame_idx = int(frame)
            except (TypeError, ValueError):
                continue
            elemental_id = loc.get("elemental_id") or loc.get("id")
            if elemental_id is None:
                continue
            out_path = crops_path / stem / f"{elemental_id}.png"
            if frame_idx not in by_frame:
                by_frame[frame_idx] = []
            by_frame[frame_idx].append((loc, out_path))
        frame_groups = [
            (frame_idx, group)
            for frame_idx, group in sorted(by_frame.items(), key=lambda kv: kv[0])
            if group
        ]
        if frame_groups:
            video_tasks.append((video_path, media, frame_groups))

    if skipped_video_media:
        logger.info(
            "Skipping video crops for %s media_id(s): no local downloaded video file found",
            skipped_video_media,
        )

    total_tasks = len(image_tasks) + len(video_tasks)
    if total_tasks == 0:
        logger.info("No localization crops to process")
        return (0, 0)
    total_locs = sum(len(g) for _, g in image_tasks) + sum(
        len(group) for _, _, frame_groups in video_tasks for _, group in frame_groups
    )
    total_video_frames = sum(len(frame_groups) for _, _, frame_groups in video_tasks)
    logger.info(f"Cropping {total_locs} localizations in {total_tasks} tasks (size={size}x{size})")
    if video_tasks:
        logger.info(
            "Video crop grouping: %s media task(s), %s frame group(s)",
            len(video_tasks),
            total_video_frames,
        )

    image_workers = max_workers or min(128, (os.cpu_count() or 4) * 2)
    video_workers = min(_DEFAULT_VIDEO_WORKERS, len(video_tasks)) if video_tasks else 0
    batch_size = _FRAME_BATCH_SIZE
    logger.info(
        f"Crop workers: {image_workers} image, {video_workers} video "
        f"(batch_size={batch_size}, timeout={_DEFAULT_CROP_TIMEOUT}s)"
    )

    num_ok = 0
    num_fail = 0

    def _collect(futures: list) -> tuple[int, int]:
        ok_total = 0
        fail_total = 0
        for fut in as_completed(futures):
            try:
                ok, fail = fut.result()
                ok_total += ok
                fail_total += fail
            except Exception as e:
                fail_total += 1
                logger.info(f"Crop task error: {e}")
        return ok_total, fail_total

    if image_tasks:
        for batch_start in range(0, len(image_tasks), batch_size):
            batch = image_tasks[batch_start : batch_start + batch_size]
            with ThreadPoolExecutor(max_workers=image_workers) as ex:
                futures = [
                    ex.submit(_crop_media_group, image_path, group, None, size)
                    for image_path, group in batch
                ]
                ok, fail = _collect(futures)
                num_ok += ok
                num_fail += fail
        logger.info(f"Image crops done: {num_ok} ok, {num_fail} failed")

    if video_tasks:
        for video_path, media, frame_groups in video_tasks:
            ok, fail = _crop_video_media_group(
                video_path,
                media,
                frame_groups,
                size,
                video_workers,
                _DEFAULT_CROP_TIMEOUT,
            )
            num_ok += ok
            num_fail += fail
        logger.info(f"Video crops done: {num_ok} ok, {num_fail} failed")

    logger.info(f"Crops done: {num_ok} saved to {crops_path}, {num_fail} failed")
    return (num_ok, num_fail)


def _load_config(path: str) -> dict[str, Any]:
    """Load configuration from YAML or JSON file."""
    ext = os.path.splitext(path)[1].lower()
    with open(path) as f:
        if ext in (".yaml", ".yml"):
            return yaml.safe_load(f) or {}
        return json.load(f)


def _load_localizations_index(jsonl_path: str) -> dict[str, dict]:
    """Load localizations JSONL and index by elemental_id (or id)."""
    index: dict[str, dict] = {}
    with open(jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                loc = json.loads(line)
            except json.JSONDecodeError:
                continue
            eid = loc.get("elemental_id") or loc.get("id")
            if eid is not None:
                index[str(eid)] = loc
    return index


def _crop_manifest_path(
    project_id: int,
    version_id: int | None,
    *,
    section_id: int | None = None,
    query: str | None = None,
) -> str:
    """Path to the crop manifest JSON for a project+version."""
    return os.path.join(
        _data_dir(project_id, version_id, section_id=section_id, query=query),
        "crop_manifest.json",
    )


def _load_crop_manifest(
    project_id: int,
    version_id: int | None,
    *,
    section_id: int | None = None,
    query: str | None = None,
) -> dict[str, dict]:
    """
    Load the crop manifest from disk.
    Returns {elemental_id: {"media_id": int, "media_stem": str}} or empty dict.
    """
    path = _crop_manifest_path(
        project_id, version_id, section_id=section_id, query=query
    )
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.info(f"Could not load crop manifest {path}: {e}")
        return {}


def _save_crop_manifest(
    project_id: int,
    version_id: int | None,
    manifest: dict[str, dict],
    *,
    section_id: int | None = None,
    query: str | None = None,
) -> None:
    """Atomically write the crop manifest to disk."""
    path = _crop_manifest_path(
        project_id, version_id, section_id=section_id, query=query
    )
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(manifest, f)
        os.replace(tmp_path, path)
    except OSError as e:
        logger.info(f"Could not save crop manifest {path}: {e}")


def _cleanup_download_dir(project_id: int) -> None:
    """Remove the download directory to reclaim disk space after crops are produced."""
    dl_dir = _download_dir(project_id)
    if os.path.isdir(dl_dir):
        try:
            shutil.rmtree(dl_dir)
            logger.info(f"Removed download directory: {dl_dir}")
        except OSError as e:
            logger.info(f"Could not remove download directory {dl_dir}: {e}")


def _cleanup_downloaded_videos(download_dir: str) -> None:
    """Remove downloaded video files to reclaim space as soon as crops are done."""
    if not download_dir or not os.path.isdir(download_dir):
        return
    removed = 0
    for f in Path(download_dir).iterdir():
        if not f.is_file():
            continue
        if f.suffix.lower() not in VIDEO_EXTENSIONS:
            continue
        try:
            f.unlink()
            removed += 1
        except OSError:
            pass
    if removed:
        logger.info(f"Removed {removed} downloaded video file(s) from {download_dir}")


def _ensure_s3_bucket_exists(bucket: str) -> None:
    """Create the S3 bucket if it does not exist. Idempotent."""
    import boto3
    from botocore.exceptions import ClientError

    region = os.environ.get("AWS_REGION") or os.environ.get(
        "AWS_DEFAULT_REGION", "us-east-1"
    )
    client = boto3.client("s3", region_name=region)

    try:
        client.head_bucket(Bucket=bucket)
        logger.debug(f"S3 bucket already exists: {bucket}")
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code not in ("404", "NoSuchBucket", "403"):
            raise

    try:
        if region == "us-east-1":
            # us-east-1 is the only region that must NOT have LocationConstraint
            client.create_bucket(Bucket=bucket)
        else:
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logger.info(f"S3 bucket created: {bucket} (region={region})")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            return
        raise


def _s3_crops_prefix(
    base_prefix: str | None, project_id: int, version_id: int | None
) -> str:
    """S3 key prefix for this project/version crops, e.g. fiftyone/raw/12/v21/crops."""
    base = (base_prefix or "").strip().rstrip("/")
    path = f"{project_id}/{_version_slug(version_id)}/crops"
    return f"{base}/{path}" if base else path


def _sync_local_dir_with_s3(
    local_dir: str, bucket: str, prefix: str | None = None
) -> None:
    """
    Two-way sync between a local directory and S3. Ensures the bucket exists.
    First pulls from S3 to local, then pushes from local to S3 (both via `aws s3 sync`
    when available, otherwise boto3 for upload and list_objects_v2 + download for pull).
    """
    prefix = (prefix or "").strip().rstrip("/")
    s3_uri = f"s3://{bucket}/{prefix}/" if prefix else f"s3://{bucket}/"

    _ensure_s3_bucket_exists(bucket)

    os.makedirs(local_dir, exist_ok=True)

    try:
        # 1. Pull: S3 -> local (so local has any files that exist only on S3)
        logger.info(f"Running aws s3 sync (pull): {s3_uri} -> {local_dir}...")
        subprocess.run(
            ["aws", "s3", "sync", s3_uri, local_dir, "--only-show-errors"],
            check=True,
            capture_output=True,
            timeout=3600,
        )
        logger.info(f"S3 pull completed: {s3_uri} -> {local_dir}")
        # 2. Push: local -> S3
        logger.info(f"Running aws s3 sync (push): {local_dir} -> {s3_uri}...")
        subprocess.run(
            ["aws", "s3", "sync", local_dir, s3_uri, "--only-show-errors"],
            check=True,
            capture_output=True,
            timeout=3600,
        )
        logger.info(f"S3 push completed: {local_dir} -> {s3_uri}")
    except FileNotFoundError:
        logger.info("aws CLI not found; using boto3 for two-way S3 sync")
        try:
            import boto3

            client = boto3.client("s3")
            # 1. Pull: list and download from S3 into local_dir
            paginator = client.get_paginator("list_objects_v2")
            prefix_slash = f"{prefix}/" if prefix else ""
            downloaded = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix_slash):
                for obj in page.get("Contents") or []:
                    key = obj["Key"]
                    if key.endswith("/"):
                        continue
                    rel = key[len(prefix_slash) :] if prefix_slash else key
                    local_path = os.path.join(local_dir, rel.replace("/", os.sep))
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    client.download_file(bucket, key, local_path)
                    downloaded += 1
            logger.info(
                f"S3 pull completed via boto3: {downloaded} file(s) {s3_uri} -> {local_dir}"
            )
            # 2. Push: upload local files to S3
            uploaded = 0
            for root, _dirs, files in os.walk(local_dir):
                for f in files:
                    local_path = os.path.join(root, f)
                    rel = os.path.relpath(local_path, local_dir)
                    key = f"{prefix}/{rel}" if prefix else rel
                    key = key.replace("\\", "/")
                    client.upload_file(local_path, bucket, key)
                    uploaded += 1
            logger.info(
                f"S3 push completed via boto3: {uploaded} file(s) {local_dir} -> {s3_uri}"
            )
        except Exception as e:
            logger.exception(f"S3 sync failed: {e}")
            raise
    except subprocess.CalledProcessError as e:
        logger.exception(f"aws s3 sync failed: {e}")
        raise


def _find_crop_cache_misses(
    localizations_jsonl_path: str,
    crops_dir: str,
    manifest: dict[str, dict],
    download_dir: str | None = None,
) -> tuple[set[int], list[dict], dict[str, dict]]:
    """
    Diff current localizations against the crop manifest and on-disk crop files.
    A localization is a "miss" (needs cropping) when:
      - its elemental_id is absent from the manifest, OR
      - its Tator modified_datetime differs from the manifest entry, OR
      - the crop file does not exist on disk.

    Returns:
        media_ids_needed: set of media IDs that must be downloaded (have >= 1 miss)
        locs_to_crop:     list of localization dicts that need cropping
        updated_manifest:  new manifest reflecting current localizations (to be saved after cropping)
    """
    download_stem_map = _media_id_to_stem(download_dir) if download_dir else {}

    manifest_stem_map: dict[int, str] = {}
    for entry in manifest.values():
        mid = entry.get("media_id")
        stem = entry.get("media_stem")
        if mid is not None and stem:
            manifest_stem_map[int(mid)] = stem

    crops_path = Path(crops_dir)

    media_ids_needed: set[int] = set()
    locs_to_crop: list[dict] = []
    updated_manifest: dict[str, dict] = {}

    with open(localizations_jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                loc = json.loads(line)
            except json.JSONDecodeError:
                continue
            eid = loc.get("elemental_id") or loc.get("id")
            if eid is None:
                continue
            eid = str(eid)
            media_id = loc.get("media")
            if media_id is None:
                continue
            mid = int(media_id)
            modified_at = loc.get("modified_datetime") or loc.get("created_datetime")

            media_stem = (
                manifest_stem_map.get(mid) or download_stem_map.get(mid) or f"{mid}"
            )

            updated_manifest[eid] = {
                "modified_at": modified_at,
                "media_id": mid,
                "media_stem": media_stem,
            }

            old_entry = manifest.get(eid)
            crop_file = crops_path / media_stem / f"{eid}.png"

            is_miss = (
                old_entry is None
                or old_entry.get("modified_at") != modified_at
                or not crop_file.exists()
            )
            if is_miss:
                media_ids_needed.add(mid)
                locs_to_crop.append(loc)

    total_locs = len(updated_manifest)
    hits = total_locs - len(locs_to_crop)
    logger.info(
        f"Crop cache: {total_locs} localizations, {hits} hits, "
        f"{len(locs_to_crop)} misses across {len(media_ids_needed)} media"
    )
    return media_ids_needed, locs_to_crop, updated_manifest


def _patch_manifest_stems(
    manifest: dict[str, dict],
    download_dir: str,
    media_objects: list[Any] | None = None,
) -> None:
    """
    After downloading new media, update manifest entries whose media_stem is
    still a bare media_id (fallback) with the real stem from the download directory
    or from media_objects (for video; no file in download dir).
    """
    real_stems = _media_id_to_stem(download_dir)
    media_stem_map: dict[int, str] = {}
    for m in (media_objects or []):
        if not isinstance(m, tator.models.Media):
            continue
        mid = getattr(m, "id", None)
        if mid is None:
            continue
        media_stem_map[mid] = f"{mid}_{getattr(m, 'name', '') or ''}"
    for entry in manifest.values():
        mid = entry.get("media_id")
        if mid is None:
            continue
        mid = int(mid)
        current_stem = entry.get("media_stem", "")
        if current_stem != str(mid):
            continue
        if real_stems and mid in real_stems:
            entry["media_stem"] = real_stems[mid]
        elif mid in media_stem_map:
            entry["media_stem"] = media_stem_map[mid]


def _cleanup_deleted_crops(
    manifest: dict[str, dict],
    updated_manifest: dict[str, dict],
    crops_dir: str,
) -> int:
    """
    Remove crop files for localizations that were deleted in Tator
    (present in old manifest but absent from updated_manifest).
    Returns count of files removed.
    """
    removed = 0
    deleted_eids = set(manifest.keys()) - set(updated_manifest.keys())
    for eid in deleted_eids:
        entry = manifest[eid]
        media_stem = entry.get("media_stem", str(entry.get("media_id", "")))
        crop_file = Path(crops_dir) / media_stem / f"{eid}.png"
        if crop_file.exists():
            try:
                crop_file.unlink()
                removed += 1
            except OSError:
                pass
    if removed:
        logger.info(
            f"Cleaned up {removed} orphaned crop files ({len(deleted_eids)} deleted localizations)"
        )
    return removed


def _resolve_localizations_jsonl(
    api: Any,
    *,
    project_id: int,
    version_id: int | None,
    api_url: str,
    token: str,
    force_sync: bool,
    media_id_batch_size: int,
    localization_batch_size: int,
    section_id: int | None = None,
    query: str | None = None,
) -> tuple[str, list[int], bool]:
    """
    Resolve localizations JSONL and media ids for crop work.

    Returns (localizations_path, media_ids_list, use_cached_jsonl).
    """
    jsonl_path = _localizations_jsonl_path(
        project_id, version_id, section_id=section_id, query=query
    )
    localizations_path = ""
    media_ids_list: list[int] = []
    use_cached_jsonl = False
    has_query = bool((query or "").strip())
    if not force_sync and _file_newer_than_days(jsonl_path, days=1.0):
        line_count, media_ids_from_jsonl = _localizations_jsonl_line_count_and_media_ids(
            jsonl_path
        )
        api_count = _get_localization_count_from_api(
            api,
            project_id,
            version_id,
            None if has_query else (media_ids_from_jsonl or None),
            media_id_batch_size,
            section_id=section_id,
            query=query,
        )
        if api_count is not None and line_count == api_count:
            use_cached_jsonl = True
            localizations_path = jsonl_path
            media_ids_list = media_ids_from_jsonl
            logger.info(
                "Bypassing media and localization fetch: JSONL is newer than 1 day and "
                "line count (%s) matches get_localization_count",
                line_count,
            )

    if not use_cached_jsonl:
        loc_media_ids: list[int] | None = None
        if not has_query:
            logger.info(
                "Fetching media IDs... host=%s project_id=%s api_url=%s",
                api_url.rstrip("/"),
                project_id,
                api_url,
            )
            media_ids_list = fetch_project_media_ids(
                api_url,
                token,
                project_id,
                version_id=version_id,
                section_id=section_id,
            )
            loc_media_ids = media_ids_list or None
        else:
            logger.info(
                "Skipping media pre-fetch: encoded_search query filters localizations directly"
            )
        logger.info("Fetching localizations...")
        localizations_path = fetch_and_save_localizations(
            api,
            project_id,
            version_id=version_id,
            media_ids=loc_media_ids,
            localization_batch_size=localization_batch_size,
            media_id_batch_size=media_id_batch_size,
            section_id=section_id,
            query=query,
        )
        if has_query and localizations_path:
            _, media_ids_list = _localizations_jsonl_line_count_and_media_ids(
                localizations_path
            )
    return (localizations_path, media_ids_list, use_cached_jsonl)


def _load_localizations_list_and_manifest(
    localizations_jsonl_path: str,
) -> tuple[list[dict], dict[str, dict], set[int]]:
    """Load JSONL localizations and build a fresh manifest map."""
    localizations: list[dict] = []
    updated_manifest: dict[str, dict] = {}
    media_ids: set[int] = set()
    with open(localizations_jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                loc = json.loads(line)
            except json.JSONDecodeError:
                continue
            eid = loc.get("elemental_id") or loc.get("id")
            media_id = loc.get("media")
            if eid is None or media_id is None:
                continue
            eid = str(eid)
            mid = int(media_id)
            modified_at = loc.get("modified_datetime") or loc.get("created_datetime")
            localizations.append(loc)
            media_ids.add(mid)
            updated_manifest[eid] = {
                "modified_at": modified_at,
                "media_id": mid,
                "media_stem": str(mid),
            }
    return (localizations, updated_manifest, media_ids)


def _delete_existing_crop_files(
    locs_to_crop: list[dict], updated_manifest: dict[str, dict], crops_dir: str
) -> int:
    """Delete existing crop files for localizations to force overwrite."""
    crops_path = Path(crops_dir)
    removed = 0
    for loc in locs_to_crop:
        eid = loc.get("elemental_id") or loc.get("id")
        if eid is None:
            continue
        entry = updated_manifest.get(str(eid)) or {}
        media_stem = entry.get("media_stem")
        if not media_stem:
            media_id = loc.get("media")
            if media_id is None:
                continue
            media_stem = str(int(media_id))
        crop_file = crops_path / str(media_stem) / f"{eid}.png"
        if crop_file.exists():
            try:
                crop_file.unlink()
                removed += 1
            except OSError:
                pass
    if removed:
        logger.info("Force crop recompute removed %s existing crop file(s)", removed)
    return removed


def _run_crop_pipeline(
    api: Any,
    *,
    project_id: int,
    version_id: int | None,
    api_url: str,
    token: str,
    force_sync: bool,
    force: bool,
    media_id_batch_size: int,
    localization_batch_size: int,
    s3_bucket: str | None = None,
    s3_crops_prefix: str | None = None,
    section_id: int | None = None,
    query: str | None = None,
) -> dict[str, Any]:
    """
    Run crop refresh pipeline and return counts/paths/context.

    This function is shared by full sync and crop-recompute jobs.
    """
    dl_dir = _download_dir(project_id)
    crops = _crops_dir(
        project_id, version_id, section_id=section_id, query=query
    )
    localizations_path = ""
    localizations_count = 0
    cache_misses = 0
    cache_hits = 0
    removed_existing = 0
    num_cropped = 0
    num_failed = 0
    used_cached_jsonl = False
    try:
        (
            localizations_path,
            media_ids_list,
            used_cached_jsonl,
        ) = _resolve_localizations_jsonl(
            api,
            project_id=project_id,
            version_id=version_id,
            api_url=api_url,
            token=token,
            force_sync=force_sync,
            media_id_batch_size=media_id_batch_size,
            localization_batch_size=localization_batch_size,
            section_id=section_id,
            query=query,
        )
        if localizations_path:
            logger.info("saved_localizations_path (JSONL): %s", localizations_path)

        # Classification-and-detection projects: append one whole-image
        # classification localization per labeled Image media alongside the
        # detection localizations already written above. Each is identified by
        # its own elemental_id; whole images are resized, boxes are cropped.
        if localizations_path and is_classification_project(api, project_id):
            added = _append_classification_localizations_to_jsonl(
                api,
                project_id=project_id,
                api_url=api_url,
                token=token,
                localizations_path=localizations_path,
                media_id_batch_size=media_id_batch_size,
                section_id=section_id,
            )
            if added:
                # The combined JSONL now differs from the detection-only file, so
                # the cached-JSONL fast path no longer applies; recompute media ids.
                used_cached_jsonl = False
                _, media_ids_list = _localizations_jsonl_line_count_and_media_ids(
                    localizations_path
                )
                logger.info(
                    "Classification project: appended %s whole-image sample(s); "
                    "total media ids now %s",
                    added,
                    len(media_ids_list),
                )
        old_manifest = _load_crop_manifest(
            project_id, version_id, section_id=section_id, query=query
        )
        if force:
            (
                all_locs,
                updated_manifest,
                media_ids_needed,
            ) = _load_localizations_list_and_manifest(localizations_path)
            locs_to_crop = all_locs
            _cleanup_deleted_crops(old_manifest, updated_manifest, crops)
            removed_existing = _delete_existing_crop_files(
                locs_to_crop, updated_manifest, crops
            )
            logger.info(
                "Force crop recompute enabled — scheduling all %s localizations",
                len(locs_to_crop),
            )
        else:
            media_ids_needed, locs_to_crop, updated_manifest = _find_crop_cache_misses(
                localizations_jsonl_path=localizations_path,
                crops_dir=crops,
                manifest=old_manifest,
                download_dir=dl_dir,
            )
            _cleanup_deleted_crops(old_manifest, updated_manifest, crops)

        localizations_count = len(updated_manifest)
        cache_misses = len(locs_to_crop)
        cache_hits = max(0, localizations_count - cache_misses)

        all_media: list[Any] = []
        if not media_ids_list:
            logger.info("No media IDs for project %s; skipping download", project_id)
        elif not media_ids_needed:
            logger.info(
                "All %s crops are cached; skipping media download", localizations_count
            )
        else:
            needed_ids = [mid for mid in media_ids_list if mid in media_ids_needed]
            logger.info(
                "Getting %s/%s media objects for cropping...",
                len(needed_ids),
                len(media_ids_list),
            )
            all_media = get_media_chunked(
                api, project_id, needed_ids, media_id_batch_size=media_id_batch_size
            )
            if not all_media:
                logger.info(
                    "No Media objects returned for %s ids; skipping download",
                    len(needed_ids),
                )
            else:
                logger.info(
                    "Saving %s media files to tmp (images + videos)...", len(all_media)
                )
                dl_dir = save_media_to_tmp(
                    api, project_id, all_media, media_ids_filter=media_ids_needed
                )

        if locs_to_crop and localizations_path:
            num_cropped, num_failed = crop_localizations_parallel(
                dl_dir,
                localizations_path,
                crops,
                size=224,
                locs_to_crop=locs_to_crop,
                media_objects=all_media,
            )
            _cleanup_downloaded_videos(dl_dir)
        elif not locs_to_crop:
            logger.info("No crop cache misses; skipping crop step")

        _patch_manifest_stems(updated_manifest, dl_dir, media_objects=all_media)
        _save_crop_manifest(
            project_id,
            version_id,
            updated_manifest,
            section_id=section_id,
            query=query,
        )

        if s3_bucket and os.path.isdir(crops):
            _sync_local_dir_with_s3(crops, s3_bucket, s3_crops_prefix)

        _cleanup_download_dir(project_id)
        return {
            "status": "ok",
            "saved_media_dir": dl_dir or None,
            "saved_localizations_path": localizations_path or None,
            "saved_crops_dir": crops or None,
            "localizations_count": localizations_count,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "num_cropped": num_cropped,
            "num_failed": num_failed,
            "removed_existing": removed_existing,
            "used_cached_jsonl": used_cached_jsonl,
        }
    except Exception as e:
        logger.error(f"Crop pipeline failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "saved_media_dir": dl_dir or None,
            "saved_localizations_path": localizations_path or None,
            "saved_crops_dir": crops or None,
            "localizations_count": localizations_count,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "num_cropped": num_cropped,
            "num_failed": num_failed,
            "removed_existing": removed_existing,
            "used_cached_jsonl": used_cached_jsonl,
        }


def _tator_localization_url(
    api_url: str,
    project_id: int,
    loc: dict,
    version_id: int | None = None,
) -> str | None:
    """
    Build Tator annotation UI URL that opens the media with this localization selected.
    Format: {base}/{project_id}/annotation/{media_id}?sort_by=$name&selected_entity={elemental_id}&selected_type=...&version=...&lock=0&fill_boxes=1&toggle_text=1
    Uses version_id if provided, else loc['version']. selected_type uses loc['type'] (id or name).
    Returns None if api_url, project_id, or media id is missing.
    """
    if not api_url or project_id is None:
        return None
    base = api_url.rstrip("/")
    vid = version_id if version_id is not None else loc.get("version")
    media_id = loc.get("media")
    if media_id is None or vid is None:
        return None
    path = f"{base}/{project_id}/annotation/{media_id}"
    elemental_id = loc.get("elemental_id") or loc.get("id")
    selected_type = loc.get("type")  # type id (int) or type name (str) if present
    if selected_type is not None:
        selected_type = str(selected_type)
    else:
        selected_type = ""
    params = {
        "sort_by": "$name",
        "selected_entity": elemental_id or "",
        "selected_type": selected_type,
        "version": str(vid),
        "lock": "0",
        "fill_boxes": "1",
        "toggle_text": "1",
    }
    query = urlencode(params, safe="")
    return f"{path}?{query}"


def _media_id_to_stem(download_dir: str) -> dict[int, str]:
    """Map media_id -> file stem for crop path resolution (e.g. 123 -> '123_image')."""
    out: dict[int, str] = {}
    if not download_dir or not os.path.exists(download_dir):
        return out
    for f in Path(download_dir).iterdir():
        if f.is_file() and f.suffix.lower() in (
            ".jpg",
            ".jpeg",
            ".png",
            ".webp",
            ".bmp",
        ):
            stem = f.stem
            if "_" in stem:
                try:
                    mid = int(stem.split("_", 1)[0])
                    out[mid] = stem
                except ValueError:
                    pass
    return out


def _media_id_to_stem_from_crops(crops_dir: str) -> dict[int, str]:
    """Fallback: derive media_id -> stem mapping from crops subdirectory names.

    Crops are stored as crops_dir/{media_stem}/{eid}.png where media_stem
    typically starts with the numeric media_id (e.g. '12345' or '12345_image').
    """
    out: dict[int, str] = {}
    if not crops_dir or not os.path.isdir(crops_dir):
        return out
    for d in Path(crops_dir).iterdir():
        if not d.is_dir():
            continue
        stem = d.name
        try:
            mid = int(stem.split("_", 1)[0])
            out[mid] = stem
        except ValueError:
            pass
    return out


def _crop_filepath_for_sample(
    media_stem: str,
    elemental_id: str,
    crops_dir: str,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
) -> str:
    """Return filepath for a crop sample: S3 URI when s3_bucket is set (enterprise/production), else local path."""
    if s3_bucket and str(s3_bucket).strip():
        bucket = s3_bucket.strip()
        prefix = (s3_prefix or "").strip().rstrip("/")
        if prefix:
            return f"s3://{bucket}/{prefix}/{media_stem}/{elemental_id}.png"
        return f"s3://{bucket}/{media_stem}/{elemental_id}.png"
    return os.path.abspath(os.path.join(crops_dir, media_stem, f"{elemental_id}.png"))


def _normalize_modified_at(val: Any) -> float | None:
    """Convert modified_at from loc or sample to a comparable float timestamp (or None)."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, datetime):
        return val.timestamp()
    if isinstance(val, date) and not isinstance(val, datetime):
        return datetime.combine(val, datetime.min.time()).timestamp()
    if isinstance(val, str):
        # Try datetime-like formats first to avoid ValueError from float(val) on "2026-03-10 00:58:37.574000"
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(
                    val.replace("Z", "+00:00")[:26], fmt
                ).timestamp()
            except (ValueError, TypeError):
                continue
        try:
            return float(val)
        except (TypeError, ValueError):
            pass
    return None


def _to_datetime(val: Any) -> datetime | None:
    """Convert to a datetime for storage."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(float(val))
    if isinstance(val, date) and not isinstance(val, datetime):
        return datetime.combine(val, datetime.min.time())
    if isinstance(val, str):
        # Try datetime-like formats first so "2026-03-10 00:58:37.574000" parses instead of raising from float(val)
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(val.replace("Z", "+00:00")[:26], fmt)
            except (ValueError, TypeError):
                continue
        try:
            return datetime.fromtimestamp(float(val))
        except (TypeError, ValueError):
            pass
    return None


def _get_tator_modified_at_datetime(sample: fo.Sample) -> tuple[datetime | None, bool]:
    """Get tator_modified_at from sample as datetime. If the stored value is not a valid
    datetime (e.g. string or number from MongoDB), convert via _to_datetime_modified_at
    and update the sample. Returns (datetime or None, True if sample was updated).
    """
    val = None
    if TATOR_MODIFIED_AT_FIELD in sample:
        val = sample[TATOR_MODIFIED_AT_FIELD]
    if val is None:
        val = getattr(sample, TATOR_MODIFIED_AT_FIELD, None)
    if val is None:
        return None, False
    if isinstance(val, datetime):
        return val, False
    dt = _to_datetime(val)
    if dt is not None:
        sample[TATOR_MODIFIED_AT_FIELD] = dt
        return dt, True
    return None, False


def _apply_loc_to_sample(
    sample: fo.Sample,
    loc: dict,
    *,
    api_url: str | None = None,
    project_id: int | None = None,
    version_id: int | None = None,
) -> None:
    """Update an existing sample's metadata from a localization (ground_truth, top1/top2_prediction, anomaly, primitives, annotation, tator_modified_at)."""
    label = _get_label_from_loc(loc)
    attrs = loc.get("attributes") or {}
    eid = loc.get("elemental_id") or loc.get("id")
    logger.debug("_apply_loc_to_sample eid=%s raw attrs keys=%s", eid, list(attrs.keys()))

    score = attrs.get("score")
    sample["ground_truth"] = fo.Classification(
        label=label,
        confidence=float(score) if score is not None else 1.0,
    )

    predicted_label = attrs.get("predicted_label") or label

    # top1_prediction: primary model prediction
    top1_kwargs = {"label": predicted_label}
    score_val = attrs.get("score")
    if score_val is not None:
        top1_kwargs["confidence"] = float(score_val)
    sample["top1_prediction"] = fo.Classification(**top1_kwargs)

    # top2_prediction: secondary/suggested label
    label_s = attrs.get("label_s")
    score_s = attrs.get("score_s")
    if label_s is not None or score_s is not None:
        top2_kwargs = {"label": str(label_s) if label_s is not None else ""}
        if score_s is not None:
            top2_kwargs["confidence"] = float(score_s)
        sample["top2_prediction"] = fo.Classification(**top2_kwargs)

    # Primitive sample-level attributes
    _PRIMITIVE_ATTR_MAP = (
        ("anomaly_score", "anomaly_score", float),
        ("noise_score", "noise_score", float),
        ("depth", "depth", float),
        ("altitude", "altitude", float),
        ("saliency", "saliency", int),
        ("area", "area", int),
        ("cluster", "cluster", str),
        ("comment", "comment", str),
        ("verified", "verified", bool),
        ("latitude", "latitude", float),
        ("longitude", "longitude", float)
    )
    applied = {}
    missing = []
    for source, target, cast in _PRIMITIVE_ATTR_MAP:
        val = attrs.get(source)
        if val is not None:
            sample[target] = cast(val)
            applied[target] = cast(val)
        else:
            missing.append(source)
    logger.debug(
        "_apply_loc_to_sample eid=%s top1_prediction.label=%s applied=%s missing=%s",
        eid, predicted_label, applied, missing,
    )
    if api_url and project_id is not None:
        tator_url = _tator_localization_url(api_url, project_id, loc, version_id)
        if tator_url:
            sample["annotation"] = tator_url
    modified_at = loc.get("modified_datetime") or loc.get("created_datetime")
    dt = _to_datetime(modified_at)
    if dt is not None:
        sample[TATOR_MODIFIED_AT_FIELD] = dt
    if loc.get("_classification"):
        sample["is_classification"] = True
        media_id = loc.get("media")
        if media_id is not None:
            sample["tator_media_id"] = int(media_id)


def _create_sample_from_loc(
    loc: dict,
    crops_dir: str,
    media_stem: str,
    include_classes: set[str],
    api_url: str | None = None,
    project_id: int | None = None,
    version_id: int | None = None,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
) -> fo.Sample | None:
    """Create a FiftyOne sample from a localization (for reconcile add-new)."""
    elemental_id = loc.get("elemental_id") or loc.get("id")
    if elemental_id is None:
        return None
    elemental_id = str(elemental_id)
    label = _get_label_from_loc(loc)
    if include_classes and label not in include_classes:
        return None
    filepath = _crop_filepath_for_sample(
        media_stem, elemental_id, crops_dir, s3_bucket=s3_bucket, s3_prefix=s3_prefix
    )
    if not (s3_bucket and s3_bucket.strip()) and not os.path.exists(filepath):
        return None
    sample = fo.Sample(filepath=filepath)
    sample["local_filepath"] = os.path.abspath(
        os.path.join(crops_dir, media_stem, f"{elemental_id}.png")
    )
    sample["elemental_id"] = elemental_id
    sample["media_stem"] = media_stem
    _apply_loc_to_sample(
        sample,
        loc,
        api_url=api_url,
        project_id=project_id,
        version_id=version_id,
    )
    return sample


def reconcile_dataset_with_tator(
    dataset: fo.Dataset,
    loc_index: dict[str, dict],
    crops_dir: str,
    download_dir: str | None,
    config: dict[str, Any],
    max_samples: int | None,
) -> fo.Dataset:
    """
    Reconcile existing dataset with current Tator localizations:
    - Remove samples whose elemental_id was deleted in Tator
    - Update samples whose modified_datetime changed (crop file already overwritten)
    - Add samples for new elemental_ids in Tator
    """
    tator_eids = set(loc_index.keys())
    include_classes = set(config.get("include_classes") or [])

    # Optimize media_id_to_stem creation - compute once and reuse
    media_id_to_stem = None
    if download_dir:
        media_id_to_stem = _media_id_to_stem(download_dir)
    if not media_id_to_stem:
        media_id_to_stem = _media_id_to_stem_from_crops(crops_dir)

    # 1. Remove samples deleted in Tator (only when we have a non-empty localization set from Tator)
    # values() with a single field returns a flat list; calling it twice and zipping avoids the
    # multi-field return format (which yields one list-per-field, not one tuple-per-sample).
    logger.info("Reconcile: Remove samples deleted in Tator")
    all_sample_ids = dataset.values("id", _enforce_natural_order=False)
    all_eids = dataset.values("elemental_id", _enforce_natural_order=False)
    to_remove: list[str] = []
    dataset_eids: set[str] = set()
    for sample_id, eid in zip(all_sample_ids, all_eids):
        if eid is not None:
            eid_str = str(eid)
            if eid_str in tator_eids:
                dataset_eids.add(eid_str)
            elif tator_eids:
                to_remove.append(sample_id)

    if to_remove:
        dataset.delete_samples(to_remove)
        logger.info(
            f"Reconcile: removed {len(to_remove)} samples (deleted in Tator)"
        )
    elif tator_eids:
        logger.info("Reconcile: no samples to remove (all present in Tator)")
    else:
        logger.info(
            "Reconcile: 0 localizations from Tator; skipping delete step (keeping existing samples)"
        )

    # 2. Update samples with changed modified_datetime (crop already overwritten by crop_localizations_parallel)
    logger.info("Reconcile: Update samples with changed modified_datetime")
    updated = 0

    # Pre-collect all samples that need checking to avoid multiple passes
    # Create a dict mapping elemental_id to sample for faster lookups
    eid_to_sample = {}
    samples_to_update = []

    for sample in dataset.iter_samples(autosave=False):
        elemental_id = getattr(sample, "elemental_id", None)
        if elemental_id and str(elemental_id) in loc_index:
            eid_to_sample[str(elemental_id)] = sample

    api_url = config.get("api_url")
    project_id = config.get("project_id")
    version_id = config.get("version_id")
    force_sync = bool(config.get("force_sync"))
    if force_sync:
        logger.info("Reconcile: force_sync enabled — rewriting all samples")

    samples_to_fix_storage: list[fo.Sample] = []
    for eid, sample in eid_to_sample.items():
        loc = loc_index[eid]

        if force_sync:
            samples_to_update.append((sample, loc))
            updated += 1
            continue

        modified_at = _to_datetime(
            loc.get("modified_datetime") or loc.get("created_datetime")
        )
        tator_modified_at, was_fixed = _get_tator_modified_at_datetime(sample)
        if was_fixed:
            samples_to_fix_storage.append(sample)
        mod_ts = _normalize_modified_at(modified_at)
        last_ts = _normalize_modified_at(tator_modified_at)

        has_prediction = sample.has_field("top1_prediction") and sample["top1_prediction"] is not None
        logger.debug(
            f"Checking sample {sample.id} for update: {eid} modified_at: {modified_at} {TATOR_MODIFIED_AT_FIELD}: {tator_modified_at} has_prediction: {has_prediction}"
        )
        needs_update = (mod_ts is not None and mod_ts != last_ts) or not has_prediction
        if needs_update:
            reason = "timestamp_changed" if (mod_ts is not None and mod_ts != last_ts) else "missing_prediction"
            logger.debug(
                f"Sample {sample.id} needs update ({reason}): {eid}"
            )
            samples_to_update.append((sample, loc))
            updated += 1

    # Persist samples whose tator_modified_at was normalized from non-datetime
    for sample in samples_to_fix_storage:
        sample.save()

    # Apply current localization data to changed samples and save
    if samples_to_update:
        batch_size = 1000
        for i in range(0, len(samples_to_update), batch_size):
            batch = samples_to_update[i : i + batch_size]
            for sample, loc in batch:
                _apply_loc_to_sample(
                    sample,
                    loc,
                    api_url=api_url,
                    project_id=project_id,
                    version_id=version_id,
                )
                sample.save()

        logger.info(f"Reconcile: updated {updated} samples (box changed)")

    # 3. Add new samples (elemental_id in Tator but not in dataset)
    # dataset_eids was built in step 1 via per-field values() calls
    new_eids = tator_eids - dataset_eids if tator_eids else set()

    # Apply max_samples limit
    if max_samples and new_eids:
        cap = max_samples - len(dataset)
        if cap <= 0:
            new_eids = set()
        else:
            # Convert to list for slicing, but only take what we need
            new_eids_list = list(new_eids)[:cap]
            new_eids = set(new_eids_list)

    if new_eids:
        # Batch create samples for better performance
        added = 0
        batch_size = 100  # Adjust based on your needs
        samples_to_add = []

        # Pre-filter valid media_ids to avoid repeated checks
        valid_media_ids = set()
        for eid in new_eids:
            loc = loc_index[eid]
            media_id = loc.get("media")
            if media_id and int(media_id) in media_id_to_stem:
                valid_media_ids.add(int(media_id))

        for eid in new_eids:
            loc = loc_index[eid]
            media_id = loc.get("media")
            if media_id is None:
                continue

            media_stem = media_id_to_stem.get(int(media_id))
            if not media_stem:
                continue

            api_url = config.get("api_url")
            project_id = config.get("project_id")
            version_id = config.get("version_id")
            s3_bucket = config.get("s3_bucket")
            s3_prefix = config.get("s3_prefix")

            sample = _create_sample_from_loc(
                loc,
                crops_dir,
                media_stem,
                include_classes,
                api_url=api_url,
                project_id=project_id,
                version_id=version_id,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
            )

            if sample:
                samples_to_add.append(sample)
                added += 1

                # Add in batches to avoid memory issues with large datasets
                if len(samples_to_add) >= batch_size:
                    dataset.add_samples(samples_to_add)
                    samples_to_add = []

        # Add any remaining samples
        if samples_to_add:
            dataset.add_samples(samples_to_add)

        if added:
            logger.info(f"Reconcile: added {added} new samples")

    return dataset


def _get_label_from_loc(loc: dict) -> str:
    """Extract label from localization attributes (Label, label) or fallback to Unknown."""
    attrs = loc.get("attributes") or {}
    label = attrs.get("Label") or attrs.get("label")
    if label is not None and str(label).strip():
        return str(label)
    return "Unknown"


def build_fiftyone_dataset_from_crops(
    crops_dir: str,
    localizations_jsonl_path: str,
    dataset_name: str,
    config: dict[str, Any] | None = None,
    download_dir: str | None = None,
) -> Any:
    """
    Build a FiftyOne dataset from crop images and localizations JSONL.

    Crops layout: crops/{media_file_stem}/{elemental_id}.png
    JSONL: one JSON per line with elemental_id, media, x, y, width, height, attributes, etc.

    Config keys (optional):
        include_classes: list of labels to include (None = all)
        image_extensions: glob patterns (default: ["*.png", "*.jpg", ...])
        max_samples: max samples to load (None = no limit)

    Returns the FiftyOne dataset.
    """
    config = config or {}
    include_classes = set(config.get("include_classes") or [])
    image_extensions = config.get("image_extensions") or [
        "*.png",
        "*.jpg",
        "*.jpeg",
        "*.bmp",
        "*.tiff",
    ]
    max_samples = config.get("max_samples")
    force_sync = bool(config.get("force_sync"))
    dataset_already_exists = dataset_name in fo.list_datasets()

    # Load localizations index by elemental_id
    loc_index = _load_localizations_index(localizations_jsonl_path)
    logger.info(f"Loaded {len(loc_index)} localizations from JSONL")

    # Collect crop filepaths
    samples: list = []
    seen = 0
    s3_bucket = config.get("s3_bucket")
    s3_prefix = config.get("s3_prefix")
    for ext in image_extensions:
        pat = os.path.join(crops_dir, "**", ext)
        for filepath in glob.glob(pat):
            seen += 1
            if max_samples and len(samples) >= max_samples:
                break
            rel = os.path.relpath(filepath, crops_dir)
            parts = Path(rel).parts
            if len(parts) < 2:
                continue
            media_stem = parts[0]
            elemental_id = Path(filepath).stem

            loc = loc_index.get(elemental_id)
            label = _get_label_from_loc(loc) if loc else (media_stem or "Unknown")

            if include_classes and label not in include_classes:
                continue

            sample_filepath = _crop_filepath_for_sample(
                media_stem,
                elemental_id,
                crops_dir,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
            )
            sample = fo.Sample(filepath=sample_filepath)
            sample["local_filepath"] = filepath
            sample["elemental_id"] = elemental_id
            sample["media_stem"] = media_stem
            media_attrs_map = config.get("media_attributes_map") or {}
            if loc:
                media_id = loc.get("media")
                if media_id is not None:
                    media_attrs = media_attrs_map.get(int(media_id)) or {}
                    for k, v in media_attrs.items():
                        if v is not None:
                            sample[k] = v
                if not dataset_already_exists or force_sync:
                    _apply_loc_to_sample(
                        sample,
                        loc,
                        api_url=config.get("api_url"),
                        project_id=config.get("project_id"),
                        version_id=config.get("version_id"),
                    )
            else:
                sample["ground_truth"] = fo.Classification(label=label, confidence=1.0)
            samples.append(sample)
        if max_samples and len(samples) >= max_samples:
            break

    if not samples:
        raise ValueError(f"No crops found in {crops_dir} (checked {seen} files)")

    logger.info(f"Collected {len(samples)} samples for dataset")

    # Handle existing dataset: always reconcile, never delete
    if dataset_already_exists:
        logger.info(f"Reconcile: loading dataset {dataset_name}...")
        dataset = fo.load_dataset(dataset_name)
        dataset.persistent = (
            True  # Ensure dataset persists in MongoDB after session ends
        )
        dataset = reconcile_dataset_with_tator(
            dataset=dataset,
            loc_index=loc_index,
            crops_dir=crops_dir,
            download_dir=download_dir,
            config=config,
            max_samples=max_samples,
        )
        _ensure_field_indexes(dataset)
        logger.info(f"Reconcile: dataset {dataset_name} loaded")
        return dataset

    logger.info(
        f"Reconcile: creating new dataset {dataset_name} in database {fo.config.database_name}"
    )
    dataset = fo.Dataset(dataset_name)
    dataset.persistent = True  # Persist dataset in MongoDB after session ends
    dataset.add_samples(samples)
    _ensure_field_indexes(dataset)
    logger.info(f"Created dataset '{dataset_name}' with {len(samples)} samples")
    return dataset


def _ensure_field_indexes(dataset: fo.Dataset) -> None:
    """Create MongoDB indexes on classification and primitive fields for faster queries."""
    for field_path in (
        "elemental_id",  # Used by reconcile (values aggregation) and sync-to-tator
        "ground_truth.label",
        "ground_truth.confidence",
        "top1_prediction.label",
        "top1_prediction.confidence",
        "top2_prediction.label",
        "top2_prediction.confidence",
        "anomaly_score",
        "depth",
        "altitude",
        "saliency",
        "area",
        "cluster",
        "comment",
        "verified",
    ):
        try:
            dataset.create_index(field_path)
        except Exception:
            pass


DEFAULT_LABEL_ATTR = "Label"
DEFAULT_SCORE_ATTR = "score"


def _sanitize_dataset_name(name: str) -> str:
    """Make a string safe for use as a FiftyOne/MongoDB dataset name."""
    if not name:
        return "default"
    # Replace anything that isn't alphanumeric, underscore, or hyphen with underscore
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(name).strip())
    return re.sub(r"_+", "_", s).strip("_") or "default"


def _default_dataset_name(api: Any, project_id: int, version_id: int | None) -> str:
    """FiftyOne dataset name (base): project_name + '_v' + version_id. Port is appended by _dataset_name_with_port."""
    try:
        project = api.get_project(project_id)
        project_name = (
            _sanitize_dataset_name(project.name)
            if project.name
            else f"project_{project_id}"
        )
    except Exception:
        project_name = f"project_{project_id}"
    if version_id is not None:
        version_part = f"v{version_id}"
    else:
        version_part = "default"
    return f"{project_name}_{version_part}"


def _dataset_name_with_port(dataset_name: str, port: int) -> str:
    """Append port to dataset name if not already present (e.g. project_v66 -> project_v66_5151)."""
    name = (dataset_name or "").strip()
    if not name:
        return name
    suffix = f"_{port}"
    return name if name.endswith(suffix) else f"{name}{suffix}"


def _normalize_elemental_id_str(elemental_id: Any) -> str:
    return str(elemental_id)


def _chunk_list(items: list[Any], size: int):
    if size <= 0:
        raise ValueError("chunk size must be positive")
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _localization_type_id(loc: Any) -> int:
    """Return localization type pk for bulk PATCH grouping (Tator requires one type per bulk update)."""
    t = getattr(loc, "type", None)
    if t is None:
        raise ValueError("localization missing type")
    if isinstance(t, int):
        return t
    tid = getattr(t, "id", None)
    if tid is not None:
        return int(tid)
    raise ValueError(f"Cannot resolve localization type id from {type(t)!r}")


def _attrs_group_key(attrs: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted(attrs.items(), key=lambda kv: kv[0]))


def _fetch_localizations_by_elemental_ids(
    api: Any,
    project_id: int,
    version_id: int,
    elemental_ids: list[str],
    *,
    chunk_size: int | None = None,
) -> dict[str, Any]:
    """Resolve elemental_ids to localization objects via LocalizationList PUT (by-id query body)."""
    out: dict[str, Any] = {}
    if not elemental_ids:
        return out
    effective_chunk = chunk_size if chunk_size is not None else _sync_to_tator_fetch_chunk()
    unique = list(dict.fromkeys(elemental_ids))
    total_chunks = max(1, (len(unique) + effective_chunk - 1) // effective_chunk)
    logger.info(
        "sync_edits_to_tator: fetch_localizations chunks=%s chunk_size=%s unique_ids=%s",
        total_chunks,
        effective_chunk,
        len(unique),
    )
    for i, chunk in enumerate(_chunk_list(unique, effective_chunk), start=1):
        locs = api.get_localization_list_by_id(
            project_id,
            localization_id_query={"elemental_ids": chunk},
            version=[version_id],
        )
        locs_list = list(locs) if not isinstance(locs, list) else locs
        for loc in locs_list:
            eid = getattr(loc, "elemental_id", None)
            if eid is None:
                continue
            out[str(eid)] = loc
        resolved_in_chunk = {str(e) for e in chunk if str(e) in out}
        unresolved_in_chunk = [str(e) for e in chunk if str(e) not in resolved_in_chunk]
        if unresolved_in_chunk:
            logger.info(
                "sync_edits_to_tator: fetch_localizations chunk %s/%s "
                "unresolved_elemental_ids=%s",
                i,
                total_chunks,
                unresolved_in_chunk,
            )
        logger.info(
            "sync_edits_to_tator: fetch_localizations chunk %s/%s size=%s resolved_total=%s",
            i,
            total_chunks,
            len(chunk),
            len(out),
        )
    return out


def _fetch_media_by_elemental_ids(
    api: Any,
    project_id: int,
    elemental_ids: list[str],
    *,
    chunk_size: int | None = None,
) -> dict[str, Any]:
    """Resolve elemental_ids to media objects (classification samples use media elemental_id)."""
    out: dict[str, Any] = {}
    if not elemental_ids:
        return out
    effective_chunk = chunk_size if chunk_size is not None else _sync_to_tator_fetch_chunk()
    unique = list(dict.fromkeys(elemental_ids))
    total_chunks = max(1, (len(unique) + effective_chunk - 1) // effective_chunk)
    logger.info(
        "sync_edits_to_tator: fetch_media chunks=%s chunk_size=%s unique_ids=%s",
        total_chunks,
        effective_chunk,
        len(unique),
    )
    for i, chunk in enumerate(_chunk_list(unique, effective_chunk), start=1):
        media_list: list[Any] = []
        try:
            result = api.get_media_list_by_id(
                project_id, media_id_query={"elemental_ids": chunk}
            )
            media_list = list(result) if not isinstance(result, list) else result
        except Exception as e:
            logger.info(
                "sync_edits_to_tator: fetch_media batch failed (%s); "
                "falling back to per-elemental_id get_media_list",
                e,
            )
            for eid in chunk:
                try:
                    one = api.get_media_list(project_id, elemental_id=eid)
                    media_list.extend(one or [])
                except Exception as e2:
                    logger.info(
                        "sync_edits_to_tator: fetch_media elemental_id=%s failed: %s",
                        eid,
                        e2,
                    )
        for media in media_list:
            eid = getattr(media, "elemental_id", None)
            if eid is None:
                continue
            out[str(eid)] = media
        unresolved_in_chunk = [str(e) for e in chunk if str(e) not in out]
        if unresolved_in_chunk:
            logger.info(
                "sync_edits_to_tator: fetch_media chunk %s/%s unresolved_elemental_ids=%s",
                i,
                total_chunks,
                unresolved_in_chunk,
            )
        logger.info(
            "sync_edits_to_tator: fetch_media chunk %s/%s size=%s resolved_total=%s",
            i,
            total_chunks,
            len(chunk),
            len(out),
        )
    return out


def _bulk_patch_media_by_elemental_id(
    api: Any,
    project_id: int,
    elemental_id_to_attrs: dict[str, dict[str, Any]],
    media_by_eid: dict[str, Any],
    *,
    chunk_size: int | None = None,
    inter_chunk_delay_seconds: float | None = None,
) -> None:
    """
    Apply attribute updates to Image media via PATCH /rest/Medias/{project}.
    Groups by attribute payload and chunks by media id.
    """
    groups: dict[tuple[tuple[str, Any], ...], list[int]] = defaultdict(list)
    missing: list[str] = []
    for eid, attrs in elemental_id_to_attrs.items():
        media = media_by_eid.get(eid)
        if media is None:
            missing.append(eid)
            continue
        mid = getattr(media, "id", None)
        if mid is None:
            missing.append(eid)
            continue
        groups[_attrs_group_key(attrs)].append(int(mid))

    if missing:
        preview = ", ".join(missing[:10])
        suffix = "..." if len(missing) > 10 else ""
        raise ValueError(f"No media found for elemental_id(s): {preview}{suffix}")

    effective_chunk = chunk_size if chunk_size is not None else _sync_to_tator_patch_chunk()
    effective_delay = (
        inter_chunk_delay_seconds
        if inter_chunk_delay_seconds is not None
        else _sync_to_tator_chunk_delay_seconds()
    )
    total_chunks = sum(
        max(1, (len(ids) + effective_chunk - 1) // effective_chunk)
        for ids in groups.values()
    )
    logger.info(
        "sync_edits_to_tator: bulk media PATCH groups=%s total_chunks=%s chunk_size=%s",
        len(groups),
        total_chunks,
        effective_chunk,
    )

    sent_chunks = 0
    for group_idx, (key, media_ids) in enumerate(groups.items(), start=1):
        attrs = dict(key)
        chunks = list(_chunk_list(media_ids, effective_chunk))
        for i, chunk in enumerate(chunks):
            api.update_media_list(
                project_id,
                media_bulk_update={"attributes": attrs, "ids": chunk},
            )
            sent_chunks += 1
            logger.info(
                "sync_edits_to_tator: bulk media PATCH chunk %s/%s "
                "(group %s/%s, size=%s)",
                sent_chunks,
                total_chunks,
                group_idx,
                len(groups),
                len(chunk),
            )
            if effective_delay > 0 and (i + 1) < len(chunks):
                time.sleep(effective_delay)


def _bulk_patch_localizations_by_elemental_id(
    api: Any,
    project_id: int,
    version_id: int,
    elemental_id_to_attrs: dict[str, dict[str, Any]],
    loc_by_eid: dict[str, Any],
    *,
    chunk_size: int | None = None,
    inter_chunk_delay_seconds: float | None = None,
) -> None:
    """
    Apply attribute updates using PATCH /rest/Localizations/{project} (bulk update).
    Groups by (localization type id, attribute payload) so each request satisfies Tator's
    single-type requirement for bulk patch. Adds a small inter-chunk sleep (configurable
    via FIFTYONE_SYNC_TO_TATOR_CHUNK_DELAY_MS) to smooth write bursts against Tator.
    """
    groups: dict[tuple[int, tuple[tuple[str, Any], ...]], list[str]] = defaultdict(list)
    missing: list[str] = []
    for eid, attrs in elemental_id_to_attrs.items():
        loc = loc_by_eid.get(eid)
        if loc is None:
            missing.append(eid)
            continue
        tid = _localization_type_id(loc)
        groups[(tid, _attrs_group_key(attrs))].append(eid)

    if missing:
        preview = ", ".join(missing[:10])
        suffix = "..." if len(missing) > 10 else ""
        raise ValueError(f"No localization found for elemental_id(s): {preview}{suffix}")

    effective_chunk = chunk_size if chunk_size is not None else _sync_to_tator_patch_chunk()
    effective_delay = (
        inter_chunk_delay_seconds
        if inter_chunk_delay_seconds is not None
        else _sync_to_tator_chunk_delay_seconds()
    )
    total_chunks = sum(
        max(1, (len(eids) + effective_chunk - 1) // effective_chunk)
        for _g, eids in groups.items()
    )
    logger.info(
        "sync_edits_to_tator: bulk PATCH groups=%s total_chunks=%s chunk_size=%s "
        "inter_chunk_delay_ms=%s",
        len(groups),
        total_chunks,
        effective_chunk,
        int(effective_delay * 1000),
    )

    sent_chunks = 0
    for group_idx, ((_tid, key), eids) in enumerate(groups.items(), start=1):
        attrs = dict(key)
        chunks = list(_chunk_list(eids, effective_chunk))
        for i, chunk in enumerate(chunks):
            api.update_localization_list(
                project_id,
                localization_bulk_update={
                    "attributes": attrs,
                    "in_place": 1,
                    "elemental_ids": chunk,
                },
                version=[version_id],
            )
            sent_chunks += 1
            logger.info(
                "sync_edits_to_tator: bulk PATCH chunk %s/%s "
                "(group %s/%s, type=%s, size=%s)",
                sent_chunks,
                total_chunks,
                group_idx,
                len(groups),
                _tid,
                len(chunk),
            )
            if effective_delay > 0 and (i + 1) < len(chunks):
                time.sleep(effective_delay)


def sync_edits_to_tator(
    project_id: int,
    version_id: int,
    port: int,
    api_url: str,
    token: str,
    dataset_name: str | None = None,
    label_attr: str | None = DEFAULT_LABEL_ATTR,
    score_attr: str | None = DEFAULT_SCORE_ATTR,
    debug: bool = False,
    project_name: str | None = None,
    force_sync: bool = False,
) -> dict[str, Any]:
    """
    Push FiftyOne dataset edits (labels, confidence) back to Tator localizations.
    Matches samples by elemental_id. Fetches localizations in batches (PUT by elemental_ids),
    then updates via bulk PATCH (update_localization_list) grouped by localization type.
    When force_sync=False (default), only samples whose last_modified_at is more than
    a few seconds after created_at are pushed, and tator_modified_at is set to last_modified_at.
    When force_sync=True, all samples with attrs are pushed regardless of timestamps.
    Returns {"status": "ok", "updated": int, "failed": int, "errors": list} or raises.
    """
    db_entry = get_database_entry_or_enterprise_default(
        project_id, port, project_name=project_name
    )
    if db_entry is None:
        raise ValueError(
            f"No database entry found for project_id={project_id} and port={port}"
        )
    db_name = database_name_from_uri(db_entry.uri)
    if not get_is_enterprise():
        fo.config.database_uri = db_entry.uri
        fo.config.database_name = db_name
        os.environ["FIFTYONE_DATABASE_URI"] = fo.config.database_uri
        os.environ["FIFTYONE_DATABASE_NAME"] = fo.config.database_name

    if get_is_enterprise():
        _test_fiftyone_connection()
    else:
        _test_mongodb_connection(db_entry.uri)

    host = api_url.rstrip("/")
    api = tator.get_api(host, token)
    ds_name = dataset_name or _default_dataset_name(api, project_id, version_id)

    # Resolve dataset by project name + port (datasets may have been created
    # with a version component or port suffix that differs from _default_dataset_name).
    try:
        _proj = api.get_project(project_id)
        project_prefix = (
            _sanitize_dataset_name(_proj.name)
            if _proj.name
            else f"project_{project_id}"
        )
    except Exception:
        project_prefix = f"project_{project_id}"
    port_suffix = f"_{port}"
    version_part = f"_v{version_id}"

    def _resolve_dataset(requested: str, allow_project_fallback: bool) -> str | None:
        """Return dataset name from exact/port matches, then project+port fallback."""
        available = fo.list_datasets()
        if requested in available:
            return requested
        # Default name has no port; stored name is base + port_suffix (e.g. project_v66_5151)
        if (requested + port_suffix) in available:
            return requested + port_suffix
        if not allow_project_fallback:
            return None
        matches = [
            d
            for d in available
            if d.startswith(project_prefix) and d.endswith(port_suffix)
        ]
        if not matches:
            return None
        if len(matches) == 1:
            return matches[0]
        version_matches = [d for d in matches if version_part in d]
        if len(version_matches) == 1:
            return version_matches[0]
        if version_matches:
            matches = version_matches
        for candidate in matches:
            if candidate == f"{project_prefix}{port_suffix}":
                return candidate
        return matches[0]

    fallback_db = f"{os.environ.get('FIFTYONE_DATABASE_DEFAULT', 'fiftyone_project')}_{project_id}"
    allow_project_fallback = dataset_name is None
    resolved = _resolve_dataset(ds_name, allow_project_fallback=allow_project_fallback)
    if resolved is None and db_name != fallback_db:
        if not get_is_enterprise():
            fo.config.database_name = fallback_db
            os.environ["FIFTYONE_DATABASE_NAME"] = fallback_db
        resolved = _resolve_dataset(
            ds_name, allow_project_fallback=allow_project_fallback
        )
    if resolved is None:
        if not get_is_enterprise():
            fo.config.database_name = db_name
        raise ValueError(
            f"No dataset matching project '{project_prefix}' with port {port} found in database '{db_name}' (or '{fallback_db}'). "
            "Run POST /sync first. Ensure FIFTYONE_DATABASE_URI and FIFTYONE_DATABASE_NAME match the sync process."
        )
    ds_name = resolved

    push_lock_key = get_sync_to_tator_lock_key(db_name, project_id, version_id)
    logger.info(
        f"Acquiring sync-to-tator lock: key={push_lock_key} "
        f"(db={db_name}, project_id={project_id}, version_id={version_id})"
    )
    if not try_acquire_sync_lock(push_lock_key):
        logger.warning(
            f"Failed to acquire sync-to-tator lock: key={push_lock_key} - "
            "another push is in progress"
        )
        return {
            "status": "busy",
            "message": (
                "Another sync-to-tator push is already running for this version. "
                "Please try again in a few minutes."
            ),
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }

    try:
        return _do_sync_edits_to_tator(
            api=api,
            project_id=project_id,
            version_id=version_id,
            ds_name=ds_name,
            label_attr=label_attr,
            score_attr=score_attr,
            debug=debug,
            force_sync=force_sync,
        )
    finally:
        logger.info(f"Releasing sync-to-tator lock: key={push_lock_key}")
        try:
            release_sync_lock(push_lock_key)
        except Exception as e:  # pragma: no cover - best-effort release
            logger.warning(f"Failed to release sync-to-tator lock {push_lock_key}: {e}")


def _do_sync_edits_to_tator(
    *,
    api: Any,
    project_id: int,
    version_id: int,
    ds_name: str,
    label_attr: str | None,
    score_attr: str | None,
    debug: bool,
    force_sync: bool,
) -> dict[str, Any]:
    """Inner push routine; the public wrapper handles DB selection and locking."""
    logger.info(f"sync_edits_to_tator: loading dataset {ds_name!r}")
    dataset = fo.load_dataset(ds_name)
    try:
        total_samples = len(dataset)
    except Exception:  # pragma: no cover - len() shouldn't fail for normal datasets
        total_samples = -1
    logger.info(
        f"sync_edits_to_tator: dataset {ds_name!r} loaded; "
        f"scanning {total_samples if total_samples >= 0 else '?'} samples "
        f"(force_sync={force_sync})"
    )

    updated = 0
    failed = 0
    skipped = 0
    errors: list[str] = []
    _debug = debug or os.environ.get("FIFTYONE_SYNC_DEBUG", "").lower() in (
        "1",
        "true",
        "yes",
    )

    pending: list[tuple[Any, str, dict[str, Any], Any]] = []
    scan_progress_every = _env_int("FIFTYONE_SYNC_TO_TATOR_SCAN_LOG_EVERY", 5000)
    scanned = 0

    for sample in dataset.iter_samples(autosave=False):
        scanned += 1
        if scan_progress_every > 0 and scanned % scan_progress_every == 0:
            logger.info(
                "sync_edits_to_tator: scan progress %s/%s pending=%s skipped=%s failed=%s",
                scanned,
                total_samples if total_samples >= 0 else "?",
                len(pending),
                skipped,
                failed,
            )
        elemental_id = sample["elemental_id"] if "elemental_id" in sample else None
        if not elemental_id:
            failed += 1
            errors.append(f"Sample {sample.id}: missing elemental_id")
            continue
        eid_str = _normalize_elemental_id_str(elemental_id)
        gt = sample["ground_truth"] if "ground_truth" in sample else None
        label = gt.label if gt else None
        confidence = sample["confidence"] if "confidence" in sample else None
        if confidence is None and gt:
            confidence = getattr(gt, "confidence", None)
        attrs: dict[str, Any] = {}
        if label is not None and label_attr:
            attrs[label_attr] = str(label)
        if confidence is not None and score_attr:
            attrs[score_attr] = float(confidence)
        if "verified" in sample:
            verified_val = sample["verified"]
            if verified_val is not None:
                attrs["verified"] = bool(verified_val)
        if not attrs:
            continue

        # By default, only push samples modified more than 5 seconds after creation;
        # force_sync=True bypasses this check and updates all samples.
        last_modified_at = getattr(sample, "last_modified_at", None)
        if not force_sync:
            created_at_fo = getattr(sample, "created_at", None)
            mod_ts = _normalize_modified_at(last_modified_at)
            created_ts = _normalize_modified_at(created_at_fo)
            allow_push = (
                mod_ts is not None
                and created_ts is not None
                and (mod_ts - created_ts) > 5
            )
            if not allow_push:
                skipped += 1
                if _debug:
                    logger.info(
                        f"SKIP elem={elemental_id} last_modified_at={last_modified_at} "
                        f"created_at={created_at_fo} (need >5s diff)"
                    )
                continue

        pending.append((sample, eid_str, attrs, last_modified_at))
        if _debug:
            logger.info(
                "sync_edits_to_tator: PENDING elem=%s label=%s attrs=%s "
                "is_classification=%s tator_media_id=%s",
                eid_str,
                label,
                attrs,
                bool(
                    sample["is_classification"]
                    if "is_classification" in sample
                    else False
                ),
                sample["tator_media_id"] if "tator_media_id" in sample else None,
            )

    logger.info(
        "sync_edits_to_tator: scan complete scanned=%s pending=%s skipped=%s failed=%s",
        scanned,
        len(pending),
        skipped,
        failed,
    )

    if pending:
        updates: dict[str, dict[str, Any]] = {}
        for _sample, eid_str, attrs, _lm in pending:
            updates[eid_str] = attrs

        logger.info(
            "sync_edits_to_tator: resolving %s elemental_ids -> localizations",
            len(updates),
        )
        loc_by_eid = _fetch_localizations_by_elemental_ids(
            api, project_id, version_id, list(updates.keys())
        )
        unresolved_for_loc = sorted(
            eid for eid in updates if eid not in loc_by_eid
        )
        if unresolved_for_loc:
            logger.info(
                "sync_edits_to_tator: %s elemental_id(s) not found as localizations; "
                "will try media lookup: %s",
                len(unresolved_for_loc),
                unresolved_for_loc,
            )

        media_by_eid: dict[str, Any] = {}
        if unresolved_for_loc:
            media_by_eid = _fetch_media_by_elemental_ids(
                api, project_id, unresolved_for_loc
            )
            for eid in unresolved_for_loc:
                if eid in media_by_eid:
                    logger.info(
                        "sync_edits_to_tator: elemental_id=%s resolved as media "
                        "(classification sample)",
                        eid,
                    )

        loc_updates = {eid: attrs for eid, attrs in updates.items() if eid in loc_by_eid}
        media_updates = {
            eid: attrs for eid, attrs in updates.items() if eid in media_by_eid
        }
        unresolved = sorted(
            eid
            for eid in updates
            if eid not in loc_by_eid and eid not in media_by_eid
        )
        if unresolved:
            logger.warning(
                "sync_edits_to_tator: %s elemental_id(s) unresolved (neither "
                "localization nor media): %s",
                len(unresolved),
                unresolved,
            )
            for eid in unresolved:
                failed += 1
                errors.append(
                    f"No localization or media found for elemental_id={eid}"
                )

        logger.info(
            "sync_edits_to_tator: bulk push samples=%s distinct_elemental_ids=%s "
            "resolved_localizations=%s resolved_media=%s unresolved=%s",
            len(pending),
            len(updates),
            len(loc_by_eid),
            len(media_by_eid),
            len(unresolved),
        )

        successful_eids: set[str] = set()
        if loc_updates:
            try:
                _bulk_patch_localizations_by_elemental_id(
                    api, project_id, version_id, loc_updates, loc_by_eid
                )
            except Exception as e:
                failed += len(loc_updates)
                errors.append(f"Localization batch update to Tator failed: {e}")
                logger.error(
                    "sync_edits_to_tator: localization PATCH failed for %s "
                    "elemental_id(s): %s",
                    len(loc_updates),
                    sorted(loc_updates.keys()),
                )
            else:
                successful_eids.update(loc_updates.keys())

        if media_updates:
            try:
                _bulk_patch_media_by_elemental_id(
                    api, project_id, media_updates, media_by_eid
                )
            except Exception as e:
                failed += len(media_updates)
                errors.append(f"Media batch update to Tator failed: {e}")
                logger.error(
                    "sync_edits_to_tator: media PATCH failed for %s elemental_id(s): %s",
                    len(media_updates),
                    sorted(media_updates.keys()),
                )
            else:
                successful_eids.update(media_updates.keys())

        if successful_eids:
            logger.info(
                "sync_edits_to_tator: writing tator_modified_at to %s samples",
                len(successful_eids),
            )
            for sample, eid_str, _attrs, last_modified_at in pending:
                if eid_str not in successful_eids:
                    continue
                try:
                    sample[TATOR_MODIFIED_AT_FIELD] = last_modified_at
                    sample.save()
                    updated += 1
                except Exception as e:
                    failed += 1
                    errors.append(f"Sample {sample.id}: {e}")

    logger.info(
        f"sync_edits_to_tator: updated={updated} skipped={skipped} failed={failed}"
    )
    return {
        "status": "ok",
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "errors": errors[:20],
    }


def run_sync_job(
    project_id: int,
    version_id: int | None,
    api_url: str,
    token: str,
    port: int,
    project_name: str,
    database_uri: str | None = None,
    database_name: str | None = None,
    force_sync: bool = False,
    vss_project_key: str | None = None,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
    section_id: int | None = None,
    query: str | None = None,
) -> dict[str, Any]:
    """
    Entrypoint for RQ worker: all args are serializable. Calls sync_project_to_fiftyone.
    When run in RQ worker context, attaches a handler that writes log lines to job.meta for the applet.
    """
    from src.app.database_manager import register_project_id_name

    logger.info(
        f"run_sync_job received project_id={project_id} version_id={version_id} "
        f"section_id={section_id} query={'set' if (query or '').strip() else 'none'}"
    )

    job_meta_handler: logging.Handler | None = None
    try:
        from rq import get_current_job

        job = get_current_job()
        if job is not None:
            job_meta_handler = _JobMetaLogHandler(job)
            job_meta_handler.setLevel(logging.DEBUG)
            logger.addHandler(job_meta_handler)
    except Exception:  # rq not installed or no worker context
        pass

    try:
        register_project_id_name(project_id, project_name)
        return sync_project_to_fiftyone(
            project_id=project_id,
            version_id=version_id,
            api_url=api_url,
            token=token,
            port=port,
            project_name=project_name,
            database_uri=database_uri,
            database_name=database_name,
            force_sync=force_sync,
            vss_project_key=vss_project_key,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            section_id=section_id,
            query=query,
        )
    finally:
        if job_meta_handler is not None:
            try:
                logger.removeHandler(job_meta_handler)
                job_meta_handler.close()
            except Exception:
                pass


def recompute_crops_for_version(
    project_id: int,
    version_id: int,
    api_url: str,
    token: str,
    port: int,
    project_name: str | None = None,
    force: bool = False,
    force_sync: bool = False,
    vss_project_key: str | None = None,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
    database_name: str | None = None,
) -> dict[str, Any]:
    """
    Recompute crops for a project/version without building the dataset.

    Uses the same crop pipeline as full sync; force=True recomputes all crops by
    bypassing cache-hit checks and deleting existing crop files before recropping.
    """
    if not (s3_bucket and s3_bucket.strip()):
        s3_cfg = get_s3_config(
            project_id, project_name=project_name, vss_project_key=vss_project_key
        )
        if s3_cfg:
            s3_bucket = s3_cfg.get("s3_bucket") or None
            s3_prefix = s3_cfg.get("s3_prefix") or s3_prefix
    if s3_bucket:
        s3_bucket = s3_bucket.strip()
        s3_prefix = (s3_prefix or "").strip() or None
    s3_crops_prefix = (
        _s3_crops_prefix(s3_prefix, project_id, version_id) if s3_bucket else None
    )
    resolved_db = (
        database_name.strip() if database_name and database_name.strip() else None
    ) or get_database_name(project_id, port, project_name=project_name)
    lock_key = get_sync_lock_key(resolved_db, project_id, version_id)
    logger.info(
        "Acquiring crop-recompute lock: key=%s project_id=%s version_id=%s force=%s force_sync=%s",
        lock_key,
        project_id,
        version_id,
        force,
        force_sync,
    )
    if not try_acquire_sync_lock(lock_key):
        return {
            "status": "busy",
            "message": "This dataset is being updated by another sync. Please try again in a few minutes.",
            "database_name": resolved_db,
        }

    config_path = os.getenv("FIFTYONE_SYNC_CONFIG_PATH")
    config: dict[str, Any] = {}
    if config_path and os.path.exists(config_path):
        try:
            config = _load_config(config_path)
        except Exception as e:
            logger.info(f"Failed to load config {config_path}: {e}")
    media_id_batch_size = (
        config.get("media_id_batch_size") or _DEFAULT_MEDIA_ID_BATCH_SIZE
    )
    localization_batch_size = (
        config.get("localization_batch_size") or _DEFAULT_LOCALIZATION_BATCH_SIZE
    )

    try:
        api = tator.get_api(api_url.rstrip("/"), token)
        crop_result = _run_crop_pipeline(
            api,
            project_id=project_id,
            version_id=version_id,
            api_url=api_url,
            token=token,
            force_sync=force_sync,
            force=force,
            media_id_batch_size=media_id_batch_size,
            localization_batch_size=localization_batch_size,
            s3_bucket=s3_bucket,
            s3_crops_prefix=s3_crops_prefix,
        )
        crop_result["database_name"] = resolved_db
        crop_result["force"] = force
        crop_result["force_sync"] = force_sync
        crop_result["port"] = port
        return crop_result
    finally:
        logger.info(f"Releasing crop-recompute lock: key={lock_key}")
        release_sync_lock(lock_key)


def run_recompute_crops_job(
    project_id: int,
    version_id: int,
    api_url: str,
    token: str,
    port: int,
    project_name: str,
    force: bool = False,
    force_sync: bool = False,
    vss_project_key: str | None = None,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
    database_name: str | None = None,
) -> dict[str, Any]:
    """RQ entrypoint for queued crop recompute jobs."""
    from src.app.database_manager import register_project_id_name

    logger.info(
        "run_recompute_crops_job received project_id=%s version_id=%s port=%s force=%s force_sync=%s",
        project_id,
        version_id,
        port,
        force,
        force_sync,
    )
    job_meta_handler: logging.Handler | None = None
    try:
        from rq import get_current_job

        job = get_current_job()
        if job is not None:
            job_meta_handler = _JobMetaLogHandler(job)
            job_meta_handler.setLevel(logging.DEBUG)
            logger.addHandler(job_meta_handler)
    except Exception:  # rq not installed or no worker context
        pass

    try:
        register_project_id_name(project_id, project_name)
        return recompute_crops_for_version(
            project_id=project_id,
            version_id=version_id,
            api_url=api_url,
            token=token,
            port=port,
            project_name=project_name,
            force=force,
            force_sync=force_sync,
            vss_project_key=vss_project_key,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            database_name=database_name,
        )
    finally:
        if job_meta_handler is not None:
            try:
                logger.removeHandler(job_meta_handler)
                job_meta_handler.close()
            except Exception:
                pass


def run_sync_to_tator_job(
    project_id: int,
    version_id: int,
    api_url: str,
    token: str,
    port: int,
    project_name: str,
    dataset_name: str | None = None,
    label_attr: str = "Label",
    score_attr: str | None = None,
    debug: bool = False,
    force_sync: bool = False,
) -> dict[str, Any]:
    """
    Entrypoint for RQ worker: push FiftyOne edits back to Tator.

    Running in the worker keeps long bulk PATCH loops off the HTTP event loop so the
    FastAPI service stays responsive even when pushing thousands of localizations.
    Attaches a job.meta log handler so the launcher applet can poll for progress.
    """
    from src.app.database_manager import register_project_id_name

    logger.info(
        "run_sync_to_tator_job received project_id=%s version_id=%s port=%s "
        "dataset_name=%r force_sync=%s",
        project_id,
        version_id,
        port,
        dataset_name,
        force_sync,
    )

    job_meta_handler: logging.Handler | None = None
    try:
        from rq import get_current_job

        job = get_current_job()
        if job is not None:
            job_meta_handler = _JobMetaLogHandler(job)
            job_meta_handler.setLevel(logging.DEBUG)
            logger.addHandler(job_meta_handler)
    except Exception:  # rq not installed or no worker context
        pass

    try:
        register_project_id_name(project_id, project_name)
        return sync_edits_to_tator(
            project_id=project_id,
            version_id=version_id,
            port=port,
            api_url=api_url,
            token=token,
            dataset_name=dataset_name,
            label_attr=label_attr,
            score_attr=score_attr,
            debug=debug,
            project_name=project_name,
            force_sync=force_sync,
        )
    finally:
        if job_meta_handler is not None:
            try:
                logger.removeHandler(job_meta_handler)
                job_meta_handler.close()
            except Exception:
                pass


def run_dimreduce_job(
    project_id: int,
    version_id: int,
    api_url: str,
    token: str,
    port: int,
    project_name: str | None,
    method: str,
    num_dims: int = 2,
    force: bool = True,
) -> dict[str, Any]:
    """
    Entrypoint for RQ worker: recompute dimensionality reduction from existing embeddings.

    When run in RQ worker context, attaches a handler that writes log lines to job.meta
    for the applet progress display.
    """
    from src.app.database_manager import register_project_id_name

    logger.info(
        "run_dimreduce_job received project_id=%s version_id=%s method=%r num_dims=%s",
        project_id,
        version_id,
        method,
        num_dims,
    )

    job_meta_handler: logging.Handler | None = None
    try:
        from rq import get_current_job

        job = get_current_job()
        if job is not None:
            job_meta_handler = _JobMetaLogHandler(job)
            job_meta_handler.setLevel(logging.DEBUG)
            logger.addHandler(job_meta_handler)
    except Exception:  # rq not installed or no worker context
        pass

    try:
        register_project_id_name(project_id, project_name)
        return recompute_dimensionality_for_version(
            project_id=project_id,
            version_id=version_id,
            api_url=api_url,
            token=token,
            port=port,
            project_name=project_name,
            method=method,
            num_dims=num_dims,
            force=force,
        )
    finally:
        if job_meta_handler is not None:
            try:
                logger.removeHandler(job_meta_handler)
                job_meta_handler.close()
            except Exception:
                pass


def sync_project_to_fiftyone(
    project_id: int,
    version_id: int | None,
    api_url: str,
    token: str,
    port: int,
    project_name: str | None = None,
    database_uri: str | None = None,
    database_name: str | None = None,
    force_sync: bool = False,
    vss_project_key: str | None = None,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
    section_id: int | None = None,
    query: str | None = None,
) -> dict[str, Any]:
    """
    Fetch Tator media and localizations, build FiftyOne dataset, launch App on given port.
    Uses per-project MongoDB database (database_uri when provided, else resolved via config; database_name override or get_database_name).
    Optional vss_project_key: selects a specific VSS project configuration for embeddings.
    Optional s3_bucket/s3_prefix: sync crop images to S3 (not full images) and build a second dataset from S3 (parent folder = label).
    Returns {"status": "ok", "dataset_name": str, "database_name": str} or raises.
    """
    if not (s3_bucket and s3_bucket.strip()):
        s3_cfg = get_s3_config(
            project_id, project_name=project_name, vss_project_key=vss_project_key
        )
        if s3_cfg:
            s3_bucket = s3_cfg.get("s3_bucket") or None
            s3_prefix = s3_cfg.get("s3_prefix") or s3_prefix
    if s3_bucket:
        s3_bucket = s3_bucket.strip()
        s3_prefix = (s3_prefix or "").strip() or None
    # Per-version S3 prefix for crops, e.g. fiftyone/raw/12/v21/crops (used for sync and dataset build)
    s3_crops_prefix = (
        _s3_crops_prefix(s3_prefix, project_id, version_id) if s3_bucket else None
    )
    logger.info(
        f"sync_project_to_fiftyone CALLED: project_id={project_id} version_id={version_id} "
        f"section_id={section_id} query={'set' if (query or '').strip() else 'none'} "
        f"api_url={api_url} port={port} s3_bucket={s3_bucket or 'none'}"
    )
    resolved_db = (
        database_name.strip() if database_name and database_name.strip() else None
    ) or get_database_name(project_id, port, project_name=project_name)
    resolved_uri = (
        database_uri.strip() if database_uri and database_uri.strip() else None
    ) or get_database_uri(project_id, port, project_name=project_name)
    if not get_is_enterprise():
        fo.config.database_uri = resolved_uri
        fo.config.database_name = resolved_db
        os.environ["FIFTYONE_DATABASE_URI"] = fo.config.database_uri
        os.environ["FIFTYONE_DATABASE_NAME"] = fo.config.database_name
    logger.info(f"database_uri={resolved_uri} database_name={resolved_db}")

    try:
        if get_is_enterprise():
            _test_fiftyone_connection()
            logger.info("FiftyOne connection OK (list_datasets)")
        else:
            _test_mongodb_connection(resolved_uri)
            logger.info("MongoDB connection OK")
    except ConnectionError as exc:
        logger.error(f"Pre-flight connection check failed: {exc}")
        raise RuntimeError(f"Pre-flight connection check failed: {exc}") from exc

    lock_key = get_sync_lock_key(resolved_db, project_id, version_id)
    logger.info(
        f"Acquiring sync lock: key={lock_key} (resolved_db={resolved_db}, project_id={project_id}, version_id={version_id})"
    )
    if not try_acquire_sync_lock(lock_key):
        logger.warning(
            f"Failed to acquire sync lock: key={lock_key} - another sync is in progress"
        )
        return {
            "status": "busy",
            "message": "This dataset is being updated by another sync. Please try again in a few minutes.",
            "database_name": resolved_db,
        }

    config_path = os.getenv("FIFTYONE_SYNC_CONFIG_PATH")
    logger.info(f"config_path={config_path}")
    config: dict[str, Any] = {}
    if config_path and os.path.exists(config_path):
        try:
            config = _load_config(config_path)
            logger.info(f"Loaded config from {config_path}")
        except Exception as e:
            logger.info(f"Failed to load config {config_path}: {e}")
    media_id_batch_size = (
        config.get("media_id_batch_size") or _DEFAULT_MEDIA_ID_BATCH_SIZE
    )
    localization_batch_size = (
        config.get("localization_batch_size") or _DEFAULT_LOCALIZATION_BATCH_SIZE
    )

    try:
        dl_dir = ""
        localizations_path = ""
        crops = _crops_dir(
            project_id, version_id, section_id=section_id, query=query
        )
        use_cached_jsonl = False
        try:
            host = api_url.rstrip("/")
            api = tator.get_api(host, token)
            crop_result = _run_crop_pipeline(
                api,
                project_id=project_id,
                version_id=version_id,
                api_url=api_url,
                token=token,
                force_sync=force_sync,
                force=False,
                media_id_batch_size=media_id_batch_size,
                localization_batch_size=localization_batch_size,
                s3_bucket=s3_bucket,
                s3_crops_prefix=s3_crops_prefix,
                section_id=section_id,
                query=query,
            )
            if crop_result.get("status") != "ok":
                return {
                    "status": "error",
                    "message": crop_result.get("message") or "Crop pipeline failed",
                    "database_name": resolved_db,
                    "saved_media_dir": crop_result.get("saved_media_dir"),
                    "saved_localizations_path": crop_result.get(
                        "saved_localizations_path"
                    ),
                    "saved_crops_dir": crop_result.get("saved_crops_dir"),
                }
            dl_dir = str(crop_result.get("saved_media_dir") or "")
            localizations_path = str(crop_result.get("saved_localizations_path") or "")
            use_cached_jsonl = bool(crop_result.get("used_cached_jsonl"))
            logger.info(
                "Crop pipeline complete: cropped=%s failed=%s misses=%s hits=%s",
                crop_result.get("num_cropped"),
                crop_result.get("num_failed"),
                crop_result.get("cache_misses"),
                crop_result.get("cache_hits"),
            )

        except Exception as e:
            logger.error(f"Sync failed: {e}")
            return {
                "status": "error",
                "message": str(e),
                "database_name": resolved_db,
                "saved_media_dir": dl_dir or None,
                "saved_localizations_path": localizations_path or None,
                "saved_crops_dir": crops or None,
            }

        if not localizations_path:
            logger.info("No localizations; skipping dataset build")
            return {
                "status": "ok",
                "message": "No crops to load; media/localizations missing or empty",
                "database_name": resolved_db,
                "dataset_name": None,
                "saved_media_dir": dl_dir or None,
                "saved_localizations_path": localizations_path or None,
                "saved_crops_dir": crops or None,
            }

        # Inject Tator base URL and ids so sample "url" can link to the localization's media page
        config["api_url"] = api_url.rstrip("/")
        config["project_id"] = project_id
        config["version_id"] = version_id
        config["force_sync"] = force_sync
        # In enterprise/production, use S3 URIs for sample filepaths so FiftyOne loads from S3
        if s3_bucket:
            config["s3_bucket"] = s3_bucket
            config["s3_prefix"] = s3_crops_prefix or ""

        dataset_name = _default_dataset_name(api, project_id, version_id)
        dataset_name = _dataset_name_with_port(dataset_name, port)

        # Set env so FiftyOne app subprocess uses the same database (only when not production)
        if not get_is_enterprise():
            os.environ["FIFTYONE_DATABASE_URI"] = fo.config.database_uri
            os.environ["FIFTYONE_DATABASE_NAME"] = fo.config.database_name

        # Media attributes (Image type only) for dataset samples
        config["media_attributes_map"] = _build_media_attributes_map(
            api,
            project_id,
            localizations_path,
            media_id_batch_size=media_id_batch_size,
        )

        # Build dataset from crops + localizations; filepath is S3 URI when s3_bucket in config
        try:
            logger.info(f"Building dataset {dataset_name} from crops")
            dataset = build_fiftyone_dataset_from_crops(
                crops_dir=crops,
                localizations_jsonl_path=localizations_path,
                dataset_name=dataset_name,
                config=config,
                download_dir=dl_dir or None,
            )
        except Exception as e:
            logger.info(f"Dataset build failed: {e}")
            return {
                "status": "error",
                "message": str(e),
                "database_name": resolved_db,
                "dataset_name": None,
                "saved_media_dir": dl_dir or None,
                "saved_localizations_path": localizations_path or None,
                "saved_crops_dir": crops or None,
            }

        logger.info(f"sync_project_to_fiftyone done: dataset={dataset_name}")
        logger.info(
            "sync_project_to_fiftyone: project=%s port=%s database=%s dataset=%s",
            project_id,
            port,
            resolved_db,
            dataset_name,
        )

        sample_count = len(dataset)
        logger.info(f"Dataset '{dataset_name}' has {sample_count} samples")

        # Always compute embeddings (from service) and UMAP; config.embeddings overrides defaults
        embeddings_config = config.get("embeddings") or {}
        if not isinstance(embeddings_config, dict):
            embeddings_config = {}
        try:
            proj = api.get_project(project_id)
            project_name_for_config = getattr(proj, "name", None) or str(project_id)
        except Exception:
            project_name_for_config = str(project_id)

        from src.app.database_manager import get_vss_project_config

        vss_project = None
        vss_config = get_vss_project_config(project_name_for_config, vss_project_key)
        if vss_config:
            vss_project = vss_config.get("vss_project")
            if vss_project_key:
                logger.info(
                    f"Using VSS project from key {vss_project_key!r}: {vss_project}"
                )
            else:
                logger.info(f"Using VSS project from config: {vss_project}")

        if vss_project:
            base_brain_key = embeddings_config.get("brain_key", "umap_viz")
            model_info = {
                "embeddings_field": embeddings_config.get(
                    "embeddings_field", "embeddings"
                ),
                # Store UMAP under `${brain_key}_umap` so other methods can live alongside
                # without overwriting the default key.
                "brain_key": f"{base_brain_key}_umap",
                "similarity_brain_key": embeddings_config.get("similarity_brain_key")
                or "",
                "similarity_metric": embeddings_config.get(
                    "similarity_metric", "cosine"
                ),
            }
            try:
                from src.app.embedding_service import is_embedding_service_available
                from src.app.embeddings_viz import (
                    compute_embeddings_and_viz,
                    has_embeddings,
                )

                embeddings_field = model_info["embeddings_field"]
                # When bypass was used (cached JSONL), skip embedding computation if embeddings already exist in MongoDB
                if use_cached_jsonl and has_embeddings(dataset, embeddings_field):
                    logger.info(
                        f"Bypass used and embeddings already exist in dataset '{embeddings_field}'; "
                        "skipping embedding computation"
                    )
                elif not is_embedding_service_available():
                    logger.info(
                        "Embedding service unavailable; skipping embeddings/UMAP (dataset still available)"
                    )
                else:
                    batch_size = embeddings_config.get("batch_size", 32)
                    logger.info(
                        f"Computing embeddings with batch size {batch_size}, UMAP, and similarity for dataset '{dataset_name}'..."
                    )
                    compute_embeddings_and_viz(
                        dataset,
                        model_info,
                        umap_seed=int(embeddings_config.get("umap_seed", 51)),
                        force_embeddings=bool(
                            embeddings_config.get("force_embeddings", False)
                        ),
                        force_umap=bool(embeddings_config.get("force_umap", False)),
                        batch_size=batch_size,
                        project_name=vss_project,
                        service_url=embeddings_config.get("service_url")
                        or os.environ.get("FASTVSS_API_URL"),
                    )
                    logger.info(
                        f"Embeddings, UMAP, and similarity completed for dataset '{dataset_name}'"
                    )
            except ImportError as e:
                logger.info(f"Skipping embeddings/UMAP (missing deps): {e}")
            except Exception as e:
                logger.info(f"Embeddings/UMAP failed (dataset still available): {e}")
                logging.getLogger(__name__).exception("Embeddings/UMAP failed")
        else:
            logger.info("No vss_project; skipping embeddings/UMAP")

        # URL for the launcher: always use FIFTYONE_APP_PUBLIC_BASE_URL as-is (no port suffix).
        # Include the full URL with any path prefix, e.g. https://cortex.shore.mbari.org/fiftyone
        app_url = os.environ.get(
            "FIFTYONE_APP_PUBLIC_BASE_URL", "http://localhost"
        ).strip().rstrip("/")
        logger.info(f"FiftyOne app URL (FIFTYONE_APP_PUBLIC_BASE_URL): {app_url}")

        result = {
            "status": "ok",
            "dataset_name": dataset_name,
            "database_name": resolved_db,
            "sample_count": sample_count,
            "saved_media_dir": dl_dir or None,
            "saved_localizations_path": localizations_path or None,
            "saved_crops_dir": crops or None,
        }
        if app_url is not None:
            result["app_url"] = app_url
        result["port"] = port
        return result
    finally:
        logger.info(f"Releasing sync lock: key={lock_key}")
        release_sync_lock(lock_key)


def main() -> None:
    """Read env (HOST, TOKEN, PROJECT_ID, optional MEDIA_IDS, VERSION_ID) and fetch media + localizations."""
    host = os.getenv("HOST", "").rstrip("/")
    token = os.getenv("TOKEN")
    project_id_str = os.getenv("PROJECT_ID")
    media_ids_str = os.getenv("MEDIA_IDS", "").strip()
    logger.info(
        f"main: HOST={'<set>' if host else '<unset>'} PROJECT_ID={project_id_str or '<unset>'} MEDIA_IDS={'<set>' if media_ids_str else '<unset>'}"
    )

    if not host or not token or not project_id_str:
        logger.info("Set HOST, TOKEN, and PROJECT_ID environment variables.")
        return
    project_id = int(project_id_str)
    media_ids_filter: list[int] | None = None
    if media_ids_str:
        media_ids_filter = [
            int(id_.strip()) for id_ in media_ids_str.split(",") if id_.strip()
        ]

    api = tator.get_api(host, token)
    version_id_str = os.getenv("VERSION_ID", "").strip()
    version_id = int(version_id_str) if version_id_str else None

    try:
        project_name = getattr(api.get_project(project_id), "name", None) or str(
            project_id
        )
    except Exception:
        project_name = str(project_id)
    port = get_port_for_project(project_id, project_name=project_name)

    # Load config early for batch sizes (see config.yml)
    config_path = os.getenv("CONFIG_PATH")
    config = (
        _load_config(config_path) if config_path and os.path.exists(config_path) else {}
    )
    media_id_batch_size_cli = (
        config.get("media_id_batch_size") or _DEFAULT_MEDIA_ID_BATCH_SIZE
    )
    localization_batch_size_cli = (
        config.get("localization_batch_size") or _DEFAULT_LOCALIZATION_BATCH_SIZE
    )

    # Fetch media IDs (lightweight)
    media_ids = fetch_project_media_ids(
        host,
        token,
        project_id,
        media_ids_filter=media_ids_filter,
        version_id=version_id,
    )
    logger.info(f"media_ids: {media_ids}")

    # Fetch localizations first (cheap metadata)
    localizations_path = fetch_and_save_localizations(
        api,
        project_id,
        version_id=version_id,
        media_ids=media_ids if media_ids else None,
        localization_batch_size=localization_batch_size_cli,
        media_id_batch_size=media_id_batch_size_cli,
    )
    if localizations_path:
        logger.info(f"saved_localizations_path (JSONL): {localizations_path}")

    dl_dir = _download_dir(project_id)
    crops = _crops_dir(project_id, version_id)

    # Determine cache misses
    if localizations_path:
        old_manifest = _load_crop_manifest(project_id, version_id)
        media_ids_needed, locs_to_crop, updated_manifest = _find_crop_cache_misses(
            localizations_jsonl_path=localizations_path,
            crops_dir=crops,
            manifest=old_manifest,
            download_dir=dl_dir,
        )
        _cleanup_deleted_crops(old_manifest, updated_manifest, crops)

        # Download only media with cache misses; get Media objects for cropping
        all_media_cli: list[Any] = []
        if media_ids and media_ids_needed:
            needed_ids = [mid for mid in media_ids if mid in media_ids_needed]
            all_media_cli = get_media_chunked(
                api, project_id, needed_ids, media_id_batch_size=media_id_batch_size_cli
            )
            if all_media_cli:
                save_media_to_tmp(
                    api, project_id, all_media_cli, media_ids_filter=media_ids_needed
                )
            else:
                logger.info("No Media objects returned; download skipped.")
        elif not media_ids_needed:
            logger.info("All crops cached; skipping media download")

        # Crop only cache misses (ffmpeg; image + video)
        if locs_to_crop:
            crop_localizations_parallel(
                dl_dir,
                localizations_path,
                crops,
                size=224,
                locs_to_crop=locs_to_crop,
                media_objects=all_media_cli,
            )
            _cleanup_downloaded_videos(dl_dir)

        # Patch manifest stems from downloaded filenames and from Media (video)
        _patch_manifest_stems(updated_manifest, dl_dir, media_objects=all_media_cli)

        # Save updated manifest
        _save_crop_manifest(project_id, version_id, updated_manifest)

        # Remove downloaded media to reclaim disk space
        _cleanup_download_dir(project_id)

    if crops and localizations_path and os.path.isdir(crops):
        if not get_is_enterprise():
            fo.config.database_uri = get_database_uri(
                project_id, port, project_name=project_name
            )
            fo.config.database_name = get_database_name(project_id, port, project_name)
            os.environ["FIFTYONE_DATABASE_URI"] = fo.config.database_uri
            os.environ["FIFTYONE_DATABASE_NAME"] = fo.config.database_name
        config["api_url"] = host.rstrip("/")
        config["project_id"] = project_id
        config["version_id"] = version_id
        config["media_attributes_map"] = _build_media_attributes_map(
            api,
            project_id,
            localizations_path,
            media_id_batch_size=media_id_batch_size_cli,
        )
        dataset_name = _default_dataset_name(api, project_id, version_id)
        dataset_name = _dataset_name_with_port(dataset_name, port)
        build_fiftyone_dataset_from_crops(
            crops_dir=crops,
            localizations_jsonl_path=localizations_path,
            dataset_name=dataset_name,
            config=config,
            download_dir=dl_dir,
        )
        logger.info(
            f"Dataset built. FiftyOne app should be running in another container on port {port}."
        )


def check_dataset_exists_for_version(
    project_id: int,
    version_id: int,
    port: int,
    api_url: str,
    token: str,
    project_name: str | None = None,
    database_uri: str | None = None,
    database_name: str | None = None,
) -> dict[str, Any]:
    """Check whether a FiftyOne dataset exists for the given version/port.

    Returns {"exists": bool, "dataset_name": str | None, "database_name": str}.
    """
    resolved_db = (
        database_name.strip() if database_name and database_name.strip() else None
    ) or get_database_name(project_id, port, project_name=project_name)
    resolved_uri = (
        database_uri.strip() if database_uri and database_uri.strip() else None
    ) or get_database_uri(project_id, port, project_name=project_name)

    if not get_is_enterprise():
        fo.config.database_uri = resolved_uri
        fo.config.database_name = resolved_db
        os.environ["FIFTYONE_DATABASE_URI"] = fo.config.database_uri
        os.environ["FIFTYONE_DATABASE_NAME"] = fo.config.database_name

    try:
        if get_is_enterprise():
            _test_fiftyone_connection()
        else:
            _test_mongodb_connection(resolved_uri)
    except ConnectionError as exc:
        raise RuntimeError(f"Connection check failed: {exc}") from exc

    host = api_url.rstrip("/")
    api = tator.get_api(host, token)
    ds_name = _default_dataset_name(api, project_id, version_id)
    ds_name_with_port = _dataset_name_with_port(ds_name, port)
    project_prefix = (
        _sanitize_dataset_name(project_name) if project_name else f"project_{project_id}"
    )
    port_suffix = f"_{port}"
    version_part = f"_v{version_id}"

    available = fo.list_datasets()

    def _find_match() -> str | None:
        if ds_name_with_port in available:
            return ds_name_with_port
        if ds_name in available:
            return ds_name
        for d in available:
            if d.startswith(project_prefix) and version_part in d and d.endswith(port_suffix):
                return d
        return None

    target = _find_match()
    return {
        "exists": target is not None,
        "dataset_name": target,
        "database_name": resolved_db,
    }


def list_available_datasets(
    project_id: int,
    port: int,
    project_name: str | None = None,
    database_uri: str | None = None,
    database_name: str | None = None,
) -> dict[str, Any]:
    """List all FiftyOne dataset names for the current project DB context."""
    resolved_db = (
        database_name.strip() if database_name and database_name.strip() else None
    ) or get_database_name(project_id, port, project_name=project_name)
    resolved_uri = (
        database_uri.strip() if database_uri and database_uri.strip() else None
    ) or get_database_uri(project_id, port, project_name=project_name)

    if not get_is_enterprise():
        fo.config.database_uri = resolved_uri
        fo.config.database_name = resolved_db
        os.environ["FIFTYONE_DATABASE_URI"] = fo.config.database_uri
        os.environ["FIFTYONE_DATABASE_NAME"] = fo.config.database_name

    try:
        if get_is_enterprise():
            _test_fiftyone_connection()
        else:
            _test_mongodb_connection(resolved_uri)
    except ConnectionError as exc:
        raise RuntimeError(f"Connection check failed: {exc}") from exc

    datasets = sorted(fo.list_datasets())
    return {
        "datasets": datasets,
        "database_name": resolved_db,
    }


_embeddings_config_cache: dict[str, Any] | None = None


def _load_embeddings_config_from_sync_yaml() -> dict[str, Any]:
    """
    Load the `embeddings` section from the YAML pointed to by FIFTYONE_SYNC_CONFIG_PATH.
    """
    global _embeddings_config_cache
    if _embeddings_config_cache is not None:
        return _embeddings_config_cache

    path = os.environ.get("FIFTYONE_SYNC_CONFIG_PATH", "").strip()
    if not path:
        raise RuntimeError(
            "FIFTYONE_SYNC_CONFIG_PATH is not set; cannot read embeddings config"
        )

    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    embeddings_cfg = cfg.get("embeddings") or {}
    if not isinstance(embeddings_cfg, dict):
        embeddings_cfg = {}

    _embeddings_config_cache = embeddings_cfg
    return embeddings_cfg


def recompute_dimensionality_for_version(
    project_id: int,
    version_id: int,
    api_url: str,
    token: str,
    port: int,
    project_name: str | None = None,
    method: str = "umap",
    num_dims: int = 2,
    force: bool = True,
) -> dict[str, Any]:
    """
    Recompute ONLY the dimensionality reduction visualization using cached embeddings.

    This deletes and recreates the FiftyOne brain run at the configured `brain_key`.
    """
    embeddings_cfg = _load_embeddings_config_from_sync_yaml()
    embeddings_field = embeddings_cfg.get("embeddings_field", "embeddings")
    base_brain_key = embeddings_cfg.get("brain_key", "umap_viz")
    seed = int(embeddings_cfg.get("umap_seed", 51))

    method_norm = (method or "").strip().lower()
    brain_key = (
        f"{base_brain_key}_umap"
        if method_norm == "umap"
        else f"{base_brain_key}_{method_norm}"
    )

    logger.info(
        "Recomputing dimensionality reduction: method=%r brain_key=%r embeddings_field=%r dataset=(project_id=%s version_id=%s port=%s)",
        method,
        brain_key,
        embeddings_field,
        project_id,
        version_id,
        port,
    )

    ds_info = check_dataset_exists_for_version(
        project_id=project_id,
        version_id=version_id,
        port=port,
        api_url=api_url,
        token=token,
        project_name=project_name,
    )
    ds_name = ds_info.get("dataset_name")
    if not ds_info.get("exists") or not ds_name:
        raise ValueError(
            f"FiftyOne dataset not found for project_id={project_id}, version_id={version_id}, port={port}"
        )

    dataset = fo.load_dataset(ds_name)
    dataset.reload()

    from src.app.embeddings_viz import compute_dimensionality_reduction

    compute_dimensionality_reduction(
        dataset,
        embeddings_field=embeddings_field,
        brain_key=brain_key,
        method=method_norm,
        seed=seed,
        num_dims=num_dims,
        force=force,
    )

    return {
        "status": "ok",
        "dataset_name": ds_name,
        "database_name": ds_info.get("database_name"),
        "method": method_norm,
        "brain_key": brain_key,
    }


def delete_dataset_for_version(
    project_id: int,
    version_id: int,
    port: int,
    api_url: str,
    token: str,
    project_name: str | None = None,
    database_uri: str | None = None,
    database_name: str | None = None,
) -> dict[str, Any]:
    """Delete the FiftyOne dataset (MongoDB) and JSONL cache for a specific version/port.

    Only removes the MongoDB dataset and the localizations JSONL file.
    Crop images and downloaded media are intentionally preserved.
    Returns {"status": "ok", "deleted": str, "database_name": str, "jsonl_deleted": bool}.
    """
    resolved_db = (
        database_name.strip() if database_name and database_name.strip() else None
    ) or get_database_name(project_id, port, project_name=project_name)
    resolved_uri = (
        database_uri.strip() if database_uri and database_uri.strip() else None
    ) or get_database_uri(project_id, port, project_name=project_name)

    if not get_is_enterprise():
        fo.config.database_uri = resolved_uri
        fo.config.database_name = resolved_db
        os.environ["FIFTYONE_DATABASE_URI"] = fo.config.database_uri
        os.environ["FIFTYONE_DATABASE_NAME"] = fo.config.database_name

    try:
        if get_is_enterprise():
            _test_fiftyone_connection()
        else:
            _test_mongodb_connection(resolved_uri)
    except ConnectionError as exc:
        raise RuntimeError(f"Connection check failed: {exc}") from exc

    host = api_url.rstrip("/")
    api = tator.get_api(host, token)
    ds_name = _default_dataset_name(api, project_id, version_id)
    ds_name_with_port = _dataset_name_with_port(ds_name, port)

    project_prefix = _sanitize_dataset_name(project_name) if project_name else f"project_{project_id}"
    port_suffix = f"_{port}"
    version_part = f"_v{version_id}"

    available = fo.list_datasets()

    def _find_match() -> str | None:
        if ds_name_with_port in available:
            return ds_name_with_port
        if ds_name in available:
            return ds_name
        for d in available:
            if d.startswith(project_prefix) and version_part in d and d.endswith(port_suffix):
                return d
        return None

    target = _find_match()
    if target is None:
        return {
            "status": "ok",
            "deleted": None,
            "database_name": resolved_db,
            "message": f"No dataset found for version {version_id} (looked for '{ds_name_with_port}')",
        }

    fo.delete_dataset(target)
    logger.info(f"Deleted dataset '{target}' from database {resolved_db}")

    jsonl_path = _localizations_jsonl_path(project_id, version_id)
    jsonl_deleted = False
    if os.path.isfile(jsonl_path):
        os.remove(jsonl_path)
        jsonl_deleted = True
        logger.info(f"Deleted JSONL file: {jsonl_path}")

    return {
        "status": "ok",
        "deleted": target,
        "database_name": resolved_db,
        "jsonl_deleted": jsonl_deleted,
    }


if __name__ == "__main__":
    main()
