# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync.py
# Description: Tator to FiftyOne sync: fetch media and localizations, build dataset, launch app.
"""
Tator to FiftyOne sync: fetch media + localizations, build FiftyOne dataset, launch app.
Phase 2 implementation. Requires fiftyone, tator, Pillow, PyYAML and MongoDB.
"""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import shutil
import subprocess
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
    get_session,
    get_s3_config,
)
from src.app.sync_lock import (
    get_sync_lock_key,
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
    return f"v{version_id}" if version_id is not None else "v_all"


def _download_dir(project_id: int) -> str:
    """Ephemeral media-download directory, isolated from JSONL and crops."""
    path = os.path.join(_SYNC_BASE, "downloads", str(project_id))
    os.makedirs(path, exist_ok=True)
    return path


def _data_dir(project_id: int, version_id: int | None) -> str:
    """Per-project+version directory for JSONL, crops, and manifest."""
    path = os.path.join(_SYNC_BASE, "data", str(project_id), _version_slug(version_id))
    os.makedirs(path, exist_ok=True)
    return path


def _crops_dir(project_id: int, version_id: int | None) -> str:
    """Per-project+version crops directory."""
    path = os.path.join(_data_dir(project_id, version_id), "crops")
    os.makedirs(path, exist_ok=True)
    return path


def _localizations_jsonl_path(project_id: int, version_id: int | None) -> str:
    """Per-project+version JSONL path."""
    return os.path.join(_data_dir(project_id, version_id), "localizations.jsonl")


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

    def _version_kw() -> dict:
        return {"version": [version_id]} if version_id is not None else {}

    try:
        loc_count = 0
        for batch in media_id_batches:
            kw = _version_kw()
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
) -> list[int]:
    """
    Fetch all media in the project. Returns list of media ids.
    If media_ids_filter is set, only those media are returned (and must exist in the project).
    If version_id is set, filters media by that version via related_attribute.
    """
    logger.info(
        f"fetch_project_media_ids: project_id={project_id} filter={media_ids_filter} version_id={version_id}"
    )
    host = api_url.rstrip("/")
    api = tator.get_api(host, token)
    kwargs: dict = {}
    if version_id is not None:
        kwargs["related_attribute"] = [f"$version::{version_id}"]
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


# Video extensions: skip download (not supported); downloads come directly from Tator for images only.
VIDEO_EXTENSIONS = (".mp4", ".mov", ".avi", ".webm", ".mkv", ".m4v")


def _is_video_name(name: str) -> bool:
    return any(name.lower().endswith(ext) for ext in VIDEO_EXTENSIONS)


def save_media_to_tmp(
    api: Any,
    project_id: int,
    media_objects: list[Any],
    media_ids_filter: set[int] | None = None,
) -> str:
    """
    Download each media to an isolated download directory.
    Videos are skipped (download not supported). Existing non-empty files are skipped.
    When media_ids_filter is provided, only media whose id is in the set are downloaded.
    Retries each download up to 3 times. Returns the download directory path.
    """
    out_dir = _download_dir(project_id)
    valid = [m for m in media_objects if isinstance(m, tator.models.Media)]
    if media_ids_filter is not None:
        valid = [m for m in valid if m.id in media_ids_filter]
    total = len(valid)
    logger.info(f"Saving {total} media files to {out_dir}")
    downloaded = 0
    skipped = 0
    failed = 0
    log_interval = max(1, total // 10)
    for idx, m in enumerate(valid, 1):
        safe_name = f"{m.id}_{m.name}"
        out_path = os.path.join(out_dir, safe_name)
        if _is_video_name(m.name):
            skipped += 1
            continue
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            skipped += 1
            continue
        num_tries = 0
        success = False
        while num_tries < 3 and not success:
            try:
                for _ in tator.util.download_media(api, m, out_path):
                    pass
                success = True
                downloaded += 1
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
                f"Download progress: {idx}/{total} processed ({downloaded} saved, {skipped} skipped, {failed} failed)"
            )
    logger.info(
        f"Download complete: {downloaded} saved, {skipped} skipped, {failed} failed -> {out_dir}"
    )
    return out_dir


def fetch_and_save_localizations(
    api: Any,
    project_id: int,
    version_id: int | None = None,
    media_ids: list[int] | None = None,
    localization_batch_size: int | None = None,
    media_id_batch_size: int | None = None,
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
    out_path = _localizations_jsonl_path(project_id, version_id)
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

    def _version_kw() -> dict:
        return {"version": [version_id]} if version_id is not None else {}

    try:
        loc_count = 0
        for mid_batch in media_id_batches:
            kw = _version_kw()
            if mid_batch:
                kw["media_id"] = mid_batch
            loc_count += api.get_localization_count(project_id, **kw)
        logger.info(
            f"get_localization_count(project_id={project_id}, media_ids={bool(media_ids)}, version={version_id}) = {loc_count}"
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
                    kw.update(_version_kw())
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


def _crop_one(
    size: int,
    image_path: Path,
    loc: dict,
    out_path: Path,
) -> bool:
    """
    Crop one localization from an image: pad shorter side to square (longest side), resize to size x size.
    Tator box: x, y, width, height as normalized 0-1.
    """
    try:
        x = float(loc.get("x", 0))
        y = float(loc.get("y", 0))
        w = float(loc.get("width", 0))
        h = float(loc.get("height", 0))
        if w <= 0 or h <= 0:
            return False
        img = Image.open(image_path).convert("RGB")
        image_width, image_height = img.size
        x1 = int(image_width * x)
        y1 = int(image_height * y)
        x2 = int(image_width * (x + w))
        y2 = int(image_height * (y + h))
        # Clamp to image bounds
        x1 = max(0, min(x1, image_width - 1))
        y1 = max(0, min(y1, image_height - 1))
        x2 = max(1, min(x2, image_width))
        y2 = max(1, min(y2, image_height))
        width = x2 - x1
        height = y2 - y1
        shorter_side = min(height, width)
        longer_side = max(height, width)
        delta = abs(longer_side - shorter_side)
        padding = delta // 2
        if width == shorter_side:
            x1 = max(0, x1 - padding)
            x2 = min(image_width, x2 + padding)
        else:
            y1 = max(0, y1 - padding)
            y2 = min(image_height, y2 + padding)
        cropped = img.crop((x1, y1, x2, y2))
        resample = (
            Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS
        )
        resized = cropped.resize((size, size), resample=resample)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        resized.save(out_path)
        return True
    except Exception as e:
        logger.info(f"Crop failed for {out_path.stem}: {e}")
        return False


def crop_localizations_parallel(
    download_dir: str,
    localizations_jsonl_path: str,
    crops_dir: str,
    size: int = 224,
    max_workers: int | None = None,
    locs_to_crop: list[dict] | None = None,
) -> tuple[int, int]:
    """
    Crop localizations from their media in parallel.
    Saves using elemental_id as filestem (e.g. elemental_id.png).

    When locs_to_crop is provided, only those localizations are cropped (cache-miss
    optimization). Otherwise falls back to reading all localizations from the JSONL.

    Returns (num_cropped, num_failed).
    """
    if not os.path.exists(download_dir):
        logger.info("Download dir missing; skipping crops")
        return (0, 0)
    if locs_to_crop is None and not os.path.exists(localizations_jsonl_path):
        logger.info(
            "Localizations JSONL missing and no locs_to_crop provided; skipping crops"
        )
        return (0, 0)
    download_path = Path(download_dir)
    crops_path = Path(crops_dir)
    crops_path.mkdir(parents=True, exist_ok=True)
    media_id_to_path: dict[int, Path] = {}
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
                    media_id_to_path[mid] = f
                except ValueError:
                    pass

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

    tasks: list[tuple[Path, dict, Path]] = []
    for mid, locs in locs_by_media.items():
        image_path = media_id_to_path.get(mid)
        if image_path is None or not image_path.exists():
            continue
        for loc in locs:
            elemental_id = loc.get("elemental_id") or loc.get("id")
            if elemental_id is None:
                continue
            out_path = crops_path / image_path.stem / f"{elemental_id}.png"
            tasks.append((image_path, loc, out_path))
    if not tasks:
        logger.info("No localization crops to process")
        return (0, 0)
    logger.info(f"Cropping {len(tasks)} localizations in parallel (size={size}x{size})")
    workers = max_workers or min(128, (os.cpu_count() or 4) * 2)
    num_ok = 0
    num_fail = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(_crop_one, size, imp, loc, op): (imp, loc, op)
            for imp, loc, op in tasks
        }
        for fut in as_completed(futures):
            if fut.exception():
                num_fail += 1
                logger.info(f"Crop task failed: {fut.exception()}")
            elif fut.result():
                num_ok += 1
            else:
                num_fail += 1
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


def _crop_manifest_path(project_id: int, version_id: int | None) -> str:
    """Path to the crop manifest JSON for a project+version."""
    return os.path.join(_data_dir(project_id, version_id), "crop_manifest.json")


def _load_crop_manifest(project_id: int, version_id: int | None) -> dict[str, dict]:
    """
    Load the crop manifest from disk.
    Returns {elemental_id: {"media_id": int, "media_stem": str}} or empty dict.
    """
    path = _crop_manifest_path(project_id, version_id)
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.info(f"Could not load crop manifest {path}: {e}")
        return {}


def _save_crop_manifest(
    project_id: int, version_id: int | None, manifest: dict[str, dict]
) -> None:
    """Atomically write the crop manifest to disk."""
    path = _crop_manifest_path(project_id, version_id)
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


# Image extensions for S3 raw-image dataset (parent folder = class name)
DEFAULT_S3_IMAGE_EXTENSIONS = (
    "png",
    "jpg",
    "jpeg",
    "bmp",
    "tiff",
    "tif",
    "webp",
    "gif",
)


def _s3_uri_parent_and_stem(s3_uri: str) -> tuple[str, str]:
    """
    Parse s3://bucket/prefix/class_name/filename.ext into (parent_folder_name, stem).
    Parent folder is used as class label; stem is the filename without extension (e.g. elemental_id).
    """
    s3_uri = (s3_uri or "").strip().rstrip("/")
    if not s3_uri.startswith("s3://"):
        return ("unknown", "")
    path_part = s3_uri.replace("s3://", "", 1)
    if "/" in path_part:
        path_part = path_part.split("/", 1)[1]  # drop bucket
    parts = [p for p in path_part.split("/") if p]
    if len(parts) >= 2:
        parent_folder = parts[-2]
        last = parts[-1]
        stem = last.rsplit(".", 1)[0] if "." in last else last
        return (parent_folder, stem)
    if len(parts) == 1:
        last = parts[0]
        stem = last.rsplit(".", 1)[0] if "." in last else last
        return ("unknown", stem)
    return ("unknown", "")


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


def build_fiftyone_dataset_from_s3(
    s3_bucket: str,
    s3_prefix: str | None,
    dataset_name: str,
    config: dict[str, Any] | None = None,
) -> fo.Dataset:
    """
    List image files in an S3 bucket/prefix using fiftyone.core.storage, then build a FiftyOne
    dataset where the parent folder of each file is used as the class label.
    Assumes layout: s3://bucket/prefix/class_name/image_id.jpg (class_name = label).
    """
    import fiftyone.core.storage as fos

    config = config or {}
    image_extensions = config.get("image_extensions") or list(
        DEFAULT_S3_IMAGE_EXTENSIONS
    )
    # Normalize to bare extensions for matching (e.g. "jpg" from "*.jpg" or "jpg")
    ext_set = set()
    for ext in image_extensions:
        e = (ext or "").strip().lstrip("*.")
        if e:
            ext_set.add(e.lower())

    prefix = (s3_prefix or "").strip().rstrip("/")
    s3_dir = f"s3://{s3_bucket}/{prefix}/" if prefix else f"s3://{s3_bucket}/"
    logger.info(f"Listing S3 files: {s3_dir} (recursive)")
    s3_files = fos.list_files(s3_dir, recursive=True, abs_paths=True)
    samples: list[fo.Sample] = []
    for s3_uri in s3_files:
        s3_uri_lower = s3_uri.lower()
        matched = False
        for ext in ext_set:
            if s3_uri_lower.endswith(f".{ext}"):
                matched = True
                break
        if not matched:
            continue
        parent_folder, elemental_id = _s3_uri_parent_and_stem(s3_uri)
        label_name = parent_folder if parent_folder else "unknown"
        sample = fo.Sample(filepath=s3_uri)
        sample["ground_truth"] = fo.Classification(label=label_name, confidence=1.0)
        sample["elemental_id"] = elemental_id
        sample["label"] = label_name
        sample["local_filepath"] = s3_uri.replace("s3://", "")
        samples.append(sample)

    if not samples:
        raise ValueError(
            f"No image files found in {s3_dir} (extensions: {list(ext_set)})"
        )
    logger.info(f"Collected {len(samples)} samples from S3 for dataset {dataset_name}")

    if dataset_name in fo.list_datasets():
        dataset = fo.load_dataset(dataset_name)
        dataset.persistent = True
        dataset.clear()
        dataset.add_samples(samples)
        logger.info(f"Replaced dataset '{dataset_name}' with {len(samples)} S3 samples")
    else:
        dataset = fo.Dataset(dataset_name)
        dataset.persistent = True
        dataset.add_samples(samples)
        logger.info(f"Created dataset '{dataset_name}' with {len(samples)} S3 samples")
    return dataset


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


def _patch_manifest_stems(manifest: dict[str, dict], download_dir: str) -> None:
    """
    After downloading new media, update manifest entries whose media_stem is
    still a bare media_id (fallback) with the real stem from the download directory.
    """
    real_stems = _media_id_to_stem(download_dir)
    if not real_stems:
        return
    for entry in manifest.values():
        mid = entry.get("media_id")
        if mid is None:
            continue
        mid = int(mid)
        current_stem = entry.get("media_stem", "")
        if current_stem == str(mid) and mid in real_stems:
            entry["media_stem"] = real_stems[mid]


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
        try:
            return float(val)
        except (TypeError, ValueError):
            pass
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
    return None


def _apply_loc_to_sample(
    sample: fo.Sample,
    loc: dict,
    *,
    api_url: str | None = None,
    project_id: int | None = None,
    version_id: int | None = None,
) -> None:
    """Update an existing sample's metadata from a localization (ground_truth, confidence, tags, annotation, tator_modified_at)."""
    label = _get_label_from_loc(loc)
    sample["ground_truth"] = fo.Classification(label=label, confidence=1.0)
    attrs = loc.get("attributes") or {}
    score = attrs.get("score")
    verified = attrs.get("verified")
    label_s = attrs.get("label_s")
    cluster = attrs.get("cluster")
    if score is not None:
        sample["confidence"] = float(score)
    # Replace tags with current values from loc (avoid stale label/cluster/verified)
    sample.tags.clear()
    if verified is not None and bool(verified):
        sample.tags.append("verified")
    if label_s is not None and "Unknown" not in label_s:
        sample.append(label_s)
    if cluster is not None and "Unknown" not in cluster:
        sample.tags.append(cluster)
    if api_url and project_id is not None:
        tator_url = _tator_localization_url(api_url, project_id, loc, version_id)
        if tator_url:
            sample["annotation"] = tator_url
    modified_at = loc.get("modified_datetime") or loc.get("created_datetime")
    if modified_at is not None:
        sample[TATOR_MODIFIED_AT_FIELD] = modified_at


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
    logger.info("Reconcile: Remove samples deleted in Tator")
    if tator_eids:
        # Use list comprehension for faster collection of IDs to remove
        # Access sample values directly without checking "in sample" each time
        # This assumes elemental_id is always present - if not guaranteed, keep the original check
        to_remove = [
            s.id
            for s in dataset
            if getattr(s, "elemental_id", None) is not None
            and str(s.elemental_id) not in tator_eids
        ]

        if to_remove:
            # Delete in batches if dataset is large (FiftyOne might handle this internally)
            dataset.delete_samples(to_remove)
            logger.info(
                f"Reconcile: removed {len(to_remove)} samples (deleted in Tator)"
            )
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

    # Now process only samples that have matching loc_index entries and have been modified
    api_url = config.get("api_url")
    project_id = config.get("project_id")
    version_id = config.get("version_id")
    for eid, sample in eid_to_sample.items():
        loc = loc_index[eid]
        modified_at = loc.get("modified_datetime") or loc.get("created_datetime")
        tator_modified_at = getattr(sample, TATOR_MODIFIED_AT_FIELD, None)
        mod_ts = _normalize_modified_at(modified_at)
        last_ts = _normalize_modified_at(tator_modified_at)

        logger.debug(
            f"====>Checking sample {sample.id} for update: {eid} modified_at: {modified_at} {TATOR_MODIFIED_AT_FIELD}: {tator_modified_at}"
        )
        if mod_ts is not None and mod_ts != last_ts:
            logger.debug(
                f"====>Sample {sample.id} needs update: {eid} modified_at: {modified_at} {TATOR_MODIFIED_AT_FIELD}: {tator_modified_at}"
            )
            samples_to_update.append((sample, loc))
            updated += 1

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
    # Use set for O(1) lookups
    dataset_eids = {
        str(s.elemental_id) for s in dataset if getattr(s, "elemental_id", None)
    }

    # Find new EIDs efficiently
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
            sample["ground_truth"] = fo.Classification(label=label, confidence=1.0)
            sample["elemental_id"] = elemental_id
            sample["media_stem"] = media_stem
            # Media-level attributes (Image type only), if provided in config
            media_attrs_map = config.get("media_attributes_map") or {}
            if loc:
                media_id = loc.get("media")
                if media_id is not None:
                    media_attrs = media_attrs_map.get(int(media_id)) or {}
                    for k, v in media_attrs.items():
                        if v is not None:
                            sample[k] = v
                # Localization attributes: confidence, verified, label_s, cluster (same as _create_sample_from_loc)
                attrs = loc.get("attributes") or {}
                score = attrs.get("score")
                verified = attrs.get("verified")
                label_s = attrs.get("label_s")
                cluster = attrs.get("cluster")
                if score is not None:
                    sample["confidence"] = float(score)
                if verified is not None and bool(verified):
                    sample.tags.append("verified")
                if label_s is not None and "Unknown" not in label_s:
                    sample.tags.append(label_s)
                if cluster is not None and "Unknown" not in cluster:
                    sample.tags.append(cluster)
                modified_at = loc.get("modified_datetime") or loc.get(
                    "created_datetime"
                )
                if modified_at is not None:
                    sample[TATOR_MODIFIED_AT_FIELD] = modified_at
            samples.append(sample)
        if max_samples and len(samples) >= max_samples:
            break

    if not samples:
        raise ValueError(f"No crops found in {crops_dir} (checked {seen} files)")

    logger.info(f"Collected {len(samples)} samples for dataset")

    # Handle existing dataset: always reconcile, never delete
    if dataset_name in fo.list_datasets():
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
        logger.info(f"Reconcile: dataset {dataset_name} loaded")
        return dataset

    logger.info(
        f"Reconcile: creating new dataset {dataset_name} in database {fo.config.database_name}"
    )
    dataset = fo.Dataset(dataset_name)
    dataset.persistent = True  # Persist dataset in MongoDB after session ends
    dataset.add_samples(samples)
    logger.info(f"Created dataset '{dataset_name}' with {len(samples)} samples")
    return dataset


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


def _update_localization_attributes(
    api: Any,
    project_id: int,
    version_id: int,
    elemental_id: str,
    attrs: dict[str, Any],
) -> None:
    """
    Update a localization's attributes by elemental_id.
    Uses get_localization_list to resolve elemental_id to id, then
    api.update_localization(id, localization_update) per Tator API.
    """
    locs = api.get_localization_list(
        project_id, version=[version_id], elemental_id=elemental_id
    )
    locs_list = list(locs) if not isinstance(locs, list) else locs
    if not locs_list:
        raise ValueError(f"No localization found for elemental_id={elemental_id}")
    loc_id = locs_list[0].id
    api.update_localization(loc_id, localization_update={"attributes": attrs})


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
) -> dict[str, Any]:
    """
    Push FiftyOne dataset edits (labels, confidence) back to Tator localizations.
    Matches samples by elemental_id; looks up localization id via get_localization_list,
    then updates attributes via update_localization(id, localization_update).
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

    def _resolve_dataset(requested: str) -> str | None:
        """Return the actual dataset name: exact match first, then name+port, then project+port match."""
        available = fo.list_datasets()
        if requested in available:
            return requested
        # Default name has no port; stored name is base + port_suffix (e.g. project_v66_5151)
        if (requested + port_suffix) in available:
            return requested + port_suffix
        matches = [
            d
            for d in available
            if d.startswith(project_prefix) and d.endswith(port_suffix)
        ]
        if not matches:
            return None
        if len(matches) == 1:
            return matches[0]
        for candidate in matches:
            if candidate == f"{project_prefix}{port_suffix}":
                return candidate
        return matches[0]

    fallback_db = f"{os.environ.get('FIFTYONE_DATABASE_DEFAULT', 'fiftyone_project')}_{project_id}"
    resolved = _resolve_dataset(ds_name)
    if resolved is None and db_name != fallback_db:
        if not get_is_enterprise():
            fo.config.database_name = fallback_db
            os.environ["FIFTYONE_DATABASE_NAME"] = fallback_db
        resolved = _resolve_dataset(ds_name)
    if resolved is None:
        if not get_is_enterprise():
            fo.config.database_name = db_name
        raise ValueError(
            f"No dataset matching project '{project_prefix}' with port {port} found in database '{db_name}' (or '{fallback_db}'). "
            "Run POST /sync first. Ensure FIFTYONE_DATABASE_URI and FIFTYONE_DATABASE_NAME match the sync process."
        )
    ds_name = resolved

    dataset = fo.load_dataset(ds_name)

    updated = 0
    failed = 0
    skipped = 0
    errors: list[str] = []
    _debug = debug or os.environ.get("FIFTYONE_SYNC_DEBUG", "").lower() in (
        "1",
        "true",
        "yes",
    )

    for sample in dataset.iter_samples(autosave=False):
        elemental_id = sample["elemental_id"] if "elemental_id" in sample else None
        if not elemental_id:
            failed += 1
            errors.append(f"Sample {sample.id}: missing elemental_id")
            continue
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
        if not attrs:
            continue

        modified_at = (
            sample[TATOR_MODIFIED_AT_FIELD]
            if TATOR_MODIFIED_AT_FIELD in sample
            else (
                sample["modified_datetime"] if "modified_datetime" in sample else None
            )
        )
        created_at = (
            sample["created_datetime"] if "created_datetime" in sample else None
        )
        # Allow push when we have no timestamps (our samples use tator_modified_at only), or when modified >= created
        mod_ts = _normalize_modified_at(modified_at)
        created_ts = _normalize_modified_at(created_at)
        allow_push = (mod_ts is None and created_ts is None) or (
            mod_ts is not None and (created_ts is None or mod_ts >= created_ts)
        )
        if allow_push:
            try:
                _update_localization_attributes(
                    api, project_id, version_id, elemental_id, attrs
                )
                sample.save()
                updated += 1
            except Exception as e:
                failed += 1
                errors.append(f"Sample {sample.id}: {e}")
        else:
            skipped += 1
            if _debug:
                logger.info(f"SKIP elem={elemental_id} unchanged since creation")
            continue

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
) -> dict[str, Any]:
    """
    Entrypoint for RQ worker: all args are serializable. Calls sync_project_to_fiftyone.
    When run in RQ worker context, attaches a handler that writes log lines to job.meta for the applet.
    """
    from src.app.database_manager import register_project_id_name

    logger.info(
        f"run_sync_job received project_id={project_id} version_id={version_id}"
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
        f"sync_project_to_fiftyone CALLED: project_id={project_id} version_id={version_id} api_url={api_url} port={port} s3_bucket={s3_bucket or 'none'}"
    )
    sess = get_session(project_id, port)
    resolved_db = (
        database_name.strip() if database_name and database_name.strip() else None
    ) or (sess.get("database_name") if sess else None)
    # Ensure resolved_db has a default value if not set
    if not resolved_db:
        resolved_db = get_database_name(project_id, port, project_name=project_name)
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
        dl_dir = _download_dir(project_id)
        localizations_path = ""
        crops = _crops_dir(project_id, version_id)
        try:
            host = api_url.rstrip("/")
            api = tator.get_api(host, token)

            # Bypass: skip expensive media + localization fetch when JSONL is fresh and count matches (unless force_sync)
            jsonl_path = _localizations_jsonl_path(project_id, version_id)
            use_cached_jsonl = False
            if not force_sync and _file_newer_than_days(jsonl_path, days=1.0):
                line_count, media_ids_from_jsonl = (
                    _localizations_jsonl_line_count_and_media_ids(jsonl_path)
                )
                api_count = _get_localization_count_from_api(
                    api,
                    project_id,
                    version_id,
                    media_ids_from_jsonl or None,
                    media_id_batch_size,
                )
                if api_count is not None and line_count == api_count:
                    use_cached_jsonl = True
                    localizations_path = jsonl_path
                    media_ids_list = media_ids_from_jsonl
                    logger.info(
                        f"Bypassing media and localization fetch: JSONL is newer than 1 day and "
                        f"line count ({line_count}) matches get_localization_count"
                    )

            if not use_cached_jsonl:
                # 1. Fetch media IDs (lightweight metadata, needed for localization query)
                logger.info(
                    f"Fetching media IDs... host={host} project_id={project_id} api_url={api_url}"
                )
                media_ids_list = fetch_project_media_ids(
                    api_url, token, project_id, version_id=version_id
                )

                # 2. Fetch localizations first (cheap metadata)
                logger.info("Fetching localizations...")
                localizations_path = fetch_and_save_localizations(
                    api,
                    project_id,
                    version_id=version_id,
                    media_ids=media_ids_list or None,
                    localization_batch_size=localization_batch_size,
                    media_id_batch_size=media_id_batch_size,
                )

            if localizations_path:
                logger.info(f"saved_localizations_path (JSONL): {localizations_path}")

            # 3. Determine which crops are missing or stale (cache miss)
            old_manifest = _load_crop_manifest(project_id, version_id)
            media_ids_needed, locs_to_crop, updated_manifest = _find_crop_cache_misses(
                localizations_jsonl_path=localizations_path,
                crops_dir=crops,
                manifest=old_manifest,
                download_dir=dl_dir,
            )

            # 4. Clean up orphaned crop files for deleted localizations
            _cleanup_deleted_crops(old_manifest, updated_manifest, crops)

            # 5. Download only media that have cache misses
            if not media_ids_list:
                logger.info(f"No media IDs for project {project_id}; skipping download")
            elif not media_ids_needed:
                logger.info(
                    f"All {len(updated_manifest)} crops are cached; skipping media download"
                )
            else:
                needed_ids = [mid for mid in media_ids_list if mid in media_ids_needed]
                logger.info(
                    f"Getting {len(needed_ids)}/{len(media_ids_list)} media objects (cache misses)..."
                )
                all_media = get_media_chunked(
                    api, project_id, needed_ids, media_id_batch_size=media_id_batch_size
                )
                if not all_media:
                    logger.info(
                        f"No Media objects returned for {len(needed_ids)} ids; skipping download"
                    )
                else:
                    logger.info(f"Downloading {len(all_media)} media to tmp...")
                    dl_dir = save_media_to_tmp(
                        api, project_id, all_media, media_ids_filter=media_ids_needed
                    )

            # 6. Crop only the cache-miss localizations
            if locs_to_crop and localizations_path:
                crop_localizations_parallel(
                    dl_dir,
                    localizations_path,
                    crops,
                    size=224,
                    locs_to_crop=locs_to_crop,
                )
            elif not locs_to_crop:
                logger.info("No crop cache misses; skipping crop step")

            # 7. Patch manifest stems from actual downloaded filenames
            _patch_manifest_stems(updated_manifest, dl_dir)

            # 8. Persist the updated manifest
            _save_crop_manifest(project_id, version_id, updated_manifest)

            # 9. Optional: two-way sync crop images with S3 (prefix = e.g. fiftyone/raw/12/v21/crops)
            if s3_bucket and os.path.isdir(crops):
                _sync_local_dir_with_s3(crops, s3_bucket, s3_crops_prefix)

            # 10. Remove downloaded media to reclaim disk space
            _cleanup_download_dir(project_id)

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
        config["source_url"] = api_url.rstrip("/")
        config["project_id"] = project_id
        config["version_id"] = version_id
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

        from database_manager import get_vss_project_config

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
            model_info = {
                "embeddings_field": embeddings_config.get(
                    "embeddings_field", "embeddings"
                ),
                "brain_key": embeddings_config.get("brain_key", "umap_viz"),
            }
            try:
                from embedding_service import is_embedding_service_available
                from embeddings_viz import compute_embeddings_and_viz, has_embeddings

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
                        f"Computing embeddings with batch size {batch_size} and UMAP for dataset '{dataset_name}'..."
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
                        f"Embeddings and UMAP completed for dataset '{dataset_name}'"
                    )
            except ImportError as e:
                logger.info(f"Skipping embeddings/UMAP (missing deps): {e}")
            except Exception as e:
                logger.info(f"Embeddings/UMAP failed (dataset still available): {e}")
                logging.getLogger(__name__).exception("Embeddings/UMAP failed")
        else:
            logger.info("No vss_project; skipping embeddings/UMAP")

        # URL for the launcher: must use FIFTYONE_APP_PUBLIC_BASE_URL so the dashboard
        # opens the correct FiftyOne app (e.g. maximilian.shore.mbari.org), always http.
        # In enterprise mode, don't append port; use the base URL directly (e.g. https://mbari.fiftyone.ai)
        public_url = os.environ.get(
            "FIFTYONE_APP_PUBLIC_BASE_URL", "http://localhost"
        ).strip()
        if get_is_enterprise():
            app_url = public_url.rstrip("/")
        else:
            app_url = f"{public_url.rstrip('/')}:{port}"
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

        # Download only media with cache misses
        if media_ids and media_ids_needed:
            needed_ids = [mid for mid in media_ids if mid in media_ids_needed]
            all_media = get_media_chunked(
                api, project_id, needed_ids, media_id_batch_size=media_id_batch_size_cli
            )
            if all_media:
                save_media_to_tmp(
                    api, project_id, all_media, media_ids_filter=media_ids_needed
                )
            else:
                logger.info("No Media objects returned; download skipped.")
        elif not media_ids_needed:
            logger.info("All crops cached; skipping media download")

        # Crop only cache misses
        if locs_to_crop:
            crop_localizations_parallel(
                dl_dir,
                localizations_path,
                crops,
                size=224,
                locs_to_crop=locs_to_crop,
            )

        # Patch manifest stems from actual downloaded filenames
        _patch_manifest_stems(updated_manifest, dl_dir)

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
        config = config
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


if __name__ == "__main__":
    main()
