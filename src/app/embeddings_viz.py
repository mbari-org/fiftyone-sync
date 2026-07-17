# fiftyone-sync, Apache-2.0 license
# Filename: src/app/embeddings_viz.py
# Description: Compute embeddings, UMAP visualization, and similarity search for FiftyOne datasets with caching.
"""
Compute embeddings, UMAP visualization, and optional similarity search for FiftyOne datasets with caching.

Embeddings are fetched from the embed service at {base}/embed/{project}
where project is typically the Tator project ID (sync passes str(project_id) by default; config can override).
Job status is received via WebSocket {base}/ws/predict/job/{job_id}/{project}.
UMAP requires umap-learn (see requirements.txt).
Similarity search uses fob.compute_similarity when config.embeddings.similarity_brain_key is set.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Optional

import fiftyone as fo

logger = logging.getLogger(__name__)

# Base URL for embed service (POST /embed/{project}, job status via WS /ws/predict/job/{job_id}/{project})
EMBED_SERVICE_BASE_URL = os.environ.get(
    "FASTVSS_API_URL", "http://cortext.shore.mbari.org/vss"
).rstrip("/")

# Stop embedding run after this many failed fetch attempts
EMBEDDING_FETCH_MAX_RETRIES = 3

# Default number of batches submitted to the embed service concurrently. Bounded (rather than
# submitting every batch upfront) so at most this many jobs are ever in flight at once, avoiding
# the job-TTL expiry that the old fully-sequential submit-all-then-poll-all pattern was written
# to prevent, while still parallelizing the network round-trip wait across batches.
DEFAULT_EMBEDDING_CONCURRENCY = 4

# Max time to wait for one job over WebSocket (align with Fast-VSS WS_MAX_WAIT)
_WS_JOB_TIMEOUT = 10.0

# Rough throughput estimate for the upfront ETA logged before processing starts;
# actual progress logs below use the measured rate instead once a few batches complete.
_ESTIMATED_SECONDS_PER_IMAGE = 0.05


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds as e.g. '45s', '3m 12s', or '1h 05m'."""
    seconds = max(0.0, seconds)
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, secs = divmod(int(round(seconds)), 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins:02d}m"


def _service_base_to_ws(base: str) -> str:
    """Derive WebSocket base URL from service base (http -> ws, https -> wss)."""
    base = base.rstrip("/")
    if base.startswith("https://"):
        return "wss://" + base[8:]
    if base.startswith("http://"):
        return "ws://" + base[7:]
    return "ws://" + base


def _ws_url_to_origin(ws_url: str) -> str:
    """Derive HTTP Origin from WebSocket URL (wss://host/path -> https://host). Many servers 403 WS without matching Origin."""
    from urllib.parse import urlparse
    parsed = urlparse(ws_url)
    scheme = "https" if parsed.scheme == "wss" else "http"
    netloc = parsed.netloc or parsed.path.split("/")[0] or "localhost"
    return f"{scheme}://{netloc}"


async def _wait_job_result_ws(ws_url: str, timeout: float = _WS_JOB_TIMEOUT) -> dict:
    """
    Wait for job completion via Fast-VSS WebSocket. Returns result dict on "done"; raises on "failed"/"error"/timeout.
    """
    import websockets
    from websockets.exceptions import InvalidStatus

    origin = _ws_url_to_origin(ws_url)
    deadline = time.monotonic() + timeout
    try:
        async with websockets.connect(
            ws_url,
            open_timeout=10,
            close_timeout=5,
            max_size=10 * 1024 * 1024,  # 10MB max message size (default is 1MB)
            additional_headers={"Origin": origin},
        ) as ws:
            while True:
                remaining = max(1.0, deadline - time.monotonic())
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                msg = json.loads(raw)
                status = msg.get("status")
                logger.debug(
                    f"WebSocket message: status={status}, keys={list(msg.keys())}, msg_size={len(raw)} bytes"
                )
                if status == "done":
                    result = msg.get("result") or msg
                    if isinstance(result, dict):
                        logger.debug(f"Result keys: {list(result.keys())}")
                    return result
                if status == "failed":
                    raise RuntimeError(msg.get("message", "Job failed"))
                if status == "error":
                    raise RuntimeError(msg.get("message", str(msg)))
    except InvalidStatus:
        raise


def has_embeddings(dataset: "fo.Dataset", embeddings_field: str) -> bool:
    """Return True if the dataset has the embeddings field and at least one sample has embeddings."""
    if not dataset.has_field(embeddings_field):
        return False
    return dataset.exists(embeddings_field).count() > 0


def has_brain_run(dataset: "fo.Dataset", brain_key: str) -> bool:
    """
    Return True if the dataset has a *usable* brain run with the given key.

    A run can be registered (present in ``list_brain_runs()``) but have no results
    if a prior computation crashed or was interrupted after registering the run but
    before saving results. Treat such broken runs as absent so they get recomputed
    instead of permanently blocking with "results not yet available" errors.
    """
    if brain_key not in dataset.list_brain_runs():
        return False
    try:
        dataset.load_brain_results(brain_key)
        return True
    except Exception:
        logger.warning(
            f"Brain run '{brain_key}' is registered but its results could not be "
            "loaded (likely an interrupted prior computation); deleting it so it "
            "will be recomputed"
        )
        try:
            dataset.delete_brain_run(brain_key)
        except Exception:
            logger.exception(f"Failed to delete broken brain run '{brain_key}'")
        return False


async def _submit_batch_with_retries(
    client: "httpx.AsyncClient",
    url: str,
    files: list,
    batch_label: str,
) -> str:
    """POST one batch to the embed service with retries. Returns the job_id."""
    last_error: Exception | None = None
    for attempt in range(EMBEDDING_FETCH_MAX_RETRIES):
        try:
            logger.info(
                f"Submitting {batch_label}"
                + (
                    f" (attempt {attempt + 1}/{EMBEDDING_FETCH_MAX_RETRIES})"
                    if attempt
                    else ""
                )
            )
            resp = await client.post(url, files=files)
            resp.raise_for_status()
            data = resp.json()
            err = data.get("error")
            if err:
                raise RuntimeError(f"Embed service error: {err}")
            job_id = data.get("job_id")
            if not job_id:
                raise RuntimeError(f"No job_id in response: {data}")
            logger.info(f"{batch_label} submitted -> job {job_id}")
            return job_id
        except Exception as e:
            last_error = e
            logger.warning(
                f"{batch_label} submit attempt {attempt + 1}/{EMBEDDING_FETCH_MAX_RETRIES} failed: {e}"
            )
    logger.error(
        f"Embedding service failed after {EMBEDDING_FETCH_MAX_RETRIES} retries; stopping. Last error: {last_error}"
    )
    raise RuntimeError(
        f"Embedding fetch failed after {EMBEDDING_FETCH_MAX_RETRIES} retries: {last_error}"
    ) from last_error


async def _poll_and_save_batch_with_retries(
    dataset: "fo.Dataset",
    ws_base: str,
    project_name: str,
    job_id: str,
    batch_ids: list[str],
    embeddings_field: str,
    batch_label: str,
    poll_timeout: float,
) -> int:
    """Poll the WebSocket for a job's result and save embeddings onto the given samples. Returns saved count."""
    import numpy as np

    ws_url = f"{ws_base}/ws/predict/job/{job_id}/{project_name}"
    logger.debug(f"WebSocket URL: {ws_url}")
    last_error: Exception | None = None
    for attempt in range(EMBEDDING_FETCH_MAX_RETRIES):
        try:
            raw_result = await _wait_job_result_ws(ws_url, timeout=poll_timeout)
            logger.debug(
                f"{batch_label} raw_result type: {type(raw_result).__name__}, "
                f"keys: {raw_result.keys() if isinstance(raw_result, dict) else 'N/A'}"
            )

            if not isinstance(raw_result, dict):
                logger.warning(
                    f"WebSocket result is not a dict (type={type(raw_result).__name__}); using as-is"
                )
                emb_list = raw_result if isinstance(raw_result, list) else []
            else:
                emb_list = raw_result.get("embeddings")
                if emb_list is None:
                    result_field = raw_result.get("result")
                    if isinstance(result_field, list):
                        emb_list = result_field
                    elif isinstance(result_field, dict):
                        emb_list = result_field.get("embeddings")
                    else:
                        emb_list = []
                if not emb_list:
                    logger.warning(
                        f"{batch_label}: No embeddings in result. Keys: {list(raw_result.keys())}"
                    )
                    logger.debug(
                        f"{batch_label}: raw_result (first 500 chars): {str(raw_result)[:500]}"
                    )

            if not emb_list:
                logger.error(f"{batch_label}: Empty embeddings list received")
                return 0

            logger.info(f"{batch_label}: Received {len(emb_list)} embeddings")
            first_emb = emb_list[0]
            logger.debug(
                f"{batch_label}: First embedding type: {type(first_emb).__name__}, "
                f"length: {len(first_emb) if hasattr(first_emb, '__len__') else 'N/A'}"
            )
            # Reload only this batch's samples by ID (ordered=True preserves submission order)
            batch_view = dataset.select(batch_ids, ordered=True)
            saved_count = 0
            for s, emb in zip(batch_view.iter_samples(autosave=True), emb_list):
                if isinstance(emb, np.ndarray):
                    emb = emb.tolist()
                elif not isinstance(emb, (list, tuple)):
                    emb = list(emb)
                s[embeddings_field] = emb
                saved_count += 1
            logger.info(f"{batch_label}: Saved {saved_count} embeddings")
            return saved_count
        except Exception as e:
            last_error = e
            logger.warning(
                f"WebSocket {batch_label} attempt {attempt + 1}/{EMBEDDING_FETCH_MAX_RETRIES} failed: {e}"
            )
    logger.error(f"{batch_label} failed after {EMBEDDING_FETCH_MAX_RETRIES} attempts: {last_error}")
    raise RuntimeError(f"Embedding job failed: {last_error}") from last_error


async def _compute_embeddings_via_service_async(
    dataset: "fo.Dataset",
    project_name: str,
    embeddings_field: str,
    service_url: str,
    batch_size: int,
    poll_timeout: float,
    concurrency: int,
) -> None:
    import httpx

    base = service_url.rstrip("/")
    ws_base = _service_base_to_ws(base)

    total_samples = len(dataset)
    if total_samples == 0:
        return

    # Scan once to collect (sample_id, filepath) pairs without holding all sample objects in memory.
    # For datasets with millions of samples this avoids an enormous in-memory list of FiftyOne objects.
    logger.info(f"Scanning {total_samples} samples for valid local filepaths...")
    valid_ids: list[str] = []
    valid_paths: list[str] = []
    for s in dataset.iter_samples():
        path = s["local_filepath"] if "local_filepath" in s else None
        if path and os.path.isfile(path):
            valid_ids.append(s.id)
            valid_paths.append(path)

    if not valid_ids:
        logger.warning("No valid samples with local_filepath found")
        return

    num_valid = len(valid_ids)
    num_batches = (num_valid + batch_size - 1) // batch_size
    concurrency = max(1, min(concurrency, num_batches))
    est_total_seconds = num_valid * _ESTIMATED_SECONDS_PER_IMAGE / concurrency
    logger.info(
        f"Processing embeddings for {num_valid} samples (out of {total_samples} total), "
        f"{num_batches} batches, concurrency={concurrency}; rough estimate "
        f"~{_format_duration(est_total_seconds)} total (~{_ESTIMATED_SECONDS_PER_IMAGE * 1000:.0f} ms/image)"
    )

    url = f"{base}/embed/{project_name}"
    processed = 0
    completed_batches = 0
    start_time = time.monotonic()
    semaphore = asyncio.Semaphore(concurrency)

    # Use a generous timeout for the HTTP POST: 512 images at several KB–MB each can take well over 5s.
    async with httpx.AsyncClient(timeout=10.0) as client:
        async def run_batch(batch_num: int) -> None:
            nonlocal processed, completed_batches

            start = batch_num * batch_size
            end = min(start + batch_size, num_valid)
            batch_paths = valid_paths[start:end]
            batch_ids = valid_ids[start:end]
            batch_label = f"batch {batch_num + 1}/{num_batches}"

            files = []
            for fp in batch_paths:
                with open(fp, "rb") as f:
                    files.append(("files", (os.path.basename(fp), f.read())))

            # Bounded to `concurrency` in-flight jobs at once: unlike submitting every batch
            # upfront (which leaves early jobs waiting for the slowest of thousands to be
            # polled, risking service-side TTL expiry), only a small, fixed number of jobs are
            # ever outstanding, while still parallelizing the network round-trip wait.
            async with semaphore:
                job_id = await _submit_batch_with_retries(client, url, files, batch_label)
                await _poll_and_save_batch_with_retries(
                    dataset,
                    ws_base,
                    project_name,
                    job_id,
                    batch_ids,
                    embeddings_field,
                    batch_label,
                    poll_timeout,
                )

            processed += len(batch_ids)
            completed_batches += 1
            elapsed = time.monotonic() - start_time
            # Log the first few completions (so progress is visible immediately even on
            # small/slow runs), then every 10th, to avoid flooding logs on large datasets.
            if completed_batches <= 3 or completed_batches % 10 == 0 or completed_batches == num_batches:
                pct = 100 * processed // num_valid
                rate = processed / elapsed if elapsed > 0 else 0.0
                remaining = num_valid - processed
                eta_seconds = (
                    remaining / rate if rate > 0 else remaining * _ESTIMATED_SECONDS_PER_IMAGE
                )
                logger.info(
                    f"Progress: {processed}/{num_valid} samples embedded ({pct}%) | "
                    f"{completed_batches}/{num_batches} batches done | "
                    f"elapsed {_format_duration(elapsed)} | rate {rate:.1f} img/s | "
                    f"ETA {_format_duration(eta_seconds)}"
                )

        await asyncio.gather(*(run_batch(i) for i in range(num_batches)))

    total_elapsed = time.monotonic() - start_time
    avg_rate = num_valid / total_elapsed if total_elapsed > 0 else 0.0
    summary = f"Embeddings stored in: {embeddings_field} ({num_valid} samples)"
    if avg_rate > 0:
        summary += (
            f" in {_format_duration(total_elapsed)} "
            f"(avg {avg_rate:.1f} img/s, {1000 / avg_rate:.0f} ms/image)"
        )
    logger.info(summary)

    dataset.reload()

    samples_with_embeddings = dataset.exists(embeddings_field).count()
    logger.info(
        f"Verification: {samples_with_embeddings} samples have embeddings in field '{embeddings_field}'"
    )
    if samples_with_embeddings == 0:
        logger.error(
            "WARNING: No embeddings were saved! This may indicate a format mismatch in the WebSocket response."
        )
        logger.error(
            f"Dataset has {len(dataset)} total samples, {num_valid} valid samples were processed"
        )


def _compute_embeddings_via_service(
    dataset: "fo.Dataset",
    project_name: str,
    embeddings_field: str,
    service_url: str,
    batch_size: int = 32,
    poll_timeout: float = 10.0,
    concurrency: int = DEFAULT_EMBEDDING_CONCURRENCY,
) -> None:
    """
    Compute embeddings by sending sample images to the embed service and writing results to the dataset.

    Service: POST {service_url}/embed/{project_name} (no trailing slash) with files -> job_id
             then WebSocket {service_url}/ws/predict/job/{job_id}/{project_name} until status done/failed.

    Up to `concurrency` batches are submitted and polled concurrently. This bounds the number of
    jobs in flight at once (unlike the old fully-sequential one-at-a-time pattern, and unlike
    submitting every batch upfront, which would leave early jobs waiting for 30+ minutes and risk
    service-side TTL expiry when there are thousands of batches).
    """
    asyncio.run(
        _compute_embeddings_via_service_async(
            dataset,
            project_name,
            embeddings_field,
            service_url,
            batch_size,
            poll_timeout,
            concurrency,
        )
    )


def compute_embeddings_and_viz(
    dataset: "fo.Dataset",
    model_info: dict,
    umap_seed: int = 51,
    force_embeddings: bool = False,
    force_umap: bool = False,
    batch_size: Optional[int] = None,
    project_name: Optional[str] = None,
    service_url: Optional[str] = None,
    concurrency: Optional[int] = None,
) -> None:
    """
    Compute embeddings, UMAP visualization, and optional similarity index with caching.

    Embeddings are fetched from the embed service at {service_url}/embed/{project_name},
    where project_name is the Tator project name (get_project(project_id).name).
    UMAP is computed locally and stored under brain_key.
    If model_info has similarity_brain_key, similarity search is computed via fob.compute_similarity.

    When is_enterprise is True, only local_filepath is passed to the embed service (sample filepath may be S3).

    Args:
        dataset: FiftyOne dataset
        model_info: Dict with embeddings_field, brain_key; optionally similarity_brain_key,
            similarity_metric (e.g. "cosine").
        umap_seed: Random seed for UMAP
        force_embeddings: If True, recompute embeddings even if they exist
        force_umap: If True, recompute UMAP even if it exists
        batch_size: Batch size for embed service requests (default 32)
        project_name: Project key for embed service URL path (usually project ID; required when using service)
        service_url: Base URL for embed service (default FASTVSS_API_URL or http://localhost:8000)
        concurrency: Max number of batches submitted to the embed service concurrently
            (default DEFAULT_EMBEDDING_CONCURRENCY). Higher values speed up submission at the
            cost of more simultaneous load on the embed service.
    """
    import fiftyone.brain as fob

    embeddings_field = model_info["embeddings_field"]
    brain_key = model_info["brain_key"]
    base_url = (service_url or EMBED_SERVICE_BASE_URL).rstrip("/")

    logger.info(
        f"Embeddings from service: {base_url}/embed/ | project={project_name} field={embeddings_field} "
        f"brain_key={brain_key} batch_size={batch_size} concurrency={concurrency or DEFAULT_EMBEDDING_CONCURRENCY}"
    )

    # --- Embeddings (from service) ---
    embeddings_exist = has_embeddings(dataset, embeddings_field)
    if embeddings_exist and not force_embeddings:
        logger.info(
            f"Embeddings already cached in '{embeddings_field}' - skipping computation (use force_embeddings to recompute)"
        )
    else:
        if not project_name:
            raise ValueError(
                "Embeddings from service require project_name (Tator project name from get_project(project_id).name)"
            )
        if embeddings_exist and force_embeddings:
            logger.info(
                "Force recomputing embeddings (cached embeddings will be overwritten)"
            )

        _compute_embeddings_via_service(
            dataset,
            project_name=project_name,
            embeddings_field=embeddings_field,
            service_url=base_url,
            batch_size=batch_size or 32,
            concurrency=concurrency or DEFAULT_EMBEDDING_CONCURRENCY,
        )

    # Reload so exists() and brain see the persisted embeddings
    dataset.reload()

    # Only run UMAP/similarity on samples that have embeddings (avoid empty array error)
    view_with_emb = dataset.exists(embeddings_field)
    n_with_emb = view_with_emb.count()
    if n_with_emb == 0:
        logger.warning(
            "UMAP/similarity skipped: no samples have embeddings (need at least 1). Embeddings may be missing or failed."
        )
        return

    brain_run_exists = has_brain_run(dataset, brain_key)
    if brain_run_exists and not force_umap:
        logger.info(
            f"UMAP visualization already cached with brain key '{brain_key}' - skipping computation (use force_umap to recompute)"
        )
    else:
        if brain_run_exists and force_umap:
            logger.info("Force recomputing UMAP (deleting existing brain run)")
            dataset.delete_brain_run(brain_key)

        logger.info(
            f"Computing UMAP visualization ({n_with_emb} samples with embeddings)..."
        )
        fob.compute_visualization(
            view_with_emb,
            embeddings=embeddings_field,
            brain_key=brain_key,
            method="umap",
            verbose=True,
            seed=umap_seed,
        )
        logger.info(f"Visualization stored with brain key: {brain_key}")

    # Similarity index uses a separate brain key (FiftyOne allows one run per key).
    similarity_brain_key = model_info.get("similarity_brain_key")
    similarity_metric = model_info.get("similarity_metric", "cosine")
    if similarity_brain_key:
        sim_run_exists = has_brain_run(dataset, similarity_brain_key)
        if sim_run_exists and not force_umap:
            logger.info(
                f"Similarity index already cached with brain key '{similarity_brain_key}' - skipping (use force_umap to recompute)"
            )
        else:
            if sim_run_exists and force_umap:
                logger.info(
                    "Force recomputing similarity (deleting existing brain run)"
                )
                dataset.delete_brain_run(similarity_brain_key)
            logger.info(
                f"Computing similarity index ({n_with_emb} samples, metric={similarity_metric})..."
            )
            fob.compute_similarity(
                view_with_emb,
                embeddings=embeddings_field,
                metric=similarity_metric,
                brain_key=similarity_brain_key,
            )
            logger.info(f"Similarity stored with brain key: {similarity_brain_key}")


def compute_dimensionality_reduction(
    dataset: "fo.Dataset",
    *,
    embeddings_field: str,
    brain_key: str,
    method: str,
    seed: int = 51,
    num_dims: int = 2,
    force: bool = True,
) -> None:
    """
    Compute (or recompute) a dimensionality reduction visualization from existing embeddings.

    This intentionally does *not* touch embeddings or similarity indexes; it only deletes/recomputes
    the FiftyOne brain run specified by ``brain_key``.
    """
    import fiftyone.brain as fob

    method = (method or "").strip().lower()
    if method not in {"pca", "tsne", "umap"}:
        raise ValueError(
            f"Unsupported dimensionality reduction method={method!r}. Expected one of: 'pca', 'tsne', 'umap'."
        )

    if not dataset.has_field(embeddings_field):
        raise ValueError(
            f"Dataset does not have embeddings field: {embeddings_field!r}"
        )

    # Use a view with only samples that actually have embeddings (avoid empty-array errors)
    view_with_emb = dataset.exists(embeddings_field)
    n_with_emb = view_with_emb.count()
    if n_with_emb == 0:
        raise ValueError(
            f"No samples in dataset have non-empty embeddings in field: {embeddings_field!r}"
        )

    brain_run_exists = has_brain_run(dataset, brain_key)
    if brain_run_exists and not force:
        logger.info(
            f"Dimensionality reduction already cached (brain_key={brain_key!r}); skipping (set force=True to recompute)"
        )
        return

    if brain_run_exists and force:
        logger.info(f"Deleting existing brain run brain_key={brain_key!r}")
        dataset.delete_brain_run(brain_key)

    logger.info(
        "Computing dimensionality reduction "
        f"(method={method!r}, brain_key={brain_key!r}, embeddings_field={embeddings_field!r}, n_samples={n_with_emb}, num_dims={num_dims})"
    )

    compute_kwargs = dict(
        embeddings=embeddings_field,
        brain_key=brain_key,
        method=method,
        verbose=True,
        num_dims=num_dims,
    )

    # FiftyOne's compute_visualization accepts seed for methods that rely on randomness.
    if method in {"tsne", "umap"}:
        compute_kwargs["seed"] = seed

    fob.compute_visualization(view_with_emb, **compute_kwargs)
