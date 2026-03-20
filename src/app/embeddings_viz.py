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

# Max time to wait for one job over WebSocket (align with Fast-VSS WS_MAX_WAIT)
_WS_JOB_TIMEOUT = 10.0


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
    """Return True if the dataset has a brain run with the given key."""
    return brain_key in dataset.list_brain_runs()


def _compute_embeddings_via_service(
    dataset: "fo.Dataset",
    project_name: str,
    embeddings_field: str,
    service_url: str,
    batch_size: int = 32,
    poll_timeout: float = 10.0,
) -> None:
    """
    Compute embeddings by sending sample images to the embed service and writing results to the dataset.

    Service: POST {service_url}/embed/{project_name} (no trailing slash) with files -> job_id
             then WebSocket {service_url}/ws/predict/job/{job_id}/{project_name} until status done/failed.

    Each batch is submitted and its WebSocket result is collected immediately before the next batch is
    submitted.  This prevents jobs from expiring on the service side (TTL) when there are thousands of
    batches and the old "submit-all-then-poll-all" pattern would leave early jobs waiting for 30+ minutes.
    """
    import httpx
    import numpy as np

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
    logger.info(
        f"Processing embeddings for {num_valid} samples (out of {total_samples} total), {num_batches} batches"
    )

    url = f"{base}/embed/{project_name}"
    processed = 0

    # Use a generous timeout for the HTTP POST: 512 images at several KB–MB each can take well over 5s.
    with httpx.Client(timeout=10.0) as client:
        for batch_num in range(num_batches):
            start = batch_num * batch_size
            end = min(start + batch_size, num_valid)
            batch_paths = valid_paths[start:end]
            batch_ids = valid_ids[start:end]

            files = []
            for fp in batch_paths:
                with open(fp, "rb") as f:
                    files.append(("files", (os.path.basename(fp), f.read())))

            # --- Submit batch (with retries) ---
            job_id: str | None = None
            last_error: Exception | None = None
            for attempt in range(EMBEDDING_FETCH_MAX_RETRIES):
                try:
                    logger.info(
                        f"Submitting batch {batch_num + 1}/{num_batches}"
                        + (
                            f" (attempt {attempt + 1}/{EMBEDDING_FETCH_MAX_RETRIES})"
                            if attempt
                            else ""
                        )
                    )
                    resp = client.post(url, files=files)
                    resp.raise_for_status()
                    data = resp.json()
                    err = data.get("error")
                    if err:
                        raise RuntimeError(f"Embed service error: {err}")
                    job_id = data.get("job_id")
                    if not job_id:
                        raise RuntimeError(f"No job_id in response: {data}")
                    logger.info(f"Batch {batch_num + 1} submitted -> job {job_id}")
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"Batch {batch_num + 1} submit attempt {attempt + 1}/{EMBEDDING_FETCH_MAX_RETRIES} failed: {e}"
                    )
            if last_error is not None:
                logger.error(
                    f"Embedding service failed after {EMBEDDING_FETCH_MAX_RETRIES} retries; stopping. Last error: {last_error}"
                )
                raise RuntimeError(
                    f"Embedding fetch failed after {EMBEDDING_FETCH_MAX_RETRIES} retries: {last_error}"
                ) from last_error

            # --- Immediately poll WebSocket for this batch's result ---
            # Polling right after submission prevents job TTL expiry that occurs when all batches
            # are queued first and only then polled (e.g. 2000+ batches x submit time >> service TTL).
            ws_url = f"{ws_base}/ws/predict/job/{job_id}/{project_name}"
            logger.debug(f"WebSocket URL: {ws_url}")
            last_error = None
            for attempt in range(EMBEDDING_FETCH_MAX_RETRIES):
                try:
                    raw_result = asyncio.run(
                        _wait_job_result_ws(ws_url, timeout=poll_timeout)
                    )
                    logger.debug(
                        f"Batch {batch_num + 1} raw_result type: {type(raw_result).__name__}, "
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
                                f"Batch {batch_num + 1}: No embeddings in result. Keys: {list(raw_result.keys())}"
                            )
                            logger.debug(
                                f"Batch {batch_num + 1}: raw_result (first 500 chars): {str(raw_result)[:500]}"
                            )

                    if not emb_list:
                        logger.error(f"Batch {batch_num + 1}: Empty embeddings list received")
                    else:
                        logger.info(
                            f"Batch {batch_num + 1}: Received {len(emb_list)} embeddings"
                        )
                        if emb_list:
                            first_emb = emb_list[0]
                            logger.debug(
                                f"Batch {batch_num + 1}: First embedding type: {type(first_emb).__name__}, "
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
                        logger.info(
                            f"Batch {batch_num + 1}: Saved {saved_count} embeddings (samples {start}–{end - 1})"
                        )

                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"WebSocket batch {batch_num + 1} attempt {attempt + 1}/{EMBEDDING_FETCH_MAX_RETRIES} failed: {e}"
                    )
            if last_error is not None:
                logger.error(
                    f"Batch {batch_num + 1} failed after {EMBEDDING_FETCH_MAX_RETRIES} attempts: {last_error}"
                )
                raise RuntimeError(f"Embedding job failed: {last_error}") from last_error

            processed += len(batch_ids)
            if batch_num % 10 == 0 or batch_num == num_batches - 1:
                pct = 100 * processed // num_valid
                logger.info(f"Progress: {processed}/{num_valid} samples embedded ({pct}%)")

    logger.info(f"Embeddings stored in: {embeddings_field} ({num_valid} samples)")

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


def compute_embeddings_and_viz(
    dataset: "fo.Dataset",
    model_info: dict,
    umap_seed: int = 51,
    force_embeddings: bool = False,
    force_umap: bool = False,
    batch_size: Optional[int] = None,
    project_name: Optional[str] = None,
    service_url: Optional[str] = None,
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
    """
    import fiftyone.brain as fob

    embeddings_field = model_info["embeddings_field"]
    brain_key = model_info["brain_key"]
    base_url = (service_url or EMBED_SERVICE_BASE_URL).rstrip("/")

    logger.info(
        f"Embeddings from service: {base_url}/embed/ | project={project_name} field={embeddings_field} brain_key={brain_key} batch_size={batch_size}"
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
