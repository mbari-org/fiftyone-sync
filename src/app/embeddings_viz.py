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

# #region agent log
_DEBUG_LOG_PATH = "/Users/dcline/Dropbox/code/ai/fiftyone-sync/.cursor/debug-e530c0.log"
_DEBUG_LOG_PATH_FALLBACK = os.environ.get("DEBUG_LOG_PATH", "/tmp/debug-e530c0.log")

def _agent_log(location: str, message: str, data: dict, hypothesis_id: str = ""):
    try:
        payload = {"sessionId": "e530c0", "location": location, "message": message, "data": data, "timestamp": int(time.time() * 1000)}
        if hypothesis_id:
            payload["hypothesisId"] = hypothesis_id
        line = json.dumps(payload) + "\n"
        for path in (_DEBUG_LOG_PATH, _DEBUG_LOG_PATH_FALLBACK):
            try:
                with open(path, "a") as f:
                    f.write(line)
                break
            except Exception:
                continue
    except Exception:
        pass
# #endregion

# Base URL for embed service (POST /embed/{project}, job status via WS /ws/predict/job/{job_id}/{project})
EMBED_SERVICE_BASE_URL = os.environ.get(
    "FASTVSS_API_URL", "http://cortext.shore.mbari.org/vss"
).rstrip("/")

# Stop embedding run after this many failed fetch attempts
EMBEDDING_FETCH_MAX_RETRIES = 3

# Max time to wait for one job over WebSocket (align with Fast-VSS WS_MAX_WAIT)
_WS_JOB_TIMEOUT = 300.0


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

    # #region agent log
    origin = _ws_url_to_origin(ws_url)
    _agent_log("embeddings_viz.py:_wait_job_result_ws", "entering WebSocket connect", {"ws_url": ws_url, "origin_header": origin}, "A")
    # #endregion
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
    except InvalidStatus as e:
        # #region agent log
        r = getattr(e, "response", None) or (e.args[0] if e.args else None)
        resp_data = {}
        if r is not None:
            resp_data["response_status"] = getattr(r, "status_code", None)
            resp_data["response_reason"] = getattr(r, "reason_phrase", None)
            if hasattr(r, "headers"):
                resp_data["response_headers"] = dict(r.headers)
        _agent_log("embeddings_viz.py:_wait_job_result_ws", "WebSocket InvalidStatus (403)", {"ws_url": ws_url, "exception": str(e), **resp_data}, "B,C,D,E")
        # #endregion
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
    poll_timeout: float = 300.0,
) -> None:
    """
    Compute embeddings by sending sample images to the embed service and writing results to the dataset.

    Service: POST {service_url}/embed/{project_name} (no trailing slash) with files -> job_id
             then WebSocket {service_url}/ws/predict/job/{job_id}/{project_name} until status done/failed.
    """
    import httpx
    import numpy as np

    base = service_url.rstrip("/")
    ws_base = _service_base_to_ws(base)
    # #region agent log
    _agent_log("embeddings_viz.py:_compute_embeddings_via_service", "embed service URL state", {"base": base, "ws_base": ws_base, "service_url": service_url, "project_name": project_name}, "A")
    # #endregion
    all_samples = list(dataset.iter_samples(autosave=True))
    if not all_samples:
        return

    # Build list of (sample, filepath) pairs for samples with valid local files
    valid_samples = []
    paths_to_open = []
    for s in all_samples:
        if "local_filepath" in s:
            path_to_open = s["local_filepath"]
        else:
            continue
        if os.path.isfile(path_to_open):
            valid_samples.append(s)
            paths_to_open.append(path_to_open)

    if not valid_samples:
        logger.warning("No valid samples with local_filepath found")
        return

    logger.info(
        f"Processing embeddings for {len(valid_samples)} samples (out of {len(all_samples)} total)"
    )

    # Submit all batches, then wait for each job via WebSocket
    num_batches = (len(paths_to_open) + batch_size - 1) // batch_size
    jobs: list[tuple[int, str]] = []

    logger.info(f"Num batches {num_batches}")
    with httpx.Client(timeout=5.0) as client:
        # Phase 1: submit every batch and collect job IDs (retry each batch up to EMBEDDING_FETCH_MAX_RETRIES)
        for start in range(0, len(paths_to_open), batch_size):
            batch_idx = start // batch_size
            batch_paths = paths_to_open[start : start + batch_size]
            files = []
            for fp in batch_paths:
                with open(fp, "rb") as f:
                    files.append(("files", (os.path.basename(fp), f.read())))
            url = f"{base}/embed/{project_name}"
            last_error = None
            for attempt in range(EMBEDDING_FETCH_MAX_RETRIES):
                try:
                    logger.info(
                        f"Submitting batch {batch_idx + 1}/{num_batches}"
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
                    jobs.append((batch_idx, job_id))
                    logger.info(f"Batch {batch_idx + 1} submitted -> job {job_id}")
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"Batch {batch_idx + 1} submit attempt {attempt + 1}/{EMBEDDING_FETCH_MAX_RETRIES} failed: {e}"
                    )
            if last_error is not None:
                logger.error(
                    f"Embedding service failed after {EMBEDDING_FETCH_MAX_RETRIES} retries; stopping to avoid continuing for thousands of images. Last error: {last_error}"
                )
                raise RuntimeError(
                    f"Embedding fetch failed after {EMBEDDING_FETCH_MAX_RETRIES} retries: {last_error}"
                ) from last_error

        for batch_idx, job_id in jobs:
            ws_url = f"{ws_base}/ws/predict/job/{job_id}/{project_name}"
            # #region agent log
            _agent_log("embeddings_viz.py:ws_url_built", "WebSocket URL for job", {"ws_url": ws_url, "job_id": job_id, "project_name": project_name, "ws_base": ws_base}, "A")
            # #endregion
            logger.info(f"Using WebSocket URL: {ws_url}")
            last_error = None
            for attempt in range(EMBEDDING_FETCH_MAX_RETRIES):
                try:
                    raw_result = asyncio.run(
                        _wait_job_result_ws(ws_url, timeout=poll_timeout)
                    )
                    logger.debug(
                        f"Batch {batch_idx + 1} raw_result type: {type(raw_result).__name__}, keys: {raw_result.keys() if isinstance(raw_result, dict) else 'N/A'}"
                    )
                    if not isinstance(raw_result, dict):
                        logger.warning(
                            f"WebSocket result is not a dict (type={type(raw_result).__name__}); using as-is"
                        )
                        emb_list = raw_result if isinstance(raw_result, list) else []
                    else:
                        # Try to extract embeddings from result
                        emb_list = raw_result.get("embeddings")
                        if emb_list is None:
                            # Fallback: check if result itself is the embeddings list
                            result_field = raw_result.get("result")
                            if isinstance(result_field, list):
                                emb_list = result_field
                            elif isinstance(result_field, dict):
                                emb_list = result_field.get("embeddings")
                            else:
                                emb_list = []

                        if not emb_list:
                            logger.warning(
                                f"Batch {batch_idx + 1}: No embeddings found in result. Available keys: {list(raw_result.keys())}"
                            )
                            logger.debug(
                                f"Batch {batch_idx + 1}: raw_result content (first 500 chars): {str(raw_result)[:500]}"
                            )

                    if not emb_list:
                        logger.error(
                            f"Batch {batch_idx + 1}: Empty embeddings list received"
                        )
                    else:
                        logger.info(
                            f"Batch {batch_idx + 1}: Received {len(emb_list)} embeddings"
                        )
                        # Log the type of the first embedding for debugging
                        if len(emb_list) > 0:
                            first_emb = emb_list[0]
                            logger.debug(
                                f"Batch {batch_idx + 1}: First embedding type: {type(first_emb).__name__}, "
                                f"length: {len(first_emb) if hasattr(first_emb, '__len__') else 'N/A'}"
                            )

                    start = batch_idx * batch_size
                    end = min(start + batch_size, len(valid_samples))
                    saved_count = 0
                    for s, emb in zip(valid_samples[start:end], emb_list):
                        # Convert embedding to list if it's not already (FiftyOne requires list/tuple)
                        if isinstance(emb, np.ndarray):
                            emb = emb.tolist()
                        elif not isinstance(emb, (list, tuple)):
                            emb = list(emb)
                        s[embeddings_field] = emb
                        s.save()  # Explicitly save each sample to ensure persistence
                        saved_count += 1
                    logger.info(
                        f"Batch {batch_idx + 1}: Set embeddings for {saved_count} samples (range {start}-{end})"
                    )
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"WebSocket batch {batch_idx + 1} attempt {attempt + 1}/{EMBEDDING_FETCH_MAX_RETRIES} failed: {e}"
                    )
            if last_error is not None:
                logger.error(
                    f"Batch {batch_idx + 1} failed after {EMBEDDING_FETCH_MAX_RETRIES} attempts: {last_error}"
                )
                raise RuntimeError(
                    f"Embedding job failed: {last_error}"
                ) from last_error

    logger.info(
        f"Embeddings stored in: {embeddings_field} ({len(valid_samples)} samples)"
    )

    # Reload dataset to ensure all changes are visible
    dataset.reload()

    # Verify embeddings were actually saved
    samples_with_embeddings = dataset.exists(embeddings_field).count()
    logger.info(
        f"Verification: {samples_with_embeddings} samples have embeddings in field '{embeddings_field}'"
    )
    if samples_with_embeddings == 0:
        logger.error(
            "WARNING: No embeddings were saved! This may indicate a format mismatch in the WebSocket response."
        )
        logger.error(
            f"Dataset has {len(dataset)} total samples, {len(valid_samples)} valid samples were processed"
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
