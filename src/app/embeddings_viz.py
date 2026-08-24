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

from src.app.embedding_service import (
    describe_error,
    fastvss_http_timeout_seconds,
    fastvss_ws_connect_timeout_seconds,
    fastvss_ws_idle_timeout_seconds,
    fastvss_ws_job_url,
    fastvss_ws_max_wait_seconds,
)

logger = logging.getLogger(__name__)

# Base URL for embed service (POST /embed/{project}, job status via WS /ws/predict/job/{job_id}/{project})
EMBED_SERVICE_BASE_URL = os.environ.get(
    "FASTVSS_API_URL", "http://cortext.shore.mbari.org/vss"
).rstrip("/")

# Stop retrying a single batch after this many failed attempts. Exhausting these fails only
# that batch; the rest of the run continues (see _compute_embeddings_via_service_async).
EMBEDDING_FETCH_MAX_RETRIES = 3

# Attempt N waits EMBEDDING_RETRY_BACKOFF_SECONDS * 2**(N-1) before retrying. Retrying a
# submit instantly, as this used to, just piles more load onto a service that is already
# struggling -- which is usually why the first attempt failed.
EMBEDDING_RETRY_BACKOFF_SECONDS = 2.0

# Default number of batches submitted to the embed service concurrently. Bounded (rather than
# submitting every batch upfront) so at most this many jobs are ever in flight at once, avoiding
# the job-TTL expiry that the old fully-sequential submit-all-then-poll-all pattern was written
# to prevent, while still parallelizing the network round-trip wait across batches.
DEFAULT_EMBEDDING_CONCURRENCY = 4


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


def _job_budget_message(timeout: float, frames: int) -> str:
    """Message for a job that heartbeated healthily but never finished in its budget."""
    return (
        f"job did not finish within {timeout:.0f}s "
        f"(received {frames} status frames; raise FASTVSS_WS_MAX_WAIT if the embed "
        "service is simply backed up)"
    )


async def _wait_job_result_ws(
    ws_url: str,
    timeout: float | None = None,
    idle_timeout: float | None = None,
) -> dict:
    """
    Wait for job completion via Fast-VSS WebSocket. Returns result dict on "done"; raises on
    "failed"/"error"/timeout.

    Two *independent* limits apply, and keeping them separate is the whole point:

    * ``timeout`` -- total wall-clock budget for the job (default ``FASTVSS_WS_MAX_WAIT``).
      Fast-VSS processes jobs on one serial RQ worker per project, so a job queued behind
      others spends nearly all of this budget legitimately in the "pending" state.
    * ``idle_timeout`` -- how long to wait for *any* frame before declaring the stream dead
      (default ``FASTVSS_WS_IDLE_TIMEOUT``). Fast-VSS heartbeats "pending" every 0.5s, so
      this only trips when the socket or the service is actually wedged.

    Waiting on ``max(1.0, deadline - now)`` -- as this previously did -- collapses the two:
    once the total budget elapses the floor pins the per-recv wait at one second, silently
    converting a job deadline into a 1s inter-frame watchdog with only a 0.5s margin over the
    server's heartbeat. Any brief event-loop or server stall then killed a perfectly healthy
    batch, and because ``str(asyncio.TimeoutError())`` is ``""`` the resulting log line ended
    in a bare colon. Both failure modes raise ``TimeoutError`` with a real message now.
    """
    import websockets

    if timeout is None:
        timeout = fastvss_ws_max_wait_seconds()
    if idle_timeout is None:
        idle_timeout = fastvss_ws_idle_timeout_seconds()

    origin = _ws_url_to_origin(ws_url)
    start = time.monotonic()
    deadline = start + timeout
    frames = 0

    async with websockets.connect(
        ws_url,
        open_timeout=fastvss_ws_connect_timeout_seconds(),
        close_timeout=5,
        max_size=10 * 1024 * 1024,  # 10MB max message size (default is 1MB)
        additional_headers={"Origin": origin},
    ) as ws:
        while True:
            now = time.monotonic()
            budget_left = deadline - now
            if budget_left <= 0:
                raise TimeoutError(_job_budget_message(timeout, frames))
            # Never wait longer than whichever limit is nearer, but attribute the timeout to
            # the right one afterwards: near the end of the budget `budget_left` is a sliver,
            # so a bare "stalled" message here would blame the connection for what is really
            # the job deadline expiring.
            wait_next = min(idle_timeout, budget_left)
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=wait_next)
            except (TimeoutError, asyncio.TimeoutError) as e:
                if time.monotonic() >= deadline:
                    raise TimeoutError(_job_budget_message(timeout, frames)) from e
                raise TimeoutError(
                    f"no message from embed service for {wait_next:.1f}s after "
                    f"{now - start:.0f}s (received {frames} status frames); "
                    "connection or service appears stalled"
                ) from e
            frames += 1
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


def has_embeddings(dataset: "fo.Dataset", embeddings_field: str) -> bool:
    """Return True if the dataset has the embeddings field and at least one sample has embeddings."""
    if not dataset.has_field(embeddings_field):
        return False
    return dataset.exists(embeddings_field).count() > 0


def count_embeddings(dataset: "fo.Dataset", embeddings_field: str) -> int:
    """Return the number of samples that currently have a non-empty embedding stored."""
    if not dataset.has_field(embeddings_field):
        return 0
    return dataset.exists(embeddings_field).count()


def load_embeddings_array(view, embeddings_field: str):
    """
    Read the stored embeddings from Voxel51/FiftyOne into a NumPy array.

    Returns ``(sample_ids, embeddings)`` where ``sample_ids`` is a list of sample IDs and
    ``embeddings`` is an ``(n_samples, dim)`` float32 array in the same order. Only samples with a
    non-empty embedding in ``embeddings_field`` are included. Both lists are aligned so the i-th row
    of the array corresponds to the i-th sample id.
    """
    import numpy as np

    emb_view = view.exists(embeddings_field)
    ids = emb_view.values("id")
    raw = emb_view.values(embeddings_field)

    ids_out: list[str] = []
    vectors: list = []
    for sid, emb in zip(ids, raw):
        if emb is None:
            continue
        ids_out.append(sid)
        vectors.append(emb)

    if not vectors:
        return ids_out, np.empty((0, 0), dtype=np.float32)

    return ids_out, np.asarray(vectors, dtype=np.float32)


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
    """POST one batch to the embed service with retries and backoff. Returns the job_id."""
    last_error: Exception | None = None
    for attempt in range(1, EMBEDDING_FETCH_MAX_RETRIES + 1):
        try:
            logger.info(
                f"Submitting {batch_label}"
                + (
                    f" (attempt {attempt}/{EMBEDDING_FETCH_MAX_RETRIES})"
                    if attempt > 1
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
                f"{batch_label} submit attempt {attempt}/{EMBEDDING_FETCH_MAX_RETRIES} "
                f"failed: {describe_error(e)}"
            )
            if attempt < EMBEDDING_FETCH_MAX_RETRIES:
                await asyncio.sleep(EMBEDDING_RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1))
    raise RuntimeError(
        f"submit failed after {EMBEDDING_FETCH_MAX_RETRIES} attempts: "
        f"{describe_error(last_error)}"
    ) from last_error


async def _poll_and_save_batch_with_retries(
    dataset: "fo.Dataset",
    ws_base: str,
    project_name: str,
    job_id: str,
    batch_ids: list[str],
    embeddings_field: str,
    batch_label: str,
    poll_timeout: float | None = None,
    *,
    save_lock: asyncio.Lock | None = None,
) -> int:
    """
    Poll the WebSocket for a job's result and save embeddings onto the given samples.
    Returns saved count.

    ``save_lock``, when given, serializes the write-back across concurrent batches. The write
    itself runs in a worker thread so it does not stall the event loop (and with it every
    other batch's WebSocket), but FiftyOne datasets are not thread-safe, so only one batch
    may be writing at a time.
    """
    import numpy as np

    ws_url = fastvss_ws_job_url(ws_base, job_id, project_name)
    logger.debug(f"WebSocket URL: {ws_url}")
    last_error: Exception | None = None
    for attempt in range(1, EMBEDDING_FETCH_MAX_RETRIES + 1):
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
            def _save(emb_list=emb_list) -> int:
                # Reload only this batch's samples by ID (ordered=True preserves submission order)
                batch_view = dataset.select(batch_ids, ordered=True)
                saved = 0
                for s, emb in zip(batch_view.iter_samples(autosave=True), emb_list):
                    if isinstance(emb, np.ndarray):
                        emb = emb.tolist()
                    elif not isinstance(emb, (list, tuple)):
                        emb = list(emb)
                    s[embeddings_field] = emb
                    saved += 1
                return saved

            # Off the event loop: these are blocking Mongo writes, and holding the loop here
            # starves every other batch's WebSocket reader.
            if save_lock is not None:
                async with save_lock:
                    saved_count = await asyncio.to_thread(_save)
            else:
                saved_count = await asyncio.to_thread(_save)
            logger.info(f"{batch_label}: Saved {saved_count} embeddings")
            return saved_count
        except Exception as e:
            last_error = e
            logger.warning(
                f"WebSocket {batch_label} attempt {attempt}/{EMBEDDING_FETCH_MAX_RETRIES} "
                f"failed: {describe_error(e)}"
            )
            if attempt < EMBEDDING_FETCH_MAX_RETRIES:
                await asyncio.sleep(EMBEDDING_RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1))
    raise RuntimeError(
        f"poll failed after {EMBEDDING_FETCH_MAX_RETRIES} attempts: "
        f"{describe_error(last_error)}"
    ) from last_error


def _read_batch_files(batch_paths: list[str]) -> list:
    """
    Read one batch's crops into httpx multipart tuples. Runs in a worker thread.

    Kept off the event loop deliberately: with every batch coroutine started at once by
    asyncio.gather, doing these reads inline let dozens of coroutines block the loop with
    synchronous disk I/O while the first few already held live WebSockets that nobody was
    draining -- which is what produced the bursts of spurious timeouts at the start of a run.
    """
    files = []
    for fp in batch_paths:
        with open(fp, "rb") as f:
            files.append(("files", (os.path.basename(fp), f.read())))
    return files


async def _compute_embeddings_via_service_async(
    dataset: "fo.Dataset",
    project_name: str,
    embeddings_field: str,
    service_url: str,
    batch_size: int,
    poll_timeout: float | None = None,
    concurrency: int = DEFAULT_EMBEDDING_CONCURRENCY,
    skip_existing: bool = True,
) -> int:
    """Compute embeddings via the embed service. Returns the number of newly saved embeddings.

    When ``skip_existing`` is True, samples that already have a non-empty embedding in
    ``embeddings_field`` are left untouched, so only the missing ones are (re)computed.

    A batch that exhausts its retries fails *that batch only*: the run continues, whatever
    succeeded is saved, and the next sync picks up the samples still missing embeddings
    (``skip_existing``). Only a run in which every batch failed raises.
    """
    import httpx

    base = service_url.rstrip("/")
    ws_base = _service_base_to_ws(base)

    total_samples = len(dataset)
    if total_samples == 0:
        return 0

    # Pre-fetch the IDs that already have embeddings in one aggregation so the scan below can
    # skip them without reading each vector. When skip_existing is False we recompute everything.
    existing_ids: set[str] = set()
    if skip_existing and dataset.has_field(embeddings_field):
        existing_ids = set(dataset.exists(embeddings_field).values("id"))
        if existing_ids:
            logger.info(
                f"{len(existing_ids)}/{total_samples} samples already have embeddings in "
                f"'{embeddings_field}'; only missing samples will be computed"
            )

    # Scan once to collect (sample_id, filepath) pairs without holding all sample objects in memory.
    # For datasets with millions of samples this avoids an enormous in-memory list of FiftyOne objects.
    logger.info(f"Scanning {total_samples} samples for valid local filepaths...")
    valid_ids: list[str] = []
    valid_paths: list[str] = []
    skipped_existing = 0
    for s in dataset.iter_samples():
        if skip_existing and s.id in existing_ids:
            skipped_existing += 1
            continue
        path = s["local_filepath"] if "local_filepath" in s else None
        if path and os.path.isfile(path):
            valid_ids.append(s.id)
            valid_paths.append(path)

    if not valid_ids:
        if skipped_existing:
            logger.info(
                f"All {skipped_existing} eligible samples already have embeddings; "
                "nothing to compute"
            )
        else:
            logger.warning("No valid samples with local_filepath found")
        return 0

    num_valid = len(valid_ids)
    num_batches = (num_valid + batch_size - 1) // batch_size
    concurrency = max(1, min(concurrency, num_batches))
    if poll_timeout is None:
        poll_timeout = fastvss_ws_max_wait_seconds()
    http_timeout = fastvss_http_timeout_seconds()
    est_total_seconds = num_valid * _ESTIMATED_SECONDS_PER_IMAGE / concurrency
    logger.info(
        f"Processing embeddings for {num_valid} samples needing computation "
        f"(out of {total_samples} total, {skipped_existing} already had embeddings), "
        f"{num_batches} batches, concurrency={concurrency}; rough estimate "
        f"~{_format_duration(est_total_seconds)} total (~{_ESTIMATED_SECONDS_PER_IMAGE * 1000:.0f} ms/image)"
    )
    logger.info(
        f"Embed service timeouts: job={poll_timeout:.0f}s "
        f"idle={fastvss_ws_idle_timeout_seconds():.0f}s http={http_timeout:.0f}s "
        f"(FASTVSS_WS_MAX_WAIT / FASTVSS_WS_IDLE_TIMEOUT / FASTVSS_HTTP_TIMEOUT)"
    )

    url = f"{base}/embed/{project_name}"
    processed = 0
    completed_batches = 0
    start_time = time.monotonic()
    semaphore = asyncio.Semaphore(concurrency)
    # Serializes the Mongo write-back across batches (FiftyOne datasets are not thread-safe)
    # while still keeping it off the event loop. Created per run: an asyncio primitive binds to
    # the loop that first awaits it, and each sync run gets a fresh loop via asyncio.run().
    save_lock = asyncio.Lock()

    # A batch is `batch_size` crops (512 in the shipped config), so one multipart POST can be
    # hundreds of MB against a service that is concurrently serving other batches. The old flat
    # 10s covered neither that upload nor a busy server, and an httpx timeout stringifies to ""
    # -- so it surfaced as "submit attempt 1/3 failed:" with no reason given.
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(http_timeout, connect=15.0)
    ) as client:
        async def run_batch(batch_num: int) -> None:
            nonlocal processed, completed_batches

            start = batch_num * batch_size
            end = min(start + batch_size, num_valid)
            batch_paths = valid_paths[start:end]
            batch_ids = valid_ids[start:end]
            batch_label = f"batch {batch_num + 1}/{num_batches}"

            # Bounded to `concurrency` in-flight jobs at once: unlike submitting every batch
            # upfront (which leaves early jobs waiting for the slowest of thousands to be
            # polled, risking service-side TTL expiry), only a small, fixed number of jobs are
            # ever outstanding, while still parallelizing the network round-trip wait.
            #
            # The crops are read *inside* the semaphore, and in a thread. Reading them before
            # acquiring it (as this used to) meant all N batch coroutines raced to load their
            # images the moment gather started, blocking the event loop with synchronous disk
            # I/O while the first `concurrency` batches already had live WebSockets going
            # unread -- the direct cause of the timeout bursts at the start of a run. It also
            # held every batch's bytes in memory at once instead of `concurrency` batches'.
            async with semaphore:
                files = await asyncio.to_thread(_read_batch_files, batch_paths)
                job_id = await _submit_batch_with_retries(client, url, files, batch_label)
                del files  # release this batch's image bytes before the (possibly long) poll
                await _poll_and_save_batch_with_retries(
                    dataset,
                    ws_base,
                    project_name,
                    job_id,
                    batch_ids,
                    embeddings_field,
                    batch_label,
                    poll_timeout,
                    save_lock=save_lock,
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

        # return_exceptions=True is load-bearing, not defensive. Without it the first batch
        # to exhaust its retries propagates immediately out of gather, which exits this
        # `async with` and closes the httpx client while dozens of sibling batches are still
        # running -- so they all die with "Cannot send a request, as the client has been
        # closed" and retry three times against a client that can never work again. One bad
        # batch took down the whole run. Now gather waits for every batch, the client stays
        # open until they are all done, and failures are tallied below.
        results = await asyncio.gather(
            *(run_batch(i) for i in range(num_batches)), return_exceptions=True
        )

    failures = [
        (i + 1, r) for i, r in enumerate(results) if isinstance(r, BaseException)
    ]
    if failures:
        logger.error(
            f"{len(failures)}/{num_batches} embedding batches failed; "
            f"{processed}/{num_valid} embeddings were saved. Re-running sync will retry "
            "only the samples still missing embeddings."
        )
        for batch_num, exc in failures[:10]:
            logger.error(f"  batch {batch_num}/{num_batches}: {describe_error(exc)}")
        if len(failures) > 10:
            logger.error(f"  ... and {len(failures) - 10} more")
        if processed == 0:
            raise RuntimeError(
                f"all {num_batches} embedding batches failed; last error: "
                f"{describe_error(failures[-1][1])}"
            )

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

    return processed


def _compute_embeddings_via_service(
    dataset: "fo.Dataset",
    project_name: str,
    embeddings_field: str,
    service_url: str,
    batch_size: int = 32,
    poll_timeout: float | None = None,
    concurrency: int = DEFAULT_EMBEDDING_CONCURRENCY,
    skip_existing: bool = True,
) -> int:
    """
    Compute embeddings by sending sample images to the embed service and writing results to the dataset.

    Service: POST {service_url}/embed/{project_name} (no trailing slash) with files -> job_id
             then WebSocket {service_url}/ws/predict/job/{job_id}/{project_name} until status done/failed.

    Up to `concurrency` batches are submitted and polled concurrently. This bounds the number of
    jobs in flight at once (unlike the old fully-sequential one-at-a-time pattern, and unlike
    submitting every batch upfront, which would leave early jobs waiting for 30+ minutes and risk
    service-side TTL expiry when there are thousands of batches).

    When ``skip_existing`` is True (default), samples that already have an embedding are skipped so
    only the missing ones are computed. Returns the number of newly saved embeddings.

    ``poll_timeout`` is the total budget for one job; None resolves it from
    ``FASTVSS_WS_MAX_WAIT`` at call time. It used to default to a flat 10s here and was never
    passed by ``compute_embeddings_and_viz``, so ``FASTVSS_WS_MAX_WAIT`` never reached this
    path at all despite being documented as the knob for it.
    """
    return asyncio.run(
        _compute_embeddings_via_service_async(
            dataset,
            project_name,
            embeddings_field,
            service_url,
            batch_size,
            poll_timeout,
            concurrency,
            skip_existing,
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
    poll_timeout: Optional[float] = None,
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
        poll_timeout: Total seconds to wait for one embed job (default: FASTVSS_WS_MAX_WAIT).
    """
    import fiftyone.brain as fob

    embeddings_field = model_info["embeddings_field"]
    brain_key = model_info["brain_key"]
    base_url = (service_url or EMBED_SERVICE_BASE_URL).rstrip("/")

    logger.info(
        f"Embeddings from service: {base_url}/embed/ | project={project_name} field={embeddings_field} "
        f"brain_key={brain_key} batch_size={batch_size} concurrency={concurrency or DEFAULT_EMBEDDING_CONCURRENCY} "
        f"poll_timeout={poll_timeout or fastvss_ws_max_wait_seconds():.0f}s"
    )

    # --- Embeddings (from service) ---
    # Count what already exists so we can (a) skip when fully covered and (b) recompute only the
    # missing samples otherwise, rather than re-embedding the whole dataset every run.
    total_samples = len(dataset)
    existing_count = count_embeddings(dataset, embeddings_field)
    newly_computed = 0
    if existing_count >= total_samples > 0 and not force_embeddings:
        logger.info(
            f"All {total_samples} samples already have embeddings in '{embeddings_field}' - "
            "skipping computation (use force_embeddings to recompute)"
        )
    else:
        if not project_name:
            raise ValueError(
                "Embeddings from service require project_name (Tator project name from get_project(project_id).name)"
            )
        if force_embeddings:
            logger.info(
                "Force recomputing embeddings for all samples (cached embeddings will be overwritten)"
            )
        else:
            logger.info(
                f"{existing_count}/{total_samples} samples already have embeddings; "
                "computing only the missing ones"
            )

        newly_computed = _compute_embeddings_via_service(
            dataset,
            project_name=project_name,
            embeddings_field=embeddings_field,
            service_url=base_url,
            batch_size=batch_size or 32,
            poll_timeout=poll_timeout,
            concurrency=concurrency or DEFAULT_EMBEDDING_CONCURRENCY,
            skip_existing=not force_embeddings,
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

    # New embeddings were added, so any existing UMAP/similarity run is stale and must be rebuilt
    # to include the new points, even if force_umap was not requested.
    embeddings_changed = newly_computed > 0
    if embeddings_changed:
        logger.info(
            f"{newly_computed} new embeddings computed; UMAP/similarity will be recomputed to include them"
        )

    # Read the stored embeddings out of Voxel51 into an array once, and reuse it for both UMAP and
    # similarity instead of having FiftyOne re-read the field from the DB for each.
    sample_ids, embeddings_array = load_embeddings_array(dataset, embeddings_field)
    logger.info(
        f"Loaded {embeddings_array.shape[0]} embeddings "
        f"(dim={embeddings_array.shape[1] if embeddings_array.ndim == 2 and embeddings_array.size else 0}) "
        f"from '{embeddings_field}' into memory for UMAP/similarity"
    )
    if embeddings_array.size == 0:
        logger.warning(
            "UMAP/similarity skipped: loaded embeddings array is empty."
        )
        return

    brain_run_exists = has_brain_run(dataset, brain_key)
    if brain_run_exists and not (force_umap or embeddings_changed):
        logger.info(
            f"UMAP visualization already cached with brain key '{brain_key}' - skipping computation (use force_umap to recompute)"
        )
    else:
        if brain_run_exists:
            logger.info("Recomputing UMAP (deleting existing brain run)")
            dataset.delete_brain_run(brain_key)

        logger.info(
            f"Computing UMAP visualization ({embeddings_array.shape[0]} embeddings)..."
        )
        fob.compute_visualization(
            view_with_emb,
            embeddings=embeddings_array,
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
        if sim_run_exists and not (force_umap or embeddings_changed):
            logger.info(
                f"Similarity index already cached with brain key '{similarity_brain_key}' - skipping (use force_umap to recompute)"
            )
        else:
            if sim_run_exists:
                logger.info(
                    "Recomputing similarity (deleting existing brain run)"
                )
                dataset.delete_brain_run(similarity_brain_key)
            logger.info(
                f"Computing similarity index ({embeddings_array.shape[0]} embeddings, metric={similarity_metric})..."
            )
            fob.compute_similarity(
                view_with_emb,
                embeddings=embeddings_array,
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
