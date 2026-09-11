# fiftyone-sync, Apache-2.0 license
# Filename: src/app/embedding_service.py
# Description: Delegates to Fast-VSS API for batch image embeddings.
"""
Embedding service: delegates to Fast-VSS API for batch image embeddings.
Fast-VSS: POST /embeddings/{project}/ with files -> job_id -> status via WebSocket /ws/predict/job/{job_id}/{project}
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any
from urllib.parse import quote, urlparse

import httpx
import websockets

logger = logging.getLogger(__name__)

_url = os.environ.get("FASTVSS_API_URL")
FASTVSS_BASE_URL = _url.strip().rstrip("/") if _url else None

# Our job_id -> (fastvss_job_id, project)
_job_map: dict[str, tuple[str, str]] = {}
_queue_results: dict[str, dict[str, Any]] = {}
_queue_lock = asyncio.Lock()

# Align with Fast-VSS WS_MAX_WAIT (max time to wait for job result over WebSocket)
def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if raw:
        try:
            return max(1.0, float(raw))
        except ValueError:
            pass
    return default


def fastvss_ws_max_wait_seconds() -> float:
    """
    Total wall-clock budget for one Fast-VSS job (FASTVSS_WS_MAX_WAIT, default 1800).

    This is a *job* deadline, not an inter-message one: Fast-VSS runs a single serial RQ
    worker per project, so a job submitted while `concurrency` others are queued ahead of it
    legitimately spends nearly all of this budget in the "pending" state. 300s was too tight
    for that once more than a couple of batches were in flight.
    """
    return _env_float("FASTVSS_WS_MAX_WAIT", 1800.0)


def fastvss_ws_idle_timeout_seconds() -> float:
    """
    Max seconds to wait for *any* frame before treating the stream as dead
    (FASTVSS_WS_IDLE_TIMEOUT, default 120).

    Fast-VSS heartbeats a {"status": "pending"} frame every WS_POLL_INTERVAL (0.5s by
    default), so a gap this long means the socket or the service is genuinely wedged rather
    than merely busy. Deliberately far above the heartbeat interval: a momentary event-loop
    or network stall must never kill an otherwise healthy job.
    """
    return _env_float("FASTVSS_WS_IDLE_TIMEOUT", 120.0)


def fastvss_ws_connect_timeout_seconds() -> float:
    """WebSocket handshake timeout (FASTVSS_WS_CONNECT_TIMEOUT, default 30)."""
    return _env_float("FASTVSS_WS_CONNECT_TIMEOUT", 30.0)


def fastvss_http_timeout_seconds() -> float:
    """
    Read/write timeout for the multipart POST that submits a batch
    (FASTVSS_HTTP_TIMEOUT, default 300).

    One request carries a whole batch of crops (512 in the shipped config), so the upload
    alone can run to hundreds of megabytes against a service that is concurrently handling
    other batches.
    """
    return _env_float("FASTVSS_HTTP_TIMEOUT", 300.0)


def fastvss_ws_test_timeout_seconds() -> float:
    """Max seconds for /vss-embedding/ws-test to wait for a fake job (FASTVSS_WS_TEST_TIMEOUT, default 120)."""
    return _env_float("FASTVSS_WS_TEST_TIMEOUT", 120.0)


def describe_error(exc: BaseException) -> str:
    """
    Render an exception for a log line, never as an empty string.

    Several exceptions on this path stringify to "" -- notably asyncio.TimeoutError and the
    httpx/httpcore timeout wrappers -- which produced log lines ending in a bare colon
    ("attempt 1/3 failed:") and made these failures effectively undiagnosable.
    """
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def fastvss_ws_job_url(ws_base: str, job_id: str, project: str) -> str:
    """Build Fast-VSS WebSocket URL with URL-encoded project path segment."""
    return f"{ws_base.rstrip('/')}/ws/predict/job/{job_id}/{quote(project, safe='')}"


def fastvss_ws_origin_from_base(ws_base: str) -> str:
    """Derive HTTP Origin from WebSocket base (wss://host -> https://host)."""
    parsed = urlparse(ws_base if "://" in ws_base else f"ws://{ws_base}")
    scheme = "https" if parsed.scheme == "wss" else "http"
    netloc = parsed.netloc or parsed.path.split("/")[0] or "localhost"
    return f"{scheme}://{netloc}"


def extract_sync_embeddings(data: Any) -> list[Any] | None:
    """
    Return embedding vectors from a synchronous Fast-VSS POST body, or None.

    A 200 without job_id is only a success when the body contains a non-empty
    embeddings list (or is itself a list of vectors). Prefix / unknown project
    names that return Comment/error JSON must not be treated as completed.
    """
    if isinstance(data, list):
        return data if _looks_like_embedding_list(data) else None
    if not isinstance(data, dict):
        return None
    if data.get("error"):
        return None
    emb = data.get("embeddings")
    if isinstance(emb, list) and _looks_like_embedding_list(emb):
        return emb
    return None


def _looks_like_embedding_list(emb: list[Any]) -> bool:
    if not emb:
        return False
    first = emb[0]
    if isinstance(first, (int, float)):
        return True
    if isinstance(first, list) and first and isinstance(first[0], (int, float)):
        return True
    return False


def is_embedding_service_available() -> bool:
    """
    Return True if the Fast-VSS embedding service is reachable (GET /projects).
    Used by sync to skip embeddings when service is unavailable; same notion as GET /vss-embedding.
    """
    if not FASTVSS_BASE_URL:
        return False
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(f"{FASTVSS_BASE_URL}/projects")
            resp.raise_for_status()
            return True
    except Exception:
        return False


async def queue_embedding_job(
    image_bytes_list: list[bytes],
    local_filepaths: list[str],
    project: str = "default",
) -> str:
    """
    Forward batch to Fast-VSS POST /embeddings/{project}/, get job_id, return our UUID.
    Results are received via WebSocket /ws/predict/job/{job_id}/{project}. Poll GET /embed/{uuid} for results.
    """
    if not FASTVSS_BASE_URL:
        raise ValueError("FASTVSS_API_URL environment variable is not set")
    job_id = str(uuid.uuid4())
    async with _queue_lock:
        _queue_results[job_id] = {
            "status": "pending",
            "embeddings": None,
            "error": None,
        }

    async def run_job() -> None:
        logger.info(
            "[embedding_service] run_job started job_id=%s project=%r FASTVSS_BASE_URL=%s",
            job_id,
            project,
            FASTVSS_BASE_URL,
        )
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(fastvss_http_timeout_seconds(), connect=15.0)
            ) as client:
                files = [
                    ("files", (os.path.basename(fp), data))
                    for fp, data in zip(local_filepaths, image_bytes_list)
                ]
                url = f"{FASTVSS_BASE_URL}/embed/{project}"
                logger.info(
                    "[embedding_service] POST %s project=%r files=%d sizes=%s",
                    url,
                    project,
                    len(files),
                    [len(b) for b in image_bytes_list],
                )
                resp = await client.post(url, files=files)
                logger.info(
                    "[embedding_service] POST response status=%s url=%s",
                    resp.status_code,
                    str(resp.url),
                )
                if resp.history:
                    for i, r in enumerate(resp.history):
                        logger.info(
                            "[embedding_service] redirect %d: %s -> %s",
                            i + 1,
                            r.status_code,
                            r.headers.get("location", ""),
                        )
                resp.raise_for_status()
                data = resp.json()
                logger.info(
                    "[embedding_service] POST json keys=%s job_id=%s",
                    list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                    data.get("job_id") or data.get("job-id") if isinstance(data, dict) else None,
                )

            fastvss_job_id = data.get("job_id") or data.get("job-id")
            if fastvss_job_id:
                async with _queue_lock:
                    _job_map[job_id] = (str(fastvss_job_id), project)
                _queue_results[job_id] = {
                    "status": "pending",
                    "embeddings": None,
                    "error": None,
                    "fastvss_job_id": fastvss_job_id,
                }

                async def wait_job() -> None:
                    if not FASTVSS_BASE_URL:
                        return
                    if FASTVSS_BASE_URL.startswith("https://"):
                        ws_base = "wss://" + FASTVSS_BASE_URL[8:]
                    elif FASTVSS_BASE_URL.startswith("http://"):
                        ws_base = "ws://" + FASTVSS_BASE_URL[7:]
                    else:
                        ws_base = "ws://" + FASTVSS_BASE_URL
                    url = fastvss_ws_job_url(ws_base, str(fastvss_job_id), project)
                    origin = fastvss_ws_origin_from_base(ws_base)
                    logger.info(
                        "[embedding_service] WebSocket connect ws_base=%s job_id=%s project=%s url=%s",
                        ws_base,
                        fastvss_job_id,
                        project,
                        url,
                    )
                    try:
                        async with websockets.connect(
                            url,
                            open_timeout=fastvss_ws_connect_timeout_seconds(),
                            close_timeout=5,
                            max_size=10 * 1024 * 1024,
                            additional_headers={"Origin": origin},
                        ) as ws:
                            job_timeout = fastvss_ws_max_wait_seconds()
                            idle_timeout = fastvss_ws_idle_timeout_seconds()
                            start = time.monotonic()
                            deadline = start + job_timeout
                            frames = 0
                            def budget_msg(n: int) -> str:
                                return (
                                    f"job did not finish within {job_timeout:.0f}s "
                                    f"(received {n} status frames; raise "
                                    "FASTVSS_WS_MAX_WAIT if the embed service is "
                                    "simply backed up)"
                                )

                            while True:
                                now = time.monotonic()
                                budget_left = deadline - now
                                if budget_left <= 0:
                                    async with _queue_lock:
                                        _queue_results[job_id] = {
                                            "status": "failed",
                                            "embeddings": None,
                                            "error": budget_msg(frames),
                                        }
                                        _job_map.pop(job_id, None)
                                    return
                                # Wait at most `idle_timeout` for the next frame, never the
                                # bare remaining budget clamped to a floor: that turns the job
                                # deadline into an inter-frame watchdog which any momentary
                                # stall trips. Attribute the timeout to whichever limit
                                # actually elapsed.
                                wait_next = min(idle_timeout, budget_left)
                                try:
                                    raw = await asyncio.wait_for(
                                        ws.recv(), timeout=wait_next
                                    )
                                except (TimeoutError, asyncio.TimeoutError):
                                    if time.monotonic() >= deadline:
                                        error = budget_msg(frames)
                                    else:
                                        error = (
                                            f"no message from Fast-VSS for {wait_next:.1f}s "
                                            f"after {now - start:.0f}s (received {frames} "
                                            "status frames); connection or service stalled"
                                        )
                                    async with _queue_lock:
                                        _queue_results[job_id] = {
                                            "status": "failed",
                                            "embeddings": None,
                                            "error": error,
                                        }
                                        _job_map.pop(job_id, None)
                                    return
                                frames += 1
                                msg = json.loads(raw)
                                status = msg.get("status")
                                logger.debug(
                                    "[embedding_service] WebSocket recv status=%s keys=%s",
                                    status,
                                    list(msg.keys()) if isinstance(msg, dict) else "n/a",
                                )
                                if status == "done":
                                    logger.info("[embedding_service] WebSocket status=done")
                                    result = msg.get("result")
                                    emb = result if result is not None else msg
                                    async with _queue_lock:
                                        _queue_results[job_id] = {
                                            "status": "completed",
                                            "embeddings": emb,
                                            "error": None,
                                        }
                                        _job_map.pop(job_id, None)
                                    return
                                if status == "failed":
                                    err_msg = msg.get("message", "Job failed")
                                    logger.warning(
                                        "[embedding_service] WebSocket status=failed: %s",
                                        err_msg,
                                    )
                                    async with _queue_lock:
                                        _queue_results[job_id] = {
                                            "status": "failed",
                                            "embeddings": None,
                                            "error": err_msg,
                                        }
                                        _job_map.pop(job_id, None)
                                    return
                                if status == "error":
                                    err_msg = msg.get("message", str(msg))
                                    logger.warning(
                                        "[embedding_service] WebSocket status=error: %s",
                                        err_msg,
                                    )
                                    async with _queue_lock:
                                        _queue_results[job_id] = {
                                            "status": "failed",
                                            "embeddings": None,
                                            "error": err_msg,
                                        }
                                        _job_map.pop(job_id, None)
                                    return
                    except Exception as e:
                        logger.warning(
                            "[embedding_service] WebSocket failed job=%s: %s (%s)",
                            job_id,
                            e,
                            type(e).__name__,
                        )
                        logger.debug("[embedding_service] WebSocket exception", exc_info=True)
                        async with _queue_lock:
                            _queue_results[job_id] = {
                                "status": "failed",
                                "embeddings": None,
                                "error": str(e),
                            }
                            _job_map.pop(job_id, None)

                asyncio.create_task(wait_job())
            else:
                keys = (
                    list(data.keys())
                    if isinstance(data, dict)
                    else type(data).__name__
                )
                logger.info(
                    "[embedding_service] Sync response (no job_id) keys=%s",
                    keys,
                )
                emb = extract_sync_embeddings(data)
                if emb is not None:
                    async with _queue_lock:
                        _queue_results[job_id] = {
                            "status": "completed",
                            "embeddings": emb,
                            "error": None,
                        }
                else:
                    if isinstance(data, dict):
                        err_msg = (
                            data.get("error")
                            or data.get("message")
                            or data.get("Comment")
                            or f"no job_id and no embeddings (keys={keys})"
                        )
                    else:
                        err_msg = f"no job_id and no embeddings ({type(data).__name__})"
                    logger.warning(
                        "[embedding_service] POST has no job_id/embeddings project=%r: %s",
                        project,
                        err_msg,
                    )
                    async with _queue_lock:
                        _queue_results[job_id] = {
                            "status": "failed",
                            "embeddings": None,
                            "error": str(err_msg),
                        }
        except Exception as e:
            err_detail = str(e)
            resp_attrs = ""
            if hasattr(e, "response") and e.response is not None:
                r = e.response
                resp_attrs = (
                    f" response_status={r.status_code} response_url={r.url} "
                    f"location={r.headers.get('location', '')}"
                )
            logger.exception(
                "[embedding_service] POST failed: %s (%s)%s",
                err_detail,
                type(e).__name__,
                resp_attrs,
            )
            async with _queue_lock:
                _queue_results[job_id] = {
                    "status": "failed",
                    "embeddings": None,
                    "error": str(e),
                }

    asyncio.create_task(run_job())
    return job_id


async def get_or_poll_embedding_result(job_id: str) -> dict[str, Any] | None:
    """
    Get cached result for a queued embedding job. Status is updated by a background WebSocket;
    clients poll GET /embed/{job_id} until status is not pending.
    """
    return _queue_results.get(job_id)


# Minimal 1x1 PNG for WebSocket connectivity test (67 bytes)
_FAKE_IMAGE_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00"
    b"\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
)

_WS_TEST_POLL_INTERVAL = 0.5


async def test_embedding_websocket(project: str = "default") -> tuple[bool, str | None]:
    """
    Send a fake 1x1 image to the embedding service and verify the WebSocket pipeline works.
    Returns (success, error_message). Used by the launcher to gate the Load from Tator button.
    """
    if not FASTVSS_BASE_URL:
        return False, "FASTVSS_API_URL is not set"
    try:
        job_id = await queue_embedding_job(
            [_FAKE_IMAGE_PNG],
            ["test_1x1.png"],
            project=project,
        )
        deadline = time.monotonic() + fastvss_ws_test_timeout_seconds()
        while time.monotonic() < deadline:
            result = _queue_results.get(job_id)
            if result is None:
                await asyncio.sleep(_WS_TEST_POLL_INTERVAL)
                continue
            status = result.get("status")
            if status == "completed":
                return True, None
            if status == "failed":
                return False, result.get("error") or "Job failed"
            await asyncio.sleep(_WS_TEST_POLL_INTERVAL)
        return False, "WebSocket test timed out"
    except Exception as e:
        return False, str(e)
