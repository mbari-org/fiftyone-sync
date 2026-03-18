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
_WS_MAX_WAIT = 300
_WS_CONNECT_TIMEOUT = 30


def is_embedding_service_available() -> bool:
    """
    Return True if the Fast-VSS embedding service is reachable (GET /projects).
    Used by sync to skip embeddings when service is unavailable; same notion as GET /vss-embedding.
    """
    if not FASTVSS_BASE_URL:
        return False
    try:
        with httpx.Client(timeout=10.0) as client:
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
            async with httpx.AsyncClient(timeout=60.0) as client:
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
                    url = f"{ws_base}/ws/predict/job/{str(fastvss_job_id)}/{project}"
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
                            open_timeout=_WS_CONNECT_TIMEOUT,
                            close_timeout=5,
                            max_size=10 * 1024 * 1024,
                        ) as ws:
                            deadline = time.monotonic() + _WS_MAX_WAIT
                            while True:
                                remaining = max(1.0, deadline - time.monotonic())
                                try:
                                    raw = await asyncio.wait_for(
                                        ws.recv(), timeout=remaining
                                    )
                                except asyncio.TimeoutError:
                                    async with _queue_lock:
                                        _queue_results[job_id] = {
                                            "status": "failed",
                                            "embeddings": None,
                                            "error": "WebSocket wait timed out",
                                        }
                                        _job_map.pop(job_id, None)
                                    return
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
                # Sync response with embeddings (no job_id, embeddings in response)
                logger.info(
                    "[embedding_service] Sync response (no job_id) keys=%s",
                    list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                )
                emb = data.get("embeddings") or data
                if isinstance(emb, list):
                    async with _queue_lock:
                        _queue_results[job_id] = {
                            "status": "completed",
                            "embeddings": emb,
                            "error": None,
                        }
                else:
                    async with _queue_lock:
                        _queue_results[job_id] = {
                            "status": "completed",
                            "embeddings": [emb],
                            "error": None,
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

_WS_TEST_TIMEOUT = 30.0
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
        deadline = time.monotonic() + _WS_TEST_TIMEOUT
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
