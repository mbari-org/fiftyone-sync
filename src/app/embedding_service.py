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
_WS_CONNECT_TIMEOUT = 10


def _ws_base_url() -> str | None:
    """Derive WebSocket base URL from FASTVSS_BASE_URL (http -> ws, https -> wss)."""
    if not FASTVSS_BASE_URL:
        return None
    if FASTVSS_BASE_URL.startswith("https://"):
        return "wss://" + FASTVSS_BASE_URL[8:]
    if FASTVSS_BASE_URL.startswith("http://"):
        return "ws://" + FASTVSS_BASE_URL[7:]
    return "ws://" + FASTVSS_BASE_URL


async def _websocket_wait_job(job_id: str, fastvss_job_id: str, project: str) -> None:
    """Connect to Fast-VSS /ws/predict/job/{job_id}/{project} and update _queue_results when done/failed."""
    ws_base = _ws_base_url()
    if not ws_base:
        return
    url = f"{ws_base}/ws/predict/job/{fastvss_job_id}/{project}"
    logger.info(f"Connecting to WebSocket URL: {url}")
    try:
        async with websockets.connect(
            url,
            open_timeout=_WS_CONNECT_TIMEOUT,
            close_timeout=5,
            max_size=10 * 1024 * 1024,  # 10MB max message size (default is 1MB)
        ) as ws:
            deadline = time.monotonic() + _WS_MAX_WAIT
            while True:
                remaining = max(1.0, deadline - time.monotonic())
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
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
                if status == "done":
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
                    async with _queue_lock:
                        _queue_results[job_id] = {
                            "status": "failed",
                            "embeddings": None,
                            "error": msg.get("message", "Job failed"),
                        }
                        _job_map.pop(job_id, None)
                    return
                if status == "error":
                    async with _queue_lock:
                        _queue_results[job_id] = {
                            "status": "failed",
                            "embeddings": None,
                            "error": msg.get("message", str(msg)),
                        }
                        _job_map.pop(job_id, None)
                    return
    except Exception as e:
        logger.warning("Fast-VSS WebSocket failed for job %s: %s", job_id, e)
        async with _queue_lock:
            _queue_results[job_id] = {
                "status": "failed",
                "embeddings": None,
                "error": str(e),
            }
            _job_map.pop(job_id, None)


def init_disk_cache() -> None:
    """No-op; Fast-VSS handles caching."""
    pass


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
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                files = [
                    ("files", (os.path.basename(fp), data))
                    for fp, data in zip(local_filepaths, image_bytes_list)
                ]
                url = f"{FASTVSS_BASE_URL}/embeddings/{project}/"
                resp = await client.post(url, files=files)
                resp.raise_for_status()
                data = resp.json()

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
                asyncio.create_task(
                    _websocket_wait_job(job_id, str(fastvss_job_id), project)
                )
            else:
                # Sync response with embeddings
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
            logger.exception("Fast-VSS embedding request failed")
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
