# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync_queue.py
# Description: Redis queue for FiftyOne sync jobs; POST /sync enqueues, RQ worker runs sync.
"""
Redis queue for FiftyOne sync jobs. Redis is required (REDIS_HOST or REDIS_URL).
POST /sync enqueues a job and returns immediately; a separate RQ worker runs
the long-running sync. Compatible with the Tator compose stack (redis service).
"""

from __future__ import annotations

import os
from typing import Any

QUEUE_NAME = "fiftyone_sync"


def _get_redis_url() -> str:
    """Return Redis URL. Raises RuntimeError if not configured."""
    url = os.environ.get("REDIS_URL", "").strip()
    if url:
        return url
    host = os.environ.get("REDIS_HOST", "").strip()
    if not host:
        raise RuntimeError("Redis not configured (set REDIS_HOST or REDIS_URL)")
    port = os.environ.get("REDIS_PORT", "6379")
    password = os.environ.get("REDIS_PASSWORD", "")
    use_ssl = os.environ.get("REDIS_USE_SSL", "false").lower() == "true"
    scheme = "rediss" if use_ssl else "redis"
    if password:
        return f"{scheme}://:{password}@{host}:{port}/0"
    return f"{scheme}://{host}:{port}/0"


def get_connection():
    """Redis connection for RQ. Raises if Redis is not configured or unavailable."""
    from redis import Redis
    from redis.backoff import ExponentialBackoff
    from redis.retry import Retry
    from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError

    url = _get_redis_url()
    retry = Retry(ExponentialBackoff(), 3)
    return Redis.from_url(
        url,
        retry=retry,
        retry_on_error=[BusyLoadingError, ConnectionError, TimeoutError],
        health_check_interval=30,
    )


def enqueue_sync(
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
) -> str:
    """
    Enqueue a sync job. Returns RQ job id. Requires Redis.
    project_name is used by the worker to resolve get_database_uri(project_id, port).
    vss_project_key is used to select a specific VSS project configuration for embeddings.
    """
    from rq import Queue

    conn = get_connection()
    queue = Queue(QUEUE_NAME, connection=conn)
    job = queue.enqueue(
        "src.app.sync.run_sync_job",
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
        job_timeout=3600 * 24,  # 24h for large projects
        result_ttl=3600 * 24,
        failure_ttl=3600,
    )
    return job.id


def get_job_status(job_id: str) -> dict[str, Any]:
    """
    Return status and result for a sync job.
    Keys: status (queued|started|finished|failed|deferred|canceled),
          result (dict when finished), error (str when failed).
    """
    from rq.job import Job

    conn = get_connection()
    try:
        job = Job.fetch(job_id, connection=conn)
    except Exception as e:
        return {"status": "unknown", "error": str(e)}
    status = job.get_status(refresh=True)
    out = {"status": status}
    if status == "finished" and job.result is not None:
        out["result"] = job.result
    if status == "failed" and job.exc_info:
        exc_text = job.exc_info
        lines = [line.strip() for line in exc_text.strip().splitlines() if line.strip()]
        out["error"] = lines[-1] if lines else exc_text
    return out


def get_job_logs(job_id: str) -> dict[str, Any]:
    """
    Return log lines stored in job metadata by the sync worker.
    Returns {"log_lines": list[str]}. Raises on job not found or Redis error.
    """
    from rq.job import Job

    conn = get_connection()
    job = Job.fetch(job_id, connection=conn)
    return {"log_lines": job.meta.get("log_lines", [])}
