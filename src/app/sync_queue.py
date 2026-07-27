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
    section_id: int | None = None,
    query: str | None = None,
    localization_type_id: int | None = None,
    verified_only: bool = False,
) -> str:
    """
    Enqueue a sync job. Returns RQ job id. Requires Redis.
    project_name is used by the worker to resolve get_database_uri(project_id, port).
    vss_project_key is used to select a specific VSS project configuration for embeddings.
    localization_type_id restricts the sync to a single Tator box (localization) type.
    verified_only restricts the built dataset to localizations with a truthy `verified` attribute.
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
        section_id=section_id,
        query=query,
        localization_type_id=localization_type_id,
        verified_only=verified_only,
        job_timeout=3600 * 24,  # 24h for large projects
        result_ttl=3600 * 24,
        failure_ttl=3600,
    )
    return job.id


def enqueue_sync_to_tator(
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
) -> str:
    """
    Enqueue a sync-to-tator job (push FiftyOne edits back to Tator). Returns RQ job id.
    Heavy bulk-patch work runs in the RQ worker so the HTTP handler is non-blocking
    and a slow push cannot starve the API. Requires Redis.
    """
    from rq import Queue

    conn = get_connection()
    queue = Queue(QUEUE_NAME, connection=conn)
    job = queue.enqueue(
        "src.app.sync.run_sync_to_tator_job",
        project_id=project_id,
        version_id=version_id,
        api_url=api_url,
        token=token,
        port=port,
        project_name=project_name,
        dataset_name=dataset_name,
        label_attr=label_attr,
        score_attr=score_attr,
        debug=debug,
        force_sync=force_sync,
        job_timeout=3600 * 6,  # up to 6h for very large pushes
        result_ttl=3600 * 24,
        failure_ttl=3600,
    )
    return job.id


def enqueue_dimreduce(
    project_id: int,
    version_id: int,
    api_url: str,
    token: str,
    port: int,
    project_name: str | None,
    method: str,
    num_dims: int = 2,
    force: bool = True,
) -> str:
    """
    Enqueue a job that recomputes a dimensionality reduction visualization
    (PCA/t-SNE/UMAP) from existing embeddings.
    """
    from rq import Queue

    conn = get_connection()
    queue = Queue(QUEUE_NAME, connection=conn)
    job = queue.enqueue(
        "src.app.sync.run_dimreduce_job",
        project_id=project_id,
        version_id=version_id,
        api_url=api_url,
        token=token,
        port=port,
        project_name=project_name,
        method=method,
        num_dims=num_dims,
        force=force,
        job_timeout=3600 * 6,  # up to 6h for very large t-SNE runs
        result_ttl=3600 * 12,
        failure_ttl=3600,
    )
    return job.id


def enqueue_recompute_crops(
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
) -> str:
    """Enqueue a crop-recompute job and return RQ job id."""
    from rq import Queue

    conn = get_connection()
    queue = Queue(QUEUE_NAME, connection=conn)
    job = queue.enqueue(
        "src.app.sync.run_recompute_crops_job",
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
        job_timeout=3600 * 24,  # 24h for large recrop jobs
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
