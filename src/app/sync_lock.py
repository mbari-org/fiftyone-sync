# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync_lock.py
# Description: Mutex for FiftyOne sync so only one sync per version runs at a time (Redis-based).
"""
Mutex for FiftyOne sync: only one sync per (database, project, version) at a time.
Different versions of the same project can sync concurrently; each version has its own lock.
Uses Redis (required). Set REDIS_HOST or REDIS_URL.
"""

from __future__ import annotations

import os

LOCK_KEY_PREFIX = "fiftyone_sync_lock"
DEFAULT_TTL_SECONDS = 7200  # 2h max hold so crashed workers don't lock forever


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


def _get_connection():
    """Redis connection for lock. Raises on failure."""
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


def _version_slug(version_id: int | None) -> str:
    """Slug for version in lock key (matches sync.py _version_slug)."""
    return f"v{version_id}" if version_id is not None else "v_all"


def get_sync_lock_key(resolved_db: str, project_id: int, version_id: int | None) -> str:
    """Return a unique key for the (database, project, version) being synced.

    Lock is per-version: different versions of the same project can sync concurrently.
    """
    return f"{LOCK_KEY_PREFIX}:{resolved_db}:{project_id}:{_version_slug(version_id)}"


def try_acquire_sync_lock(
    lock_key: str,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> bool:
    """
    Try to acquire the sync lock. Non-blocking.
    Returns True if acquired, False if another sync holds it.
    Raises if Redis is unavailable.
    """
    conn = _get_connection()
    try:
        acquired = conn.set(lock_key, "1", nx=True, ex=ttl_seconds)
        return bool(acquired)
    finally:
        conn.close()


def release_sync_lock(lock_key: str) -> None:
    """Release the sync lock. Raises if Redis is unavailable."""
    conn = _get_connection()
    try:
        conn.delete(lock_key)
    finally:
        conn.close()


def cleanup_all_sync_locks() -> int:
    """
    Delete all sync lock keys from Redis (e.g. on service startup).
    Returns the number of keys deleted.
    Raises if Redis is not configured or unavailable.
    """
    conn = _get_connection()
    try:
        pattern = f"{LOCK_KEY_PREFIX}:*"
        cursor = 0
        deleted_count = 0
        while True:
            cursor, keys = conn.scan(cursor, match=pattern, count=100)
            if keys:
                deleted_count += conn.delete(*keys)
            if cursor == 0:
                break
        return deleted_count
    finally:
        conn.close()
