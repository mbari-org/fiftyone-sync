#!/usr/bin/env python3
# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync_worker.py
# Description: RQ worker for FiftyOne sync jobs when Redis is used for background sync.
"""
RQ worker for FiftyOne sync jobs. Run when Redis is used for background sync:
  python -m src.app.sync_worker
  # or: rq worker --url redis://localhost:6379/0 fiftyone_sync

Use with Tator compose: set REDIS_HOST=redis and run this in a separate container
or on the same host that can reach Redis.
"""

from __future__ import annotations

import sys

import fiftyone as fo

from rq import Queue, Worker

from src.app.database_manager import get_is_enterprise, require_sync_config_path
from src.app.sync_queue import QUEUE_NAME, _get_redis_url, get_connection


def main() -> None:
    require_sync_config_path()
    url = _get_redis_url()
    if not url:
        print("Set REDIS_HOST or REDIS_URL to run the sync worker.", file=sys.stderr)
        sys.exit(1)
    # Clear all sync locks on startup so restarts don't leave stale locks
    try:
        from src.app.sync_lock import cleanup_all_sync_locks

        deleted = cleanup_all_sync_locks()
        if deleted > 0:
            print(f"Cleaned {deleted} stale sync lock(s) on worker startup", file=sys.stderr)
    except Exception as e:
        print(f"Warning: could not clean sync locks on startup: {e}", file=sys.stderr)
    # Launch FiftyOne app only when not enterprise (enterprise uses its own app; no local MongoDB)
    if not get_is_enterprise():
        fo.launch_app(address="0.0.0.0", port=5151)
    conn = get_connection()
    queue = Queue(QUEUE_NAME, connection=conn)
    worker = Worker([queue], connection=conn)
    worker.work()


if __name__ == "__main__":
    main()
