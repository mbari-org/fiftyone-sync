# fiftyone-sync, Apache-2.0 license
# Filename: src/app/main.py
# Description: FiftyOne sync service: embedding API, port manager, and launcher for Tator dashboards.
"""
FiftyOne sync service: embedding API, port manager, and launcher for Tator dashboards.
"""

from __future__ import annotations

import logging
import os
import time

import httpx
from fastapi import (
    APIRouter,
    File,
    Form,
    Header,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel

from src.app.database_manager import (
    get_database_entry_or_enterprise_default,
    get_is_enterprise,
    get_s3_config,
    require_sync_config_path,
)
from src.app.database_uri_config import database_name_from_uri
from src.app.embedding_service import (
    FASTVSS_BASE_URL,
    get_or_poll_embedding_result,
    init_disk_cache,
    queue_embedding_job,
)
from src.app.launcher_template import LAUNCHER_TEMPLATE
from src.app.sync_lock import LOCK_KEY_PREFIX

logger = logging.getLogger(__name__)

require_sync_config_path()

TATOR_INTERNAL_API_URL = (
    os.environ.get("TATOR_INTERNAL_API_URL", "").strip().rstrip("/")
)


def _resolve_api_url(api_url: str) -> str:
    """Return the Docker-internal API URL when running inside a container.

    The browser sends api_url=http://localhost:8080 which is unreachable from
    inside a Docker container (localhost == the container itself).  When
    TATOR_INTERNAL_API_URL is set, swap out the host so the tator SDK can
    reach the Tator nginx service on the Docker network.
    """
    if not TATOR_INTERNAL_API_URL:
        return api_url.rstrip("/")
    from urllib.parse import urlparse

    parsed = urlparse(api_url)
    if parsed.hostname in ("localhost", "127.0.0.1"):
        return TATOR_INTERNAL_API_URL
    return api_url.rstrip("/")


# Origins allowed for CORS (dashboard and applet iframes). When allow_credentials=True,
# browsers require a specific origin; "*" is not valid.
CORS_ORIGINS = [
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]
_cors_origins_env = os.environ.get("FIFTYONE_SYNC_CORS_ORIGINS", "").strip()
if _cors_origins_env:
    CORS_ORIGINS.extend(
        origin.strip() for origin in _cors_origins_env.split(",") if origin.strip()
    )

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
)

app = FastAPI(
    title="FiftyOne Sync Service",
    description="Embedding API and FiftyOne launcher for Tator dashboards",
    version="0.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    if request.url.path in ("/metrics", "/voxel51/metrics"):
        return await call_next(request)
    method = request.method
    endpoint = request.url.path
    start = time.monotonic()
    response = await call_next(request)
    elapsed = time.monotonic() - start
    REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(elapsed)
    REQUEST_COUNT.labels(
        method=method, endpoint=endpoint, status=response.status_code
    ).inc()
    return response


@app.on_event("startup")
async def cleanup_locks_on_startup():
    """Clean all sync lock keys from Redis on startup to prevent stale locks."""
    try:
        from redis import Redis
        from redis.backoff import ExponentialBackoff
        from redis.retry import Retry
        from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError

        # Get Redis URL from sync_lock module
        redis_url_env = os.environ.get("REDIS_URL", "").strip()
        if redis_url_env:
            redis_url = redis_url_env
        else:
            host = os.environ.get("REDIS_HOST", "").strip()
            if not host:
                logger.warning(
                    "Redis not configured (set REDIS_HOST or REDIS_URL), skipping lock cleanup"
                )
                return
            port = os.environ.get("REDIS_PORT", "6379")
            password = os.environ.get("REDIS_PASSWORD", "")
            use_ssl = os.environ.get("REDIS_USE_SSL", "false").lower() == "true"
            scheme = "rediss" if use_ssl else "redis"
            if password:
                redis_url = f"{scheme}://:{password}@{host}:{port}/0"
            else:
                redis_url = f"{scheme}://{host}:{port}/0"

        retry = Retry(ExponentialBackoff(), 3)
        conn = Redis.from_url(
            redis_url,
            retry=retry,
            retry_on_error=[BusyLoadingError, ConnectionError, TimeoutError],
            health_check_interval=30,
        )

        try:
            # Delete all keys matching the lock key prefix
            pattern = f"{LOCK_KEY_PREFIX}:*"
            cursor = 0
            deleted_count = 0
            while True:
                cursor, keys = conn.scan(cursor, match=pattern, count=100)
                if keys:
                    deleted_count += conn.delete(*keys)
                if cursor == 0:
                    break

            if deleted_count > 0:
                logger.info(f"Cleaned {deleted_count} stale sync lock(s) on startup")
            else:
                logger.info("No stale sync locks found on startup")
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Failed to clean sync locks on startup: {e}")


router = APIRouter(tags=["embedding"])
app_launch = APIRouter(tags=["launcher"])


class EmbedResponse(BaseModel):
    uuid: str


@router.post("/embed", response_model=EmbedResponse)
async def post_embed(
    files: list[UploadFile] = File(..., description="Batch of image files"),
    project: str = Form(
        "default", description="Project name for Fast-VSS /embeddings/{project}/"
    ),
) -> EmbedResponse:
    """
    Submit a batch of images for embedding. Forwards to Fast-VSS POST /embeddings/{project}/.
    Returns UUID to fetch results via GET /embed/{uuid}.
    """
    init_disk_cache()
    image_bytes_list: list[bytes] = []
    local_filepaths: list[str] = []
    for f in files:
        data = await f.read()
        image_bytes_list.append(data)
        local_filepaths.append(f.filename)
    if not image_bytes_list:
        raise HTTPException(status_code=400, detail="No images provided")
    if not local_filepaths:
        raise HTTPException(status_code=400, detail="No local filepaths provided")
    if not FASTVSS_BASE_URL:
        raise HTTPException(
            status_code=503,
            detail="Embedding service unavailable: FASTVSS_API_URL is not set",
        )
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{FASTVSS_BASE_URL}/projects")
            resp.raise_for_status()
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Embedding service unavailable: {e!s}",
        ) from e
    job_id = await queue_embedding_job(
        image_bytes_list, local_filepaths, project=project
    )
    return EmbedResponse(uuid=job_id)


@router.get("/embed/{job_id}")
async def get_embed(job_id: str) -> dict:
    """
    Fetch embedding results by UUID. Polls Fast-VSS if pending. Returns status and embeddings when ready.
    """
    result = await get_or_poll_embedding_result(job_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return result


@router.get("/vss-embedding")
async def get_vss_embedding() -> dict:
    """
    Proxy to embedding service GET /projects. Returns {"projects": ["name", ...]}.
    Uses FASTVSS_API_URL. Used by the launcher applet to show embedding service
    availability and whether the current project is registered. On failure returns 503.
    """
    if not FASTVSS_BASE_URL:
        raise HTTPException(
            status_code=503,
            detail="FASTVSS_API_URL is not set",
        )
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{FASTVSS_BASE_URL}/projects")
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Embedding service unavailable: {e!s}",
        ) from e


# --- Launcher (HostedTemplate) ---


# Jinja2 template for Tator Hosted Template (applet/dashboard).
# See: https://www.tator.io/docs/developer-guide/applets-and-dashboards/hosted-templates
# Required tparams: project (int, Tator project ID).
# Optional: iframe_host, base_port (5151).
# For "Sync from Tator" / "Sync to Tator": set sync_service_url, api_url (no token tparam).
# User enters their Tator API token in the applet and clicks "Verify Token"; sync controls enable after token is verified.
# Optional tparams: version_id, database_name, project_name (vss_project for embedding status only).
# database-info is called with project_id; send api_url and Authorization to resolve to config key (project name).
# Embedding service status: server uses FASTVSS_API_URL; GET /vss-embedding shows availability.
# FiftyOne opens in a new window/tab (Open FiftyOne button); no iframe.

# Minimal template: single {{ message }} parameter (for simple Hosted Template testing).
MESSAGE_TEMPLATE = """<!DOCTYPE html>
<html>
<body>
<p style="color: white;">{{ message }}</p>
</body>
</html>
"""


@app.get("/message", response_class=HTMLResponse)
async def message_template() -> HTMLResponse:
    """
    Return a minimal Jinja2 template with a single {{ message }} parameter as HTML.
    For Tator Hosted Template: set URL to this endpoint, then use tparam message (e.g. "Hello").
    """
    return HTMLResponse(content=MESSAGE_TEMPLATE, status_code=200)


@app_launch.get("/database-info")
async def get_database_info(
    project_id: int = Query(
        ...,
        description="Tator project ID; resolved to config key (project name) via API",
    ),
    api_url: str = Query(
        ..., description="Tator API base URL (required, same as /versions)"
    ),
    port: int = Query(..., description="Port for this project"),
    vss_project_key: str | None = Query(None, description="Optional VSS project key"),
    authorization: str | None = Header(None, alias="Authorization"),
) -> dict:
    """
    Resolve database (and port) from DatabaseUriConfig. Requires authentication the same as GET /versions:
    api_url query param and Authorization header (e.g. "Token <token>"). Returns 401 if missing.
    Project name is resolved via get_project(project_id).name. Returns { port, database_name, database_uri }.
    If vss_project_key is provided, also resolves S3 config from that specific VSS project.
    """
    token = _token_from_authorization(authorization)
    if not token:
        raise HTTPException(
            status_code=401, detail="Missing or invalid Authorization header"
        )
    project_name: str | None = None
    try:
        import tator

        api = tator.get_api(_resolve_api_url(api_url), token)
        proj = api.get_project(project_id)
        project_name = getattr(proj, "name", None) or str(project_id)
    except Exception as e:
        logger.warning(f"get_project({project_id}) failed: {e}")
    if not project_name or not project_name.strip():
        project_name = str(project_id)
    project_name = project_name.strip()
    logger.info(
        f"request project_id={project_id} -> project_name={project_name!r} port={port}"
    )
    database_entry = get_database_entry_or_enterprise_default(
        project_id, port, project_name=project_name
    )
    if database_entry is None:
        raise HTTPException(
            status_code=404,
            detail=f"No DatabaseUriConfig entry for project_id={project_id} (project_name={project_name!r}). Set FIFTYONE_SYNC_CONFIG_PATH and add this project.",
        )
    out = {
        "port": database_entry.port,
        "database_name": database_name_from_uri(database_entry.uri),
        "database_uri": database_entry.uri,
        "is_enterprise": get_is_enterprise(),
    }
    s3_config = get_s3_config(
        project_id, project_name=project_name, vss_project_key=vss_project_key
    )
    if s3_config:
        out["s3_bucket"] = s3_config.get("s3_bucket")
        out["s3_prefix"] = s3_config.get("s3_prefix")
    return out


@app_launch.get("/render", response_class=HTMLResponse)
async def render_launcher() -> HTMLResponse:
    """
    Return Jinja2 template for HostedTemplate. Tator fetches this URL and renders with tparams.
    Required tparams: project (Tator project ID). Optional: iframe_host (host for app URL), base_port (5151), project_name (vss_project for embedding status only).
    The applet uses GET /database-info?project_id=... to resolve port and database from DatabaseUriConfig.
    When the sync worker runs on a different host than the API, set FIFTYONE_APP_PUBLIC_BASE_URL in the worker
    environment to the hostname the browser can use to reach the FiftyOne app (e.g. worker host); the sync
    result will include app_url and the applet will open that URL.
    "Open FiftyOne" opens the app in a new window. For sync: set sync_service_url and api_url; the user enters their Tator API token in the applet and clicks "Verify Token" to enable Sync from Tator / Sync to Tator.
    Embedding service status: server uses FASTVSS_API_URL; applet calls GET /vss-embedding to show availability and project registration.
    """
    return HTMLResponse(LAUNCHER_TEMPLATE)


def _token_from_authorization(authorization: str | None) -> str | None:
    """Extract raw token from Authorization header (Token <token> or Bearer <token>)."""
    if not authorization or not authorization.strip():
        return None
    s = authorization.strip()
    if s.lower().startswith("token "):
        return s[6:].strip()
    if s.lower().startswith("bearer "):
        return s[7:].strip()
    return s


@app_launch.get("/versions")
async def get_versions(
    project_id: int = Query(..., description="Tator project ID"),
    api_url: str = Query(..., description="Tator REST API base URL"),
    authorization: str | None = Header(None, alias="Authorization"),
) -> list[dict]:
    """
    Return list of Tator versions for the given project. Used by the launcher template
    to populate the version dropdown. Token must be sent via Authorization header
    (e.g. "Token <token>") and is not accepted in the URL.
    """
    import tator

    token = _token_from_authorization(authorization)
    if not token:
        raise HTTPException(
            status_code=401, detail="Missing or invalid Authorization header"
        )
    host = _resolve_api_url(api_url)
    try:
        api = tator.get_api(host, token)
        version_list = api.get_version_list(project_id)
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e)) from e
    return [
        {"id": v.id, "name": v.name, "number": v.number, "description": v.description}
        for v in version_list
    ]


@app_launch.get("/vss-projects")
async def get_vss_projects(
    project_id: int = Query(..., description="Tator project ID"),
    api_url: str = Query(..., description="Tator REST API base URL"),
    authorization: str | None = Header(None, alias="Authorization"),
) -> list[dict]:
    """
    Return list of available VSS projects for the given Tator project. Used by the launcher
    template to populate the VSS project dropdown. Token must be sent via Authorization header.
    Returns list of dicts with 'key', 'name' (vss_project), and 'vss_service'.
    """
    import tator
    from src.app.database_manager import get_vss_projects_list

    token = _token_from_authorization(authorization)
    if not token:
        raise HTTPException(
            status_code=401, detail="Missing or invalid Authorization header"
        )
    host = _resolve_api_url(api_url)

    # Get project name to resolve VSS projects
    try:
        api = tator.get_api(host, token)
        proj = api.get_project(project_id)
        project_name = getattr(proj, "name", None) or str(project_id)
        logger.info(
            f"get_vss_projects: project_id={project_id} -> tator project name={project_name!r}"
        )
    except Exception as e:
        logger.warning(f"get_project({project_id}) failed: {e}")
        project_name = str(project_id)

    if not project_name or not project_name.strip():
        project_name = str(project_id)

    vss_projects = get_vss_projects_list(project_name.strip())
    logger.info(
        f"get_vss_projects: lookup key={project_name.strip()!r} -> {len(vss_projects)} project(s): {[p['key'] for p in vss_projects]}"
    )
    return vss_projects


@app_launch.post("/sync")
async def sync(
    project_id: int = Query(..., description="Tator project ID"),
    version_id: int | None = Query(None),
    api_url: str = Query(
        ..., description="Tator REST API base URL (e.g. https://tator.example.com)"
    ),
    token: str = Query(..., description="Tator API token"),
    port: int = Query(..., description="Port for this project"),
    force_sync: bool = Query(
        False,
        description="Force full sync; bypass cached JSONL and re-fetch media/localizations",
    ),
    vss_project_key: str | None = Query(
        None, description="Optional VSS project key (for multi-VSS projects)"
    ),
    s3_bucket: str | None = Query(
        None,
        description="Optional S3 bucket for crop image upload (crops dir, not full images); bucket created if missing",
    ),
    s3_prefix: str | None = Query(
        None, description="Optional S3 prefix (folder) for crop image upload"
    ),
) -> dict:
    """
    Trigger sync: enqueues a job to fetch Tator media + localizations, build FiftyOne dataset, launch App.
    Returns job_id immediately; poll GET /sync/status/{job_id} for progress. Requires Redis (REDIS_HOST or REDIS_URL).
    """
    from src.app.sync_queue import enqueue_sync
    from src.app.database_manager import get_vss_project_config

    project_name: str | None = None
    try:
        import tator

        api = tator.get_api(_resolve_api_url(api_url), token)
        proj = api.get_project(project_id)
        project_name = getattr(proj, "name", None) or str(project_id)
    except Exception as e:
        logger.warning(f"get_project({project_id}) failed: {e}")
    if not project_name or not project_name.strip():
        project_name = str(project_id)
    project_name = project_name.strip()
    logger.info(
        f"project_id={project_id} project_name={project_name!r} vss_project_key={vss_project_key!r} -> port={port}"
    )
    api_url_clean = _resolve_api_url(api_url)
    database_entry = get_database_entry_or_enterprise_default(
        project_id, port, project_name=project_name
    )
    if database_entry is None:
        raise HTTPException(
            status_code=404,
            detail=f"No DatabaseUriConfig entry for project_id={project_id} (project_name={project_name!r}). Set FIFTYONE_SYNC_CONFIG_PATH and add this project.",
        )

    # Resolve S3 config from VSS project if key provided and not explicitly overridden
    if vss_project_key and not s3_bucket:
        vss_config = get_vss_project_config(project_name, vss_project_key)
        if vss_config:
            s3_bucket = vss_config.get("s3_bucket")
            s3_prefix = vss_config.get("s3_prefix")
            logger.info(
                f"Using S3 config from VSS project {vss_project_key!r}: bucket={s3_bucket}, prefix={s3_prefix}"
            )

    # Disable S3 upload when is_enterprise is False (config)
    if not get_is_enterprise():
        s3_bucket = None
        s3_prefix = None

    try:
        job_id = enqueue_sync(
            project_id=project_id,
            version_id=version_id,
            api_url=api_url_clean,
            token=token,
            port=database_entry.port,
            project_name=project_name,
            database_name=database_name_from_uri(database_entry.uri),
            force_sync=force_sync,
            vss_project_key=vss_project_key,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
        )
        return {"job_id": job_id, "status": "queued", "port": port}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}") from e


@app_launch.get("/sync/status/{job_id}")
async def sync_status(job_id: str) -> dict:
    """
    Poll status of an enqueued sync job. Returns status (queued|started|finished|failed),
    and when finished: result (port, dataset_name, sample_count, etc.) or error. Requires Redis.
    """
    from src.app.sync_queue import get_job_status

    try:
        return get_job_status(job_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}") from e


@app_launch.get("/sync/logs/{job_id}")
async def sync_logs(job_id: str) -> dict:
    """
    Return log lines from the sync worker for the given job (from job metadata).
    Returns {"log_lines": list[str]}. Use while polling status to show progress in the applet.
    """
    from rq.exceptions import NoSuchJobError
    from src.app.sync_queue import get_job_logs

    try:
        return get_job_logs(job_id)
    except NoSuchJobError:
        raise HTTPException(status_code=404, detail="Job not found") from None
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}") from e


@app_launch.post("/sync-to-tator")
async def sync_to_tator(
    project_id: int = Query(..., description="Tator project ID"),
    version_id: int = Query(..., description="Tator version ID (required for update)"),
    api_url: str = Query(..., description="Tator REST API base URL"),
    token: str = Query(..., description="Tator API token"),
    port: int = Query(..., description="Port for this project"),
    dataset_name: str | None = Query(
        None,
        description="FiftyOne dataset name (default: project_name_v{version_id}_{port})",
    ),
    label_attr: str = Query("Label", description="Tator attribute name for label"),
    score_attr: str | None = Query(
        None,
        description="Tator attribute name for score/confidence; omit or empty to skip",
    ),
    debug: bool = Query(
        False,
        description="Print per-sample SKIP/UPDATE debug (or set FIFTYONE_SYNC_DEBUG=1)",
    ),
) -> dict:
    """
    Push FiftyOne dataset edits (labels, confidence) back to Tator localizations.
    Requires the dataset to exist (run POST /sync first). Uses elemental_id to match samples.
    """
    from src.app.sync import sync_edits_to_tator

    project_name = None
    try:
        import tator

        api = tator.get_api(_resolve_api_url(api_url), token)
        proj = api.get_project(project_id)
        project_name = getattr(proj, "name", None) or str(project_id)
    except Exception as e:
        logger.warning(f"sync_to_tator get_project({project_id}) failed: {e}")
    if not project_name or not str(project_name).strip():
        project_name = str(project_id)
    try:
        result = sync_edits_to_tator(
            project_id=project_id,
            version_id=version_id,
            port=port,
            api_url=_resolve_api_url(api_url),
            token=token,
            dataset_name=dataset_name,
            label_attr=label_attr,
            score_attr=score_attr,
            debug=debug,
            project_name=project_name.strip(),
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/metrics")
@app.get("/voxel51/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


# Include routers after routes are defined
app.include_router(router)
app.include_router(app_launch)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
