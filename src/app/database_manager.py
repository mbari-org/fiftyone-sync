# fiftyone-sync, Apache-2.0 license
# Filename: src/app/database_manager.py
# Description: Maps (project_id, port) to database URI/name from YAML config.
"""
Database manager: maps (project_id, port) to a database (URI + name).
Uses mandatory YAML config (FIFTYONE_SYNC_CONFIG_PATH) keyed by project name
for per-project, per-port databases.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from src.app.database_uri_config import (
    DatabaseEntry,
    DatabaseUriConfig,
    database_name_from_uri,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Print to console
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Cached config lookup: key is (project_name: str, port: int), value is DatabaseEntry.
_config: dict[tuple[str, int], DatabaseEntry] | None = None

# Full YAML config (project name -> ProjectConfig).
_yaml_config: DatabaseUriConfig | None = None

# Optional registry: project_id -> project_name (set by run_sync_job so workers can resolve without API).
_project_id_to_name: dict[int, str] = {}


def require_sync_config_path() -> None:
    """
    Require FIFTYONE_SYNC_CONFIG_PATH to be set and point to an existing file.
    Call at startup; aborts the process if not configured (this config is mandatory).
    """
    path = os.environ.get("FIFTYONE_SYNC_CONFIG_PATH", "").strip()
    if not path:
        print("FIFTYONE_SYNC_CONFIG_PATH is required and must be set.", file=sys.stderr)
        sys.exit(1)
    if not Path(path).is_file():
        print(
            f"FIFTYONE_SYNC_CONFIG_PATH must point to an existing file: {path!r}",
            file=sys.stderr,
        )
        sys.exit(1)


def _load_config() -> dict[tuple[str, int], DatabaseEntry] | None:
    global _config, _yaml_config
    if _config is not None:
        return _config
    path = os.environ.get("FIFTYONE_SYNC_CONFIG_PATH", "").strip()
    if not path or not Path(path).is_file():
        _config = None
        _yaml_config = None
        return None
    try:
        _config = {}
        _yaml_config = DatabaseUriConfig.from_yaml_path(path)
        logger.info(
            f"loaded config from {path}: projects={list(_yaml_config.projects.keys())}"
        )
        for project_name, project_config in _yaml_config.projects.items():
            logger.info(
                f"project_name={project_name!r}, databases={[db.port for db in project_config.databases]}"
            )
            for db in project_config.databases:
                key = (project_name, db.port)
                _config[key] = db
        # Log config with string keys (JSON requires str keys, not tuples)
        log_config = {
            f"{k[0]!r}:{k[1]}": {"uri": v.uri, "port": v.port}
            for k, v in _config.items()
        }
        logger.info(f"log config: {json.dumps(log_config, indent=2)}")
        return _config
    except Exception as e:
        logger.warning(f"Failed to load FIFTYONE_SYNC_CONFIG_PATH from {path}: {e}")
        _config = None
        _yaml_config = None
        return None


def get_vss_projects_list(project_name: str) -> list[dict[str, str]]:
    """Return list of available VSS projects for a Tator project.
    Returns list of dicts with 'key' and 'name' (vss_project value).
    For legacy single vss_project, returns [{'key': 'default', 'name': vss_project}].
    Embedding service URL is global (embeddings.service_url), not per-project."""
    _load_config()
    if not _yaml_config:
        path = os.environ.get("FIFTYONE_SYNC_CONFIG_PATH", "").strip()
        logger.warning(
            f"get_vss_projects_list: _yaml_config is None (FIFTYONE_SYNC_CONFIG_PATH={path!r}) — check env var is set in the API container"
        )
        return []
    if not project_name or not project_name.strip():
        return []
    proj = _yaml_config.projects.get(project_name.strip())
    if not proj:
        logger.warning(
            f"get_vss_projects_list: project {project_name.strip()!r} not found in config; available={list(_yaml_config.projects.keys())}"
        )
        return []

    result = []
    # New nested vss_projects
    if proj.vss_projects:
        for key, vss_config in proj.vss_projects.items():
            result.append(
                {
                    "key": key,
                    "name": vss_config.vss_project,
                }
            )
    # Legacy single vss_project (backward compatibility)
    elif proj.vss_project:
        result.append(
            {
                "key": "default",
                "name": proj.vss_project,
            }
        )
    return result


def get_vss_project_config(
    project_name: str, vss_project_key: str | None = None
) -> dict[str, str | None] | None:
    """Return VSS project configuration for a specific key.
    Returns dict with vss_project, s3_bucket, s3_prefix. Embedding service URL is global (embeddings.service_url).
    If vss_project_key is None, returns first/only VSS project or legacy config."""
    _load_config()
    if not _yaml_config or not project_name or not project_name.strip():
        return None
    proj = _yaml_config.projects.get(project_name.strip())
    if not proj:
        return None

    # New nested vss_projects
    if proj.vss_projects:
        if vss_project_key:
            vss_config = proj.vss_projects.get(vss_project_key)
            if vss_config:
                return {
                    "vss_project": vss_config.vss_project,
                    "s3_bucket": vss_config.s3_bucket,
                    "s3_prefix": vss_config.s3_prefix,
                }
        else:
            # No key specified, return first if only one exists
            if len(proj.vss_projects) == 1:
                vss_config = next(iter(proj.vss_projects.values()))
                return {
                    "vss_project": vss_config.vss_project,
                    "s3_bucket": vss_config.s3_bucket,
                    "s3_prefix": vss_config.s3_prefix,
                }

    # Legacy single vss_project (backward compatibility)
    if proj.vss_project:
        return {
            "vss_project": proj.vss_project,
            "s3_bucket": proj.s3_bucket,
            "s3_prefix": proj.s3_prefix,
        }

    return None


def get_is_enterprise() -> bool:
    """Return is_enterprise from config. When True: enables S3 upload and fo.config.database_uri is not set (FiftyOne uses its own config). Default False when no config."""
    _load_config()
    return getattr(_yaml_config, "is_enterprise", False) if _yaml_config else False


def get_s3_config(
    project_id: int, project_name: str | None = None, vss_project_key: str | None = None
) -> dict[str, str | None] | None:
    """
    Return S3 config for this project when configured: {"s3_bucket": str, "s3_prefix": str | None}.
    Only returns when project has valid config and s3_bucket is set. Used to show S3 field in applet.
    If vss_project_key is provided, retrieves S3 config from that specific VSS project config.
    """
    _load_config()
    if not _yaml_config:
        return None
    name = (project_name or _project_id_to_name.get(project_id) or "").strip() or str(
        project_id
    )
    proj = _yaml_config.projects.get(name)
    if not proj:
        return None

    s3_bucket = None
    s3_prefix = None

    # Get S3 config from specific VSS project if key provided
    if vss_project_key and proj.vss_projects:
        vss_config = proj.vss_projects.get(vss_project_key)
        if vss_config:
            s3_bucket = vss_config.s3_bucket
            s3_prefix = vss_config.s3_prefix

    # Fallback to legacy top-level S3 config
    if not s3_bucket:
        s3_bucket = getattr(proj, "s3_bucket", None)
        s3_prefix = getattr(proj, "s3_prefix", None)

    if not s3_bucket:
        return None

    return {
        "s3_bucket": (s3_bucket or "").strip(),
        "s3_prefix": (s3_prefix or "").strip() or None,
    }


def register_project_id_name(project_id: int, project_name: str) -> None:
    """Register project_id -> project_name so get_database_entry can resolve when project_name is not passed."""
    if project_id and project_name and str(project_name).strip():
        _project_id_to_name[project_id] = str(project_name).strip()


def get_database_entry(
    project_id: int, port: int, project_name: str | None = None
) -> DatabaseEntry | None:
    """
    Resolve FiftyOne MongoDB database entry for this project and port.
    Config is keyed by (project_name, port). Uses project_name if provided,
    else _project_id_to_name[project_id], else str(project_id).
    """
    cfg = _load_config()
    if not cfg or not project_id:
        return None
    name = (project_name or _project_id_to_name.get(project_id) or "").strip() or str(
        project_id
    )
    key = (name, port)
    logger.debug(
        f"get_database_entry: project_id={project_id}, port={port}, resolved_name={name!r}, key={key}, available_keys={list(cfg.keys())}"
    )
    return cfg.get(key)


def get_database_entry_or_enterprise_default(
    project_id: int, port: int, project_name: str | None = None
) -> DatabaseEntry | None:
    """
    Like get_database_entry, but for enterprise (is_enterprise=True) returns a synthetic
    entry using port and FIFTYONE_SYNC_CONFIG_PATH / default naming when no config entry exists.
    Callers can avoid requiring FIFTYONE_SYNC_CONFIG_PATH for enterprise.
    """
    entry = get_database_entry(project_id, port, project_name=project_name)
    if entry is not None:
        return entry
    if not get_is_enterprise():
        return None
    uri = get_database_uri(project_id, port, project_name=project_name)
    if not uri:
        uri = "mongodb://localhost:27017/" + get_database_name(
            project_id, port, project_name=project_name
        )
    return DatabaseEntry(uri=uri, port=port)


def get_database_uri(
    project_id: int, port: int, project_name: str | None = None
) -> str | None:
    """Return MongoDB URI for (project, port). Uses config or FIFTYONE_DATABASE_URI env."""
    entry = get_database_entry(project_id, port, project_name=project_name)
    if entry:
        return entry.uri
    return os.environ.get("FIFTYONE_DATABASE_URI", "").strip() or None


def get_database_name(
    project_id: int, port: int, project_name: str | None = None
) -> str:
    """Return MongoDB database name for (project, port). Uses config or default naming."""
    entry = get_database_entry(project_id, port, project_name=project_name)
    if entry:
        return database_name_from_uri(entry.uri)
    default = os.environ.get("FIFTYONE_DATABASE_DEFAULT", "fiftyone_project")
    return f"{default}_{project_id}"


def get_port_for_project(project_id: int, project_name: str | None = None) -> int:
    """Return first configured port for this project, or 5151 + (project_id - 1)."""
    cfg = _load_config()
    name = (project_name or _project_id_to_name.get(project_id) or "").strip() or str(
        project_id
    )
    if cfg:
        for (pname, port), _ in cfg.items():
            if pname == name:
                return port
    return 5151 + (project_id - 1)


