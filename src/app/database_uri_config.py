# fiftyone-sync, Apache-2.0 license
# Filename: src/app/database_uri_config.py
# Description: YAML-only configuration for per-project MongoDB URIs and FiftyOne app ports.
"""
Database URI config: YAML-only configuration for per-project MongoDB URIs and FiftyOne app ports.
See database_uris.example.yaml for structure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def database_name_from_uri(uri: str) -> str:
    """Extract MongoDB database name from URI (path component). Default 'fiftyone' if missing."""
    from urllib.parse import urlparse

    p = urlparse(uri)
    name = (p.path or "").strip("/")
    return name or "fiftyone"


@dataclass(frozen=True)
class DatabaseEntry:
    """Single database: URI and FiftyOne app port."""

    uri: str
    port: int


@dataclass
class VssProjectConfig:
    """VSS project configuration with S3 settings. Embedding service URL is global (embeddings.service_url)."""

    vss_project: str
    s3_bucket: str | None = None
    s3_prefix: str | None = None


@dataclass
class ProjectConfig:
    """Per-project config: optional vss_project (single or dict), optional S3 (bucket/prefix for crop image upload), and list of databases (uri/port). Key is project name."""

    vss_project: str | None = None  # Legacy single VSS project (backward compatibility)
    vss_projects: dict[str, VssProjectConfig] = field(
        default_factory=dict
    )  # New nested VSS projects
    s3_bucket: str | None = None
    s3_prefix: str | None = None
    databases: list[DatabaseEntry] = field(default_factory=list)


def _parse_bool(value: Any) -> bool:
    """Parse YAML value to bool. Accepts bool, or string 'true'/'false'/'yes'/'1'."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "yes", "1")
    return bool(value)


@dataclass
class DatabaseUriConfig:
    """
    Root config: projects keyed by project name -> ProjectConfig.
    Load from YAML only via from_yaml_path() or from_yaml_string().
    """

    projects: dict[str, ProjectConfig] = field(default_factory=dict)
    is_enterprise: bool = False

    @classmethod
    def from_yaml_path(cls, path: str | Path) -> DatabaseUriConfig:
        """Load config from a YAML file. Only .yaml/.yml paths accepted (no JSON)."""
        path = Path(path)
        suffix = path.suffix.lower()
        if suffix not in (".yaml", ".yml"):
            raise ValueError(
                f"Database URI config must be YAML; path must end with .yaml or .yml, got: {path!r}"
            )
        if not path.is_file():
            raise FileNotFoundError(f"Database URI config file not found: {path!r}")
        with open(path, "r", encoding="utf-8") as f:
            return cls.from_yaml_string(f.read())

    @classmethod
    def from_yaml_string(cls, content: str) -> DatabaseUriConfig:
        """Load config from a YAML string. JSON is not accepted."""
        raw = yaml.safe_load(content)
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError(
                f"Database URI config YAML must be a mapping at top level, got {type(raw).__name__}"
            )
        is_enterprise = _parse_bool(raw.get("is_enterprise", False))
        projects_raw = raw.get("projects")
        if projects_raw is None:
            return cls(projects={}, is_enterprise=is_enterprise)
        if not isinstance(projects_raw, dict):
            raise ValueError(
                f"config 'projects' must be a mapping, got {type(projects_raw).__name__}"
            )
        projects: dict[str, ProjectConfig] = {}
        for key, proj in projects_raw.items():
            if not isinstance(proj, dict):
                raise ValueError(
                    f"project {key!r} must be a mapping, got {type(proj).__name__}"
                )

            # Parse legacy single vss_project
            vss_project = proj.get("vss_project")
            if vss_project is not None and not isinstance(vss_project, str):
                raise ValueError(
                    f"project {key!r} 'vss_project' must be a string if present, got {type(vss_project).__name__}"
                )

            # Parse new nested vss_projects
            vss_projects_raw = proj.get("vss_projects")
            vss_projects_dict: dict[str, VssProjectConfig] = {}
            if vss_projects_raw is not None:
                if not isinstance(vss_projects_raw, dict):
                    raise ValueError(
                        f"project {key!r} 'vss_projects' must be a mapping, got {type(vss_projects_raw).__name__}"
                    )
                for vss_key, vss_config in vss_projects_raw.items():
                    if not isinstance(vss_config, dict):
                        raise ValueError(
                            f"project {key!r} vss_projects[{vss_key!r}] must be a mapping, got {type(vss_config).__name__}"
                        )
                    vss_proj_name = vss_config.get("vss_project")
                    if not isinstance(vss_proj_name, str):
                        raise ValueError(
                            f"project {key!r} vss_projects[{vss_key!r}] 'vss_project' must be a string, got {type(vss_proj_name).__name__}"
                        )
                    vss_s3_bucket = vss_config.get("s3_bucket")
                    if vss_s3_bucket is not None and not isinstance(vss_s3_bucket, str):
                        raise ValueError(
                            f"project {key!r} vss_projects[{vss_key!r}] 's3_bucket' must be a string if present, got {type(vss_s3_bucket).__name__}"
                        )
                    vss_s3_prefix = vss_config.get("s3_prefix")
                    if vss_s3_prefix is not None and not isinstance(vss_s3_prefix, str):
                        raise ValueError(
                            f"project {key!r} vss_projects[{vss_key!r}] 's3_prefix' must be a string if present, got {type(vss_s3_prefix).__name__}"
                        )
                    vss_projects_dict[str(vss_key)] = VssProjectConfig(
                        vss_project=vss_proj_name,
                        s3_bucket=(vss_s3_bucket.strip() or None)
                        if (vss_s3_bucket and isinstance(vss_s3_bucket, str))
                        else None,
                        s3_prefix=(vss_s3_prefix.strip() or None)
                        if (vss_s3_prefix and isinstance(vss_s3_prefix, str))
                        else None,
                    )

            # Parse legacy top-level S3 config
            s3_bucket = proj.get("s3_bucket")
            if s3_bucket is not None and not isinstance(s3_bucket, str):
                raise ValueError(
                    f"project {key!r} 's3_bucket' must be a string if present, got {type(s3_bucket).__name__}"
                )
            s3_prefix = proj.get("s3_prefix")
            if s3_prefix is not None and not isinstance(s3_prefix, str):
                raise ValueError(
                    f"project {key!r} 's3_prefix' must be a string if present, got {type(s3_prefix).__name__}"
                )

            db_list_raw = proj.get("databases")
            if db_list_raw is None:
                db_list_raw = []
            if not isinstance(db_list_raw, list):
                raise ValueError(
                    f"project {key!r} 'databases' must be a list, got {type(db_list_raw).__name__}"
                )
            entries: list[DatabaseEntry] = []
            for i, db in enumerate(db_list_raw):
                if not isinstance(db, dict):
                    raise ValueError(
                        f"project {key!r} databases[{i}] must be a mapping, got {type(db).__name__}"
                    )
                uri = db.get("uri")
                port = db.get("port")
                if not isinstance(uri, str):
                    raise ValueError(
                        f"project {key!r} databases[{i}] 'uri' must be a string, got {type(uri).__name__}"
                    )
                if not isinstance(port, int):
                    raise ValueError(
                        f"project {key!r} databases[{i}] 'port' must be an int, got {type(port).__name__}"
                    )
                entries.append(DatabaseEntry(uri=uri, port=port))
            projects[str(key)] = ProjectConfig(
                vss_project=vss_project,
                vss_projects=vss_projects_dict,
                s3_bucket=(s3_bucket.strip() or None)
                if (s3_bucket and isinstance(s3_bucket, str))
                else None,
                s3_prefix=(s3_prefix.strip() or None)
                if (s3_prefix and isinstance(s3_prefix, str))
                else None,
                databases=entries,
            )
        return cls(projects=projects, is_enterprise=is_enterprise)
