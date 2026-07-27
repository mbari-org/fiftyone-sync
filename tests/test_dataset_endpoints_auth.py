# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_dataset_endpoints_auth.py
# Description: Tests that dataset-management endpoints accept a Tator token via
# either the Authorization header or a `token` query parameter.

import sys
from types import SimpleNamespace

import pytest


def _write_config(tmp_path):
    config_path = tmp_path / "sync-config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "projects:",
                "  DemoProject:",
                "    databases:",
                "      - uri: mongodb://localhost:27017/demo",
                "        port: 5151",
            ]
        )
    )
    return config_path


def _load_main_module(monkeypatch, tmp_path):
    config_path = _write_config(tmp_path)
    monkeypatch.setenv("FIFTYONE_SYNC_CONFIG_PATH", str(config_path))
    try:
        if "src.app.main" in sys.modules:
            main_module = sys.modules["src.app.main"]
        else:
            import src.app.main as main_module
    except Exception as exc:
        pytest.skip(f"FastAPI app import unavailable in this env: {exc}")
    return main_module


def test_resolve_token_prefers_explicit_query_param(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)
    assert (
        main_module._resolve_token("Token from-header", "from-query")
        == "from-query"
    )


def test_resolve_token_falls_back_to_authorization_header(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)
    assert main_module._resolve_token("Token from-header", None) == "from-header"
    assert main_module._resolve_token("Bearer from-header", "") == "from-header"


def test_resolve_token_none_when_neither_provided(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)
    assert main_module._resolve_token(None, None) is None
    assert main_module._resolve_token("", "") is None


def _fake_tator(monkeypatch):
    fake_api = SimpleNamespace(
        get_project=lambda _pid: SimpleNamespace(name="DemoProject")
    )
    fake_tator_module = SimpleNamespace(get_api=lambda _url, _token: fake_api)
    monkeypatch.setitem(sys.modules, "tator", fake_tator_module)


def test_datasets_endpoint_accepts_query_param_token(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)
    _fake_tator(monkeypatch)

    import src.app.sync as sync

    monkeypatch.setattr(
        sync,
        "list_available_datasets",
        lambda **_kw: {"datasets": ["a", "b"], "database_name": "fiftyone_project_1"},
    )

    from fastapi.testclient import TestClient

    client = TestClient(main_module.app)
    response = client.get(
        "/datasets",
        params={
            "project_id": 1,
            "api_url": "http://localhost:8080",
            "port": 5151,
            "token": "abc123",
        },
    )
    assert response.status_code == 200
    assert response.json()["datasets"] == ["a", "b"]


def test_datasets_endpoint_401_without_any_token(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)

    from fastapi.testclient import TestClient

    client = TestClient(main_module.app)
    response = client.get(
        "/datasets",
        params={
            "project_id": 1,
            "api_url": "http://localhost:8080",
            "port": 5151,
        },
    )
    assert response.status_code == 401


def test_rename_dataset_endpoint_accepts_query_param_token(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)
    _fake_tator(monkeypatch)

    captured = {}

    def _fake_rename(**kwargs):
        captured.update(kwargs)
        return {
            "status": "ok",
            "old_name": "DemoProject_v82_5151",
            "new_name": "Zooplankton_QC_pass",
            "database_name": "fiftyone_project_1",
        }

    import src.app.sync as sync

    monkeypatch.setattr(sync, "rename_dataset_for_version", _fake_rename)

    from fastapi.testclient import TestClient

    client = TestClient(main_module.app)
    response = client.post(
        "/rename-dataset",
        params={
            "project_id": 1,
            "version_id": 82,
            "api_url": "http://localhost:8080",
            "port": 5151,
            "new_name": "Zooplankton QC pass",
            "token": "abc123",
        },
    )
    assert response.status_code == 200
    assert response.json()["new_name"] == "Zooplankton_QC_pass"
    assert captured["token"] == "abc123"


def test_rename_dataset_endpoint_401_without_any_token(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)

    from fastapi.testclient import TestClient

    client = TestClient(main_module.app)
    response = client.post(
        "/rename-dataset",
        params={
            "project_id": 1,
            "version_id": 82,
            "api_url": "http://localhost:8080",
            "port": 5151,
            "new_name": "Zooplankton QC pass",
        },
    )
    assert response.status_code == 401
