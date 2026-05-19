import importlib
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


def test_recompute_crops_endpoint_enqueue(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)

    fake_api = SimpleNamespace(get_project=lambda _pid: SimpleNamespace(name="DemoProject"))
    fake_tator = SimpleNamespace(get_api=lambda _url, _token: fake_api)
    monkeypatch.setitem(sys.modules, "tator", fake_tator)

    from src.app import sync_queue

    captured = {}

    def _fake_enqueue(**kwargs):
        captured.update(kwargs)
        return "job-123"

    monkeypatch.setattr(sync_queue, "enqueue_recompute_crops", _fake_enqueue)

    from fastapi.testclient import TestClient

    client = TestClient(main_module.app)
    response = client.post(
        "/recompute-crops",
        params={
            "project_id": 1,
            "version_id": 42,
            "api_url": "http://localhost:8080",
            "token": "abc",
            "port": 5151,
            "force": "true",
            "force_sync": "true",
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "job_id": "job-123",
        "status": "queued",
        "port": 5151,
        "version_id": 42,
    }
    assert captured["project_name"] == "DemoProject"
    assert captured["force"] is True
    assert captured["force_sync"] is True


def test_recompute_crops_status_and_logs_routes(monkeypatch, tmp_path):
    main_module = _load_main_module(monkeypatch, tmp_path)

    from src.app import sync_queue

    monkeypatch.setattr(
        sync_queue, "get_job_status", lambda _job_id: {"status": "finished"}
    )
    monkeypatch.setattr(
        sync_queue, "get_job_logs", lambda _job_id: {"log_lines": ["line-a"]}
    )

    from fastapi.testclient import TestClient

    client = TestClient(main_module.app)
    status_resp = client.get("/recompute-crops/status/job-123")
    logs_resp = client.get("/recompute-crops/logs/job-123")
    assert status_resp.status_code == 200
    assert status_resp.json() == {"status": "finished"}
    assert logs_resp.status_code == 200
    assert logs_resp.json() == {"log_lines": ["line-a"]}


def test_run_crop_pipeline_miss_only(monkeypatch, tmp_path):
    import src.app.sync as sync

    localizations = tmp_path / "localizations.jsonl"
    localizations.write_text("{}\n")
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    crops_dir = tmp_path / "crops"
    crops_dir.mkdir()

    loc = {"elemental_id": "eid-1", "media": 1}
    updated_manifest = {
        "eid-1": {"modified_at": "t1", "media_id": 1, "media_stem": "1_img"},
        "eid-2": {"modified_at": "t2", "media_id": 2, "media_stem": "2_img"},
    }

    monkeypatch.setattr(
        sync,
        "_resolve_localizations_jsonl",
        lambda *args, **kwargs: (str(localizations), [1, 2], False),
    )
    monkeypatch.setattr(sync, "_download_dir", lambda _pid: str(download_dir))
    monkeypatch.setattr(sync, "_crops_dir", lambda _pid, _vid: str(crops_dir))
    monkeypatch.setattr(sync, "_load_crop_manifest", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(sync, "_cleanup_deleted_crops", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        sync,
        "_find_crop_cache_misses",
        lambda **kwargs: ({1}, [loc], updated_manifest),
    )
    monkeypatch.setattr(sync, "get_media_chunked", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        sync, "_patch_manifest_stems", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(sync, "_save_crop_manifest", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sync, "_cleanup_download_dir", lambda *_args, **_kwargs: None)

    captured = {}

    def _fake_crop(*args, **kwargs):
        captured["locs_to_crop"] = kwargs.get("locs_to_crop")
        return (1, 0)

    monkeypatch.setattr(sync, "crop_localizations_parallel", _fake_crop)
    monkeypatch.setattr(sync, "_cleanup_downloaded_videos", lambda *_args: None)

    out = sync._run_crop_pipeline(
        object(),
        project_id=1,
        version_id=42,
        api_url="http://localhost:8080",
        token="abc",
        force_sync=False,
        force=False,
        media_id_batch_size=10,
        localization_batch_size=10,
        s3_bucket=None,
        s3_crops_prefix=None,
    )
    assert out["status"] == "ok"
    assert out["cache_misses"] == 1
    assert out["cache_hits"] == 1
    assert out["num_cropped"] == 1
    assert out["removed_existing"] == 0
    assert captured["locs_to_crop"] == [loc]


def test_run_crop_pipeline_force_all(monkeypatch, tmp_path):
    import src.app.sync as sync

    localizations = tmp_path / "localizations.jsonl"
    localizations.write_text("{}\n")
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    crops_dir = tmp_path / "crops"
    crops_dir.mkdir()

    locs = [
        {"elemental_id": "eid-1", "media": 1},
        {"elemental_id": "eid-2", "media": 1},
    ]
    updated_manifest = {
        "eid-1": {"modified_at": "t1", "media_id": 1, "media_stem": "1_img"},
        "eid-2": {"modified_at": "t2", "media_id": 1, "media_stem": "1_img"},
    }

    monkeypatch.setattr(
        sync,
        "_resolve_localizations_jsonl",
        lambda *args, **kwargs: (str(localizations), [1], True),
    )
    monkeypatch.setattr(sync, "_download_dir", lambda _pid: str(download_dir))
    monkeypatch.setattr(sync, "_crops_dir", lambda _pid, _vid: str(crops_dir))
    monkeypatch.setattr(
        sync, "_load_crop_manifest", lambda *_args, **_kwargs: {"old": {}}
    )
    monkeypatch.setattr(sync, "_cleanup_deleted_crops", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        sync,
        "_load_localizations_list_and_manifest",
        lambda _path: (locs, updated_manifest, {1}),
    )
    monkeypatch.setattr(sync, "get_media_chunked", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        sync, "_patch_manifest_stems", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(sync, "_save_crop_manifest", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(sync, "_cleanup_download_dir", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        sync, "_delete_existing_crop_files", lambda *_args, **_kwargs: 2
    )
    monkeypatch.setattr(sync, "_cleanup_downloaded_videos", lambda *_args: None)

    captured = {}

    def _fake_crop(*args, **kwargs):
        captured["locs_to_crop"] = kwargs.get("locs_to_crop")
        return (2, 0)

    monkeypatch.setattr(sync, "crop_localizations_parallel", _fake_crop)

    out = sync._run_crop_pipeline(
        object(),
        project_id=1,
        version_id=42,
        api_url="http://localhost:8080",
        token="abc",
        force_sync=False,
        force=True,
        media_id_batch_size=10,
        localization_batch_size=10,
        s3_bucket=None,
        s3_crops_prefix=None,
    )
    assert out["status"] == "ok"
    assert out["cache_misses"] == 2
    assert out["cache_hits"] == 0
    assert out["num_cropped"] == 2
    assert out["removed_existing"] == 2
    assert captured["locs_to_crop"] == locs

