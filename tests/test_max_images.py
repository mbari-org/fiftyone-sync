# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_max_images.py
# Description: Tests for FIFTYONE_SYNC_MAX_IMAGES random sampling and cache matching.

import json
import random

import src.app.sync as sync


def test_max_images_limit_from_env(monkeypatch):
    monkeypatch.setenv("FIFTYONE_SYNC_MAX_IMAGES", "250")
    assert sync._max_images_limit() == 250


def test_max_images_limit_env_overrides_config(monkeypatch):
    monkeypatch.setenv("FIFTYONE_SYNC_MAX_IMAGES", "10")
    assert sync._max_images_limit({"max_samples": 500}) == 10


def test_max_images_limit_from_config_when_env_unset(monkeypatch):
    monkeypatch.delenv("FIFTYONE_SYNC_MAX_IMAGES", raising=False)
    assert sync._max_images_limit({"max_samples": 500}) == 500


def test_max_images_limit_unset_means_unlimited(monkeypatch):
    monkeypatch.delenv("FIFTYONE_SYNC_MAX_IMAGES", raising=False)
    assert sync._max_images_limit() is None
    assert sync._max_images_limit({}) is None


def test_max_images_limit_zero_or_invalid_is_unlimited(monkeypatch):
    monkeypatch.setenv("FIFTYONE_SYNC_MAX_IMAGES", "0")
    assert sync._max_images_limit() is None
    monkeypatch.setenv("FIFTYONE_SYNC_MAX_IMAGES", "nope")
    assert sync._max_images_limit({"max_samples": 12}) == 12


def test_jsonl_cache_matches_full_count():
    assert sync._jsonl_cache_matches(100, 100) is True
    assert sync._jsonl_cache_matches(50, 100) is False
    assert sync._jsonl_cache_matches(100, None) is False


def test_jsonl_cache_matches_sampled_file():
    assert sync._jsonl_cache_matches(10, 100, max_images=10) is True
    assert sync._jsonl_cache_matches(10, 100, max_images=20) is False
    assert sync._jsonl_cache_matches(10, 10, max_images=10) is True


def test_sample_jsonl_random_noop_when_under_cap(tmp_path):
    path = tmp_path / "locs.jsonl"
    rows = [{"elemental_id": f"e{i}", "media": i} for i in range(3)]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    kept = sync._sample_jsonl_random(str(path), 10)
    assert kept == 3
    line_count, media_ids = sync._localizations_jsonl_line_count_and_media_ids(
        str(path)
    )
    assert line_count == 3
    assert media_ids == [0, 1, 2]


def test_sample_jsonl_random_keeps_max_n(tmp_path):
    path = tmp_path / "locs.jsonl"
    rows = [{"elemental_id": f"e{i}", "media": i} for i in range(10)]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    kept = sync._sample_jsonl_random(str(path), 4, rng=random.Random(0))
    assert kept == 4
    line_count, media_ids = sync._localizations_jsonl_line_count_and_media_ids(
        str(path)
    )
    assert line_count == 4
    assert len(media_ids) == 4
    assert set(media_ids) <= set(range(10))


def test_sample_jsonl_random_is_deterministic_with_rng(tmp_path):
    rows = [{"elemental_id": f"e{i}", "media": i} for i in range(8)]
    text = "".join(json.dumps(r) + "\n" for r in rows)
    path_a = tmp_path / "a.jsonl"
    path_b = tmp_path / "b.jsonl"
    path_a.write_text(text)
    path_b.write_text(text)
    sync._sample_jsonl_random(str(path_a), 3, rng=random.Random(7))
    sync._sample_jsonl_random(str(path_b), 3, rng=random.Random(7))
    assert path_a.read_text() == path_b.read_text()


def test_run_crop_pipeline_samples_jsonl_over_cap(monkeypatch, tmp_path):
    localizations = tmp_path / "localizations.jsonl"
    rows = [{"elemental_id": f"eid-{i}", "media": 1} for i in range(5)]
    localizations.write_text("".join(json.dumps(r) + "\n" for r in rows))
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    crops_dir = tmp_path / "crops"
    crops_dir.mkdir()

    monkeypatch.setattr(
        sync,
        "_resolve_localizations_jsonl",
        lambda *args, **kwargs: (str(localizations), [1], True),
    )
    monkeypatch.setattr(sync, "is_classification_project", lambda *_a, **_k: False)
    monkeypatch.setattr(sync, "_download_dir", lambda _pid: str(download_dir))
    monkeypatch.setattr(sync, "_crops_dir", lambda _pid, _vid, **_kw: str(crops_dir))
    monkeypatch.setattr(sync, "_load_crop_manifest", lambda *_a, **_k: {})
    monkeypatch.setattr(sync, "_cleanup_deleted_crops", lambda *_a, **_k: 0)

    def _no_misses(**kwargs):
        _locs, manifest, _media_ids = sync._load_localizations_list_and_manifest(
            kwargs["localizations_jsonl_path"]
        )
        return (set(), [], manifest)

    monkeypatch.setattr(sync, "_find_crop_cache_misses", _no_misses)
    monkeypatch.setattr(sync, "_patch_manifest_stems", lambda *_a, **_k: None)
    monkeypatch.setattr(sync, "_save_crop_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(sync, "_cleanup_download_dir", lambda *_a, **_k: None)

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
        max_images=2,
    )
    assert out["status"] == "ok"
    line_count, _ = sync._localizations_jsonl_line_count_and_media_ids(
        str(localizations)
    )
    assert line_count == 2
    assert out["cache_hits"] == 2
    assert out["cache_misses"] == 0
