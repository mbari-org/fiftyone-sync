# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_media_resume.py
# Description: Tests for resuming interrupted media loads and batching for very large syncs.

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import fiftyone as fo
import pytest
from PIL import Image

import src.app.sync as sync


class _PagedMediaApi:
    def __init__(self, ids):
        self.ids = ids
        self.calls: list[dict] = []

    def get_media_list(self, project_id, **kwargs):
        self.calls.append(kwargs)
        after = kwargs.get("after")
        remaining = [i for i in self.ids if after is None or i > after]
        return [SimpleNamespace(id=i) for i in remaining[: kwargs["stop"]]]


def test_fetch_project_media_ids_paginates_with_after(monkeypatch):
    api = _PagedMediaApi(list(range(1, 6)))
    monkeypatch.setattr(sync.tator, "get_api", lambda *_a, **_k: api)
    monkeypatch.setattr(sync, "_MEDIA_LIST_PAGE_SIZE", 2)

    ids = sync.fetch_project_media_ids("http://tator.example", "tok", project_id=7)

    assert ids == [1, 2, 3, 4, 5]
    assert [c.get("after") for c in api.calls] == [None, 2, 4]


def test_save_png_atomic_leaves_no_partial_file(tmp_path, monkeypatch):
    out = tmp_path / "e1.png"
    img = Image.new("RGB", (4, 4))

    def _fail(path, format=None):
        Path(path).write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(img, "save", _fail)
    with pytest.raises(OSError):
        sync._save_png_atomic(img, out)

    assert not out.exists()
    assert list(tmp_path.iterdir()) == []


def test_run_crop_pipeline_resumes_remaining_media_in_batches(monkeypatch, tmp_path):
    localizations = tmp_path / "localizations.jsonl"
    rows = [{"elemental_id": f"eid-{mid}", "media": mid} for mid in range(1, 6)]
    localizations.write_text("".join(json.dumps(r) + "\n" for r in rows))
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    crops_dir = tmp_path / "crops"
    # Media 1 was fully cropped by an earlier, interrupted run.
    (crops_dir / "1_a.png").mkdir(parents=True)
    Image.new("RGB", (4, 4)).save(crops_dir / "1_a.png" / "eid-1.png")

    monkeypatch.setenv("FIFTYONE_SYNC_MEDIA_DOWNLOAD_BATCH", "2")
    monkeypatch.setattr(
        sync,
        "_resolve_localizations_jsonl",
        lambda *a, **k: (str(localizations), [1, 2, 3, 4, 5], True),
    )
    monkeypatch.setattr(sync, "is_classification_project", lambda *_a, **_k: False)
    monkeypatch.setattr(sync, "_download_dir", lambda _pid: str(download_dir))
    monkeypatch.setattr(sync, "_crops_dir", lambda _pid, _vid, **_kw: str(crops_dir))
    monkeypatch.setattr(sync, "_load_crop_manifest", lambda *_a, **_k: {})
    monkeypatch.setattr(sync, "_save_crop_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(sync, "_cleanup_download_dir", lambda *_a, **_k: None)

    fetched: list[list[int]] = []
    monkeypatch.setattr(
        sync,
        "get_media_chunked",
        lambda _api, _pid, ids, **_k: fetched.append(list(ids)) or [],
    )
    processed: list[list[int]] = []

    def _fake_download(_api, _pid, ids, _by_id, locs_by_media, *_a, **_k):
        processed.append(list(ids))
        assert set(locs_by_media) == set(ids)
        return str(download_dir), [], len(ids), 0

    monkeypatch.setattr(sync, "_download_and_crop_media_sequentially", _fake_download)

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
    )

    assert out["status"] == "ok"
    assert fetched == [[2, 3], [4, 5]]
    assert processed == [[2, 3], [4, 5]]
    assert out["cache_hits"] == 1
    assert out["num_cropped"] == 4


class _FakeDataset:
    def __init__(self, samples=None):
        self.samples = list(samples or [])
        self.add_calls: list[int] = []
        self.persistent = False

    def __len__(self):
        return len(self.samples)

    def add_samples(self, samples):
        self.add_calls.append(len(samples))
        self.samples.extend(samples)

    def values(self, field, **_k):
        if field == "id":
            return [str(i) for i in range(len(self.samples))]
        return [s[field] for s in self.samples]

    def iter_samples(self, **_k):
        return iter(list(self.samples))

    def delete_samples(self, _ids):
        raise AssertionError("no samples should be deleted")

    def get_field_schema(self):
        return {}

    def create_index(self, _path):
        pass


def _write_crops(crops_dir: Path, n: int) -> list[dict]:
    locs = []
    for i in range(n):
        stem_dir = crops_dir / f"{i}_img{i}.png"
        stem_dir.mkdir(parents=True)
        Image.new("RGB", (4, 4)).save(stem_dir / f"eid-{i}.png")
        locs.append(
            {
                "id": i,
                "elemental_id": f"eid-{i}",
                "media": i,
                "modified_datetime": "2026-01-01T00:00:00+00:00",
                "attributes": {"Label": "fish"},
            }
        )
    return locs


def test_build_new_dataset_adds_samples_in_batches(monkeypatch, tmp_path):
    crops_dir = tmp_path / "crops"
    locs = _write_crops(crops_dir, 5)
    jsonl = tmp_path / "locs.jsonl"
    jsonl.write_text("".join(json.dumps(l) + "\n" for l in locs))

    created: list[_FakeDataset] = []
    monkeypatch.setenv("FIFTYONE_SYNC_DATASET_ADD_BATCH", "2")
    monkeypatch.setattr(sync.fo, "list_datasets", lambda: [])
    monkeypatch.setattr(
        sync.fo, "Dataset", lambda _name: created.append(_FakeDataset()) or created[-1]
    )

    sync.build_fiftyone_dataset_from_crops(str(crops_dir), str(jsonl), "ds")

    assert created[0].persistent is True
    assert created[0].add_calls == [2, 2, 1]


def test_build_existing_partial_dataset_adds_only_remainder(monkeypatch, tmp_path):
    crops_dir = tmp_path / "crops"
    locs = _write_crops(crops_dir, 5)
    jsonl = tmp_path / "locs.jsonl"
    jsonl.write_text("".join(json.dumps(l) + "\n" for l in locs))

    loaded_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    existing = []
    for i in range(3):
        s = fo.Sample(filepath=str(crops_dir / f"{i}_img{i}.png" / f"eid-{i}.png"))
        s["elemental_id"] = f"eid-{i}"
        s[sync.TATOR_MODIFIED_AT_FIELD] = loaded_at
        s["top1_prediction"] = fo.Classification(label="fish")
        existing.append(s)
    partial = _FakeDataset(existing)

    monkeypatch.setenv("FIFTYONE_SYNC_DATASET_ADD_BATCH", "1")
    monkeypatch.setattr(sync.fo, "list_datasets", lambda: ["ds"])
    monkeypatch.setattr(sync.fo, "load_dataset", lambda _name: partial)
    monkeypatch.setattr(sync, "repair_undeclared_sample_fields", lambda *_a, **_k: None)

    sync.build_fiftyone_dataset_from_crops(str(crops_dir), str(jsonl), "ds")

    assert partial.add_calls == [1, 1]
    assert sorted(s["elemental_id"] for s in partial.samples) == [
        f"eid-{i}" for i in range(5)
    ]
