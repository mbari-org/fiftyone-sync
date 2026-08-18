# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_dataset_build_memory.py
# Description: Dataset build/reconcile stay bounded in memory (GitHub issue 35).

from types import SimpleNamespace

import src.app.sync as sync
from tests.test_verified_only_filter import _FakeReconcileDataset, _FakeSample, _make_loc


def test_iter_crop_filepaths_streams_and_skips_non_images(tmp_path):
    assert sync._crop_suffixes(["*.png", "*.JPG"]) == {".png", ".jpg"}
    media = tmp_path / "1_frame"
    media.mkdir()
    (media / "eid-a.png").write_bytes(b"x")
    (media / "notes.txt").write_text("nope")
    paths = list(sync._iter_crop_filepaths(str(tmp_path), ["*.png"]))
    assert len(paths) == 1 and paths[0].endswith(".png")
    assert not isinstance(sync._iter_crop_filepaths(str(tmp_path), ["*.png"]), list)


def test_build_existing_dataset_skips_crop_walk(monkeypatch, tmp_path):
    jsonl = tmp_path / "locs.jsonl"
    jsonl.write_text('{"elemental_id": "e1", "media": 1}\n')
    (tmp_path / "crops").mkdir()
    called = {"iter": 0, "reconcile": 0}
    dataset = SimpleNamespace(persistent=False)

    def _boom_iter(*_a, **_k):
        called["iter"] += 1
        raise AssertionError("must not walk crops when the dataset already exists")

    monkeypatch.setattr(sync.fo, "list_datasets", lambda: ["existing"])
    monkeypatch.setattr(sync.fo, "load_dataset", lambda _name: dataset)
    monkeypatch.setattr(sync, "_iter_crop_filepaths", _boom_iter)
    monkeypatch.setattr(
        sync,
        "reconcile_dataset_with_tator",
        lambda **kw: called.__setitem__("reconcile", 1) or kw["dataset"],
    )
    monkeypatch.setattr(sync, "_ensure_field_indexes", lambda _ds: None)
    result = sync.build_fiftyone_dataset_from_crops(
        str(tmp_path / "crops"), str(jsonl), "existing"
    )
    assert result is dataset
    assert called == {"iter": 0, "reconcile": 1}


def test_build_new_dataset_adds_in_batches(monkeypatch, tmp_path):
    jsonl = tmp_path / "locs.jsonl"
    crops = tmp_path / "crops" / "1_media"
    crops.mkdir(parents=True)
    lines = []
    for i in range(250):
        (crops / f"e{i}.png").write_bytes(b"x")
        lines.append(f'{{"elemental_id": "e{i}", "media": 1}}\n')
    jsonl.write_text("".join(lines))
    add_calls: list[int] = []

    class _FakeDataset:
        def __init__(self, _name):
            self.persistent = False

        def add_samples(self, samples):
            add_calls.append(len(samples))

        def __len__(self):
            return sum(add_calls)

    class _LiteSample(dict):
        def __init__(self, filepath):
            super().__init__()
            self.filepath = filepath

    monkeypatch.setattr(sync, "_DATASET_ADD_BATCH_SIZE", 100)
    monkeypatch.setattr(sync.fo, "list_datasets", lambda: [])
    monkeypatch.setattr(sync.fo, "Dataset", _FakeDataset)
    monkeypatch.setattr(sync.fo, "Sample", _LiteSample)
    monkeypatch.setattr(sync.fo, "Classification", lambda **k: k)
    monkeypatch.setattr(sync, "_apply_loc_to_sample", lambda *a, **k: None)
    monkeypatch.setattr(sync, "_ensure_field_indexes", lambda _ds: None)
    dataset = sync.build_fiftyone_dataset_from_crops(
        str(tmp_path / "crops"), str(jsonl), "new-ds"
    )
    assert add_calls == [100, 100, 50]
    assert len(dataset) == 250


def test_reconcile_force_sync_saves_in_batches(monkeypatch):
    monkeypatch.setattr(sync, "repair_undeclared_sample_fields", lambda *a, **k: None)
    monkeypatch.setattr(sync, "_media_id_to_stem_from_crops", lambda *_a, **_k: {})
    monkeypatch.setattr(sync, "_apply_loc_to_sample", lambda *a, **k: None)
    monkeypatch.setattr(sync, "_DATASET_UPDATE_BATCH_SIZE", 2)
    saves: list[str] = []

    class _SavingSample(_FakeSample):
        def save(self):
            saves.append(self.id)

    samples = [_SavingSample(f"s{i}", f"eid-{i}") for i in range(5)]
    dataset = _FakeReconcileDataset(samples)
    loc_index = {
        f"eid-{i}": _make_loc({"Label": "A"}, elemental_id=f"eid-{i}")
        for i in range(5)
    }
    sync.reconcile_dataset_with_tator(
        dataset=dataset,
        loc_index=loc_index,
        crops_dir="/tmp/crops",
        download_dir=None,
        config={"force_sync": True},
        max_samples=None,
    )
    assert saves == ["s0", "s1", "s2", "s3", "s4"]
    assert dataset.save_batches == [["s0", "s1"], ["s2", "s3"], ["s4"]]
    assert dataset.added == []
