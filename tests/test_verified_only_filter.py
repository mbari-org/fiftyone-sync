# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_verified_only_filter.py
# Description: Tests for the verified_only sync/UI filter (excludes non-verified localizations).

import src.app.sync as sync


def _make_loc(attrs, elemental_id="eid-1", media=1):
    return {
        "elemental_id": elemental_id,
        "media": media,
        "attributes": attrs,
        "modified_datetime": None,
        "created_datetime": None,
    }


def test_loc_is_verified_true_when_attribute_true():
    loc = _make_loc({"verified": True})
    assert sync._loc_is_verified(loc) is True


def test_loc_is_verified_false_when_attribute_false():
    loc = _make_loc({"verified": False})
    assert sync._loc_is_verified(loc) is False


def test_loc_is_verified_false_when_attribute_missing():
    loc = _make_loc({"Label": "Krill"})
    assert sync._loc_is_verified(loc) is False


def test_loc_is_verified_false_when_loc_is_none():
    assert sync._loc_is_verified(None) is False


def test_create_sample_from_loc_verified_only_excludes_unverified():
    loc = _make_loc({"Label": "Krill", "verified": False})
    sample = sync._create_sample_from_loc(
        loc,
        crops_dir="/tmp/crops",
        media_stem="media1",
        include_classes=set(),
        s3_bucket="test-bucket",  # bypasses local filesystem existence check
        verified_only=True,
    )
    assert sample is None


def test_create_sample_from_loc_verified_only_excludes_missing_attribute():
    loc = _make_loc({"Label": "Krill"})
    sample = sync._create_sample_from_loc(
        loc,
        crops_dir="/tmp/crops",
        media_stem="media1",
        include_classes=set(),
        s3_bucket="test-bucket",
        verified_only=True,
    )
    assert sample is None


def test_create_sample_from_loc_verified_only_includes_verified():
    loc = _make_loc({"Label": "Krill", "verified": True})
    sample = sync._create_sample_from_loc(
        loc,
        crops_dir="/tmp/crops",
        media_stem="media1",
        include_classes=set(),
        s3_bucket="test-bucket",
        verified_only=True,
    )
    assert sample is not None
    assert sample["elemental_id"] == "eid-1"


def test_create_sample_from_loc_default_includes_unverified():
    """verified_only defaults to False: unverified localizations are still included."""
    loc = _make_loc({"Label": "Krill", "verified": False})
    sample = sync._create_sample_from_loc(
        loc,
        crops_dir="/tmp/crops",
        media_stem="media1",
        include_classes=set(),
        s3_bucket="test-bucket",
    )
    assert sample is not None


def test_reconcile_forwards_verified_only_to_create_sample_from_loc(monkeypatch):
    """config['verified_only'] set by sync_project_to_fiftyone must reach _create_sample_from_loc."""
    captured_kwargs = {}

    def _fake_create_sample_from_loc(loc, crops_dir, media_stem, include_classes, **kwargs):
        captured_kwargs.update(kwargs)
        return None

    monkeypatch.setattr(sync, "_create_sample_from_loc", _fake_create_sample_from_loc)
    monkeypatch.setattr(sync, "repair_undeclared_sample_fields", lambda *a, **k: None)
    monkeypatch.setattr(sync, "_media_id_to_stem_from_crops", lambda *_a, **_k: {1: "media1"})

    class _FakeDataset:
        def values(self, field, **kwargs):
            return []

        def iter_samples(self, **kwargs):
            return []

        def delete_samples(self, ids):
            pass

        def add_samples(self, samples):
            pass

        def __len__(self):
            return 0

    loc_index = {"eid-1": _make_loc({"Label": "Krill", "verified": False}, media=1)}

    sync.reconcile_dataset_with_tator(
        dataset=_FakeDataset(),
        loc_index=loc_index,
        crops_dir="/tmp/crops",
        download_dir=None,
        config={"verified_only": True},
        max_samples=None,
    )

    assert captured_kwargs.get("verified_only") is True


class _FakeSample:
    """Minimal fo.Sample stand-in for reconcile's remove/update passes."""

    def __init__(self, sample_id, elemental_id):
        self.id = sample_id
        self.elemental_id = elemental_id

    def has_field(self, _name):
        return False

    def __contains__(self, _key):
        return False

    def save(self):
        pass


class _FakeSaveContext:
    """Stand-in for FiftyOne dataset.save_context() that records flush sizes."""

    def __init__(self, dataset, batch_size):
        self._dataset = dataset
        self._batch_size = batch_size or 1000
        self._pending: list = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self._flush()
        return False

    def save(self, sample):
        self._pending.append(sample)
        if len(self._pending) >= self._batch_size:
            self._flush()

    def _flush(self):
        if not self._pending:
            return
        self._dataset.save_batches.append([s.id for s in self._pending])
        for sample in self._pending:
            sample.save()
        self._pending = []


class _FakeReconcileDataset:
    """Minimal fo.Dataset stand-in that tracks delete_samples/add_samples calls."""

    def __init__(self, samples):
        self._samples = list(samples)
        self.deleted_ids: list[str] = []
        self.added: list = []
        self.save_batches: list[list[str]] = []

    def values(self, field, **_kwargs):
        if field == "id":
            return [s.id for s in self._samples]
        if field == "elemental_id":
            return [s.elemental_id for s in self._samples]
        return []

    def iter_samples(self, **_kwargs):
        return list(self._samples)

    def delete_samples(self, ids):
        self.deleted_ids.extend(ids)
        self._samples = [s for s in self._samples if s.id not in ids]

    def add_samples(self, samples):
        self.added.extend(samples)

    def save_context(self, batch_size=None, **_kwargs):
        return _FakeSaveContext(self, batch_size)

    def __len__(self):
        return len(self._samples)


def test_reconcile_removes_samples_that_became_unverified(monkeypatch):
    """
    A sample still present in Tator (not deleted) but whose `verified` attribute
    flipped to False must be removed when verified_only is enabled, and must not
    be re-added by the "add new samples" step (since it's still unverified).
    """
    monkeypatch.setattr(sync, "repair_undeclared_sample_fields", lambda *a, **k: None)
    monkeypatch.setattr(
        sync, "_media_id_to_stem_from_crops", lambda *_a, **_k: {1: "media1", 2: "media2"}
    )
    monkeypatch.setattr(sync, "_apply_loc_to_sample", lambda *a, **k: None)

    keep_sample = _FakeSample("sample-keep", "keep-verified")
    drop_sample = _FakeSample("sample-drop", "drop-unverified")
    dataset = _FakeReconcileDataset([keep_sample, drop_sample])

    loc_index = {
        "keep-verified": _make_loc(
            {"Label": "A", "verified": True}, elemental_id="keep-verified", media=1
        ),
        "drop-unverified": _make_loc(
            {"Label": "B", "verified": False}, elemental_id="drop-unverified", media=2
        ),
    }

    sync.reconcile_dataset_with_tator(
        dataset=dataset,
        loc_index=loc_index,
        crops_dir="/tmp/crops",
        download_dir=None,
        config={"verified_only": True, "s3_bucket": "test-bucket"},
        max_samples=None,
    )

    assert dataset.deleted_ids == ["sample-drop"]
    assert dataset.added == []


def test_reconcile_keeps_unverified_samples_when_verified_only_disabled(monkeypatch):
    """Default behavior (verified_only=False) must not remove unverified samples."""
    monkeypatch.setattr(sync, "repair_undeclared_sample_fields", lambda *a, **k: None)
    monkeypatch.setattr(
        sync, "_media_id_to_stem_from_crops", lambda *_a, **_k: {1: "media1", 2: "media2"}
    )
    monkeypatch.setattr(sync, "_apply_loc_to_sample", lambda *a, **k: None)

    keep_sample = _FakeSample("sample-keep", "keep-verified")
    unverified_sample = _FakeSample("sample-unverified", "still-unverified")
    dataset = _FakeReconcileDataset([keep_sample, unverified_sample])

    loc_index = {
        "keep-verified": _make_loc(
            {"Label": "A", "verified": True}, elemental_id="keep-verified", media=1
        ),
        "still-unverified": _make_loc(
            {"Label": "B", "verified": False}, elemental_id="still-unverified", media=2
        ),
    }

    sync.reconcile_dataset_with_tator(
        dataset=dataset,
        loc_index=loc_index,
        crops_dir="/tmp/crops",
        download_dir=None,
        config={},
        max_samples=None,
    )

    assert dataset.deleted_ids == []
