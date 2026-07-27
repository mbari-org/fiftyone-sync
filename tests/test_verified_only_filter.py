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
