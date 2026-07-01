# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_sync_to_tator.py
# Description: Tests for sync-to-tator resolution of localization vs classification (media) samples.

from types import SimpleNamespace
from unittest.mock import MagicMock

import src.app.sync as sync


def test_fetch_localizations_logs_unresolved(monkeypatch):
    api = MagicMock()
    api.get_localization_list_by_id.return_value = []

    sync._fetch_localizations_by_elemental_ids(
        api, project_id=1, version_id=2, elemental_ids=["loc-a", "loc-b"]
    )

    api.get_localization_list_by_id.assert_called_once()
    call_kw = api.get_localization_list_by_id.call_args.kwargs
    assert call_kw["localization_id_query"] == {"elemental_ids": ["loc-a", "loc-b"]}


def test_fetch_media_by_elemental_ids_uses_batch_then_fallback(monkeypatch):
    api = MagicMock()
    api.get_media_list_by_id.side_effect = RuntimeError("batch unsupported")
    api.get_media_list.side_effect = [
        [SimpleNamespace(id=10, elemental_id="media-a")],
        [],
    ]

    out = sync._fetch_media_by_elemental_ids(
        api, project_id=1, elemental_ids=["media-a", "media-b"]
    )

    assert set(out.keys()) == {"media-a"}
    assert out["media-a"].id == 10
    assert api.get_media_list.call_count == 2


def _mock_sample(elemental_id, label, *, is_classification=False, media_id=None):
    fields = {
        "elemental_id": elemental_id,
        "ground_truth": SimpleNamespace(label=label, confidence=1.0),
    }
    if is_classification:
        fields["is_classification"] = True
    if media_id is not None:
        fields["tator_media_id"] = media_id

    class _Sample:
        def __init__(self):
            self.id = elemental_id
            self.last_modified_at = 100.0
            self.created_at = 1.0
            self.save = MagicMock()
            self._fields = fields

        def __contains__(self, key):
            return key in self._fields

        def __getitem__(self, key):
            return self._fields[key]

        def __setitem__(self, key, value):
            self._fields[key] = value

    return _Sample()


def test_do_sync_edits_routes_unresolved_to_media(monkeypatch):
    sample_loc = _mock_sample("loc-eid", "Fish")
    sample_cls = _mock_sample(
        "media-eid", "Krill", is_classification=True, media_id=42
    )

    dataset = MagicMock()
    dataset.__len__ = lambda self: 2
    dataset.iter_samples.return_value = [sample_loc, sample_cls]
    monkeypatch.setattr(sync.fo, "load_dataset", lambda _name: dataset)

    loc_patch = MagicMock()
    media_patch = MagicMock()
    monkeypatch.setattr(sync, "_fetch_localizations_by_elemental_ids", lambda *_a, **_k: {})
    monkeypatch.setattr(
        sync,
        "_fetch_media_by_elemental_ids",
        lambda _api, _pid, eids: {
            "media-eid": SimpleNamespace(id=42, elemental_id="media-eid")
        },
    )
    monkeypatch.setattr(sync, "_bulk_patch_localizations_by_elemental_id", loc_patch)
    monkeypatch.setattr(sync, "_bulk_patch_media_by_elemental_id", media_patch)

    out = sync._do_sync_edits_to_tator(
        api=MagicMock(),
        project_id=1,
        version_id=2,
        ds_name="test_ds",
        label_attr="Label",
        score_attr="score",
        debug=False,
        force_sync=True,
    )

    assert loc_patch.call_count == 0
    media_patch.assert_called_once()
    media_eids = media_patch.call_args.args[2]
    assert media_eids == {"media-eid": {"Label": "Krill", "score": 1.0}}
    assert out["updated"] == 1
    assert out["failed"] == 1
    assert any("loc-eid" in e for e in out["errors"])
