# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_verified_only_query_filters.py
# Description: Tests that verified_only is pushed into the Tator media/localization
# queries (related_attribute / attribute) so unverified data is never downloaded.

import json

from src.app import sync


class _FakeMedia:
    def __init__(self, media_id):
        self.id = media_id


class _FakeApi:
    """Captures kwargs passed to get_media_list / get_localization_* calls."""

    def __init__(self, media_ids=None, localizations=None):
        self.media_ids = media_ids or []
        self.localizations = localizations or []
        self.get_media_list_calls: list[dict] = []
        self.get_localization_count_calls: list[dict] = []
        self.get_localization_list_calls: list[dict] = []

    def get_media_list(self, project_id, **kwargs):
        self.get_media_list_calls.append(kwargs)
        return [_FakeMedia(mid) for mid in self.media_ids]

    def get_localization_count(self, project_id, **kwargs):
        self.get_localization_count_calls.append(kwargs)
        return len(self.localizations)

    def get_localization_list(self, project_id, **kwargs):
        self.get_localization_list_calls.append(kwargs)
        if kwargs.get("after") is not None:
            return []
        return list(self.localizations)


def test_fetch_project_media_ids_verified_only_sets_related_attribute(monkeypatch):
    fake_api = _FakeApi(media_ids=[1, 2])
    monkeypatch.setattr(sync.tator, "get_api", lambda *_a, **_k: fake_api)

    media_ids = sync.fetch_project_media_ids(
        "http://tator.example", "tok", project_id=7, verified_only=True
    )

    assert media_ids == [1, 2]
    assert fake_api.get_media_list_calls == [{"related_attribute": ["verified::true"]}]


def test_fetch_project_media_ids_default_omits_related_attribute(monkeypatch):
    fake_api = _FakeApi(media_ids=[1])
    monkeypatch.setattr(sync.tator, "get_api", lambda *_a, **_k: fake_api)

    sync.fetch_project_media_ids("http://tator.example", "tok", project_id=7)

    assert fake_api.get_media_list_calls == [{}]


def test_fetch_project_media_ids_combines_version_and_verified_only(monkeypatch):
    fake_api = _FakeApi(media_ids=[1])
    monkeypatch.setattr(sync.tator, "get_api", lambda *_a, **_k: fake_api)

    sync.fetch_project_media_ids(
        "http://tator.example",
        "tok",
        project_id=7,
        version_id=21,
        verified_only=True,
    )

    assert fake_api.get_media_list_calls == [
        {"related_attribute": ["$version::21", "verified::true"]}
    ]


class _FakeLoc:
    def __init__(self, loc_id, elemental_id, verified):
        self.id = loc_id
        self._data = {
            "id": loc_id,
            "elemental_id": elemental_id,
            "attributes": {"verified": verified},
        }

    def to_dict(self):
        return self._data


def test_fetch_and_save_localizations_verified_only_sets_attribute_filter(
    monkeypatch, tmp_path
):
    verified_loc = _FakeLoc(1, "eid-verified", True)
    fake_api = _FakeApi(localizations=[verified_loc])
    monkeypatch.setattr(
        sync,
        "_localizations_jsonl_path",
        lambda *_a, **_k: str(tmp_path / "localizations.jsonl"),
    )

    out_path = sync.fetch_and_save_localizations(
        fake_api, project_id=7, verified_only=True
    )

    # Both the count check and the paginated list call should carry the filter.
    assert all(
        kw.get("attribute") == ["verified::true"]
        for kw in fake_api.get_localization_count_calls
    )
    assert any(
        kw.get("attribute") == ["verified::true"]
        for kw in fake_api.get_localization_list_calls
    )

    with open(out_path) as f:
        lines = [json.loads(line) for line in f if line.strip()]
    assert [line["elemental_id"] for line in lines] == ["eid-verified"]


def test_fetch_and_save_localizations_default_omits_attribute_filter(
    monkeypatch, tmp_path
):
    fake_api = _FakeApi(localizations=[_FakeLoc(1, "eid-1", False)])
    monkeypatch.setattr(
        sync,
        "_localizations_jsonl_path",
        lambda *_a, **_k: str(tmp_path / "localizations.jsonl"),
    )

    sync.fetch_and_save_localizations(fake_api, project_id=7)

    assert all(
        "attribute" not in kw for kw in fake_api.get_localization_count_calls
    )
    assert all(
        "attribute" not in kw for kw in fake_api.get_localization_list_calls
    )


def test_resolve_localizations_jsonl_forwards_verified_only(monkeypatch, tmp_path):
    """verified_only must reach both the media-id and localization fetch calls."""
    captured = {}

    def _fake_fetch_project_media_ids(*_args, **kwargs):
        captured["media_kwargs"] = kwargs
        return [1]

    def _fake_fetch_and_save_localizations(*_args, **kwargs):
        captured["loc_kwargs"] = kwargs
        path = tmp_path / "localizations.jsonl"
        path.write_text("")
        return str(path)

    monkeypatch.setattr(
        sync, "fetch_project_media_ids", _fake_fetch_project_media_ids
    )
    monkeypatch.setattr(
        sync, "fetch_and_save_localizations", _fake_fetch_and_save_localizations
    )
    monkeypatch.setattr(
        sync,
        "_localizations_jsonl_path",
        lambda *_a, **_k: str(tmp_path / "does_not_exist.jsonl"),
    )

    sync._resolve_localizations_jsonl(
        object(),
        project_id=7,
        version_id=None,
        api_url="http://tator.example",
        token="tok",
        force_sync=True,
        media_id_batch_size=100,
        localization_batch_size=100,
        verified_only=True,
    )

    assert captured["media_kwargs"].get("verified_only") is True
    assert captured["loc_kwargs"].get("verified_only") is True
