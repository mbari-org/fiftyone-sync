# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_dataset_rename.py
# Description: Unit tests for dataset rename helpers and rename_dataset_for_version.

import pytest

from src.app import sync


def test_prepare_new_dataset_name_sanitizes_and_strips():
    assert sync._prepare_new_dataset_name("Midwater Time Series!!") == (
        "Midwater_Time_Series"
    )


def test_prepare_new_dataset_name_truncates_to_60_chars():
    long_name = "a" * 100
    result = sync._prepare_new_dataset_name(long_name)
    assert len(result) == sync.MAX_DATASET_NAME_LENGTH
    assert result == "a" * 60


def test_prepare_new_dataset_name_truncation_strips_trailing_separator():
    # 59 'a's + "_" lands exactly at the 60-char boundary; the trailing
    # separator exposed by truncation should be stripped off.
    name = ("a" * 59) + "_" + "cccc"
    result = sync._prepare_new_dataset_name(name)
    assert result == "a" * 59
    assert not result.endswith("_")
    assert not result.endswith("-")


class _FakeProject:
    name = "MidwaterTimeSeries"


class _FakeApi:
    def get_project(self, project_id):
        return _FakeProject()


class _FakeDataset:
    def __init__(self, name):
        self.name = name


def _patch_common(monkeypatch, *, existing_datasets, section_id=None):
    monkeypatch.setattr(sync.tator, "get_api", lambda *_a, **_k: _FakeApi())
    monkeypatch.setattr(sync, "get_database_name", lambda *_a, **_k: "fiftyone_project_1")
    monkeypatch.setattr(sync, "get_database_uri", lambda *_a, **_k: "mongodb://localhost:27017")
    monkeypatch.setattr(sync, "get_is_enterprise", lambda: False)
    monkeypatch.setattr(sync, "_test_mongodb_connection", lambda *_a, **_k: None)
    monkeypatch.setattr(sync.fo, "list_datasets", lambda: list(existing_datasets))

    loaded = {}

    def _load_dataset(name):
        ds = _FakeDataset(name)
        loaded["dataset"] = ds
        return ds

    monkeypatch.setattr(sync.fo, "load_dataset", _load_dataset)
    return loaded


def test_rename_dataset_for_version_success(monkeypatch):
    existing = ["MidwaterTimeSeries_v82_s477_5151", "other_v1_5151"]
    loaded = _patch_common(monkeypatch, existing_datasets=existing)

    result = sync.rename_dataset_for_version(
        project_id=1,
        version_id=82,
        port=5151,
        api_url="http://example.com",
        token="tok",
        new_name="Zooplankton QC pass",
        section_id=477,
    )

    assert result["status"] == "ok"
    assert result["old_name"] == "MidwaterTimeSeries_v82_s477_5151"
    assert result["new_name"] == "Zooplankton_QC_pass"
    assert loaded["dataset"].name == "Zooplankton_QC_pass"


def test_rename_dataset_for_version_truncates_long_name(monkeypatch):
    existing = ["MidwaterTimeSeries_v82_s477_5151"]
    _patch_common(monkeypatch, existing_datasets=existing)

    result = sync.rename_dataset_for_version(
        project_id=1,
        version_id=82,
        port=5151,
        api_url="http://example.com",
        token="tok",
        new_name="x" * 100,
        section_id=477,
    )

    assert len(result["new_name"]) == sync.MAX_DATASET_NAME_LENGTH


def test_rename_dataset_for_version_no_dataset_found(monkeypatch):
    _patch_common(monkeypatch, existing_datasets=[])

    result = sync.rename_dataset_for_version(
        project_id=1,
        version_id=82,
        port=5151,
        api_url="http://example.com",
        token="tok",
        new_name="new name",
        section_id=477,
    )

    assert result["status"] == "ok"
    assert result["old_name"] is None
    assert result["new_name"] is None


def test_rename_dataset_for_version_noop_when_name_unchanged(monkeypatch):
    existing = ["MidwaterTimeSeries_v82_s477_5151"]
    _patch_common(monkeypatch, existing_datasets=existing)

    result = sync.rename_dataset_for_version(
        project_id=1,
        version_id=82,
        port=5151,
        api_url="http://example.com",
        token="tok",
        new_name="MidwaterTimeSeries_v82_s477_5151",
        section_id=477,
    )

    assert result["old_name"] == result["new_name"] == "MidwaterTimeSeries_v82_s477_5151"
    assert "identical" in result["message"]


def test_rename_dataset_for_version_rejects_name_collision(monkeypatch):
    existing = ["MidwaterTimeSeries_v82_s477_5151", "Zooplankton_QC_pass"]
    _patch_common(monkeypatch, existing_datasets=existing)

    with pytest.raises(ValueError, match="already exists"):
        sync.rename_dataset_for_version(
            project_id=1,
            version_id=82,
            port=5151,
            api_url="http://example.com",
            token="tok",
            new_name="Zooplankton QC pass",
            section_id=477,
        )
