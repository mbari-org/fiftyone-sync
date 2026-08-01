# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_sync_filters.py
# Description: Unit tests for Tator section/query sync filter helpers.

from src.app.sync_filters import (
    filter_slug,
    localization_fetch_kwargs,
    media_fetch_kwargs,
    scoped_data_dir,
)


def test_filter_slug_empty():
    assert filter_slug() == ""
    assert filter_slug(section_id=None, query=None) == ""


def test_filter_slug_section_only():
    assert filter_slug(section_id=42) == "s42"


def test_filter_slug_query_only():
    slug = filter_slug(query="abc123")
    assert slug.startswith("q")
    assert len(slug) == 13


def test_filter_slug_section_and_query():
    slug = filter_slug(section_id=7, query="encoded")
    assert slug.startswith("s7_q")


def test_filter_slug_localization_type_only():
    assert filter_slug(localization_type_id=15) == "t15"


def test_filter_slug_section_and_localization_type():
    assert filter_slug(section_id=7, localization_type_id=15) == "s7_t15"


def test_localization_fetch_kwargs_version_section_query():
    kw = localization_fetch_kwargs(version_id=3, section_id=9, query="b64query")
    assert kw == {"version": [3], "section": 9, "encoded_search": "b64query"}


def test_localization_fetch_kwargs_localization_type():
    kw = localization_fetch_kwargs(version_id=3, localization_type_id=15)
    assert kw == {"version": [3], "type": [15]}


def test_localization_fetch_kwargs_strips_query():
    kw = localization_fetch_kwargs(query="  q  ")
    assert kw == {"encoded_search": "q"}


def test_localization_fetch_kwargs_verified_only():
    kw = localization_fetch_kwargs(version_id=3, verified_only=True)
    assert kw == {"version": [3], "attribute": ["verified::true"]}


def test_localization_fetch_kwargs_verified_only_false_omits_attribute():
    kw = localization_fetch_kwargs(version_id=3, verified_only=False)
    assert "attribute" not in kw


def test_media_fetch_kwargs_version_and_section():
    kw = media_fetch_kwargs(version_id=5, section_id=2)
    assert kw == {"related_attribute": ["$version::5"], "section": 2}


def test_media_fetch_kwargs_verified_only():
    kw = media_fetch_kwargs(verified_only=True)
    assert kw == {"related_attribute": ["verified::true"]}


def test_media_fetch_kwargs_version_and_verified_only_combine():
    kw = media_fetch_kwargs(version_id=5, section_id=2, verified_only=True)
    assert kw == {
        "related_attribute": ["$version::5", "verified::true"],
        "section": 2,
    }


def test_scoped_data_dir_includes_filter_slug(tmp_path):
    path = scoped_data_dir(
        str(tmp_path), 1, 10, section_id=3, query="q"
    )
    assert path == str(
        tmp_path / "data" / "1" / "v10" / filter_slug(section_id=3, query="q")
    )
