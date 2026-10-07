# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_sync_filters.py
# Description: Unit tests for Tator section/query/label sync filter helpers.

from src.app.sync_filters import (
    filter_slug,
    localization_fetch_kwargs,
    localization_id_query,
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


def test_parse_include_classes_from_string():
    from src.app.sync_filters import parse_include_classes

    assert parse_include_classes("Larvacean, Copepod") == ["Larvacean", "Copepod"]
    assert parse_include_classes("  a, a, b  ") == ["a", "b"]
    assert parse_include_classes(None) == []


def test_include_classes_slug_is_order_independent():
    from src.app.sync_filters import include_classes_slug

    assert include_classes_slug(["B", "A"]) == include_classes_slug(["A", "B"])
    assert include_classes_slug(["A"]).startswith("l")
    assert len(include_classes_slug(["A"])) == 13


def test_filter_slug_include_classes():
    from src.app.sync_filters import include_classes_slug

    assert filter_slug(include_classes=["Larvacean", "Copepod"]) == include_classes_slug(
        ["Copepod", "Larvacean"]
    )


def test_localization_fetch_kwargs_include_class():
    import base64
    import json

    kw = localization_fetch_kwargs(
        version_id=3, verified_only=True, include_classes=["Larvacean"]
    )
    assert kw["version"] == [3]
    assert kw["attribute"] == ["verified::true"]
    spec = json.loads(base64.b64decode(kw["encoded_search"]))
    assert spec == {"attribute": "Label", "operation": "eq", "value": "Larvacean"}


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


def test_media_fetch_kwargs_include_classes_uses_related_search():
    import base64
    import json

    kw = media_fetch_kwargs(
        version_id=5, verified_only=True, include_classes=["Larvacean", "Copepod"]
    )
    assert kw["related_attribute"] == ["$version::5", "verified::true"]
    spec = json.loads(base64.b64decode(kw["encoded_related_search"]))
    assert spec == {
        "method": "or",
        "operations": [
            {"attribute": "Label", "operation": "eq", "value": "Larvacean"},
            {"attribute": "Label", "operation": "eq", "value": "Copepod"},
        ],
    }


def test_media_fetch_kwargs_blank_include_classes_is_omitted():
    assert media_fetch_kwargs(include_classes=["  "]) == {}


def test_scoped_data_dir_includes_filter_slug(tmp_path):
    path = scoped_data_dir(
        str(tmp_path), 1, 10, section_id=3, query="q"
    )
    assert path == str(
        tmp_path / "data" / "1" / "v10" / filter_slug(section_id=3, query="q")
    )


def test_localization_id_query_none_without_labels():
    assert localization_id_query(query="abc") == (None, "abc")
    assert localization_id_query() == (None, None)


def test_localization_id_query_ands_decodable_query_into_body():
    from src.app.sync_filters import encode_object_search

    existing = {"attribute": "$frame", "operation": "gt", "value": 10}
    body, leftover = localization_id_query(
        query=encode_object_search(existing),
        include_classes=["Larvacean"],
        media_ids=[4, 5],
    )
    assert leftover is None
    assert body == {
        "object_search": {
            "method": "and",
            "operations": [
                existing,
                {"attribute": "Label", "operation": "eq", "value": "Larvacean"},
            ],
        },
        "media_ids": [4, 5],
    }


def test_localization_id_query_keeps_undecodable_query_as_param():
    body, leftover = localization_id_query(query="%%%", include_classes=["A"])
    assert leftover == "%%%"
    assert body == {"object_search": {"attribute": "Label", "operation": "eq", "value": "A"}}


def test_media_fetch_kwargs_media_labels_use_own_attributes():
    import base64
    import json

    kw = media_fetch_kwargs(
        version_id=128, verified_only=True, include_classes=["A"], media_labels=True
    )
    assert kw["related_attribute"] == ["$version::128"]
    assert kw["attribute"] == ["verified::true"]
    assert "encoded_related_search" not in kw
    assert json.loads(base64.b64decode(kw["encoded_search"]))["value"] == "A"
