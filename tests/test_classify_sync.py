# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_classify_sync.py
# Description: Tests for classification-project sync (padded whole-image crops, label from media attribute).

import json
from types import SimpleNamespace

import src.app.sync as sync


def test_is_classification_project_detects_label_attr(monkeypatch):
    monkeypatch.setattr(
        sync,
        "_get_image_media_type_and_attr_names",
        lambda _api, _pid: (7, ["Label", "score"]),
    )
    assert sync.is_classification_project(object(), 1) is True

    monkeypatch.setattr(
        sync,
        "_get_image_media_type_and_attr_names",
        lambda _api, _pid: (7, ["score", "depth"]),
    )
    assert sync.is_classification_project(object(), 1) is False


def test_media_to_classification_loc_full_frame_and_label():
    media = SimpleNamespace(
        id=42,
        elemental_id="abc-123",
        type=7,
        version=3,
        attributes={"Label": "Krill", "score": 0.9},
        modified_datetime="2024-01-01T00:00:00Z",
        created_datetime="2023-01-01T00:00:00Z",
    )
    loc = sync._media_to_classification_loc(media)
    assert loc is not None
    assert loc["media"] == 42
    assert loc["elemental_id"] == "abc-123"
    assert (loc["x"], loc["y"], loc["width"], loc["height"]) == (0.0, 0.0, 1.0, 1.0)
    assert loc["attributes"]["Label"] == "Krill"
    assert loc["_classification"] is True
    # Ground truth comes from the media "Label" attribute (not a localization label).
    assert sync._get_label_from_loc(loc) == "Krill"


def test_media_to_classification_loc_elemental_id_fallback():
    media = SimpleNamespace(id=99, elemental_id=None, attributes={"Label": "Salp"})
    loc = sync._media_to_classification_loc(media)
    assert loc["elemental_id"] == "m99"


def test_pad_to_square_preserves_aspect_and_centers():
    from PIL import Image

    img = Image.new("RGB", (400, 100), color=(10, 20, 30))
    squared = sync._pad_to_square(img)
    assert squared.size == (400, 400)
    # Content is centered vertically: rows < 150 and >= 250 are black padding.
    assert squared.getpixel((200, 5)) == (0, 0, 0)
    assert squared.getpixel((200, 200)) == (10, 20, 30)


def test_classification_crop_pads_and_resizes_whole_image(tmp_path):
    from PIL import Image

    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    crops_dir = tmp_path / "crops"
    crops_dir.mkdir()

    # Non-square source so a plain resize (distort) differs from pad-then-resize.
    src = download_dir / "5_image.jpg"
    Image.new("RGB", (400, 100), color=(10, 20, 30)).save(src)

    localizations = tmp_path / "localizations.jsonl"
    loc = {
        "elemental_id": "eid-5",
        "media": 5,
        "x": 0.0,
        "y": 0.0,
        "width": 1.0,
        "height": 1.0,
        "_classification": True,
    }
    localizations.write_text(json.dumps(loc) + "\n")

    num_ok, num_fail = sync.crop_localizations_parallel(
        str(download_dir),
        str(localizations),
        str(crops_dir),
        size=224,
        locs_to_crop=[loc],
        classification=True,
    )
    assert (num_ok, num_fail) == (1, 0)

    out_file = crops_dir / "5_image" / "eid-5.png"
    assert out_file.exists()
    with Image.open(out_file) as crop:
        crop = crop.convert("RGB")
        assert crop.size == (224, 224)
        # Top band is padding (black); center band is the source color. A plain
        # (non-padded) resize would make the whole image the source color.
        r, g, b = crop.getpixel((112, 3))
        assert r < 30 and g < 30 and b < 30
        assert crop.getpixel((112, 112)) == (10, 20, 30)


def test_run_crop_pipeline_routes_to_classification(monkeypatch, tmp_path):
    localizations = tmp_path / "localizations.jsonl"
    localizations.write_text("{}\n")
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    crops_dir = tmp_path / "crops"
    crops_dir.mkdir()

    loc = {"elemental_id": "eid-1", "media": 1}
    updated_manifest = {
        "eid-1": {"modified_at": "t1", "media_id": 1, "media_stem": "1_img"},
    }

    monkeypatch.setattr(sync, "is_classification_project", lambda _api, _pid: True)

    classify_called = {}

    def _fake_classify_resolve(*_args, **_kwargs):
        classify_called["yes"] = True
        return (str(localizations), [1], False)

    def _fail_loc_resolve(*_args, **_kwargs):
        raise AssertionError("non-classification resolver must not be called")

    monkeypatch.setattr(
        sync, "_resolve_classification_localizations_jsonl", _fake_classify_resolve
    )
    monkeypatch.setattr(sync, "_resolve_localizations_jsonl", _fail_loc_resolve)
    monkeypatch.setattr(sync, "_download_dir", lambda _pid: str(download_dir))
    monkeypatch.setattr(sync, "_crops_dir", lambda _pid, _vid, **_kw: str(crops_dir))
    monkeypatch.setattr(sync, "_load_crop_manifest", lambda *_a, **_k: {})
    monkeypatch.setattr(sync, "_cleanup_deleted_crops", lambda *_a, **_k: 0)
    monkeypatch.setattr(
        sync,
        "_find_crop_cache_misses",
        lambda **kwargs: ({1}, [loc], updated_manifest),
    )
    monkeypatch.setattr(sync, "get_media_chunked", lambda *_a, **_k: [])
    monkeypatch.setattr(sync, "_patch_manifest_stems", lambda *_a, **_k: None)
    monkeypatch.setattr(sync, "_save_crop_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(sync, "_cleanup_download_dir", lambda *_a, **_k: None)
    monkeypatch.setattr(sync, "_cleanup_downloaded_videos", lambda *_a: None)

    captured = {}

    def _fake_crop(*_args, **kwargs):
        captured["classification"] = kwargs.get("classification")
        captured["locs_to_crop"] = kwargs.get("locs_to_crop")
        return (1, 0)

    monkeypatch.setattr(sync, "crop_localizations_parallel", _fake_crop)

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
        s3_bucket=None,
        s3_crops_prefix=None,
    )
    assert out["status"] == "ok"
    assert classify_called.get("yes") is True
    assert captured["classification"] is True
    assert captured["locs_to_crop"] == [loc]
