# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_classify_sync.py
# Description: Tests for combined classification+detection sync (whole-image resize + box crop, per elemental_id).

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


class _FakeMedia(sync.tator.models.Media):
    def __init__(self, mid, label, mtype=7, eid=None):
        self.id = mid
        self.elemental_id = eid or f"eid-{mid}"
        self.type = mtype
        self.version = 1
        self.attributes = {"Label": label} if label is not None else {}
        self.modified_datetime = None
        self.created_datetime = None


def test_fetch_and_save_classification_localizations_requires_label(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sync, "_get_image_media_type_and_attr_names", lambda _a, _p: (7, ["Label"])
    )
    out = tmp_path / "locs.jsonl"
    # Seed a detection localization; classification samples must be appended, not overwrite.
    out.write_text(json.dumps({"elemental_id": "det-1", "media": 1}) + "\n")

    media = [
        _FakeMedia(1, "Krill"),
        _FakeMedia(2, ""),      # empty label -> skipped
        _FakeMedia(3, None),    # no label -> skipped
        _FakeMedia(4, "Salp", mtype=99),  # wrong media type -> skipped
    ]
    written = sync.fetch_and_save_classification_localizations(
        object(), 1, media, out_path=str(out), mode="a", require_label=True
    )
    assert written == 1
    lines = [json.loads(x) for x in out.read_text().splitlines() if x.strip()]
    assert len(lines) == 2  # detection + one classification
    assert lines[0]["elemental_id"] == "det-1"
    assert lines[1]["_classification"] is True
    assert lines[1]["attributes"]["Label"] == "Krill"


def test_combined_box_and_classification_crops(tmp_path):
    from PIL import Image

    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    crops_dir = tmp_path / "crops"
    crops_dir.mkdir()

    # Non-square source so pad-then-resize differs from a distorting plain resize.
    src = download_dir / "5_image.jpg"
    Image.new("RGB", (400, 100), color=(10, 20, 30)).save(src)

    box_loc = {
        "elemental_id": "box-5",
        "media": 5,
        "x": 0.1,
        "y": 0.1,
        "width": 0.2,
        "height": 0.2,
    }
    class_loc = {
        "elemental_id": "cls-5",
        "media": 5,
        "x": 0.0,
        "y": 0.0,
        "width": 1.0,
        "height": 1.0,
        "_classification": True,
    }
    localizations = tmp_path / "localizations.jsonl"
    localizations.write_text(
        json.dumps(box_loc) + "\n" + json.dumps(class_loc) + "\n"
    )

    num_ok, num_fail = sync.crop_localizations_parallel(
        str(download_dir),
        str(localizations),
        str(crops_dir),
        size=224,
        locs_to_crop=[box_loc, class_loc],
    )
    assert (num_ok, num_fail) == (2, 0)

    box_file = crops_dir / "5_image" / "box-5.png"
    cls_file = crops_dir / "5_image" / "cls-5.png"
    assert box_file.exists()
    assert cls_file.exists()

    with Image.open(cls_file) as crop:
        crop = crop.convert("RGB")
        assert crop.size == (224, 224)
        # Whole-image sample is padded: top band is black, center is source color.
        r, g, b = crop.getpixel((112, 3))
        assert r < 30 and g < 30 and b < 30
        assert crop.getpixel((112, 112)) == (10, 20, 30)


def test_run_crop_pipeline_appends_classification_for_labeled_project(
    monkeypatch, tmp_path
):
    localizations = tmp_path / "localizations.jsonl"
    localizations.write_text(json.dumps({"elemental_id": "det-1", "media": 1}) + "\n")
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    crops_dir = tmp_path / "crops"
    crops_dir.mkdir()

    loc = {"elemental_id": "det-1", "media": 1}
    updated_manifest = {
        "det-1": {"modified_at": "t1", "media_id": 1, "media_stem": "1_img"},
    }

    monkeypatch.setattr(
        sync,
        "_resolve_localizations_jsonl",
        lambda *_a, **_k: (str(localizations), [1], False),
    )
    monkeypatch.setattr(sync, "is_classification_project", lambda _api, _pid: True)

    appended = {}

    def _fake_append(*_a, **kwargs):
        appended["path"] = kwargs.get("localizations_path")
        return 3

    monkeypatch.setattr(
        sync, "_append_classification_localizations_to_jsonl", _fake_append
    )
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
    monkeypatch.setattr(sync, "crop_localizations_parallel", lambda *_a, **_k: (1, 0))

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
    assert appended.get("path") == str(localizations)
