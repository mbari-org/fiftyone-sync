# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_media_download_cleanup.py
# Description: Tests that downloaded image files are deleted immediately after cropping.

from types import SimpleNamespace

import src.app.sync as sync


def _make_media(mid=101, name="frame.png"):
    return SimpleNamespace(id=mid, name=name)


def test_cleanup_downloaded_image_removes_existing_file(tmp_path):
    download_dir = str(tmp_path)
    file_path = tmp_path / "101_frame.png"
    file_path.write_bytes(b"fake-image-bytes")

    sync._cleanup_downloaded_image(download_dir, 101, "frame.png")

    assert not file_path.exists()


def test_cleanup_downloaded_image_missing_file_is_noop(tmp_path):
    # Should not raise when the file doesn't exist.
    sync._cleanup_downloaded_image(str(tmp_path), 999, "missing.png")


def test_cleanup_downloaded_image_noop_without_download_dir_or_name():
    # No exception when args are empty/falsy.
    sync._cleanup_downloaded_image("", 1, "name.png")
    sync._cleanup_downloaded_image("/tmp/somewhere", 1, "")


def test_download_and_crop_one_media_deletes_downloaded_image(tmp_path, monkeypatch):
    dl_dir = str(tmp_path)
    media_obj = _make_media(mid=202, name="image.png")
    downloaded_path = tmp_path / "202_image.png"

    def fake_save_media_to_tmp(api, project_id, media_objects, media_ids_filter=None):
        # Simulate the real download writing the file to disk.
        downloaded_path.write_bytes(b"fake-image-bytes")
        return dl_dir

    def fake_crop_localizations_parallel(*args, **kwargs):
        # Crop step runs while the downloaded file is still present.
        assert downloaded_path.exists()
        return (1, 0)

    monkeypatch.setattr(sync, "save_media_to_tmp", fake_save_media_to_tmp)
    monkeypatch.setattr(
        sync, "crop_localizations_parallel", fake_crop_localizations_parallel
    )

    mid, returned_media_obj, ok, fail = sync._download_and_crop_one_media(
        api=None,
        project_id=1,
        mid=202,
        media_obj=media_obj,
        locs_for_media=[{"elemental_id": "eid-1"}],
        localizations_path="/tmp/does-not-matter.jsonl",
        crops_dir=str(tmp_path / "crops"),
        dl_dir=dl_dir,
        size=224,
    )

    assert (mid, returned_media_obj, ok, fail) == (202, media_obj, 1, 0)
    # Downloaded source file is gone immediately after crop, mirroring the
    # video cleanup path.
    assert not downloaded_path.exists()


def test_download_and_crop_one_media_skips_cleanup_without_media_obj(
    tmp_path, monkeypatch
):
    dl_dir = str(tmp_path)
    # A pre-existing file from an earlier run that we did NOT download this
    # time (media_obj is None) should be left alone.
    existing_path = tmp_path / "303_old.png"
    existing_path.write_bytes(b"already-there")

    def fake_crop_localizations_parallel(*args, **kwargs):
        return (1, 0)

    monkeypatch.setattr(
        sync, "crop_localizations_parallel", fake_crop_localizations_parallel
    )

    mid, returned_media_obj, ok, fail = sync._download_and_crop_one_media(
        api=None,
        project_id=1,
        mid=303,
        media_obj=None,
        locs_for_media=[{"elemental_id": "eid-1"}],
        localizations_path="/tmp/does-not-matter.jsonl",
        crops_dir=str(tmp_path / "crops"),
        dl_dir=dl_dir,
        size=224,
    )

    assert (mid, returned_media_obj, ok, fail) == (303, None, 1, 0)
    assert existing_path.exists()
