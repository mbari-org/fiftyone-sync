# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_media_mounts.py
# Description: Tests for resolving image media URLs to locally mounted files (config `mounts`).

from types import SimpleNamespace

from PIL import Image

import src.app.media_mounts as mm
import src.app.sync as sync

HOST = "https://cortex.shore.mbari.org"


def _mounts(root):
    return mm.parse_mounts(
        {"mounts": [{"name": "image", "path": str(root), "host": HOST, "nginx_root": "/DeepSea-AI"}]}
    )


def _media(mid, name, url):
    m = SimpleNamespace(id=mid, name=name)
    m.media_files = SimpleNamespace(image=[SimpleNamespace(path=url)])
    return m


def test_parse_mounts_skips_invalid_entries():
    cfg = {"mounts": [{"name": "x"}, "bad", {"path": "/mnt/a", "host": "h.org", "nginx_root": "a/"}]}
    mounts = mm.parse_mounts(cfg)
    assert mounts == [mm.MediaMount(name="mount2", path="/mnt/a", host="h.org", nginx_root="/a")]
    assert mm.parse_mounts({}) == []
    assert mm.parse_mounts({"mounts": "nope"}) == []


def test_resolve_url_to_local_path(tmp_path):
    f = tmp_path / "dir with space" / "img.png"
    f.parent.mkdir()
    f.write_bytes(b"x")
    mounts = _mounts(tmp_path)

    url = f"{HOST}/DeepSea-AI/dir%20with%20space/img.png?X-Amz-Signature=abc"
    assert mm.resolve_url_to_local_path(url, mounts) == str(f)
    # Scheme and default port do not matter; host case-insensitive.
    assert mm.resolve_url_to_local_path(
        "http://CORTEX.shore.mbari.org:80/DeepSea-AI/dir with space/img.png", mounts
    ) == str(f)


def test_resolve_url_no_match_returns_none(tmp_path):
    (tmp_path / "img.png").write_bytes(b"x")
    mounts = _mounts(tmp_path)
    # Wrong host, wrong root, prefix-only root match, missing file, traversal, relative key.
    for url in (
        "https://other.org/DeepSea-AI/img.png",
        f"{HOST}/Other/img.png",
        f"{HOST}/DeepSea-AIx/img.png",
        f"{HOST}/DeepSea-AI/missing.png",
        f"{HOST}/DeepSea-AI/../etc/passwd",
        "uploads/1/img.png",
    ):
        assert mm.resolve_url_to_local_path(url, mounts) is None, url


def test_resolve_media_local_path_images_only(tmp_path):
    (tmp_path / "a.png").write_bytes(b"x")
    (tmp_path / "a.mp4").write_bytes(b"x")
    mounts = _mounts(tmp_path)
    assert mm.resolve_media_local_path(
        _media(1, "a.png", f"{HOST}/DeepSea-AI/a.png"), mounts
    ) == str(tmp_path / "a.png")
    assert mm.resolve_media_local_path(
        _media(2, "a.mp4", f"{HOST}/DeepSea-AI/a.mp4"), mounts
    ) is None
    # source_url attribute is used when media_files has no match.
    m = SimpleNamespace(id=3, name="a.png", media_files=None,
                        attributes={"source_url": f"{HOST}/DeepSea-AI/a.png"})
    assert mm.resolve_media_local_path(m, mounts) == str(tmp_path / "a.png")


def test_load_mounts_from_config_env(tmp_path, monkeypatch):
    cfg = tmp_path / "config.yml"
    cfg.write_text(f"mounts:\n  - name: image\n    path: /mnt/x\n    host: {HOST}\n    nginx_root: /DeepSea-AI\n")
    monkeypatch.setenv("FIFTYONE_SYNC_CONFIG_PATH", str(cfg))
    assert [m.path for m in mm.load_mounts()] == ["/mnt/x"]
    monkeypatch.delenv("FIFTYONE_SYNC_CONFIG_PATH")
    assert mm.load_mounts() == []


def test_mounted_image_cropped_in_place_without_download(tmp_path, monkeypatch):
    mount = tmp_path / "mount"
    (mount / "sub").mkdir(parents=True)
    src = mount / "sub" / "orig_name.jpg"
    Image.new("RGB", (100, 100), color=(1, 2, 3)).save(src)
    dl_dir = tmp_path / "downloads"
    dl_dir.mkdir()
    crops = tmp_path / "crops"

    monkeypatch.setattr(sync, "resolve_media_local_path",
                        lambda m: mm.resolve_media_local_path(m, _mounts(mount)))

    def no_download(*a, **k):
        raise AssertionError("must not download a mounted image")

    monkeypatch.setattr(sync, "save_media_to_tmp", no_download)

    media = _media(7, "orig_name.jpg", f"{HOST}/DeepSea-AI/sub/orig_name.jpg")
    loc = {"elemental_id": "e1", "media": 7, "x": 0.1, "y": 0.1, "width": 0.5, "height": 0.5}
    mounted: list[int] = []
    mid, _, ok, fail = sync._download_and_crop_one_media(
        None, 1, 7, media, [loc], "", str(crops), str(dl_dir), 32, mounted
    )
    assert (mid, ok, fail) == (7, 1, 0)
    assert mounted == [7]
    # Crop stem follows the {media_id}_{name} convention; nothing copied to downloads.
    assert (crops / "7_orig_name" / "e1.png").exists()
    assert list(dl_dir.iterdir()) == []
    assert src.exists()


def test_unmounted_image_falls_back_to_download(tmp_path, monkeypatch):
    monkeypatch.setattr(sync, "resolve_media_local_path", lambda m: None)
    calls = []
    monkeypatch.setattr(sync, "save_media_to_tmp", lambda *a, **k: calls.append(1))
    monkeypatch.setattr(sync, "crop_localizations_parallel", lambda *a, **k: (1, 0))
    media = _media(8, "x.png", "https://elsewhere.org/x.png")
    sync._download_and_crop_one_media(None, 1, 8, media, [{"elemental_id": "e"}], "", "c", str(tmp_path), 32)
    assert calls == [1]
