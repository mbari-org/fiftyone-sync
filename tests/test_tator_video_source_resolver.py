from __future__ import annotations

from dataclasses import dataclass

from src.app.sync import _resolve_video_source_url_for_ffmpeg


@dataclass
class _MediaFiles:
    streaming: list | None = None
    download: list | None = None
    archive: list | None = None
    source: str | None = None


@dataclass
class _Media:
    media_files: object | None = None


def test_resolve_prefers_streaming_path() -> None:
    m = _Media(
        media_files=_MediaFiles(
            streaming=[{"path": "https://example.com/stream.m3u8"}],
            download=[{"path": "https://example.com/download.mp4"}],
        )
    )
    assert _resolve_video_source_url_for_ffmpeg(m) == "https://example.com/stream.m3u8"


def test_resolve_falls_back_to_download_path() -> None:
    m = _Media(
        media_files=_MediaFiles(
            streaming=[],
            download=[{"path": "https://example.com/video.mp4"}],
        )
    )
    assert _resolve_video_source_url_for_ffmpeg(m) == "https://example.com/video.mp4"


def test_resolve_falls_back_to_archive_path() -> None:
    m = _Media(
        media_files=_MediaFiles(
            streaming=None,
            download=None,
            archive=[{"url": "https://example.com/archived.mov"}],
        )
    )
    assert _resolve_video_source_url_for_ffmpeg(m) == "https://example.com/archived.mov"


def test_resolve_scans_nested_media_files_for_any_http_url() -> None:
    m = _Media(
        media_files={
            "foo": {"bar": [{"baz": "https://example.com/asset.mkv"}]},
            "streaming": [],
        }
    )
    assert _resolve_video_source_url_for_ffmpeg(m) == "https://example.com/asset.mkv"


def test_resolve_returns_none_when_no_http_url_present() -> None:
    m = _Media(media_files={"streaming": [{"path": "s3://bucket/key.mp4"}]})
    assert _resolve_video_source_url_for_ffmpeg(m) is None

