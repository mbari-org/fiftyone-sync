# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_media_presign.py
# Description: Tests that media fetched for download asks Tator for presigned URLs
# and falls back to unsigned object keys when presigning is unsupported.

import src.app.sync as sync


class _FakeMedia(sync.tator.models.Media):
    def __init__(self, mid):
        self.id = mid


class _FakeApi:
    """Records get_media_list_by_id kwargs; optionally rejects presigned requests."""

    def __init__(self, media_ids, presign_supported=True):
        self.media_ids = media_ids
        self.presign_supported = presign_supported
        self.calls: list[dict] = []

    def get_media_list_by_id(self, project_id, media_id_query, **kwargs):
        self.calls.append(kwargs)
        if kwargs.get("presigned") is not None and not self.presign_supported:
            raise RuntimeError("presigned unsupported")
        return [_FakeMedia(mid) for mid in media_id_query["ids"]]


def test_get_media_chunked_presigned_requests_signed_urls():
    api = _FakeApi(media_ids=[1, 2])

    media = sync.get_media_chunked(api, 7, [1, 2], presigned=True)

    assert [m.id for m in media] == [1, 2]
    assert api.calls == [
        {"presigned": sync.PRESIGN_EXPIRATION, "no_cache": True}
    ]


def test_get_media_chunked_falls_back_to_unsigned_urls():
    api = _FakeApi(media_ids=[1], presign_supported=False)

    media = sync.get_media_chunked(api, 7, [1], presigned=True)

    assert [m.id for m in media] == [1]
    assert api.calls == [{"presigned": sync.PRESIGN_EXPIRATION, "no_cache": True}, {}]


def test_get_media_chunked_default_does_not_presign():
    api = _FakeApi(media_ids=[1])

    sync.get_media_chunked(api, 7, [1])

    assert api.calls == [{}]
