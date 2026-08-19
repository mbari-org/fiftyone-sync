# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_fastvss_ws_url.py
# Description: Tests for Fast-VSS WebSocket URL helpers (issue #36).

from src.app.embedding_service import fastvss_ws_job_url, fastvss_ws_origin_from_base


def test_fastvss_ws_job_url_encodes_spaces_in_project():
    url = fastvss_ws_job_url(
        "wss://cortex.example.org/vss-uav",
        "abc-123",
        "MBARI UAV Images",
    )
    assert (
        url
        == "wss://cortex.example.org/vss-uav/ws/predict/job/abc-123/MBARI%20UAV%20Images"
    )


def test_fastvss_ws_job_url_leaves_simple_project_unchanged():
    url = fastvss_ws_job_url(
        "wss://cortex.example.org/vss-uav",
        "job-1",
        "high-mag",
    )
    assert url == "wss://cortex.example.org/vss-uav/ws/predict/job/job-1/high-mag"


def test_fastvss_ws_origin_from_base_https():
    assert (
        fastvss_ws_origin_from_base("wss://cortex.example.org/vss-uav")
        == "https://cortex.example.org"
    )


def test_fastvss_ws_origin_from_base_http():
    assert (
        fastvss_ws_origin_from_base("ws://localhost:8000/vss")
        == "http://localhost:8000"
    )
