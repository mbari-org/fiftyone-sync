# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_embeddings_viz.py
# Description: Tests for embeddings/brain-run coverage helpers and concurrent batch submission.

import asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx

import src.app.embeddings_viz as embeddings_viz


def test_has_brain_run_false_when_key_absent():
    dataset = MagicMock()
    dataset.list_brain_runs.return_value = []

    assert embeddings_viz.has_brain_run(dataset, "vits_umap") is False
    dataset.load_brain_results.assert_not_called()
    dataset.delete_brain_run.assert_not_called()


def test_has_brain_run_true_when_results_load():
    dataset = MagicMock()
    dataset.list_brain_runs.return_value = ["vits_umap"]
    dataset.load_brain_results.return_value = object()

    assert embeddings_viz.has_brain_run(dataset, "vits_umap") is True
    dataset.delete_brain_run.assert_not_called()


def test_has_brain_run_deletes_and_returns_false_when_results_missing():
    """Registered but broken run (e.g. crashed mid-computation) should be treated as absent."""
    dataset = MagicMock()
    dataset.list_brain_runs.return_value = ["vits_umap"]
    dataset.load_brain_results.side_effect = Exception(
        "Results for brain run with key 'vits_umap' are not yet available"
    )

    assert embeddings_viz.has_brain_run(dataset, "vits_umap") is False
    dataset.delete_brain_run.assert_called_once_with("vits_umap")


def test_has_brain_run_survives_delete_failure():
    """If deleting the broken run also fails, still report it as absent rather than raising."""
    dataset = MagicMock()
    dataset.list_brain_runs.return_value = ["vits_umap"]
    dataset.load_brain_results.side_effect = Exception("not available")
    dataset.delete_brain_run.side_effect = Exception("delete failed")

    assert embeddings_viz.has_brain_run(dataset, "vits_umap") is False


def test_has_embeddings_false_without_field():
    dataset = MagicMock()
    dataset.has_field.return_value = False

    assert embeddings_viz.has_embeddings(dataset, "embeddings") is False
    dataset.exists.assert_not_called()


def test_has_embeddings_true_when_any_sample_has_value():
    dataset = MagicMock()
    dataset.has_field.return_value = True
    dataset.exists.return_value.count.return_value = 1

    assert embeddings_viz.has_embeddings(dataset, "embeddings") is True


# ---------------------------------------------------------------------------
# _format_duration
# ---------------------------------------------------------------------------


def test_format_duration_seconds():
    assert embeddings_viz._format_duration(0) == "0s"
    assert embeddings_viz._format_duration(45) == "45s"
    assert embeddings_viz._format_duration(59.4) == "59s"


def test_format_duration_minutes():
    assert embeddings_viz._format_duration(60) == "1m 00s"
    assert embeddings_viz._format_duration(192) == "3m 12s"


def test_format_duration_hours():
    assert embeddings_viz._format_duration(3600) == "1h 00m"
    assert embeddings_viz._format_duration(5400) == "1h 30m"


def test_format_duration_negative_clamped_to_zero():
    assert embeddings_viz._format_duration(-5) == "0s"


# ---------------------------------------------------------------------------
# Concurrent batch submission (_compute_embeddings_via_service_async)
# ---------------------------------------------------------------------------


class _FakeSample:
    def __init__(self, sample_id: str, path: str):
        self.id = sample_id
        self._path = path

    def __contains__(self, key):
        return key == "local_filepath"

    def __getitem__(self, key):
        if key == "local_filepath":
            return self._path
        raise KeyError(key)


class _FakeDataset:
    def __init__(self, samples):
        self._samples = samples

    def __len__(self):
        return len(self._samples)

    def iter_samples(self):
        return iter(self._samples)

    def reload(self):
        pass

    def exists(self, field_name):
        count = len(self._samples)

        class _Existing:
            def count(self):
                return count

        return _Existing()


class _FakeAsyncClient:
    """Stand-in for httpx.AsyncClient; never actually used since submit/poll are patched."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


def _make_fake_dataset(tmp_path, num_samples: int) -> _FakeDataset:
    samples = []
    for i in range(num_samples):
        p = tmp_path / f"sample_{i}.png"
        p.write_bytes(b"fake")
        samples.append(_FakeSample(f"id{i}", str(p)))
    return _FakeDataset(samples)


def test_compute_embeddings_via_service_async_bounds_concurrency(monkeypatch, tmp_path):
    """No more than `concurrency` batches should be mid-flight (submitted but not yet saved) at once."""
    dataset = _make_fake_dataset(tmp_path, num_samples=10)
    state = {"current": 0, "max_seen": 0, "completed": []}

    async def fake_submit(client, url, files, batch_label):
        state["current"] += 1
        state["max_seen"] = max(state["max_seen"], state["current"])
        await asyncio.sleep(0.01)
        return f"job-{batch_label}"

    async def fake_poll(
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label, poll_timeout
    ):
        await asyncio.sleep(0.01)
        state["current"] -= 1
        state["completed"].append(batch_label)
        return len(batch_ids)

    monkeypatch.setattr(embeddings_viz, "_submit_batch_with_retries", fake_submit)
    monkeypatch.setattr(embeddings_viz, "_poll_and_save_batch_with_retries", fake_poll)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _FakeAsyncClient())

    asyncio.run(
        embeddings_viz._compute_embeddings_via_service_async(
            dataset,
            project_name="proj",
            embeddings_field="embeddings",
            service_url="http://embed.example",
            batch_size=2,
            poll_timeout=1.0,
            concurrency=3,
        )
    )

    assert state["max_seen"] <= 3
    assert state["max_seen"] > 1  # sanity check the batches actually overlapped
    assert len(state["completed"]) == 5  # 10 samples / batch_size=2 -> 5 batches, all completed


def test_compute_embeddings_via_service_async_clamps_concurrency_to_num_batches(
    monkeypatch, tmp_path
):
    """Requesting more concurrency than there are batches shouldn't error or over-allocate."""
    dataset = _make_fake_dataset(tmp_path, num_samples=3)
    state = {"current": 0, "max_seen": 0}

    async def fake_submit(client, url, files, batch_label):
        state["current"] += 1
        state["max_seen"] = max(state["max_seen"], state["current"])
        await asyncio.sleep(0.005)
        return f"job-{batch_label}"

    async def fake_poll(
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label, poll_timeout
    ):
        state["current"] -= 1
        return len(batch_ids)

    monkeypatch.setattr(embeddings_viz, "_submit_batch_with_retries", fake_submit)
    monkeypatch.setattr(embeddings_viz, "_poll_and_save_batch_with_retries", fake_poll)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _FakeAsyncClient())

    asyncio.run(
        embeddings_viz._compute_embeddings_via_service_async(
            dataset,
            project_name="proj",
            embeddings_field="embeddings",
            service_url="http://embed.example",
            batch_size=1,
            poll_timeout=1.0,
            concurrency=50,
        )
    )

    assert state["max_seen"] <= 3  # only 3 batches (batch_size=1, 3 samples) ever exist


def test_compute_embeddings_via_service_async_propagates_batch_failure(monkeypatch, tmp_path):
    dataset = _make_fake_dataset(tmp_path, num_samples=4)

    async def fake_submit(client, url, files, batch_label):
        raise RuntimeError(f"boom in {batch_label}")

    monkeypatch.setattr(embeddings_viz, "_submit_batch_with_retries", fake_submit)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _FakeAsyncClient())

    try:
        asyncio.run(
            embeddings_viz._compute_embeddings_via_service_async(
                dataset,
                project_name="proj",
                embeddings_field="embeddings",
                service_url="http://embed.example",
                batch_size=2,
                poll_timeout=1.0,
                concurrency=2,
            )
        )
        raised = False
    except RuntimeError:
        raised = True

    assert raised


def test_compute_embeddings_via_service_uses_default_concurrency(monkeypatch):
    fake_async = AsyncMock()
    monkeypatch.setattr(embeddings_viz, "_compute_embeddings_via_service_async", fake_async)

    embeddings_viz._compute_embeddings_via_service(
        dataset=MagicMock(),
        project_name="proj",
        embeddings_field="embeddings",
        service_url="http://embed.example",
    )

    fake_async.assert_awaited_once()
    _, kwargs = fake_async.call_args
    args = fake_async.call_args.args
    # concurrency is the last positional arg in the current signature
    assert args[-1] == embeddings_viz.DEFAULT_EMBEDDING_CONCURRENCY


def test_compute_embeddings_via_service_forwards_explicit_concurrency(monkeypatch):
    fake_async = AsyncMock()
    monkeypatch.setattr(embeddings_viz, "_compute_embeddings_via_service_async", fake_async)

    embeddings_viz._compute_embeddings_via_service(
        dataset=MagicMock(),
        project_name="proj",
        embeddings_field="embeddings",
        service_url="http://embed.example",
        concurrency=8,
    )

    fake_async.assert_awaited_once()
    args = fake_async.call_args.args
    assert args[-1] == 8


def test_compute_embeddings_and_viz_forwards_concurrency(monkeypatch):
    """compute_embeddings_and_viz should pass concurrency through to the service call."""
    dataset = MagicMock()
    dataset.has_field.return_value = False  # force the embeddings-compute path

    fake_compute = MagicMock()
    monkeypatch.setattr(
        embeddings_viz, "_compute_embeddings_via_service", fake_compute
    )
    # Avoid running the UMAP/similarity path (not relevant to this test).
    dataset.exists.return_value.count.return_value = 0

    embeddings_viz.compute_embeddings_and_viz(
        dataset,
        {"embeddings_field": "embeddings", "brain_key": "umap_viz"},
        project_name="proj",
        concurrency=6,
    )

    fake_compute.assert_called_once()
    assert fake_compute.call_args.kwargs["concurrency"] == 6
