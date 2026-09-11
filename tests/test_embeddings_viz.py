# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_embeddings_viz.py
# Description: Tests for embeddings/brain-run coverage helpers and concurrent batch submission.

import asyncio
import json
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
    def __init__(self, sample_id: str, path: str, embedding=None):
        self.id = sample_id
        self._path = path
        self.embedding = embedding

    def __contains__(self, key):
        return key == "local_filepath"

    def __getitem__(self, key):
        if key == "local_filepath":
            return self._path
        raise KeyError(key)


class _FakeExistsView:
    """Minimal stand-in for the view returned by dataset.exists(field)."""

    def __init__(self, samples, field_name):
        self._samples = [s for s in samples if getattr(s, "embedding", None) is not None]
        self._field = field_name

    def count(self):
        return len(self._samples)

    def values(self, field_name):
        if field_name == "id":
            return [s.id for s in self._samples]
        return [s.embedding for s in self._samples]

    def exists(self, field_name):
        return self


class _FakeDataset:
    def __init__(self, samples, embeddings_field="embeddings"):
        self._samples = samples
        self._embeddings_field = embeddings_field

    def __len__(self):
        return len(self._samples)

    def iter_samples(self):
        return iter(self._samples)

    def reload(self):
        pass

    def has_field(self, field_name):
        return field_name == self._embeddings_field

    def exists(self, field_name):
        return _FakeExistsView(self._samples, field_name)


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
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label,
        poll_timeout, *, save_lock=None
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
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label,
        poll_timeout, *, save_lock=None
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


def test_compute_embeddings_via_service_async_raises_when_every_batch_fails(monkeypatch, tmp_path):
    """Total failure is still an error -- there is nothing to fall back on."""
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


def test_compute_embeddings_via_service_async_isolates_one_failing_batch(monkeypatch, tmp_path):
    """
    One batch exhausting its retries must not take down the others.

    Regression test for the cascade this used to cause: gather() without return_exceptions
    propagated the first failure, which exited the `async with httpx.AsyncClient(...)` block
    and closed the client while sibling batches were still in flight, so every one of them
    then failed with "Cannot send a request, as the client has been closed".
    """
    dataset = _make_fake_dataset(tmp_path, num_samples=8)  # batch_size=2 -> 4 batches
    submitted = []
    polled = []

    async def fake_submit(client, url, files, batch_label):
        # Fail the second batch only, after a beat so siblings are genuinely mid-flight.
        await asyncio.sleep(0.01)
        if batch_label.startswith("batch 2/"):
            raise RuntimeError(f"boom in {batch_label}")
        submitted.append(batch_label)
        return f"job-{batch_label}"

    async def fake_poll(
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label,
        poll_timeout, *, save_lock=None
    ):
        await asyncio.sleep(0.01)
        polled.append(batch_label)
        return len(batch_ids)

    monkeypatch.setattr(embeddings_viz, "_submit_batch_with_retries", fake_submit)
    monkeypatch.setattr(embeddings_viz, "_poll_and_save_batch_with_retries", fake_poll)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _FakeAsyncClient())

    processed = asyncio.run(
        embeddings_viz._compute_embeddings_via_service_async(
            dataset,
            project_name="proj",
            embeddings_field="embeddings",
            service_url="http://embed.example",
            batch_size=2,
            poll_timeout=1.0,
            concurrency=4,
        )
    )

    # 3 of 4 batches survived; only the deliberately-failed one is missing.
    assert len(polled) == 3
    assert processed == 6
    assert not any(label.startswith("batch 2/") for label in polled)


def test_compute_embeddings_via_service_async_reads_files_inside_the_semaphore(
    monkeypatch, tmp_path
):
    """
    At most `concurrency` batches' image bytes are resident at once.

    Reading the crops before acquiring the semaphore let every batch coroutine load its
    images up front, blocking the event loop with synchronous disk I/O (and holding the whole
    dataset in memory) while the first few batches' WebSockets went unread.
    """
    dataset = _make_fake_dataset(tmp_path, num_samples=12)  # batch_size=2 -> 6 batches
    state = {"live": 0, "max_live": 0}
    real_read = embeddings_viz._read_batch_files

    def counting_read(batch_paths):
        state["live"] += 1
        state["max_live"] = max(state["max_live"], state["live"])
        return real_read(batch_paths)

    async def fake_submit(client, url, files, batch_label):
        await asyncio.sleep(0.01)
        return f"job-{batch_label}"

    async def fake_poll(
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label,
        poll_timeout, *, save_lock=None
    ):
        await asyncio.sleep(0.01)
        state["live"] -= 1
        return len(batch_ids)

    monkeypatch.setattr(embeddings_viz, "_read_batch_files", counting_read)
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
            concurrency=2,
        )
    )

    assert state["max_live"] <= 2


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
    args = fake_async.call_args.args
    # positional order: (..., concurrency, skip_existing)
    assert args[-2] == embeddings_viz.DEFAULT_EMBEDDING_CONCURRENCY
    assert args[-1] is True


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
    assert args[-2] == 8


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
    # Not forced -> only missing embeddings should be filled in.
    assert fake_compute.call_args.kwargs["skip_existing"] is True


# ---------------------------------------------------------------------------
# count_embeddings / load_embeddings_array
# ---------------------------------------------------------------------------


def test_count_embeddings_zero_without_field():
    dataset = MagicMock()
    dataset.has_field.return_value = False
    assert embeddings_viz.count_embeddings(dataset, "embeddings") == 0
    dataset.exists.assert_not_called()


def test_count_embeddings_counts_existing():
    samples = [
        _FakeSample("id0", "/a", embedding=[1.0, 2.0]),
        _FakeSample("id1", "/b", embedding=None),
        _FakeSample("id2", "/c", embedding=[3.0, 4.0]),
    ]
    dataset = _FakeDataset(samples)
    assert embeddings_viz.count_embeddings(dataset, "embeddings") == 2


def test_load_embeddings_array_returns_aligned_ids_and_vectors():
    samples = [
        _FakeSample("id0", "/a", embedding=[1.0, 2.0, 3.0]),
        _FakeSample("id1", "/b", embedding=None),
        _FakeSample("id2", "/c", embedding=[4.0, 5.0, 6.0]),
    ]
    dataset = _FakeDataset(samples)

    ids, arr = embeddings_viz.load_embeddings_array(dataset, "embeddings")

    assert ids == ["id0", "id2"]
    assert arr.shape == (2, 3)
    assert arr.dtype.name == "float32"
    assert arr[0].tolist() == [1.0, 2.0, 3.0]
    assert arr[1].tolist() == [4.0, 5.0, 6.0]


def test_load_embeddings_array_empty_when_none_present():
    samples = [_FakeSample("id0", "/a", embedding=None)]
    dataset = _FakeDataset(samples)

    ids, arr = embeddings_viz.load_embeddings_array(dataset, "embeddings")

    assert ids == []
    assert arr.size == 0


# ---------------------------------------------------------------------------
# Incremental (skip_existing) embedding computation
# ---------------------------------------------------------------------------


def test_compute_embeddings_via_service_async_skips_existing(monkeypatch, tmp_path):
    """Only samples missing embeddings should be submitted; the return value is the new count."""
    samples = []
    for i in range(5):
        p = tmp_path / f"s{i}.png"
        p.write_bytes(b"fake")
        # First 3 already have embeddings; last 2 are missing.
        emb = [float(i)] if i < 3 else None
        samples.append(_FakeSample(f"id{i}", str(p), embedding=emb))
    dataset = _FakeDataset(samples)

    submitted_ids: list[str] = []

    async def fake_submit(client, url, files, batch_label):
        return f"job-{batch_label}"

    async def fake_poll(
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label,
        poll_timeout, *, save_lock=None
    ):
        submitted_ids.extend(batch_ids)
        return len(batch_ids)

    monkeypatch.setattr(embeddings_viz, "_submit_batch_with_retries", fake_submit)
    monkeypatch.setattr(embeddings_viz, "_poll_and_save_batch_with_retries", fake_poll)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _FakeAsyncClient())

    new_count = asyncio.run(
        embeddings_viz._compute_embeddings_via_service_async(
            dataset,
            project_name="proj",
            embeddings_field="embeddings",
            service_url="http://embed.example",
            batch_size=10,
            poll_timeout=1.0,
            concurrency=2,
            skip_existing=True,
        )
    )

    assert new_count == 2
    assert sorted(submitted_ids) == ["id3", "id4"]


def test_compute_embeddings_via_service_async_recomputes_all_when_not_skipping(
    monkeypatch, tmp_path
):
    samples = []
    for i in range(4):
        p = tmp_path / f"s{i}.png"
        p.write_bytes(b"fake")
        samples.append(_FakeSample(f"id{i}", str(p), embedding=[float(i)]))
    dataset = _FakeDataset(samples)

    submitted_ids: list[str] = []

    async def fake_submit(client, url, files, batch_label):
        return f"job-{batch_label}"

    async def fake_poll(
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label,
        poll_timeout, *, save_lock=None
    ):
        submitted_ids.extend(batch_ids)
        return len(batch_ids)

    monkeypatch.setattr(embeddings_viz, "_submit_batch_with_retries", fake_submit)
    monkeypatch.setattr(embeddings_viz, "_poll_and_save_batch_with_retries", fake_poll)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _FakeAsyncClient())

    new_count = asyncio.run(
        embeddings_viz._compute_embeddings_via_service_async(
            dataset,
            project_name="proj",
            embeddings_field="embeddings",
            service_url="http://embed.example",
            batch_size=10,
            poll_timeout=1.0,
            concurrency=2,
            skip_existing=False,
        )
    )

    assert new_count == 4
    assert sorted(submitted_ids) == ["id0", "id1", "id2", "id3"]


def test_compute_embeddings_via_service_async_returns_zero_when_all_present(
    monkeypatch, tmp_path
):
    samples = []
    for i in range(3):
        p = tmp_path / f"s{i}.png"
        p.write_bytes(b"fake")
        samples.append(_FakeSample(f"id{i}", str(p), embedding=[float(i)]))
    dataset = _FakeDataset(samples)

    async def fake_submit(client, url, files, batch_label):  # pragma: no cover - should not run
        raise AssertionError("submit should not be called when nothing is missing")

    monkeypatch.setattr(embeddings_viz, "_submit_batch_with_retries", fake_submit)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: _FakeAsyncClient())

    new_count = asyncio.run(
        embeddings_viz._compute_embeddings_via_service_async(
            dataset,
            project_name="proj",
            embeddings_field="embeddings",
            service_url="http://embed.example",
            batch_size=10,
            poll_timeout=1.0,
            concurrency=2,
            skip_existing=True,
        )
    )

    assert new_count == 0


# ---------------------------------------------------------------------------
# WebSocket timeout semantics (_wait_job_result_ws) and error rendering
# ---------------------------------------------------------------------------


class _ScriptedWS:
    """
    Fake Fast-VSS WebSocket.

    ``script`` is a list of ``(delay, payload)`` pairs consumed in order; once exhausted,
    ``heartbeat`` (if given) is emitted forever, otherwise recv() blocks indefinitely.
    """

    def __init__(self, script, heartbeat=None, heartbeat_delay=0.01):
        self._script = list(script)
        self._heartbeat = heartbeat
        self._heartbeat_delay = heartbeat_delay
        self.frames_sent = 0

    async def recv(self):
        if self._script:
            delay, payload = self._script.pop(0)
        elif self._heartbeat is not None:
            delay, payload = self._heartbeat_delay, self._heartbeat
        else:
            await asyncio.sleep(3600)  # never resolves; caller must time out
            raise AssertionError("unreachable")
        await asyncio.sleep(delay)
        self.frames_sent += 1
        return json.dumps(payload)


class _FakeConnect:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *exc_info):
        return False


def _patch_ws(monkeypatch, ws):
    import websockets

    monkeypatch.setattr(websockets, "connect", lambda url, **kw: _FakeConnect(ws))


def test_wait_job_result_ws_returns_result_on_done(monkeypatch):
    ws = _ScriptedWS(
        [
            (0.01, {"status": "pending"}),
            (0.01, {"status": "pending"}),
            (0.01, {"status": "done", "result": {"embeddings": [[1.0, 2.0]]}}),
        ]
    )
    _patch_ws(monkeypatch, ws)

    result = asyncio.run(
        embeddings_viz._wait_job_result_ws(
            "ws://embed.example/ws/predict/job/j/proj", timeout=30.0, idle_timeout=5.0
        )
    )

    assert result == {"embeddings": [[1.0, 2.0]]}


def test_wait_job_result_ws_enforces_total_job_budget(monkeypatch):
    """
    A heartbeating-but-never-finishing job must hit the *job* deadline.

    Regression test for waiting on ``max(1.0, deadline - now)``: with heartbeats arriving
    every 0.5s the one-second floor was always satisfied, so the total budget was never
    actually enforced and this call looped forever.
    """
    ws = _ScriptedWS([], heartbeat={"status": "pending"}, heartbeat_delay=0.01)
    _patch_ws(monkeypatch, ws)

    async def run():
        # Outer guard: if the budget is not enforced this hangs rather than raising.
        return await asyncio.wait_for(
            embeddings_viz._wait_job_result_ws(
                "ws://embed.example/ws/predict/job/j/proj",
                timeout=0.5,
                idle_timeout=30.0,
            ),
            timeout=10.0,
        )

    try:
        asyncio.run(run())
        raised = None
    except TimeoutError as e:
        raised = e

    assert raised is not None
    assert "did not finish within" in str(raised)
    assert ws.frames_sent > 1  # it really was receiving heartbeats the whole time


def test_wait_job_result_ws_detects_stalled_stream_with_a_real_message(monkeypatch):
    """
    A silent socket trips the idle timeout, and the error must not stringify to "".

    ``str(asyncio.TimeoutError())`` is empty, which is why these failures used to log as
    "WebSocket batch 7/49 attempt 1/3 failed:" with no reason at all.
    """
    ws = _ScriptedWS([])  # never sends anything
    _patch_ws(monkeypatch, ws)

    try:
        asyncio.run(
            embeddings_viz._wait_job_result_ws(
                "ws://embed.example/ws/predict/job/j/proj",
                timeout=30.0,
                idle_timeout=0.3,
            )
        )
        raised = None
    except TimeoutError as e:
        raised = e

    assert raised is not None
    assert str(raised).strip()  # never empty
    assert "no message from embed service" in str(raised)


def test_wait_job_result_ws_tolerates_gaps_below_the_idle_timeout(monkeypatch):
    """A slow-but-alive stream must survive gaps far longer than one second."""
    ws = _ScriptedWS(
        [
            (1.5, {"status": "pending"}),
            (1.5, {"status": "done", "result": {"embeddings": [[3.0]]}}),
        ]
    )
    _patch_ws(monkeypatch, ws)

    result = asyncio.run(
        embeddings_viz._wait_job_result_ws(
            "ws://embed.example/ws/predict/job/j/proj", timeout=60.0, idle_timeout=10.0
        )
    )

    assert result == {"embeddings": [[3.0]]}


def test_wait_job_result_ws_raises_on_service_error_status(monkeypatch):
    ws = _ScriptedWS([(0.01, {"status": "error", "message": "Timed out waiting for job"})])
    _patch_ws(monkeypatch, ws)

    try:
        asyncio.run(
            embeddings_viz._wait_job_result_ws(
                "ws://embed.example/ws/predict/job/j/proj", timeout=30.0, idle_timeout=5.0
            )
        )
        raised = None
    except RuntimeError as e:
        raised = e

    assert raised is not None
    assert "Timed out waiting for job" in str(raised)


def test_poll_timeout_defaults_to_fastvss_ws_max_wait(monkeypatch, tmp_path):
    """
    An unset poll_timeout must resolve from FASTVSS_WS_MAX_WAIT, not a hard-coded 10s.

    compute_embeddings_and_viz never passed poll_timeout, so it fell through to a flat
    10.0-second default and FASTVSS_WS_MAX_WAIT never reached this path at all -- despite
    being documented as the knob controlling it.
    """
    monkeypatch.setenv("FASTVSS_WS_MAX_WAIT", "777")
    dataset = _make_fake_dataset(tmp_path, num_samples=2)
    seen = {}

    async def fake_submit(client, url, files, batch_label):
        return f"job-{batch_label}"

    async def fake_poll(
        ds, ws_base, project_name, job_id, batch_ids, embeddings_field, batch_label,
        poll_timeout, *, save_lock=None
    ):
        seen["poll_timeout"] = poll_timeout
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
            poll_timeout=None,
            concurrency=1,
        )
    )

    assert seen["poll_timeout"] == 777.0


def test_describe_error_never_returns_empty_string():
    from src.app.embedding_service import describe_error

    # The exceptions that made the original log lines end in a bare colon.
    assert describe_error(asyncio.TimeoutError()) == "TimeoutError"
    assert describe_error(RuntimeError("")) == "RuntimeError"
    assert describe_error(RuntimeError("boom")) == "RuntimeError: boom"
