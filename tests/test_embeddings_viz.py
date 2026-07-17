# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_embeddings_viz.py
# Description: Tests for embeddings/brain-run coverage helpers used to decide when to recompute.

from unittest.mock import MagicMock

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
