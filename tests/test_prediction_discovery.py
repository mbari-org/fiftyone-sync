# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_prediction_discovery.py
# Description: Tests for dynamic prediction-pair discovery and indexed sample label assignment.

import pytest
import src.app.sync as sync


# ---------------------------------------------------------------------------
# _discover_prediction_pairs
# ---------------------------------------------------------------------------


def test_discover_prediction_pairs_versioned_suffix():
    attrs = {"prediction_v1": "Krill", "score_v1": 0.9}
    pairs = sync._discover_prediction_pairs(attrs)
    assert pairs == [("prediction_v1", "score_v1")]


def test_discover_prediction_pairs_predict_prefix_variant():
    attrs = {"predict_v1": "Salp", "score_v1": 0.7}
    pairs = sync._discover_prediction_pairs(attrs)
    assert pairs == [("predict_v1", "score_v1")]


def test_discover_prediction_pairs_multiple_versions():
    attrs = {
        "prediction_v1": "Krill",
        "score_v1": 0.9,
        "prediction_v2": "Salp",
        "score_v2": 0.8,
    }
    pairs = sync._discover_prediction_pairs(attrs)
    assert ("prediction_v1", "score_v1") in pairs
    assert ("prediction_v2", "score_v2") in pairs


def test_discover_prediction_pairs_missing_companion_score():
    """No score_v1 present → score_attr should be None."""
    attrs = {"prediction_v1": "Krill"}
    pairs = sync._discover_prediction_pairs(attrs)
    assert pairs == [("prediction_v1", None)]


def test_discover_prediction_pairs_excludes_predicted_label():
    """'predicted_label' is handled by the top1_prediction path; must not appear here."""
    attrs = {"predicted_label": "Krill", "score": 0.9}
    pairs = sync._discover_prediction_pairs(attrs)
    assert all(k != "predicted_label" for k, _ in pairs)


def test_discover_prediction_pairs_no_predict_attrs():
    attrs = {"Label": "Krill", "score": 0.9, "depth": 3.0}
    assert sync._discover_prediction_pairs(attrs) == []


def test_discover_prediction_pairs_case_insensitive():
    """Uppercase PREDICT should also be discovered."""
    attrs = {"PREDICTION_v1": "Krill", "score_v1": 0.5}
    pairs = sync._discover_prediction_pairs(attrs)
    assert len(pairs) == 1
    assert pairs[0][0] == "PREDICTION_v1"
    assert pairs[0][1] == "score_v1"


# ---------------------------------------------------------------------------
# _apply_loc_to_sample – dynamic prediction field integration
# ---------------------------------------------------------------------------


class _MockSample(dict):
    """Minimal fo.Sample stub: stores field assignments in a plain dict."""

    def has_field(self, name):
        return name in self

    def __setitem__(self, key, value):
        super().__setitem__(key, value)

    def __getitem__(self, key):
        return super().__getitem__(key)


def _make_loc(attrs):
    return {
        "elemental_id": "eid-1",
        "media": 1,
        "attributes": attrs,
        "modified_datetime": None,
        "created_datetime": None,
    }


def test_apply_loc_sets_versioned_prediction_field(monkeypatch):
    """prediction_v1 + score_v1 attrs produce a 'prediction_v1' Classification on the sample."""
    import fiftyone as fo

    attrs = {
        "Label": "Krill",
        "prediction_v1": "Krill_v1",
        "score_v1": 0.85,
    }
    sample = fo.Sample.__new__(fo.Sample)
    sample._doc = {}

    captured = {}

    def _fake_set(key, value):
        captured[key] = value

    monkeypatch.setattr(fo.Sample, "__setitem__", lambda self, k, v: captured.__setitem__(k, v))
    monkeypatch.setattr(fo.Sample, "__getitem__", lambda self, k: captured[k])
    monkeypatch.setattr(fo.Sample, "has_field", lambda self, k: k in captured)

    sync._apply_loc_to_sample(sample, _make_loc(attrs))

    assert "prediction_v1" in captured
    cls = captured["prediction_v1"]
    assert isinstance(cls, fo.Classification)
    assert cls.label == "Krill_v1"
    assert abs(cls.confidence - 0.85) < 1e-9


def test_apply_loc_sets_multiple_versioned_prediction_fields(monkeypatch):
    """Both prediction_v1 and prediction_v2 are applied when present."""
    import fiftyone as fo

    attrs = {
        "Label": "Krill",
        "prediction_v1": "A",
        "score_v1": 0.9,
        "prediction_v2": "B",
        "score_v2": 0.7,
    }
    captured = {}
    monkeypatch.setattr(fo.Sample, "__setitem__", lambda self, k, v: captured.__setitem__(k, v))
    monkeypatch.setattr(fo.Sample, "__getitem__", lambda self, k: captured[k])
    monkeypatch.setattr(fo.Sample, "has_field", lambda self, k: k in captured)

    sample = fo.Sample.__new__(fo.Sample)
    sample._doc = {}
    sync._apply_loc_to_sample(sample, _make_loc(attrs))

    assert captured["prediction_v1"].label == "A"
    assert captured["prediction_v2"].label == "B"
    assert abs(captured["prediction_v1"].confidence - 0.9) < 1e-9
    assert abs(captured["prediction_v2"].confidence - 0.7) < 1e-9


def test_apply_loc_dynamic_prediction_no_score(monkeypatch):
    """Dynamic prediction without a companion score sets label only (no confidence)."""
    import fiftyone as fo

    attrs = {"Label": "Krill", "prediction_v1": "Krill_v1"}
    captured = {}
    monkeypatch.setattr(fo.Sample, "__setitem__", lambda self, k, v: captured.__setitem__(k, v))
    monkeypatch.setattr(fo.Sample, "__getitem__", lambda self, k: captured[k])
    monkeypatch.setattr(fo.Sample, "has_field", lambda self, k: k in captured)

    sample = fo.Sample.__new__(fo.Sample)
    sample._doc = {}
    sync._apply_loc_to_sample(sample, _make_loc(attrs))

    assert "prediction_v1" in captured
    cls = captured["prediction_v1"]
    assert cls.label == "Krill_v1"
    assert cls.confidence is None
