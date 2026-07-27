# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_export_dataset_csv.py
# Description: Unit tests for the verified-only filter in scripts/export_dataset_csv.py.

import importlib.util
import sys
from pathlib import Path

import fiftyone as fo
import pytest

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "export_dataset_csv.py"


def _load_export_module():
    spec = importlib.util.spec_from_file_location("export_dataset_csv", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def export_mod():
    return _load_export_module()


@pytest.fixture
def dataset():
    ds = fo.Dataset()
    yield ds
    ds.delete()


def test_verified_view_keeps_only_verified_true(export_mod, dataset):
    dataset.add_sample_field("verified", fo.BooleanField)
    dataset.add_samples(
        [
            fo.Sample(filepath="/tmp/a.jpg", verified=True),
            fo.Sample(filepath="/tmp/b.jpg", verified=False),
            fo.Sample(filepath="/tmp/c.jpg"),  # verified not set (None)
        ]
    )

    view = export_mod._verified_view(dataset.view())

    assert len(view) == 1
    assert view.first().filepath == "/tmp/a.jpg"


def test_verified_view_noop_when_field_missing(export_mod, dataset, capsys):
    dataset.add_samples([fo.Sample(filepath="/tmp/a.jpg")])

    view = export_mod._verified_view(dataset.view())

    assert len(view) == len(dataset)
    assert "no `verified` field" in capsys.readouterr().err


def test_csv_fields_excludes_filepath_and_embeddings(export_mod, dataset):
    dataset.add_sample_field("embedding", fo.VectorField)
    dataset.add_samples([fo.Sample(filepath="/tmp/a.jpg", embedding=[0.1, 0.2])])

    fields = export_mod._csv_fields(dataset)

    assert "filepath" not in fields
    assert "embedding" not in fields
    assert "id" in fields
