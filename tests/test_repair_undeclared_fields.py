# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_repair_undeclared_fields.py
# Description: Tests for repairing sample fields left after incomplete schema deletes.

from unittest.mock import MagicMock, patch

import pytest

import src.app.sync as sync


def test_parse_undeclared_fields_from_error_embeddings():
    exc = Exception(
        'The fields "{\'embeddings\'}" do not exist on the document '
        '"samples.6a57f8e1c6bfa42bf0e842f4"'
    )
    assert sync._parse_undeclared_fields_from_error(exc) == {"embeddings"}


def test_parse_undeclared_fields_from_error_multiple():
    exc = Exception(
        'The fields "{\'embeddings\', \'umap_viz\'}" do not exist on the document '
        '"samples.abc"'
    )
    assert sync._parse_undeclared_fields_from_error(exc) == {
        "embeddings",
        "umap_viz",
    }


def test_parse_undeclared_fields_from_error_unrelated():
    assert sync._parse_undeclared_fields_from_error(ValueError("boom")) == set()


def test_repair_preferred_field_absent_from_schema_but_on_docs():
    dataset = MagicMock()
    dataset.has_field.return_value = False
    dataset._sample_collection.find_one.return_value = {"_id": "x"}
    dataset._sample_collection_name = "samples.test"
    dataset._sample_doc_cls._delete_fields_simple = MagicMock()

    with patch.object(sync.fo.Sample, "_purge_fields") as purge:
        purged = sync.repair_undeclared_sample_fields(
            dataset, preferred_fields=["embeddings"]
        )

    assert purged == ["embeddings"]
    dataset._sample_doc_cls._delete_fields_simple.assert_called_once_with(
        ["embeddings"]
    )
    purge.assert_called_once_with("samples.test", ["embeddings"])
    dataset.reload.assert_called_once()
    dataset.iter_samples.assert_not_called()


def test_repair_no_op_when_samples_load():
    dataset = MagicMock()
    dataset.has_field.return_value = True
    dataset.iter_samples.return_value = iter([MagicMock()])

    assert sync.repair_undeclared_sample_fields(dataset) == []
    dataset._sample_doc_cls._delete_fields_simple.assert_not_called()


def test_repair_from_iter_samples_field_does_not_exist():
    dataset = MagicMock()
    dataset.has_field.return_value = True
    dataset._sample_collection_name = "samples.test"
    dataset._sample_doc_cls._delete_fields_simple = MagicMock()
    dataset._sample_collection.find.return_value.limit.return_value = [
        {"_id": 1, "filepath": "/a.png", "embeddings": [0.1, 0.2]},
    ]
    dataset.get_field_schema.return_value = {
        "filepath": MagicMock(db_field="filepath"),
    }
    dataset.iter_samples.side_effect = Exception(
        'The fields "{\'embeddings\'}" do not exist on the document "samples.abc"'
    )

    with patch.object(sync.fo.Sample, "_purge_fields"):
        purged = sync.repair_undeclared_sample_fields(dataset)

    assert purged == ["embeddings"]
    dataset._sample_doc_cls._delete_fields_simple.assert_called_once_with(
        ["embeddings"]
    )


def test_repair_reraises_unrelated_iter_error():
    dataset = MagicMock()
    dataset.has_field.return_value = True
    dataset.iter_samples.side_effect = RuntimeError("mongo down")

    with pytest.raises(RuntimeError, match="mongo down"):
        sync.repair_undeclared_sample_fields(dataset)
