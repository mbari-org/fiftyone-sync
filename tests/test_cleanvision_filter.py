# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_cleanvision_filter.py
# Description: Tests for CleanVision near-duplicate / low-quality sample removal (remove_near_duplicates option).

import sys
import types

import pytest

import src.app.cleanvision_filter as cvf


# ---------------------------------------------------------------- issue types


def test_normalize_issue_types_defaults():
    resolved = cvf.normalize_issue_types(None)
    assert set(resolved) == {"low_information", "dark", "near_duplicates"}
    assert resolved["near_duplicates"]["hash_size"] == 4
    assert resolved["near_duplicates"]["hash_type"] == "phash"


def test_normalize_issue_types_merges_over_defaults():
    resolved = cvf.normalize_issue_types({"near_duplicates": {"hash_size": 8}})
    assert resolved["near_duplicates"] == {"hash_size": 8, "hash_type": "phash"}
    # untouched defaults survive
    assert resolved["dark"] == {}
    assert resolved["low_information"] == {}


def test_normalize_issue_types_none_disables_issue_type():
    resolved = cvf.normalize_issue_types({"dark": None, "low_information": False})
    assert "dark" not in resolved
    assert "low_information" not in resolved
    assert "near_duplicates" in resolved


def test_normalize_issue_types_maps_plural_hash_types():
    resolved = cvf.normalize_issue_types(
        {"near_duplicates": {"hash_types": ["whash", "phash"], "hash_size": 4}}
    )
    # CleanVision >= 0.3 reads the singular key
    assert resolved["near_duplicates"]["hash_type"] == "whash"
    assert resolved["near_duplicates"]["hash_types"] == ["whash", "phash"]


def test_normalize_issue_types_explicit_hash_type_wins():
    resolved = cvf.normalize_issue_types(
        {"near_duplicates": {"hash_types": ["whash"], "hash_type": "phash"}}
    )
    assert resolved["near_duplicates"]["hash_type"] == "phash"


def test_normalize_issue_types_does_not_mutate_defaults():
    cvf.normalize_issue_types({"near_duplicates": {"hash_size": 16}})
    assert cvf.DEFAULT_ISSUE_TYPES["near_duplicates"]["hash_size"] == 4


def test_blur_detection_is_absent_from_the_pipeline():
    """
    Blur is removed outright, not merely defaulted off.

    CleanVision's `blurry` check scores global sharpness, which reads a small soft-edged
    organism against a uniform background as a blurred photograph and culled usable crops.
    """
    assert "blurry" not in cvf.DEFAULT_ISSUE_TYPES
    assert "blurry" not in cvf.FLAG_ISSUE_TYPES


def test_blurry_can_still_be_requested_explicitly_via_config():
    """normalize_issue_types stays a pass-through: config can add issue types back."""
    resolved = cvf.normalize_issue_types({"blurry": {"threshold": 0.9}})
    assert resolved["blurry"] == {"threshold": 0.9}
    # ...but it is still never a removal criterion.
    assert "blurry" not in cvf.FLAG_ISSUE_TYPES


# --------------------------------------------------- near-duplicate selection


def test_select_near_duplicate_removals_keeps_smallest_path():
    sets = [["/c/b.png", "/c/a.png", "/c/d.png"]]
    assert cvf.select_near_duplicate_removals(sets) == ["/c/b.png", "/c/d.png"]


def test_select_near_duplicate_removals_is_deterministic_regardless_of_input_order():
    """
    Nothing is removed upstream, so a later sync re-adds and re-prunes these samples.
    The same crops must therefore always yield the same survivor.
    """
    a = cvf.select_near_duplicate_removals([["/c/b.png", "/c/a.png", "/c/c.png"]])
    b = cvf.select_near_duplicate_removals([["/c/c.png", "/c/b.png", "/c/a.png"]])
    assert a == b == ["/c/b.png", "/c/c.png"]


def test_select_near_duplicate_removals_ignores_singleton_and_empty_sets():
    assert cvf.select_near_duplicate_removals([["/c/a.png"], [], None]) == []
    assert cvf.select_near_duplicate_removals(None) == []


def test_select_near_duplicate_removals_keeps_one_per_set():
    sets = [["/c/a.png", "/c/b.png"], ["/c/x.png", "/c/y.png", "/c/z.png"]]
    removals = cvf.select_near_duplicate_removals(sets)
    assert len(removals) == 3  # 2 sets, one survivor each


# ------------------------------------------------------------- path selection


def test_local_readable_path_prefers_local_filepath(tmp_path):
    local = tmp_path / "crop.png"
    local.write_bytes(b"x")
    assert cvf._local_readable_path(str(local), "s3://bucket/crop.png") == str(local)


def test_local_readable_path_skips_remote_uris(tmp_path):
    assert cvf._local_readable_path(None, "s3://bucket/crop.png") is None


def test_local_readable_path_falls_back_to_filepath(tmp_path):
    local = tmp_path / "crop.png"
    local.write_bytes(b"x")
    assert cvf._local_readable_path(None, str(local)) == str(local)


def test_local_readable_path_none_when_missing_on_disk(tmp_path):
    assert cvf._local_readable_path(str(tmp_path / "gone.png"), None) is None


# ------------------------------------------------------------- fake dataset


class _FakeDataset:
    """Minimal stand-in for fo.Dataset covering what remove_bad_images uses."""

    def __init__(self, rows, schema_has_local=True):
        # rows: list of (sample_id, local_filepath, filepath)
        self.rows = list(rows)
        self.schema_has_local = schema_has_local
        self.deleted = []

    def __len__(self):
        return len(self.rows)

    def get_field_schema(self):
        return {"filepath": object, **({"local_filepath": object} if self.schema_has_local else {})}

    def values(self, field, _enforce_natural_order=True):
        idx = {"id": 0, "local_filepath": 1, "filepath": 2}[field]
        return [row[idx] for row in self.rows]

    def delete_samples(self, ids):
        self.deleted.extend(ids)
        keep = set(ids)
        self.rows = [row for row in self.rows if row[0] not in keep]


def _dataset_with_crops(tmp_path, n=3):
    rows = []
    for i in range(n):
        p = tmp_path / f"crop{i}.png"
        p.write_bytes(b"x")
        rows.append((f"id{i}", str(p), f"s3://bucket/crop{i}.png"))
    return _FakeDataset(rows), [row[1] for row in rows]


def test_collect_local_paths_maps_paths_to_sample_ids(tmp_path):
    dataset, paths = _dataset_with_crops(tmp_path, 2)
    path_to_ids, skipped = cvf.collect_local_paths(dataset)
    assert skipped == 0
    assert path_to_ids == {paths[0]: ["id0"], paths[1]: ["id1"]}


def test_collect_local_paths_counts_unreadable_samples(tmp_path):
    dataset, paths = _dataset_with_crops(tmp_path, 1)
    dataset.rows.append(("id-remote", None, "s3://bucket/only-remote.png"))
    path_to_ids, skipped = cvf.collect_local_paths(dataset)
    assert skipped == 1
    assert list(path_to_ids) == [paths[0]]


def test_collect_local_paths_without_local_filepath_field(tmp_path):
    p = tmp_path / "crop.png"
    p.write_bytes(b"x")
    dataset = _FakeDataset([("id0", None, str(p))], schema_has_local=False)
    path_to_ids, skipped = cvf.collect_local_paths(dataset)
    assert path_to_ids == {str(p): ["id0"]}
    assert skipped == 0


def test_remove_bad_images_deletes_flagged_samples(monkeypatch, tmp_path):
    dataset, paths = _dataset_with_crops(tmp_path, 3)
    monkeypatch.setattr(cvf, "find_bad_images", lambda p, **kw: [paths[0], paths[2]])

    result = cvf.remove_bad_images(dataset)

    assert result["status"] == "ok"
    assert result["num_samples_before"] == 3
    assert result["num_removed"] == 2
    assert result["num_samples_after"] == 1
    assert sorted(dataset.deleted) == ["id0", "id2"]


def test_remove_bad_images_dry_run_deletes_nothing(monkeypatch, tmp_path):
    dataset, paths = _dataset_with_crops(tmp_path, 2)
    monkeypatch.setattr(cvf, "find_bad_images", lambda p, **kw: [paths[0]])

    result = cvf.remove_bad_images(dataset, dry_run=True)

    assert result["dry_run"] is True
    assert result["num_removed"] == 1
    assert result["num_samples_after"] == 2
    assert dataset.deleted == []


def test_remove_bad_images_removes_every_sample_sharing_a_path(monkeypatch, tmp_path):
    p = tmp_path / "crop.png"
    p.write_bytes(b"x")
    dataset = _FakeDataset([("id0", str(p), "s3://b/c.png"), ("id1", str(p), "s3://b/c.png")])
    monkeypatch.setattr(cvf, "find_bad_images", lambda paths, **kw: [str(p)])

    result = cvf.remove_bad_images(dataset)

    assert result["num_removed"] == 2
    assert sorted(dataset.deleted) == ["id0", "id1"]


def test_remove_bad_images_skips_when_no_local_files():
    dataset = _FakeDataset([("id0", None, "s3://bucket/crop.png")])
    result = cvf.remove_bad_images(dataset)
    assert result["status"] == "skipped"
    assert result["num_removed"] == 0
    assert dataset.deleted == []


# --------------------------------------------------------- find_bad_images


class _FakeMask:
    """Stand-in for the boolean mask in `issues[issues[col]]`."""

    def __init__(self, column):
        self.column = column


class _FakeIssues:
    """Stands in for the pandas DataFrame returned by imagelab.issues."""

    def __init__(self, flags):
        # flags: {"is_dark_issue": [paths...]}
        self._flags = flags

    @property
    def columns(self):
        return list(self._flags)

    def __getitem__(self, key):
        if isinstance(key, _FakeMask):
            return types.SimpleNamespace(index=self._flags.get(key.column, []))
        return _FakeMask(key)


class _FakeImagelab:
    last_kwargs = None

    def __init__(self, filepaths=None, verbose=True):
        self.filepaths = filepaths
        self.issues = _FakeIssues(
            {
                "is_low_information_issue": ["/c/empty.png"],
                "is_dark_issue": ["/c/dark.png"],
            }
        )
        self.info = {"near_duplicates": {"sets": [["/c/a.png", "/c/b.png"]]}}

    def find_issues(self, issue_types=None, n_jobs=None, verbose=True):
        _FakeImagelab.last_kwargs = {
            "issue_types": issue_types,
            "n_jobs": n_jobs,
            "verbose": verbose,
        }


@pytest.fixture
def fake_cleanvision(monkeypatch):
    module = types.ModuleType("cleanvision")
    module.Imagelab = _FakeImagelab
    monkeypatch.setitem(sys.modules, "cleanvision", module)
    return module


def test_find_bad_images_collects_flags_and_duplicates(fake_cleanvision):
    bad = cvf.find_bad_images(["/c/a.png", "/c/b.png", "/c/empty.png", "/c/dark.png"])
    # low_information + dark flagged outright; one of the duplicate pair is dropped
    assert set(bad) == {"/c/empty.png", "/c/dark.png", "/c/b.png"}
    # the smallest path of the duplicate set survives
    assert "/c/a.png" not in bad


def test_find_bad_images_never_removes_on_blur(fake_cleanvision, monkeypatch):
    """A `blurry` column in CleanVision's output must not cull anything."""

    class _WithBlur(_FakeImagelab):
        def __init__(self, filepaths=None, verbose=True):
            super().__init__(filepaths=filepaths, verbose=verbose)
            self.issues = _FakeIssues({"is_blurry_issue": ["/c/soft.png"]})
            self.info = {}

    monkeypatch.setattr(fake_cleanvision, "Imagelab", _WithBlur)
    assert cvf.find_bad_images(["/c/soft.png"]) == []


def test_find_bad_images_deduplicates_paths(fake_cleanvision, monkeypatch):
    class _Overlap(_FakeImagelab):
        def __init__(self, filepaths=None, verbose=True):
            super().__init__(filepaths=filepaths, verbose=verbose)
            self.issues = _FakeIssues(
                {
                    "is_low_information_issue": ["/c/a.png"],
                    "is_dark_issue": ["/c/a.png"],
                }
            )

    monkeypatch.setattr(fake_cleanvision, "Imagelab", _Overlap)
    bad = cvf.find_bad_images(["/c/a.png", "/c/b.png"])
    assert bad.count("/c/a.png") == 1


def test_find_bad_images_passes_resolved_issue_types(fake_cleanvision):
    cvf.find_bad_images(["/c/a.png"], issue_types={"near_duplicates": {"hash_size": 8}}, n_jobs=2)
    kwargs = _FakeImagelab.last_kwargs
    assert kwargs["n_jobs"] == 2
    assert kwargs["issue_types"]["near_duplicates"]["hash_size"] == 8
    assert "blurry" not in kwargs["issue_types"]


def test_find_bad_images_empty_input_short_circuits():
    # no cleanvision import needed for an empty list
    assert cvf.find_bad_images([]) == []


# ------------------------------------------- sync wiring (_remove_near_duplicate_samples)


def test_remove_near_duplicate_samples_disabled_returns_none():
    import src.app.sync as sync

    assert sync._remove_near_duplicate_samples(object(), {}, enabled=False) is None


def test_remove_near_duplicate_samples_runs_when_enabled_in_config(monkeypatch):
    import src.app.sync as sync

    monkeypatch.setattr(cvf, "is_cleanvision_available", lambda: True)
    monkeypatch.setattr(
        cvf, "remove_bad_images", lambda ds, **kw: {"status": "ok", "kwargs": kw}
    )

    config = {"cleanvision": {"enabled": True, "n_jobs": 3, "issue_types": {"dark": None}}}
    result = sync._remove_near_duplicate_samples(object(), config, enabled=False)

    assert result["status"] == "ok"
    assert result["kwargs"]["n_jobs"] == 3
    assert result["kwargs"]["issue_types"] == {"dark": None}
    assert result["kwargs"]["dry_run"] is False


def test_remove_near_duplicate_samples_skips_when_cleanvision_missing(monkeypatch):
    import src.app.sync as sync

    monkeypatch.setattr(cvf, "is_cleanvision_available", lambda: False)
    result = sync._remove_near_duplicate_samples(object(), {}, enabled=True)
    assert result == {"status": "skipped", "reason": "cleanvision not installed"}


def test_remove_near_duplicate_samples_swallows_errors(monkeypatch):
    import src.app.sync as sync

    monkeypatch.setattr(cvf, "is_cleanvision_available", lambda: True)

    def _boom(dataset, **kwargs):
        raise RuntimeError("hash failure")

    monkeypatch.setattr(cvf, "remove_bad_images", _boom)
    result = sync._remove_near_duplicate_samples(object(), {}, enabled=True)
    assert result["status"] == "error"
    assert "hash failure" in result["message"]


def test_cleanvision_config_ignores_non_dict():
    import src.app.sync as sync

    assert sync._cleanvision_config({"cleanvision": "yes"}) == {}
    assert sync._cleanvision_config({}) == {}
