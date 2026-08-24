# fiftyone-sync, Apache-2.0 license
# Filename: src/app/cleanvision_filter.py
# Description: Remove CleanVision-flagged bad images (near duplicates, blurry, dark, low information) from a FiftyOne dataset.
"""
Prune a FiftyOne dataset with `CleanVision <https://github.com/cleanlab/cleanvision>`_.

Datasets built from Tator crops frequently contain near-duplicate crops (successive
video frames, overlapping boxes on the same target) plus crops that are unusable for
annotation (blurry, dark, near-empty). Removing them before annotation reduces the
number of samples a human has to review and cuts the memory/GPU pressure of the
downstream embedding + UMAP passes.

Only the **Voxel51 (FiftyOne) samples** are removed. Nothing is deleted in Tator, and
the cropped image files on disk / in S3 are left in place, so the crop cache stays
valid and a later sync does not have to re-crop.

Because nothing is deleted upstream, a subsequent sync re-adds these samples during
reconcile and this filter removes them again (CleanVision hashing/scoring is
deterministic for the same crops), so the pruning is effectively idempotent.

CleanVision is an optional dependency: call :func:`is_cleanvision_available` first and
skip the step when it returns False.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# Default CleanVision issue types and params. Values are merged over by the
# `cleanvision.issue_types` config block, so deployments can tune them without a
# code change.
#
# `near_duplicates` groups images by perceptual-hash collision, so `hash_size` is the
# sensitivity knob: a smaller hash is coarser and therefore more aggressive.
DEFAULT_ISSUE_TYPES: dict[str, dict[str, Any]] = {
    "low_information": {},
    "dark": {},
    "blurry": {"threshold": 0.52},
    "near_duplicates": {"hash_size": 4, "hash_type": "phash"},
}

# Issue types whose flagged images are removed outright.
FLAG_ISSUE_TYPES: tuple[str, ...] = ("low_information", "dark", "blurry", "light")

# Handled separately: one image per set is kept.
NEAR_DUPLICATES = "near_duplicates"

# Used to pick which image of a near-duplicate set to keep (highest = least blurry).
_BLURRY_SCORE_COL = "blurry_score"


def is_cleanvision_available() -> bool:
    """True when the optional `cleanvision` dependency can be imported."""
    try:
        import cleanvision  # noqa: F401
    except Exception as e:  # pragma: no cover - depends on the environment
        logger.info(f"cleanvision not available: {e}")
        return False
    return True


def normalize_issue_types(
    issue_types: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """
    Merge a config `issue_types` block over :data:`DEFAULT_ISSUE_TYPES`.

    An issue type mapped to None/False is dropped (a way to switch one off in config).
    `hash_types: [...]` (plural, accepted by some CleanVision versions) is also mapped
    onto the singular `hash_type` that CleanVision >= 0.3 reads, so either spelling works.
    """
    resolved: dict[str, dict[str, Any]] = {
        name: dict(params) for name, params in DEFAULT_ISSUE_TYPES.items()
    }
    user_dup = (issue_types or {}).get(NEAR_DUPLICATES) if isinstance(issue_types, dict) else None
    user_hash_type = user_dup.get("hash_type") if isinstance(user_dup, dict) else None
    if isinstance(issue_types, dict):
        for name, params in issue_types.items():
            key = str(name)
            if params is None or params is False:
                resolved.pop(key, None)
                continue
            merged = dict(resolved.get(key) or {})
            if isinstance(params, dict):
                merged.update(
                    {k: v for k, v in params.items() if v is not None}
                )
            resolved[key] = merged

    dup_params = resolved.get(NEAR_DUPLICATES)
    if isinstance(dup_params, dict):
        # Only the caller's explicit `hash_type` outranks a `hash_types` list; the
        # module default must not silently win over a configured list.
        hash_types = dup_params.get("hash_types")
        if hash_types and not user_hash_type:
            first = hash_types[0] if isinstance(hash_types, (list, tuple)) else hash_types
            if first:
                dup_params["hash_type"] = str(first)
    return resolved


def select_near_duplicate_removals(
    duplicate_sets: Iterable[Iterable[str]],
    blurry_scores: dict[str, float] | None = None,
) -> list[str]:
    """
    For each near-duplicate set, keep one image and return the others.

    The kept image is the one with the highest `blurry_score` (least blurry). Ties, and
    the case where no blurry scores were computed, fall back to the lexicographically
    smallest path so the choice is deterministic across runs.
    """
    scores = blurry_scores or {}
    removals: list[str] = []
    for dup_set in duplicate_sets or []:
        members = sorted(str(p) for p in (dup_set or []))
        if len(members) <= 1:
            continue
        if scores:
            # Highest blurry_score wins; ties fall back to the smallest path.
            keep = min(members, key=lambda p: (-scores.get(p, 0.0), p))
        else:
            keep = members[0]
        removals.extend(p for p in members if p != keep)
    return removals


def find_bad_images(
    filepaths: Iterable[str],
    issue_types: dict[str, Any] | None = None,
    n_jobs: int | None = None,
    verbose: bool = False,
) -> list[str]:
    """
    Return the filepaths CleanVision flags as low information, dark, blurry, or as the
    redundant members of a near-duplicate set (one image per set is always kept).

    `imagelab.report()` is deliberately never called: it segfaults on large datasets.
    """
    from cleanvision import Imagelab

    paths = [str(p) for p in filepaths]
    if not paths:
        return []

    resolved = normalize_issue_types(issue_types)
    logger.info(
        f"CleanVision: scanning {len(paths)} images for issue types "
        f"{sorted(resolved)} (n_jobs={n_jobs if n_jobs is not None else 'auto'})"
    )
    imagelab = Imagelab(filepaths=paths, verbose=verbose)
    imagelab.find_issues(resolved, n_jobs=n_jobs, verbose=verbose)

    issues = imagelab.issues
    bad: list[str] = []
    for issue_type in FLAG_ISSUE_TYPES:
        column = f"is_{issue_type}_issue"
        if column not in issues.columns:
            continue
        flagged = [str(p) for p in issues[issues[column]].index]
        if flagged:
            logger.info(f"CleanVision: removing {len(flagged)} {issue_type} images")
            bad.extend(flagged)

    duplicate_sets = (imagelab.info.get(NEAR_DUPLICATES) or {}).get("sets") or []
    if duplicate_sets:
        blurry_scores: dict[str, float] = {}
        if _BLURRY_SCORE_COL in issues.columns:
            blurry_scores = {
                str(path): float(score)
                for path, score in issues[_BLURRY_SCORE_COL].items()
            }
        dup_removals = select_near_duplicate_removals(duplicate_sets, blurry_scores)
        logger.info(
            f"CleanVision: removing {len(dup_removals)} images from "
            f"{len(duplicate_sets)} near-duplicate sets, keeping the least blurry of each"
        )
        bad.extend(dup_removals)

    # De-duplicate while preserving order (an image can trip several issue types).
    seen: set[str] = set()
    unique: list[str] = []
    for path in bad:
        if path not in seen:
            seen.add(path)
            unique.append(path)
    return unique


def _local_readable_path(local_filepath: Any, filepath: Any) -> str | None:
    """
    Return a local, readable path for a sample, or None.

    Samples built for enterprise deployments carry an `s3://` `filepath` plus a
    `local_filepath` pointing at the crop on disk; CleanVision reads the local file.
    """
    for candidate in (local_filepath, filepath):
        if not candidate:
            continue
        path = str(candidate)
        if "://" in path:
            continue
        if os.path.isfile(path):
            return path
    return None


def collect_local_paths(dataset: Any) -> tuple[dict[str, list[str]], int]:
    """
    Map local crop path -> sample ids, plus a count of samples with no readable local file.

    A path can map to several samples (e.g. re-cropped duplicates), so removing a path
    removes every sample that points at it.
    """
    schema = dataset.get_field_schema() or {}
    sample_ids = dataset.values("id", _enforce_natural_order=False)
    filepaths = dataset.values("filepath", _enforce_natural_order=False)
    if "local_filepath" in schema:
        local_filepaths = dataset.values(
            "local_filepath", _enforce_natural_order=False
        )
    else:
        local_filepaths = [None] * len(sample_ids)

    path_to_ids: dict[str, list[str]] = {}
    skipped = 0
    for sample_id, local_fp, fp in zip(sample_ids, local_filepaths, filepaths):
        path = _local_readable_path(local_fp, fp)
        if path is None:
            skipped += 1
            continue
        path_to_ids.setdefault(path, []).append(str(sample_id))
    return path_to_ids, skipped


def remove_bad_images(
    dataset: Any,
    issue_types: dict[str, Any] | None = None,
    n_jobs: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Delete CleanVision-flagged samples from `dataset` (Voxel51 samples only).

    Nothing is deleted in Tator and the crop files themselves are kept. Returns a
    summary dict; on `dry_run` the samples are reported but not deleted.
    """
    num_before = len(dataset)
    path_to_ids, skipped = collect_local_paths(dataset)
    if skipped:
        logger.warning(
            f"CleanVision: {skipped} of {num_before} samples have no readable local "
            "crop file and were not scanned"
        )
    if not path_to_ids:
        logger.warning(
            "CleanVision: no locally readable crop files; skipping duplicate/quality removal"
        )
        return {
            "status": "skipped",
            "reason": "no local crop files",
            "num_samples_before": num_before,
            "num_samples_after": num_before,
            "num_removed": 0,
            "num_skipped_no_local_file": skipped,
        }

    bad_paths = find_bad_images(
        sorted(path_to_ids), issue_types=issue_types, n_jobs=n_jobs
    )
    remove_ids: list[str] = []
    for path in bad_paths:
        remove_ids.extend(path_to_ids.get(path, []))

    if remove_ids and not dry_run:
        dataset.delete_samples(remove_ids)

    num_after = len(dataset)
    logger.info(
        f"CleanVision: removed {len(remove_ids)} of {num_before} samples "
        f"({num_after} remaining){' [dry run]' if dry_run else ''}"
    )
    return {
        "status": "ok",
        "num_samples_before": num_before,
        "num_samples_after": num_after,
        "num_removed": len(remove_ids),
        "num_bad_images": len(bad_paths),
        "num_skipped_no_local_file": skipped,
        "issue_types": sorted(normalize_issue_types(issue_types)),
        "dry_run": bool(dry_run),
    }
