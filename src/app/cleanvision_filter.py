# fiftyone-sync, Apache-2.0 license
# Filename: src/app/cleanvision_filter.py
# Description: Remove CleanVision-flagged bad images (near duplicates, dark, low information) from a FiftyOne dataset.
"""
Prune a FiftyOne dataset with `CleanVision <https://github.com/cleanlab/cleanvision>`_.

Datasets built from Tator crops frequently contain near-duplicate crops (successive
video frames, overlapping boxes on the same target) plus crops that are unusable for
annotation (dark, near-empty). Removing them before annotation reduces the
number of samples a human has to review and cuts the memory/GPU pressure of the
downstream embedding + UMAP passes.

Only the **Voxel51 (FiftyOne) samples** are removed -- nothing is deleted in Tator.
The cropped image files of the removed samples are *moved* out of the crops directory
into a sibling quarantine directory (``crops_removed/`` by default), preserving the
``<media_stem>/<elemental_id>.png`` layout, so they can be inspected or restored. No
image file is ever deleted.

Moving rather than deleting keeps the crop cache usable: sync treats a crop present in
the quarantine directory as already cropped, so a later sync does not re-download and
re-crop that media. Running a sync with the option off restores the quarantined crops
first, so the full dataset comes back.

Because nothing is deleted upstream, a subsequent sync re-adds these samples during
reconcile and this filter removes them again (CleanVision hashing/scoring is
deterministic for the same crops), so the pruning is effectively idempotent.

CleanVision is an optional dependency: call :func:`is_cleanvision_available` first and
skip the step when it returns False.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# Default CleanVision issue types and params. Values are merged over by the
# `cleanvision.issue_types` config block, so deployments can tune them without a
# code change.
#
# `near_duplicates` groups images by perceptual-hash collision, so `hash_size` is the
# sensitivity knob: a smaller hash is coarser and therefore more aggressive. It is the side
# of the hash grid, i.e. hash_size**2 bits -- `4` is a 16-bit hash with only 65,536 possible
# values, coarse enough that a large crop set collapses into very few distinct buckets. `8`
# is the conventional 64-bit phash.
#
# Blur detection is deliberately absent. CleanVision's `blurry` check scores global image
# sharpness, which misreads the plankton/ROV crops this pipeline builds: a small, genuinely
# soft-edged organism against a uniform background scores like a blurred photograph, so the
# check culled usable specimens. It is not merely disabled by default here -- it is not part
# of the pipeline at all, and no blur score is computed.
DEFAULT_ISSUE_TYPES: dict[str, dict[str, Any]] = {
    "low_information": {},
    "dark": {},
    "near_duplicates": {"hash_size": 8, "hash_type": "phash"},
}

# Issue types whose flagged images are removed outright.
FLAG_ISSUE_TYPES: tuple[str, ...] = ("low_information", "dark", "light")

# Handled separately: one image per set is kept.
NEAR_DUPLICATES = "near_duplicates"

# Sibling of the crops directory that removed crop images are moved into.
REMOVED_CROPS_DIRNAME = "crops_removed"


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
) -> list[str]:
    """
    For each near-duplicate set, keep one image and return the others.

    The kept image is the lexicographically smallest path in the set. Members of a
    near-duplicate set are by definition near-identical, so which one survives matters far
    less than that the choice is deterministic: the same crops must yield the same kept
    image on every sync, since nothing is removed upstream and a later reconcile re-adds
    and re-prunes these samples.
    """
    removals: list[str] = []
    for dup_set in duplicate_sets or []:
        members = sorted(str(p) for p in (dup_set or []))
        if len(members) <= 1:
            continue
        removals.extend(members[1:])
    return removals


def find_bad_images(
    filepaths: Iterable[str],
    issue_types: dict[str, Any] | None = None,
    n_jobs: int | None = None,
    verbose: bool = False,
) -> list[str]:
    """
    Return the filepaths CleanVision flags as low information or dark, or as the
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
        dup_removals = select_near_duplicate_removals(duplicate_sets)
        logger.info(
            f"CleanVision: removing {len(dup_removals)} images from "
            f"{len(duplicate_sets)} near-duplicate sets, keeping one of each"
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


def removed_crops_dir_for(crops_dir: str) -> str:
    """
    Default quarantine directory for removed crops: a `crops_removed` sibling of `crops_dir`.

    Deliberately *outside* the crops directory -- the dataset build globs the crops tree
    and, in enterprise deployments, the crops tree is synced to S3, so quarantined images
    inside it would be re-added as samples and re-uploaded.
    """
    return os.path.join(
        os.path.dirname(os.path.abspath(crops_dir)), REMOVED_CROPS_DIRNAME
    )


def _relative_crop_path(path: str, crops_dir: str) -> str:
    """`<media_stem>/<elemental_id>.png` for a crop under `crops_dir`, else its basename."""
    try:
        rel = os.path.relpath(os.path.abspath(path), os.path.abspath(crops_dir))
    except ValueError:  # different drives on Windows
        return os.path.basename(path)
    if rel.startswith(os.pardir):
        return os.path.basename(path)
    return rel


def quarantine_crops(paths: Iterable[str], crops_dir: str, removed_dir: str) -> int:
    """
    Move removed crop images from `crops_dir` into `removed_dir`, keeping their
    `<media_stem>/<elemental_id>.png` layout. Returns the number of files moved.

    Files are moved, never deleted, so a removed crop can be reviewed or restored.
    Individual failures are logged and skipped rather than failing the sync.
    """
    moved = 0
    failed = 0
    for path in paths:
        src = str(path)
        if not os.path.isfile(src):
            continue
        dst = os.path.join(removed_dir, _relative_crop_path(src, crops_dir))
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            # move() refuses to overwrite an existing file on some platforms; a crop
            # quarantined by an earlier run is the same image, so replace it.
            if os.path.exists(dst):
                os.remove(dst)
            shutil.move(src, dst)
            moved += 1
        except OSError as e:
            failed += 1
            logger.warning(f"CleanVision: could not move {src} to {dst}: {e}")
    if failed:
        logger.warning(f"CleanVision: {failed} crop image(s) could not be moved")
    logger.info(f"CleanVision: moved {moved} removed crop image(s) to {removed_dir}")
    return moved


def restore_quarantined_crops(crops_dir: str, removed_dir: str | None = None) -> int:
    """
    Move every quarantined crop back into `crops_dir`. Returns the number restored.

    Called when a sync runs with duplicate removal switched off, so the dataset is
    rebuilt from the full set of crops again.
    """
    source = removed_dir or removed_crops_dir_for(crops_dir)
    if not os.path.isdir(source):
        return 0
    restored = 0
    for path in sorted(Path(source).rglob("*")):
        if not path.is_file():
            continue
        dst = os.path.join(crops_dir, os.path.relpath(str(path), source))
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            if os.path.exists(dst):
                os.remove(str(path))  # already back in place
            else:
                shutil.move(str(path), dst)
            restored += 1
        except OSError as e:
            logger.warning(f"CleanVision: could not restore {path} to {dst}: {e}")
    if restored:
        logger.info(
            f"CleanVision: restored {restored} previously removed crop image(s) "
            f"from {source} to {crops_dir}"
        )
    return restored


def remove_bad_images(
    dataset: Any,
    issue_types: dict[str, Any] | None = None,
    n_jobs: int | None = None,
    dry_run: bool = False,
    crops_dir: str | None = None,
    removed_dir: str | None = None,
) -> dict[str, Any]:
    """
    Delete CleanVision-flagged samples from `dataset` (Voxel51 samples only).

    Nothing is deleted in Tator. When `crops_dir` is given, the flagged crop images are
    moved out of it into `removed_dir` (default: the `crops_removed` sibling), so the
    removed images stay on disk for review. Returns a summary dict; on `dry_run` the
    samples are reported but neither deleted nor moved.
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

    num_moved = 0
    resolved_removed_dir = None
    if bad_paths and crops_dir and not dry_run:
        resolved_removed_dir = removed_dir or removed_crops_dir_for(crops_dir)
        num_moved = quarantine_crops(bad_paths, crops_dir, resolved_removed_dir)

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
        "num_crops_moved": num_moved,
        "removed_crops_dir": resolved_removed_dir,
        "issue_types": sorted(normalize_issue_types(issue_types)),
        "dry_run": bool(dry_run),
    }
