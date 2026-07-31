#!/usr/bin/env python3
# fiftyone-sync, Apache-2.0 license
# Filename: scripts/export_dataset_csv.py
# Description: Export a FiftyOne dataset to CSV (no images, no embeddings) from a remote MongoDB with no auth.
"""
One-shot export of dataset metadata to CSV.

Connects to MongoDB with no password, loads the dataset, and writes a CSV that
excludes image media and embedding/vector fields.

By default, all samples are exported. Pass --verified-only to exclude samples
that have not been marked `verified=True` (e.g. via scripts/set_verified.py).

Usage:
  python scripts/export_dataset_csv.py
  python scripts/export_dataset_csv.py -o ~/Desktop/isiis.csv
  python scripts/export_dataset_csv.py --verified-only
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# --- connection (no password) ---
MONGO_URI = "mongodb://maximilian.shore.mbari.org:27017"
DATABASE_NAME = "fiftyone"
DATASET_NAME = "902111-ISIIS-Deployments_v67_5151"
DEFAULT_OUT = Path.cwd() / f"{DATASET_NAME}.csv"

# Field names / substrings to drop from the CSV (case-insensitive match on name).
EXCLUDE_NAME_PARTS = (
    "embedding",
    "embeddings",
    "filepath",  # crop image path; drop so export is metadata-only
)


def _configure_fiftyone() -> None:
    os.environ["FIFTYONE_DATABASE_URI"] = MONGO_URI
    os.environ["FIFTYONE_DATABASE_NAME"] = DATABASE_NAME
    import fiftyone as fo

    fo.config.database_uri = MONGO_URI
    fo.config.database_name = DATABASE_NAME


def _is_excluded_field(name: str, field) -> bool:
    lower = name.lower()
    if any(part in lower for part in EXCLUDE_NAME_PARTS):
        return True
    # Drop VectorField / ArrayField-style embedding storage by type name.
    type_name = type(field).__name__.lower()
    if "vector" in type_name:
        return True
    return False


def _csv_fields(dataset) -> list[str]:
    schema = dataset.get_field_schema()
    return [name for name, field in schema.items() if not _is_excluded_field(name, field)]


def _verified_view(sample_collection):
    """Return a view restricted to samples where `verified` is True.

    Samples missing the `verified` field, or with it set to False/None, are
    excluded. Requires the `verified` field to exist on the dataset schema.
    """
    from fiftyone import ViewField as F

    schema = sample_collection.get_field_schema()
    if "verified" not in schema:
        print(
            "WARNING: dataset has no `verified` field; --verified-only has no effect.",
            file=sys.stderr,
        )
        return sample_collection
    return sample_collection.match(F("verified") == True)  # noqa: E712


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export FiftyOne dataset to CSV (no images, no embeddings)."
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output CSV path (default: {DEFAULT_OUT})",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List datasets in the database and exit (sanity check).",
    )
    parser.add_argument(
        "--verified-only",
        action="store_true",
        help=(
            "Exclude samples that are not marked verified=True. "
            "Samples missing the `verified` field are treated as unverified."
        ),
    )
    args = parser.parse_args()

    _configure_fiftyone()
    import fiftyone as fo
    import fiftyone.types as fot

    print(f"Connecting: {MONGO_URI}  db={DATABASE_NAME}")
    try:
        names = fo.list_datasets()
    except Exception as exc:
        print(f"ERROR: cannot reach MongoDB / FiftyOne: {exc}", file=sys.stderr)
        print(
            "Check VPN/network access to maximilian.shore.mbari.org:27017.",
            file=sys.stderr,
        )
        return 1

    if args.list:
        print(f"Datasets ({len(names)}):")
        for n in sorted(names):
            print(f"  {n}")
        return 0

    if DATASET_NAME not in names:
        print(f"ERROR: dataset {DATASET_NAME!r} not found.", file=sys.stderr)
        print("Available:", file=sys.stderr)
        for n in sorted(names):
            print(f"  {n}", file=sys.stderr)
        return 1

    dataset = fo.load_dataset(DATASET_NAME)
    fields = _csv_fields(dataset)
    out = args.output.expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)

    total_samples = len(dataset)
    view = dataset.view()
    if args.verified_only:
        view = _verified_view(view)
        print(
            f"Filtering to verified samples: {len(view)}/{total_samples} "
            f"({total_samples - len(view)} unverified excluded)"
        )

    print(f"Dataset: {DATASET_NAME}  samples={len(view)}")
    print(f"CSV fields ({len(fields)}): {', '.join(fields)}")
    print(f"Writing: {out}")

    view.export(
        dataset_type=fot.CSVDataset,
        labels_path=str(out),
        fields=fields,
        export_media=False,
    )
    print(f"Done: {out} ({out.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
