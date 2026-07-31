#!/usr/bin/env python3
# fiftyone-sync, Apache-2.0 license
# Filename: scripts/set_verified.py
# Description: Add a boolean `verified`=True to every sample in specific FiftyOne Enterprise datasets. Requires an API key in the FIFTYONE_API_KEY environment variable.
# required for syncing with fiftyone-sync service
"""
Set a `verified` boolean to True on every sample in the target datasets.

Connects to the FiftyOne Enterprise (professional) deployment at
https://mbari.fiftyone.ai. Requires an API key in the FIFTYONE_API_KEY
environment variable.
"""


import fiftyone as fo

DATASETS = [
    "ptvr_hm_verified_dataset",
    "902004-Planktivore_v78_5151_pd",
    "902004-Planktivore_v43_5151",
    "902004-Planktivore_v66_5151",
    "902004-Planktivore_v21_5151",
    "902004-Planktivore_v43_5151",
]


def main() -> None:
    for name in DATASETS:
        dataset = fo.load_dataset(name)
        if "verified" not in dataset.get_field_schema():
            dataset.add_sample_field("verified", fo.BooleanField)
        dataset.set_values("verified", [True] * len(dataset))
        dataset.save()
        print(f"{name}: set verified=True on {len(dataset)} samples")


if __name__ == "__main__":
    main()
