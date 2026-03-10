# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_config_parse.py
# Description: Quick test comparing broken vs correct config.yaml structure.
"""Quick test: compare broken vs correct config.yaml structure."""

from src.app.database_uri_config import DatabaseUriConfig

BROKEN = """
projects:
  902004-Planktivore:
    high-mag:
      vss_project: "902004-Planktivore-HighMag"
      vss_service: "https://cortex.shore.mbari.org/vss"
      s3_bucket: "902004-planktivore-highmag"
      s3_prefix: "fiftyone/raw"
    low-mag:
      vss_project: "902004-Planktivore-LowMag"
      vss_service: "https://cortex.shore.mbari.org/vss"
      s3_bucket: "902004-planktivore-lowmag"
      s3_prefix: "fiftyone/raw"
"""

CORRECT = """
is_enterprise: true
projects:
  902004-Planktivore:
    vss_projects:
      high-mag:
        vss_project: "902004-Planktivore-HighMag"
        vss_service: "https://cortex.shore.mbari.org/vss"
        s3_bucket: "902004-planktivore-highmag"
        s3_prefix: "fiftyone/raw"
      low-mag:
        vss_project: "902004-Planktivore-LowMag"
        vss_service: "https://cortex.shore.mbari.org/vss"
        s3_bucket: "902004-planktivore-lowmag"
        s3_prefix: "fiftyone/raw"
"""

print("=== BROKEN (your current config.yaml) ===")
cfg = DatabaseUriConfig.from_yaml_string(BROKEN)
proj = cfg.projects.get("902004-Planktivore")
print(f"  vss_projects : {proj.vss_projects}")
print(f"  databases    : {proj.databases}")

print()
print("=== CORRECT (with vss_projects: wrapper) ===")
cfg = DatabaseUriConfig.from_yaml_string(CORRECT)
proj = cfg.projects.get("902004-Planktivore")
print(f"  is_enterprise: {cfg.is_enterprise}")
print(f"  databases    : {proj.databases}")
for key, v in proj.vss_projects.items():
    print(f"  [{key}]")
    print(f"    vss_project : {v.vss_project}")
    print(f"    vss_service : {v.vss_service}")
    print(f"    s3_bucket   : {v.s3_bucket}")
    print(f"    s3_prefix   : {v.s3_prefix}")
