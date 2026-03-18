#!/usr/bin/env python3
# fiftyone-sync, Apache-2.0 license
# Filename: tests/test_vss_config.py
# Description: Tests for VSS project configuration parsing.
"""
Test script to verify VSS project configuration parsing.
Run this to validate the new vss_projects configuration format.
"""

import sys

from src.app.database_uri_config import DatabaseUriConfig, VssProjectConfig


def test_legacy_single_vss_project():
    """Test legacy single vss_project format."""
    yaml_content = """
projects:
  TestProject:
    vss_project: "test-vss-project"
    s3_bucket: "test-bucket"
    s3_prefix: "test-prefix"
    databases:
      - uri: "mongodb://localhost:27017/test"
        port: 5151
"""
    config = DatabaseUriConfig.from_yaml_string(yaml_content)

    assert "TestProject" in config.projects
    proj = config.projects["TestProject"]

    # Legacy vss_project should be set
    assert proj.vss_project == "test-vss-project"

    # vss_projects should be empty
    assert len(proj.vss_projects) == 0

    # Legacy S3 config should be set
    assert proj.s3_bucket == "test-bucket"
    assert proj.s3_prefix == "test-prefix"

    print("✓ Legacy single vss_project format works")


def test_new_nested_vss_projects():
    """Test new nested vss_projects format (no per-project vss_service; use global embeddings.service_url)."""
    yaml_content = """
projects:
  902004-Planktivore:
    vss_projects:
      high-mag:
        vss_project: "902004-Planktivore-HighMag"
        s3_bucket: "902004-planktivore-highmag"
        s3_prefix: "fiftyone/raw"
      low-mag:
        vss_project: "902004-Planktivore-LowMag"
        s3_bucket: "902004-planktivore-lowmag"
        s3_prefix: "fiftyone/raw"
    databases:
      - uri: "mongodb://localhost:27017/planktivore"
        port: 5151
"""
    config = DatabaseUriConfig.from_yaml_string(yaml_content)

    assert "902004-Planktivore" in config.projects
    proj = config.projects["902004-Planktivore"]

    # Legacy vss_project should be None
    assert proj.vss_project is None

    # vss_projects should have 2 entries
    assert len(proj.vss_projects) == 2
    assert "high-mag" in proj.vss_projects
    assert "low-mag" in proj.vss_projects

    # Check high-mag config (no vss_service; global embeddings.service_url used)
    high_mag = proj.vss_projects["high-mag"]
    assert isinstance(high_mag, VssProjectConfig)
    assert high_mag.vss_project == "902004-Planktivore-HighMag"
    assert high_mag.s3_bucket == "902004-planktivore-highmag"
    assert high_mag.s3_prefix == "fiftyone/raw"

    # Check low-mag config
    low_mag = proj.vss_projects["low-mag"]
    assert isinstance(low_mag, VssProjectConfig)
    assert low_mag.vss_project == "902004-Planktivore-LowMag"
    assert low_mag.s3_bucket == "902004-planktivore-lowmag"
    assert low_mag.s3_prefix == "fiftyone/raw"

    print("✓ New nested vss_projects format works")


def test_mixed_projects():
    """Test config with both legacy and new format projects."""
    yaml_content = """
projects:
  LegacyProject:
    vss_project: "legacy-vss"
    databases:
      - uri: "mongodb://localhost:27017/legacy"
        port: 5151

  ModernProject:
    vss_projects:
      config-a:
        vss_project: "modern-vss-a"
      config-b:
        vss_project: "modern-vss-b"
    databases:
      - uri: "mongodb://localhost:27017/modern"
        port: 5152
"""
    config = DatabaseUriConfig.from_yaml_string(yaml_content)

    # Check legacy project
    assert "LegacyProject" in config.projects
    legacy = config.projects["LegacyProject"]
    assert legacy.vss_project == "legacy-vss"
    assert len(legacy.vss_projects) == 0

    # Check modern project
    assert "ModernProject" in config.projects
    modern = config.projects["ModernProject"]
    assert modern.vss_project is None
    assert len(modern.vss_projects) == 2

    print("✓ Mixed legacy and new format projects work")


def test_database_manager_functions():
    """Test database_manager helper functions."""
    from src.app.database_manager import (
        get_vss_project_config,
        get_vss_projects_list,
    )

    # Create a test config (no per-project vss_service)
    yaml_content = """
projects:
  TestProject:
    vss_projects:
      high-mag:
        vss_project: "Test-HighMag"
        s3_bucket: "test-highmag"
        s3_prefix: "fiftyone"
      low-mag:
        vss_project: "Test-LowMag"
        s3_bucket: "test-lowmag"
    databases:
      - uri: "mongodb://localhost:27017/test"
        port: 5151
  CFEProject:
    vss_projects:
      902111-CFE:
        vss_project: "CFE"
    databases:
      - uri: "mongodb://localhost:27017/cfe"
        port: 5151
"""

    # Write temp config file
    import tempfile
    import os

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        temp_path = f.name

    try:
        # Set environment variable
        os.environ["FIFTYONE_SYNC_CONFIG_PATH"] = temp_path

        # Force reload config
        from src.app import database_manager

        database_manager._config = None
        database_manager._yaml_config = None

        # Test get_vss_projects_list
        vss_list = get_vss_projects_list("TestProject")
        assert len(vss_list) == 2
        assert vss_list[0]["key"] in ["high-mag", "low-mag"]
        assert vss_list[1]["key"] in ["high-mag", "low-mag"]

        high_mag_item = [v for v in vss_list if v["key"] == "high-mag"][0]
        assert high_mag_item["name"] == "Test-HighMag"

        # Test get_vss_project_config: vss_project is always vss_project_key when provided (no fallback)
        high_mag_config = get_vss_project_config("TestProject", "high-mag")
        assert high_mag_config is not None
        assert high_mag_config["vss_project"] == "high-mag"
        assert high_mag_config["s3_bucket"] == "test-highmag"
        assert high_mag_config["s3_prefix"] == "fiftyone"

        cfe_config = get_vss_project_config("CFEProject", "902111-CFE")
        assert cfe_config is not None
        assert cfe_config["vss_project"] == "902111-CFE"

        # Test get_vss_project_config without key (should not return anything since there are 2 projects)
        default_config = get_vss_project_config("TestProject", None)
        assert default_config is None  # No default when multiple exist

        print("✓ Database manager functions work correctly")

    finally:
        # Clean up
        os.unlink(temp_path)
        if "FIFTYONE_SYNC_CONFIG_PATH" in os.environ:
            del os.environ["FIFTYONE_SYNC_CONFIG_PATH"]


def test_legacy_compatibility():
    """Test that legacy single vss_project works with database_manager functions."""
    import tempfile
    import os
    from src.app.database_manager import get_vss_projects_list, get_vss_project_config

    yaml_content = """
projects:
  LegacyProject:
    vss_project: "legacy-vss-project"
    s3_bucket: "legacy-bucket"
    databases:
      - uri: "mongodb://localhost:27017/legacy"
        port: 5151
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        temp_path = f.name

    try:
        os.environ["FIFTYONE_SYNC_CONFIG_PATH"] = temp_path

        # Force reload config
        from src.app import database_manager

        database_manager._config = None
        database_manager._yaml_config = None

        # Test get_vss_projects_list returns legacy as "default"
        vss_list = get_vss_projects_list("LegacyProject")
        assert len(vss_list) == 1
        assert vss_list[0]["key"] == "default"
        assert vss_list[0]["name"] == "legacy-vss-project"

        # Test get_vss_project_config returns legacy config
        config = get_vss_project_config("LegacyProject", None)
        assert config is not None
        assert config["vss_project"] == "legacy-vss-project"
        assert config["s3_bucket"] == "legacy-bucket"

        print("✓ Legacy compatibility maintained")

    finally:
        os.unlink(temp_path)
        if "FIFTYONE_SYNC_CONFIG_PATH" in os.environ:
            del os.environ["FIFTYONE_SYNC_CONFIG_PATH"]


if __name__ == "__main__":
    print("Testing VSS project configuration parsing...\n")

    try:
        test_legacy_single_vss_project()
        test_new_nested_vss_projects()
        test_mixed_projects()
        test_database_manager_functions()
        test_legacy_compatibility()

        print("\n✅ All tests passed!")
        sys.exit(0)

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
