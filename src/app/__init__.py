# fiftyone-sync, Apache-2.0 license
# Filename: src/app/__init__.py
# Description: App package initializer.

try:
    from importlib.metadata import PackageNotFoundError, version as _pkg_version

    try:
        __version__: str = _pkg_version("fiftyone-sync")
    except PackageNotFoundError:
        __version__ = "0.1.0"
except ImportError:
    __version__ = "0.1.0"
