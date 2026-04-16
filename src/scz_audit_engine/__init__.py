"""Top-level package for the strict-open audit engine bootstrap."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("scz-audit-engine")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["__version__"]
