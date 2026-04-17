"""Top-level package for the benchmark mainline and exploratory namespaces."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("scz-audit-engine")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["__version__"]
