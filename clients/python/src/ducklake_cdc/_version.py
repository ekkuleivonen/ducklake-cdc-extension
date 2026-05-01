"""Package version helpers."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ducklake-cdc")
except PackageNotFoundError:
    __version__ = "0.0.0"
