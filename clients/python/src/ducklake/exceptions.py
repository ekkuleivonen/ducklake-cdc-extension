"""Exception hierarchy for the DuckLake Python client."""


class DuckLakeError(Exception):
    """Base class for all client-level errors."""


class DuckLakeConfigError(DuckLakeError, ValueError):
    """Raised when DuckLake connection configuration is invalid."""


class DuckLakeConnectionError(DuckLakeError):
    """Raised when the client cannot create or initialize a DuckDB connection."""


class DuckLakeQueryError(DuckLakeError):
    """Raised when a query fails through the client wrapper."""


class ResultCardinalityError(DuckLakeQueryError):
    """Raised when a result does not match the expected row or value count."""
