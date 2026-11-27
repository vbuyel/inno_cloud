"""Domain exceptions for expected, per-location ingestion failures."""


class IngestionError(Exception):
    """Base class for expected ingestion failures of a single location."""


class WeatherSourceError(IngestionError):
    """Raised when the weather provider cannot return a valid response."""


class ObjectAlreadyExistsError(IngestionError):
    """Raised when the target object already exists in storage."""
