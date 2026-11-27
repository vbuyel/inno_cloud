"""Fakes shared by Cloud Function tests (no network, no GCP)."""

from __future__ import annotations

import functools
from datetime import datetime, timezone

import pytest
from weather_ingest.errors import ObjectAlreadyExistsError
from weather_ingest.paths import BronzePathBuilder
from weather_ingest.service import WeatherIngestionService
from weather_ingest.sources import WeatherSource
from weather_ingest.storage import RawStorage

FIXED_NOW = datetime(2026, 11, 20, 14, 10, 0, tzinfo=timezone.utc)


class FakeSource(WeatherSource):
    """Returns a fixed payload or raises a configured error per location."""

    def __init__(self, errors: dict[str, Exception] | None = None) -> None:
        """Create the fake source.

        Args:
            errors: Mapping of location to the exception to raise for it.
        """
        self.errors = errors or {}

    def fetch_realtime(self, location: str) -> str:
        """Return a fixed payload for the location.

        Args:
            location: Requested location.

        Returns:
            str: A minimal JSON payload containing the location name.

        Raises:
            Exception: The error configured for this location, if any.
        """
        if location in self.errors:
            raise self.errors[location]
        return '{"data": {}, "location": {"name": "%s"}}' % location


class MemoryStorage(RawStorage):
    """In-memory RawStorage that refuses overwrites like GCS does."""

    def __init__(self) -> None:
        """Create an empty in-memory storage."""
        self.objects: dict[str, str] = {}

    def write(self, object_name: str, payload: str) -> str:
        """Store a payload unless the name is already taken.

        Args:
            object_name: Object path.
            payload: Raw text.

        Returns:
            str: A fake ``gs://test-bucket/...`` URI.

        Raises:
            ObjectAlreadyExistsError: If the object already exists.
        """
        if object_name in self.objects:
            raise ObjectAlreadyExistsError(f"object already exists: {object_name}")  # same text as GcsRawStorage
        self.objects[object_name] = payload
        return f"gs://test-bucket/{object_name}"


def build_service(storage: RawStorage, source: WeatherSource | None = None) -> WeatherIngestionService:
    """Build an ingestion service wired with fakes and a fixed clock.

    Args:
        storage: Storage to write into.
        source: Weather source; defaults to a FakeSource without errors.

    Returns:
        WeatherIngestionService: A WeatherIngestionService that writes into ``storage``.
    """
    return WeatherIngestionService(
        source or FakeSource(), storage, BronzePathBuilder("bronze/weather/realtime"), clock=lambda: FIXED_NOW
    )


@pytest.fixture
def fake_source():
    """Provide a factory for FakeSource.

    Returns:
        Callable[..., FakeSource]: A callable ``factory(errors=None)`` that returns a FakeSource.
    """
    return lambda errors=None: FakeSource(errors)


@pytest.fixture
def storage() -> MemoryStorage:
    """Provide a fresh in-memory storage.

    Returns:
        MemoryStorage: An empty MemoryStorage.
    """
    return MemoryStorage()


@pytest.fixture
def make_service(storage):
    """Provide a factory for services wired with fakes and a fixed clock.

    Returns:
        Callable[..., WeatherIngestionService]: ``build_service`` bound to the fixture storage. Call
            it as ``factory(source=None)`` to get a WeatherIngestionService.
    """
    return functools.partial(build_service, storage)
