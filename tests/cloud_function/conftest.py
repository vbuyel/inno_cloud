"""Fakes shared by Cloud Function tests (no network, no GCP)."""

from __future__ import annotations

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
        """Input: errors - location -> exception to raise. Output: None."""
        self.errors = errors or {}

    def fetch_realtime(self, location: str) -> str:
        """Input: location. Output: raw JSON text or raises configured error."""
        if location in self.errors:
            raise self.errors[location]
        return '{"data": {}, "location": {"name": "%s"}}' % location


class MemoryStorage(RawStorage):
    """In-memory RawStorage that refuses overwrites like GCS does."""

    def __init__(self) -> None:
        """Input: none. Output: None."""
        self.objects: dict[str, str] = {}

    def write(self, object_name: str, payload: str) -> str:
        """Input: object name and payload. Output: fake gs:// URI."""
        if object_name in self.objects:
            raise ObjectAlreadyExistsError(f"object already exists: {object_name}")  # same text as GcsRawStorage
        self.objects[object_name] = payload
        return f"gs://test-bucket/{object_name}"


@pytest.fixture
def fake_source():
    """Input: none. Purpose: build FakeSource with errors. Output: factory(errors) -> FakeSource."""
    return lambda errors=None: FakeSource(errors)


@pytest.fixture
def storage() -> MemoryStorage:
    """Input: none. Purpose: fresh in-memory storage. Output: MemoryStorage."""
    return MemoryStorage()


@pytest.fixture
def make_service(storage):
    """Input: none. Purpose: build a service with fakes. Output: factory(source) -> service."""

    def factory(source: WeatherSource | None = None) -> WeatherIngestionService:
        return WeatherIngestionService(
            source or FakeSource(), storage, BronzePathBuilder("bronze/weather/realtime"), clock=lambda: FIXED_NOW
        )

    return factory
