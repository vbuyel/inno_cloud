"""Composition root: the only place where concrete implementations are chosen."""

from __future__ import annotations

from functools import lru_cache

from google.cloud import storage

from weather_ingest.config import IngestConfig
from weather_ingest.paths import BronzePathBuilder
from weather_ingest.service import WeatherIngestionService
from weather_ingest.sources import TomorrowIoClient
from weather_ingest.storage import GcsRawStorage


@lru_cache(maxsize=1)
def shared_gcs_client() -> storage.Client:
    """Return a GCS client shared across invocations of the same instance.

    Avoid re-creating clients (and auth) on every request.

    Returns:
        storage.Client: A cached google.cloud.storage.Client.
    """
    return storage.Client()


def build_service(config: IngestConfig) -> WeatherIngestionService:
    """Build the production service: tomorrow.io source + GCS storage.

    Wire concrete implementations; to switch provider or storage, change only this function.

    Args:
        config: IngestConfig.

    Returns:
        WeatherIngestionService: Ready-to-use WeatherIngestionService.
    """
    return WeatherIngestionService(
        source=TomorrowIoClient(api_key=config.api_key, units=config.units),
        storage=GcsRawStorage(bucket_name=config.bucket_name, client=shared_gcs_client()),
        path_builder=BronzePathBuilder(config.bronze_prefix),
    )
