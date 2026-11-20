"""Bronze-layer weather ingestion package.

Modules (one responsibility each):
    config      - runtime configuration loaded from environment variables
    errors      - domain exceptions
    sources     - weather providers (interface + tomorrow.io client)
    storage     - raw payload storage (interface + GCS implementation)
    paths       - bronze object naming convention
    report      - result of an ingestion run
    service     - orchestration: fetch -> build path -> store
    factory     - composition root: wires concrete implementations together
"""

from weather_ingest.config import IngestConfig
from weather_ingest.factory import build_service
from weather_ingest.report import IngestionReport
from weather_ingest.service import WeatherIngestionService

__all__ = ["IngestConfig", "IngestionReport", "WeatherIngestionService", "build_service"]
