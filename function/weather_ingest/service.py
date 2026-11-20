"""Ingestion orchestration: fetch -> build path -> store, per location.

Depends only on abstractions (WeatherSource, RawStorage), never on concrete
APIs or SDKs; concrete implementations are wired in weather_ingest.factory.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from datetime import datetime, timezone

from weather_ingest.errors import IngestionError, ObjectAlreadyExistsError
from weather_ingest.paths import BronzePathBuilder
from weather_ingest.report import IngestionReport
from weather_ingest.sources import WeatherSource
from weather_ingest.storage import RawStorage

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    """Return the current time.

    Input:   none.
    Purpose: default clock for the service (replaceable in tests).
    Output:  timezone-aware datetime in UTC.
    """
    return datetime.now(timezone.utc)


class WeatherIngestionService:
    """Fetches weather for every location and stores raw payloads in bronze."""

    def __init__(
        self,
        source: WeatherSource,
        storage: RawStorage,
        path_builder: BronzePathBuilder,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        """Wire the service with its dependencies.

        Input:   source - weather provider; storage - raw storage;
                 path_builder - object name convention; clock - returns the
                 current UTC time (injectable for tests).
        Purpose: dependency injection: any part can be replaced (another API,
                 another storage) without touching the orchestration logic.
        Output:  None.
        """
        self._source = source
        self._storage = storage
        self._paths = path_builder
        self._clock = clock

    def ingest_location(self, location: str) -> str:
        """Run fetch -> build path -> store for a single location.

        Input:   location - one configured location.
        Purpose: isolate one unit of work so failures do not affect others.
        Output:  URI of the stored object; raises IngestionError subclasses.
        """
        fetched_at = self._clock()
        payload = self._source.fetch_realtime(location)
        object_name = self._paths.build(location, fetched_at)
        return self._storage.write(object_name, payload)

    def run(self, locations: Iterable[str]) -> IngestionReport:
        """Ingest all locations and collect a report.

        Input:   locations - locations to process.
        Purpose: process every location independently and log the outcome.
        Output:  IngestionReport with stored URIs and per-location errors.
        """
        report = IngestionReport()
        for location in locations:
            self._ingest_into_report(location, report)
        return report

    def _ingest_into_report(self, location: str, report: IngestionReport) -> None:
        """Ingest one location and record the outcome instead of raising.

        Input:   location - one location; report - report to update.
        Purpose: one bad location must not stop the rest of the run.
        Output:  None (report is updated in place).
        """
        try:
            uri = self.ingest_location(location)
            report.add_success(uri)
            logger.info("Stored %s -> %s", location, uri)
        except ObjectAlreadyExistsError as exc:
            report.add_error(location, str(exc))
            logger.warning("Skipped %s: %s", location, exc)
        except IngestionError as exc:
            report.add_error(location, str(exc))
            logger.error("Failed %s: %s", location, exc)
        except Exception:  # noqa: BLE001 - one bad location must not stop the rest
            report.add_error(location, "unexpected error")
            logger.exception("Unexpected error while ingesting %s", location)
