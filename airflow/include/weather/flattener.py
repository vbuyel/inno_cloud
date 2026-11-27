"""Flattening of a nested tomorrow.io payload into a SilverRecord."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from include.weather.schema import WEATHER_METRICS, SilverRecord
from include.weather.validator import is_number, parse_event_time


class PayloadFlattener:
    """Maps data.time, location.* and data.values.* onto silver columns."""

    def flatten(self, payload: dict, source_object: str, ingested_at: datetime) -> SilverRecord:
        """Build a flat record from a VALIDATED payload.

        Turn nested JSON into one row of the silver/gold schema.

        Args:
            payload: Payload that passed PayloadValidator.
            source_object: "gs://bucket/path.json" of the bronze file.
            ingested_at: Processing timestamp (same for the whole run).

        Returns:
            SilverRecord: One row of the silver/gold schema.
        """
        event_time = parse_event_time(payload["data"]["time"])
        if event_time is None:  # guarded by the validator; keeps the type checker honest
            raise ValueError("payload must be validated before flattening")
        values = payload["data"]["values"]
        return SilverRecord(
            source_object=source_object,
            event_time=event_time,
            ingested_at_utc=ingested_at,
            metrics={metric: self._optional_float(values.get(metric)) for metric in WEATHER_METRICS},
            **self._location_fields(payload["location"]),
        )

    def _location_fields(self, location: dict) -> dict[str, Any]:
        """Extract location columns.

        Map name/lat/lon/type onto silver columns.

        Args:
            location: Validated "location" object.

        Returns:
            dict[str, Any]: Values for location_name, location_lat, location_lon and location_type.
        """
        return {
            "location_name": location["name"].strip(),
            "location_lat": self._optional_float(location.get("lat")),
            "location_lon": self._optional_float(location.get("lon")),
            "location_type": self._optional_str(location.get("type")),
        }

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        """Convert an optional numeric value.

        Optional columns become NULL instead of failing the record.

        Args:
            value: JSON value or None.

        Returns:
            float | None: The value as float, or None if missing or not numeric.
        """
        return float(value) if is_number(value) else None

    @staticmethod
    def _optional_str(value: Any) -> str | None:
        """Convert an optional text value.

        Keep only non-empty strings.

        Args:
            value: JSON value or None.

        Returns:
            str | None: The stripped string, or None if missing or empty.
        """
        return value.strip() or None if isinstance(value, str) else None
