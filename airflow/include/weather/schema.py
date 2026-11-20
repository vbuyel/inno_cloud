"""Silver CSV / gold BigQuery contract: column order, types and the record model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

# Raw metric name in data.values -> silver column name.
WEATHER_METRICS: dict[str, str] = {
    "temperature": "weather_temperature",
    "humidity": "weather_humidity",
    "windSpeed": "weather_windSpeed",
    "cloudCover": "weather_cloudCover",
    "precipitationProbability": "weather_precipitationProbability",
}

# Column order is shared by the CSV header and the BigQuery table.
BQ_SCHEMA: tuple[dict[str, str], ...] = (
    {"name": "source_object", "type": "STRING", "mode": "REQUIRED"},
    {"name": "event_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "location_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "location_lat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "location_lon", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "location_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ingested_at_utc", "type": "TIMESTAMP", "mode": "REQUIRED"},
    *({"name": column, "type": "FLOAT", "mode": "NULLABLE"} for column in WEATHER_METRICS.values()),
)

SILVER_COLUMNS: tuple[str, ...] = tuple(field["name"] for field in BQ_SCHEMA)


def to_bq_timestamp(value: datetime) -> str:
    """Format a datetime for BigQuery CSV loads.

    Input:   value - timezone-aware datetime.
    Purpose: produce an unambiguous UTC timestamp BigQuery parses natively.
    Output:  string like "2026-11-20 14:10:00+00:00".
    """
    return value.astimezone(timezone.utc).isoformat(sep=" ")


@dataclass(frozen=True)
class SilverRecord:
    """One validated, flattened weather observation (one CSV / BigQuery row)."""

    source_object: str
    event_time: datetime
    location_name: str
    location_lat: float | None
    location_lon: float | None
    location_type: str | None
    ingested_at_utc: datetime
    metrics: dict[str, float | None]

    @property
    def dedup_key(self) -> tuple[str, datetime]:
        """Business key of the record.

        Input:   none.
        Purpose: identify the same observation coming from different files.
        Output:  (location_name, event_time).
        """
        return self.location_name, self.event_time

    def to_row(self) -> dict[str, str]:
        """Serialize the record into a CSV row.

        Input:   none.
        Purpose: map fields to SILVER_COLUMNS; None becomes "" (NULL in BigQuery).
        Output:  dict column -> string value.
        """
        row = {
            "source_object": self.source_object,
            "event_time": to_bq_timestamp(self.event_time),
            "location_name": self.location_name,
            "location_lat": self.location_lat,
            "location_lon": self.location_lon,
            "location_type": self.location_type,
            "ingested_at_utc": to_bq_timestamp(self.ingested_at_utc),
            **{column: self.metrics.get(metric) for metric, column in WEATHER_METRICS.items()},
        }
        return {column: "" if row[column] is None else str(row[column]) for column in SILVER_COLUMNS}
