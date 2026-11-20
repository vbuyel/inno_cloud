"""Runtime configuration of the ingestion function.

Environment variables (set by Terraform):
    WEATHER_API_KEY  tomorrow.io API key, injected from Secret Manager
    DATA_BUCKET      target GCS bucket name
    LOCATIONS        ';'-separated tomorrow.io locations, e.g. "minsk;53.9,27.56"
                     (';' because "lat,lon" locations contain commas)
    BRONZE_PREFIX    optional, defaults to "bronze/weather/realtime"
    WEATHER_UNITS    optional, "metric" (default) or "imperial"
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

LOCATIONS_SEPARATOR = ";"


@dataclass(frozen=True)
class IngestConfig:
    """Immutable runtime configuration of the ingestion function."""

    api_key: str
    bucket_name: str
    locations: tuple[str, ...]
    bronze_prefix: str = "bronze/weather/realtime"
    units: str = "metric"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> IngestConfig:
        """Build the configuration from environment variables.

        Validate required settings once, at the start of a request.

        Args:
            env: Mapping of variables; defaults to os.environ.

        Returns:
            IngestConfig: Validated configuration.

        Raises:
            RuntimeError: If a required variable is missing or LOCATIONS is empty.
        """
        env = os.environ if env is None else env

        locations = cls._parse_locations(cls._required(env, "LOCATIONS"))
        if not locations:
            raise RuntimeError("LOCATIONS does not contain any location")

        return cls(
            api_key=cls._required(env, "WEATHER_API_KEY"),
            bucket_name=cls._required(env, "DATA_BUCKET"),
            locations=locations,
            bronze_prefix=env.get("BRONZE_PREFIX", cls.bronze_prefix).strip("/"),
            units=env.get("WEATHER_UNITS", cls.units),
        )

    @staticmethod
    def _required(env: Mapping[str, str], name: str) -> str:
        """Read a mandatory variable.

        Fail fast with a clear message instead of a later KeyError.

        Args:
            env: Mapping of variables.
            name: Variable name.

        Returns:
            str: The stripped, non-empty value.

        Raises:
            RuntimeError: If missing.
        """
        value = env.get(name, "").strip()
        if not value:
            raise RuntimeError(f"Environment variable {name} is not set")
        return value

    @staticmethod
    def _parse_locations(raw: str) -> tuple[str, ...]:
        """Split the LOCATIONS value into individual locations.

        Normalize user input (trim spaces, drop empty items).

        Args:
            raw: E.g. " minsk; 53.9,27.56 ".

        Returns:
            tuple[str, ...]: Locations, e.g. ("minsk", "53.9,27.56").
        """
        return tuple(loc.strip() for loc in raw.split(LOCATIONS_SEPARATOR) if loc.strip())
