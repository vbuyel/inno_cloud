"""Bronze object naming convention (Airflow relies on the same layout)."""

from __future__ import annotations

import re
from datetime import datetime


class BronzePathBuilder:
    """Builds object names: <prefix>/<location>/YYYY/MM/DD/HH/<timestamp>.json."""

    _NON_ALNUM = re.compile(r"[^a-z0-9]+")

    def __init__(self, prefix: str) -> None:
        """Create a builder for a given bronze prefix.

        Input:   prefix - base path, e.g. "bronze/weather/realtime".
        Purpose: keep the path convention in one place.
        Output:  None.
        """
        self._prefix = prefix.strip("/")

    @classmethod
    def location_slug(cls, location: str) -> str:
        """Convert a location into a safe path segment.

        Input:   location - e.g. "New York" or "53.9,27.56".
        Purpose: produce stable, URL/path-safe folder names.
        Output:  slug, e.g. "new-york" or "53-9-27-56"; raises ValueError if
                 nothing usable is left.
        """
        slug = cls._NON_ALNUM.sub("-", location.strip().lower()).strip("-")
        if not slug:
            raise ValueError(f"Cannot build a path segment from location {location!r}")
        return slug

    def build(self, location: str, fetched_at: datetime) -> str:
        """Build the full object name for one payload.

        Input:   location - raw location string; fetched_at - UTC datetime of
                 the API call.
        Purpose: place the file into the hourly partition of its location.
        Output:  object name, e.g.
                 "bronze/weather/realtime/minsk/2026/11/20/14/20261120T141000Z.json".
        """
        return (
            f"{self._prefix}/{self.location_slug(location)}/"
            f"{fetched_at:%Y/%m/%d/%H}/{fetched_at:%Y%m%dT%H%M%SZ}.json"
        )
