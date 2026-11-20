"""Path conventions of the bronze and silver layers.

bronze: <bronze_prefix>/<location>/YYYY/MM/DD/HH/<timestamp>.json
silver: <silver_prefix>/<location>/YYYY/MM/DD/HH/<run_ts>.csv
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def location_slug(location: str) -> str:
    """Convert a location into a path segment (same rule as the ingest function).

    Input:   location - e.g. "Minsk, Belarus".
    Purpose: build stable folder names.
    Output:  slug, e.g. "minsk-belarus" ("unknown" if nothing is left).
    """
    return _NON_ALNUM.sub("-", location.strip().lower()).strip("-") or "unknown"


def floor_to_hour(moment: datetime) -> datetime:
    """Truncate a datetime to the start of its UTC hour.

    Input:   moment - timezone-aware datetime.
    Purpose: normalize timestamps to hourly partitions.
    Output:  UTC datetime with minutes/seconds/microseconds set to 0.
    """
    return moment.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


@dataclass(frozen=True, order=True)
class Partition:
    """Hourly partition of one location."""

    location: str
    hour: datetime

    @property
    def path(self) -> str:
        """Return the partition path fragment.

        Input:   none.
        Purpose: shared by bronze and silver layouts.
        Output:  "<location>/YYYY/MM/DD/HH".
        """
        return f"{self.location}/{self.hour:%Y/%m/%d/%H}"


@dataclass(frozen=True)
class SourceSelection:
    """What to read from GCS: a prefix plus an optional glob filter."""

    bucket: str
    prefix: str
    match_glob: str | None = None

    @property
    def uri(self) -> str:
        """Return a readable URI for logs.

        Input:   none.
        Purpose: show exactly what was scanned.
        Output:  "gs://bucket/prefix" (+ " [glob]" when a glob is set).
        """
        suffix = f" [{self.match_glob}]" if self.match_glob else ""
        return f"gs://{self.bucket}/{self.prefix}{suffix}"


class BronzeLayout:
    """Knows how bronze objects are named and how to select an hour."""

    _GS_URI = re.compile(r"^gs://(?P<bucket>[^/]+)/?(?P<prefix>.*)$")

    def __init__(self, prefix: str) -> None:
        """Create the layout.

        Input:   prefix - e.g. "bronze/weather/realtime".
        Purpose: one place for the bronze naming convention.
        Output:  None.
        """
        self._prefix = prefix.strip("/")
        self._object_re = re.compile(
            rf"^{re.escape(self._prefix)}/(?P<location>[^/]+)/"
            r"(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/(?P<h>\d{2})/[^/]+\.json$"
        )

    def previous_hour(self, bucket: str, reference: datetime) -> SourceSelection:
        """Select all locations' files of the hour before `reference`.

        Input:   bucket - data bucket; reference - aware datetime (run time).
        Purpose: default input when no source_path param is given.
        Output:  SourceSelection; e.g. reference 14:23 UTC -> hour 13 of all
                 locations via glob "<prefix>/*/YYYY/MM/DD/13/*.json".
        """
        hour = floor_to_hour(reference) - timedelta(hours=1)
        return SourceSelection(
            bucket=bucket,
            prefix=f"{self._prefix}/",
            match_glob=f"{self._prefix}/*/{hour:%Y/%m/%d/%H}/*.json",
        )

    def from_source_path(self, source_path: str, default_bucket: str) -> SourceSelection:
        """Select files under a user-provided path.

        Input:   source_path - "gs://bucket/prefix" or a prefix in default_bucket;
                 default_bucket - data bucket.
        Purpose: support the optional `source_path` DAG param.
        Output:  SourceSelection with all *.json under the prefix.
        """
        match = self._GS_URI.match(source_path.strip())
        bucket, prefix = (match["bucket"], match["prefix"]) if match else (default_bucket, source_path.strip())
        prefix = prefix.lstrip("/")
        return SourceSelection(bucket=bucket, prefix=prefix, match_glob=f"{prefix}**.json")

    def partition_of(self, object_name: str) -> Partition | None:
        """Extract location and hour from a bronze object name.

        Input:   object_name - e.g. "bronze/weather/realtime/minsk/2026/11/20/13/x.json".
        Purpose: place output files into the same partition as their input.
        Output:  Partition, or None if the name does not follow the convention.
        """
        match = self._object_re.match(object_name)
        if not match:
            return None
        hour = datetime(int(match["y"]), int(match["m"]), int(match["d"]), int(match["h"]), tzinfo=timezone.utc)
        return Partition(location=match["location"], hour=hour)


class SilverLayout:
    """Knows how silver CSV objects are named."""

    def __init__(self, prefix: str) -> None:
        """Create the layout.

        Input:   prefix - e.g. "silver/weather/realtime".
        Purpose: one place for the silver naming convention.
        Output:  None.
        """
        self._prefix = prefix.strip("/")

    def object_name(self, partition: Partition, run_ts: datetime) -> str:
        """Build the CSV object name for one partition.

        Input:   partition - location + hour; run_ts - processing time.
        Purpose: one CSV per location-hour per run (never overwrites).
        Output:  "<prefix>/<location>/YYYY/MM/DD/HH/<YYYYMMDDTHHMMSSZ>.csv".
        """
        return f"{self._prefix}/{partition.path}/{run_ts.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}.csv"
