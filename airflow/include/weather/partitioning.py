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

    Build stable folder names.

    Args:
        location: E.g. "Minsk, Belarus".

    Returns:
        str: Path segment such as "minsk-belarus", or "unknown" if nothing is left.
    """
    return _NON_ALNUM.sub("-", location.strip().lower()).strip("-") or "unknown"


def floor_to_hour(moment: datetime) -> datetime:
    """Truncate a datetime to the start of its UTC hour.

    Normalize timestamps to hourly partitions.

    Args:
        moment: Timezone-aware datetime.

    Returns:
        datetime: UTC datetime with minutes/seconds/microseconds set to 0.
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

        Shared by bronze and silver layouts.

        Returns:
            str: "<location>/YYYY/MM/DD/HH".
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

        Show exactly what was scanned.

        Returns:
            str: "gs://bucket/prefix" (+ " [glob]" when a glob is set).
        """
        suffix = f" [{self.match_glob}]" if self.match_glob else ""
        return f"gs://{self.bucket}/{self.prefix}{suffix}"


class BronzeLayout:
    """Knows how bronze objects are named and how to select an hour."""

    _GS_URI = re.compile(r"^gs://(?P<bucket>[^/]+)/?(?P<prefix>.*)$")

    def __init__(self, prefix: str) -> None:
        """Create the layout.

        One place for the bronze naming convention.

        Args:
            prefix: E.g. "bronze/weather/realtime".
        """
        self._prefix = prefix.strip("/")
        self._object_re = re.compile(
            rf"^{re.escape(self._prefix)}/(?P<location>[^/]+)/"
            r"(?P<y>\d{4})/(?P<m>\d{2})/(?P<d>\d{2})/(?P<h>\d{2})/[^/]+\.json$"
        )

    def previous_hour(self, bucket: str, reference: datetime) -> SourceSelection:
        """Select all locations' files of the hour before `reference`.

        Default input when no source_path param is given.

        Args:
            bucket: Data bucket.
            reference: Aware datetime (run time).

        Returns:
            SourceSelection: Glob over all locations for that hour, e.g. reference 14:23 UTC ->
                "<prefix>/*/YYYY/MM/DD/13/*.json".
        """
        hour = floor_to_hour(reference) - timedelta(hours=1)
        return SourceSelection(
            bucket=bucket,
            prefix=f"{self._prefix}/",
            match_glob=f"{self._prefix}/*/{hour:%Y/%m/%d/%H}/*.json",
        )

    def from_source_path(self, source_path: str, default_bucket: str) -> SourceSelection:
        """Select files under a user-provided path.

        Support the optional `source_path` DAG param.

        Args:
            source_path: "gs://bucket/prefix" or a prefix in default_bucket.
            default_bucket: Data bucket.

        Returns:
            SourceSelection: Selection of all *.json files under the prefix.
        """
        match = self._GS_URI.match(source_path.strip())
        bucket, prefix = (match["bucket"], match["prefix"]) if match else (default_bucket, source_path.strip())
        prefix = prefix.lstrip("/")
        return SourceSelection(bucket=bucket, prefix=prefix, match_glob=f"{prefix}**.json")

    def partition_of(self, object_name: str) -> Partition | None:
        """Extract location and hour from a bronze object name.

        Place output files into the same partition as their input.

        Args:
            object_name: E.g. "bronze/weather/realtime/minsk/2026/11/20/13/x.json".

        Returns:
            Partition | None: Partition, or None if the name does not follow the convention.
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

        One place for the silver naming convention.

        Args:
            prefix: E.g. "silver/weather/realtime".
        """
        self._prefix = prefix.strip("/")

    def object_name(self, partition: Partition, run_ts: datetime) -> str:
        """Build the CSV object name for one partition.

        One CSV per location-hour per run (never overwrites).

        Args:
            partition: Location + hour.
            run_ts: Processing time.

        Returns:
            str: "<prefix>/<location>/YYYY/MM/DD/HH/<YYYYMMDDTHHMMSSZ>.csv".
        """
        return f"{self._prefix}/{partition.path}/{run_ts.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}.csv"
