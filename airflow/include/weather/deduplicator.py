"""Deduplication of silver records by business key (location_name, event_time)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from include.weather.schema import SilverRecord


@dataclass(frozen=True)
class DedupResult:
    """Unique records plus the duplicates that were dropped."""

    unique: list[SilverRecord]
    duplicates: list[SilverRecord]


class RecordDeduplicator:
    """Keeps one record per (location_name, event_time)."""

    def deduplicate(self, records: Iterable[SilverRecord]) -> DedupResult:
        """Remove duplicates, keeping the record from the latest source file.

        The tomorrow.io API may return the same observation in several calls; gold must contain each
        (location_name, event_time) once.

        Args:
            records: Validated records (any order).

        Returns:
            DedupResult: DedupResult(unique sorted by key, dropped duplicates).
        """
        kept: dict[tuple, SilverRecord] = {}
        duplicates: list[SilverRecord] = []
        # Bronze file names contain the fetch timestamp, so sorting by
        # source_object makes "latest file wins" deterministic.
        for record in sorted(records, key=lambda r: r.source_object):
            previous = kept.get(record.dedup_key)
            if previous is not None:
                duplicates.append(previous)
            kept[record.dedup_key] = record
        return DedupResult(unique=sorted(kept.values(), key=lambda r: r.dedup_key), duplicates=duplicates)
