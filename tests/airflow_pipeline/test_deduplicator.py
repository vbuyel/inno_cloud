"""Tests for RecordDeduplicator."""

from datetime import datetime, timezone

from include.weather.deduplicator import RecordDeduplicator
from include.weather.schema import SilverRecord

T0 = datetime(2026, 11, 20, 13, tzinfo=timezone.utc)
T1 = datetime(2026, 11, 20, 13, 10, tzinfo=timezone.utc)


def record(source: str, time: datetime, name: str = "Minsk") -> SilverRecord:
    """Input: source object, time, name. Purpose: minimal record. Output: SilverRecord."""
    return SilverRecord(source, time, name, None, None, None, T1, {})


def test_keeps_record_from_latest_source_file():
    """Same (location_name, time): the latest file wins, the older one is dropped."""
    older, newer = record("gs://b/130000Z.json", T0), record("gs://b/130500Z.json", T0)
    result = RecordDeduplicator().deduplicate([newer, older])
    assert result.unique == [newer]
    assert result.duplicates == [older]


def test_different_keys_are_kept_and_sorted():
    """Different time or location are not duplicates; output is sorted by key."""
    records = [record("a", T1), record("b", T0, "Warsaw"), record("c", T0)]
    result = RecordDeduplicator().deduplicate(records)
    assert [r.dedup_key for r in result.unique] == [("Minsk", T0), ("Minsk", T1), ("Warsaw", T0)]
    assert result.duplicates == []
