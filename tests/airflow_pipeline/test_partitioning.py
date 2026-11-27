"""Tests for bronze/silver path conventions."""

from datetime import datetime, timezone

import pytest
from include.weather.partitioning import BronzeLayout, Partition, SilverLayout, location_slug

layout = BronzeLayout("bronze/weather/realtime/")


@pytest.mark.parametrize(
    ("reference", "hour_path"),
    [(datetime(2026, 11, 20, 14, 23, tzinfo=timezone.utc), "2026/11/20/13"),
     (datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), "2025/12/31/23")],
)
def test_previous_hour_selects_all_locations_of_previous_utc_hour(reference, hour_path):
    """14:23 -> hour 13; 00:05 on Jan 1 -> 23:00 of the previous year."""
    selection = layout.previous_hour("bkt", reference)
    assert selection.prefix == "bronze/weather/realtime/"
    assert selection.match_glob == f"bronze/weather/realtime/*/{hour_path}/*.json"


@pytest.mark.parametrize(
    ("source_path", "bucket", "prefix"),
    [("gs://other/bronze/x/", "other", "bronze/x/"), ("/bronze/x/", "default", "bronze/x/")],
)
def test_from_source_path_accepts_uri_or_prefix(source_path, bucket, prefix):
    """source_path may be a gs:// URI or a prefix inside the default bucket."""
    selection = layout.from_source_path(source_path, default_bucket="default")
    assert (selection.bucket, selection.prefix, selection.match_glob) == (bucket, prefix, f"{prefix}**.json")


def test_partition_of_parses_conventional_names_only():
    """Location and hour are read from the object name; other names give None."""
    name = "bronze/weather/realtime/minsk/2026/11/20/13/20261120T131000Z.json"
    assert layout.partition_of(name) == Partition("minsk", datetime(2026, 11, 20, 13, tzinfo=timezone.utc))
    assert layout.partition_of("bronze/weather/realtime/minsk/x.json") is None


def test_silver_object_name_and_slug():
    """Silver path mirrors the partition; run_ts names the file."""
    partition = Partition("minsk", datetime(2026, 11, 20, 13, tzinfo=timezone.utc))
    run_ts = datetime(2026, 11, 20, 14, 5, 7, tzinfo=timezone.utc)
    expected = "silver/weather/realtime/minsk/2026/11/20/13/20261120T140507Z.csv"
    assert SilverLayout("/silver/weather/realtime/").object_name(partition, run_ts) == expected
    assert location_slug("Minsk, Belarus") == "minsk-belarus" and location_slug("!!") == "unknown"
