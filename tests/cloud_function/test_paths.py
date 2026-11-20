"""Tests for BronzePathBuilder."""

from datetime import datetime, timezone

import pytest
from weather_ingest.paths import BronzePathBuilder


@pytest.mark.parametrize(
    ("location", "slug"),
    [("New York", "new-york"), ("53.9,27.56", "53-9-27-56"), ("  Minsk ", "minsk"), ("São Paulo", "s-o-paulo")],
)
def test_location_slug(location, slug):
    """Locations become lowercase, path-safe segments."""
    assert BronzePathBuilder.location_slug(location) == slug


def test_location_slug_rejects_empty_result():
    """A location without letters/digits cannot be a folder name."""
    with pytest.raises(ValueError):
        BronzePathBuilder.location_slug(" ,;! ")


def test_build_uses_hour_partition_and_utc_timestamp():
    """Object name follows <prefix>/<loc>/YYYY/MM/DD/HH/<ts>.json."""
    fetched_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    name = BronzePathBuilder("/bronze/weather/realtime/").build("Minsk", fetched_at)
    assert name == "bronze/weather/realtime/minsk/2026/01/02/03/20260102T030405Z.json"
