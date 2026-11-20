"""Tests for IngestConfig.from_env."""

import pytest
from weather_ingest.config import IngestConfig

BASE_ENV = {"WEATHER_API_KEY": "key", "DATA_BUCKET": "bucket", "LOCATIONS": "minsk"}


def test_parses_locations_with_semicolon_and_keeps_lat_lon_intact():
    """Locations are split by ';' so 'lat,lon' stays one location."""
    config = IngestConfig.from_env({**BASE_ENV, "LOCATIONS": " minsk ; 53.9,27.56 ;; "})
    assert config.locations == ("minsk", "53.9,27.56")


def test_applies_defaults_and_normalizes_prefix():
    """Optional variables fall back to defaults; slashes are stripped."""
    config = IngestConfig.from_env({**BASE_ENV, "BRONZE_PREFIX": "/bronze/x/"})
    assert config.bronze_prefix == "bronze/x"
    assert config.units == "metric"


@pytest.mark.parametrize("missing", ["WEATHER_API_KEY", "DATA_BUCKET", "LOCATIONS"])
def test_missing_required_variable_raises(missing):
    """Every required variable is enforced with a clear message."""
    env = {k: v for k, v in BASE_ENV.items() if k != missing}
    with pytest.raises(RuntimeError, match=missing):
        IngestConfig.from_env(env)


def test_locations_with_only_separators_raise():
    """LOCATIONS=';;' contains no usable location."""
    with pytest.raises(RuntimeError, match="any location"):
        IngestConfig.from_env({**BASE_ENV, "LOCATIONS": " ; ; "})
