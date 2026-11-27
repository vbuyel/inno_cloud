"""Tests for PayloadFlattener."""

from datetime import datetime, timezone

import pytest
from include.weather.flattener import PayloadFlattener

INGESTED = datetime(2026, 11, 20, 14, 5, tzinfo=timezone.utc)


def test_flattens_nested_payload(make_payload):
    """data.time, location.* and data.values.* map onto silver fields."""
    record = PayloadFlattener().flatten(make_payload(name=" Minsk "), "gs://b/o.json", INGESTED)
    assert record.event_time == datetime(2026, 11, 20, 13, tzinfo=timezone.utc)
    assert (record.location_name, record.location_lat, record.location_type) == ("Minsk", 53.9, "administrative")
    assert record.metrics["humidity"] == 80.0
    assert (record.source_object, record.ingested_at_utc) == ("gs://b/o.json", INGESTED)


def test_missing_optional_fields_become_none(make_payload):
    """Optional location fields and absent metrics become NULL."""
    payload = make_payload(drop=("cloudCover",))
    payload["location"].update(lat="53.9", type="")
    record = PayloadFlattener().flatten(payload, "gs://b/o.json", INGESTED)
    assert record.location_lat is None and record.location_type is None
    assert record.metrics["cloudCover"] is None


def test_refuses_unvalidated_payload(make_payload):
    """Flattening a payload with an invalid time is a programming error."""
    with pytest.raises(ValueError):
        PayloadFlattener().flatten(make_payload(time="bad"), "gs://b/o.json", INGESTED)
