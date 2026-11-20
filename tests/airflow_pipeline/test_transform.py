"""End-to-end tests of SilverTransformer (pure Python, no GCS)."""

from datetime import datetime, timezone

import pytest
from include.transform import NoValidRecordsError, SilverTransformer
from include.weather.partitioning import BronzeLayout, SilverLayout

INGESTED_AT = datetime(2026, 11, 20, 14, 5, tzinfo=timezone.utc)


@pytest.fixture
def transformer() -> SilverTransformer:
    """Input: none. Purpose: transformer with project layouts. Output: SilverTransformer."""
    return SilverTransformer(BronzeLayout("bronze/weather/realtime"), SilverLayout("silver/weather/realtime"))


def test_mixed_batch(transformer, make_file, make_payload):
    """Valid rows are kept, invalid dropped, duplicates removed, one CSV per partition."""
    files = [
        make_file("130000Z.json", make_payload()),
        make_file("130500Z.json", make_payload(temperature=1.7)),  # duplicate of 13:00
        make_file("131000Z.json", make_payload(time="2026-11-20T13:10:00Z")),
        make_file("132000Z.json", make_payload(humidity=120)),
        make_file("133000Z.json", b"{broken"),
    ]
    result = transformer.transform(files, INGESTED_AT)
    assert result.summary() == {"valid_records": 2, "rejected_records": 2, "duplicates_removed": 1, "silver_files": 1}
    (name, text), = result.outputs.items()
    assert name == "silver/weather/realtime/minsk/2026/11/20/13/20261120T140500Z.csv"
    assert ",1.7," in text and "130000Z.json" not in text


def test_no_valid_records_fails(transformer, make_file, make_payload):
    """If nothing survives validation the run must fail."""
    files = [make_file("a.json", make_payload(humidity=500)), make_file("b.json", b"")]
    with pytest.raises(NoValidRecordsError, match="rejected_records': 2"):
        transformer.transform(files, INGESTED_AT)


def test_each_location_gets_its_own_csv(transformer, make_file, make_payload):
    """Partitions follow the bronze path: two locations -> two CSV files."""
    files = [
        make_file("a.json", make_payload()),
        make_file("b.json", make_payload(name="Warsaw"), prefix="bronze/weather/realtime/warsaw/2026/11/20/13/"),
    ]
    outputs = transformer.transform(files, INGESTED_AT).outputs
    assert sorted(name.split("/")[3] for name in outputs) == ["minsk", "warsaw"]


def test_files_outside_convention_use_record_location_and_hour(transformer, make_file, make_payload):
    """Custom source_path: partition falls back to location_name + event hour."""
    files = [make_file("x.json", make_payload(time="2026-11-20T09:40:00Z"), prefix="manual/upload/")]
    (name,) = transformer.transform(files, INGESTED_AT).outputs
    assert name.startswith("silver/weather/realtime/minsk-belarus/2026/11/20/09/")
