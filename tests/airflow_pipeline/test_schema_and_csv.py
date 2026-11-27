"""Tests for the silver/gold contract and CSV rendering."""

import csv
import io
import re
from datetime import datetime, timezone
from pathlib import Path

from include.weather.csv_writer import SilverCsvWriter
from include.weather.schema import BQ_SCHEMA, SILVER_COLUMNS, SilverRecord

TERRAFORM_BQ = Path(__file__).resolve().parents[2] / "terraform" / "bigquery.tf"
T = datetime(2026, 11, 20, 13, tzinfo=timezone.utc)


def test_columns_match_task_schema_order():
    """Column order is fixed by the task's BigQuery schema."""
    assert SILVER_COLUMNS[:7] == ("source_object", "event_time", "location_name", "location_lat",
                                  "location_lon", "location_type", "ingested_at_utc")
    assert len(SILVER_COLUMNS) == 12


def test_schema_matches_terraform_table():
    """Airflow schema and Terraform table must not drift apart."""
    fields = re.findall(r'name = "(\w+)", type = "(\w+)", mode = "(\w+)"', TERRAFORM_BQ.read_text())
    assert fields == [(f["name"], f["type"], f["mode"]) for f in BQ_SCHEMA]


def test_csv_has_header_nulls_as_empty_and_quotes_commas():
    """Header == schema; None -> empty cell; commas inside values are quoted."""
    row = SilverRecord("gs://b/o.json", T, "Minsk, Belarus", 53.9, None, None, T, {"temperature": 1.5})
    rows = list(csv.DictReader(io.StringIO(SilverCsvWriter().to_csv([row]))))
    assert list(rows[0]) == list(SILVER_COLUMNS)
    assert rows[0]["location_name"] == "Minsk, Belarus"
    assert rows[0]["location_lon"] == "" and rows[0]["weather_humidity"] == ""
    assert rows[0]["event_time"] == "2026-11-20 13:00:00+00:00"
