"""Tests for PayloadValidator: every validation rule from the task."""

import pytest
from include.weather.validator import PayloadValidator, ValidationRules, is_number, parse_event_time

validator = PayloadValidator()


def test_valid_payload_has_no_errors(make_payload):
    """A complete, in-range payload passes."""
    assert validator.validate(make_payload()) == []


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "not a JSON object"),
        ({"location": {}}, "'data' is missing"),
        ({"data": {"time": "x"}, "location": {}}, "'data.values' is missing"),
        ({"data": {"values": {}}, "location": "minsk"}, "'location' is missing"),
    ],
)
def test_structure_rules(payload, message):
    """data, location and data.values must exist and be objects."""
    assert any(message in error for error in validator.validate(payload))


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"time": None}, "'data.time'"),
        ({"time": "2026-11-20T13:00:00"}, "'data.time'"),  # naive timestamp
        ({"name": "  "}, "'location.name'"),
    ],
)
def test_mandatory_fields(make_payload, overrides, message):
    """event_time and location_name are REQUIRED in gold."""
    assert any(message in error for error in validator.validate(make_payload(**overrides)))


@pytest.mark.parametrize("bad", ["12", True, [1], float("nan"), float("inf")])
def test_metric_must_be_numeric(make_payload, bad):
    """Strings, booleans, NaN and infinity are rejected."""
    errors = validator.validate(make_payload(temperature=bad))
    assert errors and "not numeric" in errors[0]


@pytest.mark.parametrize(
    ("metric", "value"),
    [("humidity", 100.1), ("humidity", -1), ("cloudCover", 101), ("precipitationProbability", -0.5),
     ("temperature", -90.1), ("temperature", 70.1), ("windSpeed", -1), ("windSpeed", 150.1)],
)
def test_out_of_range_metrics_are_rejected(make_payload, metric, value):
    """Each metric range from the task is enforced."""
    low, high = ValidationRules().ranges[metric]
    assert validator.validate(make_payload(**{metric: value})) == [f"{metric}={value} is outside [{low}, {high}]"]


@pytest.mark.parametrize(
    ("metric", "value"),
    [("humidity", 0), ("humidity", 100), ("temperature", -90), ("temperature", 70), ("windSpeed", 150)],
)
def test_range_bounds_are_inclusive(make_payload, metric, value):
    """Boundary values are valid."""
    assert validator.validate(make_payload(**{metric: value})) == []


def test_absent_or_null_metrics_are_allowed(make_payload):
    """Rules apply only 'if field is present'."""
    assert validator.validate(make_payload(drop=("humidity",), cloudCover=None)) == []


def test_helpers():
    """is_number and parse_event_time edge cases."""
    assert is_number(1) and is_number(1.5) and not is_number(False)
    assert parse_event_time("2026-11-20T13:00:00Z").utcoffset().total_seconds() == 0
    assert parse_event_time("not a date") is None
