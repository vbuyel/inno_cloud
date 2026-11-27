"""Tests for IngestionReport."""

import json
from http import HTTPStatus

import pytest
from weather_ingest.report import IngestionReport


@pytest.mark.parametrize(
    ("written", "errors", "status"),
    [
        (["gs://a"], {}, HTTPStatus.OK),
        (["gs://a"], {"x": "boom"}, HTTPStatus.MULTI_STATUS),
        ([], {"x": "boom"}, HTTPStatus.BAD_GATEWAY),
    ],
)
def test_http_status_reflects_outcome(written, errors, status):
    """All stored -> OK, partial -> MULTI_STATUS, nothing -> BAD_GATEWAY."""
    assert IngestionReport(written=written, errors=errors).http_status is status


def test_add_methods_and_json_serialization():
    """add_success/add_error fill the report; to_json returns both parts."""
    report = IngestionReport()
    report.add_success("gs://b/o.json")
    report.add_error("minsk", "tomorrow.io HTTP 429 Too Many Requests")
    assert json.loads(report.to_json()) == {
        "written": ["gs://b/o.json"],
        "errors": {"minsk": "tomorrow.io HTTP 429 Too Many Requests"},
    }
