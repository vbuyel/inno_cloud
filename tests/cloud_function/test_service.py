"""Tests for WeatherIngestionService orchestration."""

from http import HTTPStatus

from weather_ingest.errors import WeatherSourceError

EXPECTED_MINSK = "bronze/weather/realtime/minsk/2026/11/20/14/20261120T141000Z.json"


def test_all_locations_stored(make_service, storage):
    """Every location is fetched and written to its hour partition."""
    report = make_service().run(["minsk", "warsaw"])
    assert report.http_status is HTTPStatus.OK
    assert EXPECTED_MINSK in storage.objects
    assert len(report.written) == 2


def test_source_error_does_not_stop_other_locations(make_service, fake_source):
    """One failing location yields partial success."""
    source = fake_source({"bad": WeatherSourceError("tomorrow.io HTTP 429 Too Many Requests")})
    report = make_service(source).run(["bad", "minsk"])
    assert report.http_status is HTTPStatus.MULTI_STATUS
    assert report.errors == {"bad": "tomorrow.io HTTP 429 Too Many Requests"}


def test_existing_object_is_reported_not_overwritten(make_service, storage):
    """A second run at the same timestamp must not overwrite bronze."""
    service = make_service()
    service.run(["minsk"])
    report = service.run(["minsk"])
    assert report.http_status is HTTPStatus.BAD_GATEWAY
    assert "already exists" in report.errors["minsk"]


def test_unexpected_exception_is_captured(make_service, fake_source):
    """Unknown errors are reported as 'unexpected error' instead of crashing."""
    report = make_service(fake_source({"minsk": KeyError("x")})).run(["minsk"])
    assert report.errors == {"minsk": "unexpected error"}
