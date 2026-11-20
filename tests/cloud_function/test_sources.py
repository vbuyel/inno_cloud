"""Tests for TomorrowIoClient (HTTP is faked, no network)."""

from __future__ import annotations

import pytest
import requests
from weather_ingest.errors import WeatherSourceError
from weather_ingest.sources import TomorrowIoClient


def make_response(status: int, body: bytes = b"{}") -> requests.Response:
    """Input: status and body. Purpose: real Response object without network. Output: Response."""
    response = requests.Response()
    response.status_code, response._content = status, body
    response.url = "https://api.tomorrow.io/v4/weather/realtime?apikey=SECRET"
    return response


class FakeSession:
    """Records the request and returns a prepared response or raises."""

    def __init__(self, result) -> None:
        """Input: Response or exception. Output: None."""
        self.result, self.calls = result, []

    def get(self, url, **kwargs):
        """Input: requests.get arguments. Output: prepared response or raises."""
        self.calls.append((url, kwargs))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def test_returns_raw_body_and_sends_expected_params():
    """Body is returned unchanged; location, units and key are sent."""
    session = FakeSession(make_response(200, b'{"data": 1}'))
    body = TomorrowIoClient("SECRET", units="metric", session=session).fetch_realtime("minsk")
    assert body == '{"data": 1}'
    assert session.calls[0][1]["params"] == {"location": "minsk", "units": "metric", "apikey": "SECRET"}


def test_http_error_is_wrapped_without_leaking_api_key():
    """HTTP errors carry the real status; the key never appears in the message."""
    client = TomorrowIoClient("SECRET", session=FakeSession(make_response(429)))
    with pytest.raises(WeatherSourceError, match="HTTP 429 Too Many Requests") as exc_info:
        client.fetch_realtime("minsk")
    assert "SECRET" not in str(exc_info.value)


def test_network_error_is_wrapped():
    """Connection problems become WeatherSourceError with the error type."""
    client = TomorrowIoClient("SECRET", session=FakeSession(requests.ConnectionError("down")))
    with pytest.raises(WeatherSourceError, match="request failed: ConnectionError"):
        client.fetch_realtime("minsk")


@pytest.mark.parametrize(("response", "text"), [(None, "HTTP status unknown"), (make_response(599), "HTTP 599")])
def test_describe_status_handles_missing_and_nonstandard_codes(response, text):
    """No response or a non-standard code still produce a readable message."""
    assert TomorrowIoClient._describe_status(response) == text
