"""Weather providers: the WeatherSource interface and its tomorrow.io implementation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from http import HTTPStatus

import requests

from weather_ingest.errors import WeatherSourceError


class WeatherSource(ABC):
    """Interface of a provider that returns the current weather as raw text."""

    @abstractmethod
    def fetch_realtime(self, location: str) -> str:
        """Fetch the current weather for one location.

        Get the raw payload to be stored in bronze without changes.

        Args:
            location: Provider-specific location (city name or "lat,lon").

        Returns:
            str: Raw response body (JSON string).

        Raises:
            WeatherSourceError: If the provider cannot return a valid response.
        """


class TomorrowIoClient(WeatherSource):
    """WeatherSource implementation for the tomorrow.io realtime endpoint."""

    API_URL = "https://api.tomorrow.io/v4/weather/realtime"

    def __init__(
        self,
        api_key: str,
        units: str = "metric",
        timeout_s: float = 20.0,
        session: requests.Session | None = None,
    ) -> None:
        """Create a tomorrow.io client.

        Keep HTTP details (URL, params, headers) in one place.

        Args:
            api_key: The tomorrow.io API key.
            units: "metric" or "imperial".
            timeout_s: HTTP timeout.
            session: Optional requests.Session (reused connections, easy to mock in tests).
        """
        self._api_key = api_key
        self._units = units
        self._timeout_s = timeout_s
        self._session = session or requests.Session()

    def fetch_realtime(self, location: str) -> str:
        """Call GET /v4/weather/realtime for one location.

        Download the current weather exactly as the API returns it.

        Args:
            location: City name or "lat,lon".

        Returns:
            str: The raw JSON response body.

        Raises:
            WeatherSourceError: On HTTP or network errors (the URL is never logged: it contains the
                key).
        """
        try:
            response = self._session.get(
                self.API_URL,
                params={"location": location, "units": self._units, "apikey": self._api_key},
                headers={"accept": "application/json"},
                timeout=self._timeout_s,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise WeatherSourceError(f"tomorrow.io {self._describe_status(exc.response)}") from None
        except requests.RequestException as exc:
            raise WeatherSourceError(f"request failed: {type(exc).__name__}") from None
        return response.text

    @staticmethod
    def _describe_status(response: requests.Response | None) -> str:
        """Turn an HTTP error response into a readable status description.

        Report the real status from the API instead of a fixed text.

        Args:
            response: Failed requests.Response (may be None).

        Returns:
            str: A description such as "HTTP 429 Too Many Requests", "HTTP 599" for a non-standard
                code, or "HTTP status unknown" if there is no response.
        """
        if response is None:
            return "HTTP status unknown"
        try:
            status = HTTPStatus(response.status_code)
        except ValueError:
            return f"HTTP {response.status_code}"
        return f"HTTP {status.value} {status.phrase}"
