"""Payload/file factories shared by silver-processing tests."""

from __future__ import annotations

import json

import pytest
from include.transform import BronzeFile

BRONZE_HOUR = "bronze/weather/realtime/minsk/2026/11/20/13/"
DEFAULT_VALUES = {"temperature": 1.5, "humidity": 80, "windSpeed": 3.2, "cloudCover": 100, "precipitationProbability": 0}


@pytest.fixture
def make_payload():
    """Input: none. Purpose: build tomorrow.io-like payloads. Output: factory(time, name, drop, **values)."""

    def factory(time="2026-11-20T13:00:00Z", name="Minsk, Belarus", drop=(), **values) -> dict:
        merged = {k: v for k, v in {**DEFAULT_VALUES, **values}.items() if k not in drop}
        location = {"lat": 53.9, "lon": 27.56, "name": name, "type": "administrative"}
        return {"data": {"time": time, "values": merged}, "location": location}

    return factory


@pytest.fixture
def make_file():
    """Input: none. Purpose: wrap a payload as a bronze file. Output: factory(file_name, payload, prefix)."""

    def factory(file_name: str, payload, prefix: str = BRONZE_HOUR) -> BronzeFile:
        content = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        return BronzeFile("bkt", prefix + file_name, content)

    return factory
