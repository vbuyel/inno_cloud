"""Payload/file factories shared by silver-processing tests."""

from __future__ import annotations

import json

import pytest
from include.transform import BronzeFile

BRONZE_HOUR = "bronze/weather/realtime/minsk/2026/11/20/13/"
DEFAULT_VALUES = {"temperature": 1.5, "humidity": 80, "windSpeed": 3.2, "cloudCover": 100, "precipitationProbability": 0}


def build_payload(time="2026-11-20T13:00:00Z", name="Minsk, Belarus", drop=(), **values) -> dict:
    """Build one payload.

    Args:
        time: Value of data.time.
        name: Value of location.name.
        drop: Metric names to remove.
        **values: Metric overrides.

    Returns:
        dict: A tomorrow.io-like payload dict.
    """
    merged = {k: v for k, v in {**DEFAULT_VALUES, **values}.items() if k not in drop}
    location = {"lat": 53.9, "lon": 27.56, "name": name, "type": "administrative"}
    return {"data": {"time": time, "values": merged}, "location": location}


@pytest.fixture
def make_payload():
    """Provide a factory for tomorrow.io-like payloads.

    Returns:
        Callable[..., dict]: ``build_payload(time, name, drop, **values)`` that returns a payload
            dict; ``drop`` removes metrics, ``values`` overrides them.
    """
    return build_payload


def build_file(file_name: str, payload, prefix: str = BRONZE_HOUR) -> BronzeFile:
    """Wrap one payload.

    Args:
        file_name: Object file name.
        payload: Dict (JSON-encoded) or raw bytes.
        prefix: Bronze partition prefix.

    Returns:
        BronzeFile: A BronzeFile in bucket "bkt".
    """
    content = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return BronzeFile("bkt", prefix + file_name, content)


@pytest.fixture
def make_file():
    """Provide a factory that wraps a payload as a bronze file.

    Returns:
        Callable[..., BronzeFile]: ``build_file(file_name, payload, prefix)`` that returns a
            BronzeFile; bytes payloads are used as-is, dicts are JSON-encoded.
    """
    return build_file
