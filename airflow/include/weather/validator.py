"""Validation rules for raw tomorrow.io payloads (bronze -> silver)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from include.weather.schema import WEATHER_METRICS


def is_number(value: Any) -> bool:
    """Check that a JSON value is a real finite number.

    Input:   value - any parsed JSON value.
    Purpose: reject strings, booleans (bool is an int subclass), NaN and inf.
    Output:  True if value is int/float, not bool, and finite.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def parse_event_time(value: Any) -> datetime | None:
    """Parse data.time into an aware datetime.

    Input:   value - e.g. "2026-11-20T14:10:00Z".
    Purpose: event_time is REQUIRED in gold, so it must be a valid timestamp.
    Output:  timezone-aware datetime, or None if missing/invalid/naive.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


@dataclass(frozen=True)
class ValidationRules:
    """Configurable rules: which metrics must be numeric and their allowed ranges."""

    numeric_fields: tuple[str, ...] = tuple(WEATHER_METRICS)
    ranges: dict[str, tuple[float, float]] = field(
        default_factory=lambda: {
            "humidity": (0, 100),
            "cloudCover": (0, 100),
            "precipitationProbability": (0, 100),
            "temperature": (-90, 70),
            "windSpeed": (0, 150),
        }
    )


class PayloadValidator:
    """Checks one raw payload and explains every violation."""

    def __init__(self, rules: ValidationRules | None = None) -> None:
        """Create a validator.

        Input:   rules - ValidationRules; defaults to the project rules.
        Purpose: keep thresholds configurable and out of the checking code.
        Output:  None.
        """
        self._rules = rules or ValidationRules()

    def validate(self, payload: Any) -> list[str]:
        """Run all checks against one payload.

        Input:   payload - parsed JSON document from a bronze file.
        Purpose: decide whether the payload can become a silver record.
        Output:  list of human-readable errors; empty list means valid.
        """
        errors = self._check_structure(payload)
        if errors:
            return errors  # deeper checks need a valid structure
        errors += self._check_mandatory(payload)
        errors += self._check_metrics(payload["data"]["values"])
        return errors

    @staticmethod
    def _check_structure(payload: Any) -> list[str]:
        """Check the JSON skeleton.

        Input:   payload - parsed JSON document.
        Purpose: data, location and data.values must exist and be objects.
        Output:  list of structural errors.
        """
        if not isinstance(payload, dict):
            return ["payload is not a JSON object"]
        errors = []
        if not isinstance(payload.get("data"), dict):
            errors.append("'data' is missing or not an object")
        elif not isinstance(payload["data"].get("values"), dict):
            errors.append("'data.values' is missing or not an object")
        if not isinstance(payload.get("location"), dict):
            errors.append("'location' is missing or not an object")
        return errors

    @staticmethod
    def _check_mandatory(payload: dict) -> list[str]:
        """Check fields that are REQUIRED in the gold table.

        Input:   payload - structurally valid payload.
        Purpose: event_time and location_name cannot be NULL in BigQuery.
        Output:  list of errors for missing/invalid mandatory fields.
        """
        errors = []
        if parse_event_time(payload["data"].get("time")) is None:
            errors.append("'data.time' is missing or not an ISO timestamp with timezone")
        name = payload["location"].get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append("'location.name' is missing or empty")
        return errors

    def _check_metrics(self, values: dict) -> list[str]:
        """Check metric types and ranges (only for metrics that are present).

        Input:   values - data.values object.
        Purpose: enforce "numeric if present" and "within range if present".
        Output:  list of errors, e.g. "humidity=120 is outside [0, 100]".
        """
        errors = []
        for name in self._rules.numeric_fields:
            if name not in values or values[name] is None:
                continue
            value = values[name]
            if not is_number(value):
                errors.append(f"{name}={value!r} is not numeric")
                continue
            if name in self._rules.ranges:
                low, high = self._rules.ranges[name]
                if not low <= value <= high:
                    errors.append(f"{name}={value} is outside [{low}, {high}]")
        return errors
