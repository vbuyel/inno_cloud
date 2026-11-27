"""Validation rules for raw tomorrow.io payloads (bronze -> silver)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from include.weather.schema import WEATHER_METRICS


def is_number(value: Any) -> bool:
    """Check that a JSON value is a real finite number.

    Reject strings, booleans (bool is an int subclass), NaN and inf.

    Args:
        value: Any parsed JSON value.

    Returns:
        bool: True if value is int/float, not bool, and finite.
    """
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def parse_event_time(value: Any) -> datetime | None:
    """Parse data.time into an aware datetime.

    Event_time is REQUIRED in gold, so it must be a valid timestamp.

    Args:
        value: E.g. "2026-11-20T14:10:00Z".

    Returns:
        datetime | None: Timezone-aware datetime, or None if missing/invalid/naive.
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

        Keep thresholds configurable and out of the checking code.

        Args:
            rules: ValidationRules; defaults to the project rules.
        """
        self._rules = rules or ValidationRules()

    def validate(self, payload: Any) -> list[str]:
        """Run all checks against one payload.

        Decide whether the payload can become a silver record.

        Args:
            payload: Parsed JSON document from a bronze file.

        Returns:
            list[str]: Human-readable errors; an empty list means the payload is valid.
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

        Data, location and data.values must exist and be objects.

        Args:
            payload: Parsed JSON document.

        Returns:
            list[str]: Structural errors.
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

        Event_time and location_name cannot be NULL in BigQuery.

        Args:
            payload: Structurally valid payload.

        Returns:
            list[str]: Errors for missing or invalid mandatory fields.
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

        Enforce "numeric if present" and "within range if present".

        Args:
            values: Data.values object.

        Returns:
            list[str]: Range/type errors, e.g. "humidity=120 is outside [0, 100]".
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
