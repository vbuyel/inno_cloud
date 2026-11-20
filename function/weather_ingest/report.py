"""Result of one ingestion run and its HTTP representation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import ClassVar


@dataclass
class IngestionReport:
    """Result of one ingestion run across all locations."""

    # Outcome -> HTTP status mapping (standard library constants, no magic numbers).
    STATUS_ALL_STORED: ClassVar[HTTPStatus] = HTTPStatus.OK
    STATUS_PARTIAL: ClassVar[HTTPStatus] = HTTPStatus.MULTI_STATUS
    STATUS_NOTHING_STORED: ClassVar[HTTPStatus] = HTTPStatus.BAD_GATEWAY

    written: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)

    def add_success(self, uri: str) -> None:
        """Register a stored object.

        Collect successful writes for the response and logs.

        Args:
            uri: "gs://..." URI of the stored payload.
        """
        self.written.append(uri)

    def add_error(self, location: str, reason: str) -> None:
        """Register a failed location.

        Report per-location failures without stopping the run.

        Args:
            location: Location that failed.
            reason: Short description.
        """
        self.errors[location] = reason

    @property
    def http_status(self) -> HTTPStatus:
        """Map the run outcome to an HTTP status.

        Let Cloud Scheduler distinguish success from failure. Uses written/errors.

        Returns:
            HTTPStatus: STATUS_ALL_STORED (OK) - all locations stored; STATUS_PARTIAL (MULTI_STATUS)
                - partial success; STATUS_NOTHING_STORED (BAD_GATEWAY) - nothing stored.
        """
        if not self.errors:
            return self.STATUS_ALL_STORED
        return self.STATUS_PARTIAL if self.written else self.STATUS_NOTHING_STORED

    def to_json(self) -> str:
        """Serialize the report for the HTTP response.

        Return a machine-readable summary to the caller.

        Returns:
            str: JSON string {"written": [...], "errors": {...}}.
        """
        return json.dumps({"written": self.written, "errors": self.errors})
