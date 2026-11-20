"""Serialization of silver records to CSV."""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable

from include.weather.schema import SILVER_COLUMNS, SilverRecord


class SilverCsvWriter:
    """Writes records as CSV with a header that matches the BigQuery schema."""

    def to_csv(self, records: Iterable[SilverRecord]) -> str:
        """Render records as CSV text.

        Input:   records - silver records.
        Purpose: produce the silver file; column order == BigQuery column order.
        Output:  CSV string with a header row ("\\n" line endings, minimal quoting).
        """
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=SILVER_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(record.to_row() for record in records)
        return buffer.getvalue()
