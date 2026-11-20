"""Bronze -> silver transformation: parse, validate, flatten, deduplicate, partition.

Pure Python: receives file contents and returns CSV texts, so it can be tested
without Airflow or GCP. I/O is done by the DAG.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime

from include.weather.csv_writer import SilverCsvWriter
from include.weather.deduplicator import RecordDeduplicator
from include.weather.flattener import PayloadFlattener
from include.weather.partitioning import BronzeLayout, Partition, SilverLayout, floor_to_hour, location_slug
from include.weather.schema import SilverRecord
from include.weather.validator import PayloadValidator

logger = logging.getLogger(__name__)


class NoValidRecordsError(Exception):
    """Raised when nothing is left after validation and deduplication."""


@dataclass(frozen=True)
class BronzeFile:
    """One raw file downloaded from bronze."""

    bucket: str
    object_name: str
    content: bytes

    @property
    def uri(self) -> str:
        """Return the file URI (stored in the source_object column).

        Input:   none.
        Purpose: keep lineage from every gold row back to its raw file.
        Output:  "gs://<bucket>/<object_name>".
        """
        return f"gs://{self.bucket}/{self.object_name}"


@dataclass(frozen=True)
class RejectedFile:
    """A bronze file that failed validation, with the reasons."""

    source_object: str
    reasons: list[str]


@dataclass
class TransformResult:
    """Output of one transformation run."""

    outputs: dict[str, str] = field(default_factory=dict)  # silver object name -> CSV text
    valid_records: int = 0
    rejected: list[RejectedFile] = field(default_factory=list)
    duplicates: int = 0

    def summary(self) -> dict[str, int]:
        """Return counters for logs and XCom.

        Input:   none.
        Purpose: make the run outcome visible in the Airflow UI.
        Output:  dict with valid/rejected/duplicates/files counters.
        """
        return {
            "valid_records": self.valid_records,
            "rejected_records": len(self.rejected),
            "duplicates_removed": self.duplicates,
            "silver_files": len(self.outputs),
        }


class SilverTransformer:
    """Orchestrates the bronze -> silver steps for a batch of files."""

    def __init__(
        self,
        bronze_layout: BronzeLayout,
        silver_layout: SilverLayout,
        validator: PayloadValidator | None = None,
        flattener: PayloadFlattener | None = None,
        deduplicator: RecordDeduplicator | None = None,
        csv_writer: SilverCsvWriter | None = None,
    ) -> None:
        """Wire the transformer with its collaborators.

        Input:   layouts for both layers; optional validator/flattener/
                 deduplicator/csv_writer (defaults are the project ones).
        Purpose: dependency injection, every step can be replaced or tested alone.
        Output:  None.
        """
        self._bronze = bronze_layout
        self._silver = silver_layout
        self._validator = validator or PayloadValidator()
        self._flattener = flattener or PayloadFlattener()
        self._deduplicator = deduplicator or RecordDeduplicator()
        self._csv = csv_writer or SilverCsvWriter()

    def transform(self, files: Iterable[BronzeFile], ingested_at: datetime) -> TransformResult:
        """Turn raw files into silver CSV texts grouped by location-hour.

        Input:   files - downloaded bronze files; ingested_at - processing time
                 (written to ingested_at_utc and used in CSV names).
        Purpose: apply all validation rules, drop invalid records with warnings,
                 deduplicate and split output into partitions.
        Output:  TransformResult; raises NoValidRecordsError if nothing is valid.
        """
        result = TransformResult()
        records, partitions = self._collect_records(files, ingested_at, result)
        unique = self._deduplicate(records, result)
        if not unique:
            raise NoValidRecordsError(f"No valid records left: {result.summary()}")

        grouped: dict[Partition, list[SilverRecord]] = defaultdict(list)
        for record in unique:
            grouped[partitions[record.source_object]].append(record)
        for partition, partition_records in sorted(grouped.items()):
            result.outputs[self._silver.object_name(partition, ingested_at)] = self._csv.to_csv(partition_records)
        result.valid_records = len(unique)
        return result

    def _collect_records(
        self, files: Iterable[BronzeFile], ingested_at: datetime, result: TransformResult
    ) -> tuple[list[SilverRecord], dict[str, Partition]]:
        """Convert every file into a record, skipping rejected ones.

        Input:   files - bronze files; ingested_at - processing time;
                 result - collects rejected files.
        Purpose: keep valid records together with their target partition.
        Output:  (records, {source_object: Partition}).
        """
        records: list[SilverRecord] = []
        partitions: dict[str, Partition] = {}
        for file in files:
            record = self._to_record(file, ingested_at, result)
            if record is not None:
                records.append(record)
                partitions[record.source_object] = self._partition_for(file, record)
        return records, partitions

    def _deduplicate(self, records: list[SilverRecord], result: TransformResult) -> list[SilverRecord]:
        """Drop duplicates and log each of them.

        Input:   records - valid records; result - receives the duplicate count.
        Purpose: rule "deduplication by (location_name, time)".
        Output:  unique records.
        """
        dedup = self._deduplicator.deduplicate(records)
        result.duplicates = len(dedup.duplicates)
        for duplicate in dedup.duplicates:
            logger.warning("Duplicate dropped: %s %s from %s", *duplicate.dedup_key, duplicate.source_object)
        return dedup.unique

    def _to_record(self, file: BronzeFile, ingested_at: datetime, result: TransformResult) -> SilverRecord | None:
        """Parse, validate and flatten one file.

        Input:   file - bronze file; ingested_at - processing time;
                 result - collects rejected files.
        Purpose: isolate per-file failures (bad JSON or failed rules).
        Output:  SilverRecord, or None if the file was rejected (logged as warning).
        """
        try:
            payload = json.loads(file.content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            reasons = [f"invalid JSON: {exc}"]
        else:
            reasons = self._validator.validate(payload)
            if not reasons:
                return self._flattener.flatten(payload, file.uri, ingested_at)

        result.rejected.append(RejectedFile(file.uri, reasons))
        logger.warning("Rejected %s: %s", file.uri, "; ".join(reasons))
        return None

    def _partition_for(self, file: BronzeFile, record: SilverRecord) -> Partition:
        """Choose the silver partition for a record.

        Input:   file - its bronze file; record - flattened record.
        Purpose: mirror the bronze partition; for files outside the convention
                 (custom source_path) fall back to location_name + event hour.
        Output:  Partition.
        """
        return self._bronze.partition_of(file.object_name) or Partition(
            location=location_slug(record.location_name), hour=floor_to_hour(record.event_time)
        )
