"""Cloud Functions entry point for bronze weather ingestion.

Triggered over HTTP by Cloud Scheduler every 10 minutes. For every configured
location the raw tomorrow.io response is written unchanged to:

    bronze/weather/realtime/<location>/YYYY/MM/DD/HH/<timestamp>.json

Business logic lives in the weather_ingest package; this module only adapts
an HTTP request to it.
"""

from __future__ import annotations

import logging

import functions_framework

from weather_ingest import IngestConfig, build_service

logging.basicConfig(level=logging.INFO)


@functions_framework.http
def ingest(request):  # noqa: ARG001 - request body is not used
    """HTTP entry point called by Cloud Scheduler.

    Load config and run ingestion for all configured locations.

    Args:
        request: The incoming flask.Request (body is ignored).

    Returns:
        tuple[str, int, dict[str, str]]: JSON report, HTTP status code and response headers.
    """
    config = IngestConfig.from_env()
    report = build_service(config).run(config.locations)
    return report.to_json(), int(report.http_status), {"Content-Type": "application/json"}
