"""Tests for the HTTP entry point (main.ingest) and the factory."""

import json

import main
import weather_ingest.factory as factory
from weather_ingest.sources import TomorrowIoClient
from weather_ingest.storage import GcsRawStorage


def test_ingest_returns_json_status_and_headers(monkeypatch, make_service):
    """Entry point loads config, runs the service and returns an HTTP tuple."""
    monkeypatch.setenv("WEATHER_API_KEY", "k")
    monkeypatch.setenv("DATA_BUCKET", "b")
    monkeypatch.setenv("LOCATIONS", "minsk")
    monkeypatch.setattr(main, "build_service", lambda config: make_service())
    body, status, headers = main.ingest(request=None)
    assert type(status) is int and status == 200
    assert json.loads(body)["errors"] == {}
    assert headers == {"Content-Type": "application/json"}


def test_build_service_wires_production_implementations(monkeypatch):
    """Factory picks tomorrow.io + GCS (GCS client creation is stubbed)."""
    monkeypatch.setattr(factory, "shared_gcs_client", lambda: object())
    config = main.IngestConfig(api_key="k", bucket_name="b", locations=("minsk",))
    service = factory.build_service(config)
    assert isinstance(service._source, TomorrowIoClient)
    assert isinstance(service._storage, GcsRawStorage)
