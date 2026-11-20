"""Tests for GcsRawStorage (GCS client is faked)."""

from __future__ import annotations

import pytest
from google.api_core.exceptions import PreconditionFailed
from weather_ingest.errors import ObjectAlreadyExistsError
from weather_ingest.storage import GcsRawStorage


class FakeBlob:
    """Records upload arguments; can simulate an existing object."""

    def __init__(self, exists: bool) -> None:
        """Input: exists - simulate precondition failure. Output: None."""
        self.exists, self.uploads = exists, []

    def upload_from_string(self, data, **kwargs):
        """Input: data and upload options. Output: None or raises PreconditionFailed."""
        if self.exists:
            raise PreconditionFailed("exists")
        self.uploads.append((data, kwargs))


class FakeClient:
    """Minimal stand-in for storage.Client().bucket(...).blob(...)."""

    def __init__(self, blob: FakeBlob) -> None:
        """Input: blob returned for any name. Output: None."""
        self.blob_obj, self.bucket_name = blob, None

    def bucket(self, name):
        """Input: bucket name. Output: self (acts as bucket too)."""
        self.bucket_name = name
        return self

    def blob(self, name):
        """Input: object name. Output: the prepared FakeBlob."""
        return self.blob_obj


def test_write_uploads_json_without_overwrite():
    """Upload uses if_generation_match=0 and returns the gs:// URI."""
    blob = FakeBlob(exists=False)
    uri = GcsRawStorage("bkt", client=FakeClient(blob)).write("a/b.json", "{}")
    assert uri == "gs://bkt/a/b.json"
    assert blob.uploads == [("{}", {"content_type": "application/json", "if_generation_match": 0})]


def test_existing_object_raises_domain_error():
    """GCS precondition failure becomes ObjectAlreadyExistsError."""
    storage = GcsRawStorage("bkt", client=FakeClient(FakeBlob(exists=True)))
    with pytest.raises(ObjectAlreadyExistsError, match="a/b.json"):
        storage.write("a/b.json", "{}")
