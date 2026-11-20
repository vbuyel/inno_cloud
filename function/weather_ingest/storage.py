"""Raw payload storage: the RawStorage interface and its GCS implementation."""

from __future__ import annotations

from abc import ABC, abstractmethod

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

from weather_ingest.errors import ObjectAlreadyExistsError


class RawStorage(ABC):
    """Interface of a storage for raw (bronze) payloads."""

    @abstractmethod
    def write(self, object_name: str, payload: str) -> str:
        """Persist one raw payload.

        Input:   object_name - path inside the storage; payload - raw text.
        Purpose: store data immutably (never overwrite existing objects).
        Output:  full URI of the stored object; raises ObjectAlreadyExistsError.
        """


class GcsRawStorage(RawStorage):
    """RawStorage implementation backed by a Google Cloud Storage bucket."""

    def __init__(self, bucket_name: str, client: storage.Client | None = None) -> None:
        """Bind the storage to a bucket.

        Input:   bucket_name - target GCS bucket; client - optional
                 storage.Client (created lazily if not given).
        Purpose: hide GCS SDK details behind the RawStorage interface.
        Output:  None.
        """
        self._bucket_name = bucket_name
        self._client = client

    @property
    def _bucket(self) -> storage.Bucket:
        """Return the bucket handle, creating the GCS client on first use.

        Input:   none.
        Purpose: avoid creating a client until it is actually needed.
        Output:  google.cloud.storage.Bucket.
        """
        if self._client is None:
            self._client = storage.Client()
        return self._client.bucket(self._bucket_name)

    def write(self, object_name: str, payload: str) -> str:
        """Upload a JSON payload to GCS without overwriting.

        Input:   object_name - object path in the bucket; payload - JSON text.
        Purpose: store the raw API response in the bronze layer.
        Output:  "gs://<bucket>/<object_name>"; raises ObjectAlreadyExistsError
                 if the object already exists (if_generation_match=0).
        """
        blob = self._bucket.blob(object_name)
        try:
            blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
        except PreconditionFailed:
            raise ObjectAlreadyExistsError(f"object already exists: {object_name}") from None
        return f"gs://{self._bucket_name}/{object_name}"
