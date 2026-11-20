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

        Store data immutably (never overwrite existing objects).

        Args:
            object_name: Path inside the storage.
            payload: Raw text.

        Returns:
            str: Full URI of the stored object.

        Raises:
            ObjectAlreadyExistsError: If an object with this name already exists.
        """


class GcsRawStorage(RawStorage):
    """RawStorage implementation backed by a Google Cloud Storage bucket."""

    def __init__(self, bucket_name: str, client: storage.Client | None = None) -> None:
        """Bind the storage to a bucket.

        Hide GCS SDK details behind the RawStorage interface.

        Args:
            bucket_name: Target GCS bucket.
            client: Optional storage.Client (created lazily if not given).
        """
        self._bucket_name = bucket_name
        self._client = client

    @property
    def _bucket(self) -> storage.Bucket:
        """Return the bucket handle, creating the GCS client on first use.

        Avoid creating a client until it is actually needed.

        Returns:
            storage.Bucket: The google.cloud.storage.Bucket handle.
        """
        if self._client is None:
            self._client = storage.Client()
        return self._client.bucket(self._bucket_name)

    def write(self, object_name: str, payload: str) -> str:
        """Upload a JSON payload to GCS without overwriting.

        Store the raw API response in the bronze layer.

        Args:
            object_name: Object path in the bucket.
            payload: JSON text.

        Returns:
            str: "gs://<bucket>/<object_name>".

        Raises:
            ObjectAlreadyExistsError: If the object already exists (if_generation_match=0).
        """
        blob = self._bucket.blob(object_name)
        try:
            blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
        except PreconditionFailed:
            raise ObjectAlreadyExistsError(f"object already exists: {object_name}") from None
        return f"gs://{self._bucket_name}/{object_name}"
