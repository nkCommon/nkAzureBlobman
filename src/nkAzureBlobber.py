"""
Azure Blob Storage client using MSAL (app registration) for auth.
No azure-identity dependency — only msal + azure-storage-blob.
"""

import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import msal
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AccessToken, TokenCredential
from dotenv import load_dotenv



# Scope for Azure Storage (not Graph — Graph cannot access blob storage)
STORAGE_SCOPE = "https://storage.azure.com/.default"


@dataclass
class BlobInfo:
    """Blob metadata from list_blobs(include_properties=True). Use .name, .size, .container_name, etc."""
    name: str
    container_name: str = ""
    size: Optional[int] = None
    creation_time: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    content_type: Optional[str] = None
    etag: Optional[str] = None


class MSALTokenCredential(TokenCredential):
    """Credential that gets tokens for Azure Storage using MSAL (client secret)."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str) -> None:
        self._app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
        )

    def get_token(self, *scopes: str, **kwargs) -> AccessToken:
        result = self._app.acquire_token_for_client(scopes=[STORAGE_SCOPE])
        if "access_token" not in result:
            raise RuntimeError(
                f"Failed to acquire token: {result.get('error_description', result)}"
            )
        # expires_in is seconds from now; Azure expects Unix timestamp
        expires_on = int(time.time()) + int(result.get("expires_in", 3600))
        return AccessToken(token=result["access_token"], expires_on=expires_on)


class AzureBlobContainerClient:
    """
    Read / write / update / delete files in an Azure Blob container.
    Auth: App registration (client id + secret) with MSAL; no Graph, no azure-identity.
    """

    def __init__(
        self,
        account_url: str,
        container_name: str = None,
        *,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ) -> None:
        """
        account_url: e.g. https://naestvedprivategpt.blob.core.windows.net
        container_name: e.g. kommuneguides
        Credentials from args or env: TenantId, clientId, clientSecret.
        """
        self.container_name = container_name
        tenant_id = tenant_id or os.environ.get("TenantId")
        client_id = client_id or os.environ.get("clientId")
        client_secret = client_secret or os.environ.get("clientSecret")
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError("TenantId, clientId, clientSecret required (args or env)")

        credential = MSALTokenCredential(tenant_id, client_id, client_secret)
        self._client = BlobServiceClient(
            account_url=account_url.rstrip("/"),
            credential=credential,
        )
        if self.container_name is not None:
            self._container = self._client.get_container_client(self.container_name)
        else:
            self._container = None

    def check_container_name(self, container_name: str) -> None:
        if container_name is not None:
            self.container_name = container_name
            self._container = self._client.get_container_client(container_name)
        if self._container is None:
            raise ValueError("Container name is required")


    def read(self, blob_name: str, container_name: str = None) -> bytes:
        """Download blob contents as bytes."""
        self.check_container_name(container_name)
        
        blob_client = self._container.get_blob_client(blob_name)
        return blob_client.download_blob().readall()

    def read_text(self, blob_name: str, encoding: str = "utf-8") -> str:
        """Download blob contents as string."""
        return self.read(blob_name).decode(encoding)

    def write(self, blob_name: str, data: bytes | str, overwrite: bool = True, container_name: str = None) -> None:
        """Upload bytes or string to a blob (create or overwrite)."""
        self.check_container_name(container_name)
        if isinstance(data, str):
            data = data.encode("utf-8")
        blob_client = self._container.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=overwrite)

    def upload_file(self, blob_name: str, local_path: str, overwrite: bool = True) -> None:
        """Upload a local file to a blob."""
        with open(local_path, "rb") as f:
            self.write(blob_name, f.read(), overwrite=overwrite)

    def update(self, blob_name: str, data: bytes | str) -> None:
        """Overwrite existing blob (same as write with overwrite=True)."""
        self.write(blob_name, data, overwrite=True)

    def delete(self, blob_name: str, container_name: str = None) -> None:
        self.check_container_name(container_name)
        """Delete a blob."""
        blob_client = self._container.get_blob_client(blob_name)
        blob_client.delete_blob()

    def list_blobs(
        self, name_starts_with: str = "", include_properties: bool = False, container_name: str = None
    ) -> list[str] | list[BlobInfo]:
        """
        List blobs in the container (optional prefix).

        If include_properties is False, returns a list of blob names (str).
        If include_properties is True, returns a list of BlobInfo with .name, .size,
        .creation_time, .last_modified, .content_type, .etag.
        """
        self.check_container_name(container_name)
        
        if not include_properties:
            return [b.name for b in self._container.list_blobs(name_starts_with=name_starts_with)]
        result: list[BlobInfo] = []
        for b in self._container.list_blobs(name_starts_with=name_starts_with):
            result.append(
                BlobInfo(
                    name=b.name,
                    container_name=self.container_name,
                    size=getattr(b, "size", None),
                    creation_time=getattr(b, "creation_time", None),
                    last_modified=getattr(b, "last_modified", None),
                    content_type=(
                        b.content_settings.content_type
                        if getattr(b, "content_settings", None)
                        else None
                    ),
                    etag=getattr(b, "etag", None),
                )
            )
        return result

    def list_container_names(self) -> list[str]:
        """List all container names in the storage account."""
        return [c["name"] for c in self._client.list_containers()]

    def exists(self, blob_name: str, container_name: str = None) -> bool:
        self.check_container_name(container_name)
        """Check if a blob exists."""
        return self._container.get_blob_client(blob_name).exists()
