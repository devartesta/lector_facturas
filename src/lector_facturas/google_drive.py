"""Google Drive client utilities for centralised invoice storage."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import json


@dataclass(frozen=True)
class GoogleOAuthConfig:
    client_id: str
    client_secret: str
    refresh_token: str


@dataclass(frozen=True)
class DriveConfig:
    oauth: GoogleOAuthConfig
    shared_drive_id: str = ""
    root_folder_id: str = ""


def refresh_google_access_token(config: GoogleOAuthConfig) -> str:
    payload = urlencode(
        {
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "refresh_token": config.refresh_token,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    request = Request(
        "https://oauth2.googleapis.com/token",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data["access_token"]
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Token refresh failed: {exc.code} {detail}") from exc


class GoogleDriveClient:
    def __init__(self, config: DriveConfig) -> None:
        self.config = config

    def about(self) -> dict[str, object]:
        return self._request_json(
            "GET",
            "https://www.googleapis.com/drive/v3/about?fields=user(displayName,emailAddress),storageQuota",
        )

    def get_file(self, file_id: str) -> dict[str, object]:
        return self._request_json(
            "GET",
            f"https://www.googleapis.com/drive/v3/files/{file_id}?fields=id,name,mimeType,parents,webViewLink,webContentLink",
        )

    def list_folders(self, *, parent_id: str, name: str | None = None) -> list[dict[str, object]]:
        clauses = [
            "mimeType = 'application/vnd.google-apps.folder'",
            "trashed = false",
            f"'{parent_id}' in parents",
        ]
        if name:
            safe_name = name.replace("'", "\\'")
            clauses.append(f"name = '{safe_name}'")
        query = " and ".join(clauses)
        url = (
            "https://www.googleapis.com/drive/v3/files"
            f"?q={quote(query, safe='')}"
            "&fields=files(id,name,parents,webViewLink)"
            "&pageSize=1000"
        )
        response = self._request_json("GET", url)
        return list(response.get("files", []))

    def create_folder(self, *, name: str, parent_id: str) -> dict[str, object]:
        payload = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        return self._request_json(
            "POST",
            "https://www.googleapis.com/drive/v3/files?fields=id,name,parents,webViewLink&supportsAllDrives=true",
            payload=payload,
        )

    def ensure_folder(self, *, name: str, parent_id: str) -> dict[str, object]:
        existing = self.list_folders(parent_id=parent_id, name=name)
        if existing:
            return existing[0]
        return self.create_folder(name=name, parent_id=parent_id)

    def list_files(self, *, parent_id: str, name: str | None = None) -> list[dict[str, object]]:
        clauses = [
            "trashed = false",
            f"'{parent_id}' in parents",
        ]
        if name:
            safe_name = name.replace("'", "\\'")
            clauses.append(f"name = '{safe_name}'")
        query = " and ".join(clauses)
        url = (
            "https://www.googleapis.com/drive/v3/files"
            f"?q={quote(query, safe='')}"
            "&fields=files(id,name,parents,webViewLink,webContentLink,mimeType)"
            "&pageSize=1000"
            "&supportsAllDrives=true"
            "&includeItemsFromAllDrives=true"
        )
        response = self._request_json("GET", url)
        return list(response.get("files", []))

    def upload_file(self, *, name: str, parent_id: str, content: bytes, mime_type: str = "application/pdf") -> dict[str, object]:
        boundary = "lector_facturas_boundary"
        metadata = json.dumps({"name": name, "parents": [parent_id]}).encode("utf-8")
        body = (
            b"--" + boundary.encode("ascii") + b"\r\n"
            + b"Content-Type: application/json; charset=UTF-8\r\n\r\n"
            + metadata + b"\r\n"
            + b"--" + boundary.encode("ascii") + b"\r\n"
            + f"Content-Type: {mime_type}\r\n\r\n".encode("ascii")
            + content + b"\r\n"
            + b"--" + boundary.encode("ascii") + b"--\r\n"
        )
        return self._request_json(
            "POST",
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,parents,webViewLink,webContentLink&supportsAllDrives=true",
            payload_bytes=body,
            content_type=f"multipart/related; boundary={boundary}",
        )

    def download_file_bytes(self, *, file_id: str) -> bytes:
        access_token = refresh_google_access_token(self.config.oauth)
        request = Request(
            f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&supportsAllDrives=true",
            method="GET",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        try:
            with urlopen(request, timeout=60) as response:
                return response.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Google Drive download failed: {exc.code} {detail}") from exc

    def ensure_file(
        self,
        *,
        name: str,
        parent_id: str,
        content: bytes,
        mime_type: str = "application/pdf",
    ) -> dict[str, object]:
        existing = self.list_files(parent_id=parent_id, name=name)
        if existing:
            return existing[0]
        return self.upload_file(name=name, parent_id=parent_id, content=content, mime_type=mime_type)

    def update_file_name(self, *, file_id: str, name: str) -> dict[str, object]:
        return self._request_json(
            "PATCH",
            f"https://www.googleapis.com/drive/v3/files/{file_id}?fields=id,name,parents,webViewLink,webContentLink&supportsAllDrives=true",
            payload={"name": name},
        )

    def move_file(self, *, file_id: str, new_parent_id: str) -> dict[str, object]:
        current = self.get_file(file_id)
        existing_parents = ",".join(str(parent_id) for parent_id in current.get("parents", []))
        return self._request_json(
            "PATCH",
            f"https://www.googleapis.com/drive/v3/files/{file_id}?addParents={quote(new_parent_id, safe='')}&removeParents={quote(existing_parents, safe=',')}&fields=id,name,parents,webViewLink,webContentLink&supportsAllDrives=true",
            payload={},
        )

    def trash_file(self, *, file_id: str) -> dict[str, object]:
        return self._request_json(
            "PATCH",
            f"https://www.googleapis.com/drive/v3/files/{file_id}?fields=id,name,trashed&supportsAllDrives=true",
            payload={"trashed": True},
        )

    def ocr_pdf_to_text(self, *, name: str, content: bytes) -> str:
        boundary = f"ocr_boundary_{name.replace(' ', '_')}"
        metadata = json.dumps(
            {
                "name": f"{name} OCR",
                "mimeType": "application/vnd.google-apps.document",
            }
        ).encode("utf-8")
        body = (
            b"--" + boundary.encode("ascii") + b"\r\n"
            + b"Content-Type: application/json; charset=UTF-8\r\n\r\n"
            + metadata + b"\r\n"
            + b"--" + boundary.encode("ascii") + b"\r\n"
            + b"Content-Type: application/pdf\r\n\r\n"
            + content + b"\r\n"
            + b"--" + boundary.encode("ascii") + b"--\r\n"
        )
        created = self._request_json(
            "POST",
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,mimeType&supportsAllDrives=true",
            payload_bytes=body,
            content_type=f"multipart/related; boundary={boundary}",
        )
        document_id = str(created["id"])
        try:
            access_token = refresh_google_access_token(self.config.oauth)
            request = Request(
                f"https://www.googleapis.com/drive/v3/files/{document_id}/export?mimeType=text/plain",
                method="GET",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            with urlopen(request, timeout=60) as response:
                return response.read().decode("utf-8", errors="replace")
        finally:
            self.trash_file(file_id=document_id)

    def _request_json(
        self,
        method: str,
        url: str,
        payload: dict[str, object] | None = None,
        *,
        payload_bytes: bytes | None = None,
        content_type: str = "application/json",
    ) -> dict[str, object]:
        access_token = refresh_google_access_token(self.config.oauth)
        data = payload_bytes if payload_bytes is not None else (None if payload is None else json.dumps(payload).encode("utf-8"))
        request = Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": content_type,
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Google Drive API request failed: {exc.code} {detail}") from exc
