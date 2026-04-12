#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from urllib.parse import urlparse, quote
from uuid import uuid4


class DocumentHandlerMixin:
    def _handle_lead_documents_get(self, lead_external_id: str) -> None:
        import local_db_api as api
        try:
            lead = api._load_supabase_lead_by_external_id(lead_external_id)
            if not lead:
                self._set_headers(404)
                self.wfile.write(json.dumps({"ok": False, "error": "lead_not_found"}).encode("utf-8"))
                return
            lead_id = int(lead.get("lead_id") or 0)
            rows = api._supabase_rest(
                f"lead_document?select=*&lead_id=eq.{lead_id}&archived_at=is.null&order=inserted_at.desc",
                method="GET",
            ) or []
            items = [api._lead_document_row_to_payload(row) for row in rows if isinstance(row, dict)]
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "leadId": lead_id, "items": items}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            message = str(exc)
            if "lead_document" in message:
                message = "Lead document hub is not configured yet. Run lead_document_hub.sql in Supabase."
            self.wfile.write(json.dumps({"ok": False, "error": message}).encode("utf-8"))


    def _handle_lead_documents_post(self, lead_external_id: str) -> None:
        import local_db_api as api
        try:
            lead = api._load_supabase_lead_by_external_id(lead_external_id)
            if not lead:
                self._set_headers(404)
                self.wfile.write(json.dumps({"ok": False, "error": "lead_not_found"}).encode("utf-8"))
                return
            data = self._read_json_body()
            file_name = api._trim(data.get("fileName"))
            source_url = api._trim(data.get("sourceUrl"))
            document_category = api._trim(data.get("documentCategory")) or "general"
            notes = api._trim(data.get("notes"))
            mime_type = api._trim(data.get("mimeType"))
            uploaded_by_email = api._trim(data.get("uploadedByEmail"))
            source_kind = "link" if source_url and not data.get("contentBase64") else "upload"
            storage_path = ""
            file_size_bytes = int(data.get("fileSizeBytes") or 0)

            if source_kind == "upload":
                content_base64 = api._trim(data.get("contentBase64"))
                if not content_base64:
                    raise ValueError("contentBase64 is required for uploads")
                try:
                    payload_bytes = base64.b64decode(content_base64, validate=True)
                except Exception as exc:
                    raise ValueError("Invalid file payload") from exc
                if not file_name:
                    raise ValueError("fileName is required for uploads")
                file_size_bytes = len(payload_bytes)
                if file_size_bytes <= 0:
                    raise ValueError("Uploaded file is empty")
                if file_size_bytes > api.LEAD_DOCUMENT_MAX_BYTES:
                    raise ValueError(f"File exceeds max size of {api.LEAD_DOCUMENT_MAX_BYTES} bytes")
                api._ensure_lead_document_bucket()
                safe_name = api._sanitize_storage_filename(file_name)
                storage_path = f"lead-{int(lead['lead_id'])}/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:8]}-{safe_name}"
                api._supabase_storage_rest(
                    f"object/{quote(api.LEAD_DOCUMENT_BUCKET, safe='')}/{quote(storage_path, safe='/')}",
                    method="POST",
                    body=payload_bytes,
                    content_type=mime_type or "application/octet-stream",
                    extra_headers={"x-upsert": "false"},
                )
            else:
                if not source_url:
                    raise ValueError("Add a document link or file.")
                if not file_name:
                    parsed_name = urlparse(source_url).path.rsplit("/", 1)[-1]
                    file_name = parsed_name or "External document"

            row_payload = {
                "lead_id": int(lead["lead_id"]),
                "source_kind": source_kind,
                "document_category": document_category,
                "file_name": file_name,
                "storage_bucket": api.LEAD_DOCUMENT_BUCKET if storage_path else None,
                "storage_path": storage_path or None,
                "source_url": source_url or None,
                "mime_type": mime_type or None,
                "file_size_bytes": file_size_bytes or None,
                "notes": notes or None,
                "uploaded_by_email": uploaded_by_email or None,
            }
            created = api._supabase_rest(
                "lead_document?select=*",
                method="POST",
                body=row_payload,
                extra_headers={"Prefer": "return=representation"},
            ) or []
            row = created[0] if isinstance(created, list) and created else row_payload
            try:
                api._supabase_rest(
                    "call_desk_activity",
                    method="POST",
                    body={
                        "lead_id": int(lead["lead_id"]),
                        "activity_date": api.now_iso(),
                        "channel": "portal",
                        "activity_type": "document_added",
                        "owner": uploaded_by_email or None,
                        "notes": f"{document_category}: {file_name}",
                    },
                )
            except Exception:
                pass
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": api._lead_document_row_to_payload(row)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            message = str(exc)
            if "lead_document" in message:
                message = "Lead document hub is not configured yet. Run lead_document_hub.sql in Supabase."
            self.wfile.write(json.dumps({"ok": False, "error": message}).encode("utf-8"))


    def _handle_lead_document_archive(self, document_id_raw: str) -> None:
        import local_db_api as api
        try:
            document_id = int(document_id_raw or 0)
            if document_id <= 0:
                raise ValueError("document_id is required")
            updated = api._supabase_rest(
                f"lead_document?document_id=eq.{document_id}&select=*",
                method="PATCH",
                body={"archived_at": api.now_iso()},
                extra_headers={"Prefer": "return=representation"},
            ) or []
            row = updated[0] if isinstance(updated, list) and updated else None
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": api._lead_document_row_to_payload(row or {})}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            message = str(exc)
            if "lead_document" in message:
                message = "Lead document hub is not configured yet. Run lead_document_hub.sql in Supabase."
            self.wfile.write(json.dumps({"ok": False, "error": message}).encode("utf-8"))

