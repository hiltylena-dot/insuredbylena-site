#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import csv
import re
import mimetypes
import base64
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import shutil
from urllib import error as urlerror, request as urlrequest
from urllib.parse import parse_qs, quote, urlparse
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from clean_lead_master_data import run_cleanup

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "insurance_lifecycle.db"
DRIVE_DB_DIR = Path(
    "/Users/hankybot/Library/CloudStorage/GoogleDrive-hiltylena@gmail.com/My Drive/Database"
)
DRIVE_DB_PATH = DRIVE_DB_DIR / "insurance_lifecycle.db"
HOST = os.getenv("HOST", "127.0.0.1").strip() or "127.0.0.1"
PORT = int(os.getenv("PORT", "8787").strip() or "8787")
APP_TIMEZONE = ZoneInfo(os.getenv("PORTAL_TIMEZONE", "America/New_York").strip() or "America/New_York")


def load_local_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            os.environ[key] = value.strip().strip("'").strip('"')
    except Exception:
        return


load_local_env()

GOG_CLIENT = os.getenv("GOG_CLIENT", "custom")
GOG_ACCOUNT = os.getenv("GOG_ACCOUNT", "hiltylena@gmail.com")
GOG_CALENDAR_ID = os.getenv("GOG_CALENDAR_ID", "primary")
GOOGLE_CALENDAR_WEB_APP_URL = os.getenv("GOOGLE_CALENDAR_WEB_APP_URL", "").strip()
GOOGLE_CALENDAR_SECRET = os.getenv("GOOGLE_CALENDAR_SECRET", "").strip()
CONTENT_SCHEDULER_WEBHOOK_URL = os.getenv("CONTENT_SCHEDULER_WEBHOOK_URL", "").strip()
CONTENT_SCHEDULER_API_KEY = os.getenv("CONTENT_SCHEDULER_API_KEY", "").strip()
CONTENT_SCHEDULER_NAME = os.getenv("CONTENT_SCHEDULER_NAME", "buffer_postiz").strip() or "buffer_postiz"
CONTENT_PUBLISHER_MODE = os.getenv("CONTENT_PUBLISHER_MODE", "").strip().lower()
CONTENT_AUTO_DRIVE_MEDIA_LOOKUP = os.getenv("CONTENT_AUTO_DRIVE_MEDIA_LOOKUP", "1").strip() != "0"
CONTENT_FILES_ROOT = Path("/Users/hankybot/Documents/Playground/insuredbylena-content")
BUFFER_IMPORT_DEFAULT_PATH = CONTENT_FILES_ROOT / "buffer-import.json"
MEDIA_LINKS_CSV_PATH = CONTENT_FILES_ROOT / "media-links.csv"
MEDIA_LINKS_TEMPLATE_CSV_PATH = CONTENT_FILES_ROOT / "media-links-template.csv"
BUFFER_API_BASE_URL = os.getenv("BUFFER_API_BASE_URL", "https://api.buffer.com").strip() or "https://api.buffer.com"
BUFFER_API_KEY = os.getenv("BUFFER_API_KEY", "").strip()
BUFFER_ORGANIZATION_ID = os.getenv("BUFFER_ORGANIZATION_ID", "").strip()
BUFFER_CHANNEL_ID_INSTAGRAM = os.getenv("BUFFER_CHANNEL_ID_INSTAGRAM", "").strip()
BUFFER_CHANNEL_ID_FACEBOOK = os.getenv("BUFFER_CHANNEL_ID_FACEBOOK", "").strip()
BUFFER_CHANNEL_ID_TIKTOK = os.getenv("BUFFER_CHANNEL_ID_TIKTOK", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
PUBLIC_MEDIA_SYNC_SCRIPT = CONTENT_FILES_ROOT / "scripts" / "sync_public_media.py"
_DRIVE_MEDIA_CACHE: dict[str, str] = {}
_ASSET_HINTS_CACHE: dict[str, str] | None = None
_BUFFER_CHANNEL_CACHE: dict[str, str] | None = None
LEAD_DOCUMENT_BUCKET = os.getenv("LEAD_DOCUMENT_BUCKET", "lead-documents").strip() or "lead-documents"
LEAD_DOCUMENT_MAX_BYTES = int(os.getenv("LEAD_DOCUMENT_MAX_BYTES", "10485760").strip() or "10485760")
_LEAD_DOCUMENT_BUCKET_READY = False


def _content_store_mode() -> str:
    if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
        return "supabase"
    return "sqlite"


def _use_supabase_backend() -> bool:
    return _content_store_mode() == "supabase"


def _supabase_rest(
    path: str,
    method: str = "GET",
    body: Any | None = None,
    extra_headers: dict[str, str] | None = None,
) -> Any:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("supabase_not_configured")
    url = f"{SUPABASE_URL}/rest/v1/{path.lstrip('/')}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urlrequest.Request(url, data=data, method=method.upper(), headers=headers)
    try:
        with urlrequest.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else None
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"supabase_rest_{exc.code}: {detail}") from exc


def _supabase_storage_rest(
    path: str,
    *,
    method: str = "GET",
    body: bytes | dict | list | None = None,
    extra_headers: dict[str, str] | None = None,
    content_type: str | None = "application/json",
) -> Any:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("supabase_not_configured")
    url = f"{SUPABASE_URL}/storage/v1/{path.lstrip('/')}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    if content_type:
        headers["Content-Type"] = content_type
    if extra_headers:
        headers.update(extra_headers)
    data = body
    if isinstance(body, (dict, list)):
        data = json.dumps(body).encode("utf-8")
    req = urlrequest.Request(url, data=data, method=method.upper(), headers=headers)
    try:
        with urlrequest.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else None
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"supabase_storage_{exc.code}: {detail}") from exc


def _sanitize_storage_filename(name: str) -> str:
    value = _trim(name)
    if not value:
        return "document"
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")
    return cleaned[:120] or "document"


def _ensure_lead_document_bucket() -> None:
    global _LEAD_DOCUMENT_BUCKET_READY
    if _LEAD_DOCUMENT_BUCKET_READY:
        return
    try:
        _supabase_storage_rest(
            "bucket",
            method="POST",
            body={
                "id": LEAD_DOCUMENT_BUCKET,
                "name": LEAD_DOCUMENT_BUCKET,
                "public": False,
                "file_size_limit": LEAD_DOCUMENT_MAX_BYTES,
                "allowed_mime_types": [
                    "application/pdf",
                    "image/jpeg",
                    "image/png",
                    "image/webp",
                    "text/plain",
                    "application/msword",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ],
            },
        )
    except RuntimeError as exc:
        message = str(exc)
        if "supabase_storage_400" not in message and "supabase_storage_409" not in message:
            raise
    _LEAD_DOCUMENT_BUCKET_READY = True


def _lead_document_signed_url(storage_path: str) -> str:
    path = _trim(storage_path)
    if not path:
        return ""
    result = _supabase_storage_rest(
        f"object/sign/{quote(LEAD_DOCUMENT_BUCKET, safe='')}/{quote(path, safe='/')}",
        method="POST",
        body={"expiresIn": 60 * 60 * 12},
    ) or {}
    signed = _trim(result.get("signedURL"))
    if not signed:
        return ""
    if signed.startswith("http://") or signed.startswith("https://"):
        return signed
    return f"{SUPABASE_URL}/storage/v1{signed}"


def _load_supabase_lead_by_external_id(lead_external_id: str) -> dict[str, Any] | None:
    external_id = _trim(lead_external_id)
    if not external_id:
        return None
    rows = _supabase_rest(
        f"lead_master?select=lead_id,lead_external_id,full_name,email,mobile_phone&lead_external_id=eq.{quote(external_id, safe='')}&limit=1",
        method="GET",
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    return row if isinstance(row, dict) else None


def _lead_document_row_to_payload(row: dict[str, Any]) -> dict[str, Any]:
    storage_path = _trim(row.get("storage_path"))
    source_url = _trim(row.get("source_url"))
    return {
        "documentId": int(row.get("document_id") or 0),
        "leadId": int(row.get("lead_id") or 0),
        "fileName": _trim(row.get("file_name")),
        "documentCategory": _trim(row.get("document_category")) or "general",
        "sourceKind": _trim(row.get("source_kind")) or "upload",
        "sourceUrl": source_url,
        "downloadUrl": _lead_document_signed_url(storage_path) if storage_path else source_url,
        "mimeType": _trim(row.get("mime_type")),
        "fileSizeBytes": int(row.get("file_size_bytes") or 0),
        "notes": _trim(row.get("notes")),
        "uploadedByEmail": _trim(row.get("uploaded_by_email")),
        "insertedAt": _trim(row.get("inserted_at")),
        "updatedAt": _trim(row.get("updated_at")),
        "storagePath": storage_path,
    }


def _google_calendar_via_web_app(
    *,
    client_name: str,
    email: str,
    phone: str,
    scheduled_at: str,
    description: str,
    duration_minutes: int = 30,
    existing_event_id: str = "",
) -> dict[str, Any]:
    if not GOOGLE_CALENDAR_WEB_APP_URL:
        raise RuntimeError("google_calendar_web_app_not_configured")
    payload = {
        "secret": GOOGLE_CALENDAR_SECRET,
        "clientName": client_name,
        "email": email,
        "phone": phone,
        "scheduledAt": scheduled_at,
        "description": description,
        "durationMinutes": max(int(duration_minutes or 30), 15),
        "existingEventId": existing_event_id,
    }
    req = urlrequest.Request(
        GOOGLE_CALENDAR_WEB_APP_URL,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlrequest.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw or "{}")
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"google_calendar_http_{exc.code}: {detail}") from exc
    except urlerror.URLError as exc:
        raise RuntimeError(f"google_calendar_network_error: {exc.reason}") from exc
    if not isinstance(data, dict) or not data.get("ok"):
        raise RuntimeError(str((data or {}).get("error") or "google_calendar_sync_failed"))
    return data


def _supabase_content_record_to_post(record: dict[str, Any]) -> dict:
    platforms_raw = record.get("platforms_json")
    if isinstance(platforms_raw, list):
        platforms = platforms_raw
    else:
        try:
            platforms = json.loads(str(platforms_raw or "[]"))
        except Exception:
            platforms = []
    if not isinstance(platforms, list):
        platforms = []
    hashtags_text = str(record.get("hashtags_text") or "").strip()
    hashtags = [tag for tag in hashtags_text.split() if tag]
    return {
        "id": int(record.get("id") or 0),
        "post_id": str(record.get("post_id") or ""),
        "week_number": int(record.get("week_number") or 0),
        "day": int(record.get("day") or 0),
        "post_date": str(record.get("post_date") or ""),
        "post_time": str(record.get("post_time") or ""),
        "scheduled_for": str(record.get("scheduled_for") or ""),
        "platforms": platforms,
        "post_type": str(record.get("post_type") or ""),
        "topic": str(record.get("topic") or ""),
        "hook": str(record.get("hook") or ""),
        "caption": str(record.get("caption") or ""),
        "reel_script": str(record.get("reel_script") or ""),
        "visual_prompt": str(record.get("visual_prompt") or ""),
        "canva_design_link": str(record.get("canva_design_link") or ""),
        "asset_filename": str(record.get("asset_filename") or ""),
        "cta": str(record.get("cta") or ""),
        "hashtags_text": hashtags_text,
        "hashtags": hashtags,
        "status": str(record.get("status") or "draft"),
        "source_file": str(record.get("source_file") or ""),
        "scheduler_external_id": str(record.get("scheduler_external_id") or ""),
        "last_publish_error": str(record.get("last_publish_error") or ""),
        "created_by": str(record.get("created_by") or ""),
        "approved_by": str(record.get("approved_by") or ""),
        "approved_at": str(record.get("approved_at") or ""),
        "created_at": str(record.get("created_at") or ""),
        "updated_at": str(record.get("updated_at") or ""),
    }


def _supabase_content_patch_post(content_post_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    post_id = int(content_post_id or 0)
    if post_id <= 0:
        raise RuntimeError("content_post_id_required")
    body = dict(payload or {})
    rows = _supabase_rest(
        f"content_post?id=eq.{post_id}&select=*",
        method="PATCH",
        body=body,
        extra_headers={"Prefer": "return=representation"},
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    if not isinstance(row, dict):
        raise RuntimeError("content_post_patch_failed")
    return _supabase_content_record_to_post(row)


def _supabase_content_insert_publish_job(payload: dict[str, Any]) -> int:
    body = {
        "content_post_id": int(payload.get("content_post_id") or 0),
        "scheduler": _trim(payload.get("scheduler")) or "buffer_graphql",
        "request_json": payload.get("request_json") or {},
        "status": _trim(payload.get("status")) or "queued",
    }
    rows = _supabase_rest(
        "content_publish_job?select=id",
        method="POST",
        body=body,
        extra_headers={"Prefer": "return=representation"},
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    job_id = int((row or {}).get("id") or 0) if isinstance(row, dict) else 0
    if job_id <= 0:
        raise RuntimeError("content_publish_job_insert_failed")
    return job_id


def _supabase_content_get_post(content_post_id: int) -> dict[str, Any] | None:
    post_id = int(content_post_id or 0)
    if post_id <= 0:
        return None
    rows = _supabase_rest(
        f"content_post?select=*&id=eq.{post_id}&limit=1",
        method="GET",
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    return _supabase_content_record_to_post(row) if isinstance(row, dict) else None


def _supabase_content_next_revision_number(content_post_id: int) -> int:
    rows = _supabase_rest(
        f"content_revision?select=revision_number&content_post_id=eq.{int(content_post_id)}&order=revision_number.desc,id.desc&limit=1",
        method="GET",
    ) or []
    current = rows[0] if isinstance(rows, list) and rows else {}
    return int((current or {}).get("revision_number") or 0) + 1


def _supabase_content_insert_revision(
    content_post_id: int,
    changed_by: str,
    change_note: str,
    snapshot: dict[str, Any],
) -> int:
    rows = _supabase_rest(
        "content_revision?select=id",
        method="POST",
        body={
            "content_post_id": int(content_post_id),
            "revision_number": _supabase_content_next_revision_number(content_post_id),
            "changed_by": _trim(changed_by) or None,
            "change_note": _trim(change_note) or None,
            "snapshot_json": snapshot or {},
        },
        extra_headers={"Prefer": "return=representation"},
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    revision_id = int((row or {}).get("id") or 0) if isinstance(row, dict) else 0
    if revision_id <= 0:
        raise RuntimeError("content_revision_insert_failed")
    return revision_id


def _supabase_content_insert_approval(content_post_id: int, decision: str, note: str, actor: str) -> int:
    rows = _supabase_rest(
        "content_approval?select=id",
        method="POST",
        body={
            "content_post_id": int(content_post_id),
            "decision": _trim(decision) or "submitted",
            "note": _trim(note) or None,
            "actor": _trim(actor) or None,
        },
        extra_headers={"Prefer": "return=representation"},
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    approval_id = int((row or {}).get("id") or 0) if isinstance(row, dict) else 0
    if approval_id <= 0:
        raise RuntimeError("content_approval_insert_failed")
    return approval_id


def _supabase_content_write_payload(item: dict[str, Any]) -> dict[str, Any]:
    platforms_raw = item.get("platforms_json")
    if isinstance(platforms_raw, str):
        try:
            platforms_raw = json.loads(platforms_raw)
        except Exception:
            platforms_raw = []
    if not isinstance(platforms_raw, list):
        platforms_raw = []
    return {
        "post_id": item.get("post_id"),
        "week_number": int(item.get("week_number") or 0),
        "day": int(item.get("day") or 0),
        "post_date": _trim(item.get("post_date")) or None,
        "post_time": _trim(item.get("post_time")) or None,
        "scheduled_for": _trim(item.get("scheduled_for")) or None,
        "platforms_json": platforms_raw,
        "post_type": _trim(item.get("post_type")) or None,
        "topic": _trim(item.get("topic")) or None,
        "hook": _trim(item.get("hook")) or None,
        "caption": _trim(item.get("caption")) or None,
        "reel_script": _trim(item.get("reel_script")) or None,
        "visual_prompt": _trim(item.get("visual_prompt")) or None,
        "canva_design_link": _trim(item.get("canva_design_link")) or None,
        "asset_filename": _trim(item.get("asset_filename")) or None,
        "cta": _trim(item.get("cta")) or None,
        "hashtags_text": _trim(item.get("hashtags_text")) or None,
        "status": _trim(item.get("status")) or "draft",
        "source_file": _trim(item.get("source_file")) or None,
        "created_by": _trim(item.get("created_by")) or None,
        "updated_at": now_iso(),
    }


def _supabase_content_import_posts(posts: list[dict], week_number: int, actor: str, source_file: str) -> tuple[int, int]:
    imported = 0
    updated = 0
    for raw_item in posts:
        if not isinstance(raw_item, dict):
            continue
        item = normalize_content_post_input(raw_item, week_number, actor, source_file)
        post_id = quote(_trim(item.get("post_id")), safe="")
        post_date = quote(_trim(item.get("post_date")), safe="")
        existing_rows = _supabase_rest(
            f"content_post?select=*&post_id=eq.{post_id}&post_date=eq.{post_date}&limit=1",
            method="GET",
        ) or []
        existing = existing_rows[0] if isinstance(existing_rows, list) and existing_rows else None
        payload = _supabase_content_write_payload(item)
        if isinstance(existing, dict):
            current = _supabase_content_record_to_post(existing)
            _supabase_content_insert_revision(
                int(existing.get("id") or 0),
                actor,
                "import_update",
                _content_snapshot_from_post(current),
            )
            _supabase_content_patch_post(int(existing.get("id") or 0), payload)
            updated += 1
        else:
            created = _supabase_rest(
                "content_post?select=id",
                method="POST",
                body=payload,
                extra_headers={"Prefer": "return=representation"},
            ) or []
            row = created[0] if isinstance(created, list) and created else None
            if not isinstance(row, dict) or int(row.get("id") or 0) <= 0:
                raise RuntimeError("content_post_insert_failed")
            imported += 1
    return imported, updated


def _supabase_portal_save_call_desk(payload: dict[str, Any]) -> dict[str, Any]:
    response = _supabase_rest(
        "rpc/portal_save_call_desk",
        method="POST",
        body={"p_payload": payload},
    ) or {}
    if not isinstance(response, dict) or not response.get("ok"):
        raise RuntimeError(str((response or {}).get("error") or "portal_save_call_desk_failed"))
    return response


def _lead_sync_payload_from_request(data: dict[str, Any], force_external_id: str = "") -> dict[str, Any]:
    contact_id = _trim(force_external_id) or _trim(data.get("contactId") or data.get("lead_external_id"))
    first_name = _trim(data.get("firstName") or data.get("first_name"))
    last_name = _trim(data.get("lastName") or data.get("last_name"))
    full_name = _trim(data.get("fullName") or data.get("full_name"))
    next_appointment = _trim(data.get("nextAppointmentTime") or data.get("next_appointment_time"))
    disposition = _trim(data.get("disposition") or data.get("lead_status"))
    should_schedule_raw = data.get("shouldSchedule")
    if should_schedule_raw is None:
        should_schedule = bool(next_appointment and disposition in {"callback", "follow_up"})
    else:
        should_schedule = str(should_schedule_raw).strip().lower() in {"true", "t", "1", "yes", "on"}
    return {
        "contactId": contact_id,
        "firstName": first_name,
        "lastName": last_name,
        "fullName": full_name or " ".join(part for part in [first_name, last_name] if part).strip(),
        "phone": _trim(data.get("phone") or data.get("mobile_phone")),
        "email": _trim(data.get("email")),
        "notes": _trim(data.get("notes")),
        "tags": _trim(data.get("tags") or data.get("raw_tags")),
        "lastActivity": _trim(data.get("lastActivity") or data.get("last_activity_at_source")) or now_iso(),
        "age": _trim(data.get("age")),
        "tobacco": _trim(data.get("tobacco")),
        "healthPosture": _trim(data.get("healthPosture") or data.get("health_posture")),
        "disposition": disposition,
        "carrierMatch": _trim(data.get("carrierMatch") or data.get("carrier_match")),
        "confidence": _trim(data.get("confidence")),
        "pipelineStatus": _trim(data.get("pipelineStatus") or data.get("pipeline_status")),
        "nextAppointmentTime": next_appointment,
        "shouldSchedule": should_schedule,
        "leadSource": _trim(data.get("leadSource") or data.get("lead_source")),
        "leadSourceDetail": _trim(data.get("leadSourceDetail") or data.get("lead_source_detail")),
        "productLine": _trim(data.get("productLine") or data.get("product_line")),
        "productInterest": _trim(data.get("productInterest") or data.get("product_interest")),
    }


def _supabase_update_lead_fields(lead_external_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    external_id = _trim(lead_external_id)
    if not external_id:
        raise RuntimeError("lead_external_id_required")
    payload = {key: value for key, value in (fields or {}).items() if value is not None}
    rows = _supabase_rest(
        f"lead_master?lead_external_id=eq.{quote(external_id, safe='')}&select=*",
        method="PATCH",
        body=payload,
        extra_headers={"Prefer": "return=representation"},
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    return row if isinstance(row, dict) else None


def _supabase_import_leads(rows: list[dict]) -> dict[str, int]:
    added = 0
    updated = 0
    skipped_invalid = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped_invalid += 1
            continue
        lead_external_id = _nv(row.get("lead_external_id"))
        if not lead_external_id:
            skipped_invalid += 1
            continue
        body = {
            "lead_external_id": lead_external_id,
            "first_name": _nv(row.get("first_name")),
            "last_name": _nv(row.get("last_name")),
            "full_name": _nv(row.get("full_name")),
            "email": _nv(row.get("email")),
            "mobile_phone": _nv(row.get("mobile_phone")),
            "business_name": _nv(row.get("business_name")),
            "lead_source": _nv(row.get("lead_source")),
            "lead_source_detail": _nv(row.get("lead_source_detail")),
            "campaign_name": _nv(row.get("campaign_name")),
            "product_interest": _nv(row.get("product_interest")),
            "product_line": _nv(row.get("product_line")),
            "owner_queue": _nv(row.get("owner_queue")),
            "lead_status": _nv(row.get("lead_status")),
            "booking_status": _nv(row.get("booking_status")),
            "consent_status": _nv(row.get("consent_status")),
            "consent_channel_sms": _nv(row.get("consent_channel_sms")),
            "consent_channel_email": _nv(row.get("consent_channel_email")),
            "consent_channel_whatsapp": _nv(row.get("consent_channel_whatsapp")),
            "dnc_status": _nv(row.get("dnc_status")),
            "contact_eligibility": _nv(row.get("contact_eligibility")),
            "created_at_source": _nv(row.get("created_at_source")),
            "last_activity_at_source": _nv(row.get("last_activity_at_source")),
            "notes": _nv(row.get("notes")),
            "raw_tags": _nv(row.get("raw_tags")),
            "routing_bucket": _nv(row.get("routing_bucket")),
            "suppress_reason": _nv(row.get("suppress_reason")),
            "recommended_channel": _nv(row.get("recommended_channel")),
            "sequence_name": _nv(row.get("sequence_name")),
            "recommended_next_action": _nv(row.get("recommended_next_action")),
            "priority_tier": _nv(row.get("priority_tier")),
            "age": _nv(row.get("age")),
            "tobacco": _nv(row.get("tobacco")),
            "health_posture": _nv(row.get("health_posture")),
            "disposition": _nv(row.get("disposition")),
            "carrier_match": _nv(row.get("carrier_match")),
            "confidence": _nv(row.get("confidence")),
            "pipeline_status": _nv(row.get("pipeline_status") or row.get("pipelineStatus")),
            "calendar_event_id": _nv(row.get("calendar_event_id") or row.get("calendarEventId")),
            "next_appointment_time": _nv(row.get("next_appointment_time") or row.get("nextAppointmentTime")),
            "last_opened_at": _nv(row.get("last_opened_at") or row.get("lastOpenedAt")),
            "updated_at": now_iso(),
        }
        existing = _load_supabase_lead_by_external_id(lead_external_id)
        if existing:
            _supabase_update_lead_fields(lead_external_id, body)
            updated += 1
        else:
            created = _supabase_rest(
                "lead_master?select=lead_id",
                method="POST",
                body=body,
                extra_headers={"Prefer": "return=representation"},
            ) or []
            row_data = created[0] if isinstance(created, list) and created else None
            if not isinstance(row_data, dict) or int(row_data.get("lead_id") or 0) <= 0:
                raise RuntimeError("lead_master_insert_failed")
            added += 1
    return {
        "added": added,
        "updated": updated,
        "skipped_existing": 0,
        "skipped_invalid": skipped_invalid,
    }


def _supabase_load_leads_by_ids(lead_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not lead_ids:
        return {}
    id_list = ",".join(str(int(item)) for item in lead_ids if int(item) > 0)
    if not id_list:
        return {}
    rows = _supabase_rest(
        f"lead_master?select=lead_id,lead_external_id,full_name,email,mobile_phone,next_appointment_time,disposition,pipeline_status&lead_id=in.({id_list})",
        method="GET",
    ) or []
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        if isinstance(row, dict):
            result[int(row.get("lead_id") or 0)] = row
    return result


def _supabase_calendar_items(start_dt: datetime, end_dt: datetime) -> list[dict[str, Any]]:
    start_iso = start_dt.astimezone(timezone.utc).isoformat()
    end_iso = end_dt.astimezone(timezone.utc).isoformat()
    rows = _supabase_rest(
        f"appointment?select=appointment_id,lead_id,booking_date,booking_status,show_status,appointment_type,owner&booking_date=gte.{quote(start_iso, safe='')}&booking_date=lt.{quote(end_iso, safe='')}&booking_status=in.(Booked,Rescheduled,Pending)&order=booking_date.asc&limit=200",
        method="GET",
    ) or []
    lead_map = _supabase_load_leads_by_ids(
        [int(row.get("lead_id") or 0) for row in rows if isinstance(row, dict)]
    )
    items = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        lead = lead_map.get(int(row.get("lead_id") or 0), {})
        summary = _trim(lead.get("full_name")) or _trim(lead.get("lead_external_id")) or "Scheduled follow-up"
        items.append(
            {
                "id": str(row.get("appointment_id") or ""),
                "summary": summary,
                "start": _trim(row.get("booking_date")),
                "end": _trim(row.get("booking_date")),
                "htmlLink": "",
                "attendees": ([{"email": _trim(lead.get("email"))}] if _trim(lead.get("email")) else []),
                "leadExternalId": _trim(lead.get("lead_external_id")),
                "phone": _trim(lead.get("mobile_phone")),
                "appointmentType": _trim(row.get("appointment_type")),
                "owner": _trim(row.get("owner")),
            }
        )
    items.sort(key=lambda row: row["start"] or "")
    return items


def _supabase_sync_calendar_schedule(contact_id: str, start_iso: str, event_id: str = "") -> None:
    lead = _load_supabase_lead_by_external_id(contact_id)
    if not lead:
        return
    lead_id = int(lead.get("lead_id") or 0)
    if lead_id <= 0:
        return
    _supabase_update_lead_fields(
        contact_id,
        {
            "next_appointment_time": start_iso,
            "calendar_event_id": _trim(event_id) or None,
            "booking_status": "Booked",
            "updated_at": now_iso(),
        },
    )
    rows = _supabase_rest(
        f"appointment?select=appointment_id&lead_id=eq.{lead_id}&owner=eq.call_desk&booking_status=in.(Booked,Rescheduled,Pending)&limit=1",
        method="GET",
    ) or []
    body = {
        "lead_id": lead_id,
        "booking_date": start_iso,
        "booking_status": "Booked",
        "show_status": "pending",
        "appointment_type": "callback",
        "owner": "call_desk",
    }
    if isinstance(rows, list) and rows:
        appointment_id = int((rows[0] or {}).get("appointment_id") or 0)
        if appointment_id > 0:
            _supabase_rest(
                f"appointment?appointment_id=eq.{appointment_id}",
                method="PATCH",
                body=body,
                extra_headers={"Prefer": "return=minimal"},
            )
            return
    _supabase_rest(
        "appointment",
        method="POST",
        body=body,
        extra_headers={"Prefer": "return=minimal"},
    )


def _content_snapshot_from_post(post: dict) -> dict[str, Any]:
    return {
        "id": int(post.get("id") or 0),
        "post_id": str(post.get("post_id") or ""),
        "week_number": int(post.get("week_number") or 0),
        "day": int(post.get("day") or 0),
        "post_date": _trim(post.get("post_date")),
        "post_time": _trim(post.get("post_time")),
        "scheduled_for": _trim(post.get("scheduled_for")),
        "platforms": post.get("platforms") or [],
        "post_type": _trim(post.get("post_type")),
        "topic": _trim(post.get("topic")),
        "hook": _trim(post.get("hook")),
        "caption": _trim(post.get("caption")),
        "reel_script": _trim(post.get("reel_script")),
        "visual_prompt": _trim(post.get("visual_prompt")),
        "canva_design_link": _trim(post.get("canva_design_link")),
        "asset_filename": _trim(post.get("asset_filename")),
        "cta": _trim(post.get("cta")),
        "hashtags_text": _trim(post.get("hashtags_text")),
        "status": _trim(post.get("status")) or "draft",
    }


def _normalize_drive_url(url: str) -> str:
    value = _trim(url)
    if not value:
        return ""
    match = re.search(r"drive\.google\.com/file/d/([A-Za-z0-9_-]+)", value)
    if match:
        return f"https://drive.google.com/uc?export=download&id={match.group(1)}"
    return value


def _extract_drive_file_id(url: str) -> str:
    value = _trim(url)
    if not value:
        return ""
    match = re.search(r"[?&]id=([A-Za-z0-9_-]+)", value)
    if match:
        return match.group(1)
    match = re.search(r"drive\.google\.com/file/d/([A-Za-z0-9_-]+)", value)
    if match:
        return match.group(1)
    return ""


def _mime_type_for_url(url: str) -> str:
    clean = _trim(url).split("?", 1)[0]
    guessed, _ = mimetypes.guess_type(clean)
    return _trim(guessed).lower()


def _publisher_mode() -> str:
    mode = _trim(CONTENT_PUBLISHER_MODE).lower()
    if mode:
        return mode
    if BUFFER_API_KEY:
        return "buffer_graphql"
    if CONTENT_SCHEDULER_WEBHOOK_URL:
        return "webhook"
    return "mock"


def _buffer_channel_env_map() -> dict[str, str]:
    return {
        "instagram": _trim(BUFFER_CHANNEL_ID_INSTAGRAM),
        "facebook": _trim(BUFFER_CHANNEL_ID_FACEBOOK),
        "tiktok": _trim(BUFFER_CHANNEL_ID_TIKTOK),
    }


def sync_public_media_links() -> dict[str, Any]:
    if not PUBLIC_MEDIA_SYNC_SCRIPT.exists():
        return {"ok": False, "skipped": True, "reason": "sync_script_missing"}
    try:
        proc = subprocess.run(
            ["python3", str(PUBLIC_MEDIA_SYNC_SCRIPT)],
            capture_output=True,
            text=True,
            check=False,
            timeout=120.0,
        )
        stdout = _trim(proc.stdout)
        stderr = _trim(proc.stderr)
        if proc.returncode != 0:
            return {
                "ok": False,
                "skipped": False,
                "reason": "sync_failed",
                "error": stderr or stdout or f"exit_{proc.returncode}",
            }
        parsed = json.loads(stdout) if stdout else {}
        if isinstance(parsed, dict):
            return {"ok": True, **parsed}
        return {"ok": True, "raw": stdout}
    except Exception as exc:
        return {"ok": False, "skipped": False, "reason": "sync_exception", "error": str(exc)}


def _is_placeholder_file_id(file_id: str) -> bool:
    return _trim(file_id).upper().startswith("FILE_ID_")


def _candidate_asset_ids(week_number: int, day_in_week: int, platform: str) -> list[str]:
    p = _trim(platform).lower()
    absolute_day = ((int(week_number or 0) - 1) * 7) + int(day_in_week or 0)
    return [
        f"w{week_number}-d{day_in_week}-{p}",
        f"w{week_number}d{day_in_week}-{p}",
        f"w{week_number}-d{absolute_day}-{p}",
        f"w{week_number}d{absolute_day}-{p}",
    ]


def _is_media_mime(mime_type: str) -> bool:
    value = _trim(mime_type).lower()
    return value.startswith("image/") or value.startswith("video/")


def _parse_iso_like(value: str) -> datetime:
    raw = _trim(value).replace("Z", "+00:00")
    if not raw:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _load_asset_filename_hints() -> dict[str, str]:
    global _ASSET_HINTS_CACHE
    if _ASSET_HINTS_CACHE is not None:
        return _ASSET_HINTS_CACHE

    hints: dict[str, str] = {}
    for file_path in sorted(CONTENT_FILES_ROOT.glob("WEEK*_SCHEDULER_EXPORT.json")):
        try:
            rows = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            post_id = _trim(row.get("post_id"))
            asset_filename = _trim(row.get("asset_filename"))
            if not post_id or not asset_filename:
                continue
            match = re.match(r"^W(\d+)D(\d+)$", post_id, re.IGNORECASE)
            if not match:
                continue
            week_number = int(match.group(1))
            day_in_week = int(match.group(2))
            absolute_day = ((week_number - 1) * 7) + day_in_week
            placeholder = f"FILE_ID_W{week_number}D{absolute_day}"
            if placeholder not in hints:
                hints[placeholder] = asset_filename
    _ASSET_HINTS_CACHE = hints
    return hints


def _search_drive_for_media(query: str) -> dict[str, Any] | None:
    if not _trim(query):
        return None
    try:
        cmd = [
            "gog",
            "--client",
            GOG_CLIENT,
            "--account",
            GOG_ACCOUNT,
            "--json",
            "drive",
            "search",
            query,
            "--results-only",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=4.0)
        if proc.returncode != 0:
            return None
        output = _trim(proc.stdout)
        payload = json.loads(output) if output else []
    except Exception:
        return None
    rows = payload if isinstance(payload, list) else []
    matches = [row for row in rows if isinstance(row, dict) and _is_media_mime(_trim(row.get("mimeType")))]
    if not matches:
        return None
    exact = [row for row in matches if _trim(row.get("name")).lower() == _trim(query).lower()]
    candidates = exact or matches
    candidates.sort(key=lambda row: _parse_iso_like(_trim(row.get("modifiedTime"))), reverse=True)
    return candidates[0] if candidates else None


def _ensure_drive_public_read(file_id: str) -> None:
    if not _trim(file_id):
        return
    try:
        run_gog_json(
            [
                "drive",
                "share",
                file_id,
                "--to",
                "anyone",
                "--role",
                "reader",
                "--discoverable",
                "false",
            ]
        )
    except Exception:
        # Permissions may already exist or policy may block public sharing.
        return


def _auto_resolve_drive_media_url(file_id_placeholder: str, week_number: int, day_in_week: int, platform: str) -> str:
    key = _trim(file_id_placeholder).upper()
    if not key or not _is_placeholder_file_id(key):
        return ""
    if key in _DRIVE_MEDIA_CACHE:
        return _DRIVE_MEDIA_CACHE[key]

    hints = _load_asset_filename_hints()
    absolute_day = ((int(week_number or 0) - 1) * 7) + int(day_in_week or 0)
    platform_slug = _trim(platform).lower()
    queries: list[str] = []

    hinted = _trim(hints.get(key))
    if hinted:
        queries.append(hinted)
    queries.extend(
        [
            f"w{week_number}d{absolute_day}-{platform_slug}",
            f"w{week_number}-d{absolute_day}-{platform_slug}",
            f"w{week_number}d{absolute_day}",
            f"W{week_number}D{absolute_day}",
            key,
        ]
    )

    seen: set[str] = set()
    for query in queries:
        q = _trim(query)
        if not q or q.lower() in seen:
            continue
        seen.add(q.lower())
        found = _search_drive_for_media(q)
        if not found:
            continue
        found_id = _trim(found.get("id"))
        if not found_id:
            continue
        _ensure_drive_public_read(found_id)
        resolved = f"https://drive.google.com/uc?export=download&id={found_id}"
        _DRIVE_MEDIA_CACHE[key] = resolved
        return resolved
    return ""


def load_media_links_map() -> dict[str, str]:
    by_key: dict[str, str] = {}
    for csv_path in (MEDIA_LINKS_CSV_PATH, MEDIA_LINKS_TEMPLATE_CSV_PATH):
        if not csv_path.exists():
            continue
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
                reader = csv.DictReader(fp)
                for row in reader:
                    if not isinstance(row, dict):
                        continue
                    key = _trim(row.get("key"))
                    asset_id = _trim(row.get("asset_id") or row.get("asset") or row.get("placeholder"))
                    media_url = _normalize_drive_url(_trim(row.get("media_url") or row.get("url")))
                    file_id = _trim(row.get("file_id") or row.get("id"))
                    resolved = ""
                    media_url_lower = media_url.lower()
                    media_url_is_placeholder = (
                        "file_placeholder" in media_url_lower
                        or "id=file_id_" in media_url_lower
                    )
                    if media_url and not media_url_is_placeholder:
                        resolved = media_url
                    elif file_id and not _is_placeholder_file_id(file_id):
                        resolved = f"https://drive.google.com/uc?export=download&id={file_id}"
                    if key and resolved:
                        by_key[key.lower()] = resolved
                    if asset_id and resolved:
                        by_key[asset_id.lower()] = resolved
        except Exception:
            continue
    return by_key


def resolve_media_url(
    media_url: str,
    week_number: int,
    day_in_week: int,
    platform: str,
    media_links_map: dict[str, str],
    allow_auto_drive_lookup: bool = True,
) -> str:
    normalized = _normalize_drive_url(media_url)
    file_id = _extract_drive_file_id(normalized)
    if file_id and not _is_placeholder_file_id(file_id):
        return normalized

    lookup_keys: list[str] = []
    absolute_day = ((int(week_number or 0) - 1) * 7) + int(day_in_week or 0)
    is_placeholder = _is_placeholder_file_id(file_id)
    if not is_placeholder and file_id:
        lookup_keys.append(file_id.lower())
    lookup_keys.extend(_candidate_asset_ids(week_number, day_in_week, platform))
    compact_platform = _platform_slug(platform)
    lookup_keys.append(f"w{week_number}-d{day_in_week}-{compact_platform}")
    lookup_keys.append(f"w{week_number}-d{absolute_day}-{compact_platform}")

    for key in lookup_keys:
        found = _trim(media_links_map.get(key))
        if found:
            return _normalize_drive_url(found)
    if is_placeholder and allow_auto_drive_lookup and CONTENT_AUTO_DRIVE_MEDIA_LOOKUP:
        auto = _auto_resolve_drive_media_url(file_id, week_number, day_in_week, platform)
        if auto:
            return _normalize_drive_url(auto)
    return normalized


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _trim(value) -> str:
    return str(value or "").strip()


def mirror_db_to_drive() -> None:
    if not DRIVE_DB_DIR.exists():
        return
    DRIVE_DB_DIR.mkdir(parents=True, exist_ok=True)
    backup_dir = DRIVE_DB_DIR / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DB_PATH, DRIVE_DB_PATH)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(DB_PATH, backup_dir / f"insurance_lifecycle_{stamp}.db")


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    # Harden SQLite for frequent concurrent reads/writes from dashboard + API calls.
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def normalize_payload(data: dict) -> dict:
    contact_id = _trim(data.get("contactId"))
    first_name = _trim(data.get("firstName"))
    last_name = _trim(data.get("lastName"))
    full_name = " ".join(part for part in [first_name, last_name] if part).strip()
    payload = {
        "lead_external_id": contact_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "mobile_phone": _trim(data.get("phone")),
        "email": _trim(data.get("email")),
        "notes": _trim(data.get("notes")),
        "raw_tags": _trim(data.get("tags")),
        "last_activity_at_source": _trim(data.get("lastActivity")) or now_iso(),
        "age": _trim(data.get("age")),
        "tobacco": _trim(data.get("tobacco")),
        "health_posture": _trim(data.get("healthPosture")),
        "disposition": _trim(data.get("disposition")),
        "carrier_match": _trim(data.get("carrierMatch")),
        "confidence": _trim(data.get("confidence")),
        "pipeline_status": _trim(data.get("pipelineStatus") or data.get("pipeline_status")),
        "calendar_event_id": _trim(data.get("calendarEventId") or data.get("calendar_event_id")),
        "next_appointment_time": _trim(data.get("nextAppointmentTime") or data.get("next_appointment_time")),
        "last_opened_at": _trim(data.get("lastOpenedAt") or data.get("last_opened_at")),
    }
    return payload


def _nv(value: Any) -> str | None:
    value = _trim(value)
    return value or None


def _parse_local_iso(dt_raw: str) -> datetime:
    cleaned = _trim(dt_raw).replace("Z", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return dt


def _is_due_by_schedule(scheduled_for: str, now_dt: datetime) -> bool:
    value = _trim(scheduled_for)
    if not value:
        return True
    try:
        scheduled_dt = _parse_local_iso(value).astimezone(now_dt.tzinfo or timezone.utc)
    except Exception:
        # Fall back to previous behavior if parsing fails for unexpected formats.
        return value <= now_dt.isoformat()
    return scheduled_dt <= now_dt


def _published_duplicate_reason(
    conn: sqlite3.Connection,
    content_post_id: int,
    post_id: str,
) -> str:
    same_row_success = conn.execute(
        """
        SELECT 1
        FROM content_publish_job
        WHERE content_post_id = ? AND status = 'published'
        LIMIT 1
        """,
        (content_post_id,),
    ).fetchone()
    if same_row_success:
        return "this row already has a successful publish job"

    other_row_success = conn.execute(
        """
        SELECT id
        FROM content_post
        WHERE post_id = ? AND id != ? AND status = 'published'
        LIMIT 1
        """,
        (post_id, content_post_id),
    ).fetchone()
    if other_row_success:
        return f"post_id already published by row {int(other_row_success['id'])}"

    return ""


def run_gog_json(args: list[str]) -> Any:
    cmd = [
        "gog",
        "--client",
        GOG_CLIENT,
        "--account",
        GOG_ACCOUNT,
        "--json",
        *args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        message = proc.stderr.strip() or proc.stdout.strip() or f"gog exited with {proc.returncode}"
        raise RuntimeError(message)
    output = proc.stdout.strip()
    if not output:
        return {}
    return json.loads(output)


def _extract_event_id(payload: Any) -> str:
    if isinstance(payload, dict):
        if _trim(payload.get("id")):
            return _trim(payload.get("id"))
        for key in ("event", "result", "data"):
            found = _extract_event_id(payload.get(key))
            if found:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _extract_event_id(item)
            if found:
                return found
    return ""


def _extract_events(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("items", "events", "result", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
            if isinstance(value, dict):
                nested = _extract_events(value)
                if nested:
                    return nested
    return []


def ensure_lead_master_columns(conn: sqlite3.Connection) -> None:
    required = {
        "age": "TEXT",
        "tobacco": "TEXT",
        "health_posture": "TEXT",
        "disposition": "TEXT",
        "carrier_match": "TEXT",
        "confidence": "TEXT",
        "pipeline_status": "TEXT",
        "calendar_event_id": "TEXT",
        "next_appointment_time": "TEXT",
        "last_opened_at": "TEXT",
    }
    existing = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(lead_master)").fetchall()
    }
    for column, column_type in required.items():
        if column in existing:
            continue
        conn.execute(f"ALTER TABLE lead_master ADD COLUMN {column} {column_type}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_master_last_opened_at ON lead_master (last_opened_at)")


def ensure_agent_carrier_config_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_carrier_config (
          id INTEGER PRIMARY KEY,
          carrier_name TEXT NOT NULL UNIQUE,
          writing_number TEXT,
          portal_url TEXT,
          support_phone TEXT
        )
        """
    )


def ensure_content_studio_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS content_post (
          id INTEGER PRIMARY KEY,
          post_id TEXT,
          week_number INTEGER,
          day INTEGER,
          post_date TEXT,
          post_time TEXT,
          scheduled_for TEXT,
          platforms_json TEXT NOT NULL DEFAULT '[]',
          post_type TEXT,
          topic TEXT,
          hook TEXT,
          caption TEXT,
          reel_script TEXT,
          visual_prompt TEXT,
          canva_design_link TEXT,
          asset_filename TEXT,
          cta TEXT,
          hashtags_text TEXT,
          status TEXT NOT NULL DEFAULT 'draft',
          source_file TEXT,
          scheduler_external_id TEXT,
          last_publish_error TEXT,
          created_by TEXT,
          approved_by TEXT,
          approved_at TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS content_revision (
          id INTEGER PRIMARY KEY,
          content_post_id INTEGER NOT NULL,
          revision_number INTEGER NOT NULL DEFAULT 1,
          changed_by TEXT,
          change_note TEXT,
          snapshot_json TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (content_post_id) REFERENCES content_post (id)
        );

        CREATE TABLE IF NOT EXISTS content_approval (
          id INTEGER PRIMARY KEY,
          content_post_id INTEGER NOT NULL,
          decision TEXT NOT NULL,
          note TEXT,
          actor TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (content_post_id) REFERENCES content_post (id)
        );

        CREATE TABLE IF NOT EXISTS content_publish_job (
          id INTEGER PRIMARY KEY,
          content_post_id INTEGER NOT NULL,
          scheduler TEXT NOT NULL DEFAULT 'buffer_postiz',
          request_json TEXT,
          response_json TEXT,
          status TEXT NOT NULL DEFAULT 'queued',
          error_message TEXT,
          run_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          completed_at TEXT,
          FOREIGN KEY (content_post_id) REFERENCES content_post (id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_content_post_post_id_day ON content_post (post_id, post_date);
        CREATE INDEX IF NOT EXISTS idx_content_post_status_schedule ON content_post (status, scheduled_for);
        CREATE INDEX IF NOT EXISTS idx_content_revision_post ON content_revision (content_post_id, revision_number);
        CREATE INDEX IF NOT EXISTS idx_content_publish_job_post ON content_publish_job (content_post_id, run_at);
        """
    )
    existing_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(content_post)").fetchall()}
    if "canva_design_link" not in existing_columns:
        conn.execute("ALTER TABLE content_post ADD COLUMN canva_design_link TEXT")


def row_to_content_post(row: sqlite3.Row) -> dict:
    platforms_raw = str(row["platforms_json"] or "[]")
    try:
        platforms = json.loads(platforms_raw)
    except Exception:
        platforms = []
    if not isinstance(platforms, list):
        platforms = []
    hashtags_text = str(row["hashtags_text"] or "").strip()
    hashtags = [tag for tag in hashtags_text.split() if tag]
    return {
        "id": int(row["id"]),
        "post_id": str(row["post_id"] or ""),
        "week_number": int(row["week_number"] or 0),
        "day": int(row["day"] or 0),
        "post_date": str(row["post_date"] or ""),
        "post_time": str(row["post_time"] or ""),
        "scheduled_for": str(row["scheduled_for"] or ""),
        "platforms": platforms,
        "post_type": str(row["post_type"] or ""),
        "topic": str(row["topic"] or ""),
        "hook": str(row["hook"] or ""),
        "caption": str(row["caption"] or ""),
        "reel_script": str(row["reel_script"] or ""),
        "visual_prompt": str(row["visual_prompt"] or ""),
        "canva_design_link": str(row["canva_design_link"] or ""),
        "asset_filename": str(row["asset_filename"] or ""),
        "cta": str(row["cta"] or ""),
        "hashtags_text": hashtags_text,
        "hashtags": hashtags,
        "status": str(row["status"] or "draft"),
        "source_file": str(row["source_file"] or ""),
        "scheduler_external_id": str(row["scheduler_external_id"] or ""),
        "last_publish_error": str(row["last_publish_error"] or ""),
        "created_by": str(row["created_by"] or ""),
        "approved_by": str(row["approved_by"] or ""),
        "approved_at": str(row["approved_at"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def normalize_content_post_input(item: dict, week_number: int, actor: str, source_file: str) -> dict:
    platforms = item.get("platforms")
    if isinstance(platforms, str):
        platforms = [part.strip() for part in platforms.split(",") if part.strip()]
    if not isinstance(platforms, list):
        platforms = []
    hashtags = item.get("hashtags")
    hashtags_text = ""
    if isinstance(hashtags, list):
        hashtags_text = " ".join([str(tag).strip() for tag in hashtags if str(tag).strip()])
    elif isinstance(hashtags, str):
        hashtags_text = hashtags.strip()
    else:
        hashtags_text = str(item.get("hashtags_text") or "").strip()

    post_id = str(item.get("post_id") or "").strip()
    post_date = str(item.get("post_date") or "").strip()
    if not post_id:
        day = str(item.get("day") or "").strip() or "0"
        post_id = f"W{week_number}D{day}"
    return {
        "post_id": post_id,
        "week_number": int(item.get("week_number") or week_number or 0),
        "day": int(item.get("day") or 0),
        "post_date": post_date,
        "post_time": str(item.get("post_time") or "").strip(),
        "scheduled_for": str(item.get("scheduled_for") or "").strip(),
        "platforms_json": json.dumps(platforms),
        "post_type": str(item.get("post_type") or "").strip(),
        "topic": str(item.get("topic") or "").strip(),
        "hook": str(item.get("hook") or "").strip(),
        "caption": str(item.get("caption") or "").strip(),
        "reel_script": str(item.get("reel_script") or "").strip(),
        "visual_prompt": str(item.get("visual_prompt") or "").strip(),
        "canva_design_link": str(item.get("canva_design_link") or item.get("canvaDesignLink") or "").strip(),
        "asset_filename": str(item.get("asset_filename") or "").strip(),
        "cta": str(item.get("cta") or "").strip(),
        "hashtags_text": hashtags_text,
        "status": str(item.get("status") or "draft").strip() or "draft",
        "source_file": source_file,
        "created_by": actor,
    }


def _remove_hashtag_only_lines(caption: str) -> str:
    lines = str(caption or "").splitlines()
    kept = []
    for line in lines:
        trimmed = line.strip()
        if not trimmed:
            kept.append(line)
            continue
        parts = [part for part in trimmed.split() if part]
        if parts and all(part.startswith("#") for part in parts):
            continue
        kept.append(line)
    return "\n".join(kept).strip()


def _hashtags_text_from_caption(caption: str) -> str:
    tags = []
    for token in str(caption or "").replace("\n", " ").split(" "):
        token = token.strip()
        if token.startswith("#") and len(token) > 1:
            tags.append(token)
    return " ".join(tags)


def _platform_slug(platform: str) -> str:
    p = str(platform or "").strip().lower()
    if p == "instagram":
        return "ig"
    if p == "facebook":
        return "fb"
    if p == "tiktok":
        return "tt"
    return (p[:2] or "na")


def convert_buffer_rows_to_content_posts(rows: list[dict], media_links_map: dict[str, str] | None = None) -> list[dict]:
    valid_rows = [row for row in rows if isinstance(row, dict) and _trim(row.get("day")) and _trim(row.get("platform"))]
    if not valid_rows:
        return []
    media_links_map = media_links_map or {}

    unique_dates = sorted({_trim(row.get("day")) for row in valid_rows if _trim(row.get("day"))})
    base_date = None
    if unique_dates:
        base_date = datetime.fromisoformat(f"{unique_dates[0]}T00:00:00")

    content_posts = []
    for row in valid_rows:
        post_date = _trim(row.get("day"))
        post_time = _trim(row.get("time")) or "09:00"
        platform = _trim(row.get("platform"))
        caption_raw = _trim(row.get("caption"))
        caption_clean = _remove_hashtag_only_lines(caption_raw)
        hashtags_text = _hashtags_text_from_caption(caption_raw)
        hook = ""
        for line in caption_clean.splitlines():
            if _trim(line):
                hook = _trim(line)
                break

        ordinal_day = 1
        if base_date and post_date:
            current_date = datetime.fromisoformat(f"{post_date}T00:00:00")
            ordinal_day = int((current_date - base_date).days) + 1
            if ordinal_day < 1:
                ordinal_day = 1
        week_number = int((ordinal_day - 1) // 7) + 1
        day_in_week = int((ordinal_day - 1) % 7) + 1
        media_url = resolve_media_url(
            _trim(row.get("media_url")),
            week_number,
            day_in_week,
            platform.lower(),
            media_links_map,
            allow_auto_drive_lookup=False,
        )

        content_posts.append(
            {
                "post_id": f"W{week_number}D{day_in_week}-{_platform_slug(platform)}",
                "week_number": week_number,
                "day": day_in_week,
                "post_date": post_date,
                "post_time": post_time,
                "platforms": [platform.lower()] if platform else [],
                "post_type": "reel" if platform.lower() == "tiktok" else "social",
                "topic": hook[:90],
                "hook": hook[:140],
                "caption": caption_clean,
                "reel_script": "",
                "visual_prompt": "",
                "asset_filename": media_url,
                "cta": "Visit insuredbylena.com for a 100% free quote comparison. Comment GUIDE and I'll DM you the 2026 Insurance Planning Checklist.",
                "hashtags_text": hashtags_text,
                "status": "draft",
            }
        )
    return content_posts


def upsert_content_posts(conn: sqlite3.Connection, posts: list[dict], week_number: int, actor: str, source_file: str) -> tuple[int, int]:
    imported = 0
    updated = 0
    for raw_item in posts:
        if not isinstance(raw_item, dict):
            continue
        item = normalize_content_post_input(raw_item, week_number, actor, source_file)
        existing = conn.execute(
            "SELECT id FROM content_post WHERE post_id = ? AND post_date = ? LIMIT 1",
            (item["post_id"], item["post_date"]),
        ).fetchone()
        if existing:
            content_post_id = int(existing["id"])
            create_content_revision(conn, content_post_id, actor, "import_update")
            conn.execute(
                """
                UPDATE content_post
                SET week_number = ?,
                    day = ?,
                    post_time = ?,
                    platforms_json = ?,
                    post_type = ?,
                    topic = ?,
                    hook = ?,
                    caption = ?,
                    reel_script = ?,
                    visual_prompt = ?,
                    canva_design_link = ?,
                    asset_filename = ?,
                    cta = ?,
                    hashtags_text = ?,
                    source_file = COALESCE(NULLIF(?, ''), source_file),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    item["week_number"],
                    item["day"],
                    item["post_time"],
                    item["platforms_json"],
                    item["post_type"],
                    item["topic"],
                    item["hook"],
                    item["caption"],
                    item["reel_script"],
                    item["visual_prompt"],
                    item["canva_design_link"],
                    item["asset_filename"],
                    item["cta"],
                    item["hashtags_text"],
                    item["source_file"],
                    content_post_id,
                ),
            )
            updated += 1
        else:
            conn.execute(
                """
                INSERT INTO content_post (
                  post_id, week_number, day, post_date, post_time, scheduled_for,
                  platforms_json, post_type, topic, hook, caption, reel_script,
                  visual_prompt, canva_design_link, asset_filename, cta, hashtags_text, status, source_file, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["post_id"],
                    item["week_number"],
                    item["day"],
                    item["post_date"],
                    item["post_time"],
                    item["scheduled_for"],
                    item["platforms_json"],
                    item["post_type"],
                    item["topic"],
                    item["hook"],
                    item["caption"],
                    item["reel_script"],
                    item["visual_prompt"],
                    item["canva_design_link"],
                    item["asset_filename"],
                    item["cta"],
                    item["hashtags_text"],
                    item["status"],
                    item["source_file"],
                    item["created_by"],
                ),
            )
            imported += 1
    return imported, updated


def create_content_revision(conn: sqlite3.Connection, content_post_id: int, changed_by: str, note: str) -> None:
    row = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
    if not row:
        return
    revision_number = int(
        conn.execute(
            "SELECT COALESCE(MAX(revision_number), 0) + 1 FROM content_revision WHERE content_post_id = ?",
            (content_post_id,),
        ).fetchone()[0]
    )
    snapshot = row_to_content_post(row)
    conn.execute(
        """
        INSERT INTO content_revision (
          content_post_id, revision_number, changed_by, change_note, snapshot_json
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (content_post_id, revision_number, changed_by, note, json.dumps(snapshot)),
    )


def publish_to_scheduler(payload: dict) -> dict:
    if not CONTENT_SCHEDULER_WEBHOOK_URL:
        raise RuntimeError("CONTENT_SCHEDULER_WEBHOOK_URL not configured")
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if CONTENT_SCHEDULER_API_KEY:
        headers["Authorization"] = f"Bearer {CONTENT_SCHEDULER_API_KEY}"
    req = urlrequest.Request(
        CONTENT_SCHEDULER_WEBHOOK_URL,
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=20) as response:
            raw = response.read().decode("utf-8")
            if not raw.strip():
                return {"ok": True}
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                return {"ok": True, "raw": raw}
            except Exception:
                return {"ok": True, "raw": raw}
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"scheduler_http_{exc.code}: {detail or exc.reason}") from exc
    except urlerror.URLError as exc:
        raise RuntimeError(f"scheduler_unreachable: {exc}") from exc


def _http_json_request(url: str, payload: dict, headers: dict[str, str], timeout: float = 25.0) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(url, data=body, headers=headers, method="POST")
    try:
        with urlrequest.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw.strip() else {}
            if not isinstance(parsed, dict):
                return {"raw": parsed}
            return parsed
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"http_{exc.code}: {detail or exc.reason}") from exc
    except urlerror.URLError as exc:
        raise RuntimeError(f"unreachable: {exc}") from exc


def buffer_graphql_request(query: str, variables: dict | None = None) -> dict:
    if not BUFFER_API_KEY:
        raise RuntimeError("BUFFER_API_KEY not configured")
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    response = _http_json_request(
        BUFFER_API_BASE_URL,
        payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {BUFFER_API_KEY}",
            "Accept": "application/json",
            "User-Agent": "InsuredByLenaPublisher/1.0",
        },
        timeout=30.0,
    )
    errors = response.get("errors")
    if isinstance(errors, list) and errors:
        messages = []
        for item in errors:
            if isinstance(item, dict) and _trim(item.get("message")):
                messages.append(_trim(item.get("message")))
        raise RuntimeError("; ".join(messages) or "buffer_graphql_error")
    return response


def buffer_fetch_organizations() -> list[dict]:
    query = """
    query GetOrganizations {
      account {
        organizations {
          id
          name
          ownerEmail
        }
      }
    }
    """
    response = buffer_graphql_request(query)
    orgs = (((response.get("data") or {}).get("account") or {}).get("organizations") or [])
    return [org for org in orgs if isinstance(org, dict)]


def buffer_fetch_channels(organization_id: str) -> list[dict]:
    if not _trim(organization_id):
        raise RuntimeError("BUFFER_ORGANIZATION_ID not configured")
    query = """
    query GetChannels($organizationId: OrganizationId!) {
      channels(input: { organizationId: $organizationId }) {
        id
        name
        displayName
        service
        avatar
        isQueuePaused
      }
    }
    """
    response = buffer_graphql_request(query, {"organizationId": organization_id})
    channels = ((response.get("data") or {}).get("channels") or [])
    return [channel for channel in channels if isinstance(channel, dict)]


def _buffer_autodiscovered_channel_map() -> dict[str, str]:
    global _BUFFER_CHANNEL_CACHE
    if _BUFFER_CHANNEL_CACHE is not None:
        return _BUFFER_CHANNEL_CACHE
    explicit = _buffer_channel_env_map()
    if all(explicit.values()) or not (BUFFER_API_KEY and BUFFER_ORGANIZATION_ID):
        _BUFFER_CHANNEL_CACHE = explicit
        return explicit

    discovered = dict(explicit)
    channels = buffer_fetch_channels(BUFFER_ORGANIZATION_ID)
    for channel in channels:
        service = _trim(channel.get("service")).lower()
        channel_id = _trim(channel.get("id"))
        if not channel_id:
            continue
        if service in discovered and not discovered[service]:
            discovered[service] = channel_id
    _BUFFER_CHANNEL_CACHE = discovered
    return discovered


def buffer_channel_id_for_platform(platform: str) -> str:
    platform_key = _trim(platform).lower()
    channel_id = _trim(_buffer_autodiscovered_channel_map().get(platform_key))
    if channel_id:
        return channel_id
    raise RuntimeError(f"buffer_channel_not_configured_for_platform: {platform_key}")


def _iso_with_timezone(dt_raw: str, fallback_date: str = "", fallback_time: str = "") -> str:
    raw = _trim(dt_raw)
    if not raw and _trim(fallback_date):
        time_part = _trim(fallback_time) or "09:00"
        raw = f"{_trim(fallback_date)}T{time_part}:00"
    if not raw:
        return ""
    dt = _parse_local_iso(raw)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _buffer_assets_input(asset_url: str) -> dict:
    url = _trim(asset_url)
    if not url:
        return {}
    mime_type = _mime_type_for_url(url)
    if mime_type.startswith("video/"):
        return {"videos": [{"url": url}]}
    return {"images": [{"url": url}]}


def _to_graphql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(_to_graphql_literal(item) for item in value) + "]"
    if isinstance(value, dict):
        parts = [f"{key}: {_to_graphql_literal(item)}" for key, item in value.items()]
        return "{ " + ", ".join(parts) + " }"
    return json.dumps(str(value))


def publish_to_buffer_graphql(payload: dict) -> dict:
    platforms = payload.get("platforms") or []
    platform = ""
    if isinstance(platforms, list) and platforms:
        platform = _trim(platforms[0]).lower()
    if not platform:
        raise RuntimeError("buffer_publish_requires_single_platform")
    channel_id = buffer_channel_id_for_platform(platform)
    due_at = _iso_with_timezone(
        _trim(payload.get("scheduled_for")),
        fallback_date=_trim(payload.get("post_date")),
        fallback_time=_trim(payload.get("post_time")),
    )
    mode = "customScheduled" if due_at else "addToQueue"
    fields = [
        'text: %s' % json.dumps(_trim(payload.get("caption"))),
        'channelId: %s' % json.dumps(channel_id),
        "schedulingType: automatic",
        f"mode: {mode}",
        'source: "insuredbylena-content-studio"',
        "aiAssisted: true",
    ]
    metadata_literal = ""
    raw_post_type = _trim(payload.get("post_type")).lower()
    normalized_type = "post"
    if raw_post_type == "reel":
        normalized_type = "reel"
    elif raw_post_type == "story":
        normalized_type = "story"
    if platform == "instagram":
        metadata_literal = f"{{ instagram: {{ type: {normalized_type}, shouldShareToFeed: true }} }}"
    elif platform == "facebook":
        metadata_literal = f"{{ facebook: {{ type: {normalized_type} }} }}"
    if due_at:
        fields.append('dueAt: %s' % json.dumps(due_at))
    if metadata_literal:
        fields.append(f"metadata: {metadata_literal}")
    assets = _buffer_assets_input(_trim(payload.get("asset_filename")))
    if assets:
        fields.append("assets: %s" % _to_graphql_literal(assets))
    mutation = f"""
    mutation CreatePost {{
      createPost(input: {{
        {", ".join(fields)}
      }}) {{
        ... on PostActionSuccess {{
          post {{
            id
            text
            dueAt
            status
            channel {{
              id
              service
            }}
            assets {{
              id
              mimeType
            }}
          }}
        }}
        ... on MutationError {{
          message
        }}
      }}
    }}
    """
    response = buffer_graphql_request(mutation)
    result = ((response.get("data") or {}).get("createPost") or {})
    if not isinstance(result, dict):
        raise RuntimeError("buffer_create_post_invalid_response")
    message = _trim(result.get("message"))
    if message:
        raise RuntimeError(message)
    post = result.get("post") or {}
    if not isinstance(post, dict) or not _trim(post.get("id")):
        raise RuntimeError("buffer_create_post_missing_post_id")
    return {
        "ok": True,
        "id": _trim(post.get("id")),
        "post": post,
        "publisher": "buffer_graphql",
    }


def publisher_status_snapshot() -> dict:
    mode = _publisher_mode()
    snapshot = {
        "mode": mode,
        "webhook_configured": bool(CONTENT_SCHEDULER_WEBHOOK_URL),
        "buffer_api_key_configured": bool(BUFFER_API_KEY),
        "buffer_organization_id": BUFFER_ORGANIZATION_ID,
        "buffer_channels": _buffer_channel_env_map(),
    }
    if mode == "buffer_graphql" and BUFFER_API_KEY:
        try:
            snapshot["buffer_channels"] = _buffer_autodiscovered_channel_map()
        except Exception as exc:
            snapshot["channel_discovery_error"] = str(exc)
    return snapshot


def publish_content_payload(payload: dict) -> dict:
    mode = _publisher_mode()
    if mode == "buffer_graphql":
        return publish_to_buffer_graphql(payload)
    if mode == "webhook":
        return publish_to_scheduler(payload)
    if mode == "mock":
        return {
            "ok": True,
            "id": f"mock-{int(datetime.now(timezone.utc).timestamp())}",
            "publisher": "mock",
        }
    raise RuntimeError(f"unsupported_publisher_mode: {mode}")


def get_agent_carrier_config_rows(conn: sqlite3.Connection) -> list[dict]:
    ensure_agent_carrier_config_table(conn)
    rows = conn.execute(
        """
        SELECT id, carrier_name, writing_number, portal_url, support_phone
        FROM agent_carrier_config
        ORDER BY carrier_name
        """
    ).fetchall()
    return [
        {
            "id": int(row[0]),
            "carrier_name": str(row[1] or ""),
            "writing_number": str(row[2] or ""),
            "portal_url": str(row[3] or ""),
            "support_phone": str(row[4] or ""),
        }
        for row in rows
    ]


def save_agent_carrier_config_rows(conn: sqlite3.Connection, rows: list[dict]) -> int:
    ensure_agent_carrier_config_table(conn)
    saved = 0
    for row in rows:
        carrier_name = _trim(row.get("carrier_name") or row.get("carrierName"))
        if not carrier_name:
            continue
        conn.execute(
            """
            INSERT INTO agent_carrier_config (
              carrier_name, writing_number, portal_url, support_phone
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(carrier_name) DO UPDATE SET
              writing_number=excluded.writing_number,
              portal_url=excluded.portal_url,
              support_phone=excluded.support_phone
            """,
            (
                carrier_name,
                _trim(row.get("writing_number") or row.get("writingNumber")),
                _trim(row.get("portal_url") or row.get("portalUrl")),
                _trim(row.get("support_phone") or row.get("supportPhone")),
            ),
        )
        saved += 1
    return saved


def import_rows(conn: sqlite3.Connection, rows: list[dict]) -> dict:
    added = 0
    skipped_existing = 0
    skipped_invalid = 0
    for row in rows:
        lead_external_id = _nv(row.get("lead_external_id"))
        if not lead_external_id:
            skipped_invalid += 1
            continue
        payload = {
            "lead_external_id": lead_external_id,
            "first_name": _nv(row.get("first_name")),
            "last_name": _nv(row.get("last_name")),
            "full_name": _nv(row.get("full_name")),
            "email": _nv(row.get("email")),
            "mobile_phone": _nv(row.get("mobile_phone")),
            "business_name": _nv(row.get("business_name")),
            "lead_source": _nv(row.get("lead_source")),
            "lead_source_detail": _nv(row.get("lead_source_detail")),
            "campaign_name": _nv(row.get("campaign_name")),
            "product_interest": _nv(row.get("product_interest")),
            "product_line": _nv(row.get("product_line")),
            "owner_queue": _nv(row.get("owner_queue")),
            "lead_status": _nv(row.get("lead_status")),
            "booking_status": _nv(row.get("booking_status")),
            "consent_status": _nv(row.get("consent_status")),
            "consent_channel_sms": _nv(row.get("consent_channel_sms")),
            "consent_channel_email": _nv(row.get("consent_channel_email")),
            "consent_channel_whatsapp": _nv(row.get("consent_channel_whatsapp")),
            "dnc_status": _nv(row.get("dnc_status")),
            "contact_eligibility": _nv(row.get("contact_eligibility")),
            "created_at_source": _nv(row.get("created_at_source")),
            "last_activity_at_source": _nv(row.get("last_activity_at_source")),
            "notes": _nv(row.get("notes")),
            "raw_tags": _nv(row.get("raw_tags")),
            "routing_bucket": _nv(row.get("routing_bucket")),
            "suppress_reason": _nv(row.get("suppress_reason")),
            "recommended_channel": _nv(row.get("recommended_channel")),
            "sequence_name": _nv(row.get("sequence_name")),
            "recommended_next_action": _nv(row.get("recommended_next_action")),
            "priority_tier": _nv(row.get("priority_tier")),
            # Expanded schema defaults for new imports.
            "age": _nv(row.get("age")),
            "tobacco": _nv(row.get("tobacco")),
            "health_posture": _nv(row.get("health_posture")),
            "disposition": _nv(row.get("disposition")),
            "carrier_match": _nv(row.get("carrier_match")),
            "confidence": _nv(row.get("confidence")),
            "pipeline_status": _nv(row.get("pipeline_status") or row.get("pipelineStatus")),
            "calendar_event_id": _nv(row.get("calendar_event_id") or row.get("calendarEventId")),
            "next_appointment_time": _nv(row.get("next_appointment_time") or row.get("nextAppointmentTime")),
            "last_opened_at": _nv(row.get("last_opened_at") or row.get("lastOpenedAt")),
        }
        cur = conn.execute(
            """
            INSERT INTO lead_master (
              lead_external_id, first_name, last_name, full_name, email, mobile_phone, business_name,
              lead_source, lead_source_detail, campaign_name, product_interest, product_line,
              owner_queue, lead_status, booking_status, consent_status, consent_channel_sms,
              consent_channel_email, consent_channel_whatsapp, dnc_status, contact_eligibility,
              created_at_source, last_activity_at_source, notes, raw_tags, routing_bucket,
              suppress_reason, recommended_channel, sequence_name, recommended_next_action, priority_tier,
              age, tobacco, health_posture, disposition, carrier_match, confidence, pipeline_status,
              calendar_event_id, next_appointment_time, last_opened_at
            ) VALUES (
              :lead_external_id, :first_name, :last_name, :full_name, :email, :mobile_phone, :business_name,
              :lead_source, :lead_source_detail, :campaign_name, :product_interest, :product_line,
              :owner_queue, :lead_status, :booking_status, :consent_status, :consent_channel_sms,
              :consent_channel_email, :consent_channel_whatsapp, :dnc_status, :contact_eligibility,
              :created_at_source, :last_activity_at_source, :notes, :raw_tags, :routing_bucket,
              :suppress_reason, :recommended_channel, :sequence_name, :recommended_next_action, :priority_tier,
              :age, :tobacco, :health_posture, :disposition, :carrier_match, :confidence, :pipeline_status,
              :calendar_event_id, :next_appointment_time, :last_opened_at
            )
            ON CONFLICT(lead_external_id) DO NOTHING
            """,
            payload,
        )
        if cur.rowcount == 1:
            added += 1
        else:
            skipped_existing += 1

    return {
        "added": added,
        "skipped_existing": skipped_existing,
        "skipped_invalid": skipped_invalid,
    }


def ensure_lead_exists(conn: sqlite3.Connection, lead_external_id: str, payload: dict) -> int:
    if lead_external_id:
        row = conn.execute(
            "SELECT lead_id FROM lead_master WHERE lead_external_id=? LIMIT 1",
            (lead_external_id,),
        ).fetchone()
        if row:
            return int(row[0])

    conn.execute(
        """
        INSERT INTO lead_master (
          lead_external_id, first_name, last_name, full_name, email, mobile_phone,
          lead_source, lead_source_detail, campaign_name, owner_queue, lead_status,
          contact_eligibility, notes, raw_tags, recommended_channel, recommended_next_action,
          age, tobacco, health_posture, disposition, carrier_match, confidence, pipeline_status,
          calendar_event_id, next_appointment_time, last_opened_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["lead_external_id"] or f"NEW-{int(datetime.now().timestamp())}",
            payload["first_name"],
            payload["last_name"],
            payload["full_name"],
            payload["email"],
            payload["mobile_phone"],
            "call_desk",
            "local_db_api",
            "Call Desk Sync",
            "call_queue",
            "active",
            "eligible",
            payload["notes"],
            payload["raw_tags"],
            "phone_call",
            "continue call flow",
            payload["age"],
            payload["tobacco"],
            payload["health_posture"],
            payload["disposition"],
            payload["carrier_match"],
            payload["confidence"],
            payload["pipeline_status"],
            payload["calendar_event_id"],
            payload["next_appointment_time"],
            payload["last_opened_at"],
        ),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def update_lead(conn: sqlite3.Connection, lead_id: int, payload: dict) -> None:
    conn.execute(
        """
        UPDATE lead_master
        SET
          first_name = COALESCE(NULLIF(?, ''), first_name),
          last_name = COALESCE(NULLIF(?, ''), last_name),
          full_name = COALESCE(NULLIF(?, ''), full_name),
          mobile_phone = COALESCE(NULLIF(?, ''), mobile_phone),
          email = COALESCE(NULLIF(?, ''), email),
          raw_tags = COALESCE(NULLIF(?, ''), raw_tags),
          notes = COALESCE(NULLIF(?, ''), notes),
          last_activity_at_source = COALESCE(NULLIF(?, ''), last_activity_at_source),
          age = COALESCE(NULLIF(?, ''), age),
          tobacco = COALESCE(NULLIF(?, ''), tobacco),
          health_posture = COALESCE(NULLIF(?, ''), health_posture),
          disposition = COALESCE(NULLIF(?, ''), disposition),
          carrier_match = COALESCE(NULLIF(?, ''), carrier_match),
          confidence = COALESCE(NULLIF(?, ''), confidence),
          pipeline_status = COALESCE(NULLIF(?, ''), pipeline_status),
          calendar_event_id = COALESCE(NULLIF(?, ''), calendar_event_id),
          next_appointment_time = COALESCE(NULLIF(?, ''), next_appointment_time),
          last_opened_at = COALESCE(NULLIF(?, ''), last_opened_at),
          updated_at = CURRENT_TIMESTAMP
        WHERE lead_id = ?
        """,
        (
            payload["first_name"],
            payload["last_name"],
            payload["full_name"],
            payload["mobile_phone"],
            payload["email"],
            payload["raw_tags"],
            payload["notes"],
            payload["last_activity_at_source"],
            payload["age"],
            payload["tobacco"],
            payload["health_posture"],
            payload["disposition"],
            payload["carrier_match"],
            payload["confidence"],
            payload["pipeline_status"],
            payload["calendar_event_id"],
            payload["next_appointment_time"],
            payload["last_opened_at"],
            lead_id,
        ),
    )


class LeadSyncHandler(BaseHTTPRequestHandler):
    def _set_headers(self, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        self._set_headers(status)
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def _send_error(self, message: str, status: int = 500) -> None:
        self._send_json({"ok": False, "error": str(message)}, status=status)

    def _send_not_found(self) -> None:
        self._send_error("not_found", status=404)

    def _path_parts(self) -> list[str]:
        return [part for part in urlparse(self.path).path.split("/") if part]

    def do_OPTIONS(self) -> None:
        self._set_headers(204)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        exact_routes = {
            "/api/content/publisher/test": self._handle_content_publisher_test,
            "/api/content/posts/import": self._handle_content_posts_import,
            "/api/content/posts/import-buffer-current": self._handle_content_posts_import_buffer_current,
            "/api/content/publish/run": self._handle_content_publish_run,
            "/api/leads/import": self._handle_import,
            "/api/admin/purge-test-data": self._handle_purge_test_data,
            "/api/calendar/schedule": self._handle_calendar_schedule,
            "/api/google-calendar/sync": self._handle_google_calendar_sync,
            "/api/carrier-config": self._handle_carrier_config_save,
            "/api/leads/sync": self._handle_sync,
        }
        handler = exact_routes.get(parsed.path)
        if handler:
            handler()
            return
        parts = self._path_parts()
        if len(parts) == 5 and parts[:3] == ["api", "content", "posts"] and parts[4] == "restore":
            self._handle_content_post_restore(parts[3])
            return
        if len(parts) == 5 and parts[:3] == ["api", "content", "posts"]:
            self._handle_content_post_action(parts[3], parts[4])
            return
        if len(parts) == 4 and parts[:2] == ["api", "leads"] and parts[3] == "documents":
            self._handle_lead_documents_post(parts[2])
            return
        if len(parts) == 4 and parts[:2] == ["api", "lead-documents"] and parts[3] == "archive":
            self._handle_lead_document_archive(parts[2])
            return
        self._send_not_found()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        exact_routes = {
            "/api/health": self._handle_api_health_get,
            "/api/content/publisher/status": self._handle_content_publisher_status_get,
            "/api/content/posts": lambda: self._handle_content_posts_get(parsed.query),
            "/api/content/publish/jobs": lambda: self._handle_content_publish_jobs_get(parsed.query),
            "/api/carrier-config": self._handle_carrier_config_get,
            "/api/calendar/today": self._handle_calendar_today,
            "/api/calendar/week": self._handle_calendar_week,
        }
        handler = exact_routes.get(parsed.path)
        if handler:
            handler()
            return
        parts = self._path_parts()
        if len(parts) == 5 and parts[:3] == ["api", "content", "posts"] and parts[4] == "revisions":
            self._handle_content_post_revisions_get(parts[3])
            return
        if len(parts) == 4 and parts[:2] == ["api", "leads"] and parts[3] == "documents":
            self._handle_lead_documents_get(parts[2])
            return
        self._send_not_found()

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        parts = self._path_parts()
        if len(parts) == 4 and parts[:3] == ["api", "content", "posts"]:
            self._handle_content_post_update(parts[3])
            return
        if not parsed.path.startswith("/api/leads/"):
            self._send_not_found()
            return
        if len(parts) == 4 and parts[:2] == ["api", "leads"] and parts[3] == "pipeline":
            self._handle_pipeline_update(parts[2])
            return
        if len(parts) == 4 and parts[:2] == ["api", "leads"] and parts[3] == "open":
            self._handle_open_update(parts[2])
            return
        # PUT /api/leads/:id also flows through the same sync logic.
        self._handle_sync(force_external_id=parsed.path.rsplit("/", 1)[-1])

    def _handle_sync(self, force_external_id: str = "") -> None:
        try:
            data = self._read_json_body()
            if force_external_id and not data.get("contactId"):
                data["contactId"] = force_external_id
            if _use_supabase_backend():
                result = _supabase_portal_save_call_desk(_lead_sync_payload_from_request(data, force_external_id))
                lead = result.get("lead") if isinstance(result, dict) else {}
                self._send_json(
                    {
                        "ok": True,
                        "leadId": int((lead or {}).get("lead_id") or 0),
                        "lead": lead,
                        "appointmentId": result.get("appointmentId"),
                        "scheduledInternally": bool(result.get("scheduledInternally")),
                    }
                )
                return
            payload = normalize_payload(data)

            with connect_db() as conn:
                ensure_lead_master_columns(conn)
                lead_id = ensure_lead_exists(conn, payload["lead_external_id"], payload)
                update_lead(conn, lead_id, payload)
                conn.commit()
            mirror_db_to_drive()

            self._send_json({"ok": True, "leadId": lead_id})
        except Exception as exc:  # pragma: no cover - defensive handler
            self._send_error(str(exc))

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            raise ValueError("Expected a JSON object body")
        return data

    def _handle_content_posts_get(self, query: str) -> None:
        try:
            params = parse_qs(query or "")
            status_filter = _trim((params.get("status") or [""])[0])
            if _use_supabase_backend():
                path = "content_post?select=*&order=scheduled_for.desc.nullslast,post_date.desc,post_time.desc,id.desc"
                if status_filter:
                    path += f"&status=eq.{quote(status_filter, safe='')}"
                rows = _supabase_rest(path, method="GET") or []
                posts = [_supabase_content_record_to_post(row) for row in rows if isinstance(row, dict)]
            else:
                with connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    ensure_content_studio_tables(conn)
                    if status_filter:
                        rows = conn.execute(
                            """
                            SELECT * FROM content_post
                            WHERE status = ?
                            ORDER BY COALESCE(NULLIF(scheduled_for, ''), post_date || 'T' || post_time) DESC, id DESC
                            """,
                            (status_filter,),
                        ).fetchall()
                    else:
                        rows = conn.execute(
                            """
                            SELECT * FROM content_post
                            ORDER BY COALESCE(NULLIF(scheduled_for, ''), post_date || 'T' || post_time) DESC, id DESC
                            """
                        ).fetchall()
                posts = [row_to_content_post(row) for row in rows]
            self._send_json({"ok": True, "items": posts, "posts": posts, "count": len(posts)})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_api_health_get(self) -> None:
        try:
            response = {
                "ok": True,
                "service": "insuredbylena-portal-api",
                "content_store_mode": _content_store_mode(),
                "publisher_mode": _publisher_mode(),
                "supabase_configured": bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY),
                "buffer_configured": bool(BUFFER_API_KEY),
            }
            self._send_json(response)
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_content_publisher_status_get(self) -> None:
        try:
            snapshot = publisher_status_snapshot()
            if BUFFER_API_KEY:
                try:
                    snapshot["organizations"] = buffer_fetch_organizations()
                except Exception as exc:
                    snapshot["organizations_error"] = str(exc)
                if BUFFER_ORGANIZATION_ID:
                    try:
                        snapshot["available_channels"] = buffer_fetch_channels(BUFFER_ORGANIZATION_ID)
                    except Exception as exc:
                        snapshot["available_channels_error"] = str(exc)
            self._send_json({"ok": True, "content_store_mode": _content_store_mode(), **snapshot})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_content_publisher_test(self) -> None:
        try:
            data = self._read_json_body()
            mode = _publisher_mode()
            response = {"mode": mode}
            if mode == "buffer_graphql":
                response["organizations"] = buffer_fetch_organizations()
                if BUFFER_ORGANIZATION_ID:
                    response["channels"] = buffer_fetch_channels(BUFFER_ORGANIZATION_ID)
            elif mode == "webhook":
                response["webhook_url"] = CONTENT_SCHEDULER_WEBHOOK_URL
            else:
                response["message"] = "mock publisher active"
            self._send_json({"ok": True, **response})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_content_posts_import(self) -> None:
        try:
            data = self._read_json_body()
            posts = data.get("posts") or data.get("rows") or []
            if not isinstance(posts, list):
                raise ValueError("'posts' must be an array")
            week_number = int(data.get("week_number") or data.get("weekNumber") or 0)
            actor = _trim(data.get("actor")) or "portal"
            source_file = _trim(data.get("source_file") or data.get("sourceFile"))
            if _use_supabase_backend():
                imported, updated = _supabase_content_import_posts(posts, week_number, actor, source_file)
            else:
                with connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    ensure_content_studio_tables(conn)
                    imported, updated = upsert_content_posts(conn, posts, week_number, actor, source_file)
                    conn.commit()
                mirror_db_to_drive()
            self._send_json({"ok": True, "imported": imported, "updated": updated})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_content_posts_import_buffer_current(self) -> None:
        try:
            data = self._read_json_body()
            actor = _trim(data.get("actor")) or "automation"
            source_file = _trim(data.get("source_file") or data.get("sourceFile")) or "buffer-import.json"
            requested_path = _trim(data.get("path") or data.get("file"))
            file_path = Path(requested_path) if requested_path else BUFFER_IMPORT_DEFAULT_PATH
            if not file_path.is_absolute():
                file_path = CONTENT_FILES_ROOT / file_path
            file_path = file_path.resolve()
            if CONTENT_FILES_ROOT not in file_path.parents and file_path != CONTENT_FILES_ROOT:
                raise ValueError("buffer_import_path_must_be_inside_content_root")
            if not file_path.exists():
                raise FileNotFoundError(f"buffer_import_file_not_found: {file_path}")

            parsed = json.loads(file_path.read_text(encoding="utf-8"))
            if not isinstance(parsed, list):
                raise ValueError("buffer_import_json_must_be_array")
            media_links_map = load_media_links_map()
            posts = convert_buffer_rows_to_content_posts(parsed, media_links_map=media_links_map)
            if not posts:
                raise ValueError("buffer_import_json_has_no_valid_rows")

            if _use_supabase_backend():
                imported, updated = _supabase_content_import_posts(posts, week_number=0, actor=actor, source_file=source_file)
            else:
                with connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    ensure_content_studio_tables(conn)
                    imported, updated = upsert_content_posts(conn, posts, week_number=0, actor=actor, source_file=source_file)
                    conn.commit()
                mirror_db_to_drive()
            self._send_json(
                {
                    "ok": True,
                    "imported": imported,
                    "updated": updated,
                    "file": str(file_path),
                    "rows": len(posts),
                    "media_links_loaded": len(media_links_map),
                }
            )
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_content_post_update(self, post_id_raw: str) -> None:
        try:
            content_post_id = int(post_id_raw)
            data = self._read_json_body()
            actor = _trim(data.get("actor")) or "editor"
            if _use_supabase_backend():
                current = _supabase_content_get_post(content_post_id)
                if not current:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                _supabase_content_insert_revision(content_post_id, actor, "manual_edit", _content_snapshot_from_post(current))
                platforms = data.get("platforms")
                if isinstance(platforms, str):
                    platforms = [part.strip() for part in platforms.split(",") if part.strip()]
                if not isinstance(platforms, list):
                    platforms = current.get("platforms") or []
                hashtags_text = _trim(data.get("hashtags_text") or data.get("hashtagsText"))
                if not hashtags_text:
                    hashtags = data.get("hashtags")
                    if isinstance(hashtags, list):
                        hashtags_text = " ".join([str(tag).strip() for tag in hashtags if str(tag).strip()])
                if not hashtags_text:
                    hashtags_text = current.get("hashtags_text") or ""
                payload = {
                    "post_date": _trim(data.get("post_date") or data.get("postDate")) or current.get("post_date") or None,
                    "post_time": _trim(data.get("post_time") or data.get("postTime")) or current.get("post_time") or None,
                    "scheduled_for": _trim(data.get("scheduled_for") or data.get("scheduledFor")) or current.get("scheduled_for") or None,
                    "platforms_json": platforms,
                    "post_type": _trim(data.get("post_type") or data.get("postType")) or current.get("post_type") or None,
                    "topic": _trim(data.get("topic")) or current.get("topic") or None,
                    "hook": _trim(data.get("hook")) or current.get("hook") or None,
                    "caption": _trim(data.get("caption")) or current.get("caption") or None,
                    "reel_script": _trim(data.get("reel_script") or data.get("reelScript")) or current.get("reel_script") or None,
                    "visual_prompt": _trim(data.get("visual_prompt") or data.get("visualPrompt")) or current.get("visual_prompt") or None,
                    "canva_design_link": _trim(data.get("canva_design_link") or data.get("canvaDesignLink")),
                    "asset_filename": _trim(data.get("asset_filename") or data.get("assetFilename")) or current.get("asset_filename") or None,
                    "cta": _trim(data.get("cta")) or current.get("cta") or None,
                    "hashtags_text": hashtags_text,
                    "updated_at": now_iso(),
                }
                updated = _supabase_content_patch_post(content_post_id, payload)
                self._set_headers(200)
                self.wfile.write(json.dumps({"ok": True, "item": updated}).encode("utf-8"))
                return
            with connect_db() as conn:
                conn.row_factory = sqlite3.Row
                ensure_content_studio_tables(conn)
                row = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
                if not row:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                create_content_revision(conn, content_post_id, actor, "manual_edit")
                platforms = data.get("platforms")
                if isinstance(platforms, str):
                    platforms = [part.strip() for part in platforms.split(",") if part.strip()]
                if not isinstance(platforms, list):
                    platforms = json.loads(str(row["platforms_json"] or "[]"))
                hashtags_text = _trim(data.get("hashtags_text") or data.get("hashtagsText"))
                if not hashtags_text:
                    hashtags = data.get("hashtags")
                    if isinstance(hashtags, list):
                        hashtags_text = " ".join([str(tag).strip() for tag in hashtags if str(tag).strip()])
                if not hashtags_text:
                    hashtags_text = str(row["hashtags_text"] or "")
                canva_design_link = _trim(data.get("canva_design_link") or data.get("canvaDesignLink"))
                conn.execute(
                    """
                    UPDATE content_post
                    SET post_date = COALESCE(NULLIF(?, ''), post_date),
                        post_time = COALESCE(NULLIF(?, ''), post_time),
                        scheduled_for = COALESCE(NULLIF(?, ''), scheduled_for),
                        platforms_json = ?,
                        post_type = COALESCE(NULLIF(?, ''), post_type),
                        topic = COALESCE(NULLIF(?, ''), topic),
                        hook = COALESCE(NULLIF(?, ''), hook),
                        caption = COALESCE(NULLIF(?, ''), caption),
                        reel_script = COALESCE(NULLIF(?, ''), reel_script),
                        visual_prompt = COALESCE(NULLIF(?, ''), visual_prompt),
                        canva_design_link = ?,
                        asset_filename = COALESCE(NULLIF(?, ''), asset_filename),
                        cta = COALESCE(NULLIF(?, ''), cta),
                        hashtags_text = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        _trim(data.get("post_date") or data.get("postDate")),
                        _trim(data.get("post_time") or data.get("postTime")),
                        _trim(data.get("scheduled_for") or data.get("scheduledFor")),
                        json.dumps(platforms),
                        _trim(data.get("post_type") or data.get("postType")),
                        _trim(data.get("topic")),
                        _trim(data.get("hook")),
                        _trim(data.get("caption")),
                        _trim(data.get("reel_script") or data.get("reelScript")),
                        _trim(data.get("visual_prompt") or data.get("visualPrompt")),
                        canva_design_link,
                        _trim(data.get("asset_filename") or data.get("assetFilename")),
                        _trim(data.get("cta")),
                        hashtags_text,
                        content_post_id,
                    ),
                )
                conn.commit()
                updated = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
            mirror_db_to_drive()
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": row_to_content_post(updated)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))

    def _handle_content_post_revisions_get(self, post_id_raw: str) -> None:
        try:
            content_post_id = int(post_id_raw)
            if _use_supabase_backend():
                rows = _supabase_rest(
                    f"content_revision?select=id,revision_number,changed_by,change_note,created_at&content_post_id=eq.{content_post_id}&order=revision_number.desc,id.desc&limit=100",
                    method="GET",
                ) or []
                items = [
                    {
                        "id": int(row.get("id") or 0),
                        "revision_number": int(row.get("revision_number") or 0),
                        "changed_by": str(row.get("changed_by") or ""),
                        "change_note": str(row.get("change_note") or ""),
                        "created_at": str(row.get("created_at") or ""),
                    }
                    for row in rows
                    if isinstance(row, dict)
                ]
            else:
                with connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    ensure_content_studio_tables(conn)
                    rows = conn.execute(
                        """
                        SELECT id, revision_number, changed_by, change_note, created_at
                        FROM content_revision
                        WHERE content_post_id = ?
                        ORDER BY revision_number DESC, id DESC
                        LIMIT 100
                        """,
                        (content_post_id,),
                    ).fetchall()
                items = [
                    {
                        "id": int(row["id"]),
                        "revision_number": int(row["revision_number"] or 0),
                        "changed_by": str(row["changed_by"] or ""),
                        "change_note": str(row["change_note"] or ""),
                        "created_at": str(row["created_at"] or ""),
                    }
                    for row in rows
                ]
            self._set_headers(200)
            self.wfile.write(
                json.dumps({"ok": True, "items": items, "jobs": items, "count": len(items)}).encode("utf-8")
            )
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))

    def _handle_content_post_restore(self, post_id_raw: str) -> None:
        try:
            content_post_id = int(post_id_raw)
            data = self._read_json_body()
            revision_id = int(data.get("revision_id") or data.get("revisionId") or 0)
            actor = _trim(data.get("actor")) or "editor"
            if revision_id <= 0:
                self._set_headers(400)
                self.wfile.write(json.dumps({"ok": False, "error": "revision_id_required"}).encode("utf-8"))
                return
            if _use_supabase_backend():
                current = _supabase_content_get_post(content_post_id)
                if not current:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                revision_rows = _supabase_rest(
                    f"content_revision?select=snapshot_json&id=eq.{revision_id}&content_post_id=eq.{content_post_id}&limit=1",
                    method="GET",
                ) or []
                if not revision_rows:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "revision_not_found"}).encode("utf-8"))
                    return
                snapshot = revision_rows[0].get("snapshot_json") or {}
                if isinstance(snapshot, str):
                    snapshot = json.loads(snapshot)
                _supabase_content_insert_revision(content_post_id, actor, "restore_before_restore", _content_snapshot_from_post(current))
                updated = _supabase_content_patch_post(
                    content_post_id,
                    {
                        "post_date": _trim(snapshot.get("post_date")) or None,
                        "post_time": _trim(snapshot.get("post_time")) or None,
                        "scheduled_for": _trim(snapshot.get("scheduled_for")) or None,
                        "platforms_json": snapshot.get("platforms") or [],
                        "post_type": _trim(snapshot.get("post_type")) or None,
                        "topic": _trim(snapshot.get("topic")) or None,
                        "hook": _trim(snapshot.get("hook")) or None,
                        "caption": _trim(snapshot.get("caption")) or None,
                        "reel_script": _trim(snapshot.get("reel_script")) or None,
                        "visual_prompt": _trim(snapshot.get("visual_prompt")) or None,
                        "canva_design_link": _trim(snapshot.get("canva_design_link") or snapshot.get("canvaDesignLink")) or None,
                        "asset_filename": _trim(snapshot.get("asset_filename")) or None,
                        "cta": _trim(snapshot.get("cta")) or None,
                        "hashtags_text": _trim(snapshot.get("hashtags_text")) or None,
                        "status": _trim(snapshot.get("status")) or "draft",
                        "updated_at": now_iso(),
                    },
                )
                _supabase_content_insert_revision(content_post_id, actor, f"restored_revision_{revision_id}", _content_snapshot_from_post(updated))
                self._set_headers(200)
                self.wfile.write(json.dumps({"ok": True, "item": updated}).encode("utf-8"))
                return
            with connect_db() as conn:
                conn.row_factory = sqlite3.Row
                ensure_content_studio_tables(conn)
                current = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
                if not current:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                revision_row = conn.execute(
                    "SELECT snapshot_json FROM content_revision WHERE id = ? AND content_post_id = ? LIMIT 1",
                    (revision_id, content_post_id),
                ).fetchone()
                if not revision_row:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "revision_not_found"}).encode("utf-8"))
                    return
                snapshot = json.loads(str(revision_row["snapshot_json"] or "{}"))
                create_content_revision(conn, content_post_id, actor, "restore_before_restore")
                conn.execute(
                    """
                    UPDATE content_post
                    SET post_date = ?,
                        post_time = ?,
                        scheduled_for = ?,
                        platforms_json = ?,
                        post_type = ?,
                        topic = ?,
                        hook = ?,
                        caption = ?,
                        reel_script = ?,
                        visual_prompt = ?,
                        canva_design_link = ?,
                        asset_filename = ?,
                        cta = ?,
                        hashtags_text = ?,
                        status = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        _trim(snapshot.get("post_date")),
                        _trim(snapshot.get("post_time")),
                        _trim(snapshot.get("scheduled_for")),
                        json.dumps(snapshot.get("platforms") or []),
                        _trim(snapshot.get("post_type")),
                        _trim(snapshot.get("topic")),
                        _trim(snapshot.get("hook")),
                        _trim(snapshot.get("caption")),
                        _trim(snapshot.get("reel_script")),
                        _trim(snapshot.get("visual_prompt")),
                        _trim(snapshot.get("canva_design_link") or snapshot.get("canvaDesignLink")),
                        _trim(snapshot.get("asset_filename")),
                        _trim(snapshot.get("cta")),
                        _trim(snapshot.get("hashtags_text")),
                        _trim(snapshot.get("status")) or "draft",
                        content_post_id,
                    ),
                )
                create_content_revision(conn, content_post_id, actor, f"restored_revision_{revision_id}")
                conn.commit()
                updated = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
            mirror_db_to_drive()
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": row_to_content_post(updated)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))

    def _handle_content_post_action(self, post_id_raw: str, action: str) -> None:
        try:
            content_post_id = int(post_id_raw)
            data = self._read_json_body()
            actor = _trim(data.get("actor")) or "approver"
            note = _trim(data.get("note"))
            if _use_supabase_backend():
                current = _supabase_content_get_post(content_post_id)
                if not current:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                if action == "submit-review":
                    updated = _supabase_content_patch_post(
                        content_post_id,
                        {"status": "in_review", "updated_at": now_iso()},
                    )
                    _supabase_content_insert_approval(content_post_id, "submitted", note, actor)
                elif action == "approve":
                    scheduled_for = _trim(data.get("scheduled_for") or data.get("scheduledFor")) or current.get("scheduled_for") or None
                    updated = _supabase_content_patch_post(
                        content_post_id,
                        {
                            "status": "approved",
                            "scheduled_for": scheduled_for,
                            "approved_by": actor,
                            "approved_at": now_iso(),
                            "last_publish_error": None,
                            "updated_at": now_iso(),
                        },
                    )
                    _supabase_content_insert_approval(content_post_id, "approved", note, actor)
                elif action == "request-changes":
                    updated = _supabase_content_patch_post(
                        content_post_id,
                        {"status": "draft", "updated_at": now_iso()},
                    )
                    _supabase_content_insert_approval(content_post_id, "request_changes", note, actor)
                elif action == "schedule":
                    scheduled_for = _trim(data.get("scheduled_for") or data.get("scheduledFor"))
                    if not scheduled_for:
                        row_post_date = _trim(current.get("post_date"))
                        row_post_time = _trim(current.get("post_time")) or "09:00"
                        scheduled_for = f"{row_post_date}T{row_post_time}:00"
                    updated = _supabase_content_patch_post(
                        content_post_id,
                        {"status": "scheduled", "scheduled_for": scheduled_for, "updated_at": now_iso()},
                    )
                    _supabase_content_insert_approval(content_post_id, "scheduled", note, actor)
                else:
                    self._set_headers(400)
                    self.wfile.write(json.dumps({"ok": False, "error": "unsupported_action"}).encode("utf-8"))
                    return
                self._set_headers(200)
                self.wfile.write(json.dumps({"ok": True, "item": updated}).encode("utf-8"))
                return
            with connect_db() as conn:
                conn.row_factory = sqlite3.Row
                ensure_content_studio_tables(conn)
                row = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
                if not row:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                if action == "submit-review":
                    conn.execute(
                        "UPDATE content_post SET status='in_review', updated_at=CURRENT_TIMESTAMP WHERE id = ?",
                        (content_post_id,),
                    )
                    conn.execute(
                        "INSERT INTO content_approval (content_post_id, decision, note, actor) VALUES (?, 'submitted', ?, ?)",
                        (content_post_id, note, actor),
                    )
                elif action == "approve":
                    scheduled_for = _trim(data.get("scheduled_for") or data.get("scheduledFor"))
                    conn.execute(
                        """
                        UPDATE content_post
                        SET status='approved',
                            scheduled_for=COALESCE(NULLIF(?, ''), scheduled_for),
                            approved_by=?,
                            approved_at=?,
                            last_publish_error=NULL,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (scheduled_for, actor, now_iso(), content_post_id),
                    )
                    conn.execute(
                        "INSERT INTO content_approval (content_post_id, decision, note, actor) VALUES (?, 'approved', ?, ?)",
                        (content_post_id, note, actor),
                    )
                elif action == "request-changes":
                    conn.execute(
                        "UPDATE content_post SET status='draft', updated_at=CURRENT_TIMESTAMP WHERE id = ?",
                        (content_post_id,),
                    )
                    conn.execute(
                        "INSERT INTO content_approval (content_post_id, decision, note, actor) VALUES (?, 'request_changes', ?, ?)",
                        (content_post_id, note, actor),
                    )
                elif action == "schedule":
                    scheduled_for = _trim(data.get("scheduled_for") or data.get("scheduledFor"))
                    if not scheduled_for:
                        row_post_date = _trim(row["post_date"])
                        row_post_time = _trim(row["post_time"]) or "09:00"
                        scheduled_for = f"{row_post_date}T{row_post_time}:00"
                    conn.execute(
                        "UPDATE content_post SET status='scheduled', scheduled_for=?, updated_at=CURRENT_TIMESTAMP WHERE id = ?",
                        (scheduled_for, content_post_id),
                    )
                    conn.execute(
                        "INSERT INTO content_approval (content_post_id, decision, note, actor) VALUES (?, 'scheduled', ?, ?)",
                        (content_post_id, note, actor),
                    )
                else:
                    self._set_headers(400)
                    self.wfile.write(json.dumps({"ok": False, "error": "unsupported_action"}).encode("utf-8"))
                    return
                conn.commit()
                updated = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
            mirror_db_to_drive()
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": row_to_content_post(updated)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))

    def _handle_content_publish_run(self) -> None:
        try:
            data = self._read_json_body()
            limit = int(data.get("limit") or 20)
            requested_post_ids: list[int] = []
            raw_post_ids = data.get("postIds")
            if isinstance(raw_post_ids, list):
                for value in raw_post_ids:
                    try:
                        parsed_value = int(value)
                    except (TypeError, ValueError):
                        continue
                    if parsed_value > 0 and parsed_value not in requested_post_ids:
                        requested_post_ids.append(parsed_value)
            if not requested_post_ids:
                try:
                    selected_id = int(data.get("selectedPostId") or 0)
                except (TypeError, ValueError):
                    selected_id = 0
                if selected_id > 0:
                    requested_post_ids.append(selected_id)
            now_dt = datetime.now().astimezone()
            results = []
            sync_result = sync_public_media_links()
            sync_error = ""
            if not sync_result.get("ok") and not sync_result.get("skipped"):
                sync_error = _trim(sync_result.get("error")) or _trim(sync_result.get("reason"))
            media_links_map = load_media_links_map()
            if _use_supabase_backend():
                if requested_post_ids:
                    id_filter = ",".join(str(item) for item in requested_post_ids)
                    rows = _supabase_rest(
                        f"content_post?select=*&id=in.({id_filter})&status=in.(approved,scheduled)&order=scheduled_for.asc.nullsfirst,post_date.asc,post_time.asc,id.asc",
                        method="GET",
                    ) or []
                else:
                    rows = _supabase_rest(
                        f"content_post?select=*&status=in.(approved,scheduled)&order=scheduled_for.asc.nullsfirst,post_date.asc,post_time.asc,id.asc&limit={limit}",
                        method="GET",
                    ) or []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    item = _supabase_content_record_to_post(row)
                    if not _trim(item["scheduled_for"]):
                        results.append(
                            {
                                "id": item["id"],
                                "post_id": item["post_id"],
                                "status": "skipped_missing_schedule",
                                "reason": "scheduled_for_required",
                            }
                        )
                        continue
                    duplicate_reason = ""
                    if _trim(item["scheduler_external_id"]) and str(item.get("status")).lower() == "published":
                        duplicate_reason = "this row already has a scheduler external id"
                    if duplicate_reason:
                        _supabase_content_patch_post(
                            int(item["id"]),
                            {
                                "status": "draft",
                                "last_publish_error": f"duplicate_publish_skipped: {duplicate_reason}",
                                "updated_at": now_iso(),
                            },
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "skipped_duplicate", "reason": duplicate_reason})
                        continue
                    primary_platform = item["platforms"][0] if item["platforms"] else ""
                    resolved_asset = resolve_media_url(
                        item["asset_filename"],
                        int(item.get("week_number") or 0),
                        int(item.get("day") or 0),
                        primary_platform,
                        media_links_map,
                    )
                    _supabase_content_patch_post(int(item["id"]), {"status": "publishing", "last_publish_error": None, "updated_at": now_iso()})
                    if resolved_asset and resolved_asset != item["asset_filename"]:
                        _supabase_content_patch_post(int(item["id"]), {"asset_filename": resolved_asset, "updated_at": now_iso()})
                        item["asset_filename"] = resolved_asset
                    request_payload = {
                        "post_id": item["post_id"],
                        "topic": item["topic"],
                        "post_type": item["post_type"],
                        "caption": item["caption"],
                        "reel_script": item["reel_script"],
                        "visual_prompt": item["visual_prompt"],
                        "asset_filename": item["asset_filename"],
                        "cta": item["cta"],
                        "hashtags": item["hashtags"],
                        "platforms": item["platforms"],
                        "post_date": item["post_date"],
                        "post_time": item["post_time"],
                        "scheduled_for": item["scheduled_for"],
                    }
                    job_id = _supabase_content_insert_publish_job(
                        {
                            "content_post_id": int(item["id"]),
                            "scheduler": _publisher_mode() if _publisher_mode() != "webhook" else CONTENT_SCHEDULER_NAME,
                            "request_json": request_payload,
                            "status": "publishing",
                        }
                    )
                    try:
                        unresolved_id = _extract_drive_file_id(item["asset_filename"])
                        if unresolved_id and _is_placeholder_file_id(unresolved_id):
                            raise RuntimeError(
                                f"unresolved_media_placeholder: {unresolved_id} (add mapping in {MEDIA_LINKS_CSV_PATH})"
                            )
                        response_payload = publish_content_payload(request_payload)
                        external_id = _trim(response_payload.get("id")) or _trim(response_payload.get("postId"))
                        _supabase_content_patch_post(
                            int(item["id"]),
                            {
                                "status": "published",
                                "scheduler_external_id": external_id or item.get("scheduler_external_id") or None,
                                "last_publish_error": None,
                                "updated_at": now_iso(),
                            },
                        )
                        _supabase_rest(
                            f"content_publish_job?id=eq.{job_id}",
                            method="PATCH",
                            body={"status": "published", "response_json": response_payload, "completed_at": now_iso()},
                            extra_headers={"Prefer": "return=minimal"},
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "published"})
                    except Exception as publish_error:
                        err = str(publish_error)
                        _supabase_content_patch_post(
                            int(item["id"]),
                            {"status": "failed", "last_publish_error": err, "updated_at": now_iso()},
                        )
                        _supabase_rest(
                            f"content_publish_job?id=eq.{job_id}",
                            method="PATCH",
                            body={"status": "failed", "error_message": err, "completed_at": now_iso()},
                            extra_headers={"Prefer": "return=minimal"},
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "failed", "error": err})
                response_payload = {"ok": True, "processed": len(results), "results": results, "media_sync": sync_result}
                if sync_error:
                    response_payload["media_sync_error"] = sync_error
                self._set_headers(200)
                self.wfile.write(json.dumps(response_payload).encode("utf-8"))
                return
            with connect_db() as conn:
                conn.row_factory = sqlite3.Row
                ensure_content_studio_tables(conn)
                if requested_post_ids:
                    placeholders = ",".join("?" for _ in requested_post_ids)
                    rows = conn.execute(
                        f"""
                        SELECT * FROM content_post
                        WHERE status IN ('approved', 'scheduled')
                          AND id IN ({placeholders})
                        ORDER BY COALESCE(NULLIF(scheduled_for, ''), post_date || 'T' || post_time) ASC, id ASC
                        """,
                        tuple(requested_post_ids),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT * FROM content_post
                        WHERE status IN ('approved', 'scheduled')
                        ORDER BY COALESCE(NULLIF(scheduled_for, ''), post_date || 'T' || post_time) ASC, id ASC
                        LIMIT ?
                        """,
                        (limit,),
                    ).fetchall()
                for row in rows:
                    item = row_to_content_post(row)
                    if not _trim(item["scheduled_for"]):
                        results.append(
                            {
                                "id": item["id"],
                                "post_id": item["post_id"],
                                "status": "skipped_missing_schedule",
                                "reason": "scheduled_for_required",
                            }
                        )
                        continue
                    duplicate_reason = _published_duplicate_reason(conn, int(item["id"]), str(item["post_id"]))
                    if duplicate_reason:
                        conn.execute(
                            """
                            UPDATE content_post
                            SET status='draft',
                                last_publish_error=?,
                                updated_at=CURRENT_TIMESTAMP
                            WHERE id = ?
                            """,
                            (f"duplicate_publish_skipped: {duplicate_reason}", item["id"]),
                        )
                        results.append(
                            {
                                "id": item["id"],
                                "post_id": item["post_id"],
                                "status": "skipped_duplicate",
                                "reason": duplicate_reason,
                            }
                        )
                        continue
                    primary_platform = item["platforms"][0] if item["platforms"] else ""
                    resolved_asset = resolve_media_url(
                        item["asset_filename"],
                        int(item.get("week_number") or 0),
                        int(item.get("day") or 0),
                        primary_platform,
                        media_links_map,
                    )
                    conn.execute(
                        "UPDATE content_post SET status='publishing', last_publish_error=NULL, updated_at=CURRENT_TIMESTAMP WHERE id = ?",
                        (item["id"],),
                    )
                    if resolved_asset and resolved_asset != item["asset_filename"]:
                        conn.execute(
                            "UPDATE content_post SET asset_filename=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                            (resolved_asset, item["id"]),
                        )
                        item["asset_filename"] = resolved_asset
                    request_payload = {
                        "post_id": item["post_id"],
                        "topic": item["topic"],
                        "post_type": item["post_type"],
                        "caption": item["caption"],
                        "reel_script": item["reel_script"],
                        "visual_prompt": item["visual_prompt"],
                        "asset_filename": item["asset_filename"],
                        "cta": item["cta"],
                        "hashtags": item["hashtags"],
                        "platforms": item["platforms"],
                        "post_date": item["post_date"],
                        "post_time": item["post_time"],
                        "scheduled_for": item["scheduled_for"],
                    }
                    job_id = int(
                        conn.execute(
                            """
                            INSERT INTO content_publish_job (content_post_id, scheduler, request_json, status)
                            VALUES (?, ?, ?, 'publishing')
                            """,
                            (item["id"], _publisher_mode() if _publisher_mode() != "webhook" else CONTENT_SCHEDULER_NAME, json.dumps(request_payload)),
                        ).lastrowid
                    )
                    try:
                        unresolved_id = _extract_drive_file_id(item["asset_filename"])
                        if unresolved_id and _is_placeholder_file_id(unresolved_id):
                            raise RuntimeError(
                                f"unresolved_media_placeholder: {unresolved_id} (add mapping in {MEDIA_LINKS_CSV_PATH})"
                            )
                        response_payload = publish_content_payload(request_payload)
                        external_id = _trim(response_payload.get("id")) or _trim(response_payload.get("postId"))
                        conn.execute(
                            """
                            UPDATE content_post
                            SET status='published',
                                scheduler_external_id=COALESCE(NULLIF(?, ''), scheduler_external_id),
                                last_publish_error=NULL,
                                updated_at=CURRENT_TIMESTAMP
                            WHERE id = ?
                            """,
                            (external_id, item["id"]),
                        )
                        conn.execute(
                            """
                            UPDATE content_publish_job
                            SET status='published', response_json=?, completed_at=?
                            WHERE id = ?
                            """,
                            (json.dumps(response_payload), now_iso(), job_id),
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "published"})
                    except Exception as publish_error:
                        err = str(publish_error)
                        conn.execute(
                            "UPDATE content_post SET status='failed', last_publish_error=?, updated_at=CURRENT_TIMESTAMP WHERE id = ?",
                            (err, item["id"]),
                        )
                        conn.execute(
                            """
                            UPDATE content_publish_job
                            SET status='failed', error_message=?, completed_at=?
                            WHERE id = ?
                            """,
                            (err, now_iso(), job_id),
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "failed", "error": err})
                conn.commit()
            mirror_db_to_drive()
            response_payload = {"ok": True, "processed": len(results), "results": results, "media_sync": sync_result}
            if sync_error:
                response_payload["media_sync_error"] = sync_error
            self._set_headers(200)
            self.wfile.write(json.dumps(response_payload).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))

    def _handle_content_publish_jobs_get(self, query: str) -> None:
        try:
            params = parse_qs(query or "")
            limit = int((params.get("limit") or ["50"])[0] or "50")
            if _use_supabase_backend():
                rows = _supabase_rest(
                    f"content_publish_job?select=*,content_post(post_id)&order=id.desc&limit={limit}",
                    method="GET",
                ) or []
                items = []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    raw_response = row.get("response_json")
                    parsed_response = raw_response
                    if isinstance(raw_response, str) and raw_response.strip():
                        try:
                            parsed_response = json.loads(raw_response)
                        except Exception:
                            parsed_response = raw_response
                    post_rel = row.get("content_post") or {}
                    items.append(
                        {
                            "id": int(row.get("id") or 0),
                            "content_post_id": int(row.get("content_post_id") or 0),
                            "post_id": str((post_rel or {}).get("post_id") or ""),
                            "scheduler": str(row.get("scheduler") or ""),
                            "status": str(row.get("status") or ""),
                            "error_message": str(row.get("error_message") or ""),
                            "run_at": str(row.get("run_at") or ""),
                            "completed_at": str(row.get("completed_at") or ""),
                            "response": parsed_response,
                        }
                    )
            else:
                with connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    ensure_content_studio_tables(conn)
                    rows = conn.execute(
                        """
                        SELECT j.*, p.post_id
                        FROM content_publish_job j
                        JOIN content_post p ON p.id = j.content_post_id
                        ORDER BY j.id DESC
                        LIMIT ?
                        """,
                        (limit,),
                    ).fetchall()
                items = []
                for row in rows:
                    raw_response = str(row["response_json"] or "").strip()
                    parsed_response = None
                    if raw_response:
                        try:
                            parsed_response = json.loads(raw_response)
                        except Exception:
                            parsed_response = raw_response
                    items.append(
                        {
                            "id": int(row["id"]),
                            "content_post_id": int(row["content_post_id"]),
                            "post_id": str(row["post_id"] or ""),
                            "scheduler": str(row["scheduler"] or ""),
                            "status": str(row["status"] or ""),
                            "error_message": str(row["error_message"] or ""),
                            "run_at": str(row["run_at"] or ""),
                            "completed_at": str(row["completed_at"] or ""),
                            "response": parsed_response,
                        }
                    )
            self._set_headers(200)
            self.wfile.write(
                json.dumps({"ok": True, "items": items, "jobs": items, "count": len(items)}).encode("utf-8")
            )
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))

    def _handle_import(self) -> None:
        try:
            data = self._read_json_body()
            rows = data.get("rows") or []
            if not isinstance(rows, list):
                raise ValueError("'rows' must be an array")
            with_cleanup = bool(data.get("withCleanup"))
            cleanup_backup = bool(data.get("cleanupBackup"))

            cleanup_result = None
            if _use_supabase_backend():
                result = _supabase_import_leads(rows)
                if with_cleanup:
                    cleanup_result = {"ok": False, "skipped": True, "reason": "cleanup_not_supported_in_cloud_mode"}
            else:
                with connect_db() as conn:
                    ensure_lead_master_columns(conn)
                    result = import_rows(conn, rows)
                    conn.commit()

                if with_cleanup:
                    cleanup_result = run_cleanup(
                        db_path=DB_PATH,
                        audit_dir=ROOT.parent / "exports",
                        create_backup=cleanup_backup,
                    )

                mirror_db_to_drive()
            response = {"ok": True, **result}
            if cleanup_result is not None:
                response["cleanup"] = cleanup_result
            self._send_json(response)
        except Exception as exc:  # pragma: no cover - defensive handler
            self._send_error(str(exc))

    def _handle_carrier_config_get(self) -> None:
        try:
            with connect_db() as conn:
                rows = get_agent_carrier_config_rows(conn)
            self._send_json({"ok": True, "rows": rows})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_carrier_config_save(self) -> None:
        try:
            data = self._read_json_body()
            rows = data.get("rows") or []
            if not isinstance(rows, list):
                raise ValueError("'rows' must be an array")
            with connect_db() as conn:
                saved = save_agent_carrier_config_rows(conn, rows)
                conn.commit()
            mirror_db_to_drive()
            self._send_json({"ok": True, "saved": saved})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_calendar_schedule(self) -> None:
        try:
            data = self._read_json_body()

            contact_id = _trim(data.get("contactId") or data.get("leadId"))
            client_name = _trim(data.get("clientName") or data.get("name")) or "Client Follow-up"
            email = _trim(data.get("email"))
            phone = _trim(data.get("phone"))
            start_raw = _trim(data.get("scheduledAt") or data.get("start") or data.get("dateTime"))
            if not start_raw:
                raise ValueError("scheduledAt is required")
            duration_minutes = int(data.get("durationMinutes") or 30)
            description = _trim(data.get("description")) or "Insurance follow-up call"

            start_dt = _parse_local_iso(start_raw)
            end_dt = start_dt + timedelta(minutes=max(duration_minutes, 15))
            start_iso = start_dt.isoformat()
            end_iso = end_dt.isoformat()
            summary = f"Insurance Follow-up: {client_name}"
            full_description = description
            if phone:
                full_description = f"{full_description}\nPhone: {phone}".strip()

            if _use_supabase_backend():
                event_payload = _google_calendar_via_web_app(
                    client_name=client_name,
                    email=email,
                    phone=phone,
                    scheduled_at=start_iso,
                    description=full_description,
                    duration_minutes=duration_minutes,
                )
                event_id = _trim(event_payload.get("calendarEventId"))
                if contact_id:
                    _supabase_sync_calendar_schedule(contact_id, start_iso, event_id)
            else:
                gog_args = [
                    "calendar",
                    "create",
                    GOG_CALENDAR_ID,
                    f"--summary={summary}",
                    f"--from={start_iso}",
                    f"--to={end_iso}",
                    f"--description={full_description}",
                    "--send-updates=all",
                ]
                if "@" in email:
                    gog_args.append(f"--attendees={email}")
                event_payload = run_gog_json(gog_args)
                event_id = _extract_event_id(event_payload)

                if contact_id:
                    with connect_db() as conn:
                        ensure_lead_master_columns(conn)
                        conn.execute(
                            """
                            UPDATE lead_master
                            SET
                              next_appointment_time = ?,
                              calendar_event_id = COALESCE(NULLIF(?, ''), calendar_event_id),
                              updated_at = CURRENT_TIMESTAMP
                            WHERE lead_external_id = ?
                            """,
                            (start_iso, event_id, contact_id),
                        )
                        lead_row = conn.execute(
                            "SELECT lead_id FROM lead_master WHERE lead_external_id=? LIMIT 1",
                            (contact_id,),
                        ).fetchone()
                        if lead_row:
                            conn.execute(
                                """
                                INSERT INTO appointment (
                                  lead_id, booking_date, booking_status, show_status, appointment_type, owner
                                ) VALUES (?, ?, 'Booked', 'pending', 'callback', 'call_desk')
                                """,
                                (int(lead_row[0]), start_iso),
                            )
                        conn.commit()
                    mirror_db_to_drive()

            self._send_json(
                {
                    "ok": True,
                    "calendarEventId": event_id,
                    "nextAppointmentTime": start_iso,
                    "start": start_iso,
                    "end": end_iso,
                    "calendarId": GOG_CALENDAR_ID,
                }
            )
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_google_calendar_sync(self) -> None:
        try:
            data = self._read_json_body()

            client_name = _trim(data.get("clientName")) or "Client Follow-up"
            email = _trim(data.get("email"))
            phone = _trim(data.get("phone"))
            scheduled_at = _trim(data.get("scheduledAt") or data.get("start") or data.get("dateTime"))
            if not scheduled_at:
                raise ValueError("scheduledAt is required")
            description = _trim(data.get("description")) or "Insurance follow-up call"
            duration_minutes = int(data.get("durationMinutes") or 30)
            existing_event_id = _trim(data.get("existingEventId"))

            payload = _google_calendar_via_web_app(
                client_name=client_name,
                email=email,
                phone=phone,
                scheduled_at=scheduled_at,
                description=description,
                duration_minutes=duration_minutes,
                existing_event_id=existing_event_id,
            )
            self._send_json(payload)
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_lead_documents_get(self, lead_external_id: str) -> None:
        try:
            lead = _load_supabase_lead_by_external_id(lead_external_id)
            if not lead:
                self._set_headers(404)
                self.wfile.write(json.dumps({"ok": False, "error": "lead_not_found"}).encode("utf-8"))
                return
            lead_id = int(lead.get("lead_id") or 0)
            rows = _supabase_rest(
                f"lead_document?select=*&lead_id=eq.{lead_id}&archived_at=is.null&order=inserted_at.desc",
                method="GET",
            ) or []
            items = [_lead_document_row_to_payload(row) for row in rows if isinstance(row, dict)]
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "leadId": lead_id, "items": items}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            message = str(exc)
            if "lead_document" in message:
                message = "Lead document hub is not configured yet. Run lead_document_hub.sql in Supabase."
            self.wfile.write(json.dumps({"ok": False, "error": message}).encode("utf-8"))

    def _handle_lead_documents_post(self, lead_external_id: str) -> None:
        try:
            lead = _load_supabase_lead_by_external_id(lead_external_id)
            if not lead:
                self._set_headers(404)
                self.wfile.write(json.dumps({"ok": False, "error": "lead_not_found"}).encode("utf-8"))
                return
            data = self._read_json_body()
            file_name = _trim(data.get("fileName"))
            source_url = _trim(data.get("sourceUrl"))
            document_category = _trim(data.get("documentCategory")) or "general"
            notes = _trim(data.get("notes"))
            mime_type = _trim(data.get("mimeType"))
            uploaded_by_email = _trim(data.get("uploadedByEmail"))
            source_kind = "link" if source_url and not data.get("contentBase64") else "upload"
            storage_path = ""
            file_size_bytes = int(data.get("fileSizeBytes") or 0)

            if source_kind == "upload":
                content_base64 = _trim(data.get("contentBase64"))
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
                if file_size_bytes > LEAD_DOCUMENT_MAX_BYTES:
                    raise ValueError(f"File exceeds max size of {LEAD_DOCUMENT_MAX_BYTES} bytes")
                _ensure_lead_document_bucket()
                safe_name = _sanitize_storage_filename(file_name)
                storage_path = f"lead-{int(lead['lead_id'])}/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:8]}-{safe_name}"
                _supabase_storage_rest(
                    f"object/{quote(LEAD_DOCUMENT_BUCKET, safe='')}/{quote(storage_path, safe='/')}",
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
                "storage_bucket": LEAD_DOCUMENT_BUCKET if storage_path else None,
                "storage_path": storage_path or None,
                "source_url": source_url or None,
                "mime_type": mime_type or None,
                "file_size_bytes": file_size_bytes or None,
                "notes": notes or None,
                "uploaded_by_email": uploaded_by_email or None,
            }
            created = _supabase_rest(
                "lead_document?select=*",
                method="POST",
                body=row_payload,
                extra_headers={"Prefer": "return=representation"},
            ) or []
            row = created[0] if isinstance(created, list) and created else row_payload
            try:
                _supabase_rest(
                    "call_desk_activity",
                    method="POST",
                    body={
                        "lead_id": int(lead["lead_id"]),
                        "activity_date": now_iso(),
                        "channel": "portal",
                        "activity_type": "document_added",
                        "owner": uploaded_by_email or None,
                        "notes": f"{document_category}: {file_name}",
                    },
                )
            except Exception:
                pass
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": _lead_document_row_to_payload(row)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            message = str(exc)
            if "lead_document" in message:
                message = "Lead document hub is not configured yet. Run lead_document_hub.sql in Supabase."
            self.wfile.write(json.dumps({"ok": False, "error": message}).encode("utf-8"))

    def _handle_lead_document_archive(self, document_id_raw: str) -> None:
        try:
            document_id = int(document_id_raw or 0)
            if document_id <= 0:
                raise ValueError("document_id is required")
            updated = _supabase_rest(
                f"lead_document?document_id=eq.{document_id}&select=*",
                method="PATCH",
                body={"archived_at": now_iso()},
                extra_headers={"Prefer": "return=representation"},
            ) or []
            row = updated[0] if isinstance(updated, list) and updated else None
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": _lead_document_row_to_payload(row or {})}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            message = str(exc)
            if "lead_document" in message:
                message = "Lead document hub is not configured yet. Run lead_document_hub.sql in Supabase."
            self.wfile.write(json.dumps({"ok": False, "error": message}).encode("utf-8"))

    def _handle_calendar_today(self) -> None:
        try:
            if _use_supabase_backend():
                now_local = datetime.now(APP_TIMEZONE)
                start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
                end_local = start_local + timedelta(days=1)
                rows = _supabase_calendar_items(start_local, end_local)
            else:
                payload = run_gog_json(
                    [
                        "calendar",
                        "events",
                        GOG_CALENDAR_ID,
                        "--today",
                        "--max=50",
                    ]
                )
                events = _extract_events(payload)
                rows = []
                for event in events:
                    start = event.get("start", {}) if isinstance(event.get("start"), dict) else {}
                    end = event.get("end", {}) if isinstance(event.get("end"), dict) else {}
                    rows.append(
                        {
                            "id": _trim(event.get("id")),
                            "summary": _trim(event.get("summary")) or "(No title)",
                            "start": _trim(start.get("dateTime") or start.get("date")),
                            "end": _trim(end.get("dateTime") or end.get("date")),
                            "htmlLink": _trim(event.get("htmlLink")),
                        }
                    )
                rows.sort(key=lambda row: row["start"] or "")
            self._send_json({"ok": True, "items": rows, "count": len(rows)})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_calendar_week(self) -> None:
        try:
            if _use_supabase_backend():
                now_local = datetime.now(APP_TIMEZONE)
                start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
                end_local = start_local + timedelta(days=7)
                rows = _supabase_calendar_items(start_local, end_local)
            else:
                payload = run_gog_json(
                    [
                        "calendar",
                        "events",
                        GOG_CALENDAR_ID,
                        "--days=7",
                        "--max=100",
                    ]
                )
                events = _extract_events(payload)
                rows = []
                for event in events:
                    start = event.get("start", {}) if isinstance(event.get("start"), dict) else {}
                    end = event.get("end", {}) if isinstance(event.get("end"), dict) else {}
                    attendees = event.get("attendees") if isinstance(event.get("attendees"), list) else []
                    rows.append(
                        {
                            "id": _trim(event.get("id")),
                            "summary": _trim(event.get("summary")) or "(No title)",
                            "start": _trim(start.get("dateTime") or start.get("date")),
                            "end": _trim(end.get("dateTime") or end.get("date")),
                            "htmlLink": _trim(event.get("htmlLink")),
                            "attendees": [
                                {"email": _trim(att.get("email"))}
                                for att in attendees
                                if isinstance(att, dict) and _trim(att.get("email"))
                            ],
                        }
                    )
                rows.sort(key=lambda row: row["start"] or "")
            self._send_json({"ok": True, "items": rows, "count": len(rows)})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_purge_test_data(self) -> None:
        try:
            with connect_db() as conn:
                ensure_lead_master_columns(conn)
                deleted = conn.execute(
                    """
                    DELETE FROM lead_master
                    WHERE first_name LIKE '%Test%'
                       OR last_name LIKE '%Test%'
                    """
                ).rowcount
                reset = conn.execute(
                    """
                    UPDATE lead_master
                    SET pipeline_status = NULL,
                        calendar_event_id = NULL
                    """
                ).rowcount
                conn.commit()
            mirror_db_to_drive()
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "deleted": int(deleted or 0), "reset": int(reset or 0)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))

    def _handle_pipeline_update(self, lead_external_id: str) -> None:
        try:
            data = self._read_json_body()
            status_raw = _trim(data.get("status"))
            status_map = {
                "App Submitted": "app_submitted",
                "Underwriting": "underwriting",
                "Approved": "approved",
                "Issued": "issued",
                "Paid": "paid",
            }
            pipeline_status = status_map.get(status_raw, _trim(data.get("pipeline_status")) or "app_submitted")
            if _use_supabase_backend():
                row = _supabase_update_lead_fields(
                    lead_external_id,
                    {"pipeline_status": pipeline_status, "updated_at": now_iso()},
                )
                if not row:
                    self._send_error("lead_not_found", status=404)
                    return
            else:
                with connect_db() as conn:
                    ensure_lead_master_columns(conn)
                    row = conn.execute(
                        "SELECT lead_id FROM lead_master WHERE lead_external_id=? LIMIT 1",
                        (_trim(lead_external_id),),
                    ).fetchone()
                    if not row:
                        self._send_error("lead_not_found", status=404)
                        return
                    conn.execute(
                        """
                        UPDATE lead_master
                        SET pipeline_status=?, updated_at=CURRENT_TIMESTAMP
                        WHERE lead_external_id=?
                        """,
                        (pipeline_status, _trim(lead_external_id)),
                    )
                    conn.commit()
                mirror_db_to_drive()
            self._send_json(
                {
                    "ok": True,
                    "leadId": _trim(lead_external_id),
                    "pipeline_status": pipeline_status,
                }
            )
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

    def _handle_open_update(self, lead_external_id: str) -> None:
        try:
            opened_at = now_iso()
            if _use_supabase_backend():
                row = _supabase_update_lead_fields(
                    lead_external_id,
                    {"last_opened_at": opened_at, "updated_at": opened_at},
                )
                if not row:
                    self._send_error("lead_not_found", status=404)
                    return
            else:
                with connect_db() as conn:
                    ensure_lead_master_columns(conn)
                    row = conn.execute(
                        "SELECT lead_id FROM lead_master WHERE lead_external_id=? LIMIT 1",
                        (_trim(lead_external_id),),
                    ).fetchone()
                    if not row:
                        self._send_error("lead_not_found", status=404)
                        return
                    conn.execute(
                        """
                        UPDATE lead_master
                        SET last_opened_at=?, updated_at=CURRENT_TIMESTAMP
                        WHERE lead_external_id=?
                        """,
                        (opened_at, _trim(lead_external_id)),
                    )
                    conn.commit()
                mirror_db_to_drive()
            self._send_json({"ok": True, "leadId": _trim(lead_external_id), "last_opened_at": opened_at})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


def main() -> None:
    server = HTTPServer((HOST, PORT), LeadSyncHandler)
    print(f"Local DB API listening on http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
