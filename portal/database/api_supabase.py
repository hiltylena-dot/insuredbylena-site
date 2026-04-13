#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib import error as urlerror, request as urlrequest
from urllib.parse import quote


def _supabase_rest(
    path: str,
    method: str = "GET",
    body: Any | None = None,
    extra_headers: dict[str, str] | None = None,
) -> Any:
    import api_support as api
    if not api.SUPABASE_URL or not api.SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("supabase_not_configured")
    url = f"{api.SUPABASE_URL}/rest/v1/{path.lstrip('/')}"
    headers = {
        "apikey": api.SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {api.SUPABASE_SERVICE_ROLE_KEY}",
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
    import api_support as api
    if not api.SUPABASE_URL or not api.SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("supabase_not_configured")
    url = f"{api.SUPABASE_URL}/storage/v1/{path.lstrip('/')}"
    headers = {
        "apikey": api.SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {api.SUPABASE_SERVICE_ROLE_KEY}",
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
    import api_support as api
    value = api._trim(name)
    if not value:
        return "document"
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")
    return cleaned[:120] or "document"

def _ensure_lead_document_bucket() -> None:
    import api_support as api
    if api.LEAD_DOCUMENT_BUCKET_READY:
        return
    try:
        _supabase_storage_rest(
            "bucket",
            method="POST",
            body={
                "id": api.LEAD_DOCUMENT_BUCKET,
                "name": api.LEAD_DOCUMENT_BUCKET,
                "public": False,
                "file_size_limit": api.LEAD_DOCUMENT_MAX_BYTES,
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
    api.LEAD_DOCUMENT_BUCKET_READY = True

def _lead_document_signed_url(storage_path: str) -> str:
    import api_support as api
    path = api._trim(storage_path)
    if not path:
        return ""
    result = _supabase_storage_rest(
        f"object/sign/{quote(api.LEAD_DOCUMENT_BUCKET, safe='')}/{quote(path, safe='/')}",
        method="POST",
        body={"expiresIn": 60 * 60 * 12},
    ) or {}
    signed = api._trim(result.get("signedURL"))
    if not signed:
        return ""
    if signed.startswith("http://") or signed.startswith("https://"):
        return signed
    return f"{api.SUPABASE_URL}/storage/v1{signed}"

def _load_supabase_lead_by_external_id(lead_external_id: str) -> dict[str, Any] | None:
    import api_support as api
    external_id = api._trim(lead_external_id)
    if not external_id:
        return None
    rows = _supabase_rest(
        f"lead_master?select=lead_id,lead_external_id,full_name,email,mobile_phone&lead_external_id=eq.{quote(external_id, safe='')}&limit=1",
        method="GET",
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    return row if isinstance(row, dict) else None

def _lead_document_row_to_payload(row: dict[str, Any]) -> dict[str, Any]:
    import api_support as api
    storage_path = api._trim(row.get("storage_path"))
    source_url = api._trim(row.get("source_url"))
    return {
        "documentId": int(row.get("document_id") or 0),
        "leadId": int(row.get("lead_id") or 0),
        "fileName": api._trim(row.get("file_name")),
        "documentCategory": api._trim(row.get("document_category")) or "general",
        "sourceKind": api._trim(row.get("source_kind")) or "upload",
        "sourceUrl": source_url,
        "downloadUrl": _lead_document_signed_url(storage_path) if storage_path else source_url,
        "mimeType": api._trim(row.get("mime_type")),
        "fileSizeBytes": int(row.get("file_size_bytes") or 0),
        "notes": api._trim(row.get("notes")),
        "uploadedByEmail": api._trim(row.get("uploaded_by_email")),
        "insertedAt": api._trim(row.get("inserted_at")),
        "updatedAt": api._trim(row.get("updated_at")),
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
    import api_support as api
    if not api.GOOGLE_CALENDAR_WEB_APP_URL:
        raise RuntimeError("google_calendar_web_app_not_configured")
    payload = {
        "secret": api.GOOGLE_CALENDAR_SECRET,
        "clientName": client_name,
        "email": email,
        "phone": phone,
        "scheduledAt": scheduled_at,
        "description": description,
        "durationMinutes": max(int(duration_minutes or 30), 15),
        "existingEventId": existing_event_id,
    }
    req = urlrequest.Request(
        api.GOOGLE_CALENDAR_WEB_APP_URL,
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
    import api_support as api
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
    import api_support as api
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
    import api_support as api
    body = {
        "content_post_id": int(payload.get("content_post_id") or 0),
        "scheduler": api._trim(payload.get("scheduler")) or "buffer_graphql",
        "request_json": payload.get("request_json") or {},
        "status": api._trim(payload.get("status")) or "queued",
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
    import api_support as api
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
    import api_support as api
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
    import api_support as api
    rows = _supabase_rest(
        "content_revision?select=id",
        method="POST",
        body={
            "content_post_id": int(content_post_id),
            "revision_number": _supabase_content_next_revision_number(content_post_id),
            "changed_by": api._trim(changed_by) or None,
            "change_note": api._trim(change_note) or None,
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
    import api_support as api
    rows = _supabase_rest(
        "content_approval?select=id",
        method="POST",
        body={
            "content_post_id": int(content_post_id),
            "decision": api._trim(decision) or "submitted",
            "note": api._trim(note) or None,
            "actor": api._trim(actor) or None,
        },
        extra_headers={"Prefer": "return=representation"},
    ) or []
    row = rows[0] if isinstance(rows, list) and rows else None
    approval_id = int((row or {}).get("id") or 0) if isinstance(row, dict) else 0
    if approval_id <= 0:
        raise RuntimeError("content_approval_insert_failed")
    return approval_id

def _supabase_content_write_payload(item: dict[str, Any]) -> dict[str, Any]:
    import api_support as api
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
        "post_date": api._trim(item.get("post_date")) or None,
        "post_time": api._trim(item.get("post_time")) or None,
        "scheduled_for": api._trim(item.get("scheduled_for")) or None,
        "platforms_json": platforms_raw,
        "post_type": api._trim(item.get("post_type")) or None,
        "topic": api._trim(item.get("topic")) or None,
        "hook": api._trim(item.get("hook")) or None,
        "caption": api._trim(item.get("caption")) or None,
        "reel_script": api._trim(item.get("reel_script")) or None,
        "visual_prompt": api._trim(item.get("visual_prompt")) or None,
        "canva_design_link": api._trim(item.get("canva_design_link")) or None,
        "asset_filename": api._trim(item.get("asset_filename")) or None,
        "cta": api._trim(item.get("cta")) or None,
        "hashtags_text": api._trim(item.get("hashtags_text")) or None,
        "status": api._trim(item.get("status")) or "draft",
        "source_file": api._trim(item.get("source_file")) or None,
        "created_by": api._trim(item.get("created_by")) or None,
        "updated_at": api.now_iso(),
    }

def _supabase_content_import_posts(posts: list[dict], week_number: int, actor: str, source_file: str) -> tuple[int, int]:
    import api_support as api
    imported = 0
    updated = 0
    for raw_item in posts:
        if not isinstance(raw_item, dict):
            continue
        item = api.normalize_content_post_input(raw_item, week_number, actor, source_file)
        post_id = quote(api._trim(item.get("post_id")), safe="")
        post_date = quote(api._trim(item.get("post_date")), safe="")
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
                api._content_snapshot_from_post(current),
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
    import api_support as api
    response = _supabase_rest(
        "rpc/portal_save_call_desk",
        method="POST",
        body={"p_payload": payload},
    ) or {}
    if not isinstance(response, dict) or not response.get("ok"):
        raise RuntimeError(str((response or {}).get("error") or "portal_save_call_desk_failed"))
    return response

def _lead_sync_payload_from_request(data: dict[str, Any], force_external_id: str = "") -> dict[str, Any]:
    import api_support as api
    contact_id = api._trim(force_external_id) or api._trim(data.get("contactId") or data.get("lead_external_id"))
    first_name = api._trim(data.get("firstName") or data.get("first_name"))
    last_name = api._trim(data.get("lastName") or data.get("last_name"))
    full_name = api._trim(data.get("fullName") or data.get("full_name"))
    next_appointment = api._trim(data.get("nextAppointmentTime") or data.get("next_appointment_time"))
    disposition = api._trim(data.get("disposition") or data.get("lead_status"))
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
        "phone": api._trim(data.get("phone") or data.get("mobile_phone")),
        "email": api._trim(data.get("email")),
        "notes": api._trim(data.get("notes")),
        "tags": api._trim(data.get("tags") or data.get("raw_tags")),
        "lastActivity": api._trim(data.get("lastActivity") or data.get("last_activity_at_source")) or api.now_iso(),
        "age": api._trim(data.get("age")),
        "tobacco": api._trim(data.get("tobacco")),
        "healthPosture": api._trim(data.get("healthPosture") or data.get("health_posture")),
        "disposition": disposition,
        "carrierMatch": api._trim(data.get("carrierMatch") or data.get("carrier_match")),
        "confidence": api._trim(data.get("confidence")),
        "pipelineStatus": api._trim(data.get("pipelineStatus") or data.get("pipeline_status")),
        "nextAppointmentTime": next_appointment,
        "shouldSchedule": should_schedule,
        "leadSource": api._trim(data.get("leadSource") or data.get("lead_source")),
        "leadSourceDetail": api._trim(data.get("leadSourceDetail") or data.get("lead_source_detail")),
        "productLine": api._trim(data.get("productLine") or data.get("product_line")),
        "productInterest": api._trim(data.get("productInterest") or data.get("product_interest")),
    }

def _supabase_update_lead_fields(lead_external_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    import api_support as api
    external_id = api._trim(lead_external_id)
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
    import api_support as api
    added = 0
    updated = 0
    skipped_invalid = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped_invalid += 1
            continue
        lead_external_id = api._nv(row.get("lead_external_id"))
        if not lead_external_id:
            skipped_invalid += 1
            continue
        body = {
            "lead_external_id": lead_external_id,
            "first_name": api._nv(row.get("first_name")),
            "last_name": api._nv(row.get("last_name")),
            "full_name": api._nv(row.get("full_name")),
            "email": api._nv(row.get("email")),
            "mobile_phone": api._nv(row.get("mobile_phone")),
            "business_name": api._nv(row.get("business_name")),
            "lead_source": api._nv(row.get("lead_source")),
            "lead_source_detail": api._nv(row.get("lead_source_detail")),
            "campaign_name": api._nv(row.get("campaign_name")),
            "product_interest": api._nv(row.get("product_interest")),
            "product_line": api._nv(row.get("product_line")),
            "owner_queue": api._nv(row.get("owner_queue")),
            "lead_status": api._nv(row.get("lead_status")),
            "booking_status": api._nv(row.get("booking_status")),
            "consent_status": api._nv(row.get("consent_status")),
            "consent_channel_sms": api._nv(row.get("consent_channel_sms")),
            "consent_channel_email": api._nv(row.get("consent_channel_email")),
            "consent_channel_whatsapp": api._nv(row.get("consent_channel_whatsapp")),
            "dnc_status": api._nv(row.get("dnc_status")),
            "contact_eligibility": api._nv(row.get("contact_eligibility")),
            "created_at_source": api._nv(row.get("created_at_source")),
            "last_activity_at_source": api._nv(row.get("last_activity_at_source")),
            "notes": api._nv(row.get("notes")),
            "raw_tags": api._nv(row.get("raw_tags")),
            "routing_bucket": api._nv(row.get("routing_bucket")),
            "suppress_reason": api._nv(row.get("suppress_reason")),
            "recommended_channel": api._nv(row.get("recommended_channel")),
            "sequence_name": api._nv(row.get("sequence_name")),
            "recommended_next_action": api._nv(row.get("recommended_next_action")),
            "priority_tier": api._nv(row.get("priority_tier")),
            "age": api._nv(row.get("age")),
            "tobacco": api._nv(row.get("tobacco")),
            "health_posture": api._nv(row.get("health_posture")),
            "disposition": api._nv(row.get("disposition")),
            "carrier_match": api._nv(row.get("carrier_match")),
            "confidence": api._nv(row.get("confidence")),
            "pipeline_status": api._nv(row.get("pipeline_status") or row.get("pipelineStatus")),
            "calendar_event_id": api._nv(row.get("calendar_event_id") or row.get("calendarEventId")),
            "next_appointment_time": api._nv(row.get("next_appointment_time") or row.get("nextAppointmentTime")),
            "last_opened_at": api._nv(row.get("last_opened_at") or row.get("lastOpenedAt")),
            "updated_at": api.now_iso(),
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
    import api_support as api
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
    import api_support as api
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
        summary = api._trim(lead.get("full_name")) or api._trim(lead.get("lead_external_id")) or "Scheduled follow-up"
        items.append(
            {
                "id": str(row.get("appointment_id") or ""),
                "summary": summary,
                "start": api._trim(row.get("booking_date")),
                "end": api._trim(row.get("booking_date")),
                "htmlLink": "",
                "attendees": ([{"email": api._trim(lead.get("email"))}] if api._trim(lead.get("email")) else []),
                "leadExternalId": api._trim(lead.get("lead_external_id")),
                "phone": api._trim(lead.get("mobile_phone")),
                "appointmentType": api._trim(row.get("appointment_type")),
                "owner": api._trim(row.get("owner")),
            }
        )
    items.sort(key=lambda row: row["start"] or "")
    return items

def _supabase_sync_calendar_schedule(contact_id: str, start_iso: str, event_id: str = "") -> None:
    import api_support as api
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
            "calendar_event_id": api._trim(event_id) or None,
            "booking_status": "Booked",
            "updated_at": api.now_iso(),
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
