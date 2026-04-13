#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib import error as urlerror, request as urlrequest
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / "database" / ".env"


def load_env() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


load_env()

API_BASE = os.getenv("API_BASE", "https://insuredbylena-portal-api-607620457436.us-central1.run.app").strip().rstrip("/")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()


def http_json(
    url: str,
    *,
    method: str = "GET",
    body: dict | list | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, object]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=payload,
        method=method.upper(),
        headers={
            "Content-Type": "application/json",
            **(headers or {}),
        },
    )
    try:
        with urlrequest.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, json.loads(raw or "{}")
    except urlerror.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(raw or "{}")
        except Exception:
            return exc.code, {"ok": False, "error": raw}


def http_json_with_headers(
    url: str,
    *,
    method: str = "GET",
    body: dict | list | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, object, dict[str, str]]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=payload,
        method=method.upper(),
        headers={
            "Content-Type": "application/json",
            **(headers or {}),
        },
    )
    try:
        with urlrequest.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return resp.status, json.loads(raw or "{}"), dict(resp.headers.items())
    except urlerror.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload_obj = json.loads(raw or "{}")
        except Exception:
            payload_obj = {"ok": False, "error": raw}
        return exc.code, payload_obj, dict(exc.headers.items())


def get_header(headers: dict[str, str], name: str) -> str:
    target = name.lower()
    for key, value in headers.items():
        if str(key).lower() == target:
            return str(value)
    return ""


def supabase_rest(
    path: str,
    *,
    method: str = "GET",
    body: dict | list | None = None,
    prefer: str = "",
) -> tuple[int, object]:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required for backend regression tests.")
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    }
    if prefer:
        headers["Prefer"] = prefer
    return http_json(f"{SUPABASE_URL}/rest/v1/{path.lstrip('/')}", method=method, body=body, headers=headers)


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _json_ok(response: object) -> bool:
    return isinstance(response, dict) and bool(response.get("ok"))


def _first_id(rows: object, key: str) -> int:
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return int(rows[0].get(key) or 0)
    return 0


def main() -> int:
    stamp = str(int(time.time()))
    phone = f"556{stamp[-7:]}"
    email = f"backend-regression-{stamp}@example.com"
    contact = f"API-{stamp}"
    imported_contact = f"IMPORT-{stamp}"
    imported_email = f"import-{stamp}@example.com"
    content_post_key = f"API-{stamp}"

    lead_id = 0
    lead_external_id = contact
    content_post_id = 0
    results: dict[str, object] = {}
    try:
        status, health, health_headers = http_json_with_headers(f"{API_BASE}/api/health")
        assert_true(status == 200 and _json_ok(health), f"API health failed: {health}")
        assert_true(bool(get_header(health_headers, "X-Request-Id")), f"Health response missing X-Request-Id header: {health_headers}")
        assert_true(bool((health or {}).get("requestId")), f"Health response missing requestId body field: {health}")
        results["health"] = {"status": status, "requestId": (health or {}).get("requestId")}

        status, version = http_json(f"{API_BASE}/api/version")
        assert_true(status == 200 and _json_ok(version), f"API version failed: {version}")
        assert_true(bool((version or {}).get("service")), f"Version response missing service: {version}")
        results["version"] = {
            "service": (version or {}).get("service"),
            "buildSha": (version or {}).get("buildSha"),
            "revision": (version or {}).get("revision"),
        }

        status, bad_doc, _ = http_json_with_headers(f"{API_BASE}/api/lead-documents/0/archive", method="POST", body={})
        assert_true(status == 500 and isinstance(bad_doc, dict), f"Expected document archive error response: {bad_doc}")
        assert_true(bool(bad_doc.get("requestId")), f"Error response missing requestId: {bad_doc}")
        assert_true(bool(bad_doc.get("errorCode")), f"Error response missing errorCode: {bad_doc}")
        results["error_envelope"] = {"status": status, "errorCode": bad_doc.get("errorCode")}

        status, created = http_json(
            f"{API_BASE}/api/leads/sync",
            method="POST",
            body={
                "contactId": contact,
                "firstName": "Backend",
                "lastName": "Regression",
                "phone": phone,
                "email": email,
                "disposition": "callback",
                "shouldSchedule": True,
                "nextAppointmentTime": "2026-04-18T15:00:00Z",
                "notes": "regression create",
            },
        )
        assert_true(status == 200 and _json_ok(created), f"Lead sync create failed: {created}")
        lead = (created or {}).get("lead") if isinstance(created, dict) else {}
        lead_id = int((lead or {}).get("lead_id") or (created or {}).get("leadId") or 0)
        lead_external_id = str((lead or {}).get("lead_external_id") or contact)
        assert_true(lead_id > 0, f"Lead id missing after create: {created}")
        results["lead_sync_create"] = {"leadId": lead_id, "appointmentId": (created or {}).get("appointmentId")}

        status, updated = http_json(
            f"{API_BASE}/api/leads/{quote(contact, safe='')}",
            method="PUT",
            body={
                "notes": "regression update",
                "confidence": "high",
                "disposition": "follow_up",
                "shouldSchedule": True,
                "nextAppointmentTime": "2026-04-19T16:00:00Z",
            },
        )
        assert_true(status == 200 and _json_ok(updated), f"Lead sync update failed: {updated}")
        results["lead_sync_update"] = {"status": status}

        status, pipeline = http_json(
            f"{API_BASE}/api/leads/{quote(contact, safe='')}/pipeline",
            method="PUT",
            body={"status": "Underwriting"},
        )
        assert_true(status == 200 and _json_ok(pipeline), f"Pipeline update failed: {pipeline}")
        results["lead_pipeline_update"] = {"pipeline_status": (pipeline or {}).get("pipeline_status")}

        status, opened = http_json(
            f"{API_BASE}/api/leads/{quote(contact, safe='')}/open",
            method="PUT",
            body={},
        )
        assert_true(status == 200 and _json_ok(opened), f"Open update failed: {opened}")
        results["lead_open_update"] = {"last_opened_at": (opened or {}).get("last_opened_at")}

        status, imported = http_json(
            f"{API_BASE}/api/leads/import",
            method="POST",
            body={
                "rows": [
                    {
                        "lead_external_id": imported_contact,
                        "first_name": "Imported",
                        "last_name": "Lead",
                        "full_name": "Imported Lead",
                        "email": imported_email,
                        "mobile_phone": "5550001234",
                        "lead_source": "api_import",
                        "pipeline_status": "app_submitted",
                    }
                ]
            },
        )
        assert_true(status == 200 and _json_ok(imported), f"Lead import failed: {imported}")
        results["lead_import"] = {"added": (imported or {}).get("added"), "updated": (imported or {}).get("updated")}

        status, content_import = http_json(
            f"{API_BASE}/api/content/posts/import",
            method="POST",
            body={
                "actor": "backend-regression",
                "source_file": "backend-regression.json",
                "week_number": 1,
                "posts": [
                    {
                        "post_id": content_post_key,
                        "week_number": 1,
                        "day": 1,
                        "post_date": "2026-04-20",
                        "post_time": "09:30",
                        "platforms": ["facebook"],
                        "post_type": "social",
                        "topic": "Backend import",
                        "hook": "Backend hook",
                        "caption": "Backend caption https://insuredbylena.com GUIDE",
                        "reel_script": "",
                        "visual_prompt": "Blue graphic",
                        "asset_filename": "https://example.com/asset.jpg",
                        "cta": "GUIDE",
                        "hashtags_text": "#test",
                        "status": "draft",
                    }
                ],
            },
        )
        assert_true(status == 200 and _json_ok(content_import), f"Content import failed: {content_import}")
        results["content_import"] = {"imported": (content_import or {}).get("imported")}

        status, content_rows = supabase_rest(
            f"content_post?select=id,status,scheduled_for&post_id=eq.{quote(content_post_key, safe='')}&limit=1"
        )
        assert_true(status == 200 and isinstance(content_rows, list) and content_rows, f"Imported content row missing: {content_rows}")
        content_post_id = int(content_rows[0]["id"])
        results["content_post_id"] = content_post_id

        status, content_update = http_json(
            f"{API_BASE}/api/content/posts/{content_post_id}",
            method="PUT",
            body={
                "actor": "backend-regression",
                "topic": "Backend updated",
                "caption": "Updated caption https://insuredbylena.com GUIDE",
            },
        )
        assert_true(status == 200 and _json_ok(content_update), f"Content update failed: {content_update}")

        status, submit_review = http_json(
            f"{API_BASE}/api/content/posts/{content_post_id}/submit-review",
            method="POST",
            body={"actor": "backend-regression", "note": "submit"},
        )
        assert_true(status == 200 and _json_ok(submit_review), f"Submit review failed: {submit_review}")

        status, approve = http_json(
            f"{API_BASE}/api/content/posts/{content_post_id}/approve",
            method="POST",
            body={"actor": "backend-regression", "note": "approve", "scheduledFor": "2026-04-20T14:00:00"},
        )
        assert_true(status == 200 and _json_ok(approve), f"Approve failed: {approve}")

        status, schedule = http_json(
            f"{API_BASE}/api/content/posts/{content_post_id}/schedule",
            method="POST",
            body={"actor": "backend-regression", "note": "schedule", "scheduledFor": "2026-04-20T15:00:00"},
        )
        assert_true(status == 200 and _json_ok(schedule), f"Schedule failed: {schedule}")

        status, revisions = http_json(f"{API_BASE}/api/content/posts/{content_post_id}/revisions")
        revision_items = (revisions or {}).get("items") if isinstance(revisions, dict) else None
        assert_true(status == 200 and isinstance(revision_items, list) and revision_items, f"Revisions missing: {revisions}")
        revision_id = int(revision_items[0]["id"])
        results["content_revisions"] = {"count": len(revision_items)}

        status, restore = http_json(
            f"{API_BASE}/api/content/posts/{content_post_id}/restore",
            method="POST",
            body={"actor": "backend-regression", "revisionId": revision_id},
        )
        assert_true(status == 200 and _json_ok(restore), f"Restore failed: {restore}")

        status, publisher_status = http_json(f"{API_BASE}/api/content/publisher/status")
        assert_true(status == 200 and _json_ok(publisher_status), f"Publisher status failed: {publisher_status}")
        results["publisher_status"] = {"mode": (publisher_status or {}).get("mode")}

        status, publish_jobs = http_json(f"{API_BASE}/api/content/publish/jobs")
        assert_true(status == 200 and _json_ok(publish_jobs), f"Publish jobs failed: {publish_jobs}")
        results["publish_jobs"] = {"count": (publish_jobs or {}).get("count")}

        status, docs = http_json(
            f"{API_BASE}/api/leads/{quote(lead_external_id, safe='')}/documents",
            method="POST",
            body={
                "documentCategory": "general",
                "sourceUrl": "https://example.com/test-policy.pdf",
                "fileName": "test-policy.pdf",
                "notes": "Regression test document",
                "uploadedByEmail": "codex-regression@example.com",
            },
        )
        assert_true(status == 200 and _json_ok(docs), f"Document create failed: {docs}")
        document_id = int(((docs or {}).get("item") or {}).get("documentId") or 0)
        assert_true(document_id > 0, f"Document id missing: {docs}")
        results["document_create"] = {"documentId": document_id}

        status, doc_list = http_json(f"{API_BASE}/api/leads/{quote(lead_external_id, safe='')}/documents")
        doc_items = (doc_list or {}).get("items") if isinstance(doc_list, dict) else None
        assert_true(
            status == 200 and isinstance(doc_items, list) and any(int(item.get("documentId") or 0) == document_id for item in doc_items),
            f"Document list missing created item: {doc_list}",
        )

        status, doc_archived = http_json(f"{API_BASE}/api/lead-documents/{document_id}/archive", method="POST", body={})
        assert_true(status == 200 and _json_ok(doc_archived), f"Document archive failed: {doc_archived}")

        status, calendar_today = http_json(f"{API_BASE}/api/calendar/today")
        assert_true(status == 200 and _json_ok(calendar_today), f"Calendar today failed: {calendar_today}")
        status, calendar_week = http_json(f"{API_BASE}/api/calendar/week")
        assert_true(status == 200 and _json_ok(calendar_week), f"Calendar week failed: {calendar_week}")
        results["calendar"] = {
            "todayCount": (calendar_today or {}).get("count"),
            "weekCount": (calendar_week or {}).get("count"),
        }

        status, carrier_config = http_json(f"{API_BASE}/api/carrier-config")
        assert_true(status == 200 and _json_ok(carrier_config), f"Carrier config get failed: {carrier_config}")
        status, carrier_save = http_json(
            f"{API_BASE}/api/carrier-config",
            method="POST",
            body={"rows": (carrier_config or {}).get("rows") or []},
        )
        assert_true(status == 200 and _json_ok(carrier_save), f"Carrier config save failed: {carrier_save}")
        results["carrier_config"] = {"rows": len((carrier_config or {}).get("rows") or [])}

        print(
            json.dumps(
                {
                    "ok": True,
                    "apiBase": API_BASE,
                    "leadId": lead_id,
                    "contentPostId": content_post_id,
                    "results": results,
                }
            )
        )
        return 0
    finally:
        if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
            if content_post_id:
                supabase_rest(f"content_approval?content_post_id=eq.{content_post_id}", method="DELETE")
                supabase_rest(f"content_revision?content_post_id=eq.{content_post_id}", method="DELETE")
                supabase_rest(f"content_publish_job?content_post_id=eq.{content_post_id}", method="DELETE")
                supabase_rest(f"content_post?id=eq.{content_post_id}", method="DELETE")
            if lead_id:
                supabase_rest(f"call_desk_activity?lead_id=eq.{lead_id}", method="DELETE")
                supabase_rest(f"lead_document?lead_id=eq.{lead_id}", method="DELETE")
                supabase_rest(f"appointment?lead_id=eq.{lead_id}", method="DELETE")
                supabase_rest(f"lead_master?lead_id=eq.{lead_id}", method="DELETE")
            supabase_rest(f"lead_master?lead_external_id=eq.{quote(imported_contact, safe='')}", method="DELETE")
            supabase_rest(f"lead_master?email=eq.{quote(imported_email, safe='')}", method="DELETE")
            supabase_rest(f"lead_master?email=eq.{quote(email, safe='')}", method="DELETE")
            supabase_rest(f"lead_master?lead_external_id=eq.{quote(contact, safe='')}", method="DELETE")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        raise SystemExit(1)
    except Exception as exc:  # pragma: no cover
        print(json.dumps({"ok": False, "error": str(exc)}))
        raise SystemExit(1)
