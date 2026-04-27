#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
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

PORTAL_URL = os.getenv("PORTAL_URL", "https://insuredbylena.com/portal/").strip()
API_BASE = os.getenv("API_BASE", "https://insuredbylena-portal-api-607620457436.us-central1.run.app").strip().rstrip("/")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()


def http_json(url: str, *, method: str = "GET", body: dict | list | None = None, headers: dict | None = None) -> tuple[int, object]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    req = urlrequest.Request(url, data=payload, method=method.upper(), headers={
        "Content-Type": "application/json",
        **(headers or {}),
    })
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


def http_text(url: str) -> tuple[int, str]:
    req = urlrequest.Request(url, method="GET")
    with urlrequest.urlopen(req, timeout=120) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def supabase_rest(path: str, *, method: str = "GET", body: dict | list | None = None, prefer: str = "") -> tuple[int, object]:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required for smoke tests.")
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


def future_utc_iso(days_ahead: int, hour: int, minute: int) -> str:
    target = datetime.now(timezone.utc) + timedelta(days=days_ahead)
    target = target.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target.isoformat().replace("+00:00", "Z")


def main() -> int:
    stamp = str(int(time.time()))
    phone = f"555{stamp[-7:]}"
    email = f"codex-smoke-{stamp}@example.com"
    contact_one = f"SMOKE-{stamp}-A"
    contact_two = f"SMOKE-{stamp}-B"
    schedule_one = future_utc_iso(1, 15, 0)
    schedule_two = future_utc_iso(2, 16, 0)
    lead_id = None
    lead_external_id = contact_one
    appointment_id = None
    document_id = None
    document_status = "not_run"
    try:
        status, html = http_text(PORTAL_URL)
        assert_true(status == 200, f"Portal returned {status}")
        assert_true("Insurance Ops Dashboard" in html, "Portal title missing")
        assert_true("app.js?v=" in html and "styles.css?v=" in html, "Portal assets missing")

        status, health = http_json(f"{API_BASE}/api/health")
        assert_true(status == 200 and isinstance(health, dict) and health.get("ok"), f"API health failed: {health}")

        save_payload = {
            "contactId": contact_one,
            "firstName": "Codex",
            "lastName": "Smoke",
            "phone": phone,
            "email": email,
            "disposition": "callback",
            "shouldSchedule": True,
            "nextAppointmentTime": schedule_one,
            "notes": "Initial smoke save",
        }
        status, rpc_one = supabase_rest("rpc/portal_save_call_desk", method="POST", body={"p_payload": save_payload})
        assert_true(status in (200, 201) and isinstance(rpc_one, dict) and rpc_one.get("ok"), f"RPC save 1 failed: {rpc_one}")

        save_payload["contactId"] = contact_two
        save_payload["nextAppointmentTime"] = schedule_two
        save_payload["notes"] = "Second smoke save"
        status, rpc_two = supabase_rest("rpc/portal_save_call_desk", method="POST", body={"p_payload": save_payload})
        assert_true(status in (200, 201) and isinstance(rpc_two, dict) and rpc_two.get("ok"), f"RPC save 2 failed: {rpc_two}")

        status, leads = supabase_rest(
            f"lead_master?select=lead_id,lead_external_id,full_name,next_appointment_time&or=(mobile_phone.eq.{quote(phone, safe='')},email.eq.{quote(email, safe='')})"
        )
        assert_true(status == 200 and isinstance(leads, list) and len(leads) == 1, f"Expected 1 deduped lead, got: {leads}")
        lead_id = int(leads[0]["lead_id"])
        lead_external_id = str(leads[0].get("lead_external_id") or contact_one)

        status, appointments = supabase_rest(
            f"appointment?select=appointment_id,lead_id,booking_status,booking_date&lead_id=eq.{lead_id}&owner=eq.call_desk&booking_status=in.(Booked,Rescheduled,Pending)"
        )
        assert_true(status == 200 and isinstance(appointments, list) and len(appointments) == 1, f"Expected 1 active appointment, got: {appointments}")
        appointment_id = int(appointments[0]["appointment_id"])

        status, docs = http_json(
            f"{API_BASE}/api/leads/{quote(lead_external_id, safe='')}/documents",
            method="POST",
            body={
                "documentCategory": "general",
                "sourceUrl": "https://example.com/test-policy.pdf",
                "fileName": "test-policy.pdf",
                "notes": "Smoke test document",
                "uploadedByEmail": "codex-smoke@example.com",
            },
        )
        if status == 200 and isinstance(docs, dict) and docs.get("ok"):
            document_id = int((docs.get("item") or {}).get("documentId") or 0)
            assert_true(document_id > 0, f"Document id missing: {docs}")

            status, doc_list = http_json(f"{API_BASE}/api/leads/{quote(lead_external_id, safe='')}/documents")
            items = doc_list.get("items") if isinstance(doc_list, dict) else None
            assert_true(status == 200 and isinstance(items, list) and any(int(item.get("documentId") or 0) == document_id for item in items), f"Document list missing item: {doc_list}")

            status, archived = http_json(f"{API_BASE}/api/lead-documents/{document_id}/archive", method="POST", body={})
            assert_true(status == 200 and isinstance(archived, dict) and archived.get("ok"), f"Document archive failed: {archived}")

            status, doc_list = http_json(f"{API_BASE}/api/leads/{quote(lead_external_id, safe='')}/documents")
            items = doc_list.get("items") if isinstance(doc_list, dict) else None
            assert_true(status == 200 and isinstance(items, list) and not any(int(item.get("documentId") or 0) == document_id for item in items), f"Archived document still visible: {doc_list}")
            document_status = "passed"
        else:
            document_status = f"skipped: {(docs or {}).get('error', 'document hub unavailable')}"

        print(json.dumps({
            "ok": True,
            "portal": PORTAL_URL,
            "apiBase": API_BASE,
            "leadId": lead_id,
            "appointmentId": appointment_id,
            "documentId": document_id,
            "documentStatus": document_status,
        }))
        return 0
    finally:
        if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
            if lead_id:
                supabase_rest(f"call_desk_activity?lead_id=eq.{lead_id}", method="DELETE")
                supabase_rest(f"lead_document?lead_id=eq.{lead_id}", method="DELETE")
                supabase_rest(f"appointment?lead_id=eq.{lead_id}", method="DELETE")
                supabase_rest(f"lead_master?lead_id=eq.{lead_id}", method="DELETE")
            else:
                supabase_rest(f"lead_master?email=eq.{quote(email, safe='')}", method="DELETE")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        raise SystemExit(1)
    except Exception as exc:  # pragma: no cover
        print(json.dumps({"ok": False, "error": str(exc)}))
        raise SystemExit(1)
