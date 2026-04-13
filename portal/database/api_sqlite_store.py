#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def normalize_payload(data: dict) -> dict:
    import api_support as api
    contact_id = api._trim(data.get("contactId"))
    first_name = api._trim(data.get("firstName"))
    last_name = api._trim(data.get("lastName"))
    full_name = " ".join(part for part in [first_name, last_name] if part).strip()
    payload = {
        "lead_external_id": contact_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "mobile_phone": api._trim(data.get("phone")),
        "email": api._trim(data.get("email")),
        "notes": api._trim(data.get("notes")),
        "raw_tags": api._trim(data.get("tags")),
        "last_activity_at_source": api._trim(data.get("lastActivity")) or api.now_iso(),
        "age": api._trim(data.get("age")),
        "tobacco": api._trim(data.get("tobacco")),
        "health_posture": api._trim(data.get("healthPosture")),
        "disposition": api._trim(data.get("disposition")),
        "carrier_match": api._trim(data.get("carrierMatch")),
        "confidence": api._trim(data.get("confidence")),
        "pipeline_status": api._trim(data.get("pipelineStatus") or data.get("pipeline_status")),
        "calendar_event_id": api._trim(data.get("calendarEventId") or data.get("calendar_event_id")),
        "next_appointment_time": api._trim(data.get("nextAppointmentTime") or data.get("next_appointment_time")),
        "last_opened_at": api._trim(data.get("lastOpenedAt") or data.get("last_opened_at")),
    }
    return payload

def _nv(value: Any) -> str | None:
    import api_support as api
    value = api._trim(value)
    return value or None

def _parse_local_iso(dt_raw: str) -> datetime:
    import api_support as api
    cleaned = api._trim(dt_raw).replace("Z", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return dt

def _is_due_by_schedule(scheduled_for: str, now_dt: datetime) -> bool:
    import api_support as api
    value = api._trim(scheduled_for)
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

def ensure_lead_master_columns(conn: sqlite3.Connection) -> None:
    import api_support as api
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
    import api_support as api
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
    import api_support as api
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
    import api_support as api
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
    import api_support as api
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

def upsert_content_posts(conn: sqlite3.Connection, posts: list[dict], week_number: int, actor: str, source_file: str) -> tuple[int, int]:
    import api_support as api
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
    import api_support as api
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

def get_agent_carrier_config_rows(conn: sqlite3.Connection) -> list[dict]:
    import api_support as api
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
    import api_support as api
    ensure_agent_carrier_config_table(conn)
    saved = 0
    for row in rows:
        carrier_name = api._trim(row.get("carrier_name") or row.get("carrierName"))
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
                api._trim(row.get("writing_number") or row.get("writingNumber")),
                api._trim(row.get("portal_url") or row.get("portalUrl")),
                api._trim(row.get("support_phone") or row.get("supportPhone")),
            ),
        )
        saved += 1
    return saved

def import_rows(conn: sqlite3.Connection, rows: list[dict]) -> dict:
    import api_support as api
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
    import api_support as api
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
    import api_support as api
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
