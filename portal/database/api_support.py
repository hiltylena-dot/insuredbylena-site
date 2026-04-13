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

import api_publisher as _api_publisher
import api_sqlite_store as _api_sqlite_store
import api_supabase as _api_supabase


for _module in (_api_publisher, _api_sqlite_store, _api_supabase):
    for _name in dir(_module):
        if _name.startswith("__"):
            continue
        globals()[_name] = getattr(_module, _name)
