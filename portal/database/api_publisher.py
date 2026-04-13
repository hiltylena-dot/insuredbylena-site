#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import mimetypes
import re
import subprocess
from datetime import datetime, timezone
from typing import Any
from urllib import error as urlerror, request as urlrequest


def _normalize_drive_url(url: str) -> str:
    import api_support as api
    value = api._trim(url)
    if not value:
        return ""
    match = re.search(r"drive\.google\.com/file/d/([A-Za-z0-9_-]+)", value)
    if match:
        return f"https://drive.google.com/uc?export=download&id={match.group(1)}"
    return value

def _extract_drive_file_id(url: str) -> str:
    import api_support as api
    value = api._trim(url)
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
    import api_support as api
    clean = api._trim(url).split("?", 1)[0]
    guessed, _ = mimetypes.guess_type(clean)
    return api._trim(guessed).lower()

def _publisher_mode() -> str:
    import api_support as api
    mode = api._trim(api.CONTENT_PUBLISHER_MODE).lower()
    if mode:
        return mode
    if api.BUFFER_API_KEY:
        return "buffer_graphql"
    if api.CONTENT_SCHEDULER_WEBHOOK_URL:
        return "webhook"
    return "mock"

def _buffer_channel_env_map() -> dict[str, str]:
    import api_support as api
    return {
        "instagram": api._trim(api.BUFFER_CHANNEL_ID_INSTAGRAM),
        "facebook": api._trim(api.BUFFER_CHANNEL_ID_FACEBOOK),
        "tiktok": api._trim(api.BUFFER_CHANNEL_ID_TIKTOK),
    }

def sync_public_media_links() -> dict[str, Any]:
    import api_support as api
    if not api.PUBLIC_MEDIA_SYNC_SCRIPT.exists():
        return {"ok": False, "skipped": True, "reason": "sync_script_missing"}
    try:
        proc = subprocess.run(
            ["python3", str(api.PUBLIC_MEDIA_SYNC_SCRIPT)],
            capture_output=True,
            text=True,
            check=False,
            timeout=120.0,
        )
        stdout = api._trim(proc.stdout)
        stderr = api._trim(proc.stderr)
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
    import api_support as api
    return api._trim(file_id).upper().startswith("FILE_ID_")

def _candidate_asset_ids(week_number: int, day_in_week: int, platform: str) -> list[str]:
    import api_support as api
    p = api._trim(platform).lower()
    absolute_day = ((int(week_number or 0) - 1) * 7) + int(day_in_week or 0)
    return [
        f"w{week_number}-d{day_in_week}-{p}",
        f"w{week_number}d{day_in_week}-{p}",
        f"w{week_number}-d{absolute_day}-{p}",
        f"w{week_number}d{absolute_day}-{p}",
    ]

def _is_media_mime(mime_type: str) -> bool:
    import api_support as api
    value = api._trim(mime_type).lower()
    return value.startswith("image/") or value.startswith("video/")

def _load_asset_filename_hints() -> dict[str, str]:
    import api_support as api
    global _ASSET_HINTS_CACHE
    if _ASSET_HINTS_CACHE is not None:
        return _ASSET_HINTS_CACHE

    hints: dict[str, str] = {}
    for file_path in sorted(api.CONTENT_FILES_ROOT.glob("WEEK*_SCHEDULER_EXPORT.json")):
        try:
            rows = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            post_id = api._trim(row.get("post_id"))
            asset_filename = api._trim(row.get("asset_filename"))
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
    import api_support as api
    if not api._trim(query):
        return None
    try:
        cmd = [
            "gog",
            "--client",
            api.GOG_CLIENT,
            "--account",
            api.GOG_ACCOUNT,
            "--json",
            "drive",
            "search",
            query,
            "--results-only",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=4.0)
        if proc.returncode != 0:
            return None
        output = api._trim(proc.stdout)
        payload = json.loads(output) if output else []
    except Exception:
        return None
    rows = payload if isinstance(payload, list) else []
    matches = [row for row in rows if isinstance(row, dict) and _is_media_mime(api._trim(row.get("mimeType")))]
    if not matches:
        return None
    exact = [row for row in matches if api._trim(row.get("name")).lower() == api._trim(query).lower()]
    candidates = exact or matches
    candidates.sort(key=lambda row: api._parse_iso_like(api._trim(row.get("modifiedTime"))), reverse=True)
    return candidates[0] if candidates else None

def _ensure_drive_public_read(file_id: str) -> None:
    import api_support as api
    if not api._trim(file_id):
        return
    try:
        api.run_gog_json(
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
    import api_support as api
    key = api._trim(file_id_placeholder).upper()
    if not key or not _is_placeholder_file_id(key):
        return ""
    if key in _DRIVE_MEDIA_CACHE:
        return _DRIVE_MEDIA_CACHE[key]

    hints = _load_asset_filename_hints()
    absolute_day = ((int(week_number or 0) - 1) * 7) + int(day_in_week or 0)
    platform_slug = api._trim(platform).lower()
    queries: list[str] = []

    hinted = api._trim(hints.get(key))
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
        q = api._trim(query)
        if not q or q.lower() in seen:
            continue
        seen.add(q.lower())
        found = _search_drive_for_media(q)
        if not found:
            continue
        found_id = api._trim(found.get("id"))
        if not found_id:
            continue
        _ensure_drive_public_read(found_id)
        resolved = f"https://drive.google.com/uc?export=download&id={found_id}"
        _DRIVE_MEDIA_CACHE[key] = resolved
        return resolved
    return ""

def load_media_links_map() -> dict[str, str]:
    import api_support as api
    by_key: dict[str, str] = {}
    for csv_path in (api.MEDIA_LINKS_CSV_PATH, api.MEDIA_LINKS_TEMPLATE_CSV_PATH):
        if not csv_path.exists():
            continue
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
                reader = csv.DictReader(fp)
                for row in reader:
                    if not isinstance(row, dict):
                        continue
                    key = api._trim(row.get("key"))
                    asset_id = api._trim(row.get("asset_id") or row.get("asset") or row.get("placeholder"))
                    media_url = _normalize_drive_url(api._trim(row.get("media_url") or row.get("url")))
                    file_id = api._trim(row.get("file_id") or row.get("id"))
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
    import api_support as api
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
        found = api._trim(media_links_map.get(key))
        if found:
            return _normalize_drive_url(found)
    if is_placeholder and allow_auto_drive_lookup and api.CONTENT_AUTO_DRIVE_MEDIA_LOOKUP:
        auto = _auto_resolve_drive_media_url(file_id, week_number, day_in_week, platform)
        if auto:
            return _normalize_drive_url(auto)
    return normalized

def _remove_hashtag_only_lines(caption: str) -> str:
    import api_support as api
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
    import api_support as api
    tags = []
    for token in str(caption or "").replace("\n", " ").split(" "):
        token = token.strip()
        if token.startswith("#") and len(token) > 1:
            tags.append(token)
    return " ".join(tags)

def _platform_slug(platform: str) -> str:
    import api_support as api
    p = str(platform or "").strip().lower()
    if p == "instagram":
        return "ig"
    if p == "facebook":
        return "fb"
    if p == "tiktok":
        return "tt"
    return (p[:2] or "na")

def convert_buffer_rows_to_content_posts(rows: list[dict], media_links_map: dict[str, str] | None = None) -> list[dict]:
    import api_support as api
    valid_rows = [row for row in rows if isinstance(row, dict) and api._trim(row.get("day")) and api._trim(row.get("platform"))]
    if not valid_rows:
        return []
    media_links_map = media_links_map or {}

    unique_dates = sorted({api._trim(row.get("day")) for row in valid_rows if api._trim(row.get("day"))})
    base_date = None
    if unique_dates:
        base_date = datetime.fromisoformat(f"{unique_dates[0]}T00:00:00")

    content_posts = []
    for row in valid_rows:
        post_date = api._trim(row.get("day"))
        post_time = api._trim(row.get("time")) or "09:00"
        platform = api._trim(row.get("platform"))
        caption_raw = api._trim(row.get("caption"))
        caption_clean = _remove_hashtag_only_lines(caption_raw)
        hashtags_text = _hashtags_text_from_caption(caption_raw)
        hook = ""
        for line in caption_clean.splitlines():
            if api._trim(line):
                hook = api._trim(line)
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
            api._trim(row.get("media_url")),
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

def publish_to_scheduler(payload: dict) -> dict:
    import api_support as api
    if not api.CONTENT_SCHEDULER_WEBHOOK_URL:
        raise RuntimeError("api.CONTENT_SCHEDULER_WEBHOOK_URL not configured")
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api.CONTENT_SCHEDULER_API_KEY:
        headers["Authorization"] = f"Bearer {api.CONTENT_SCHEDULER_API_KEY}"
    req = urlrequest.Request(
        api.CONTENT_SCHEDULER_WEBHOOK_URL,
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
    import api_support as api
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
    import api_support as api
    if not api.BUFFER_API_KEY:
        raise RuntimeError("api.BUFFER_API_KEY not configured")
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    response = _http_json_request(
        api.BUFFER_API_BASE_URL,
        payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api.BUFFER_API_KEY}",
            "Accept": "application/json",
            "User-Agent": "InsuredByLenaPublisher/1.0",
        },
        timeout=30.0,
    )
    errors = response.get("errors")
    if isinstance(errors, list) and errors:
        messages = []
        for item in errors:
            if isinstance(item, dict) and api._trim(item.get("message")):
                messages.append(api._trim(item.get("message")))
        raise RuntimeError("; ".join(messages) or "buffer_graphql_error")
    return response

def buffer_fetch_organizations() -> list[dict]:
    import api_support as api
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
    import api_support as api
    if not api._trim(organization_id):
        raise RuntimeError("api.BUFFER_ORGANIZATION_ID not configured")
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
    import api_support as api
    global _BUFFER_CHANNEL_CACHE
    if _BUFFER_CHANNEL_CACHE is not None:
        return _BUFFER_CHANNEL_CACHE
    explicit = _buffer_channel_env_map()
    if all(explicit.values()) or not (api.BUFFER_API_KEY and api.BUFFER_ORGANIZATION_ID):
        _BUFFER_CHANNEL_CACHE = explicit
        return explicit

    discovered = dict(explicit)
    channels = buffer_fetch_channels(api.BUFFER_ORGANIZATION_ID)
    for channel in channels:
        service = api._trim(channel.get("service")).lower()
        channel_id = api._trim(channel.get("id"))
        if not channel_id:
            continue
        if service in discovered and not discovered[service]:
            discovered[service] = channel_id
    _BUFFER_CHANNEL_CACHE = discovered
    return discovered

def buffer_channel_id_for_platform(platform: str) -> str:
    import api_support as api
    platform_key = api._trim(platform).lower()
    channel_id = api._trim(_buffer_autodiscovered_channel_map().get(platform_key))
    if channel_id:
        return channel_id
    raise RuntimeError(f"buffer_channel_not_configured_for_platform: {platform_key}")

def _iso_with_timezone(dt_raw: str, fallback_date: str = "", fallback_time: str = "") -> str:
    import api_support as api
    raw = api._trim(dt_raw)
    if not raw and api._trim(fallback_date):
        time_part = api._trim(fallback_time) or "09:00"
        raw = f"{api._trim(fallback_date)}T{time_part}:00"
    if not raw:
        return ""
    dt = api._parse_local_iso(raw)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _buffer_assets_input(asset_url: str) -> dict:
    import api_support as api
    url = api._trim(asset_url)
    if not url:
        return {}
    mime_type = _mime_type_for_url(url)
    if mime_type.startswith("video/"):
        return {"videos": [{"url": url}]}
    return {"images": [{"url": url}]}

def _to_graphql_literal(value: Any) -> str:
    import api_support as api
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
    import api_support as api
    platforms = payload.get("platforms") or []
    platform = ""
    if isinstance(platforms, list) and platforms:
        platform = api._trim(platforms[0]).lower()
    if not platform:
        raise RuntimeError("buffer_publish_requires_single_platform")
    channel_id = buffer_channel_id_for_platform(platform)
    due_at = _iso_with_timezone(
        api._trim(payload.get("scheduled_for")),
        fallback_date=api._trim(payload.get("post_date")),
        fallback_time=api._trim(payload.get("post_time")),
    )
    mode = "customScheduled" if due_at else "addToQueue"
    fields = [
        'text: %s' % json.dumps(api._trim(payload.get("caption"))),
        'channelId: %s' % json.dumps(channel_id),
        "schedulingType: automatic",
        f"mode: {mode}",
        'source: "insuredbylena-content-studio"',
        "aiAssisted: true",
    ]
    metadata_literal = ""
    raw_post_type = api._trim(payload.get("post_type")).lower()
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
    assets = _buffer_assets_input(api._trim(payload.get("asset_filename")))
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
    message = api._trim(result.get("message"))
    if message:
        raise RuntimeError(message)
    post = result.get("post") or {}
    if not isinstance(post, dict) or not api._trim(post.get("id")):
        raise RuntimeError("buffer_create_post_missing_post_id")
    return {
        "ok": True,
        "id": api._trim(post.get("id")),
        "post": post,
        "publisher": "buffer_graphql",
    }

def publisher_status_snapshot() -> dict:
    import api_support as api
    mode = _publisher_mode()
    snapshot = {
        "mode": mode,
        "webhook_configured": bool(api.CONTENT_SCHEDULER_WEBHOOK_URL),
        "buffer_api_key_configured": bool(api.BUFFER_API_KEY),
        "buffer_organization_id": api.BUFFER_ORGANIZATION_ID,
        "buffer_channels": _buffer_channel_env_map(),
    }
    if mode == "buffer_graphql" and api.BUFFER_API_KEY:
        try:
            snapshot["buffer_channels"] = _buffer_autodiscovered_channel_map()
        except Exception as exc:
            snapshot["channel_discovery_error"] = str(exc)
    return snapshot

def publish_content_payload(payload: dict) -> dict:
    import api_support as api
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
