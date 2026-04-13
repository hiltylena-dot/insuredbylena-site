#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote


class ContentHandlerMixin:
    def _handle_content_posts_get(self, query: str) -> None:
        import local_db_api as api
        try:
            params = parse_qs(query or "")
            status_filter = api._trim((params.get("status") or [""])[0])
            if api._use_supabase_backend():
                path = "content_post?select=*&order=scheduled_for.desc.nullslast,post_date.desc,post_time.desc,id.desc"
                if status_filter:
                    path += f"&status=eq.{quote(status_filter, safe='')}"
                rows = api._supabase_rest(path, method="GET") or []
                posts = [api._supabase_content_record_to_post(row) for row in rows if isinstance(row, dict)]
            else:
                with api.connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    api.ensure_content_studio_tables(conn)
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
                posts = [api.row_to_content_post(row) for row in rows]
            self._send_json({"ok": True, "items": posts, "posts": posts, "count": len(posts)})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_api_health_get(self) -> None:
        import local_db_api as api
        try:
            response = {
                "ok": True,
                "service": "insuredbylena-portal-api",
                "content_store_mode": api._content_store_mode(),
                "publisher_mode": api._publisher_mode(),
                "supabase_configured": bool(api.SUPABASE_URL and api.SUPABASE_SERVICE_ROLE_KEY),
                "buffer_configured": bool(api.BUFFER_API_KEY),
            }
            self._send_json(response)
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_api_version_get(self) -> None:
        import local_db_api as api
        try:
            self._send_json(
                {
                    "ok": True,
                    "service": api.SERVICE_NAME,
                    "buildSha": api.BUILD_SHA,
                    "buildTime": api.BUILD_TIME,
                    "revision": api.REVISION_NAME,
                    "contentStoreMode": api._content_store_mode(),
                    "publisherMode": api._publisher_mode(),
                    "supabaseConfigured": bool(api.SUPABASE_URL and api.SUPABASE_SERVICE_ROLE_KEY),
                    "googleCalendarConfigured": bool(api.GOOGLE_CALENDAR_WEB_APP_URL),
                }
            )
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_content_publisher_status_get(self) -> None:
        import local_db_api as api
        try:
            snapshot = api.publisher_status_snapshot()
            if api.BUFFER_API_KEY:
                try:
                    snapshot["organizations"] = api.buffer_fetch_organizations()
                except Exception as exc:
                    snapshot["organizations_error"] = str(exc)
                if api.BUFFER_ORGANIZATION_ID:
                    try:
                        snapshot["available_channels"] = api.buffer_fetch_channels(api.BUFFER_ORGANIZATION_ID)
                    except Exception as exc:
                        snapshot["available_channels_error"] = str(exc)
            self._send_json({"ok": True, "content_store_mode": api._content_store_mode(), **snapshot})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_content_publisher_test(self) -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()
            mode = api._publisher_mode()
            response = {"mode": mode}
            if mode == "buffer_graphql":
                response["organizations"] = api.buffer_fetch_organizations()
                if api.BUFFER_ORGANIZATION_ID:
                    response["channels"] = api.buffer_fetch_channels(api.BUFFER_ORGANIZATION_ID)
            elif mode == "webhook":
                response["webhook_url"] = api.CONTENT_SCHEDULER_WEBHOOK_URL
            else:
                response["message"] = "mock publisher active"
            self._send_json({"ok": True, **response})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_content_posts_import(self) -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()
            posts = data.get("posts") or data.get("rows") or []
            if not isinstance(posts, list):
                raise ValueError("'posts' must be an array")
            week_number = int(data.get("week_number") or data.get("weekNumber") or 0)
            actor = api._trim(data.get("actor")) or "portal"
            source_file = api._trim(data.get("source_file") or data.get("sourceFile"))
            if api._use_supabase_backend():
                imported, updated = api._supabase_content_import_posts(posts, week_number, actor, source_file)
            else:
                with api.connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    api.ensure_content_studio_tables(conn)
                    imported, updated = api.upsert_content_posts(conn, posts, week_number, actor, source_file)
                    conn.commit()
                api.mirror_db_to_drive()
            self._send_json({"ok": True, "imported": imported, "updated": updated})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_content_posts_import_buffer_current(self) -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()
            actor = api._trim(data.get("actor")) or "automation"
            source_file = api._trim(data.get("source_file") or data.get("sourceFile")) or "buffer-import.json"
            requested_path = api._trim(data.get("path") or data.get("file"))
            file_path = Path(requested_path) if requested_path else api.BUFFER_IMPORT_DEFAULT_PATH
            if not file_path.is_absolute():
                file_path = api.CONTENT_FILES_ROOT / file_path
            file_path = file_path.resolve()
            if api.CONTENT_FILES_ROOT not in file_path.parents and file_path != api.CONTENT_FILES_ROOT:
                raise ValueError("buffer_import_path_must_be_inside_content_root")
            if not file_path.exists():
                raise FileNotFoundError(f"buffer_import_file_not_found: {file_path}")

            parsed = json.loads(file_path.read_text(encoding="utf-8"))
            if not isinstance(parsed, list):
                raise ValueError("buffer_import_json_must_be_array")
            media_links_map = api.load_media_links_map()
            posts = api.convert_buffer_rows_to_content_posts(parsed, media_links_map=media_links_map)
            if not posts:
                raise ValueError("buffer_import_json_has_no_valid_rows")

            if api._use_supabase_backend():
                imported, updated = api._supabase_content_import_posts(posts, week_number=0, actor=actor, source_file=source_file)
            else:
                with api.connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    api.ensure_content_studio_tables(conn)
                    imported, updated = api.upsert_content_posts(conn, posts, week_number=0, actor=actor, source_file=source_file)
                    conn.commit()
                api.mirror_db_to_drive()
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
        import local_db_api as api
        try:
            content_post_id = int(post_id_raw)
            data = self._read_json_body()
            actor = api._trim(data.get("actor")) or "editor"
            if api._use_supabase_backend():
                current = api._supabase_content_get_post(content_post_id)
                if not current:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                api._supabase_content_insert_revision(content_post_id, actor, "manual_edit", api._content_snapshot_from_post(current))
                platforms = data.get("platforms")
                if isinstance(platforms, str):
                    platforms = [part.strip() for part in platforms.split(",") if part.strip()]
                if not isinstance(platforms, list):
                    platforms = current.get("platforms") or []
                hashtags_text = api._trim(data.get("hashtags_text") or data.get("hashtagsText"))
                if not hashtags_text:
                    hashtags = data.get("hashtags")
                    if isinstance(hashtags, list):
                        hashtags_text = " ".join([str(tag).strip() for tag in hashtags if str(tag).strip()])
                if not hashtags_text:
                    hashtags_text = current.get("hashtags_text") or ""
                payload = {
                    "post_date": api._trim(data.get("post_date") or data.get("postDate")) or current.get("post_date") or None,
                    "post_time": api._trim(data.get("post_time") or data.get("postTime")) or current.get("post_time") or None,
                    "scheduled_for": api._trim(data.get("scheduled_for") or data.get("scheduledFor")) or current.get("scheduled_for") or None,
                    "platforms_json": platforms,
                    "post_type": api._trim(data.get("post_type") or data.get("postType")) or current.get("post_type") or None,
                    "topic": api._trim(data.get("topic")) or current.get("topic") or None,
                    "hook": api._trim(data.get("hook")) or current.get("hook") or None,
                    "caption": api._trim(data.get("caption")) or current.get("caption") or None,
                    "reel_script": api._trim(data.get("reel_script") or data.get("reelScript")) or current.get("reel_script") or None,
                    "visual_prompt": api._trim(data.get("visual_prompt") or data.get("visualPrompt")) or current.get("visual_prompt") or None,
                    "canva_design_link": api._trim(data.get("canva_design_link") or data.get("canvaDesignLink")),
                    "asset_filename": api._trim(data.get("asset_filename") or data.get("assetFilename")) or current.get("asset_filename") or None,
                    "cta": api._trim(data.get("cta")) or current.get("cta") or None,
                    "hashtags_text": hashtags_text,
                    "updated_at": api.now_iso(),
                }
                updated = api._supabase_content_patch_post(content_post_id, payload)
                self._set_headers(200)
                self.wfile.write(json.dumps({"ok": True, "item": updated}).encode("utf-8"))
                return
            with api.connect_db() as conn:
                conn.row_factory = sqlite3.Row
                api.ensure_content_studio_tables(conn)
                row = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
                if not row:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                api.create_content_revision(conn, content_post_id, actor, "manual_edit")
                platforms = data.get("platforms")
                if isinstance(platforms, str):
                    platforms = [part.strip() for part in platforms.split(",") if part.strip()]
                if not isinstance(platforms, list):
                    platforms = json.loads(str(row["platforms_json"] or "[]"))
                hashtags_text = api._trim(data.get("hashtags_text") or data.get("hashtagsText"))
                if not hashtags_text:
                    hashtags = data.get("hashtags")
                    if isinstance(hashtags, list):
                        hashtags_text = " ".join([str(tag).strip() for tag in hashtags if str(tag).strip()])
                if not hashtags_text:
                    hashtags_text = str(row["hashtags_text"] or "")
                canva_design_link = api._trim(data.get("canva_design_link") or data.get("canvaDesignLink"))
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
                        api._trim(data.get("post_date") or data.get("postDate")),
                        api._trim(data.get("post_time") or data.get("postTime")),
                        api._trim(data.get("scheduled_for") or data.get("scheduledFor")),
                        json.dumps(platforms),
                        api._trim(data.get("post_type") or data.get("postType")),
                        api._trim(data.get("topic")),
                        api._trim(data.get("hook")),
                        api._trim(data.get("caption")),
                        api._trim(data.get("reel_script") or data.get("reelScript")),
                        api._trim(data.get("visual_prompt") or data.get("visualPrompt")),
                        canva_design_link,
                        api._trim(data.get("asset_filename") or data.get("assetFilename")),
                        api._trim(data.get("cta")),
                        hashtags_text,
                        content_post_id,
                    ),
                )
                conn.commit()
                updated = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
            api.mirror_db_to_drive()
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": api.row_to_content_post(updated)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))


    def _handle_content_post_revisions_get(self, post_id_raw: str) -> None:
        import local_db_api as api
        try:
            content_post_id = int(post_id_raw)
            if api._use_supabase_backend():
                rows = api._supabase_rest(
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
                with api.connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    api.ensure_content_studio_tables(conn)
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
        import local_db_api as api
        try:
            content_post_id = int(post_id_raw)
            data = self._read_json_body()
            revision_id = int(data.get("revision_id") or data.get("revisionId") or 0)
            actor = api._trim(data.get("actor")) or "editor"
            if revision_id <= 0:
                self._set_headers(400)
                self.wfile.write(json.dumps({"ok": False, "error": "revision_id_required"}).encode("utf-8"))
                return
            if api._use_supabase_backend():
                current = api._supabase_content_get_post(content_post_id)
                if not current:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                revision_rows = api._supabase_rest(
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
                api._supabase_content_insert_revision(content_post_id, actor, "restore_before_restore", api._content_snapshot_from_post(current))
                updated = api._supabase_content_patch_post(
                    content_post_id,
                    {
                        "post_date": api._trim(snapshot.get("post_date")) or None,
                        "post_time": api._trim(snapshot.get("post_time")) or None,
                        "scheduled_for": api._trim(snapshot.get("scheduled_for")) or None,
                        "platforms_json": snapshot.get("platforms") or [],
                        "post_type": api._trim(snapshot.get("post_type")) or None,
                        "topic": api._trim(snapshot.get("topic")) or None,
                        "hook": api._trim(snapshot.get("hook")) or None,
                        "caption": api._trim(snapshot.get("caption")) or None,
                        "reel_script": api._trim(snapshot.get("reel_script")) or None,
                        "visual_prompt": api._trim(snapshot.get("visual_prompt")) or None,
                        "canva_design_link": api._trim(snapshot.get("canva_design_link") or snapshot.get("canvaDesignLink")) or None,
                        "asset_filename": api._trim(snapshot.get("asset_filename")) or None,
                        "cta": api._trim(snapshot.get("cta")) or None,
                        "hashtags_text": api._trim(snapshot.get("hashtags_text")) or None,
                        "status": api._trim(snapshot.get("status")) or "draft",
                        "updated_at": api.now_iso(),
                    },
                )
                api._supabase_content_insert_revision(content_post_id, actor, f"restored_revision_{revision_id}", api._content_snapshot_from_post(updated))
                self._set_headers(200)
                self.wfile.write(json.dumps({"ok": True, "item": updated}).encode("utf-8"))
                return
            with api.connect_db() as conn:
                conn.row_factory = sqlite3.Row
                api.ensure_content_studio_tables(conn)
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
                api.create_content_revision(conn, content_post_id, actor, "restore_before_restore")
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
                        api._trim(snapshot.get("post_date")),
                        api._trim(snapshot.get("post_time")),
                        api._trim(snapshot.get("scheduled_for")),
                        json.dumps(snapshot.get("platforms") or []),
                        api._trim(snapshot.get("post_type")),
                        api._trim(snapshot.get("topic")),
                        api._trim(snapshot.get("hook")),
                        api._trim(snapshot.get("caption")),
                        api._trim(snapshot.get("reel_script")),
                        api._trim(snapshot.get("visual_prompt")),
                        api._trim(snapshot.get("canva_design_link") or snapshot.get("canvaDesignLink")),
                        api._trim(snapshot.get("asset_filename")),
                        api._trim(snapshot.get("cta")),
                        api._trim(snapshot.get("hashtags_text")),
                        api._trim(snapshot.get("status")) or "draft",
                        content_post_id,
                    ),
                )
                api.create_content_revision(conn, content_post_id, actor, f"restored_revision_{revision_id}")
                conn.commit()
                updated = conn.execute("SELECT * FROM content_post WHERE id = ? LIMIT 1", (content_post_id,)).fetchone()
            api.mirror_db_to_drive()
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": api.row_to_content_post(updated)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))


    def _handle_content_post_action(self, post_id_raw: str, action: str) -> None:
        import local_db_api as api
        try:
            content_post_id = int(post_id_raw)
            data = self._read_json_body()
            actor = api._trim(data.get("actor")) or "approver"
            note = api._trim(data.get("note"))
            if api._use_supabase_backend():
                current = api._supabase_content_get_post(content_post_id)
                if not current:
                    self._set_headers(404)
                    self.wfile.write(json.dumps({"ok": False, "error": "content_post_not_found"}).encode("utf-8"))
                    return
                if action == "submit-review":
                    updated = api._supabase_content_patch_post(
                        content_post_id,
                        {"status": "in_review", "updated_at": api.now_iso()},
                    )
                    api._supabase_content_insert_approval(content_post_id, "submitted", note, actor)
                elif action == "approve":
                    scheduled_for = api._trim(data.get("scheduled_for") or data.get("scheduledFor")) or current.get("scheduled_for") or None
                    updated = api._supabase_content_patch_post(
                        content_post_id,
                        {
                            "status": "approved",
                            "scheduled_for": scheduled_for,
                            "approved_by": actor,
                            "approved_at": api.now_iso(),
                            "last_publish_error": None,
                            "updated_at": api.now_iso(),
                        },
                    )
                    api._supabase_content_insert_approval(content_post_id, "approved", note, actor)
                elif action == "request-changes":
                    updated = api._supabase_content_patch_post(
                        content_post_id,
                        {"status": "draft", "updated_at": api.now_iso()},
                    )
                    api._supabase_content_insert_approval(content_post_id, "request_changes", note, actor)
                elif action == "schedule":
                    scheduled_for = api._trim(data.get("scheduled_for") or data.get("scheduledFor"))
                    if not scheduled_for:
                        row_post_date = api._trim(current.get("post_date"))
                        row_post_time = api._trim(current.get("post_time")) or "09:00"
                        scheduled_for = f"{row_post_date}T{row_post_time}:00"
                    updated = api._supabase_content_patch_post(
                        content_post_id,
                        {"status": "scheduled", "scheduled_for": scheduled_for, "updated_at": api.now_iso()},
                    )
                    api._supabase_content_insert_approval(content_post_id, "scheduled", note, actor)
                else:
                    self._set_headers(400)
                    self.wfile.write(json.dumps({"ok": False, "error": "unsupported_action"}).encode("utf-8"))
                    return
                self._set_headers(200)
                self.wfile.write(json.dumps({"ok": True, "item": updated}).encode("utf-8"))
                return
            with api.connect_db() as conn:
                conn.row_factory = sqlite3.Row
                api.ensure_content_studio_tables(conn)
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
                    scheduled_for = api._trim(data.get("scheduled_for") or data.get("scheduledFor"))
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
                        (scheduled_for, actor, api.now_iso(), content_post_id),
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
                    scheduled_for = api._trim(data.get("scheduled_for") or data.get("scheduledFor"))
                    if not scheduled_for:
                        row_post_date = api._trim(row["post_date"])
                        row_post_time = api._trim(row["post_time"]) or "09:00"
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
            api.mirror_db_to_drive()
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "item": api.row_to_content_post(updated)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))


    def _handle_content_publish_run(self) -> None:
        import local_db_api as api
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
            sync_result = api.sync_public_media_links()
            sync_error = ""
            if not sync_result.get("ok") and not sync_result.get("skipped"):
                sync_error = api._trim(sync_result.get("error")) or api._trim(sync_result.get("reason"))
            media_links_map = api.load_media_links_map()
            if api._use_supabase_backend():
                if requested_post_ids:
                    id_filter = ",".join(str(item) for item in requested_post_ids)
                    rows = api._supabase_rest(
                        f"content_post?select=*&id=in.({id_filter})&status=in.(approved,scheduled)&order=scheduled_for.asc.nullsfirst,post_date.asc,post_time.asc,id.asc",
                        method="GET",
                    ) or []
                else:
                    rows = api._supabase_rest(
                        f"content_post?select=*&status=in.(approved,scheduled)&order=scheduled_for.asc.nullsfirst,post_date.asc,post_time.asc,id.asc&limit={limit}",
                        method="GET",
                    ) or []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    item = api._supabase_content_record_to_post(row)
                    if not api._trim(item["scheduled_for"]):
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
                    if api._trim(item["scheduler_external_id"]) and str(item.get("status")).lower() == "published":
                        duplicate_reason = "this row already has a scheduler external id"
                    if duplicate_reason:
                        api._supabase_content_patch_post(
                            int(item["id"]),
                            {
                                "status": "draft",
                                "last_publish_error": f"duplicate_publish_skipped: {duplicate_reason}",
                                "updated_at": api.now_iso(),
                            },
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "skipped_duplicate", "reason": duplicate_reason})
                        continue
                    primary_platform = item["platforms"][0] if item["platforms"] else ""
                    resolved_asset = api.resolve_media_url(
                        item["asset_filename"],
                        int(item.get("week_number") or 0),
                        int(item.get("day") or 0),
                        primary_platform,
                        media_links_map,
                    )
                    api._supabase_content_patch_post(int(item["id"]), {"status": "publishing", "last_publish_error": None, "updated_at": api.now_iso()})
                    if resolved_asset and resolved_asset != item["asset_filename"]:
                        api._supabase_content_patch_post(int(item["id"]), {"asset_filename": resolved_asset, "updated_at": api.now_iso()})
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
                    job_id = api._supabase_content_insert_publish_job(
                        {
                            "content_post_id": int(item["id"]),
                            "scheduler": api._publisher_mode() if api._publisher_mode() != "webhook" else api.CONTENT_SCHEDULER_NAME,
                            "request_json": request_payload,
                            "status": "publishing",
                        }
                    )
                    try:
                        unresolved_id = api._extract_drive_file_id(item["asset_filename"])
                        if unresolved_id and api._is_placeholder_file_id(unresolved_id):
                            raise RuntimeError(
                                f"unresolved_media_placeholder: {unresolved_id} (add mapping in {api.MEDIA_LINKS_CSV_PATH})"
                            )
                        response_payload = api.publish_content_payload(request_payload)
                        external_id = api._trim(response_payload.get("id")) or api._trim(response_payload.get("postId"))
                        api._supabase_content_patch_post(
                            int(item["id"]),
                            {
                                "status": "published",
                                "scheduler_external_id": external_id or item.get("scheduler_external_id") or None,
                                "last_publish_error": None,
                                "updated_at": api.now_iso(),
                            },
                        )
                        api._supabase_rest(
                            f"content_publish_job?id=eq.{job_id}",
                            method="PATCH",
                            body={"status": "published", "response_json": response_payload, "completed_at": api.now_iso()},
                            extra_headers={"Prefer": "return=minimal"},
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "published"})
                    except Exception as publish_error:
                        err = str(publish_error)
                        api._supabase_content_patch_post(
                            int(item["id"]),
                            {"status": "failed", "last_publish_error": err, "updated_at": api.now_iso()},
                        )
                        api._supabase_rest(
                            f"content_publish_job?id=eq.{job_id}",
                            method="PATCH",
                            body={"status": "failed", "error_message": err, "completed_at": api.now_iso()},
                            extra_headers={"Prefer": "return=minimal"},
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "failed", "error": err})
                response_payload = {"ok": True, "processed": len(results), "results": results, "media_sync": sync_result}
                if sync_error:
                    response_payload["media_sync_error"] = sync_error
                self._set_headers(200)
                self.wfile.write(json.dumps(response_payload).encode("utf-8"))
                return
            with api.connect_db() as conn:
                conn.row_factory = sqlite3.Row
                api.ensure_content_studio_tables(conn)
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
                    item = api.row_to_content_post(row)
                    if not api._trim(item["scheduled_for"]):
                        results.append(
                            {
                                "id": item["id"],
                                "post_id": item["post_id"],
                                "status": "skipped_missing_schedule",
                                "reason": "scheduled_for_required",
                            }
                        )
                        continue
                    duplicate_reason = api._published_duplicate_reason(conn, int(item["id"]), str(item["post_id"]))
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
                    resolved_asset = api.resolve_media_url(
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
                            (item["id"], api._publisher_mode() if api._publisher_mode() != "webhook" else api.CONTENT_SCHEDULER_NAME, json.dumps(request_payload)),
                        ).lastrowid
                    )
                    try:
                        unresolved_id = api._extract_drive_file_id(item["asset_filename"])
                        if unresolved_id and api._is_placeholder_file_id(unresolved_id):
                            raise RuntimeError(
                                f"unresolved_media_placeholder: {unresolved_id} (add mapping in {api.MEDIA_LINKS_CSV_PATH})"
                            )
                        response_payload = api.publish_content_payload(request_payload)
                        external_id = api._trim(response_payload.get("id")) or api._trim(response_payload.get("postId"))
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
                            (json.dumps(response_payload), api.now_iso(), job_id),
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
                            (err, api.now_iso(), job_id),
                        )
                        results.append({"id": item["id"], "post_id": item["post_id"], "status": "failed", "error": err})
                conn.commit()
            api.mirror_db_to_drive()
            response_payload = {"ok": True, "processed": len(results), "results": results, "media_sync": sync_result}
            if sync_error:
                response_payload["media_sync_error"] = sync_error
            self._set_headers(200)
            self.wfile.write(json.dumps(response_payload).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))


    def _handle_content_publish_jobs_get(self, query: str) -> None:
        import local_db_api as api
        try:
            params = parse_qs(query or "")
            limit = int((params.get("limit") or ["50"])[0] or "50")
            if api._use_supabase_backend():
                rows = api._supabase_rest(
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
                with api.connect_db() as conn:
                    conn.row_factory = sqlite3.Row
                    api.ensure_content_studio_tables(conn)
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
