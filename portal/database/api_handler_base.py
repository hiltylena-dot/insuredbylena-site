#!/usr/bin/env python3
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from typing import Any
from urllib.parse import urlparse


class BackendBaseHandlerMixin:
    def _set_headers(self, status: int = 200) -> None:
        import local_db_api as api
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        import local_db_api as api
        self._set_headers(status)
        self.wfile.write(json.dumps(payload).encode("utf-8"))


    def _send_error(self, message: str, status: int = 500) -> None:
        import local_db_api as api
        self._send_json({"ok": False, "error": str(message)}, status=status)


    def _send_not_found(self) -> None:
        import local_db_api as api
        self._send_error("not_found", status=404)


    def _path_parts(self) -> list[str]:
        import local_db_api as api
        return [part for part in urlparse(self.path).path.split("/") if part]


    def do_OPTIONS(self) -> None:
        import local_db_api as api
        self._set_headers(204)


    def do_POST(self) -> None:
        import local_db_api as api
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
        import local_db_api as api
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
        import local_db_api as api
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


    def _read_json_body(self) -> dict:
        import local_db_api as api
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        data = json.loads(raw or "{}")
        if not isinstance(data, dict):
            raise ValueError("Expected a JSON object body")
        return data


