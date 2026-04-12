#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3


class LeadHandlerMixin:
    def _handle_sync(self, force_external_id: str = "") -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()
            if force_external_id and not data.get("contactId"):
                data["contactId"] = force_external_id
            if api._use_supabase_backend():
                result = api._supabase_portal_save_call_desk(api._lead_sync_payload_from_request(data, force_external_id))
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
            payload = api.normalize_payload(data)

            with api.connect_db() as conn:
                api.ensure_lead_master_columns(conn)
                lead_id = api.ensure_lead_exists(conn, payload["lead_external_id"], payload)
                api.update_lead(conn, lead_id, payload)
                conn.commit()
            api.mirror_db_to_drive()

            self._send_json({"ok": True, "leadId": lead_id})
        except Exception as exc:  # pragma: no cover - defensive handler
            self._send_error(str(exc))


    def _handle_import(self) -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()
            rows = data.get("rows") or []
            if not isinstance(rows, list):
                raise ValueError("'rows' must be an array")
            with_cleanup = bool(data.get("withCleanup"))
            cleanup_backup = bool(data.get("cleanupBackup"))

            cleanup_result = None
            if api._use_supabase_backend():
                result = api._supabase_import_leads(rows)
                if with_cleanup:
                    cleanup_result = {"ok": False, "skipped": True, "reason": "cleanup_not_supported_in_cloud_mode"}
            else:
                with api.connect_db() as conn:
                    api.ensure_lead_master_columns(conn)
                    result = api.import_rows(conn, rows)
                    conn.commit()

                if with_cleanup:
                    cleanup_result = api.run_cleanup(
                        db_path=api.DB_PATH,
                        audit_dir=api.ROOT.parent / "exports",
                        create_backup=cleanup_backup,
                    )

                api.mirror_db_to_drive()
            response = {"ok": True, **result}
            if cleanup_result is not None:
                response["cleanup"] = cleanup_result
            self._send_json(response)
        except Exception as exc:  # pragma: no cover - defensive handler
            self._send_error(str(exc))


    def _handle_carrier_config_get(self) -> None:
        import local_db_api as api
        try:
            with api.connect_db() as conn:
                rows = api.get_agent_carrier_config_rows(conn)
            self._send_json({"ok": True, "rows": rows})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_carrier_config_save(self) -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()
            rows = data.get("rows") or []
            if not isinstance(rows, list):
                raise ValueError("'rows' must be an array")
            with api.connect_db() as conn:
                saved = api.save_agent_carrier_config_rows(conn, rows)
                conn.commit()
            api.mirror_db_to_drive()
            self._send_json({"ok": True, "saved": saved})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_purge_test_data(self) -> None:
        import local_db_api as api
        try:
            with api.connect_db() as conn:
                api.ensure_lead_master_columns(conn)
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
            api.mirror_db_to_drive()
            self._set_headers(200)
            self.wfile.write(json.dumps({"ok": True, "deleted": int(deleted or 0), "reset": int(reset or 0)}).encode("utf-8"))
        except Exception as exc:  # pragma: no cover
            self._set_headers(500)
            self.wfile.write(json.dumps({"ok": False, "error": str(exc)}).encode("utf-8"))


    def _handle_pipeline_update(self, lead_external_id: str) -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()
            status_raw = api._trim(data.get("status"))
            status_map = {
                "App Submitted": "app_submitted",
                "Underwriting": "underwriting",
                "Approved": "approved",
                "Issued": "issued",
                "Paid": "paid",
            }
            pipeline_status = status_map.get(status_raw, api._trim(data.get("pipeline_status")) or "app_submitted")
            if api._use_supabase_backend():
                row = api._supabase_update_lead_fields(
                    lead_external_id,
                    {"pipeline_status": pipeline_status, "updated_at": api.now_iso()},
                )
                if not row:
                    self._send_error("lead_not_found", status=404)
                    return
            else:
                with api.connect_db() as conn:
                    api.ensure_lead_master_columns(conn)
                    row = conn.execute(
                        "SELECT lead_id FROM lead_master WHERE lead_external_id=? LIMIT 1",
                        (api._trim(lead_external_id),),
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
                        (pipeline_status, api._trim(lead_external_id)),
                    )
                    conn.commit()
                api.mirror_db_to_drive()
            self._send_json(
                {
                    "ok": True,
                    "leadId": api._trim(lead_external_id),
                    "pipeline_status": pipeline_status,
                }
            )
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_open_update(self, lead_external_id: str) -> None:
        import local_db_api as api
        try:
            opened_at = api.now_iso()
            if api._use_supabase_backend():
                row = api._supabase_update_lead_fields(
                    lead_external_id,
                    {"last_opened_at": opened_at, "updated_at": opened_at},
                )
                if not row:
                    self._send_error("lead_not_found", status=404)
                    return
            else:
                with api.connect_db() as conn:
                    api.ensure_lead_master_columns(conn)
                    row = conn.execute(
                        "SELECT lead_id FROM lead_master WHERE lead_external_id=? LIMIT 1",
                        (api._trim(lead_external_id),),
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
                        (opened_at, api._trim(lead_external_id)),
                    )
                    conn.commit()
                api.mirror_db_to_drive()
            self._send_json({"ok": True, "leadId": api._trim(lead_external_id), "last_opened_at": opened_at})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

