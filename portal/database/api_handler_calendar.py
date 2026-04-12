#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timedelta


class CalendarHandlerMixin:
    def _handle_calendar_schedule(self) -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()

            contact_id = api._trim(data.get("contactId") or data.get("leadId"))
            client_name = api._trim(data.get("clientName") or data.get("name")) or "Client Follow-up"
            email = api._trim(data.get("email"))
            phone = api._trim(data.get("phone"))
            start_raw = api._trim(data.get("scheduledAt") or data.get("start") or data.get("dateTime"))
            if not start_raw:
                raise ValueError("scheduledAt is required")
            duration_minutes = int(data.get("durationMinutes") or 30)
            description = api._trim(data.get("description")) or "Insurance follow-up call"

            start_dt = api._parse_local_iso(start_raw)
            end_dt = start_dt + timedelta(minutes=max(duration_minutes, 15))
            start_iso = start_dt.isoformat()
            end_iso = end_dt.isoformat()
            summary = f"Insurance Follow-up: {client_name}"
            full_description = description
            if phone:
                full_description = f"{full_description}\nPhone: {phone}".strip()

            if api._use_supabase_backend():
                event_payload = api._google_calendar_via_web_app(
                    client_name=client_name,
                    email=email,
                    phone=phone,
                    scheduled_at=start_iso,
                    description=full_description,
                    duration_minutes=duration_minutes,
                )
                event_id = api._trim(event_payload.get("calendarEventId"))
                if contact_id:
                    api._supabase_sync_calendar_schedule(contact_id, start_iso, event_id)
            else:
                gog_args = [
                    "calendar",
                    "create",
                    api.GOG_CALENDAR_ID,
                    f"--summary={summary}",
                    f"--from={start_iso}",
                    f"--to={end_iso}",
                    f"--description={full_description}",
                    "--send-updates=all",
                ]
                if "@" in email:
                    gog_args.append(f"--attendees={email}")
                event_payload = api.run_gog_json(gog_args)
                event_id = api._extract_event_id(event_payload)

                if contact_id:
                    with api.connect_db() as conn:
                        api.ensure_lead_master_columns(conn)
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
                    api.mirror_db_to_drive()

            self._send_json(
                {
                    "ok": True,
                    "calendarEventId": event_id,
                    "nextAppointmentTime": start_iso,
                    "start": start_iso,
                    "end": end_iso,
                    "calendarId": api.GOG_CALENDAR_ID,
                }
            )
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_google_calendar_sync(self) -> None:
        import local_db_api as api
        try:
            data = self._read_json_body()

            client_name = api._trim(data.get("clientName")) or "Client Follow-up"
            email = api._trim(data.get("email"))
            phone = api._trim(data.get("phone"))
            scheduled_at = api._trim(data.get("scheduledAt") or data.get("start") or data.get("dateTime"))
            if not scheduled_at:
                raise ValueError("scheduledAt is required")
            description = api._trim(data.get("description")) or "Insurance follow-up call"
            duration_minutes = int(data.get("durationMinutes") or 30)
            existing_event_id = api._trim(data.get("existingEventId"))

            payload = api._google_calendar_via_web_app(
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


    def _handle_calendar_today(self) -> None:
        import local_db_api as api
        try:
            if api._use_supabase_backend():
                now_local = datetime.now(api.APP_TIMEZONE)
                start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
                end_local = start_local + timedelta(days=1)
                rows = api._supabase_calendar_items(start_local, end_local)
            else:
                payload = api.run_gog_json(
                    [
                        "calendar",
                        "events",
                        api.GOG_CALENDAR_ID,
                        "--today",
                        "--max=50",
                    ]
                )
                events = api._extract_events(payload)
                rows = []
                for event in events:
                    start = event.get("start", {}) if isinstance(event.get("start"), dict) else {}
                    end = event.get("end", {}) if isinstance(event.get("end"), dict) else {}
                    rows.append(
                        {
                            "id": api._trim(event.get("id")),
                            "summary": api._trim(event.get("summary")) or "(No title)",
                            "start": api._trim(start.get("dateTime") or start.get("date")),
                            "end": api._trim(end.get("dateTime") or end.get("date")),
                            "htmlLink": api._trim(event.get("htmlLink")),
                        }
                    )
                rows.sort(key=lambda row: row["start"] or "")
            self._send_json({"ok": True, "items": rows, "count": len(rows)})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))


    def _handle_calendar_week(self) -> None:
        import local_db_api as api
        try:
            if api._use_supabase_backend():
                now_local = datetime.now(api.APP_TIMEZONE)
                start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
                end_local = start_local + timedelta(days=7)
                rows = api._supabase_calendar_items(start_local, end_local)
            else:
                payload = api.run_gog_json(
                    [
                        "calendar",
                        "events",
                        api.GOG_CALENDAR_ID,
                        "--days=7",
                        "--max=100",
                    ]
                )
                events = api._extract_events(payload)
                rows = []
                for event in events:
                    start = event.get("start", {}) if isinstance(event.get("start"), dict) else {}
                    end = event.get("end", {}) if isinstance(event.get("end"), dict) else {}
                    attendees = event.get("attendees") if isinstance(event.get("attendees"), list) else []
                    rows.append(
                        {
                            "id": api._trim(event.get("id")),
                            "summary": api._trim(event.get("summary")) or "(No title)",
                            "start": api._trim(start.get("dateTime") or start.get("date")),
                            "end": api._trim(end.get("dateTime") or end.get("date")),
                            "htmlLink": api._trim(event.get("htmlLink")),
                            "attendees": [
                                {"email": api._trim(att.get("email"))}
                                for att in attendees
                                if isinstance(att, dict) and api._trim(att.get("email"))
                            ],
                        }
                    )
                rows.sort(key=lambda row: row["start"] or "")
            self._send_json({"ok": True, "items": rows, "count": len(rows)})
        except Exception as exc:  # pragma: no cover
            self._send_error(str(exc))

