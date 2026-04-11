#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


DB_PATH = Path("/Users/hankybot/Documents/Playground/insurance-dashboard/database/insurance_lifecycle.db")
AUDIT_DIR = Path("/Users/hankybot/Documents/Playground/insurance-dashboard/exports")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
SCI_PHONE_RE = re.compile(r"^\d+\.\d+E\d+$", re.IGNORECASE)


@dataclass
class LeadRow:
    lead_id: int
    lead_external_id: str
    first_name: str
    last_name: str
    full_name: str
    email: str
    mobile_phone: str
    created_at_source: str
    last_activity_at_source: str
    notes: str
    raw_tags: str


def is_email(value: str) -> bool:
    return bool(EMAIL_RE.match((value or "").strip()))


def is_iso_date(value: str) -> bool:
    return bool(DATE_RE.match((value or "").strip()))


def phone_from_any(value: str) -> Optional[str]:
    text = (value or "").strip()
    if not text:
        return None
    if SCI_PHONE_RE.match(text):
        try:
            num = int(float(text))
            text = str(num)
        except Exception:
            pass
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 10:
        return None
    if len(digits) > 15:
        digits = digits[:15]
    return digits


def looks_like_shifted_row(row: LeadRow) -> bool:
    return phone_from_any(row.last_name or "") is not None and (
        "lead" in (row.mobile_phone or "").lower() or is_email(row.created_at_source or "")
    )


def clean_row(row: LeadRow) -> tuple[LeadRow, list[str]]:
    updated = LeadRow(**row.__dict__)
    changes: list[str] = []

    if not updated.email and is_email(updated.created_at_source):
        updated.email = updated.created_at_source.strip()
        changes.append("email<-created_at_source")
        if is_iso_date(updated.last_activity_at_source):
            updated.created_at_source = updated.last_activity_at_source.strip()
            changes.append("created_at_source<-last_activity_at_source")

    corrected_phone = phone_from_any(updated.last_name)
    if corrected_phone and phone_from_any(updated.mobile_phone) is None:
        updated.mobile_phone = corrected_phone
        changes.append("mobile_phone<-last_name")

    if looks_like_shifted_row(updated) or corrected_phone:
        # Common shift pattern: lead_external_id contains first name, first_name contains last name.
        if updated.lead_external_id and updated.first_name and updated.lead_external_id.isalpha():
            updated.full_name = f"{updated.lead_external_id.strip()} {updated.first_name.strip()}".strip()
            changes.append("full_name<-lead_external_id+first_name")
            updated.last_name = updated.first_name.strip()
            updated.first_name = updated.lead_external_id.strip()
            changes.append("first_name/last_name_shift_fix")
        elif updated.first_name and corrected_phone:
            updated.full_name = updated.first_name.strip()
            updated.last_name = ""
            changes.append("last_name_numeric_cleared")
        elif corrected_phone and not updated.first_name and not updated.lead_external_id:
            updated.first_name = "Unknown"
            updated.last_name = ""
            updated.full_name = "Unknown"
            changes.append("unknown_name_fallback")

    if updated.email and not is_email(updated.email):
        if not updated.notes:
            updated.notes = updated.email.strip()
            updated.email = ""
            changes.append("non_email_value_moved_to_notes")

    # If mobile_phone currently holds comma-delimited tag text, move to raw_tags if empty.
    if "," in (row.mobile_phone or "") and "lead" in (row.mobile_phone or "").lower():
        if not updated.raw_tags:
            updated.raw_tags = row.mobile_phone.strip()
            changes.append("raw_tags<-mobile_phone_text")
        if phone_from_any(updated.mobile_phone) is None:
            updated.mobile_phone = ""
            changes.append("mobile_phone_cleared_non_phone")

    # Rebuild full name when possible and current full_name has phone-like token.
    if phone_from_any(updated.full_name or "") and updated.first_name:
        updated.full_name = f"{updated.first_name} {updated.last_name}".strip()
        changes.append("full_name_rebuilt")

    # Final light normalization.
    updated.first_name = (updated.first_name or "").strip()
    updated.last_name = (updated.last_name or "").strip()
    updated.full_name = (updated.full_name or "").strip()
    updated.email = (updated.email or "").strip()
    updated.mobile_phone = (updated.mobile_phone or "").strip()
    updated.notes = (updated.notes or "").strip()
    updated.raw_tags = (updated.raw_tags or "").strip()

    return updated, changes


def run_cleanup(db_path: Path = DB_PATH, audit_dir: Path = AUDIT_DIR, create_backup: bool = True) -> dict[str, str | int]:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path: Path | None = None
    if create_backup:
        backup_path = db_path.with_suffix(f".bak_{stamp}.db")
        shutil.copy2(db_path, backup_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT lead_id, lead_external_id, first_name, last_name, full_name, email, mobile_phone,
               created_at_source, last_activity_at_source, notes, raw_tags
        FROM lead_master
        """
    ).fetchall()

    changed = 0
    audit_rows: list[list[str]] = []
    for r in rows:
        original = LeadRow(
            lead_id=int(r["lead_id"]),
            lead_external_id=r["lead_external_id"] or "",
            first_name=r["first_name"] or "",
            last_name=r["last_name"] or "",
            full_name=r["full_name"] or "",
            email=r["email"] or "",
            mobile_phone=r["mobile_phone"] or "",
            created_at_source=r["created_at_source"] or "",
            last_activity_at_source=r["last_activity_at_source"] or "",
            notes=r["notes"] or "",
            raw_tags=r["raw_tags"] or "",
        )

        cleaned, reasons = clean_row(original)
        if not reasons:
            continue

        cur.execute(
            """
            UPDATE lead_master
            SET first_name=?, last_name=?, full_name=?, email=?, mobile_phone=?,
                created_at_source=?, notes=?, raw_tags=?, updated_at=CURRENT_TIMESTAMP
            WHERE lead_id=?
            """,
            (
                cleaned.first_name,
                cleaned.last_name,
                cleaned.full_name,
                cleaned.email,
                cleaned.mobile_phone,
                cleaned.created_at_source,
                cleaned.notes,
                cleaned.raw_tags,
                cleaned.lead_id,
            ),
        )
        changed += 1
        audit_rows.append(
            [
                str(cleaned.lead_id),
                ";".join(reasons),
                original.first_name,
                cleaned.first_name,
                original.last_name,
                cleaned.last_name,
                original.full_name,
                cleaned.full_name,
                original.email,
                cleaned.email,
                original.mobile_phone,
                cleaned.mobile_phone,
                original.created_at_source,
                cleaned.created_at_source,
                original.notes,
                cleaned.notes,
                original.raw_tags,
                cleaned.raw_tags,
            ]
        )

    conn.commit()
    conn.close()

    audit_path = audit_dir / f"lead_master_cleanup_audit_{stamp}.csv"
    with audit_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "lead_id",
                "reasons",
                "first_name_before",
                "first_name_after",
                "last_name_before",
                "last_name_after",
                "full_name_before",
                "full_name_after",
                "email_before",
                "email_after",
                "mobile_before",
                "mobile_after",
                "created_at_before",
                "created_at_after",
                "notes_before",
                "notes_after",
                "raw_tags_before",
                "raw_tags_after",
            ]
        )
        w.writerows(audit_rows)

    result: dict[str, str | int] = {
        "changed_rows": changed,
        "audit": str(audit_path),
    }
    if backup_path is not None:
        result["backup"] = str(backup_path)
    return result


def main() -> None:
    result = run_cleanup()
    if "backup" in result:
        print(f"backup={result['backup']}")
    print(f"changed_rows={result['changed_rows']}")
    print(f"audit={result['audit']}")


if __name__ == "__main__":
    main()
