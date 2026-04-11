# Lifecycle Database

This builds a lifecycle database for Lena leads and call-desk operations:

- lead intake (`lead_master`)
- Lena source evidence (`lena_source_profile`)
- call desk touches (`call_desk_activity`)
- product proposals (`lead_product_proposal`)
- appointments (`appointment`)
- closeout (`policy_closeout`)
- timeline events (`lifecycle_event`)
- carrier documents (`carrier_document`)

## Build / Rebuild

```bash
python3 /Users/hankybot/Documents/Playground/insurance-dashboard/database/build_lifecycle_db.py
```

During build, `lead_master` is now auto-cleaned (field-shift and malformed contact repairs) and an audit CSV is written to `exports/`.

To also create a DB backup during cleanup:

```bash
python3 /Users/hankybot/Documents/Playground/insurance-dashboard/database/build_lifecycle_db.py --cleanup-backup
```

Database output:

- `/Users/hankybot/Documents/Playground/insurance-dashboard/database/insurance_lifecycle.db`

## Local Sync API (for Call Desk saves)

Run this lightweight local API so the dashboard can persist discovery fields to SQLite on every save:

```bash
python3 /Users/hankybot/Documents/Playground/insurance-dashboard/database/local_db_api.py
```

Endpoint used by `app.js`:

- `POST http://127.0.0.1:8787/api/leads/sync`

## Load a New Lena Lead File

Use `--lena-csv` with any new Lena-format file:

```bash
python3 /Users/hankybot/Documents/Playground/insurance-dashboard/database/build_lifecycle_db.py \
  --lena-csv /absolute/path/to/new_lena_leads.csv
```

Optional overrides:

- `--raw-leads-csv`
- `--activity-csv`
- `--bookings-csv`
- `--sales-csv`
- `--carrier-docs-csv`
- `--db`

## Useful Views

- `v_lead_lifecycle_summary`
- `v_open_pipeline`
- `v_product_funnel`
- `v_carrier_document_status`
