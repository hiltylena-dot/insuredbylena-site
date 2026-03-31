# Insurance Dashboard

Local web dashboard for insurance lead operations using the Lena lead dataset.

## What it shows

- lead volume and contactability
- compliance and DNC readiness
- routing queue distribution
- outreach channel mix
- automation sequence mix
- high-priority records that need manual review
- starter ROI metrics

## Run it

```bash
cd /Users/hankybot/Documents/Playground/insurance-dashboard
python3 -m http.server 4173
```

Then open:

```text
http://127.0.0.1:4173
```

## Data files

The app reads these local CSVs from `./data/`:

- `raw_leads.csv`
- `raw_activity.csv`
- `raw_bookings.csv`
- `raw_sales.csv`
- `source_targets.csv`

`raw_leads.csv` is preloaded from the prepared Lena lead set.

## Notes

- You can load a different lead CSV from the sidebar without changing files.
- Uploaded lead CSV rows are auto-sanitized (common field-shift and malformed contact fixes) before rendering.
- Populate the activity, bookings, and sales CSVs over time to unlock fuller ROI views.
