# Supabase Migration Plan

This portal currently depends on:

- static frontend at `/portal/`
- local Python API at `127.0.0.1:8787`
- local SQLite DB at `portal/database/insurance_lifecycle.db`

To make `insuredbylena.com/portal/` work remotely, the goal is:

- Supabase Auth for login
- Supabase Postgres for persistent lead/content/calendar data
- server-side API for privileged actions
- same-origin or proxied API paths from the portal

## What Is Already Prepared

- PostgreSQL/Supabase schema: [supabase_schema.sql](/Users/hankybot/Documents/Playground/insuredbylena-site/portal/database/supabase_schema.sql)
- local env template updated with Supabase keys: [portal/database/.env.example](/Users/hankybot/Documents/Playground/insuredbylena-site/portal/database/.env.example)

## Recommended Rollout

### Phase 1: Database + Auth

1. In Supabase SQL Editor, run [supabase_schema.sql](/Users/hankybot/Documents/Playground/insuredbylena-site/portal/database/supabase_schema.sql).
2. Create the first portal user in Supabase Auth.
3. Confirm that row level security is enabled.
4. Insert or update that auth user in `public.app_user_profile` with `role='admin'`.

Notes:

- The schema assumes authenticated admin-only access initially.
- The `service_role` key must stay server-side only.
- Because the service role key was shared in chat, rotate it after setup.

### Phase 2: Backend API

The current Python backend in:

- [local_db_api.py](/Users/hankybot/Documents/Playground/insurance-dashboard/database/local_db_api.py)

still handles:

- public intake
- lead sync/update
- carrier config
- calendar reads/writes
- content post operations

This should be moved into a deployable backend service.

Best target:

- Python API on a small host or VPS
- environment variables for `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`
- frontend calls changed from `:8787` to a production API base such as `/portal-api`

### Phase 3: Data Migration

Move existing SQLite data from:

- [insurance_lifecycle.db](/Users/hankybot/Documents/Playground/insuredbylena-site/portal/database/insurance_lifecycle.db)

into Supabase Postgres.

Suggested order:

1. `lead_master`
2. `lena_source_profile`
3. `call_desk_activity`
4. `appointment`
5. `policy_closeout`
6. `lifecycle_event`
7. `agent_carrier_config`
8. `carrier_document`
9. `content_post`
10. `content_revision`
11. `content_approval`
12. `content_publish_job`

Important:

- preserve `lead_id` relationships
- import parent tables first
- convert SQLite text timestamps into timestamptz values where possible

### Phase 4: Portal Frontend

The frontend currently points to:

- `http(s)://<hostname>:8787`

inside:

- [portal/app.js](/Users/hankybot/Documents/Playground/insuredbylena-site/portal/app.js)

Update this to:

- use a configurable API base
- authenticate users with Supabase Auth
- require an authenticated session before loading portal data

## Immediate Next Tasks

1. Run the Supabase schema.
2. Create the first admin auth user.
3. Build a production API config layer into `portal/app.js`.
4. Replace the first read/write path:
   - `lead_master`
5. Then migrate intake forms from local/Google fallback to the hosted backend.

## Hosting Recommendation

- Keep the marketing site on Namecheap.
- Keep `/portal/` behind `.htaccess` for now as an extra outer layer.
- Add Supabase Auth inside the portal for real application login.
- Host the API separately and keep the `service_role` key there only.
