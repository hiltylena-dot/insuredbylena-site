# Portal Architecture Notes

## Current Direction

The portal now uses two different patterns on purpose:

1. `Content Studio`
- reads and writes directly to Supabase from the browser
- uses the same portal login/session
- does not depend on the old `/portal-api` path for normal content editing

2. `Cloud Run API`
- reserved for worker-style and privileged server-side tasks
- useful for:
  - future Buffer publisher worker
  - publish job execution
  - legacy lead/calendar operations still using the Python API
  - health checks and server-side integrations

## Content Studio

What is live now:
- load shared posts
- edit copy
- save draft
- request changes
- approve
- schedule
- restore revisions
- import JSON plans
- view publish job history

What is intentionally not live yet:
- direct Buffer publish from the browser
- remote `buffer-import.json` shortcut publish path

Why:
- publishing should happen from a small hosted worker, not directly from the browser

## Production Frontend Config

`/portal/index.html` contains the live runtime config for:

- `supabaseUrl`
- `supabasePublishableKey`
- `apiBase`

`apiBase` still points at the hosted Cloud Run service because other portal operations may continue using that backend even though Content Studio itself no longer depends on it for normal editing.

## Cloud Run Service

Live service:
- [insuredbylena-portal-api](https://insuredbylena-portal-api-607620457436.us-central1.run.app)

Health:
- [api/health](https://insuredbylena-portal-api-607620457436.us-central1.run.app/api/health)

Cloud Run should now be treated as:
- backend utility layer
- future publisher/worker host
- not the primary data path for day-to-day Content Studio editing

## Recommended Next Step

Build a tiny hosted publisher worker that:

1. reads approved/scheduled posts from Supabase
2. pushes to Buffer server-side
3. writes results into `content_publish_job`

That keeps:
- editing in Supabase/browser
- publishing on the server
