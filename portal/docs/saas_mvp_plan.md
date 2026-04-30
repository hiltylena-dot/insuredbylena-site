# Agent Portal SaaS MVP

This is the first productization path for turning the current portal into a hosted monthly subscription product for independent insurance agents.

## MVP Scope

The first paid version should stay focused on the operational workflows that already work well:

- Call Desk
- Lead Selection
- Follow-up queue and portal calendar
- Client Document Hub
- Maintenance tools
- Basic carrier settings
- Workspace branding
- Subscription status gating

These stay out of the first version:

- Content Studio
- Buffer publishing
- Google Calendar sync
- custom domains
- multi-rep assignment

Those can be added later once the core CRM product is stable.

## Tenancy Model

Every customer gets one `agent_workspace`.

Users join workspaces through `workspace_member`. A user can technically belong to more than one workspace, but the MVP assumes one active/default workspace per user through `app_user_profile.default_workspace_id`.

The important rule is simple:

Every customer-owned row must have a `workspace_id`.

The first tenant-scoped tables are:

- `lead_master`
- `appointment`
- `call_desk_activity`
- `lead_document`
- `agent_carrier_config`
- `maintenance_audit_log`

The migration in [saas_mvp_foundation.sql](/Users/hankybot/Documents/Playground/insuredbylena-site/portal/database/saas_mvp_foundation.sql) adds the workspace tables, tenant columns, indexes, RLS policies, `create_agent_workspace`, subscription write guards, and `saas_portal_save_call_desk`.

## Build Order

1. Run the SaaS foundation SQL in a staging Supabase project.
2. Add a workspace bootstrap screen that calls `create_agent_workspace`.
3. Change the frontend/backend Call Desk save path from `portal_save_call_desk` to `saas_portal_save_call_desk`.
4. Add workspace settings to the portal config panel.
5. Hide non-MVP features behind `feature_flags`.
6. Add Stripe Checkout and subscription webhooks.
7. Add a platform admin page for customer status and support.
8. Run cross-workspace isolation tests before allowing real customers.

## Required Safety Tests

Before this is sold, the test suite needs tenant isolation checks:

- Agent A cannot read Agent B leads.
- Agent A cannot schedule Agent B appointments.
- Agent A cannot open Agent B documents.
- Duplicate prevention only dedupes inside the same workspace.
- Canceling or archiving a lead only affects that workspace.
- A disabled workspace member cannot access the portal.
- A canceled subscription blocks write actions but preserves read/export access.

## Billing Shape

The simplest first billing model:

- one workspace per subscription
- one owner/admin user included
- optional additional users later
- monthly Stripe subscription
- `trialing`, `active`, `past_due`, `canceled`, and `paused` states stored on `agent_workspace.subscription_status`

Stripe should be enforced server-side. The browser can show billing state, but it should not be trusted as the access control layer.

## Product Positioning

The first version is not "all of Insured by Lena."

It is:

An insurance agent call desk and follow-up operating system with lead tracking, scheduling, notes, documents, and cleanup tools.
