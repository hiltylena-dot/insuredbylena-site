create extension if not exists pgcrypto;

create table if not exists public.agent_workspace (
  workspace_id uuid primary key default gen_random_uuid(),
  workspace_slug text not null unique,
  display_name text not null,
  owner_user_id uuid references auth.users(id) on delete set null,
  subscription_status text not null default 'trialing'
    check (subscription_status in ('trialing', 'active', 'past_due', 'canceled', 'paused')),
  stripe_customer_id text,
  stripe_subscription_id text,
  trial_ends_at timestamptz,
  billing_email text,
  brand_settings jsonb not null default '{}'::jsonb,
  feature_flags jsonb not null default '{}'::jsonb,
  inserted_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (workspace_slug ~ '^[a-z0-9][a-z0-9-]{1,60}[a-z0-9]$')
);

create table if not exists public.workspace_member (
  workspace_id uuid not null references public.agent_workspace(workspace_id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  role text not null default 'agent'
    check (role in ('owner', 'admin', 'agent', 'viewer')),
  status text not null default 'active'
    check (status in ('active', 'invited', 'disabled')),
  invited_email text,
  inserted_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (workspace_id, user_id)
);

alter table public.app_user_profile
  add column if not exists default_workspace_id uuid references public.agent_workspace(workspace_id) on delete set null;

drop trigger if exists trg_agent_workspace_touch_updated_at on public.agent_workspace;
create trigger trg_agent_workspace_touch_updated_at
before update on public.agent_workspace
for each row execute function public.touch_updated_at();

drop trigger if exists trg_workspace_member_touch_updated_at on public.workspace_member;
create trigger trg_workspace_member_touch_updated_at
before update on public.workspace_member
for each row execute function public.touch_updated_at();

create or replace function public.current_workspace_id()
returns uuid
language sql
stable
security definer
set search_path = public
as $$
  select coalesce(
    (
      select p.default_workspace_id
      from public.app_user_profile p
      where p.user_id = (select auth.uid())
      limit 1
    ),
    (
      select wm.workspace_id
      from public.workspace_member wm
      where wm.user_id = (select auth.uid())
        and wm.status = 'active'
      order by wm.inserted_at asc
      limit 1
    )
  );
$$;

create or replace function public.is_workspace_member(p_workspace_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.workspace_member wm
    where wm.workspace_id = p_workspace_id
      and wm.user_id = (select auth.uid())
      and wm.status = 'active'
  );
$$;

create or replace function public.is_workspace_admin(p_workspace_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.workspace_member wm
    where wm.workspace_id = p_workspace_id
      and wm.user_id = (select auth.uid())
      and wm.status = 'active'
      and wm.role in ('owner', 'admin')
  );
$$;

create or replace function public.workspace_allows_writes(p_workspace_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.agent_workspace w
    where w.workspace_id = p_workspace_id
      and w.subscription_status in ('trialing', 'active')
  );
$$;

create or replace function public.create_agent_workspace(
  p_workspace_slug text,
  p_display_name text,
  p_billing_email text default null
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_user_id uuid := (select auth.uid());
  v_workspace_id uuid;
  v_workspace_slug text := lower(trim(coalesce(p_workspace_slug, '')));
  v_display_name text := nullif(trim(coalesce(p_display_name, '')), '');
  v_billing_email text := nullif(lower(trim(coalesce(p_billing_email, ''))), '');
begin
  if v_user_id is null then
    raise exception 'auth_required';
  end if;

  if v_workspace_slug !~ '^[a-z0-9][a-z0-9-]{1,60}[a-z0-9]$' then
    raise exception 'invalid_workspace_slug';
  end if;

  if v_display_name is null then
    raise exception 'display_name_required';
  end if;

  insert into public.agent_workspace (
    workspace_slug,
    display_name,
    owner_user_id,
    subscription_status,
    trial_ends_at,
    billing_email,
    feature_flags
  ) values (
    v_workspace_slug,
    v_display_name,
    v_user_id,
    'trialing',
    now() + interval '14 days',
    v_billing_email,
    jsonb_build_object(
      'contentStudio', false,
      'googleCalendar', false,
      'bufferPublishing', false
    )
  )
  returning workspace_id into v_workspace_id;

  insert into public.workspace_member (workspace_id, user_id, role, status, invited_email)
  values (v_workspace_id, v_user_id, 'owner', 'active', v_billing_email)
  on conflict (workspace_id, user_id) do update
    set role = 'owner',
        status = 'active',
        invited_email = excluded.invited_email,
        updated_at = now();

  update public.app_user_profile
  set default_workspace_id = coalesce(default_workspace_id, v_workspace_id)
  where user_id = v_user_id;

  return v_workspace_id;
exception
  when unique_violation then
    raise exception 'workspace_slug_taken';
end;
$$;

revoke all on function public.current_workspace_id() from public;
grant execute on function public.current_workspace_id() to authenticated;

revoke all on function public.is_workspace_member(uuid) from public;
grant execute on function public.is_workspace_member(uuid) to authenticated;

revoke all on function public.is_workspace_admin(uuid) from public;
grant execute on function public.is_workspace_admin(uuid) to authenticated;

revoke all on function public.workspace_allows_writes(uuid) from public;
grant execute on function public.workspace_allows_writes(uuid) to authenticated;

revoke all on function public.create_agent_workspace(text, text, text) from public;
grant execute on function public.create_agent_workspace(text, text, text) to authenticated;

do $$
declare
  v_table text;
begin
  foreach v_table in array array[
    'lead_master',
    'lena_source_profile',
    'call_desk_activity',
    'lead_product_proposal',
    'appointment',
    'policy_closeout',
    'lifecycle_event',
    'agent_carrier_config',
    'maintenance_audit_log',
    'lead_document'
  ]
  loop
    if to_regclass('public.' || v_table) is not null then
      execute format(
        'alter table public.%I add column if not exists workspace_id uuid references public.agent_workspace(workspace_id) on delete cascade',
        v_table
      );
    end if;
  end loop;
end;
$$;

insert into public.agent_workspace (
  workspace_slug,
  display_name,
  subscription_status,
  billing_email,
  brand_settings,
  feature_flags
) values (
  'insured-by-lena',
  'Insured by Lena',
  'active',
  'hiltylena@gmail.com',
  jsonb_build_object(
    'brandName', 'Insured by Lena',
    'primaryColor', '#8f8477',
    'surfaceColor', '#fdf9f4'
  ),
  jsonb_build_object(
    'contentStudio', false,
    'googleCalendar', false,
    'bufferPublishing', false
  )
)
on conflict (workspace_slug) do nothing;

update public.app_user_profile p
set default_workspace_id = w.workspace_id
from public.agent_workspace w
where w.workspace_slug = 'insured-by-lena'
  and p.default_workspace_id is null;

insert into public.workspace_member (workspace_id, user_id, role, status, invited_email)
select w.workspace_id,
       p.user_id,
       case when p.role = 'admin' then 'owner' else 'agent' end,
       'active',
       p.email
from public.app_user_profile p
cross join public.agent_workspace w
where w.workspace_slug = 'insured-by-lena'
on conflict (workspace_id, user_id) do nothing;

do $$
declare
  v_workspace_id uuid;
  v_table text;
begin
  select workspace_id into v_workspace_id
  from public.agent_workspace
  where workspace_slug = 'insured-by-lena'
  limit 1;

  if v_workspace_id is null then
    raise exception 'Default insured-by-lena workspace was not created.';
  end if;

  foreach v_table in array array[
    'lead_master',
    'lena_source_profile',
    'call_desk_activity',
    'lead_product_proposal',
    'appointment',
    'policy_closeout',
    'lifecycle_event',
    'agent_carrier_config',
    'maintenance_audit_log',
    'lead_document'
  ]
  loop
    if to_regclass('public.' || v_table) is not null then
      execute format('update public.%I set workspace_id = $1 where workspace_id is null', v_table)
      using v_workspace_id;
    end if;
  end loop;
end;
$$;

alter table public.lead_master drop constraint if exists lead_master_lead_external_id_key;
drop index if exists public.idx_lead_master_workspace_external_unique;
create unique index idx_lead_master_workspace_external_unique
  on public.lead_master (workspace_id, lead_external_id)
  where workspace_id is not null and lead_external_id is not null;

alter table public.agent_carrier_config drop constraint if exists agent_carrier_config_carrier_name_key;
drop index if exists public.idx_agent_carrier_config_workspace_carrier_unique;
create unique index idx_agent_carrier_config_workspace_carrier_unique
  on public.agent_carrier_config (workspace_id, carrier_name)
  where workspace_id is not null and carrier_name is not null;

create index if not exists idx_app_user_profile_default_workspace
  on public.app_user_profile (default_workspace_id);
create index if not exists idx_workspace_member_user_status
  on public.workspace_member (user_id, status, workspace_id);
create index if not exists idx_workspace_member_workspace_status
  on public.workspace_member (workspace_id, status);
create index if not exists idx_lead_master_workspace_email
  on public.lead_master (workspace_id, lower(email));
create index if not exists idx_lead_master_workspace_phone
  on public.lead_master (workspace_id, mobile_phone);
create index if not exists idx_lead_master_workspace_next_appointment
  on public.lead_master (workspace_id, next_appointment_time);
create index if not exists idx_appointment_workspace_booking
  on public.appointment (workspace_id, booking_date, booking_status);
create unique index if not exists idx_appointment_call_desk_active_unique
  on public.appointment (lead_id, owner)
  where owner = 'call_desk'
    and booking_status in ('Booked', 'Rescheduled', 'Pending');

do $$
begin
  if to_regclass('public.lead_document') is not null then
    create index if not exists idx_lead_document_workspace_active
      on public.lead_document (workspace_id, archived_at, inserted_at desc);
  end if;
end;
$$;

create or replace function public.set_workspace_id_from_current()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
begin
  if new.workspace_id is not null then
    return new;
  end if;

  new.workspace_id := public.current_workspace_id();

  if new.workspace_id is null then
    raise exception 'workspace_id is required';
  end if;

  return new;
end;
$$;

create or replace function public.enforce_workspace_write_access()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if new.workspace_id is null then
    raise exception 'workspace_id is required';
  end if;

  if public.is_admin() or current_setting('request.jwt.claim.role', true) = 'service_role' then
    return new;
  end if;

  if not public.is_workspace_member(new.workspace_id) then
    raise exception 'workspace_access_denied';
  end if;

  if not public.workspace_allows_writes(new.workspace_id) then
    raise exception 'workspace_subscription_inactive';
  end if;

  return new;
end;
$$;

create or replace function public.attach_workspace_triggers()
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_table text;
begin
  foreach v_table in array array[
    'call_desk_activity',
    'lead_product_proposal',
    'appointment',
    'policy_closeout',
    'lifecycle_event',
    'agent_carrier_config',
    'maintenance_audit_log',
    'lead_document'
  ]
  loop
    if to_regclass('public.' || v_table) is not null then
      execute format('drop trigger if exists trg_%I_set_workspace on public.%I', v_table, v_table);
      execute format('drop trigger if exists trg_%I_enforce_workspace_write on public.%I', v_table, v_table);
      execute format('drop trigger if exists %I on public.%I', 'trg_010_' || v_table || '_set_workspace', v_table);
      execute format('drop trigger if exists %I on public.%I', 'trg_020_' || v_table || '_enforce_workspace_write', v_table);
      execute format(
        'create trigger %I before insert on public.%I for each row execute function public.set_workspace_id_from_current()',
        'trg_010_' || v_table || '_set_workspace',
        v_table
      );

      execute format(
        'create trigger %I before insert or update on public.%I for each row execute function public.enforce_workspace_write_access()',
        'trg_020_' || v_table || '_enforce_workspace_write',
        v_table
      );
    end if;
  end loop;
end;
$$;

select public.attach_workspace_triggers();

create or replace function public.saas_portal_save_call_desk(p_payload jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_workspace_id uuid;
  v_existing public.lead_master%rowtype;
  v_saved public.lead_master%rowtype;
  v_appointment_id bigint;
  v_contact_id text := nullif(trim(coalesce(p_payload->>'contactId', '')), '');
  v_first_name text := nullif(trim(coalesce(p_payload->>'firstName', '')), '');
  v_last_name text := nullif(trim(coalesce(p_payload->>'lastName', '')), '');
  v_full_name text := nullif(trim(coalesce(p_payload->>'fullName', '')), '');
  v_email text := nullif(lower(trim(coalesce(p_payload->>'email', ''))), '');
  v_phone text := nullif(regexp_replace(coalesce(p_payload->>'phone', ''), '\D', '', 'g'), '');
  v_disposition text := nullif(trim(coalesce(p_payload->>'disposition', '')), '');
  v_age text := nullif(trim(coalesce(p_payload->>'age', '')), '');
  v_tobacco text := nullif(trim(coalesce(p_payload->>'tobacco', '')), '');
  v_health_posture text := nullif(trim(coalesce(p_payload->>'healthPosture', '')), '');
  v_carrier_match text := nullif(trim(coalesce(p_payload->>'carrierMatch', '')), '');
  v_confidence text := nullif(trim(coalesce(p_payload->>'confidence', '')), '');
  v_notes text := nullif(trim(coalesce(p_payload->>'notes', '')), '');
  v_tags text := nullif(trim(coalesce(p_payload->>'tags', '')), '');
  v_lead_source text := nullif(trim(coalesce(p_payload->>'leadSource', '')), '');
  v_lead_source_detail text := nullif(trim(coalesce(p_payload->>'leadSourceDetail', '')), '');
  v_product_line text := nullif(trim(coalesce(p_payload->>'productLine', '')), '');
  v_product_interest text := nullif(trim(coalesce(p_payload->>'productInterest', '')), '');
  v_pipeline_status text := nullif(trim(coalesce(p_payload->>'pipelineStatus', '')), '');
  v_should_schedule boolean := lower(trim(coalesce(p_payload->>'shouldSchedule', 'false'))) in ('true', 't', '1', 'yes', 'on');
  v_next_appointment_raw text := nullif(trim(coalesce(p_payload->>'nextAppointmentTime', '')), '');
  v_next_appointment_time timestamptz;
  v_has_active_schedule boolean;
  v_now timestamptz := now();
begin
  begin
    v_workspace_id := nullif(trim(coalesce(p_payload->>'workspaceId', '')), '')::uuid;
  exception
    when invalid_text_representation then
      raise exception 'invalid_workspace_id';
  end;

  v_workspace_id := coalesce(v_workspace_id, public.current_workspace_id());

  if v_workspace_id is null or not public.is_workspace_member(v_workspace_id) then
    raise exception 'workspace_access_denied';
  end if;

  if v_next_appointment_raw is not null then
    begin
      v_next_appointment_time := v_next_appointment_raw::timestamptz;
    exception
      when others then
        raise exception 'Invalid nextAppointmentTime';
    end;
  end if;

  v_has_active_schedule := v_should_schedule
    and v_next_appointment_time is not null
    and coalesce(v_disposition, '') in ('callback', 'follow_up');

  if v_contact_id is not null then
    select *
    into v_existing
    from public.lead_master
    where workspace_id = v_workspace_id
      and lead_external_id = v_contact_id
    order by inserted_at desc
    limit 1;
  end if;

  if v_existing.lead_id is null and v_phone is not null then
    select *
    into v_existing
    from public.lead_master
    where workspace_id = v_workspace_id
      and regexp_replace(coalesce(mobile_phone, ''), '\D', '', 'g') = v_phone
    order by coalesce(last_activity_at_source, inserted_at) desc, inserted_at desc
    limit 1;
  end if;

  if v_existing.lead_id is null and v_email is not null then
    select *
    into v_existing
    from public.lead_master
    where workspace_id = v_workspace_id
      and lower(coalesce(email, '')) = v_email
    order by coalesce(last_activity_at_source, inserted_at) desc, inserted_at desc
    limit 1;
  end if;

  v_contact_id := coalesce(
    v_contact_id,
    v_existing.lead_external_id,
    'CD-' || upper(substr(replace(gen_random_uuid()::text, '-', ''), 1, 12))
  );
  v_full_name := coalesce(v_full_name, nullif(trim(concat_ws(' ', v_first_name, v_last_name)), ''));

  if v_existing.lead_id is null then
    insert into public.lead_master (
      workspace_id,
      lead_external_id,
      first_name,
      last_name,
      full_name,
      email,
      mobile_phone,
      lead_source,
      lead_source_detail,
      campaign_name,
      product_interest,
      product_line,
      owner_queue,
      lead_status,
      booking_status,
      consent_status,
      dnc_status,
      contact_eligibility,
      notes,
      raw_tags,
      routing_bucket,
      recommended_channel,
      sequence_name,
      recommended_next_action,
      priority_tier,
      age,
      tobacco,
      health_posture,
      disposition,
      carrier_match,
      confidence,
      pipeline_status,
      next_appointment_time,
      last_activity_at_source,
      created_at_source
    ) values (
      v_workspace_id,
      v_contact_id,
      v_first_name,
      v_last_name,
      v_full_name,
      v_email,
      v_phone,
      coalesce(v_lead_source, 'call_desk'),
      coalesce(v_lead_source_detail, 'manual_call_desk_entry'),
      'Call Desk',
      v_product_interest,
      v_product_line,
      'call_desk_queue',
      coalesce(v_disposition, 'working'),
      case when v_has_active_schedule then 'Booked' else 'not_started' end,
      'review_required',
      'pending_check',
      'review_required',
      v_notes,
      coalesce(v_tags, 'call_desk,manual_entry'),
      'call_desk_queue',
      case when v_phone is not null then 'phone_call' else 'manual_review' end,
      'call_desk_manual_followup',
      'Continue discovery and set follow-up',
      'normal',
      v_age,
      v_tobacco,
      v_health_posture,
      v_disposition,
      v_carrier_match,
      v_confidence,
      v_pipeline_status,
      case when v_has_active_schedule then v_next_appointment_time else null end,
      v_now,
      v_now
    )
    returning * into v_saved;
  else
    update public.lead_master
    set first_name = coalesce(v_first_name, first_name),
        last_name = coalesce(v_last_name, last_name),
        full_name = coalesce(v_full_name, full_name),
        email = coalesce(v_email, email),
        mobile_phone = coalesce(v_phone, mobile_phone),
        lead_source = coalesce(v_lead_source, lead_source),
        lead_source_detail = coalesce(v_lead_source_detail, lead_source_detail),
        product_interest = coalesce(v_product_interest, product_interest),
        product_line = coalesce(v_product_line, product_line),
        lead_status = coalesce(v_disposition, lead_status),
        booking_status = case when v_has_active_schedule then 'Booked' else 'not_started' end,
        notes = coalesce(v_notes, notes),
        raw_tags = coalesce(v_tags, raw_tags),
        age = coalesce(v_age, age),
        tobacco = coalesce(v_tobacco, tobacco),
        health_posture = coalesce(v_health_posture, health_posture),
        disposition = coalesce(v_disposition, disposition),
        carrier_match = coalesce(v_carrier_match, carrier_match),
        confidence = coalesce(v_confidence, confidence),
        pipeline_status = coalesce(v_pipeline_status, pipeline_status),
        next_appointment_time = case when v_has_active_schedule then v_next_appointment_time else null end,
        last_activity_at_source = v_now
    where lead_id = v_existing.lead_id
      and workspace_id = v_workspace_id
    returning * into v_saved;
  end if;

  if v_has_active_schedule then
    insert into public.appointment (
      workspace_id,
      lead_id,
      booking_date,
      booking_status,
      show_status,
      appointment_type,
      owner
    ) values (
      v_workspace_id,
      v_saved.lead_id,
      v_next_appointment_time,
      'Booked',
      'pending',
      case when coalesce(v_disposition, '') = 'callback' then 'callback' else 'follow_up' end,
      'call_desk'
    )
    on conflict (lead_id, owner)
      where owner = 'call_desk' and booking_status in ('Booked', 'Rescheduled', 'Pending')
    do update
      set booking_date = excluded.booking_date,
          booking_status = excluded.booking_status,
          show_status = excluded.show_status,
          appointment_type = excluded.appointment_type,
          workspace_id = excluded.workspace_id
    returning appointment_id into v_appointment_id;
  elsif v_saved.lead_id is not null then
    update public.appointment
    set booking_status = 'Canceled',
        show_status = 'canceled'
    where workspace_id = v_workspace_id
      and lead_id = v_saved.lead_id
      and owner = 'call_desk'
      and booking_status in ('Booked', 'Rescheduled', 'Pending');
  end if;

  return jsonb_build_object(
    'ok', true,
    'workspaceId', v_workspace_id,
    'lead', to_jsonb(v_saved),
    'appointmentId', v_appointment_id,
    'scheduledInternally', v_has_active_schedule
  );
end;
$$;

revoke all on function public.saas_portal_save_call_desk(jsonb) from public;
grant execute on function public.saas_portal_save_call_desk(jsonb) to authenticated;

alter table public.agent_workspace enable row level security;
alter table public.workspace_member enable row level security;

drop policy if exists "workspace visible to members" on public.agent_workspace;
create policy "workspace visible to members"
on public.agent_workspace
for select
to authenticated
using ((select public.is_workspace_member(workspace_id)) or public.is_admin());

drop policy if exists "workspace admins can update workspace" on public.agent_workspace;
create policy "workspace admins can update workspace"
on public.agent_workspace
for update
to authenticated
using ((select public.is_workspace_admin(workspace_id)) or public.is_admin())
with check ((select public.is_workspace_admin(workspace_id)) or public.is_admin());

drop policy if exists "workspace members visible to members" on public.workspace_member;
create policy "workspace members visible to members"
on public.workspace_member
for select
to authenticated
using ((select public.is_workspace_member(workspace_id)) or user_id = (select auth.uid()) or public.is_admin());

drop policy if exists "workspace admins manage members" on public.workspace_member;
create policy "workspace admins manage members"
on public.workspace_member
for all
to authenticated
using ((select public.is_workspace_admin(workspace_id)) or public.is_admin())
with check ((select public.is_workspace_admin(workspace_id)) or public.is_admin());

drop policy if exists "workspace lead access" on public.lead_master;
create policy "workspace lead access"
on public.lead_master
for all
to authenticated
using ((select public.is_workspace_member(workspace_id)) or public.is_admin())
with check ((select public.is_workspace_member(workspace_id)) or public.is_admin());

drop policy if exists "workspace appointment access" on public.appointment;
create policy "workspace appointment access"
on public.appointment
for all
to authenticated
using ((select public.is_workspace_member(workspace_id)) or public.is_admin())
with check ((select public.is_workspace_member(workspace_id)) or public.is_admin());

drop policy if exists "workspace call desk access" on public.call_desk_activity;
create policy "workspace call desk access"
on public.call_desk_activity
for all
to authenticated
using ((select public.is_workspace_member(workspace_id)) or public.is_admin())
with check ((select public.is_workspace_member(workspace_id)) or public.is_admin());

drop policy if exists "workspace carrier config access" on public.agent_carrier_config;
create policy "workspace carrier config access"
on public.agent_carrier_config
for all
to authenticated
using ((select public.is_workspace_member(workspace_id)) or public.is_admin())
with check ((select public.is_workspace_admin(workspace_id)) or public.is_admin());

do $$
begin
  if to_regclass('public.lead_document') is not null then
    drop policy if exists "workspace lead document access" on public.lead_document;
    create policy "workspace lead document access"
    on public.lead_document
    for all
    to authenticated
    using ((select public.is_workspace_member(workspace_id)) or public.is_admin())
    with check ((select public.is_workspace_member(workspace_id)) or public.is_admin());
  end if;

  if to_regclass('public.maintenance_audit_log') is not null then
    drop policy if exists "workspace maintenance audit access" on public.maintenance_audit_log;
    create policy "workspace maintenance audit access"
    on public.maintenance_audit_log
    for all
    to authenticated
    using ((select public.is_workspace_admin(workspace_id)) or public.is_admin())
    with check ((select public.is_workspace_admin(workspace_id)) or public.is_admin());
  end if;
end;
$$;
