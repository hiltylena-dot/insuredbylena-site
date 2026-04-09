create unique index if not exists idx_appointment_call_desk_active_unique
on public.appointment (lead_id, owner)
where owner = 'call_desk'
  and booking_status in ('Booked', 'Rescheduled', 'Pending');

create or replace function public.portal_save_call_desk(p_payload jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
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
  v_pipeline_status text := nullif(trim(coalesce(p_payload->>'pipelineStatus', '')), '');
  v_tags text := nullif(trim(coalesce(p_payload->>'tags', '')), '');
  v_notes text := nullif(trim(coalesce(p_payload->>'notes', '')), '');
  v_lead_source text := nullif(trim(coalesce(p_payload->>'leadSource', '')), '');
  v_lead_source_detail text := nullif(trim(coalesce(p_payload->>'leadSourceDetail', '')), '');
  v_product_line text := nullif(trim(coalesce(p_payload->>'productLine', '')), '');
  v_product_interest text := nullif(trim(coalesce(p_payload->>'productInterest', '')), '');
  v_should_schedule boolean := lower(trim(coalesce(p_payload->>'shouldSchedule', 'false'))) in ('true', 't', '1', 'yes', 'on');
  v_next_appointment_raw text := nullif(trim(coalesce(p_payload->>'nextAppointmentTime', '')), '');
  v_next_appointment_time timestamptz;
  v_now timestamptz := now();
begin
  if v_next_appointment_raw is not null then
    begin
      v_next_appointment_time := v_next_appointment_raw::timestamptz;
    exception
      when others then
        raise exception 'Invalid nextAppointmentTime';
    end;
  end if;

  if v_contact_id is not null then
    select *
    into v_existing
    from public.lead_master
    where lead_external_id = v_contact_id
    order by inserted_at desc
    limit 1;
  end if;

  if v_existing.lead_id is null and v_phone is not null then
    select *
    into v_existing
    from public.lead_master
    where regexp_replace(coalesce(mobile_phone, ''), '\D', '', 'g') = v_phone
    order by coalesce(last_activity_at_source, inserted_at) desc, inserted_at desc
    limit 1;
  end if;

  if v_existing.lead_id is null and v_email is not null then
    select *
    into v_existing
    from public.lead_master
    where lower(coalesce(email, '')) = v_email
    order by coalesce(last_activity_at_source, inserted_at) desc, inserted_at desc
    limit 1;
  end if;

  v_contact_id := coalesce(v_contact_id, v_existing.lead_external_id, 'CD-' || upper(substr(replace(gen_random_uuid()::text, '-', ''), 1, 12)));
  v_full_name := coalesce(v_full_name, nullif(trim(concat_ws(' ', v_first_name, v_last_name)), ''));

  if v_existing.lead_id is null then
    insert into public.lead_master (
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
      case when v_should_schedule and v_next_appointment_time is not null then 'Booked' else 'not_started' end,
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
      case when v_should_schedule then v_next_appointment_time else null end,
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
        booking_status = case
          when v_should_schedule and v_next_appointment_time is not null then 'Booked'
          else booking_status
        end,
        notes = coalesce(v_notes, notes),
        raw_tags = coalesce(v_tags, raw_tags),
        age = coalesce(v_age, age),
        tobacco = coalesce(v_tobacco, tobacco),
        health_posture = coalesce(v_health_posture, health_posture),
        disposition = coalesce(v_disposition, disposition),
        carrier_match = coalesce(v_carrier_match, carrier_match),
        confidence = coalesce(v_confidence, confidence),
        pipeline_status = coalesce(v_pipeline_status, pipeline_status),
        next_appointment_time = case
          when v_should_schedule and v_next_appointment_time is not null then v_next_appointment_time
          else next_appointment_time
        end,
        last_activity_at_source = v_now
    where lead_id = v_existing.lead_id
    returning * into v_saved;
  end if;

  if v_should_schedule and v_next_appointment_time is not null then
    insert into public.appointment (
      lead_id,
      booking_date,
      booking_status,
      show_status,
      appointment_type,
      owner
    ) values (
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
          appointment_type = excluded.appointment_type
    returning appointment_id into v_appointment_id;
  end if;

  return jsonb_build_object(
    'ok', true,
    'lead', to_jsonb(v_saved),
    'appointmentId', v_appointment_id,
    'scheduledInternally', (v_should_schedule and v_next_appointment_time is not null)
  );
end;
$$;

revoke all on function public.portal_save_call_desk(jsonb) from public;
grant execute on function public.portal_save_call_desk(jsonb) to authenticated;
