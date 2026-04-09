import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm";

const DATA_FILES = {
  leads: "./data/raw_leads.csv",
  activity: "./data/raw_activity.csv",
  bookings: "./data/raw_bookings.csv",
  sales: "./data/raw_sales.csv",
  targets: "./data/source_targets.csv",
  sourced: "./data/sourced_leads.csv",
  carrierDocs: "./data/carrier_documents.csv",
};

const formatNumber = new Intl.NumberFormat("en-US");
const formatCurrency = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 0,
});

const PORTAL_CONFIG = window.PORTAL_CONFIG || {};
const SUPABASE_URL = String(PORTAL_CONFIG.supabaseUrl || "").trim();
const SUPABASE_PUBLISHABLE_KEY = String(PORTAL_CONFIG.supabasePublishableKey || "").trim();
const API_ORIGIN = String(PORTAL_CONFIG.apiBase || "").trim();
const supabase =
  SUPABASE_URL && SUPABASE_PUBLISHABLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY, {
        auth: {
          persistSession: true,
          autoRefreshToken: true,
        },
      })
    : null;
let hasInitialized = false;

const state = {
  leads: [],
  activity: [],
  bookings: [],
  sales: [],
  targets: [],
  sourcedLeads: [],
  contentPosts: [],
  contentPublishJobs: [],
  contentRevisions: [],
  carrierDocs: [],
  carrierGrid: [],
  todayAppointments: [],
  calendarTodayEvents: [],
  calendarWeekEvents: [],
  createdLeads: [],
  ui: {
    leadPreset: "none",
    sourcedPreset: "none",
    selectedSourcedLeadId: "",
    selectedLeadSelectionId: "",
    selectedCampaignLeadId: "",
    selectedContentPostId: "",
    contentSelectedPostIds: [],
    contentDraftOverrides: {},
    contentPreviewPlatform: "instagram",
    contentPostSearch: "",
    contentPostWeekFilter: "",
    contentPostPlatformFilter: "",
    contentPostStatusFilter: "",
    campaignSelectedLeadIds: [],
    selectedCallDeskLeadId: "",
    currentCallLeadId: "",
    leadId: null,
    currentLeadEmail: "",
    mainCallQueue: [],
    lastHandledObjection: "",
    primaryCarrier: "Life Lane Review",
    primaryConfidence: "Low confidence (0%)",
    primaryLane: "Needs more qualification",
    primaryWhyLane: "",
    carrierConfigs: [],
    isSaving: false,
    saveStatus: "idle",
    uploadedLeadRows: [],
    uploadCriticalErrors: 0,
    activeTab: localStorage.getItem("insurance-dashboard-active-tab") || "calldesk",
    leadSelectionSort: { key: "", dir: "asc" },
    workflowAnswers: {
      productPath: "",
      goal: "",
      age: "",
      tobacco: "",
      health: "",
      budget: "",
      duration: "",
      speed: "",
      healthCoverageType: "",
      healthNeed: "",
      healthPriority: "",
    },
  },
  sourcedLeadState: {},
};

function getContentDraftOverride(postId) {
  const normalized = String(postId || "").trim();
  if (!normalized) return {};
  return state.ui.contentDraftOverrides?.[normalized] || {};
}

function setContentDraftOverride(postId, patch) {
  const normalized = String(postId || "").trim();
  if (!normalized || !patch || typeof patch !== "object") return;
  state.ui.contentDraftOverrides = state.ui.contentDraftOverrides || {};
  state.ui.contentDraftOverrides[normalized] = {
    ...(state.ui.contentDraftOverrides[normalized] || {}),
    ...patch,
  };
}

function clearContentDraftOverride(postId) {
  const normalized = String(postId || "").trim();
  if (!normalized || !state.ui.contentDraftOverrides?.[normalized]) return;
  delete state.ui.contentDraftOverrides[normalized];
}

function createGlobalStore(initial) {
  const listeners = new Set();
  let data = { ...initial };
  return {
    getState() {
      return data;
    },
    setState(patch) {
      data = { ...data, ...patch };
      listeners.forEach((listener) => listener(data));
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

const appStore = createGlobalStore({
  currentLeadId: "",
  mainCallQueue: [],
});

const CRITERIA_STORAGE_KEY = "insurance-dashboard-sourcing-criteria";
const SOURCED_STATE_STORAGE_KEY = "insurance-dashboard-sourced-state";
const ACTIVE_TAB_STORAGE_KEY = "insurance-dashboard-active-tab";
const NOTES_STORAGE_KEY = "insurance-dashboard-call-notes";
const CALL_DESK_CREATED_LEADS_KEY = "insurance-dashboard-call-desk-created-leads";
const ROLE_VIEW_STORAGE_KEY = "insurance-dashboard-role-view";
const OUTCOMES_STORAGE_KEY = "insurance-dashboard-call-outcomes";
const CARRIER_CONFIGS_STORAGE_KEY = "insurance-dashboard-carrier-configs";
const ACTIVE_SESSION_STORAGE_KEY = "openclaw_active_session";
const LEAD_SELECTION_MAX_ROWS = 250;
const GOOGLE_WEBHOOK_URL =
  "https://script.google.com/macros/s/AKfycbxWyhM9FG7Jwd9iri4ppb0699ohLRGHMpdXFFp047B2FabnSUUzBEv0k7Vw-t0MA-3xpQ/exec";
const GOOGLE_CALENDAR_WEB_APP_URL = "";
const GOOGLE_CALENDAR_WEB_APP_SECRET = "Lvmh0509";
const LOCAL_DB_SYNC_URL = API_ORIGIN ? `${API_ORIGIN}/api/leads/sync` : "";
const LOCAL_DB_IMPORT_URL = API_ORIGIN ? `${API_ORIGIN}/api/leads/import` : "";
const LOCAL_DB_LEAD_BASE_URL = API_ORIGIN ? `${API_ORIGIN}/api/leads` : "";
const LEAD_OPEN_LEASE_URL = API_ORIGIN ? `${API_ORIGIN}/api/leads` : "";
const LOCAL_DB_CARRIER_CONFIG_URL = API_ORIGIN ? `${API_ORIGIN}/api/carrier-config` : "";
const LOCAL_DB_CALENDAR_SCHEDULE_URL = API_ORIGIN ? `${API_ORIGIN}/api/calendar/schedule` : "";
const LOCAL_DB_CALENDAR_TODAY_URL = API_ORIGIN ? `${API_ORIGIN}/api/calendar/today` : "";
const LOCAL_DB_CALENDAR_WEEK_URL = API_ORIGIN ? `${API_ORIGIN}/api/calendar/week` : "";
const LOCAL_DB_PURGE_TEST_DATA_URL = API_ORIGIN ? `${API_ORIGIN}/api/admin/purge-test-data` : "";
const LOCAL_DB_CONTENT_POSTS_URL = API_ORIGIN ? `${API_ORIGIN}/api/content/posts` : "";
const LOCAL_DB_CONTENT_POSTS_IMPORT_URL = API_ORIGIN ? `${API_ORIGIN}/api/content/posts/import` : "";
const LOCAL_DB_CONTENT_POSTS_IMPORT_BUFFER_CURRENT_URL = API_ORIGIN ? `${API_ORIGIN}/api/content/posts/import-buffer-current` : "";
const LOCAL_DB_CONTENT_PUBLISH_RUN_URL = API_ORIGIN ? `${API_ORIGIN}/api/content/publish/run` : "";
const LOCAL_DB_CONTENT_PUBLISH_JOBS_URL = API_ORIGIN ? `${API_ORIGIN}/api/content/publish/jobs` : "";
const GOOGLE_CALENDAR_EMBED_SRC = "hiltylena@gmail.com";
const GOOGLE_CALENDAR_TZ = "America/New_York";
const PIPELINE_STAGES = ["app_submitted", "underwriting", "approved", "issued", "paid"];
const LEASE_WINDOW_MS = 15 * 60 * 1000;
const FINAL_DISPOSITIONS = new Set(["sold", "not_qualified", "not_interested", "issued", "paid"]);
const DESK_OBJECTION_SNIPPETS = {
  budget:
    "I understand. Most of my clients are on a fixed budget. If we could find something for $10 less, would that help?",
  spouse:
    "I totally get that. Is your spouse usually the one who handles the final say, or do you just want to make sure they're protected too?",
  thinking:
    "Thinking objection pivot: Makes sense. What specific part needs more clarity: price, approval odds, or product type?",
};
const DESK_DYNAMIC_SCRIPTS = {
  lost_coverage:
    "I see you recently lost coverage. How many days has it been since that policy ended? We need to check for a Special Enrollment Period.",
  new:
    "Since this is a new policy for you, are we looking to protect your income for your family, or specifically for final expenses?",
  replace:
    "What is it about your current policy that is not working for you anymore? Is it the price or the coverage amount?",
};
const CONTENT_VIBE_PRESETS = [
  {
    label: "Future Protection",
    text: "The best thing you can do to secure your future is protect your income before life gets expensive.",
  },
  {
    label: "Life Insurance Myths",
    text: "5 myths about life insurance that cost families money:\n1) It's too expensive\n2) Work coverage is enough\n3) I'm too healthy to need it\n4) I'll buy it later\n5) Policies are confusing on purpose",
  },
  {
    label: "Wait Cost",
    text: "Waiting to get covered is usually the most expensive option. Health and age do not move in your favor.",
  },
  {
    label: "Work Coverage Gap",
    text: "If your life insurance is only through work, ask this: what happens to coverage if your job changes tomorrow?",
  },
  {
    label: "Parents Angle",
    text: "If someone depends on your income, life insurance is not optional. It is a love plan with math behind it.",
  },
  {
    label: "Term vs Whole",
    text: "Term vs Whole in plain English: term protects for a period, whole adds lifelong structure. The right choice depends on your goal and timeline.",
  },
  {
    label: "Policy Checkup CTA",
    text: "Comment CHECKLIST and I will send you a free 5-minute policy checkup guide.",
  },
  {
    label: "DM CTA",
    text: "DM FUTURE and I will help you map a simple protection plan with no pressure and no jargon.",
  },
];

function setAuthStatus(message, tone = "") {
  const node = document.getElementById("authStatus");
  if (!node) return;
  node.textContent = message || "";
  if (tone) node.dataset.tone = tone;
  else delete node.dataset.tone;
}

function setAuthLocked(locked) {
  document.body.classList.toggle("auth-locked", Boolean(locked));
}

function setPortalUser(session) {
  const email = session?.user?.email || "";
  const emailNode = document.getElementById("portalUserEmail");
  const logoutBtn = document.getElementById("portalLogoutBtn");
  if (emailNode) {
    emailNode.textContent = email;
    emailNode.hidden = !email;
  }
  if (logoutBtn) {
    logoutBtn.hidden = !email;
  }
}

function toIsoOrNull(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

function nowIso() {
  return new Date().toISOString();
}

function buildLeadPayloadForSupabase(row = {}) {
  const leadExternalId = String(row.lead_external_id || row.contactId || row.leadExternalId || "").trim();
  const firstName = String(row.first_name || row.firstName || "").trim();
  const lastName = String(row.last_name || row.lastName || "").trim();
  const fullName =
    String(row.full_name || row.fullName || "").trim() || `${firstName} ${lastName}`.trim() || null;

  return {
    lead_external_id: leadExternalId || null,
    first_name: firstName || null,
    last_name: lastName || null,
    full_name: fullName || null,
    email: String(row.email || "").trim() || null,
    mobile_phone: digitsOnly(row.mobile_phone || row.mobilePhone || row.phone || "") || null,
    business_name: String(row.business_name || row.businessName || "").trim() || null,
    lead_source: String(row.lead_source || row.leadSource || "").trim() || null,
    lead_source_detail: String(row.lead_source_detail || row.leadSourceDetail || "").trim() || null,
    campaign_name: String(row.campaign_name || row.campaignName || "").trim() || null,
    product_interest: String(row.product_interest || row.productInterest || "").trim() || null,
    product_line: String(row.product_line || row.productLine || "").trim() || null,
    owner_queue: String(row.owner_queue || row.ownerQueue || "").trim() || null,
    lead_status: String(row.lead_status || row.leadStatus || "").trim() || null,
    booking_status: String(row.booking_status || row.bookingStatus || "").trim() || null,
    consent_status: String(row.consent_status || row.consentStatus || "").trim() || null,
    consent_channel_sms: String(row.consent_channel_sms || row.consentChannelSms || "").trim() || null,
    consent_channel_email: String(row.consent_channel_email || row.consentChannelEmail || "").trim() || null,
    consent_channel_whatsapp: String(row.consent_channel_whatsapp || row.consentChannelWhatsapp || "").trim() || null,
    dnc_status: String(row.dnc_status || row.dncStatus || "").trim() || null,
    contact_eligibility: String(row.contact_eligibility || row.contactEligibility || "").trim() || null,
    created_at_source: toIsoOrNull(row.created_at_source || row.createdAtSource) || null,
    last_activity_at_source: toIsoOrNull(row.last_activity_at_source || row.lastActivity || row.lastActivityAtSource) || nowIso(),
    notes: String(row.notes || "").trim() || null,
    raw_tags: String(row.raw_tags || row.tags || row.rawTags || "").trim() || null,
    routing_bucket: String(row.routing_bucket || row.routingBucket || "").trim() || null,
    suppress_reason: String(row.suppress_reason || row.suppressReason || "").trim() || null,
    recommended_channel: String(row.recommended_channel || row.recommendedChannel || "").trim() || null,
    sequence_name: String(row.sequence_name || row.sequenceName || "").trim() || null,
    recommended_next_action: String(row.recommended_next_action || row.recommendedNextAction || "").trim() || null,
    priority_tier: String(row.priority_tier || row.priorityTier || "").trim() || null,
    age: String(row.age || "").trim() || null,
    tobacco: String(row.tobacco || "").trim() || null,
    health_posture: String(row.health_posture || row.healthPosture || "").trim() || null,
    disposition: String(row.disposition || "").trim() || null,
    carrier_match: String(row.carrier_match || row.carrierMatch || "").trim() || null,
    confidence: String(row.confidence || "").trim() || null,
    pipeline_status: String(row.pipeline_status || row.pipelineStatus || "").trim() || null,
    calendar_event_id: String(row.calendar_event_id || row.calendarEventId || "").trim() || null,
    next_appointment_time: toIsoOrNull(row.next_appointment_time || row.nextAppointmentTime) || null,
    last_opened_at: toIsoOrNull(row.last_opened_at || row.lastOpenedAt) || null,
  };
}

async function loadLeadRowsFromSupabase() {
  if (!supabase) return null;
  const allRows = [];
  const pageSize = 1000;
  let from = 0;

  while (true) {
    const to = from + pageSize - 1;
    const { data, error } = await supabase
      .from("lead_master")
      .select("*")
      .order("inserted_at", { ascending: false })
      .range(from, to);
    if (error) throw error;
    const rows = Array.isArray(data) ? data : [];
    allRows.push(...rows);
    if (rows.length < pageSize) break;
    from += pageSize;
  }

  return allRows;
}

async function upsertLeadRowsToSupabase(rows) {
  if (!supabase) throw new Error("Supabase lead store is not configured.");
  const payloads = rows
    .map((row) => buildLeadPayloadForSupabase(row))
    .filter((row) => row.lead_external_id);
  if (!payloads.length) return { added: 0, skipped_existing: 0, skipped_invalid: rows.length };

  const chunkSize = 250;
  for (let i = 0; i < payloads.length; i += chunkSize) {
    const chunk = payloads.slice(i, i + chunkSize);
    const { error } = await supabase
      .from("lead_master")
      .upsert(chunk, { onConflict: "lead_external_id" });
    if (error) throw error;
  }

  return {
    added: payloads.length,
    skipped_existing: 0,
    skipped_invalid: Math.max(0, rows.length - payloads.length),
  };
}

async function saveLeadToSupabase(row) {
  if (!supabase) throw new Error("Supabase lead store is not configured.");
  const payload = buildLeadPayloadForSupabase(row);
  if (!payload.lead_external_id) throw new Error("Lead external ID is required.");
  const { data, error } = await supabase
    .from("lead_master")
    .upsert(payload, { onConflict: "lead_external_id" })
    .select("*")
    .single();
  if (error) throw error;
  return data || payload;
}

async function updateLeadPipelineInSupabase(lead, stage) {
  if (!supabase) throw new Error("Supabase lead store is not configured.");
  const leadId = String(lead?.lead_external_id || "").trim();
  if (!leadId) return true;
  const { error } = await supabase
    .from("lead_master")
    .update({
      pipeline_status: String(stage || "").trim() || null,
      last_activity_at_source: nowIso(),
    })
    .eq("lead_external_id", leadId);
  if (error) throw error;
  return true;
}

async function markLeadOpenedInSupabase(leadId) {
  if (!supabase) throw new Error("Supabase lead store is not configured.");
  const normalized = String(leadId || "").trim();
  if (!normalized) return true;
  const openedAt = nowIso();
  const { error } = await supabase
    .from("lead_master")
    .update({ last_opened_at: openedAt })
    .eq("lead_external_id", normalized);
  if (error) throw error;
  return openedAt;
}

function getLeadByExternalId(leadExternalId) {
  const normalized = String(leadExternalId || "").trim();
  if (!normalized) return null;
  return state.leads.find((row) => String(row.lead_external_id || "").trim() === normalized) || null;
}

function getLeadByInternalId(leadId) {
  const normalized = Number(leadId || 0);
  if (!Number.isFinite(normalized) || normalized <= 0) return null;
  return state.leads.find((row) => Number(row.lead_id || 0) === normalized) || null;
}

function startOfDayIso(offsetDays = 0) {
  const dt = new Date();
  dt.setHours(0, 0, 0, 0);
  dt.setDate(dt.getDate() + offsetDays);
  return dt.toISOString();
}

function endOfDayIso(offsetDays = 0) {
  const dt = new Date();
  dt.setHours(23, 59, 59, 999);
  dt.setDate(dt.getDate() + offsetDays);
  return dt.toISOString();
}

function buildCalendarEventFromAppointment(row = {}) {
  const lead = getLeadByInternalId(row.lead_id);
  const leadName = lead
    ? String(lead.full_name || `${lead.first_name || ""} ${lead.last_name || ""}`).trim()
    : "";
  const label = row.appointment_type === "callback" ? "Callback" : "Follow-up";
  const summary = leadName ? `${label}: ${leadName}` : label;
  return {
    summary,
    start: row.booking_date,
    htmlLink: "",
    source: "supabase",
    lead_id: row.lead_id,
    lead_external_id: lead?.lead_external_id || "",
    attendees: lead?.email ? [{ email: lead.email }] : [],
  };
}

function dedupeCalendarEvents(items = []) {
  const seen = new Map();
  for (const item of Array.isArray(items) ? items : []) {
    const leadKey = String(item?.lead_external_id || "").trim();
    const fallbackKey = `${String(item?.summary || "").trim()}|${String(item?.start || "").trim()}`;
    const key = leadKey || fallbackKey;
    if (!key) continue;
    const existing = seen.get(key);
    const itemTs = Date.parse(String(item?.start || "")) || 0;
    const existingTs = Date.parse(String(existing?.start || "")) || 0;
    if (!existing || itemTs >= existingTs) {
      seen.set(key, item);
    }
  }
  return Array.from(seen.values()).sort(
    (a, b) => (Date.parse(String(a?.start || "")) || 0) - (Date.parse(String(b?.start || "")) || 0),
  );
}

function upsertLocalCalendarEvent(event) {
  if (!event?.lead_external_id || !event?.start) return;
  const matchesEvent = (row) =>
    String(row.lead_external_id || "").trim() === String(event.lead_external_id || "").trim();

  const upsertInto = (list) => {
    const rows = Array.isArray(list) ? [...list] : [];
    const existingIndex = rows.findIndex(matchesEvent);
    if (existingIndex >= 0) rows[existingIndex] = event;
    else rows.unshift(event);
    return dedupeCalendarEvents(rows);
  };

  const startTs = Date.parse(String(event.start || ""));
  const todayStart = Date.parse(startOfDayIso(0));
  const todayEnd = Date.parse(endOfDayIso(0));
  const weekEnd = Date.parse(endOfDayIso(30));

  if (Number.isFinite(startTs) && startTs >= todayStart && startTs <= todayEnd) {
    state.calendarTodayEvents = upsertInto(state.calendarTodayEvents);
  }
  if (Number.isFinite(startTs) && startTs >= todayStart && startTs <= weekEnd) {
    state.calendarWeekEvents = upsertInto(state.calendarWeekEvents);
  }
}

async function loadAppointmentsFromSupabase({ start, end, limit = 200 } = {}) {
  if (!supabase) return [];
  let query = supabase
    .from("appointment")
    .select("*")
    .order("booking_date", { ascending: true })
    .limit(limit);
  if (start) query = query.gte("booking_date", start);
  if (end) query = query.lte("booking_date", end);
  const { data, error } = await query;
  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

async function scheduleAppointmentInSupabase({
  contactId,
  clientName,
  email,
  phone,
  scheduledAt,
  description,
  disposition,
}) {
  if (!supabase) throw new Error("Supabase scheduling is not configured.");
  let lead = getLeadByExternalId(contactId);
  if (!lead?.lead_id) {
    const { data: dbLead, error: leadLookupError } = await supabase
      .from("lead_master")
      .select("*")
      .eq("lead_external_id", String(contactId || "").trim())
      .maybeSingle();
    if (leadLookupError) throw leadLookupError;
    if (dbLead) lead = dbLead;
  }
  if (!lead?.lead_id) throw new Error("Lead must be synced to Supabase before scheduling.");

  const bookingDate = toIsoOrNull(scheduledAt);
  if (!bookingDate) throw new Error("Follow-up date/time is required.");

  const { data: existingRows, error: existingError } = await supabase
    .from("appointment")
    .select("appointment_id, booking_date")
    .eq("lead_id", lead.lead_id)
    .eq("owner", "call_desk")
    .in("booking_status", ["Booked", "Rescheduled", "Pending"])
    .gte("booking_date", startOfDayIso(-1))
    .order("booking_date", { ascending: false })
    .limit(1);
  if (existingError) throw existingError;

  const appointmentPayload = {
    lead_id: lead.lead_id,
    booking_date: bookingDate,
    booking_status: "Booked",
    show_status: "pending",
    appointment_type: disposition === "callback" ? "callback" : "follow_up",
    owner: "call_desk",
  };

  if (Array.isArray(existingRows) && existingRows.length) {
    const { error: updateError } = await supabase
      .from("appointment")
      .update(appointmentPayload)
      .eq("appointment_id", existingRows[0].appointment_id);
    if (updateError) throw updateError;
  } else {
    const { error: insertError } = await supabase
      .from("appointment")
      .insert(appointmentPayload);
    if (insertError) throw insertError;
  }

  const mergedNotes = [String(lead.notes || "").trim(), String(description || "").trim()]
    .filter(Boolean)
    .join(" | ");
  const leadUpdate = {
    next_appointment_time: bookingDate,
    booking_status: "Booked",
    last_activity_at_source: nowIso(),
    notes: mergedNotes || null,
  };
  const { error: leadError } = await supabase
    .from("lead_master")
    .update(leadUpdate)
    .eq("lead_id", lead.lead_id);
  if (leadError) throw leadError;

  return {
    ok: true,
    calendarEventId: "",
    nextAppointmentTime: bookingDate,
    scheduledInternally: true,
    warning: "Saved to portal schedule. Google Calendar sync is still local-only.",
  };
}

async function createGoogleCalendarEvent({
  clientName,
  email,
  phone,
  scheduledAt,
  description,
  durationMinutes = 30,
}) {
  if (!GOOGLE_CALENDAR_WEB_APP_URL.trim()) {
    return {
      ok: false,
      error: "Google Calendar web app URL is not configured.",
    };
  }
  const response = await fetch(GOOGLE_CALENDAR_WEB_APP_URL, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain;charset=utf-8",
    },
    body: JSON.stringify({
      secret: GOOGLE_CALENDAR_WEB_APP_SECRET,
      clientName,
      email,
      phone,
      scheduledAt,
      description,
      durationMinutes,
    }),
  });
  const responseText = await response.text().catch(() => "");
  const data = (() => {
    try {
      return responseText ? JSON.parse(responseText) : {};
    } catch (_error) {
      return {};
    }
  })();
  if (!response.ok || !data?.ok) {
    throw new Error(
      String(
        data?.error
          || (responseText && responseText.length < 200 ? responseText : "")
          || `Google Calendar sync failed (${response.status})`,
      ),
    );
  }
  return data;
}

async function loadCarrierConfigsFromSupabase() {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from("agent_carrier_config")
    .select("*")
    .order("carrier_name", { ascending: true });
  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

async function saveCarrierConfigsToSupabase(rows = []) {
  if (!supabase) throw new Error("Supabase carrier config is not configured.");
  const payload = rows
    .map((row) => ({
      carrier_name: String(row.carrier_name || "").trim(),
      writing_number: String(row.writing_number || "").trim() || null,
      portal_url: String(row.portal_url || "").trim() || null,
      support_phone: String(row.support_phone || "").trim() || null,
    }))
    .filter((row) => row.carrier_name);

  const existing = await loadCarrierConfigsFromSupabase();
  const nextNames = new Set(payload.map((row) => row.carrier_name.toLowerCase()));
  const staleIds = existing
    .filter((row) => !nextNames.has(String(row.carrier_name || "").trim().toLowerCase()))
    .map((row) => row.id)
    .filter(Boolean);

  if (staleIds.length) {
    const { error: deleteError } = await supabase
      .from("agent_carrier_config")
      .delete()
      .in("id", staleIds);
    if (deleteError) throw deleteError;
  }

  if (payload.length) {
    const { error: upsertError } = await supabase
      .from("agent_carrier_config")
      .upsert(payload, { onConflict: "carrier_name" });
    if (upsertError) throw upsertError;
  }

  return payload;
}

async function ensureAuthenticatedSession() {
  if (!supabase) {
    setAuthLocked(true);
    setAuthStatus("Supabase portal login is not configured yet.", "error");
    return null;
  }

  const { data, error } = await supabase.auth.getSession();
  if (error) {
    setAuthLocked(true);
    setAuthStatus(error.message || "Could not verify session.", "error");
    return null;
  }
  return data.session || null;
}

async function signInToPortal(email, password) {
  if (!supabase) throw new Error("Supabase portal login is not configured.");
  const { data, error } = await supabase.auth.signInWithPassword({
    email,
    password,
  });
  if (error) throw error;
  return data.session || null;
}

async function signOutOfPortal() {
  if (!supabase) return;
  await supabase.auth.signOut();
}
const DESK_DISCOVERY_FIELD_IDS = new Set([
  "workflowGoal",
  "workflowAge",
  "workflowTobacco",
  "workflowHealth",
  "workflowBudget",
  "workflowDuration",
  "workflowSpeed",
  "deskProductPath",
  "deskGoal",
  "deskAge",
  "deskTobacco",
  "deskHealth",
  "deskBudget",
  "deskDuration",
  "deskSpeed",
  "deskHealthCoverageType",
  "deskHealthNeed",
  "deskHealthPriority",
  "deskNeedArea",
  "deskHealthGap",
  "deskLifeNeed",
  "deskProtectionLoad",
  "deskMedicareFlag",
  "deskLifeFeasibility",
  "deskCurrentCoverage",
  "deskExistingPolicy",
  "deskPolicyIntent",
  "deskDecisionMaker",
  "deskDecisionTimeline",
  "deskHealthState",
]);
const ACTIVE_SESSION_FIELD_IDS = [
  "deskClientName",
  "deskPhone",
  "deskCoverage",
  "deskNeedArea",
  "deskCurrentCoverage",
  "deskProductPath",
  "deskGoal",
  "deskAge",
  "deskTobacco",
  "deskHealth",
  "deskBudget",
  "deskDuration",
  "deskHealthCoverageType",
  "deskHealthNeed",
  "deskHealthPriority",
  "deskDecisionMaker",
  "deskDecisionTimeline",
  "deskGoalNote",
  "deskHealthNotes",
  "deskObjection",
  "deskDisposition",
  "deskNextStep",
  "deskFollowUp",
  "deskCallNotes",
];
let deskScriptToastTimer = 0;
const opsCharts = {
  pipeline: null,
  carrier: null,
  queue: null,
  contentPillar: null,
};

function ymd(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}`;
}

function buildGoogleEmbedUrl(options = {}) {
  const params = new URLSearchParams({
    src: GOOGLE_CALENDAR_EMBED_SRC,
    ctz: GOOGLE_CALENDAR_TZ,
    showTitle: "0",
    showNav: "1",
    showPrint: "0",
    showTabs: options.mode === "WEEK" ? "1" : "0",
    showCalendars: "0",
    mode: options.mode || "WEEK",
    color: "#222222",
    bgcolor: "#121212",
  });
  if (options.dates) params.set("dates", options.dates);
  return `https://calendar.google.com/calendar/embed?${params.toString()}`;
}

function setGoogleCalendarEmbeds() {
  const mainFrame = document.getElementById("calendarEmbedFrame");
  if (mainFrame) {
    mainFrame.src = buildGoogleEmbedUrl({ mode: "WEEK" });
  }

  const agendaFrame = document.getElementById("dashboardAgendaFrame");
  if (agendaFrame) {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const end = new Date(start);
    end.setDate(end.getDate() + 1);
    agendaFrame.src = buildGoogleEmbedUrl({
      mode: "AGENDA",
      dates: `${ymd(start)}/${ymd(end)}`,
    });
  }
}

function hideDeskScriptToast() {
  const toastEl = document.getElementById("deskScriptToast");
  if (!toastEl) return;
  toastEl.hidden = true;
  if (deskScriptToastTimer) {
    clearTimeout(deskScriptToastTimer);
    deskScriptToastTimer = 0;
  }
}

function showDeskScriptToast(scriptText) {
  const toastEl = document.getElementById("deskScriptToast");
  if (!toastEl || !scriptText) return;
  toastEl.textContent = scriptText;
  toastEl.hidden = false;
  if (deskScriptToastTimer) clearTimeout(deskScriptToastTimer);
  deskScriptToastTimer = window.setTimeout(() => {
    toastEl.hidden = true;
    deskScriptToastTimer = 0;
  }, 10000);
}

function setPrimaryCarrier(value) {
  state.ui.primaryCarrier = String(value || "");
}

function setConfidence(value) {
  state.ui.primaryConfidence = value === null || value === undefined ? "Low confidence (0%)" : String(value);
}

function setPrimaryLane(value) {
  state.ui.primaryLane = String(value || "");
}

function setPrimaryWhyLane(value) {
  state.ui.primaryWhyLane = String(value || "");
}

function syncPrimaryFromAgeHealth() {
  const ageRaw = String(state.ui.workflowAnswers.age || "");
  const healthRaw = String(state.ui.workflowAnswers.health || "");
  const tobacco = String(state.ui.workflowAnswers.tobacco || "");
  const age =
    ageRaw === "65to75" || ageRaw === "65-75"
      ? "65-75"
      : ageRaw === "76plus" || ageRaw === "76+"
        ? "76+"
        : ageRaw === "under50" || ageRaw === "Under 50"
          ? "Under 50"
          : ageRaw;
  const health = String(healthRaw || "").toLowerCase();

  if (age === "65-75" || age === "76+") {
    if (health === "healthy" || health === "managed") {
      setPrimaryCarrier("Mutual of Omaha");
      setConfidence("95%");
      setPrimaryLane("Final Expense - Level");
    } else if (health === "challenging") {
      setPrimaryCarrier("AIG (Guaranteed Issue)");
      setConfidence("98%");
      setPrimaryLane("Final Expense - GI");
    } else {
      setPrimaryCarrier("Life Lane Review");
      setConfidence("Low confidence (0%)");
      setPrimaryLane("Needs more qualification");
    }
  } else if (age === "Under 50" && health === "healthy") {
    setPrimaryCarrier("Preferred Term");
    setConfidence("85%");
    setPrimaryLane("Term - Standard");
  } else {
    setPrimaryCarrier("Life Lane Review");
    setConfidence("Low confidence (0%)");
    setPrimaryLane("Needs more qualification");
  }
  if (tobacco === "yes") setPrimaryWhyLane("Applying Tobacco Rates.");
  else setPrimaryWhyLane("");
}

function runRecommendationEffect() {
  // Reactive effect equivalent: invoked on age/health/tobacco changes.
  syncPrimaryFromAgeHealth();
}

const defaultCriteria = {
  geography: "us_only",
  quality: "strong",
  triggers: {
    newParent: true,
    homeBuyer: true,
    jobLoss: true,
    jobChange: true,
    medicare: false,
    businessOwner: false,
  },
  rules: {
    requireUs: true,
    requireName: true,
    requireTrigger: true,
    requirePath: true,
    rejectAnonymous: true,
  },
};

function loadCriteria() {
  try {
    const saved = JSON.parse(localStorage.getItem(CRITERIA_STORAGE_KEY) || "null");
    return saved ? { ...defaultCriteria, ...saved } : structuredClone(defaultCriteria);
  } catch {
    return structuredClone(defaultCriteria);
  }
}

state.criteria = loadCriteria();
state.sourcedLeadState = loadSourcedLeadState();
state.createdLeads = loadCreatedLeads();
state.callOutcomes = loadCallOutcomes();

function loadSourcedLeadState() {
  try {
    return JSON.parse(localStorage.getItem(SOURCED_STATE_STORAGE_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveSourcedLeadState() {
  localStorage.setItem(SOURCED_STATE_STORAGE_KEY, JSON.stringify(state.sourcedLeadState));
}

function loadCreatedLeads() {
  try {
    const parsed = JSON.parse(localStorage.getItem(CALL_DESK_CREATED_LEADS_KEY) || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveCreatedLeads() {
  localStorage.setItem(CALL_DESK_CREATED_LEADS_KEY, JSON.stringify(state.createdLeads));
}

function loadCallOutcomes() {
  try {
    const parsed = JSON.parse(localStorage.getItem(OUTCOMES_STORAGE_KEY) || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveCallOutcomes() {
  localStorage.setItem(OUTCOMES_STORAGE_KEY, JSON.stringify(state.callOutcomes || []));
}

function mergeLeadsByExternalId(baseLeads, extraLeads) {
  const merged = new Map();
  [...baseLeads, ...extraLeads].forEach((lead) => {
    const key = String(lead?.lead_external_id || "").trim();
    if (!key) return;
    merged.set(key, lead);
  });
  return Array.from(merged.values());
}

function hasValue(value) {
  return String(value || "").trim() !== "";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function digitCount(value) {
  return String(value || "")
    .split("")
    .filter((char) => char >= "0" && char <= "9").length;
}

function extractPhoneParts(value) {
  const raw = String(value || "").trim();
  if (!raw) return { base: "", extension: "" };
  const extMatch = raw.match(/(?:ext\.?|extension|x|#)\s*(\d{1,6})\s*$/i);
  if (!extMatch) return { base: raw, extension: "" };
  const base = raw.slice(0, extMatch.index).trim();
  return { base, extension: extMatch[1] || "" };
}

function normalizePhone(value) {
  const { base } = extractPhoneParts(value);
  if (!base) return "";
  let candidate = base;
  if (/^\d+\.\d+e\d+$/i.test(candidate)) {
    const num = Number(candidate);
    if (Number.isFinite(num)) candidate = String(Math.trunc(num));
  }
  const digits = candidate.replace(/\D/g, "");
  if (digits.length < 10) return "";
  if (digits.length > 15) return digits.slice(0, 15);
  return digits;
}

function normalizePhoneExtension(value) {
  return extractPhoneParts(value).extension || "";
}

function formatPhone(value, extension = "") {
  const digits = normalizePhone(value);
  if (!digits) return "";
  const ext = String(extension || normalizePhoneExtension(value) || "").trim();
  const withExt = (formatted) => (ext ? `${formatted} x${ext}` : formatted);
  if (digits.length === 10) {
    return withExt(`(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`);
  }
  if (digits.length === 11 && digits.startsWith("1")) {
    return withExt(`+1 (${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`);
  }
  return withExt(`+${digits}`);
}

function isLikelyEmail(value) {
  const text = String(value || "").trim();
  if (!text) return false;
  return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(text);
}

function looksLikeTagText(value) {
  const text = String(value || "").toLowerCase();
  return text.includes("lead") || text.includes("source") || text.includes(",");
}

function sanitizeLeadRow(row) {
  const cleaned = { ...row };
  const originalLeadExternalId = String(cleaned.lead_external_id || "").trim();
  const originalFirstName = String(cleaned.first_name || "").trim();
  const originalLastName = String(cleaned.last_name || "").trim();
  let changed = false;

  const namePhone = normalizePhone(originalLastName);
  const mobilePhone = normalizePhone(cleaned.mobile_phone);
  const mobileExt = normalizePhoneExtension(cleaned.mobile_phone);
  const formattedMobilePhone = formatPhone(mobilePhone, mobileExt);
  const leadIdLooksLikeName = /^[A-Za-z][A-Za-z' -]{1,40}$/.test(originalLeadExternalId);
  const firstNameLooksLikeName = /^[A-Za-z][A-Za-z' -]{1,40}$/.test(originalFirstName);

  if (namePhone && leadIdLooksLikeName && firstNameLooksLikeName) {
    cleaned.first_name = originalLeadExternalId;
    cleaned.last_name = originalFirstName;
    cleaned.lead_external_id = `SHIFTED-${originalLeadExternalId}-${originalFirstName}-${namePhone}`;
    cleaned.full_name = `${cleaned.first_name} ${cleaned.last_name}`.trim();
    cleaned.mobile_phone = formatPhone(namePhone);
    changed = true;
  }

  if (namePhone && !mobilePhone) {
    cleaned.mobile_phone = formatPhone(namePhone);
    if (!cleaned.last_name || cleaned.last_name === originalLastName) cleaned.last_name = "";
    changed = true;
  } else if (mobilePhone && cleaned.mobile_phone !== formattedMobilePhone) {
    cleaned.mobile_phone = formattedMobilePhone;
    changed = true;
  }

  if (!isLikelyEmail(cleaned.email) && isLikelyEmail(cleaned.created_at_source)) {
    cleaned.email = String(cleaned.created_at_source || "").trim();
    changed = true;
  } else if (cleaned.email && !isLikelyEmail(cleaned.email)) {
    cleaned.notes = [String(cleaned.notes || "").trim(), `Recovered from email field: ${cleaned.email}`]
      .filter(Boolean)
      .join(" | ");
    cleaned.email = "";
    changed = true;
  }

  if (looksLikeTagText(cleaned.mobile_phone) && !normalizePhone(cleaned.mobile_phone)) {
    if (!String(cleaned.raw_tags || "").trim()) cleaned.raw_tags = String(cleaned.mobile_phone || "").trim();
    cleaned.mobile_phone = "";
    changed = true;
  }

  if (digitCount(cleaned.first_name) >= 4) {
    cleaned.first_name = "";
    changed = true;
  }
  if (digitCount(cleaned.last_name) >= 4 && !normalizePhone(cleaned.last_name)) {
    cleaned.last_name = "";
    changed = true;
  }

  const firstName = String(cleaned.first_name || "").trim();
  const lastName = String(cleaned.last_name || "").trim();
  const rebuiltFullName = `${firstName} ${lastName}`.trim();
  if (!String(cleaned.full_name || "").trim() || digitCount(cleaned.full_name) >= 6) {
    cleaned.full_name = rebuiltFullName || String(cleaned.full_name || "").trim();
    changed = true;
  }

  return { row: cleaned, changed };
}

function sanitizeLeadRows(rows) {
  const sanitized = [];
  let changedRows = 0;
  rows.forEach((row) => {
    const { row: cleaned, changed } = sanitizeLeadRow(row);
    sanitized.push(cleaned);
    if (changed) changedRows += 1;
  });
  return { rows: sanitized, changedRows };
}

function normalizeLeadRow(row) {
  const normalizedPhone = normalizePhone(row?.mobile_phone);
  const normalizedExt = normalizePhoneExtension(row?.mobile_phone);
  const formattedPhone = formatPhone(normalizedPhone, normalizedExt);
  return {
    lead_external_id: String(row?.lead_external_id || "").trim(),
    first_name: String(row?.first_name || "").trim(),
    last_name: String(row?.last_name || "").trim(),
    full_name: String(row?.full_name || "").trim(),
    email: String(row?.email || "").trim(),
    mobile_phone: formattedPhone || String(row?.mobile_phone || "").trim(),
    mobile_phone_extension: normalizedExt,
    routing_bucket: String(row?.routing_bucket || "").trim(),
    lead_source: String(row?.lead_source || "").trim(),
    recommended_next_action: String(row?.recommended_next_action || "").trim(),
    recommended_channel: String(row?.recommended_channel || "").trim(),
    priority_tier: String(row?.priority_tier || "").trim(),
    contact_eligibility: String(row?.contact_eligibility || "").trim(),
    consent_status: String(row?.consent_status || "").trim(),
    consent_channel_email: String(row?.consent_channel_email || "").trim(),
    dnc_status: String(row?.dnc_status || "").trim(),
    suppress_reason: String(row?.suppress_reason || "").trim(),
    sequence_name: String(row?.sequence_name || "").trim(),
    campaign_name: String(row?.campaign_name || "").trim(),
    owner_queue: String(row?.owner_queue || "").trim(),
    last_activity_at_source: String(row?.last_activity_at_source || "").trim(),
    notes: String(row?.notes || "").trim(),
    age: String(row?.age || "").trim(),
    tobacco: String(row?.tobacco || "").trim(),
    health_posture: String(row?.health_posture || row?.healthPosture || "").trim(),
    disposition: String(row?.disposition || "").trim(),
    carrier_match: String(row?.carrier_match || row?.carrierMatch || "").trim(),
    confidence: String(row?.confidence || "").trim(),
    pipeline_status: String(row?.pipeline_status || row?.pipelineStatus || "").trim().toLowerCase(),
    last_opened_at: String(row?.last_opened_at || row?.lastOpenedAt || "").trim(),
    product_line: String(row?.product_line || "").trim(),
    product_interest: String(row?.product_interest || "").trim(),
    lead_source_detail: String(row?.lead_source_detail || "").trim(),
  };
}

function debounce(fn, wait = 120) {
  let timeoutId = null;
  return (...args) => {
    if (timeoutId) clearTimeout(timeoutId);
    timeoutId = setTimeout(() => {
      fn(...args);
    }, wait);
  };
}

function isEnrichmentNeeded(row) {
  const firstName = hasValue(row["First Name"]);
  const lastName = hasValue(row["Last Name"]);
  const phone = hasValue(row.Phone);
  const email = hasValue(row.Email);
  const contactPath = hasValue(row["Contact Path"]);
  const phoneConfidence = String(row["Phone Confidence"] || "").trim().toLowerCase();
  const emailConfidence = String(row["Email Confidence"] || "").trim().toLowerCase();
  const businessName = hasValue(row["Business Name"]);
  const trigger = hasValue(row["Trigger Event"]);
  const evidence = hasValue(row["Source Evidence"]);
  const postUrl = hasValue(row["Post or Message URL"]);
  const anonymous = String(row["First Name"] || "").trim().toLowerCase() === "anonymous";

  if (!trigger || !evidence) return true;
  if (!firstName) return true;
  if (anonymous) return true;
  if (!lastName && !businessName) return true;
  if (!phone && !email && !contactPath && !postUrl) return true;
  if (phone && !["high", "medium"].includes(phoneConfidence)) return true;
  if (email && !["high", "medium"].includes(emailConfidence)) return true;
  return false;
}

function sourcedQueueLabel(row) {
  return isEnrichmentNeeded(row) ? "Enrichment Needed" : "Ready For Review";
}

function rowText(row) {
  return Object.values(row)
    .join(" ")
    .toLowerCase();
}

function isUsLead(row) {
  const text = rowText(row);
  const usSignals = [
    "arkansas",
    "arizona",
    "california",
    "florida",
    "texas",
    "new york",
    "north carolina",
    "south carolina",
    "georgia",
    "ohio",
    "illinois",
    "siloam springs",
    "united states",
    "usa",
  ];
  const nonUsSignals = [
    "india",
    "maharashtra",
    "pune",
    "canada",
    "uk",
    "united kingdom",
    "australia",
  ];
  if (nonUsSignals.some((signal) => text.includes(signal))) return false;
  return usSignals.some((signal) => text.includes(signal));
}

function triggerMatchesCriteria(row, criteria) {
  const trigger = String(row["Trigger Event"] || "").toLowerCase();
  const notes = rowText(row);
  const checks = [
    [criteria.triggers.newParent, trigger.includes("new_parent") || notes.includes("baby")],
    [criteria.triggers.homeBuyer, trigger.includes("home") || notes.includes("home buyer") || notes.includes("mortgage")],
    [criteria.triggers.jobLoss, trigger.includes("job_loss") || trigger.includes("coverage_gap") || notes.includes("lost my job") || notes.includes("medicaid")],
    [criteria.triggers.jobChange, trigger.includes("job_change") || notes.includes("started a new position")],
    [criteria.triggers.medicare, trigger.includes("medicare") || notes.includes("medicare")],
    [criteria.triggers.businessOwner, notes.includes("business owner") || notes.includes("founder") || notes.includes("self-employed")],
  ];
  return checks.some(([enabled, matched]) => enabled && matched);
}

function leadPassesCriteria(row, criteria) {
  const reasons = [];
  const anonymous = String(row["First Name"] || "").trim().toLowerCase() === "anonymous";
  const hasName = hasValue(row["First Name"]);
  const hasTrigger = hasValue(row["Trigger Event"]);
  const hasPath = hasValue(row["Post or Message URL"]) || hasValue(row["Contact Path"]);
  const enrichmentNeeded = isEnrichmentNeeded(row);

  if (criteria.rules.requireUs && !isUsLead(row)) reasons.push("Non-U.S. or missing U.S. geography");
  if (!triggerMatchesCriteria(row, criteria)) reasons.push("Trigger type not selected");
  if (criteria.rules.requireName && !hasName) reasons.push("Missing real name");
  if (criteria.rules.requireTrigger && !hasTrigger) reasons.push("Missing trigger");
  if (criteria.rules.requirePath && !hasPath) reasons.push("Missing source path");
  if (criteria.rules.rejectAnonymous && anonymous) reasons.push("Anonymous lead");
  if (criteria.quality === "strong" && enrichmentNeeded) reasons.push("Needs enrichment");

  return { accepted: reasons.length === 0, reasons };
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let value = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        value += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(value);
      value = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") i += 1;
      row.push(value);
      if (row.some((cell) => cell !== "")) rows.push(row);
      row = [];
      value = "";
      continue;
    }

    value += char;
  }

  if (value.length > 0 || row.length > 0) {
    row.push(value);
    if (row.some((cell) => cell !== "")) rows.push(row);
  }

  if (!rows.length) return [];
  const headers = rows[0];
  return rows.slice(1).map((entry) => {
    const record = {};
    headers.forEach((header, index) => {
      record[header] = entry[index] ?? "";
    });
    return record;
  });
}

async function loadCsv(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`Failed to load ${path}`);
  return parseCsv(await response.text());
}

async function loadJson(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`Failed to load ${path}`);
  return response.json();
}

function inferProductLineFromTrigger(triggerEvent) {
  const trigger = String(triggerEvent || "").toLowerCase();
  if (trigger.includes("medicare") || trigger.includes("coverage_gap")) return "Health";
  return "Life";
}

function mapLenaRowsToDashboardLeads(rows) {
  return rows.map((row, index) => {
    const firstName = String(row["First Name"] || "").trim();
    const lastName = String(row["Last Name"] || "").trim();
    const fullName = `${firstName} ${lastName}`.trim();
    const leadExternalId = String(row["Contact Id"] || "").trim() || `LENA-UPLOAD-${index + 1}`;
    const sourcePlatform = String(row["Source Platform"] || "").trim() || "social";
    const sourceType = String(row["Source Type"] || "").trim() || "lena_sourced";
    const productLine = inferProductLineFromTrigger(row["Trigger Event"]);
    const mergedNotes = [row.Notes, row["Lead Circumstances"], row["Enrichment Notes"]]
      .filter((part) => String(part || "").trim() !== "")
      .join(" | ");

    return {
      lead_external_id: leadExternalId,
      first_name: firstName,
      last_name: lastName,
      full_name: fullName,
      email: String(row.Email || "").trim(),
      mobile_phone: String(row.Phone || "").trim(),
      business_name: String(row["Business Name"] || "").trim(),
      lead_source: sourceType,
      lead_source_detail: sourcePlatform,
      campaign_name: "Lena Uploaded Leads",
      product_interest: productLine.toLowerCase(),
      product_line: productLine,
      owner_queue: "intake_queue",
      lead_status: "new",
      booking_status: "not_started",
      consent_status: "review_required",
      consent_channel_sms: "",
      consent_channel_email: "",
      consent_channel_whatsapp: "",
      dnc_status: "pending_check",
      contact_eligibility: "review_required",
      created_at_source: String(row.Created || "").trim(),
      last_activity_at_source: String(row["Last Activity"] || "").trim(),
      notes: mergedNotes,
      raw_tags: String(row.Tags || "").trim(),
      raw_created: String(row.Created || "").trim(),
      raw_last_activity: String(row["Last Activity"] || "").trim(),
      routing_bucket: "intake_queue",
      suppress_reason: "",
      recommended_channel: "manual_review",
      sequence_name: "lena_manual_review",
      recommended_next_action: String(row["First Touch Strategy"] || "").trim() || "review source and route next step",
      priority_tier: "normal",
    };
  });
}

function splitFullName(fullName = "") {
  const cleaned = String(fullName || "").trim().replace(/\s+/g, " ");
  if (!cleaned) return { firstName: "", lastName: "" };
  const parts = cleaned.split(" ");
  if (parts.length === 1) {
    return {
      firstName: parts[0],
      lastName: "",
    };
  }
  return {
    firstName: parts.slice(0, -1).join(" "),
    lastName: parts.slice(-1).join(" "),
  };
}

function hasHeaderAlias(headers = [], aliases = []) {
  return aliases.some((alias) => headers.includes(alias));
}

function findHeaderAlias(headers = [], aliases = []) {
  return aliases.find((alias) => headers.includes(alias)) || "";
}

const GENERIC_IMPORT_ALIASES = {
  fullName: ["Full Name", "Name", "Lead Name", "Customer Name"],
  firstName: ["First Name", "first_name", "firstName", "Fname"],
  lastName: ["Last Name", "last_name", "lastName", "Lname"],
  phone: ["Phone Number", "Phone", "Mobile", "Mobile Phone", "Cell", "cell_phone"],
  email: ["Email", "Email Address", "email_address"],
  state: ["State", "state", "Region"],
  leadId: ["Lead ID", "lead_id", "lead_external_id", "Contact Id", "Contact ID"],
  leadType: ["Lead Type", "Product", "Product Type", "Interest", "Coverage Type"],
  source: ["Lead Source", "Source", "source"],
  campaign: ["Campaign", "Campaign Name", "campaign_name"],
  businessName: ["Business Name", "Company", "Company Name"],
  dob: ["DOB", "Date of Birth", "Birthdate"],
  notes: ["Notes", "Note", "Comments", "Description"],
};

function inferProductLineFromLeadType(leadType = "") {
  const normalized = String(leadType || "").trim().toLowerCase();
  if (!normalized) return "Health";
  if (
    normalized.includes("life")
    || normalized.includes("iul")
    || normalized.includes("final expense")
    || normalized.includes("mortgage")
    || normalized.includes("term")
  ) {
    return "Life";
  }
  if (
    normalized.includes("medicare")
    || normalized.includes("health")
    || normalized.includes("aca")
    || normalized.includes("under 65")
  ) {
    return "Health";
  }
  return "Health";
}

function mapGenericLeadRowsToDashboardLeads(rows) {
  const headers = Object.keys(rows[0] || {});
  const fullNameHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.fullName);
  const firstNameHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.firstName);
  const lastNameHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.lastName);
  const phoneHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.phone);
  const emailHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.email);
  const stateHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.state);
  const leadIdHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.leadId);
  const leadTypeHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.leadType);
  const sourceHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.source);
  const campaignHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.campaign);
  const businessNameHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.businessName);
  const dobHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.dob);
  const notesHeader = findHeaderAlias(headers, GENERIC_IMPORT_ALIASES.notes);

  return rows.map((row, index) => {
    const fullName = fullNameHeader
      ? String(row[fullNameHeader] || "").trim()
      : "";
    const firstNameValue = firstNameHeader ? String(row[firstNameHeader] || "").trim() : "";
    const lastNameValue = lastNameHeader ? String(row[lastNameHeader] || "").trim() : "";
    const splitName = splitFullName(fullName);
    const firstName = firstNameValue || splitName.firstName;
    const lastName = lastNameValue || splitName.lastName;
    const leadType = leadTypeHeader ? String(row[leadTypeHeader] || "").trim() : "";
    const source = sourceHeader ? String(row[sourceHeader] || "").trim() : "generic_import";
    const campaign = campaignHeader ? String(row[campaignHeader] || "").trim() : "Imported Leads";
    const stateValue = stateHeader ? String(row[stateHeader] || "").trim() : "";
    const productLine = inferProductLineFromLeadType(leadType);
    const combinedNotes = [
      notesHeader ? String(row[notesHeader] || "").trim() : "",
      dobHeader ? String(row[dobHeader] || "").trim() ? `DOB: ${String(row[dobHeader] || "").trim()}` : "" : "",
      stateValue ? `State: ${stateValue}` : "",
    ]
      .filter(Boolean)
      .join(" | ");

    return {
      lead_external_id: leadIdHeader ? String(row[leadIdHeader] || "").trim() || `GENERIC-UPLOAD-${index + 1}` : `GENERIC-UPLOAD-${index + 1}`,
      first_name: firstName,
      last_name: lastName,
      full_name: fullName || `${firstName} ${lastName}`.trim(),
      email: emailHeader ? String(row[emailHeader] || "").trim() : "",
      mobile_phone: phoneHeader ? String(row[phoneHeader] || "").trim() : "",
      business_name: businessNameHeader ? String(row[businessNameHeader] || "").trim() : "",
      lead_source: source || "generic_import",
      lead_source_detail: leadType || "generic_csv_import",
      campaign_name: campaign,
      product_interest: leadType.toLowerCase() || productLine.toLowerCase(),
      product_line: productLine,
      owner_queue: "intake_queue",
      lead_status: "new",
      booking_status: "not_started",
      consent_status: "review_required",
      consent_channel_sms: "",
      consent_channel_email: "",
      consent_channel_whatsapp: "",
      dnc_status: "pending_check",
      contact_eligibility: "review_required",
      created_at_source: "",
      last_activity_at_source: "",
      notes: combinedNotes,
      raw_tags: "",
      raw_created: "",
      raw_last_activity: "",
      routing_bucket: "intake_queue",
      suppress_reason: "",
      recommended_channel: "manual_review",
      sequence_name: "generic_manual_review",
      recommended_next_action: `review ${productLine.toLowerCase()} options and contact lead`,
      priority_tier: "normal",
      state: stateValue,
    };
  });
}

function mapClosrLeadsRowsToDashboardLeads(rows) {
  return rows.map((row, index) => {
    const fullName = String(row["Full Name"] || "").trim();
    const { firstName, lastName } = splitFullName(fullName);
    const leadType = String(row["Lead Type"] || "").trim();
    const productLine = inferProductLineFromLeadType(leadType);
    const stateValue = String(row.State || row.state || "").trim();
    const notes = [
      stateValue ? `State: ${stateValue}` : "",
      String(row.DOB || "").trim() ? `DOB: ${String(row.DOB || "").trim()}` : "",
      String(row["Update sheet"] || "").trim() ? `Update Sheet: ${String(row["Update sheet"] || "").trim()}` : "",
    ]
      .filter(Boolean)
      .join(" | ");

    return {
      lead_external_id: String(row["Lead ID"] || "").trim() || `CLOSRLEADS-UPLOAD-${index + 1}`,
      first_name: firstName,
      last_name: lastName,
      full_name: fullName || `${firstName} ${lastName}`.trim(),
      email: String(row.Email || "").trim(),
      mobile_phone: String(row["Phone Number"] || "").trim(),
      business_name: "",
      lead_source: "closrleads",
      lead_source_detail: leadType || "closrleads_import",
      campaign_name: "ClosrLeads Upload",
      product_interest: leadType.toLowerCase() || productLine.toLowerCase(),
      product_line: productLine,
      owner_queue: "intake_queue",
      lead_status: "new",
      booking_status: "not_started",
      consent_status: "review_required",
      consent_channel_sms: "",
      consent_channel_email: "",
      consent_channel_whatsapp: "",
      dnc_status: "pending_check",
      contact_eligibility: "review_required",
      created_at_source: "",
      last_activity_at_source: "",
      notes,
      raw_tags: "",
      raw_created: "",
      raw_last_activity: "",
      routing_bucket: "intake_queue",
      suppress_reason: "",
      recommended_channel: "manual_review",
      sequence_name: "closrleads_manual_review",
      recommended_next_action: `review ${productLine.toLowerCase()} options and contact lead`,
      priority_tier: "normal",
      state: stateValue,
    };
  });
}

function detectLeadFormat(rows) {
  const headers = Object.keys(rows[0] || {});
  if (headers.includes("lead_external_id")) return "dashboard";
  if (headers.includes("Contact Id") && headers.includes("First Name")) return "lena";
  if (headers.includes("Full Name") && headers.includes("Phone Number") && headers.includes("Lead ID")) return "closrleads";
  const hasPhone = hasHeaderAlias(headers, GENERIC_IMPORT_ALIASES.phone);
  const hasName = hasHeaderAlias(headers, GENERIC_IMPORT_ALIASES.fullName)
    || (
      hasHeaderAlias(headers, GENERIC_IMPORT_ALIASES.firstName)
      && hasHeaderAlias(headers, GENERIC_IMPORT_ALIASES.lastName)
    );
  if (hasPhone && hasName) return "generic";
  return "unknown";
}

function setUploadStatus(message, variant = "neutral") {
  const el = document.getElementById("dashboardUploadStatus");
  if (!el) return;
  el.textContent = message;
  el.classList.remove("upload-status-success", "upload-status-error");
  if (variant === "success") el.classList.add("upload-status-success");
  if (variant === "error") el.classList.add("upload-status-error");
}

function toNameCase(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join(" ");
}

function digitsOnly(value) {
  return String(value || "").replace(/\D/g, "");
}

function applyUploadPreflightValidation(rows) {
  const validatedRows = [];
  let changedRows = 0;
  let criticalErrors = 0;

  rows.forEach((row, idx) => {
    const updated = { ...row };
    let changed = false;

    const phoneDigits = digitsOnly(updated.mobile_phone || updated.phone || "");
    if (String(updated.mobile_phone || "") !== phoneDigits) changed = true;
    updated.mobile_phone = phoneDigits;
    updated.__uploadInvalidPhone = phoneDigits.length !== 10;
    updated.__uploadRowIndex = idx + 1;
    if (updated.__uploadInvalidPhone) criticalErrors += 1;

    const firstName = toNameCase(updated.first_name || updated.firstName || "");
    const lastName = toNameCase(updated.last_name || updated.lastName || "");
    if (firstName !== String(updated.first_name || "")) changed = true;
    if (lastName !== String(updated.last_name || "")) changed = true;
    updated.first_name = firstName;
    updated.last_name = lastName;
    updated.full_name = `${firstName} ${lastName}`.trim() || String(updated.full_name || "").trim();

    const stateValue = String(updated.state || updated.State || "").trim();
    if (!stateValue) {
      updated.state = "Check State";
      changed = true;
    } else {
      updated.state = stateValue;
    }

    if (changed) changedRows += 1;
    validatedRows.push(updated);
  });

  return {
    rows: validatedRows,
    changedRows,
    criticalErrors,
  };
}

function renderUploadPreview(rows = []) {
  const table = document.getElementById("uploadPreviewTable");
  const summary = document.getElementById("uploadPreviewSummary");
  if (!table || !summary) return;
  if (!Array.isArray(rows) || !rows.length) {
    table.innerHTML = `<tr><td colspan="5" class="muted">Upload a CSV to preview validation.</td></tr>`;
    summary.textContent = "No preview rows loaded.";
    return;
  }
  const invalidCount = rows.filter((row) => Boolean(row.__uploadInvalidPhone)).length;
  summary.textContent = invalidCount
    ? `${formatNumber.format(invalidCount)} phone number issue(s) must be fixed before import.`
    : `${formatNumber.format(rows.length)} row(s) ready to import.`;
  table.innerHTML = rows
    .slice(0, 40)
    .map((row) => {
      const invalid = Boolean(row.__uploadInvalidPhone);
      const name = escapeHtml(`${row.first_name || ""} ${row.last_name || ""}`.trim() || row.full_name || "Unnamed");
      const phone = escapeHtml(String(row.mobile_phone || ""));
      const stateValue = escapeHtml(String(row.state || ""));
      const queue = escapeHtml(String(row.routing_bucket || "-"));
      const issue = invalid ? "Invalid phone (needs 10 digits)" : "OK";
      return `
        <tr class="${invalid ? "upload-preview-row-error" : ""}">
          <td>${escapeHtml(String(row.__uploadRowIndex || ""))}</td>
          <td>${name}</td>
          <td>${phone}</td>
          <td>${stateValue}</td>
          <td>${issue} • ${queue}</td>
        </tr>
      `;
    })
    .join("");
}

function setImportButtonsState(enabled, loading = false) {
  const importBtn = document.getElementById("importUploadedToDbBtn");
  const cleanBtn = document.getElementById("importUploadedToDbCleanBtn");
  const hasCriticalErrors = Number(state.ui.uploadCriticalErrors || 0) > 0;
  const disabled = !enabled || loading || hasCriticalErrors;
  if (importBtn) {
    importBtn.disabled = disabled;
    importBtn.textContent = loading ? "Importing..." : "Finalize Import";
  }
  if (cleanBtn) {
    cleanBtn.disabled = disabled;
    cleanBtn.textContent = loading ? "Importing..." : "Import + Cleanup";
  }
}

async function importUploadedLeadsToLocalDb(withCleanup = false) {
  const rows = Array.isArray(state.ui.uploadedLeadRows) ? state.ui.uploadedLeadRows : [];
  if (!rows.length) {
    setUploadStatus("Upload a CSV first, then import.", "error");
    return;
  }
  if (!supabase && !LOCAL_DB_IMPORT_URL.trim()) {
    setUploadStatus("Local DB import URL is not configured.", "error");
    return;
  }
  if (Number(state.ui.uploadCriticalErrors || 0) > 0) {
    setUploadStatus("Fix invalid phone numbers in the preview before finalizing import.", "error");
    return;
  }

  setImportButtonsState(true, true);
  setUploadStatus(
    withCleanup
      ? "Importing leads to master DB with cleanup..."
      : "Importing leads to master DB...",
  );
  try {
    const cleanedRows = rows.map((row) => {
      const payload = { ...row };
      delete payload.__uploadInvalidPhone;
      delete payload.__uploadRowIndex;
      return payload;
    });
    const result = supabase
      ? await upsertLeadRowsToSupabase(cleanedRows)
      : await (async () => {
          const response = await fetch(LOCAL_DB_IMPORT_URL, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              rows: cleanedRows,
              withCleanup,
            }),
          });
          if (!response.ok) {
            throw new Error(`Import failed (${response.status})`);
          }
          return await response.json();
        })();
    const added = Number(result?.added || 0);
    const skipped = Number(result?.skipped_existing || 0);
    const invalid = Number(result?.skipped_invalid || 0);
    const cleanupChanged = Number(result?.cleanup?.changed_rows || 0);
    setUploadStatus(
      withCleanup
        ? `Import complete. Added ${formatNumber.format(added)}, skipped ${formatNumber.format(skipped)}, invalid ${formatNumber.format(invalid)}. Cleanup changed ${formatNumber.format(cleanupChanged)} row(s).`
        : `Import complete. Added ${formatNumber.format(added)}, skipped ${formatNumber.format(skipped)}, invalid ${formatNumber.format(invalid)}.`,
      "success",
    );
    if (supabase) {
      const refreshedLeads = await loadLeadRowsFromSupabase();
      renderDashboard({
        leads: refreshedLeads || [],
        activity: state.activity,
        bookings: state.bookings,
        sales: state.sales,
        targets: state.targets,
        sourcedLeads: state.sourcedLeads,
        carrierDocs: state.carrierDocs,
      });
    }
  } catch (error) {
    setUploadStatus(String(error.message || error), "error");
  } finally {
    setImportButtonsState(true, false);
  }
}

async function handleLeadUploadFromFileInput(event) {
  const file = event.target.files?.[0];
  if (!file) return;

  try {
    const text = await file.text();
    const parsedRows = parseCsv(text);
    if (!parsedRows.length) throw new Error("This CSV file is empty.");

    const detectedFormat = detectLeadFormat(parsedRows);
    let leads = parsedRows;
    let formatLabel = "dashboard";

    if (detectedFormat === "lena") {
      leads = mapLenaRowsToDashboardLeads(parsedRows);
      formatLabel = "Lena";
    } else if (detectedFormat === "closrleads") {
      leads = mapClosrLeadsRowsToDashboardLeads(parsedRows);
      formatLabel = "ClosrLeads";
    } else if (detectedFormat === "generic") {
      leads = mapGenericLeadRowsToDashboardLeads(parsedRows);
      formatLabel = "Generic";
    } else if (detectedFormat === "dashboard") {
      formatLabel = "dashboard";
    } else {
      throw new Error("Unsupported CSV format. Upload a Lena lead file, ClosrLeads export, dashboard lead export, or a CSV with recognizable name and phone columns.");
    }

    const cleanedUpload = sanitizeLeadRows(leads);
    const validatedUpload = applyUploadPreflightValidation(cleanedUpload.rows);
    state.ui.uploadedLeadRows = validatedUpload.rows;
    state.ui.uploadCriticalErrors = validatedUpload.criticalErrors;
    renderUploadPreview(validatedUpload.rows);

    renderDashboard({
      leads: validatedUpload.rows,
      activity: state.activity,
      bookings: state.bookings,
      sales: state.sales,
      targets: state.targets,
      sourcedLeads: state.sourcedLeads,
    });

    setUploadStatus(
      `${formatNumber.format(validatedUpload.rows.length)} ${formatLabel} leads loaded from ${file.name}. Cleaned ${formatNumber.format(cleanedUpload.changedRows + validatedUpload.changedRows)} row(s). ${formatNumber.format(validatedUpload.criticalErrors)} critical phone error(s).`,
      validatedUpload.criticalErrors ? "error" : "success",
    );
    setImportButtonsState(validatedUpload.rows.length > 0, false);
  } catch (error) {
    const message = String(error.message || error);
    document.getElementById("datasetStatus").textContent = message;
    document.getElementById("datasetStatus").style.background = "var(--red-soft)";
    document.getElementById("datasetStatus").style.color = "var(--red)";
    setUploadStatus(message, "error");
    state.ui.uploadedLeadRows = [];
    state.ui.uploadCriticalErrors = 0;
    renderUploadPreview([]);
    setImportButtonsState(false, false);
  } finally {
    event.target.value = "";
  }
}

function countBy(rows, key) {
  return rows.reduce((acc, row) => {
    const value = row[key] || "Unspecified";
    acc[value] = (acc[value] || 0) + 1;
    return acc;
  }, {});
}

function sumBy(rows, key) {
  return rows.reduce((acc, row) => acc + Number(row[key] || 0), 0);
}

function pct(part, whole) {
  if (!whole) return "0%";
  return `${Math.round((part / whole) * 100)}%`;
}

function topEntries(record) {
  return Object.entries(record).sort((a, b) => b[1] - a[1]);
}

function getSourcedLeadState(id) {
  return state.sourcedLeadState[id] || { stage: "new", ownerNote: "" };
}

function createBarRows(container, values, color = "var(--accent-gradient)") {
  if (!container) return;
  container.innerHTML = "";
  const entries = topEntries(values);
  const max = entries[0]?.[1] || 1;

  entries.forEach(([label, value]) => {
    const row = document.createElement("div");
    row.className = "bar-row";
    row.innerHTML = `
      <div class="bar-row-top">
        <span>${label}</span>
        <strong>${formatNumber.format(value)}</strong>
      </div>
      <div class="bar-track">
        <div class="bar-fill" style="width:${(value / max) * 100}%; background:${color};"></div>
      </div>
    `;
    container.appendChild(row);
  });
}

function normalizeDispositionBucket(row) {
  const raw =
    String(row.disposition || row.lead_status || row.contact_eligibility || "")
      .trim()
      .toLowerCase() || "new";

  if (["new", "not_started"].includes(raw)) return "New";
  if (["review_required", "manual_review", "working"].includes(raw)) return "Review Required";
  if (raw === "quoted") return "Quoted";
  if (raw === "sold" || raw === "active") return "Sold";
  if (raw === "not_qualified" || raw === "blocked") return "Not Qualified";
  if (raw === "not_interested") return "Not Interested";
  if (raw === "follow_up" || raw === "callback" || raw === "no_answer") return "Follow-up";
  return "Review Required";
}

function toFriendlyQueueLabel(value) {
  return String(value || "Unspecified")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function toFriendlyChannelLabel(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized || normalized === "manual_review") return "Manual Review";
  if (normalized === "phone_call") return "Phone";
  if (normalized === "email") return "Email";
  if (normalized === "sms") return "SMS";
  return toFriendlyQueueLabel(normalized);
}

function toFriendlyCarrierLabel(value) {
  const normalized = String(value || "").trim();
  return normalized || "Unmatched";
}

function destroyChartIfExists(key) {
  if (!opsCharts[key]) return;
  opsCharts[key].destroy();
  opsCharts[key] = null;
}

function buildDispositionCounts(leads) {
  return leads.reduce((acc, row) => {
    const label = normalizeDispositionBucket(row);
    acc[label] = (acc[label] || 0) + 1;
    return acc;
  }, {});
}

function renderOpsCharts(leads) {
  if (typeof Chart === "undefined") return;
  const pipelineCanvas = document.getElementById("pipelineChart");
  const carrierCanvas = document.getElementById("carrierMixChart");
  const queueCanvas = document.getElementById("queueHealthChart");
  if (!pipelineCanvas || !carrierCanvas || !queueCanvas) return;

  const dispositionCounts = buildDispositionCounts(leads);
  const carrierCounts = leads.reduce((acc, row) => {
    const label = toFriendlyCarrierLabel(row.carrier_match);
    acc[label] = (acc[label] || 0) + 1;
    return acc;
  }, {});
  const queueCounts = countBy(leads, "routing_bucket");

  const pipelineEntries = topEntries(dispositionCounts);
  const carrierEntries = topEntries(carrierCounts).slice(0, 8);
  const queueEntries = topEntries(queueCounts).slice(0, 8);

  destroyChartIfExists("pipeline");
  destroyChartIfExists("carrier");
  destroyChartIfExists("queue");

  opsCharts.pipeline = new Chart(pipelineCanvas, {
    type: "bar",
    data: {
      labels: pipelineEntries.map(([label]) => label),
      datasets: [
        {
          label: "Leads",
          data: pipelineEntries.map(([, value]) => value),
          backgroundColor: "#38bdf8",
          borderRadius: 8,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: {
          beginAtZero: true,
          ticks: { precision: 0 },
          grid: { color: "rgba(148, 163, 184, 0.2)" },
        },
        x: {
          grid: { display: false },
        },
      },
    },
  });

  opsCharts.carrier = new Chart(carrierCanvas, {
    type: "doughnut",
    data: {
      labels: carrierEntries.map(([label]) => label),
      datasets: [
        {
          data: carrierEntries.map(([, value]) => value),
          backgroundColor: ["#22c55e", "#38bdf8", "#f59e0b", "#f97316", "#a78bfa", "#ef4444", "#14b8a6", "#64748b"],
          borderWidth: 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: { boxWidth: 12, usePointStyle: true },
        },
      },
      cutout: "60%",
    },
  });

  opsCharts.queue = new Chart(queueCanvas, {
    type: "bar",
    data: {
      labels: queueEntries.map(([label]) => toFriendlyQueueLabel(label)),
      datasets: [
        {
          label: "Leads",
          data: queueEntries.map(([, value]) => value),
          backgroundColor: "#60a5fa",
          borderRadius: 8,
        },
      ],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: {
          beginAtZero: true,
          ticks: { precision: 0 },
          grid: { color: "rgba(148, 163, 184, 0.2)" },
        },
        y: {
          grid: { display: false },
        },
      },
    },
  });
}

function formatAppointmentTime(value) {
  const raw = String(value || "").trim();
  if (!raw) return "Time TBD";
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return raw;
  return dt.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function toDateTimeLocalValue(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return raw.slice(0, 16);
  const pad = (n) => String(n).padStart(2, "0");
  return `${dt.getFullYear()}-${pad(dt.getMonth() + 1)}-${pad(dt.getDate())}T${pad(dt.getHours())}:${pad(
    dt.getMinutes(),
  )}`;
}

function renderTodaysAppointments(items = []) {
  const listEl = document.getElementById("todayAppointmentsList");
  const statusEl = document.getElementById("todayAppointmentsStatus");
  if (!listEl || !statusEl) return;
  if (!Array.isArray(items) || !items.length) {
    listEl.innerHTML = `<li class="muted">No appointments scheduled for today.</li>`;
    statusEl.textContent = "0 scheduled";
    return;
  }
  statusEl.textContent = `${items.length} scheduled`;
  listEl.innerHTML = items
    .map((item) => {
      const summary = escapeHtml(String(item.summary || "(No title)"));
      const time = escapeHtml(formatAppointmentTime(item.start));
      const link = String(item.htmlLink || "").trim();
      const action = link
        ? `<a href="${escapeHtml(link)}" target="_blank" rel="noopener noreferrer">Open</a>`
        : "";
      return `<li><strong>${time}</strong> ${summary} ${action}</li>`;
    })
    .join("");
}

async function refreshTodaysAppointments() {
  const statusEl = document.getElementById("todayAppointmentsStatus");
  if (statusEl) statusEl.textContent = "Loading...";
  try {
    let items = [];
    if (supabase) {
      const appointmentRows = await loadAppointmentsFromSupabase({
        start: startOfDayIso(0),
        end: endOfDayIso(0),
        limit: 100,
      });
      items = appointmentRows.map((row) => buildCalendarEventFromAppointment(row));
    } else {
      if (!LOCAL_DB_CALENDAR_TODAY_URL.trim()) throw new Error("Calendar endpoint not configured");
      const response = await fetch(LOCAL_DB_CALENDAR_TODAY_URL, { method: "GET" });
      if (!response.ok) throw new Error(`Calendar fetch failed (${response.status})`);
      const data = await response.json();
      items = Array.isArray(data?.items) ? data.items : [];
    }
    state.todayAppointments = items;
    renderTodaysAppointments(items);
  } catch (error) {
    if (statusEl) statusEl.textContent = "Calendar unavailable";
    renderTodaysAppointments([]);
    console.error(error);
  }
}

function formatDateTimeShort(value) {
  const raw = String(value || "").trim();
  if (!raw) return "-";
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return raw;
  return dt.toLocaleString([], { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

function matchLeadFromCalendarEvent(eventRow) {
  const summary = String(eventRow?.summary || "").toLowerCase();
  const attendees = Array.isArray(eventRow?.attendees) ? eventRow.attendees : [];
  const attendeeEmails = attendees.map((item) => String(item?.email || "").toLowerCase()).filter(Boolean);
  return (
    state.leads.find((lead) => {
      const fullName = String(lead.full_name || `${lead.first_name || ""} ${lead.last_name || ""}`)
        .trim()
        .toLowerCase();
      const email = String(lead.email || "").toLowerCase();
      if (fullName && summary.includes(fullName)) return true;
      if (email && attendeeEmails.includes(email)) return true;
      return false;
    }) || null
  );
}

function renderCalendarEventList(listId, countId, items) {
  const listEl = document.getElementById(listId);
  const countEl = document.getElementById(countId);
  if (!listEl || !countEl) return;
  const rows = dedupeCalendarEvents(items);
  countEl.textContent = String(rows.length);
  if (!rows.length) {
    listEl.innerHTML = `<li class="muted">No events.</li>`;
    return;
  }
  listEl.innerHTML = rows
    .slice(0, 25)
    .map((row) => {
      const time = escapeHtml(formatDateTimeShort(row.start));
      const summary = escapeHtml(String(row.summary || "(No title)"));
      const matchedLead = matchLeadFromCalendarEvent(row);
      const openLink = String(row.htmlLink || "").trim();
      const openBtn = openLink
        ? `<a class="ghost-button slim link-button" href="${escapeHtml(openLink)}" target="_blank" rel="noreferrer">Open</a>`
        : "";
      const loadBtn = matchedLead
        ? `<button class="ghost-button slim" type="button" data-calendar-load-lead="${escapeHtml(
            String(matchedLead.lead_external_id || ""),
          )}">Load Lead</button>`
        : "";
      return `<li><strong>${time}</strong> ${summary} <span class="detail-actions">${openBtn}${loadBtn}</span></li>`;
    })
    .join("");
}

function classifyFollowUpStatus(value) {
  const raw = String(value || "").trim();
  if (!raw) return "No appointment";
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return "Scheduled";
  const now = new Date();
  if (dt.getTime() < now.getTime()) return "Overdue";
  return "Upcoming";
}

function renderCalendarLeadQueue() {
  const table = document.getElementById("calendarLeadQueueTable");
  const countEl = document.getElementById("calendarLeadQueueCount");
  if (!table || !countEl) return;
  const byLeadId = new Map();
  for (const lead of state.leads) {
    const leadId = String(lead?.lead_external_id || "").trim();
    const nextAt = String(lead?.next_appointment_time || "").trim();
    if (!leadId || !nextAt) continue;
    const existing = byLeadId.get(leadId);
    const leadTs = Date.parse(nextAt) || Number.MAX_SAFE_INTEGER;
    const existingTs = Date.parse(String(existing?.next_appointment_time || "")) || Number.MAX_SAFE_INTEGER;
    if (!existing || leadTs >= existingTs) {
      byLeadId.set(leadId, lead);
    }
  }
  const rows = Array.from(byLeadId.values()).sort((a, b) => {
    const aTs = Date.parse(String(a.next_appointment_time || "")) || Number.MAX_SAFE_INTEGER;
    const bTs = Date.parse(String(b.next_appointment_time || "")) || Number.MAX_SAFE_INTEGER;
    return aTs - bTs;
  });
  countEl.textContent = `${rows.length} queued`;
  if (!rows.length) {
    table.innerHTML = `<tr><td colspan="5" class="muted">No scheduled follow-ups in local lead records.</td></tr>`;
    return;
  }
  table.innerHTML = rows
    .slice(0, 150)
    .map((lead) => {
      const leadId = String(lead.lead_external_id || "");
      const fullName = escapeHtml(String(lead.full_name || `${lead.first_name || ""} ${lead.last_name || ""}`).trim() || "Unnamed");
      const contact = escapeHtml(String(lead.mobile_phone || lead.email || "-"));
      const when = escapeHtml(formatDateTimeShort(lead.next_appointment_time));
      const status = escapeHtml(classifyFollowUpStatus(lead.next_appointment_time));
      return `<tr>
        <td>${when}</td>
        <td>${fullName}</td>
        <td>${contact}</td>
        <td>${status}</td>
        <td><button class="ghost-button slim" type="button" data-calendar-load-local="${escapeHtml(leadId)}">Load in Call Desk</button></td>
      </tr>`;
    })
    .join("");
}

function renderCalendarTab() {
  renderCalendarEventList("calendarTodayList", "calendarTodayCount", state.calendarTodayEvents);
  renderCalendarEventList("calendarWeekList", "calendarWeekCount", state.calendarWeekEvents);
  renderCalendarLeadQueue();
}

async function refreshCalendarTabData() {
  const statusEl = document.getElementById("calendarStatus");
  if (statusEl) statusEl.textContent = "Loading...";
  try {
    if (supabase) {
      const [todayRows, weekRows] = await Promise.all([
        loadAppointmentsFromSupabase({
          start: startOfDayIso(0),
          end: endOfDayIso(0),
          limit: 100,
        }),
        loadAppointmentsFromSupabase({
          start: startOfDayIso(0),
          end: endOfDayIso(30),
          limit: 500,
        }),
      ]);
      state.calendarTodayEvents = todayRows.map((row) => buildCalendarEventFromAppointment(row));
      state.calendarWeekEvents = weekRows.map((row) => buildCalendarEventFromAppointment(row));
    } else {
      const [todayResp, weekResp] = await Promise.all([
        fetch(LOCAL_DB_CALENDAR_TODAY_URL, { method: "GET" }),
        fetch(LOCAL_DB_CALENDAR_WEEK_URL, { method: "GET" }),
      ]);
      if (!todayResp.ok) throw new Error(`Today fetch failed (${todayResp.status})`);
      if (!weekResp.ok) throw new Error(`Week fetch failed (${weekResp.status})`);
      const todayData = await todayResp.json();
      const weekData = await weekResp.json();
      state.calendarTodayEvents = Array.isArray(todayData?.items) ? todayData.items : [];
      state.calendarWeekEvents = Array.isArray(weekData?.items) ? weekData.items : [];
    }
    renderCalendarTab();
    if (statusEl) {
      statusEl.textContent = `Updated ${new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}`;
    }
  } catch (error) {
    console.error(error);
    if (statusEl) statusEl.textContent = "Calendar unavailable";
    state.calendarTodayEvents = [];
    state.calendarWeekEvents = [];
    renderCalendarTab();
  }
}

function buildSummary(leads, activity, bookings, sales, targets) {
  const queueCounts = countBy(leads, "routing_bucket");
  const channelCounts = countBy(leads, "recommended_channel");
  const sequenceCounts = countBy(leads, "sequence_name");
  const sourceCounts = countBy(leads, "Source Type");
  const touchCounts = countBy(leads, "First Touch Strategy");
  const totalRevenue = sumBy(sales, "commission_revenue");
  const campaignSpend = sumBy(targets, "campaign_spend");
  const quoteCount = sales.filter((row) => row.policy_status === "Quoted").length;
  const closedCount = sales.filter((row) => row.policy_status === "Active").length;
  const bookedCount = bookings.filter((row) => row.booking_status === "Booked").length;
  const contactableLeads = leads.filter(
    (row) => row.recommended_channel === "phone_call" || row.recommended_channel === "email",
  ).length;
  const reviewRequired = leads.filter((row) => row.contact_eligibility === "review_required").length;
  const blockedLeads = leads.filter((row) => row.contact_eligibility === "blocked").length;
  const pendingDnc = leads.filter((row) => row.dnc_status === "pending_check").length;
  const highPriority = leads.filter((row) => row.priority_tier === "high").length;
  const contactedLeads = contactableLeads || leads.length || 1;
  const conversionRatePct = Number(((closedCount / contactedLeads) * 100).toFixed(1));
  const underwritingCount = leads.filter(
    (row) => String(row.pipeline_status || "").trim().toLowerCase() === "underwriting",
  ).length;
  const premiumRows = sales.filter((row) => Number(row.annual_premium) > 0);
  const avgAnnualPremium = premiumRows.length ? sumBy(premiumRows, "annual_premium") / premiumRows.length : 1200;
  const pipelineValue = underwritingCount * avgAnnualPremium;

  return {
    totalLeads: leads.length,
    queueCounts,
    channelCounts,
    sequenceCounts,
    sourceCounts,
    touchCounts,
    contactableLeads,
    reviewRequired,
    blockedLeads,
    pendingDnc,
    highPriority,
    conversionRatePct,
    pipelineValue,
    phonePreferred: channelCounts.phone_call || 0,
    emailOnly: channelCounts.email || 0,
    totalRevenue,
    campaignSpend,
    quoteCount,
    closedCount,
    bookedCount,
    revenuePerLead: leads.length ? totalRevenue / leads.length : 0,
    costPerLead: leads.length ? campaignSpend / leads.length : 0,
    primaryQueueLabel: topEntries(queueCounts)[0]?.[0] || "No queue data",
  };
}

function buildSourcedSummary(sourcedLeads) {
  const queueCounts = { "Enrichment Needed": 0, "Ready For Review": 0 };
  const missingFields = {
    Phone: 0,
    Email: 0,
    "Last Name": 0,
    "Business Name": 0,
    "Post URL": 0,
  };

  let rejected = 0;

  sourcedLeads.forEach((row) => {
    const decision = leadPassesCriteria(row, state.criteria);
    if (!decision.accepted) {
      rejected += 1;
      return;
    }
    queueCounts[sourcedQueueLabel(row)] += 1;
    if (!hasValue(row.Phone)) missingFields.Phone += 1;
    if (!hasValue(row.Email)) missingFields.Email += 1;
    if (!hasValue(row["Last Name"])) missingFields["Last Name"] += 1;
    if (!hasValue(row["Business Name"])) missingFields["Business Name"] += 1;
    if (!hasValue(row["Post or Message URL"])) missingFields["Post URL"] += 1;
  });

  return {
    queueCounts,
    missingFields,
    enrichmentNeeded: queueCounts["Enrichment Needed"],
    readyForReview: queueCounts["Ready For Review"],
    rejected,
  };
}

function criteriaTriggerCount(criteria) {
  return Object.values(criteria.triggers).filter(Boolean).length;
}

function tooltipIcon(text) {
  const copy = String(text || "").trim();
  if (!copy) return "";
  return `<span class="oc-tooltip" tabindex="0"><span class="oc-tooltip-icon" aria-hidden="true">ⓘ</span><span class="oc-tooltip-bubble" role="tooltip">${escapeHtml(copy)}</span></span>`;
}

function renderMetrics(summary) {
  const grid = document.getElementById("metricsGrid");
  const cards = [
    {
      label: "Total Leads",
      value: formatNumber.format(summary.totalLeads),
      note: `${formatNumber.format(summary.contactableLeads)} ready for call or email once cleared`,
    },
    {
      label: "Needs Review",
      value: formatNumber.format(summary.reviewRequired),
      note: "Consent and DNC should be checked before automation",
    },
    {
      label: "Conversion Rate",
      value: `${summary.conversionRatePct}%`,
      note: "Issued-policy conversion on contacted leads",
      tooltip: "(Total Issued Policies / Total Contacted Leads) * 100",
    },
    {
      label: "Pipeline Value",
      value: formatCurrency.format(summary.pipelineValue),
      note: "Estimated premium in underwriting stage",
      tooltip: "Estimated annual premium of all apps currently in Underwriting.",
    },
    {
      label: "High Priority",
      value: formatNumber.format(summary.highPriority),
      note: "Manual work before sequence enrollment",
    },
    {
      label: "Revenue / Lead",
      value: formatCurrency.format(summary.revenuePerLead),
      note: "Will improve as bookings and sales are logged",
    },
  ];

  grid.innerHTML = cards
    .map(
      (card) => `
        <article class="metric-card">
          <p class="panel-label">${card.label}${tooltipIcon(card.tooltip)}</p>
          <h3>${card.value}</h3>
          <p class="muted">${card.note}</p>
        </article>
      `,
    )
    .join("");
}

function renderTriage(summary) {
  const grid = document.getElementById("triageGrid");
  const cards = [
    {
      title: "Start Here",
      value: `${formatNumber.format(summary.highPriority)} leads`,
      note: "Manual follow-up, cleanup, or compliance review first.",
      tone: "amber",
    },
    {
      title: "Ready For Nurture",
      value: `${formatNumber.format(summary.phonePreferred)} call-first`,
      note: "Main batch for round-2 reactivation after DNC and consent review.",
      tone: "green",
    },
    {
      title: "Email Salvage",
      value: `${formatNumber.format(summary.emailOnly)} leads`,
      note: "Bad phone or email-first records worth saving.",
      tone: "teal",
    },
    {
      title: "Blocked Now",
      value: `${formatNumber.format(summary.blockedLeads)} leads`,
      note: "Do not automate until status changes.",
      tone: "red",
    },
  ];

  grid.innerHTML = cards
    .map(
      (card) => `
        <article class="triage-card ${card.tone}">
          <p class="panel-label">${card.title}</p>
          <h3>${card.value}</h3>
          <p class="muted">${card.note}</p>
        </article>
      `,
    )
    .join("");
}

function renderCompliance(summary) {
  const body = document.getElementById("complianceTable");
  const rows = [
    [
      "Consent review required",
      summary.reviewRequired,
      "Keep these out of call, SMS, WhatsApp, and email automation until consent is verified.",
    ],
    [
      "Blocked leads",
      summary.blockedLeads,
      "Do not contact, suppression, or insufficient contact data.",
    ],
    [
      "Pending DNC check",
      summary.pendingDnc,
      "Run DNC and internal suppression checks before outbound activity.",
    ],
    [
      "Email-only salvage",
      summary.emailOnly,
      "Useful when the phone path is weak but email still exists.",
    ],
  ];

  body.innerHTML = rows
    .map(
      ([metric, count, note]) => `
        <tr>
          <td>${metric}</td>
          <td><strong>${formatNumber.format(count)}</strong></td>
          <td>${note}</td>
        </tr>
      `,
    )
    .join("");
}

function renderPriorityTable(leads) {
  const table = document.getElementById("priorityTable");
  if (!table) return;
  const important = leads
    .filter(
      (row) =>
        row.priority_tier === "high" ||
        row.routing_bucket !== "health_nurture_queue" ||
        row.recommended_channel === "none",
    )
    .slice(0, 12);

  table.innerHTML = important
    .map(
      (row) => `
        <tr>
          <td>${row.full_name || `${row.first_name} ${row.last_name}`.trim()}</td>
          <td>${row.routing_bucket}</td>
          <td>${row.recommended_channel || "manual_review"}</td>
          <td>${row.recommended_next_action}</td>
          <td>${row.notes || row.raw_tags || ""}</td>
        </tr>
      `,
    )
    .join("");
}

function renderReadiness(summary) {
  const list = document.getElementById("readinessList");
  const dncEl = document.getElementById("topDncCount");
  const queueEl = document.getElementById("topQueueCount");
  const statusPills = document.getElementById("topStatusPills");
  const items = [
    `${formatNumber.format(summary.pendingDnc)} leads still need DNC review`,
    `${formatNumber.format(summary.reviewRequired)} leads still need consent review`,
    `${formatNumber.format(summary.highPriority)} leads should be worked manually first`,
    `${formatNumber.format(summary.phonePreferred)} leads are your main call queue after checks`,
  ];

  if (list) list.innerHTML = items.map((item) => `<li>${item}</li>`).join("");
  if (dncEl) dncEl.textContent = `DNC: ${formatNumber.format(summary.pendingDnc)}`;
  if (queueEl) queueEl.textContent = `Queue: ${formatNumber.format(summary.phonePreferred)}`;
  if (statusPills) statusPills.title = items.join(" | ");
}

function renderOutcomeAnalytics() {
  const container = document.getElementById("outcomeFunnelBars");
  if (!container) return;
  const outcomes = state.callOutcomes || [];
  if (!outcomes.length) {
    container.innerHTML = `<p class="muted">No call outcomes yet. Save outcomes from Call Desk to populate this funnel.</p>`;
    return;
  }
  const stageCounts = {};
  outcomes.forEach((row) => {
    const key = `${row.needArea || "unknown"} -> ${row.lane || "unknown"} -> ${row.disposition || "unknown"}`;
    stageCounts[key] = (stageCounts[key] || 0) + 1;
  });
  createBarRows(container, stageCounts, "linear-gradient(90deg, #2f67d8, #71d8ff)");
}

function renderDataQualityGuardrails(leads) {
  const container = document.getElementById("dataQualityList");
  if (!container) return;
  const now = Date.now();
  const staleDays = 30;
  const metrics = {
    missing_phone: 0,
    missing_email: 0,
    missing_consent: 0,
    stale_activity: 0,
  };

  leads.forEach((row) => {
    if (!String(row.mobile_phone || "").trim()) metrics.missing_phone += 1;
    if (!String(row.email || "").trim()) metrics.missing_email += 1;
    if (!String(row.consent_status || "").trim() || String(row.consent_status || "").toLowerCase() === "unknown") {
      metrics.missing_consent += 1;
    }
    const rawDate = String(row.last_activity_at_source || "").trim();
    if (rawDate) {
      const parsed = Date.parse(rawDate);
      if (!Number.isNaN(parsed)) {
        const ageDays = (now - parsed) / (1000 * 60 * 60 * 24);
        if (ageDays > staleDays) metrics.stale_activity += 1;
      }
    }
  });
  createBarRows(container, metrics, "linear-gradient(90deg, #a96c11, #f7c96a)");
}

function renderSourcing(summary) {
  const morning = document.getElementById("morningChecklist");
  const evening = document.getElementById("eveningChecklist");

  const morningItems = [
    "Check Facebook groups and Meta inbox for baby, home-buying, job-change, and Medicare triggers.",
    "Review LinkedIn recent job changes, founders, and self-employed targets.",
    "Scan Reddit help posts for immediate insurance questions and trigger events.",
    "Classify new candidates into Social, Paid Lead Vendor, or Unknown before outreach.",
  ];

  const eveningItems = [
    "Deduplicate new source candidates into the master Lena-format file.",
    "Review unknown-source leads and add source evidence.",
    "Verify first-touch strategy: text first for social, call first for paid.",
    "Summarize trigger counts, blockers, and follow-up opportunities for tomorrow.",
  ];

  morning.innerHTML = morningItems.map((item) => `<li>${item}</li>`).join("");
  evening.innerHTML = eveningItems.map((item) => `<li>${item}</li>`).join("");

  createBarRows(
    document.getElementById("sourceBars"),
    summary.sourceCounts,
    "linear-gradient(90deg, #8f4c1f, #d89a4c)",
  );
  createBarRows(
    document.getElementById("touchBars"),
    summary.touchCounts,
    "linear-gradient(90deg, #2f7a57, #7eb98f)",
  );
}

function renderCarrierDocIntake(carrierDocs) {
  const summaryEl = document.getElementById("carrierDocSummary");
  const tableEl = document.getElementById("carrierDocTable");
  if (!summaryEl || !tableEl) return;

  const docs = (carrierDocs || []).map((row) => ({
    document_id: String(row.document_id || "").trim(),
    carrier: String(row.carrier || "Unknown").trim() || "Unknown",
    title: String(row.title || "Untitled").trim() || "Untitled",
    source_url: String(row.source_url || "").trim(),
    status: String(row.status || "pending_review").trim() || "pending_review",
    priority: String(row.priority || "normal").trim() || "normal",
    notes: String(row.notes || "").trim(),
  }));

  const statusCounts = docs.reduce((acc, row) => {
    const key = row.status || "pending_review";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  const pendingCount = statusCounts.pending_review || 0;
  const reviewedCount = docs.length - pendingCount;

  summaryEl.innerHTML = `
    <article class="metric-card">
      <p class="panel-label">Carrier Docs</p>
      <h3>${formatNumber.format(docs.length)}</h3>
      <p class="muted">External references linked into intake process</p>
    </article>
    <article class="metric-card">
      <p class="panel-label">Pending Review</p>
      <h3>${formatNumber.format(pendingCount)}</h3>
      <p class="muted">Needs carrier/product classification before automation</p>
    </article>
    <article class="metric-card">
      <p class="panel-label">Reviewed</p>
      <h3>${formatNumber.format(reviewedCount)}</h3>
      <p class="muted">Ready to use in quoting and workflow guidance</p>
    </article>
  `;

  tableEl.innerHTML = docs
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(row.document_id)}</td>
          <td>${escapeHtml(row.carrier)}</td>
          <td><a href="${escapeHtml(row.source_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(row.title)}</a></td>
          <td>${escapeHtml(row.status)}</td>
          <td>${escapeHtml(row.priority)}</td>
          <td>${escapeHtml(row.notes || "-")}</td>
        </tr>
      `,
    )
    .join("");
}

function renderCriteriaPanel() {
  const { criteria } = state;
  document.getElementById("criteriaGeography").value = criteria.geography;
  document.getElementById("criteriaQuality").value = criteria.quality;
  document.getElementById("triggerNewParent").checked = criteria.triggers.newParent;
  document.getElementById("triggerHomeBuyer").checked = criteria.triggers.homeBuyer;
  document.getElementById("triggerJobLoss").checked = criteria.triggers.jobLoss;
  document.getElementById("triggerJobChange").checked = criteria.triggers.jobChange;
  document.getElementById("triggerMedicare").checked = criteria.triggers.medicare;
  document.getElementById("triggerBusinessOwner").checked = criteria.triggers.businessOwner;
  document.getElementById("ruleRequireUs").checked = criteria.rules.requireUs;
  document.getElementById("ruleRequireName").checked = criteria.rules.requireName;
  document.getElementById("ruleRequireTrigger").checked = criteria.rules.requireTrigger;
  document.getElementById("ruleRequirePath").checked = criteria.rules.requirePath;
  document.getElementById("ruleRejectAnonymous").checked = criteria.rules.rejectAnonymous;

  const qualityLabel = criteria.quality === "strong" ? "strong only" : "reviewable+";
  document.getElementById("criteriaSummary").textContent =
    `${criteriaTriggerCount(criteria)} trigger types active • ${qualityLabel} • U.S.-only`;
}

function renderGuidancePanel() {
  const strongLeads = [
    "Clear U.S. location evidence like city, state, local group, or U.S. employer.",
    "Real trigger moment: new baby, home purchase, job loss, or new job.",
    "Real identity plus a stable profile or message path.",
    "Specific life context that supports a relevant insurance conversation.",
  ];
  const weakLeads = [
    "Anonymous posts with no stable profile.",
    "No U.S. location signal or clearly non-U.S. profile.",
    "Generic interest with no trigger event.",
    "No profile, no employer, no comments, and no way to re-find the person.",
  ];
  const sourceMoves = [
    "Favor U.S. local Facebook groups over broad global groups.",
    "Work comments and profile details before saving a lead row.",
    "Prefer posts with recent activity, not stale posts with no reply path.",
    "Use enrichment only to confirm a candidate, not to rescue low-quality noise.",
  ];

  document.getElementById("strongLeadList").innerHTML = strongLeads.map((item) => `<li>${item}</li>`).join("");
  document.getElementById("weakLeadList").innerHTML = weakLeads.map((item) => `<li>${item}</li>`).join("");
  document.getElementById("sourceMoveList").innerHTML = sourceMoves.map((item) => `<li>${item}</li>`).join("");
}

function renderEnrichmentQueues(sourcedSummary) {
  document.getElementById("enrichmentNeededCount").textContent = formatNumber.format(
    sourcedSummary.enrichmentNeeded,
  );
  document.getElementById("readyReviewCount").textContent = formatNumber.format(
    sourcedSummary.readyForReview,
  );
  document.getElementById("enrichmentNeededNote").textContent = sourcedSummary.enrichmentNeeded
    ? "These leads still need identity, contact-path, or source-detail completion."
    : "No sourced leads are currently waiting on enrichment.";
  document.getElementById("readyReviewNote").textContent = sourcedSummary.readyForReview
    ? "These leads are complete enough for manual review and qualification."
    : "No sourced leads are review-ready yet.";
  document.getElementById("rejectedCriteriaCount").textContent = formatNumber.format(
    sourcedSummary.rejected,
  );
  document.getElementById("rejectedCriteriaNote").textContent = sourcedSummary.rejected
    ? "These leads fail your current U.S., trigger, or quality rules and are hidden from the working table."
    : "No sourced leads are currently being rejected by your active criteria.";

  createBarRows(
    document.getElementById("missingFieldBars"),
    sourcedSummary.missingFields,
    "linear-gradient(90deg, #b37717, #d7a148)",
  );
}

function renderRoiStats(summary) {
  const container = document.getElementById("roiStats");
  const stats = [
    ["Campaign spend", formatCurrency.format(summary.campaignSpend)],
    ["Booked appointments", formatNumber.format(summary.bookedCount)],
    ["Closed policies", formatNumber.format(summary.closedCount)],
    ["Total commission revenue", formatCurrency.format(summary.totalRevenue)],
    ["Cost per lead", formatCurrency.format(summary.costPerLead)],
    ["Close rate", pct(summary.closedCount, summary.quoteCount || summary.totalLeads)],
  ];

  container.innerHTML = stats
    .map(
      ([label, value]) => `
        <div class="stat-item">
          <span class="muted">${label}</span>
          <strong>${value}</strong>
        </div>
      `,
    )
    .join("");
}

function populateFilters(leads) {
  const queueFilter = document.getElementById("queueFilter");
  const channelFilter = document.getElementById("channelFilter");
  if (!queueFilter || !channelFilter) return;
  const queues = ["all", ...Object.keys(countBy(leads, "routing_bucket"))];
  const channels = ["all", ...Object.keys(countBy(leads, "recommended_channel"))];

  queueFilter.innerHTML = queues
    .map((value) => `<option value="${value}">${value === "all" ? "All queues" : value}</option>`)
    .join("");
  channelFilter.innerHTML = channels
    .map((value) => `<option value="${value}">${value === "all" ? "All channels" : value}</option>`)
    .join("");
}

function populateLeadSelectionFilters(leads) {
  const queueFilter = document.getElementById("leadSelectQueue");
  const eligibilityFilter = document.getElementById("leadSelectEligibility");
  if (!queueFilter || !eligibilityFilter) return;

  const queues = ["all", ...Object.keys(countBy(leads, "routing_bucket"))];
  const eligibilityValues = ["all", ...Object.keys(countBy(leads, "contact_eligibility"))];

  queueFilter.innerHTML = queues
    .map((value) => `<option value="${value}">${value === "all" ? "All queues" : value}</option>`)
    .join("");
  eligibilityFilter.innerHTML = eligibilityValues
    .map((value) => `<option value="${value}">${value === "all" ? "All eligibility" : value}</option>`)
    .join("");
}

function populateCampaignFilters(leads) {
  const queueFilter = document.getElementById("campaignQueue");
  const eligibilityFilter = document.getElementById("campaignEligibility");
  if (!queueFilter || !eligibilityFilter) return;

  const normalized = leads.map((row) => normalizeLeadRow(row));
  const queues = ["all", ...Object.keys(countBy(normalized, "routing_bucket"))];
  const eligibilityValues = ["all", ...Object.keys(countBy(normalized, "contact_eligibility"))];

  queueFilter.innerHTML = queues
    .map((value) => `<option value="${value}">${value === "all" ? "All queues" : value}</option>`)
    .join("");
  eligibilityFilter.innerHTML = eligibilityValues
    .map((value) => `<option value="${value}">${value === "all" ? "All eligibility" : value}</option>`)
    .join("");
}

function isFinalDisposition(row) {
  const disposition = String(row.disposition || row.lead_status || "").trim().toLowerCase();
  return FINAL_DISPOSITIONS.has(disposition);
}

function isLeadLeaseLocked(row) {
  const openedRaw = String(row.last_opened_at || "").trim();
  if (!openedRaw || isFinalDisposition(row)) return false;
  const openedAt = Date.parse(openedRaw);
  if (!Number.isFinite(openedAt)) return false;
  return Date.now() - openedAt < LEASE_WINDOW_MS;
}

function getLeadSelectionFilteredLeads() {
  const search = document.getElementById("leadSelectSearch")?.value?.trim().toLowerCase() || "";
  const queue = document.getElementById("leadSelectQueue")?.value || "all";
  const priority = document.getElementById("leadSelectPriority")?.value || "all";
  const eligibility = document.getElementById("leadSelectEligibility")?.value || "all";

  return state.leads.map((row) => normalizeLeadRow(row)).filter((row) => {
    const haystack = [
      row.lead_external_id,
      row.full_name,
      row.first_name,
      row.last_name,
      row.mobile_phone,
      row.email,
      row.routing_bucket,
      row.lead_source,
      row.recommended_next_action,
    ]
      .join(" ")
      .toLowerCase();

    if (search && !haystack.includes(search)) return false;
    if (queue !== "all" && row.routing_bucket !== queue) return false;
    if (priority !== "all" && row.priority_tier !== priority) return false;
    if (eligibility !== "all" && row.contact_eligibility !== eligibility) return false;
    return true;
  });
}

function getCampaignFilteredLeads() {
  const search = document.getElementById("campaignSearch")?.value?.trim().toLowerCase() || "";
  const queue = document.getElementById("campaignQueue")?.value || "all";
  const priority = document.getElementById("campaignPriority")?.value || "all";
  const eligibility = document.getElementById("campaignEligibility")?.value || "all";
  const channel = document.getElementById("campaignChannel")?.value || "all";
  const consent = document.getElementById("campaignConsent")?.value || "all";

  return state.leads.map((row) => normalizeLeadRow(row)).filter((row) => {
    const haystack = [
      row.lead_external_id,
      row.full_name,
      row.first_name,
      row.last_name,
      row.email,
      row.routing_bucket,
      row.lead_source,
      row.lead_source_detail,
      row.recommended_next_action,
      row.notes,
      row.sequence_name,
    ]
      .join(" ")
      .toLowerCase();

    if (!row.email) return false;
    if (search && !haystack.includes(search)) return false;
    if (queue !== "all" && row.routing_bucket !== queue) return false;
    if (priority !== "all" && row.priority_tier !== priority) return false;
    if (eligibility !== "all" && row.contact_eligibility !== eligibility) return false;
    if (channel !== "all" && row.recommended_channel !== channel) return false;

    const emailConsent = String(row.consent_channel_email || "").trim().toLowerCase();
    if (consent === "clear" && !emailConsent) return false;
    if (consent === "review_required" && String(row.consent_status || "").trim() !== "review_required") return false;
    if (consent === "blank" && emailConsent) return false;

    return true;
  });
}

function campaignLeadFlags(row) {
  const flags = [];
  if (String(row.consent_channel_email || "").trim()) flags.push("email consent on file");
  else if (String(row.consent_status || "").trim() === "review_required") flags.push("consent review required");
  else flags.push("no email consent recorded");

  if (String(row.suppress_reason || "").trim()) flags.push(`suppression: ${row.suppress_reason}`);
  if (String(row.dnc_status || "").trim() && String(row.dnc_status).trim() !== "pending_check") flags.push(`dnc: ${row.dnc_status}`);
  if (String(row.recommended_channel || "").trim() !== "email") flags.push(`channel says ${row.recommended_channel || "manual review"}`);
  return flags;
}

function buildCampaignMessage(row) {
  const senderName = document.getElementById("campaignSenderName")?.value?.trim() || "Lena";
  const angle = document.getElementById("campaignAngle")?.value?.trim() || "Help the lead restart the conversation with a short, personal check-in";
  const cta = document.getElementById("campaignCta")?.value?.trim() || "Reply with a good time this week or the best number to reach you.";
  const firstName = row.first_name || getLeadDisplayName(row).split(" ")[0] || "there";
  const product = row.product_line || row.product_interest || "coverage";
  const sourceHint = row.lead_source_detail || row.lead_source || "your request";
  const nextAction = row.recommended_next_action || row.notes || "pick up where the conversation left off";
  const subject = `${firstName}, quick follow-up on your ${product.toLowerCase()} options`;
  const body = [
    `Hi ${firstName},`,
    ``,
    `I wanted to follow up because ${angle.toLowerCase()}.`,
    `I saw your file came through from ${sourceHint}, and my note says the next best step is to ${nextAction}.`,
    ``,
    `If ${product.toLowerCase()} is still on your list, I can help you sort through the best fit without making this complicated.`,
    `${cta}`,
    ``,
    `- ${senderName}`,
  ].join("\n");

  return { subject, body };
}

function getSelectedCampaignRows() {
  const selectedIds = new Set(state.ui.campaignSelectedLeadIds || []);
  return getCampaignFilteredLeads().filter((row) => selectedIds.has(row.lead_external_id));
}

function buildCampaignExportRows() {
  return getSelectedCampaignRows().map((row) => {
    const message = buildCampaignMessage(row);
    return {
      lead_external_id: row.lead_external_id,
      full_name: getLeadDisplayName(row),
      first_name: row.first_name,
      last_name: row.last_name,
      email: row.email,
      routing_bucket: row.routing_bucket,
      priority_tier: row.priority_tier,
      consent_status: row.consent_status,
      consent_channel_email: row.consent_channel_email,
      dnc_status: row.dnc_status,
      suppress_reason: row.suppress_reason,
      recommended_channel: row.recommended_channel,
      sequence_name: row.sequence_name,
      recommended_next_action: row.recommended_next_action,
      campaign_name: row.campaign_name,
      openclaw_mode: "draft_review",
      approval_required: "yes",
      subject: message.subject,
      body: message.body,
    };
  });
}

function renderLeadSelectionTable() {
  const table = document.getElementById("leadSelectTable");
  const summary = document.getElementById("leadSelectSummary");
  const status = document.getElementById("leadSelectStatus");
  const loadBtn = document.getElementById("leadSelectLoadBtn");
  if (!table || !summary || !status) return;
  try {
    const allMatches = getLeadSelectionFilteredLeads().filter((row) => hasValue(row.lead_external_id));
    const sortKey = state.ui.leadSelectionSort.key;
    const sortDir = state.ui.leadSelectionSort.dir;

    const priorityRank = (value) => {
      const normalized = String(value || "").toLowerCase();
      if (normalized === "high") return 3;
      if (normalized === "normal") return 2;
      if (normalized === "low") return 1;
      return 0;
    };

    const getSortValue = (row, key) => {
      switch (key) {
        case "name":
          return getLeadDisplayName(row);
        case "contact":
          return row.mobile_phone || row.email || "";
        case "queue":
          return row.routing_bucket || "";
        case "priority":
          return priorityRank(row.priority_tier);
        case "last_activity":
          return Date.parse(row.last_activity_at_source || "") || 0;
        case "next_step":
          return row.recommended_next_action || row.notes || "";
        default:
          return "";
      }
    };

    if (sortKey) {
      allMatches.sort((a, b) => {
        const aVal = getSortValue(a, sortKey);
        const bVal = getSortValue(b, sortKey);
        if (typeof aVal === "number" && typeof bVal === "number") {
          return sortDir === "asc" ? aVal - bVal : bVal - aVal;
        }
        const comparison = String(aVal).localeCompare(String(bVal));
        return sortDir === "asc" ? comparison : -comparison;
      });
    } else {
      allMatches.sort((a, b) => {
        const aPriority = String(a.priority_tier || "").toLowerCase() === "high" ? 1 : 0;
        const bPriority = String(b.priority_tier || "").toLowerCase() === "high" ? 1 : 0;
        if (bPriority !== aPriority) return bPriority - aPriority;
        const aLast = Date.parse(a.last_activity_at_source || "") || 0;
        const bLast = Date.parse(b.last_activity_at_source || "") || 0;
        if (bLast !== aLast) return bLast - aLast;
        return getLeadDisplayName(a).localeCompare(getLeadDisplayName(b));
      });
    }
    const rows = allMatches.slice(0, LEAD_SELECTION_MAX_ROWS);

    if (!rows.length) {
      table.innerHTML = `<tr><td colspan="8" class="muted">No leads match the current filters.</td></tr>`;
      summary.textContent = "0 leads in filtered list";
      status.textContent = "No lead selected.";
      state.ui.selectedLeadSelectionId = "";
      if (loadBtn) loadBtn.disabled = true;
      return;
    }

    if (
      !state.ui.selectedLeadSelectionId
      || !rows.some((row) => row.lead_external_id === state.ui.selectedLeadSelectionId)
      || isLeadLeaseLocked(rows.find((row) => row.lead_external_id === state.ui.selectedLeadSelectionId) || {})
    ) {
      const firstOpenRow = rows.find((row) => !isLeadLeaseLocked(row));
      state.ui.selectedLeadSelectionId = (firstOpenRow || rows[0]).lead_external_id;
    }

    table.innerHTML = rows
      .map((row) => {
        const selected = row.lead_external_id === state.ui.selectedLeadSelectionId;
        const locked = isLeadLeaseLocked(row);
        const name = row.full_name || `${row.first_name || ""} ${row.last_name || ""}`.trim() || "Unnamed lead";
        const contact = row.mobile_phone || row.email || "No contact";
        const nextStep = row.recommended_next_action || row.notes || "-";
        const firstName = row.first_name || (name.split(" ")[0] || "");
        const lastName = row.last_name || name.split(" ").slice(1).join(" ");
        const safeId = escapeHtml(row.lead_external_id);
        const safeName = escapeHtml(`${firstName} ${lastName}`.trim() || name);
        const safeContact = escapeHtml(contact);
        const safeQueue = escapeHtml(row.routing_bucket || "-");
        const safePriority = escapeHtml(row.priority_tier || "-");
        const safeActivity = escapeHtml(row.last_activity_at_source || "-");
        const safeNext = escapeHtml(nextStep);
        const lockBadge = locked ? `<span class="lead-lock-icon" title="Locked for 15 minutes to prevent double-dial">🔒</span> ` : "";
        return `
          <tr class="interactive-row ${selected ? "selected" : ""} ${locked ? "locked-row" : ""}" data-lead-select-id="${safeId}" data-lead-locked="${locked ? "true" : "false"}" tabindex="${locked ? "-1" : "0"}">
            <td><input type="radio" name="lead-select-radio" ${selected ? "checked" : ""} ${locked ? "disabled" : ""} /></td>
            <td class="lead-select-name" title="${safeName}">${lockBadge}${safeName}</td>
            <td class="lead-select-name" title="${safeContact}">${safeContact}</td>
            <td>${safeQueue}</td>
            <td>${safePriority}</td>
            <td>${safeActivity}</td>
            <td class="lead-select-next" title="${safeNext}">${safeNext}</td>
            <td><button class="ghost-button slim" type="button" data-move-pipeline="${safeId}" ${locked ? "disabled" : ""}>Move to Pipeline</button></td>
          </tr>
        `;
      })
      .join("");

    const selectedRow = rows.find((row) => row.lead_external_id === state.ui.selectedLeadSelectionId);
    summary.textContent =
      allMatches.length > LEAD_SELECTION_MAX_ROWS
        ? `${formatNumber.format(allMatches.length)} leads match filters • showing first ${formatNumber.format(LEAD_SELECTION_MAX_ROWS)}`
        : `${formatNumber.format(allMatches.length)} leads match filters`;
    status.textContent = selectedRow
      ? `${isLeadLeaseLocked(selectedRow) ? "Locked: " : "Selected: "}${selectedRow.full_name || `${selectedRow.first_name || ""} ${selectedRow.last_name || ""}`.trim()}`
      : "No lead selected.";
    if (loadBtn) loadBtn.disabled = !selectedRow || isLeadLeaseLocked(selectedRow || {});
    updateLeadSelectionSortIndicators();
  } catch (error) {
    table.innerHTML = `<tr><td colspan="8" class="muted">Lead table failed to render. Check data format.</td></tr>`;
    summary.textContent = "Render error";
    status.textContent = String(error.message || error);
    if (loadBtn) loadBtn.disabled = true;
  }
}

function renderCampaignPreview() {
  const previewLead = document.getElementById("campaignPreviewLead");
  const previewFlags = document.getElementById("campaignPreviewFlags");
  const previewSubject = document.getElementById("campaignPreviewSubject");
  const previewBody = document.getElementById("campaignPreviewBody");
  if (!previewLead || !previewFlags || !previewSubject || !previewBody) return;

  const rows = getCampaignFilteredLeads();
  const selected = rows.find((row) => row.lead_external_id === state.ui.selectedCampaignLeadId);
  if (!selected) {
    previewLead.textContent = "No lead selected";
    previewFlags.textContent = "Select a row to preview the generated message.";
    previewFlags.className = "table-summary";
    previewSubject.value = "";
    previewBody.value = "";
    return;
  }

  const message = buildCampaignMessage(selected);
  const flags = campaignLeadFlags(selected);
  previewLead.textContent = `${getLeadDisplayName(selected)} <${selected.email}>`;
  previewFlags.textContent = flags.join(" • ");
  previewFlags.className = `table-summary ${flags.some((flag) => flag.includes("required") || flag.includes("no email consent") || flag.includes("suppression") || flag.includes("dnc")) ? "campaign-warning" : "campaign-good"}`;
  previewSubject.value = message.subject;
  previewBody.value = message.body;
}

function renderCampaignBatchStats() {
  const container = document.getElementById("campaignBatchStats");
  if (!container) return;
  const selected = buildCampaignExportRows();
  const highPriority = selected.filter((row) => String(row.priority_tier).toLowerCase() === "high").length;
  const reviewRequired = selected.filter((row) => !String(row.consent_channel_email || "").trim()).length;
  const queues = [...new Set(selected.map((row) => row.routing_bucket).filter(Boolean))];

  container.innerHTML = `
    <div class="stat-item">
      <span class="muted">Selected leads</span>
      <strong>${formatNumber.format(selected.length)}</strong>
    </div>
    <div class="stat-item">
      <span class="muted">High-priority leads</span>
      <strong>${formatNumber.format(highPriority)}</strong>
    </div>
    <div class="stat-item">
      <span class="muted">Needs consent review</span>
      <strong>${formatNumber.format(reviewRequired)}</strong>
    </div>
    <div class="stat-item">
      <span class="muted">Queues represented</span>
      <strong>${queues.length ? escapeHtml(queues.join(", ")) : "None"}</strong>
    </div>
  `;
}

function renderCampaignTable() {
  const table = document.getElementById("campaignTable");
  const summary = document.getElementById("campaignSummary");
  const status = document.getElementById("campaignStatus");
  if (!table || !summary || !status) return;

  const allMatches = getCampaignFilteredLeads()
    .filter((row) => hasValue(row.lead_external_id))
    .sort((a, b) => {
      const aPriority = String(a.priority_tier || "").toLowerCase() === "high" ? 1 : 0;
      const bPriority = String(b.priority_tier || "").toLowerCase() === "high" ? 1 : 0;
      if (bPriority !== aPriority) return bPriority - aPriority;
      return getLeadDisplayName(a).localeCompare(getLeadDisplayName(b));
    });

  const validIds = new Set(allMatches.map((row) => row.lead_external_id));
  state.ui.campaignSelectedLeadIds = (state.ui.campaignSelectedLeadIds || []).filter((id) => validIds.has(id));
  if (!state.ui.selectedCampaignLeadId || !validIds.has(state.ui.selectedCampaignLeadId)) {
    state.ui.selectedCampaignLeadId = allMatches[0]?.lead_external_id || "";
  }

  if (!allMatches.length) {
    table.innerHTML = `<tr><td colspan="8" class="muted">No email-capable leads match the current filters.</td></tr>`;
    summary.textContent = "0 email leads in filtered list";
    status.textContent = "No leads selected.";
    renderCampaignPreview();
    renderCampaignBatchStats();
    return;
  }

  const selectedIds = new Set(state.ui.campaignSelectedLeadIds || []);
  table.innerHTML = allMatches
    .slice(0, LEAD_SELECTION_MAX_ROWS)
    .map((row) => {
      const selected = row.lead_external_id === state.ui.selectedCampaignLeadId;
      const checked = selectedIds.has(row.lead_external_id);
      const flags = campaignLeadFlags(row);
      return `
        <tr class="interactive-row ${selected ? "selected" : ""}" data-campaign-lead-id="${escapeHtml(row.lead_external_id)}" tabindex="0">
          <td><input type="checkbox" data-campaign-checkbox ${checked ? "checked" : ""} /></td>
          <td>${escapeHtml(getLeadDisplayName(row))}</td>
          <td class="lead-select-name" title="${escapeHtml(row.email)}">${escapeHtml(row.email)}</td>
          <td>${escapeHtml(row.routing_bucket || "-")}</td>
          <td>${escapeHtml(row.priority_tier || "-")}</td>
          <td title="${escapeHtml(flags.join(" • "))}">${escapeHtml(row.consent_channel_email || row.consent_status || "missing")}</td>
          <td>${escapeHtml(row.recommended_channel || "-")}</td>
          <td class="lead-select-next" title="${escapeHtml(row.recommended_next_action || row.notes || "-")}">${escapeHtml(row.recommended_next_action || row.notes || "-")}</td>
        </tr>
      `;
    })
    .join("");

  summary.textContent =
    allMatches.length > LEAD_SELECTION_MAX_ROWS
      ? `${formatNumber.format(allMatches.length)} email-ready leads match filters • showing first ${formatNumber.format(LEAD_SELECTION_MAX_ROWS)}`
      : `${formatNumber.format(allMatches.length)} email-ready leads match filters`;
  status.textContent = `${formatNumber.format(selectedIds.size)} leads selected for export`;
  renderCampaignPreview();
  renderCampaignBatchStats();
}

function populateSourcedFilters(leads) {
  const platformFilter = document.getElementById("sourcedPlatformFilter");
  const platforms = ["all", ...Object.keys(countBy(leads, "Source Platform"))];
  platformFilter.innerHTML = platforms
    .map((value) => `<option value="${value}">${value === "all" ? "All platforms" : value}</option>`)
    .join("");
}

function getFilteredLeads() {
  const search = document.getElementById("searchInput")?.value?.trim().toLowerCase() || "";
  const queue = document.getElementById("queueFilter")?.value || "all";
  const channel = document.getElementById("channelFilter")?.value || "all";
  const priority = document.getElementById("priorityFilter")?.value || "all";

  return state.leads.filter((row) => {
    const haystack = [
      row.full_name,
      row.first_name,
      row.last_name,
      row.email,
      row.mobile_phone,
      row.raw_tags,
      row.notes,
      row.recommended_next_action,
    ]
      .join(" ")
      .toLowerCase();

    if (search && !haystack.includes(search)) return false;
    if (queue !== "all" && row.routing_bucket !== queue) return false;
    if (channel !== "all" && row.recommended_channel !== channel) return false;
    if (priority !== "all" && row.priority_tier !== priority) return false;
    return true;
  });
}

function getFilteredSourcedLeads() {
  const search = document.getElementById("sourcedSearchInput").value.trim().toLowerCase();
  const queue = document.getElementById("sourcedQueueFilter").value;
  const platform = document.getElementById("sourcedPlatformFilter").value;
  const stage = document.getElementById("sourcedStageFilter").value;

  return state.sourcedLeads.filter((row) => {
    const decision = leadPassesCriteria(row, state.criteria);
    if (!decision.accepted) return false;
    const rowQueue = sourcedQueueLabel(row);
    const rowStage = getSourcedLeadState(row["Contact Id"]).stage;
    const haystack = [
      row["First Name"],
      row["Last Name"],
      row["Source Evidence"],
      row["Lead Circumstances"],
      row.Notes,
      row["Trigger Event"],
      row["Enrichment Notes"],
    ]
      .join(" ")
      .toLowerCase();

    if (search && !haystack.includes(search)) return false;
    if (queue !== "all" && rowQueue !== queue) return false;
    if (platform !== "all" && row["Source Platform"] !== platform) return false;
    if (stage !== "all" && rowStage !== stage) return false;
    return true;
  });
}

function renderLeadTable() {
  const table = document.getElementById("leadTable");
  const summary = document.getElementById("tableSummary");
  if (!table || !summary) return;
  const leads = getFilteredLeads().slice(0, 50);

  table.innerHTML = leads
    .map((row) => {
      const name = row.full_name || `${row.first_name} ${row.last_name}`.trim();
      const statusTone =
        row.priority_tier === "high" || row.contact_eligibility === "blocked"
          ? "danger"
          : row.contact_eligibility === "review_required"
            ? "warn"
            : "good";
      return `
        <tr>
          <td>
            <div class="lead-cell">
              <strong>${name || "Unnamed lead"}</strong>
              <span class="muted">${row.email || row.mobile_phone || "No direct contact"}</span>
            </div>
          </td>
          <td><span class="chip">${row.routing_bucket}</span></td>
          <td>${row.recommended_channel || "manual_review"}</td>
          <td><span class="chip ${statusTone}">${row.contact_eligibility}</span></td>
          <td>${row.recommended_next_action}</td>
          <td>${row.notes || row.raw_tags || ""}</td>
        </tr>
      `;
    })
    .join("");

  summary.textContent = `${formatNumber.format(getFilteredLeads().length)} leads match your current filters`;
}

function renderSourcedLeadTable() {
  const table = document.getElementById("sourcedLeadTable");
  const summary = document.getElementById("sourcedTableSummary");
  const rows = getFilteredSourcedLeads().slice(0, 60);

  if (!rows.length) {
    table.innerHTML = `
      <tr>
        <td colspan="6" class="muted">No sourced leads match the current filters.</td>
      </tr>
    `;
    summary.textContent = "0 sourced leads loaded";
    renderSourcedDetail();
    return;
  }

  table.innerHTML = rows
    .map((row) => {
      const id = row["Contact Id"] || "";
      const name = `${row["First Name"] || ""} ${row["Last Name"] || ""}`.trim() || "Unnamed lead";
      const leadState = getSourcedLeadState(id);
      const selected = id === state.ui.selectedSourcedLeadId ? "selected" : "";
      return `
        <tr class="interactive-row ${selected}" data-source-id="${id}">
          <td>
            <div class="lead-cell">
              <strong>${name}</strong>
              <span class="muted">${row["Source Platform"] || "unknown"} • ${row.Created || ""}</span>
            </div>
          </td>
          <td>
            <div class="pipeline-cell">
              <span class="chip ${isEnrichmentNeeded(row) ? "warn" : "good"}">${sourcedQueueLabel(row)}</span>
              <span class="chip stage">${leadState.stage}</span>
            </div>
          </td>
          <td>${row["Trigger Event"] || ""}</td>
          <td>${row["First Touch Strategy"] || ""}</td>
          <td>${row["Contact Path"] || row["Post or Message URL"] || "-"}</td>
          <td>${row["Lead Circumstances"] || row.Notes || ""}</td>
        </tr>
      `;
    })
    .join("");

  const sourcedSummary = buildSourcedSummary(state.sourcedLeads);
  summary.textContent = `${formatNumber.format(rows.length)} in working view • ${formatNumber.format(
    sourcedSummary.enrichmentNeeded,
  )} need enrichment • ${formatNumber.format(sourcedSummary.readyForReview)} ready for review`;

  if (!state.ui.selectedSourcedLeadId || !rows.some((row) => row["Contact Id"] === state.ui.selectedSourcedLeadId)) {
    state.ui.selectedSourcedLeadId = rows[0]["Contact Id"];
  }
  renderSourcedDetail();
}

function renderSourcedDetail() {
  const selected = state.sourcedLeads.find((row) => row["Contact Id"] === state.ui.selectedSourcedLeadId);
  const detailName = document.getElementById("detailLeadName");
  const detailMeta = document.getElementById("detailLeadMeta");
  const detailCircumstances = document.getElementById("detailCircumstances");
  const detailEvidence = document.getElementById("detailEvidence");
  const detailTrigger = document.getElementById("detailTrigger");
  const detailTouch = document.getElementById("detailTouch");
  const detailPath = document.getElementById("detailPath");
  const detailConfidence = document.getElementById("detailConfidence");
  const detailStageSelect = document.getElementById("detailStageSelect");
  const detailOwnerNote = document.getElementById("detailOwnerNote");
  const openLeadUrlBtn = document.getElementById("openLeadUrlBtn");

  if (!selected) {
    detailName.textContent = "Select a sourced lead";
    detailMeta.textContent = "Pick a row to review its trigger, evidence, and next step.";
    detailCircumstances.textContent = "No lead selected.";
    detailEvidence.textContent = "No lead selected.";
    detailTrigger.textContent = "-";
    detailTouch.textContent = "-";
    detailPath.textContent = "-";
    detailConfidence.textContent = "-";
    detailStageSelect.value = "new";
    detailOwnerNote.value = "";
    openLeadUrlBtn.href = "#";
    openLeadUrlBtn.classList.add("disabled");
    return;
  }

  const leadState = getSourcedLeadState(selected["Contact Id"]);
  detailName.textContent = `${selected["First Name"] || ""} ${selected["Last Name"] || ""}`.trim() || "Unnamed lead";
  detailMeta.textContent = `${selected["Source Platform"] || "unknown"} • ${selected.Created || ""} • ${sourcedQueueLabel(selected)}`;
  detailCircumstances.textContent = selected["Lead Circumstances"] || selected.Notes || "No circumstances captured.";
  detailEvidence.textContent = selected["Source Evidence"] || "No evidence captured.";
  detailTrigger.textContent = selected["Trigger Event"] || "-";
  detailTouch.textContent = selected["First Touch Strategy"] || "-";
  detailPath.textContent = selected["Contact Path"] || selected["Post or Message URL"] || "-";
  detailConfidence.textContent = [
    selected["Source Confidence"] || "source n/a",
    selected["Phone Confidence"] || "phone n/a",
    selected["Email Confidence"] || "email n/a",
  ].join(" • ");
  detailStageSelect.value = leadState.stage || "new";
  detailOwnerNote.value = leadState.ownerNote || "";

  if (selected["Post or Message URL"]) {
    openLeadUrlBtn.href = selected["Post or Message URL"];
    openLeadUrlBtn.classList.remove("disabled");
  } else {
    openLeadUrlBtn.href = "#";
    openLeadUrlBtn.classList.add("disabled");
  }
}

function setLeadPreset(preset) {
  state.ui.leadPreset = preset;
  const queueFilter = document.getElementById("queueFilter");
  const channelFilter = document.getElementById("channelFilter");
  const priorityFilter = document.getElementById("priorityFilter");
  const searchInput = document.getElementById("searchInput");
  const leadSelectQueue = document.getElementById("leadSelectQueue");
  const leadSelectPriority = document.getElementById("leadSelectPriority");
  const leadSelectSearch = document.getElementById("leadSelectSearch");

  const applyIfPresent = (el, value) => {
    if (el) el.value = value;
  };

  if (preset === "call_queue") {
    applyIfPresent(queueFilter, "health_nurture_queue");
    applyIfPresent(channelFilter, "phone_call");
    applyIfPresent(priorityFilter, "all");
    applyIfPresent(searchInput, "");
    applyIfPresent(leadSelectQueue, "health_nurture_queue");
    applyIfPresent(leadSelectPriority, "all");
    applyIfPresent(leadSelectSearch, "");
  } else if (preset === "needs_review") {
    applyIfPresent(queueFilter, "all");
    applyIfPresent(channelFilter, "all");
    applyIfPresent(priorityFilter, "high");
    applyIfPresent(searchInput, "");
    applyIfPresent(leadSelectQueue, "all");
    applyIfPresent(leadSelectPriority, "high");
    applyIfPresent(leadSelectSearch, "");
  } else if (preset === "email_salvage") {
    applyIfPresent(queueFilter, "all");
    applyIfPresent(channelFilter, "email");
    applyIfPresent(priorityFilter, "all");
    applyIfPresent(searchInput, "");
    applyIfPresent(leadSelectQueue, "all");
    applyIfPresent(leadSelectPriority, "all");
    applyIfPresent(leadSelectSearch, "");
  } else {
    applyIfPresent(queueFilter, "all");
    applyIfPresent(channelFilter, "all");
    applyIfPresent(priorityFilter, "all");
    applyIfPresent(leadSelectQueue, "all");
    applyIfPresent(leadSelectPriority, "all");
  }

  const leadPresetLabel = document.getElementById("leadPresetLabel");
  if (leadPresetLabel) leadPresetLabel.textContent = `Preset: ${preset.replaceAll("_", " ")}`;
  renderLeadTable();
  renderLeadSelectionTable();
}

function setSourcedPreset(preset) {
  state.ui.sourcedPreset = preset;
  const queueFilter = document.getElementById("sourcedQueueFilter");
  const stageFilter = document.getElementById("sourcedStageFilter");
  const searchInput = document.getElementById("sourcedSearchInput");

  if (preset === "sourced_ready") {
    queueFilter.value = "Ready For Review";
    stageFilter.value = "all";
    searchInput.value = "";
  } else if (preset === "sourced_enrichment") {
    queueFilter.value = "Enrichment Needed";
    stageFilter.value = "all";
    searchInput.value = "";
  } else {
    queueFilter.value = "all";
    stageFilter.value = "all";
  }

  document.getElementById("sourcedPresetLabel").textContent = `Preset: ${preset.replaceAll("_", " ")}`;
  renderSourcedLeadTable();
}

function copyText(text) {
  navigator.clipboard.writeText(text).catch(() => {});
}

function normalizeCarrierName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\(.*?\)/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadCarrierConfigsFromStorage() {
  try {
    const parsed = JSON.parse(localStorage.getItem(CARRIER_CONFIGS_STORAGE_KEY) || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveCarrierConfigsToStorage(configs) {
  localStorage.setItem(CARRIER_CONFIGS_STORAGE_KEY, JSON.stringify(Array.isArray(configs) ? configs : []));
}

function getKnownCarrierNames() {
  const fromGrid = Array.isArray(state.carrierGrid)
    ? state.carrierGrid.map((row) => String(row?.name || "").trim()).filter(Boolean)
    : [];
  const fromLeads = state.leads.map((row) => String(row?.carrier_match || "").trim()).filter(Boolean);
  const fromSaved = (state.ui.carrierConfigs || []).map((row) => String(row?.carrier_name || "").trim()).filter(Boolean);
  return Array.from(new Set([...fromGrid, ...fromLeads, ...fromSaved])).sort((a, b) => a.localeCompare(b));
}

function findCarrierConfigByName(carrierName) {
  const target = normalizeCarrierName(carrierName);
  if (!target) return null;
  const configs = Array.isArray(state.ui.carrierConfigs) ? state.ui.carrierConfigs : [];
  let match = configs.find((cfg) => normalizeCarrierName(cfg.carrier_name) === target);
  if (match) return match;
  match = configs.find((cfg) => target.includes(normalizeCarrierName(cfg.carrier_name)));
  if (match) return match;
  match = configs.find((cfg) => normalizeCarrierName(cfg.carrier_name).includes(target));
  return match || null;
}

function renderCarrierActionCard() {
  const card = document.getElementById("deskCarrierActionCard");
  const nameEl = document.getElementById("deskCarrierActionName");
  const writingInput = document.getElementById("deskCarrierWritingNumberInput");
  const supportEl = document.getElementById("deskCarrierSupportPhone");
  const launchBtn = document.getElementById("deskLaunchCarrierPortalBtn");
  const copyBtn = document.getElementById("deskCopyWritingBtn");
  if (!card || !nameEl || !writingInput || !supportEl || !launchBtn || !copyBtn) return;

  const carrierName = String(state.ui.primaryCarrier || "").trim();
  if (!carrierName || carrierName.toLowerCase().includes("review")) {
    card.hidden = true;
    return;
  }

  const config = findCarrierConfigByName(carrierName);
  const writingNumber = String(config?.writing_number || "").trim();
  const portalUrl = String(config?.portal_url || "").trim();
  const supportPhone = String(config?.support_phone || "").trim();
  const normalizedCarrier = normalizeCarrierName(carrierName);
  const genericPortalMap = {
    "mutual of omaha": "https://auth.mutualofomaha.com",
    aetna: "https://www.aetna.com",
    "aig": "https://www.aig.com",
    ethos: "https://agents.ethoslife.com",
    foresters: "https://www.foresters.com",
  };
  const genericPortal = Object.entries(genericPortalMap).find(([key]) =>
    normalizedCarrier.includes(key),
  )?.[1] || `https://www.google.com/search?q=${encodeURIComponent(carrierName + " agent portal")}`;
  const launchUrl = portalUrl || genericPortal;

  nameEl.textContent = carrierName;
  writingInput.dataset.carrierName = carrierName;
  writingInput.value = writingNumber;
  writingInput.placeholder = "Agent Writing Number";
  supportEl.textContent = supportPhone || "Not configured";
  copyBtn.disabled = !writingNumber;
  launchBtn.disabled = !launchUrl;
  launchBtn.dataset.portalUrl = launchUrl;
  card.hidden = false;
}

async function persistActionCardWritingNumber() {
  const writingInput = document.getElementById("deskCarrierWritingNumberInput");
  if (!writingInput) return;
  const carrierName = String(writingInput.dataset.carrierName || state.ui.primaryCarrier || "").trim();
  if (!carrierName) return;
  const writingNumber = String(writingInput.value || "").trim();

  const existing = Array.isArray(state.ui.carrierConfigs) ? state.ui.carrierConfigs : [];
  const idx = existing.findIndex((cfg) => normalizeCarrierName(cfg.carrier_name) === normalizeCarrierName(carrierName));
  const previous = idx >= 0 ? existing[idx] : {};
  const updated = {
    carrier_name: carrierName,
    writing_number: writingNumber,
    portal_url: String(previous?.portal_url || "").trim(),
    support_phone: String(previous?.support_phone || "").trim(),
  };

  const next = idx >= 0 ? existing.map((cfg, i) => (i === idx ? updated : cfg)) : [...existing, updated];
  state.ui.carrierConfigs = next;
  saveCarrierConfigsToStorage(next);

  try {
    if (supabase) {
      await saveCarrierConfigsToSupabase(next);
    } else if (LOCAL_DB_CARRIER_CONFIG_URL.trim()) {
      await fetch(LOCAL_DB_CARRIER_CONFIG_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rows: next }),
      });
    }
  } catch {
    // Keep local persistence even if API is offline.
  }
}

function renderCarrierSettingsRows() {
  const listEl = document.getElementById("carrierSettingsList");
  if (!listEl) return;
  const names = getKnownCarrierNames();
  if (!names.length) {
    listEl.innerHTML = `<p class="muted">No carriers detected yet. Once carrier matching runs, they will appear here.</p>`;
    return;
  }
  listEl.innerHTML = names
    .map((name, index) => {
      const config = findCarrierConfigByName(name) || {};
      return `
        <article class="carrier-setting-row" data-carrier-row="${index}">
          <h4>${escapeHtml(name)}</h4>
          <input type="hidden" data-carrier-name value="${escapeHtml(name)}" />
          <div class="carrier-setting-grid">
            <label>
              <span>Writing Number</span>
              <input type="text" data-carrier-writing value="${escapeHtml(String(config.writing_number || ""))}" />
            </label>
            <label>
              <span>Portal URL</span>
              <input type="url" data-carrier-portal value="${escapeHtml(String(config.portal_url || ""))}" />
            </label>
            <label>
              <span>Support Phone</span>
              <input type="text" data-carrier-support value="${escapeHtml(String(config.support_phone || ""))}" />
            </label>
          </div>
        </article>
      `;
    })
    .join("");
}

async function loadCarrierConfigs() {
  let configs = [];
  if (supabase) {
    try {
      configs = await loadCarrierConfigsFromSupabase();
    } catch {
      // Fall back to local storage.
    }
  } else if (LOCAL_DB_CARRIER_CONFIG_URL.trim()) {
    try {
      const response = await fetch(LOCAL_DB_CARRIER_CONFIG_URL, { method: "GET" });
      if (response.ok) {
        const data = await response.json();
        if (data?.ok && Array.isArray(data.rows)) configs = data.rows;
      }
    } catch {
      // Fall back to local storage.
    }
  }
  if (!configs.length) {
    configs = loadCarrierConfigsFromStorage();
  }
  state.ui.carrierConfigs = Array.isArray(configs) ? configs : [];
  renderCarrierSettingsRows();
  renderCarrierActionCard();
}

async function saveCarrierConfigs() {
  const statusEl = document.getElementById("carrierSettingsStatus");
  const rowEls = Array.from(document.querySelectorAll("[data-carrier-row]"));
  const rows = rowEls.map((rowEl) => ({
    carrier_name: String(rowEl.querySelector("[data-carrier-name]")?.value || "").trim(),
    writing_number: String(rowEl.querySelector("[data-carrier-writing]")?.value || "").trim(),
    portal_url: String(rowEl.querySelector("[data-carrier-portal]")?.value || "").trim(),
    support_phone: String(rowEl.querySelector("[data-carrier-support]")?.value || "").trim(),
  })).filter((row) => row.carrier_name);

  state.ui.carrierConfigs = rows;
  saveCarrierConfigsToStorage(rows);

  if (statusEl) statusEl.textContent = "Saving...";
  try {
    if (supabase) {
      await saveCarrierConfigsToSupabase(rows);
    } else if (LOCAL_DB_CARRIER_CONFIG_URL.trim()) {
      await fetch(LOCAL_DB_CARRIER_CONFIG_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rows }),
      });
    }
    if (statusEl) statusEl.textContent = "Saved";
  } catch (error) {
    console.error(error);
    if (statusEl) statusEl.textContent = supabase ? "Saved locally (Supabase unavailable)" : "Saved locally (API unavailable)";
  }
  renderCarrierActionCard();
}

function attachCarrierSettingsHandlers() {
  const modal = document.getElementById("carrierSettingsModal");
  const openBtn = document.getElementById("carrierSettingsBtn");
  const deskOpenBtn = document.getElementById("deskOpenCarrierSettingsBtn");
  const closeBtn = document.getElementById("carrierSettingsCloseBtn");
  const closeBtnFloating = document.getElementById("carrierSettingsCloseBtnFloating");
  const saveBtn = document.getElementById("carrierSettingsSaveBtn");
  const saveBtnTop = document.getElementById("carrierSettingsSaveBtnTop");
  const saveBtnFloating = document.getElementById("carrierSettingsSaveBtnFloating");
  if (!modal || !closeBtn || !saveBtn) return;

  const openModal = () => {
    renderCarrierSettingsRows();
    modal.hidden = false;
    window.requestAnimationFrame(() => {
      modal.scrollTop = 0;
      const card = modal.querySelector(".carrier-settings-modal");
      if (card) card.scrollTop = 0;
    });
  };
  openBtn?.addEventListener("click", openModal);
  deskOpenBtn?.addEventListener("click", openModal);
  closeBtn.addEventListener("click", () => {
    modal.hidden = true;
  });
  closeBtnFloating?.addEventListener("click", () => {
    modal.hidden = true;
  });
  modal.addEventListener("click", (event) => {
    if (event.target === modal) modal.hidden = true;
  });
  saveBtn.addEventListener("click", async () => {
    await saveCarrierConfigs();
  });
  saveBtnTop?.addEventListener("click", async () => {
    await saveCarrierConfigs();
  });
  saveBtnFloating?.addEventListener("click", async () => {
    await saveCarrierConfigs();
  });

  document.getElementById("deskCopyWritingBtn")?.addEventListener("click", () => {
    const writingNumber = String(document.getElementById("deskCarrierWritingNumberInput")?.value || "").trim();
    if (!writingNumber) return;
    copyText(writingNumber);
  });

  const actionCardWritingInput = document.getElementById("deskCarrierWritingNumberInput");
  actionCardWritingInput?.addEventListener("input", () => {
    const value = String(actionCardWritingInput.value || "").trim();
    const copyBtn = document.getElementById("deskCopyWritingBtn");
    if (copyBtn) copyBtn.disabled = !value;
  });
  actionCardWritingInput?.addEventListener("change", async () => {
    await persistActionCardWritingNumber();
    renderCarrierActionCard();
  });
  actionCardWritingInput?.addEventListener("blur", async () => {
    await persistActionCardWritingNumber();
    renderCarrierActionCard();
  });

  document.getElementById("deskLaunchCarrierPortalBtn")?.addEventListener("click", async () => {
    await handleLaunchPortal();
  });
}

function renderChipList(containerId, items, tone) {
  const container = document.getElementById(containerId);
  if (!container) return;
  if (!items.length) {
    container.innerHTML = `<span class="muted">None yet</span>`;
    return;
  }
  container.innerHTML = items
    .map((item) => `<span class="option-chip ${tone}">${item}</span>`)
    .join("");
}

function getCallDeskPath() {
  const productPath = String(state.ui.workflowAnswers.productPath || document.getElementById("deskProductPath")?.value || "");
  if (["health", "life", "both", "unclear"].includes(productPath)) return productPath;
  const needArea = String(document.getElementById("deskNeedArea")?.value || "");
  return ["health", "life", "both", "unclear"].includes(needArea) ? needArea : "";
}

function ageBandRange(ageBand) {
  if (ageBand === "under50" || ageBand === "Under 50") return [18, 49];
  if (ageBand === "50to64" || ageBand === "50-64") return [50, 64];
  if (ageBand === "65to75" || ageBand === "65-75") return [65, 75];
  if (ageBand === "76plus" || ageBand === "76+") return [76, 120];
  return [18, 120];
}

function ageBandLabel(ageBand) {
  const labels = {
    under50: "Under 50",
    "Under 50": "Under 50",
    "50to64": "50-64",
    "50-64": "50-64",
    "65to75": "65-75",
    "65-75": "65-75",
    "76plus": "76+",
    "76+": "76+",
  };
  return labels[String(ageBand || "")] || String(ageBand || "Unknown");
}

function overlapsAgeBand(ageBand, minAge, maxAge) {
  const [low, high] = ageBandRange(ageBand);
  return high >= Number(minAge || 0) && low <= Number(maxAge || 120);
}

function healthRiskRank(value) {
  const map = { healthy: 1, managed: 2, challenging: 3 };
  return map[String(value || "").toLowerCase()] || 0;
}

function normalizeHealthRisk(value) {
  return String(value || "").trim().toLowerCase();
}

function getCarrierProductTypes(path, answers) {
  if (!["life", "both", "unclear"].includes(path)) return [];
  const goal = String(answers.goal || "");
  const duration = String(answers.duration || "");
  const types = new Set();

  if (goal === "income" || duration === "temporary") types.add("Term");
  if (goal === "burial" || goal === "permanent" || duration === "lifelong") types.add("Final Expense");
  if (!types.size || path === "both" || path === "unclear") {
    types.add("Term");
    types.add("Final Expense");
  }
  return Array.from(types);
}

function getCarrierMatches(path, answers) {
  const grid = Array.isArray(state.carrierGrid) ? state.carrierGrid : [];
  const productTypes = getCarrierProductTypes(path, answers);
  if (!grid.length || !productTypes.length) {
    return { matches: [], productTypes, reasons: [], snippets: [] };
  }

  const callerAgeBand = String(answers.age || "");
  const callerTobacco = String(answers.tobacco || "");
  const callerHealthRank = healthRiskRank(answers.health);

  const matches = grid.filter((carrier) => {
    if (!carrier || !productTypes.includes(String(carrier.productType || ""))) return false;
    const eligibility = carrier.eligibility || {};
    const minAge = eligibility.minAge ?? carrier.minAge ?? 0;
    const maxAge = eligibility.maxAge ?? carrier.maxAge ?? 120;
    const acceptsTobacco = eligibility.acceptsTobacco ?? carrier.acceptsTobacco ?? true;
    const thresholdList = Array.isArray(carrier.healthThresholds)
      ? carrier.healthThresholds.map((entry) => normalizeHealthRisk(entry))
      : [];
    const maxHealthRisk = normalizeHealthRisk(eligibility.maxHealthRisk || carrier.maxHealthRisk);

    if (callerAgeBand && !overlapsAgeBand(callerAgeBand, minAge, maxAge)) return false;
    if (callerTobacco === "yes" && acceptsTobacco === false) return false;
    if (thresholdList.length && answers.health && !thresholdList.includes(normalizeHealthRisk(answers.health))) {
      return false;
    }
    const maxHealthRank = healthRiskRank(maxHealthRisk);
    if (callerHealthRank && maxHealthRank && callerHealthRank > maxHealthRank) return false;
    return true;
  });

  const reasons = [];
  if (callerAgeBand) reasons.push(`Age band ${callerAgeBand} applied to carrier age windows.`);
  if (callerTobacco) reasons.push(`Tobacco answer (${callerTobacco}) applied to tobacco eligibility.`);
  if (answers.health) reasons.push(`Health posture (${answers.health}) applied to underwriting tolerance.`);

  const snippets = matches.map((carrier) => String(carrier.scriptSnippet || "").trim()).filter(Boolean).slice(0, 2);
  return { matches, productTypes, reasons, snippets };
}

function getRecommendations() {
  const path = getCallDeskPath();
  const answers = state.ui.workflowAnswers;
  const matches = getCarrierMatches(path, answers).matches;
  return matches;
}

function isAgeOver65(ageBand) {
  return ["65to75", "76plus", "65-75", "76+"].includes(String(ageBand || ""));
}

function applyAgeToProductPath(newAge) {
  const ageValue = String(newAge || "");
  const productField = document.getElementById("deskProductPath");
  const needAreaField = document.getElementById("deskNeedArea");
  const goalField = document.getElementById("deskGoal");

  if (ageValue === "65to75" || ageValue === "76plus" || ageValue === "65-75" || ageValue === "76+") {
    state.ui.workflowAnswers.productPath = "health";
    if (productField) productField.value = "health";
    if (needAreaField) needAreaField.value = "health";
  } else if (ageValue === "under50" || ageValue === "Under 50") {
    state.ui.workflowAnswers.productPath = "life";
    state.ui.workflowAnswers.goal = "income";
    if (productField) productField.value = "life";
    if (needAreaField) needAreaField.value = "life";
    if (goalField) goalField.value = "income";
  }
}

function getLiveCarrierRecommendation(path, answers) {
  const normalizedPath = String(path || "");
  const tobaccoYes = String(answers?.tobacco || "") === "yes";
  const health = String(answers?.health || "");
  const ageBand = String(answers?.age || "");
  const ageOver65 = isAgeOver65(ageBand);
  const ageUnder50 = ["under50", "Under 50"].includes(ageBand);

  let lane = "";
  let primary = "";
  let confidence = null;

  if ((ageBand === "65to75" || ageBand === "65-75") && health === "healthy") {
    lane = "Final Expense - Level";
    primary = "Mutual of Omaha (Living Promise)";
    confidence = 95;
  } else if (ageOver65 && health === "challenging") {
    lane = "Final Expense - GI";
    primary = "AIG (Guaranteed Issue)";
    confidence = 98;
  } else if (ageUnder50 && health === "healthy") {
    lane = "Term - Standard";
    primary = "Preferred Term";
    confidence = 85;
  }

  let matches = getRecommendations().map((carrier) => String(carrier.name || "")).filter(Boolean);
  if (tobaccoYes) {
    matches = matches.filter((name) => !name.toLowerCase().includes("preferred"));
    if (primary.toLowerCase().includes("preferred")) primary = "";
  }
  if (primary) matches = [primary, ...matches.filter((name) => name !== primary)];

  let warning = tobaccoYes
    ? "Tobacco rates applied. Filtering for nicotine-friendly carriers."
    : "";

  if (!lane && normalizedPath === "health") lane = "Health - Qualification Review";
  if (!lane && normalizedPath === "life") lane = "Life - Qualification Review";
  if (!lane && (normalizedPath === "both" || normalizedPath === "unclear" || !normalizedPath)) {
    lane = "Needs more qualification";
  }

  if (state.ui.primaryCarrier) {
    if (state.ui.primaryLane) lane = state.ui.primaryLane;
    primary = state.ui.primaryCarrier;
    if (state.ui.primaryConfidence !== null && state.ui.primaryConfidence !== undefined) confidence = state.ui.primaryConfidence;
    if (primary && primary !== "Life Lane Review" && !matches.includes(primary)) matches = [primary, ...matches];
    if (state.ui.primaryWhyLane) warning = state.ui.primaryWhyLane;
  }

  return {
    lane,
    primary,
    confidence,
    matches,
    warning,
  };
}

function evaluateWorkflowOptions() {
  const answers = state.ui.workflowAnswers;
  const productPath = getCallDeskPath();
  const needArea = document.getElementById("deskNeedArea")?.value || "";
  const healthGap = document.getElementById("deskHealthGap")?.value || "";
  const lifeNeed = document.getElementById("deskLifeNeed")?.value || "";
  const protectionLoad = document.getElementById("deskProtectionLoad")?.value || "";
  const medicareFlag = document.getElementById("deskMedicareFlag")?.value || "";
  const lifeFeasibility = document.getElementById("deskLifeFeasibility")?.value || "";

  if (!productPath || productPath === "unclear" || needArea === "unclear") {
    return {
      lane: "Needs more qualification",
      primary: ["Clarify the immediate problem", "Confirm health vs. life urgency"],
      available: ["Review trigger event", "Confirm current coverage", "Identify who is at risk"],
      eliminated: [],
      reasons: [
        "Do not jump into quotes until you know whether the main issue is medical coverage, financial protection, or both.",
        "Use the feasibility questions to decide if this is a health-first call, life-first call, or a combined review.",
      ],
    };
  }

  if (
    productPath === "both" ||
    needArea === "both" ||
    (healthGap === "yes" && (lifeNeed === "yes" || protectionLoad === "yes"))
  ) {
    const healthUrgent = medicareFlag === "yes" || medicareFlag === "soon" || healthGap === "yes";
    const lifeUrgent = lifeNeed === "yes" || protectionLoad === "yes";
    const lifeHard = lifeFeasibility === "challenging";

    let lane = "Combined health + life workflow";
    let primary = ["Health first", "Life second"];
    const reasons = [
      "This lead has both a coverage problem and a protection conversation, so the rep should decide what gets solved first instead of trying to quote everything at once.",
    ];

    if (!healthUrgent && lifeUrgent) {
      lane = "Life first, then health review";
      primary = ["Life protection first", "Health review second"];
      reasons.push("There is a financial protection need, and the health side does not appear to be the immediate fire.");
    } else if (lifeHard && healthUrgent) {
      lane = "Health first, qualify life carefully";
      primary = ["Health review first", "Life feasibility review second"];
      reasons.push("Medical coverage is urgent, and life underwriting may be challenging, so do not promise a life quote path too early.");
    } else {
      reasons.push("Health should usually come first when there is an active coverage gap, Medicare timing issue, or urgent deductible/network problem.");
    }

    return {
      lane,
      primary,
      available: ["Combined household budget check", "Schedule second review", "Cross-sell follow-up"],
      eliminated: [],
      reasons,
    };
  }

  if (
    productPath === "health" ||
    needArea === "health" ||
    healthGap === "yes" ||
    medicareFlag === "yes" ||
    medicareFlag === "soon"
  ) {
    if (!answers.healthCoverageType) {
      return {
        lane: "Health first",
        primary: ["Choose health coverage type"],
        available: ["Under-65 review", "Medicare review", "Supplemental gap review"],
        eliminated: [],
        reasons: [
          "This looks like a health-first call, but the desk still needs the health coverage type before it can recommend the right path.",
        ],
      };
    }
  }

  if (
    productPath === "life" ||
    needArea === "life" ||
    lifeNeed === "yes" ||
    protectionLoad === "yes"
  ) {
    if (!answers.goal) {
      return {
        lane: "Life first",
        primary: ["Clarify protection goal"],
        available: ["Income replacement", "Burial / final expense", "Permanent coverage", "Retirement / annuity"],
        eliminated: [],
        reasons: [
          "This looks like a life-first call, but you still need to identify what they are protecting before deciding the quote lane.",
        ],
      };
    }
  }

  if (productPath === "health") {
    const stateValue = document.getElementById("deskHealthState")?.value?.trim() || "";
    const laneBits = [];
    const primary = [];
    const available = [];
    const reasons = [];

    if (answers.healthCoverageType === "under65") {
      laneBits.push("Under-65 health review");
      primary.push("ACA Marketplace review", "Off-exchange comparison", "SEP eligibility check");
      reasons.push("Under-65 individual and family cases should start with SEP eligibility, subsidy fit, and network comparison.");
    } else if (answers.healthCoverageType === "medicare") {
      laneBits.push("Medicare review");
      primary.push("Medicare Advantage comparison", "Medigap review", "Prescription check");
      reasons.push("Medicare calls should start with current doctors, drug list, and whether the gap is Advantage, Supplement, or Part D related.");
    } else if (answers.healthCoverageType === "smallgroup") {
      laneBits.push("Small-group / business health review");
      primary.push("Group census review", "Employer contribution check", "Carrier market comparison");
      reasons.push("Group cases need census, participation, contribution, and employer goals before shopping carriers.");
    } else if (answers.healthCoverageType === "gap") {
      laneBits.push("Supplemental health gap review");
      primary.push("Hospital indemnity", "Critical illness", "Accident coverage");
      reasons.push("Gap coverage calls should start with deductible exposure, hospital risk, and missed benefits in the current plan.");
    } else {
      laneBits.push("Health intake");
      reasons.push("Choose the health coverage type first so the desk can recommend the right comparison path.");
    }

    if (answers.healthNeed === "lost_coverage") {
      primary.unshift("Special enrollment check");
      reasons.push("Lost coverage usually creates a short enrollment window, so eligibility and effective date come first.");
    } else if (answers.healthNeed === "deductible") {
      available.push("Hospital indemnity", "Critical illness");
      reasons.push("Deductible risk usually points to supplemental protection even if the main medical plan stays in place.");
    } else if (answers.healthNeed === "doctors") {
      available.push("Doctor lookup", "Network comparison");
      reasons.push("Doctor and hospital access should be checked before premium-only comparison.");
    } else if (answers.healthNeed === "rx") {
      available.push("Formulary check", "Part D / prescription review");
      reasons.push("Prescription issues require formulary and preferred pharmacy review, not just premium comparison.");
    }

    if (answers.healthPriority === "today") {
      available.unshift("Fast effective-date options");
      reasons.push("Immediate-need calls should prioritize effective date and enrollment timing.");
    }

    if (stateValue) {
      reasons.push(`State-specific plan availability and rules will need to be reviewed for ${stateValue}.`);
    }

    return {
      lane: laneBits.join(" • ") || "Health intake",
      primary: primary.slice(0, 3),
      available: [...new Set([...primary.slice(3), ...available])].slice(0, 5),
      eliminated: [],
      reasons: reasons.length ? reasons : ["Complete the health branch to show the recommended path."],
    };
  }

  if (productPath === "both") {
    const healthType = answers.healthCoverageType;
    const healthNeed = answers.healthNeed;
    const reasons = [
      "Start with the more urgent problem first so the call does not split into two half-done conversations.",
      "Capture shared household and budget facts once, then branch into health and life only where needed.",
    ];
    const primary = [];
    if (healthType === "medicare") {
      primary.push("Medicare review first", "Life protection follow-up");
      reasons.push("Medicare timing and doctor/drug fit are usually more urgent than life shopping in the same call.");
    } else if (healthNeed === "lost_coverage") {
      primary.push("Special enrollment first", "Life protection follow-up");
      reasons.push("Lost coverage is time-sensitive, so health eligibility should come before life quotes.");
    } else {
      primary.push("Life protection quote", "Health review second");
      reasons.push("If there is no health urgency, start with the protection need that triggered the call, then review medical coverage.");
    }
    return {
      lane: "Combined health + life workflow",
      primary,
      available: ["Schedule second review", "Combined household budget check", "Cross-sell follow-up"],
      eliminated: [],
      reasons,
    };
  }

  const carriers = {
    Ethos: { status: "available", score: 0 },
    SBLI: { status: "available", score: 0 },
    "Mutual of Omaha": { status: "available", score: 0 },
    Foresters: { status: "available", score: 0 },
    Corebridge: { status: "available", score: 0 },
    "F&G": { status: "available", score: 0 },
  };
  const reasons = [];
  let lane = "Answer the questions to narrow the lane.";

  const eliminate = (name, reason) => {
    if (carriers[name].status !== "eliminated") {
      carriers[name].status = "eliminated";
      reasons.push(reason);
    }
  };

  if (answers.goal === "income") {
    lane = "Income protection / term lane";
    eliminate("F&G", "Income protection points to term-first carriers instead of annuity carriers.");
    carriers.Ethos.score += 3;
    carriers.SBLI.score += 3;
    carriers["Mutual of Omaha"].score += 3;
    carriers.Foresters.score += 2;
    carriers.Corebridge.score += 1;
  } else if (answers.goal === "burial") {
    lane = "Burial / final expense lane";
    eliminate("SBLI", "Burial need removes SBLI EasyTrak because it is a term product, not final expense.");
    eliminate("F&G", "Burial need removes annuity-first conversations.");
    carriers["Mutual of Omaha"].score += 4;
    carriers.Foresters.score += 3;
    carriers.Corebridge.score += 2;
    carriers.Ethos.score += 1;
  } else if (answers.goal === "permanent") {
    lane = "Permanent coverage / cash value lane";
    eliminate("SBLI", "Lifetime or cash value need removes SBLI EasyTrak because it is term-only.");
    eliminate("F&G", "Lifetime coverage or cash value need is not an annuity-first conversation.");
    carriers["Mutual of Omaha"].score += 4;
    carriers.Foresters.score += 4;
    carriers.Corebridge.score += 3;
    carriers.Ethos.score += 1;
  } else if (answers.goal === "retirement") {
    lane = "Retirement / annuity lane";
    eliminate("Ethos", "Retirement income or accumulation pushes this away from life-insurance-first carriers.");
    eliminate("SBLI", "Retirement income or accumulation pushes this away from term-first carriers.");
    eliminate("Foresters", "Retirement income or accumulation pushes this away from life-insurance-first carriers.");
    eliminate("Mutual of Omaha", "Retirement income or accumulation pushes this toward annuity carriers first.");
    carriers["F&G"].score += 5;
    carriers.Corebridge.score += 4;
  }

  if (answers.age === "65to75") {
    eliminate("SBLI", "SBLI EasyTrak Digital Term is documented for ages 18-60 only.");
    carriers.Ethos.score -= 1;
    carriers["Mutual of Omaha"].score += 2;
    carriers.Foresters.score += 2;
    carriers.Corebridge.score += 1;
    reasons.push("Age 65 to 75 shifts strength away from simple digital term-first lanes and toward permanent or final-expense-friendly options.");
  } else if (answers.age === "76plus") {
    eliminate("SBLI", "SBLI EasyTrak Digital Term is documented for ages 18-60 only.");
    if (answers.goal === "income") {
      eliminate("Ethos", "Ethos term is documented only through age 65, so it is not a fit for income-protection term at this age.");
      eliminate("Mutual of Omaha", "Mutual of Omaha Term Life Express is documented only through issue age 75, so it should not be shown for 76+ term cases.");
    }
    carriers["Mutual of Omaha"].score += 2;
    carriers.Foresters.score += 2;
    carriers.Corebridge.score += 2;
    reasons.push("Very advanced age usually removes many term paths and favors final expense or annuity options.");
  } else if (answers.age === "50to64") {
    carriers["Mutual of Omaha"].score += 1;
    carriers.Foresters.score += 1;
  } else if (answers.age === "under50") {
    carriers.Ethos.score += 1;
    carriers.SBLI.score += 1;
  }

  if (answers.goal === "income" && (answers.age === "65to75" || answers.age === "76plus")) {
    eliminate("Ethos", "Ethos term is documented for ages 20-65, so it should not be shown for income-protection term above 65.");
  }

  if (answers.tobacco === "yes") {
    carriers.Ethos.score -= 1;
    carriers.SBLI.score -= 1;
    carriers["Mutual of Omaha"].score += 0;
    carriers.Foresters.score -= 1;
    reasons.push("Tobacco use does not automatically knock out Mutual of Omaha or SBLI, but it usually removes non-nicotine pricing and can change who is most competitive.");
    reasons.push("Foresters accelerated underwriting materials are strongest for non-tobacco cases, so tobacco should usually push Foresters down rather than eliminate it outright.");
  }

  if (answers.health === "managed") {
    carriers.SBLI.score -= 1;
    carriers.Ethos.score += 1;
    carriers["Mutual of Omaha"].score += 1;
    carriers.Foresters.score += 1;
    reasons.push("Managed conditions make simplified and flexible underwriting paths more valuable.");
  } else if (answers.health === "challenging") {
    if (answers.goal === "income") {
      eliminate("SBLI", "Serious health issues should remove SBLI EasyTrak first because it is simplified-issue term with tighter eligibility.");
      eliminate("Ethos", "For income-protection term, serious health issues make Ethos term much less likely and usually push the case out of the digital term lane.");
    }
    carriers["Mutual of Omaha"].score += 3;
    carriers.Foresters.score += 2;
    carriers.Ethos.score += 2;
    reasons.push("Serious health issues can eliminate term lanes and push the case toward simplified issue, guaranteed issue, or final expense.");
    if (answers.goal === "burial" || answers.goal === "permanent") {
      reasons.push("Ethos can stay alive here because guaranteed issue whole life is designed for people declined elsewhere, subject to age and state rules.");
    }
  }

  if (answers.budget === "low") {
    if (answers.goal !== "retirement") eliminate("F&G", "Low-budget protection need is not an annuity-first case.");
    if (answers.goal === "permanent") carriers.Corebridge.score -= 1;
    carriers.Ethos.score += 2;
    carriers.SBLI.score += 2;
    reasons.push("Low budget usually narrows the field toward affordable term or smaller-face final expense.");
  } else if (answers.budget === "high") {
    carriers["Mutual of Omaha"].score += 1;
    carriers.Foresters.score += 1;
    carriers.Corebridge.score += 2;
    carriers["F&G"].score += 2;
  }

  if (answers.duration === "temporary") {
    if (answers.goal === "permanent") {
      eliminate("Foresters", "Temporary need conflicts with permanent-first positioning here.");
      carriers.Corebridge.score -= 1;
    }
    carriers.Ethos.score += 2;
    carriers.SBLI.score += 2;
    carriers["Mutual of Omaha"].score += 1;
    reasons.push("Temporary need pushes the recommendation toward term-first carriers.");
  } else if (answers.duration === "lifelong") {
    if (answers.goal !== "income") eliminate("SBLI", "Lifelong need removes SBLI EasyTrak because it is term-only.");
    carriers["Mutual of Omaha"].score += 2;
    carriers.Foresters.score += 2;
    carriers.Corebridge.score += 1;
    reasons.push("Lifelong need removes many temporary-only solutions and elevates permanent carriers.");
  }

  if (answers.speed === "speed") {
    carriers.Ethos.score += 3;
    carriers.SBLI.score += 2;
    carriers.Corebridge.score -= 1;
    reasons.push("Fastest path favors digital and simplified quoting first.");
  } else if (answers.speed === "optimize") {
    carriers["Mutual of Omaha"].score += 2;
    carriers.Foresters.score += 2;
    carriers.Corebridge.score += 1;
    reasons.push("Best long-term fit favors broader comparison carriers over speed-only paths.");
  }

  if (answers.goal === "income" && answers.age && answers.age !== "under50") {
    reasons.push("Mutual of Omaha and Foresters stay in play longer on older term cases than SBLI EasyTrak, which has a documented age cap of 60.");
  }

  if (answers.goal === "burial") {
    reasons.push("Corebridge and Ethos final-expense-style paths may include graded death benefit or waiting-period features depending on product and approval route.");
  }

  const residualChecks = [];
  if (carriers.SBLI.status !== "eliminated") {
    residualChecks.push("SBLI still needs non-medical eligibility checks like age 18-60, U.S. residency, employment status, health insurance, and no replacement use.");
  }
  if (carriers.Ethos.status !== "eliminated") {
    residualChecks.push("Ethos still depends on product-specific age band: term is documented 20-65 and whole life up to 85, with guaranteed issue typically used when health blocks other options.");
  }
  if (carriers["Mutual of Omaha"].status !== "eliminated") {
    residualChecks.push("Mutual of Omaha tobacco users can still be eligible, but tobacco usually means nicotine rates rather than non-nicotine classes.");
  }
  reasons.push(...residualChecks);

  const entries = Object.entries(carriers);
  const available = entries
    .filter(([, meta]) => meta.status !== "eliminated")
    .sort((a, b) => b[1].score - a[1].score)
    .map(([name]) => name);
  const eliminated = entries.filter(([, meta]) => meta.status === "eliminated").map(([name]) => name);
  const primary = available.slice(0, 3);

  return {
    lane,
    primary,
    available,
    eliminated,
    reasons: reasons.length ? reasons : ["Each answer will remove or reorder carriers here."],
  };
}

function renderCallDeskQualification() {
  const questionsEl = document.getElementById("deskScript2Questions");
  const checksEl = document.getElementById("deskCarrierDecisionChecks");
  const cueEl = document.getElementById("deskDynamicScriptCue");
  if (!questionsEl || !checksEl) return;

  const answers = state.ui.workflowAnswers;
  const path = getCallDeskPath();
  const existingPolicy = String(document.getElementById("deskExistingPolicy")?.value || "");
  const policyIntent = String(document.getElementById("deskPolicyIntent")?.value || "");

  const tobaccoKnown = Boolean(String(answers.tobacco || "").trim());
  const hasCurrentCoverage = Boolean(String(document.getElementById("deskCurrentCoverage")?.value || "").trim());
  const hasDecisionMaker = Boolean(String(document.getElementById("deskDecisionMaker")?.value || "").trim());
  const questions = ["What made you look into coverage right now?"];

  if (path === "health") {
    questions.push("Which health problem is most urgent: premium, doctors/network, prescriptions, or lost coverage?");
    if (!hasCurrentCoverage) questions.push("Is this your first policy, or do you already have coverage?");
    if (!policyIntent) questions.push("Are you replacing current coverage or adding to it?");
    if (!tobaccoKnown) questions.push("Do you currently use tobacco or nicotine products?");
    questions.push(
      "When do you need coverage effective (today, 30 days, or just reviewing)?",
    );
  } else if (path === "life") {
    if (!hasDecisionMaker) questions.push("Who depends on you financially (spouse, kids, mortgage, final expenses)?");
    if (!policyIntent) questions.push("Are you replacing current coverage or adding to it?");
    questions.push("Is this mostly temporary protection or lifelong planning?");
    questions.push("What monthly budget feels comfortable?");
    if (!tobaccoKnown) questions.push("Do you currently use tobacco or nicotine products?");
  } else if (path === "both" || path === "unclear" || !path) {
    if (!hasCurrentCoverage) questions.push("Is this your first policy, or do you already have coverage?");
    if (!hasDecisionMaker) questions.push("Who depends on you financially (spouse, kids, mortgage, final expenses)?");
    if (!policyIntent) questions.push("Are you replacing current coverage or adding to it?");
    questions.push("Which health problem is most urgent: premium, doctors/network, prescriptions, or lost coverage?");
    questions.push("When do you need coverage effective (today, 30 days, or just reviewing)?");
    questions.push("Is this mostly temporary protection or lifelong planning?");
    questions.push("What monthly budget feels comfortable?");
    if (!tobaccoKnown) questions.push("Do you currently use tobacco or nicotine products?");
  }

  const checks = [];
  if (path === "health" || path === "both" || path === "unclear") {
    checks.push("Confirm lane first: Under-65, Medicare, small-group, or supplemental gap.");
    if (answers.healthNeed === "lost_coverage") checks.push("Treat as time-sensitive: run special enrollment timing first.");
  }
  if (path === "life" || path === "both" || path === "unclear" || !path) {
    checks.push("If replacing a policy, verify replacement intent before quoting.");
    if (answers.goal === "retirement") checks.push("Retirement lane: prioritize F&G and Corebridge first.");
    if (answers.goal === "income" && (answers.age === "65to75" || answers.age === "76plus")) {
      checks.push("Older income-protection term case: SBLI and Ethos term lanes may be limited.");
    }
    if (answers.health === "challenging") {
      checks.push("Challenging health: move from digital term-first to simplified/final-expense options.");
    }
    if (answers.goal === "burial" || answers.goal === "permanent") {
      checks.push("Burial/permanent lane: keep Mutual of Omaha and Foresters high in comparison order.");
    }
  }
  if (existingPolicy === "yes" && !policyIntent) {
    checks.push("Existing policy flagged: capture replace vs supplement before producing a quote.");
  }
  if (existingPolicy === "yes" && policyIntent === "replace") {
    checks.push("Replacement intent selected: confirm policy details and avoid accidental downgrade.");
  }
  if (!checks.length) checks.push("Answer discovery fields to unlock lane-specific carrier checks.");

  questionsEl.innerHTML = questions.map((q) => `<li>${escapeHtml(q)}</li>`).join("");
  checksEl.innerHTML = checks.map((c) => `<li>${escapeHtml(c)}</li>`).join("");

  if (cueEl) {
    const triggerKey =
      answers.healthNeed === "lost_coverage"
        ? "lost_coverage"
        : policyIntent === "replace"
          ? "replace"
          : policyIntent === "new"
            ? "new"
            : "";
    const script = DESK_DYNAMIC_SCRIPTS[triggerKey];
    if (script) {
      cueEl.hidden = false;
      cueEl.textContent = `"${script}"`;
    } else {
      cueEl.hidden = true;
      cueEl.textContent = "";
    }
  }
  renderDeskReadiness();
}

function buildDeskReadinessRequirements() {
  const getValue = (id) => String(document.getElementById(id)?.value || "").trim();
  const productPath = getCallDeskPath();
  const existingPolicy = getValue("deskExistingPolicy");
  const needsHealth = ["health", "both", "unclear"].includes(productPath);
  const needsLife = ["life", "both", "unclear"].includes(productPath);

  const required = [
    { id: "deskNeedArea", label: "Need area", value: getValue("deskNeedArea") },
    { id: "deskCurrentCoverage", label: "Current coverage", value: getValue("deskCurrentCoverage") },
    { id: "deskProductPath", label: "Product path", value: getValue("deskProductPath") },
  ];

  if (needsHealth) {
    required.push(
      { id: "deskHealthCoverageType", label: "Health coverage type", value: getValue("deskHealthCoverageType") },
      { id: "deskHealthNeed", label: "Health pain point", value: getValue("deskHealthNeed") },
      { id: "deskHealthPriority", label: "Health urgency", value: getValue("deskHealthPriority") },
    );
  }

  if (needsLife) {
    required.push(
      { id: "deskGoal", label: "Life goal", value: getValue("deskGoal") },
      { id: "deskAge", label: "Age band", value: getValue("deskAge") },
      { id: "deskBudget", label: "Budget level", value: getValue("deskBudget") },
      { id: "deskDuration", label: "Coverage duration", value: getValue("deskDuration") },
    );
    if (productPath !== "life") {
      required.push({ id: "deskHealth", label: "Health posture", value: getValue("deskHealth") });
    }
  }

  if (existingPolicy === "yes") {
    required.push({
      id: "deskPolicyIntent",
      label: "Policy intent (replace vs supplement)",
      value: getValue("deskPolicyIntent"),
    });
  }

  return required;
}

function canBypassReadinessForSave() {
  const disposition = String(document.getElementById("deskDisposition")?.value || "").trim().toLowerCase();
  return Boolean(disposition);
}

function updateSaveButtonAvailability() {
  const btn = document.getElementById("deskSaveToNotesBtn");
  if (!btn) return;
  if (state.ui.saveStatus === "saving") {
    btn.disabled = true;
    return;
  }
  const missing = buildDeskReadinessRequirements().some((item) => !item.value);
  btn.disabled = missing && !canBypassReadinessForSave();
}

function updateDeskRequiredFieldIndicators(required) {
  const byFieldId = new Map(required.map((item) => [item.id, item]));
  document.querySelectorAll("label[data-field-id]").forEach((label) => {
    const fieldId = String(label.dataset.fieldId || "");
    const requirement = byFieldId.get(fieldId);
    label.classList.remove("field-required-missing", "field-required-complete");
    const fieldEl = document.getElementById(fieldId);
    if (fieldEl) fieldEl.classList.remove("border-red-500", "border-green-500");
    if (!requirement) return;
    const complete = Boolean(requirement.value);
    label.classList.add(complete ? "field-required-complete" : "field-required-missing");
    if (fieldEl) fieldEl.classList.add(complete ? "border-green-500" : "border-red-500");
  });

  ["deskProductPath", "deskNeedArea", "deskCurrentCoverage"].forEach((id) => {
    const labelEl = document.querySelector(`label[data-field-id="${id}"] > span`);
    const req = byFieldId.get(id);
    if (!labelEl || !req) return;
    labelEl.style.color = req.value ? "#4ade80" : "#f87171";
  });
}

function renderDeskReadiness() {
  const statusEl = document.getElementById("deskReadinessStatus");
  const listEl = document.getElementById("deskReadinessList");
  const headingEl = document.getElementById("deskReadinessHeading");
  if (!statusEl || !listEl) return;

  const required = buildDeskReadinessRequirements();
  const missing = required.filter((item) => !item.value).map((item) => item.label);
  updateDeskRequiredFieldIndicators(required);

  const copyBtn = document.getElementById("deskCopySummaryBtn");
  const saveBtn = document.getElementById("deskSaveToNotesBtn");
  if (!missing.length) {
    if (headingEl) {
      headingEl.textContent = "Ready to Quote";
      headingEl.classList.add("ready-pill");
    }
    statusEl.textContent = "Ready to quote: all required answers are captured.";
    statusEl.style.background = "var(--green-soft)";
    statusEl.style.color = "var(--green)";
    listEl.innerHTML = required
      .map((item) => `<li class="readiness-item complete">${escapeHtml(item.label)}</li>`)
      .join("");
    if (copyBtn) copyBtn.disabled = false;
    if (saveBtn) updateSaveButtonAvailability();
    return { missingCount: 0, missing };
  }

  statusEl.textContent = `Missing ${missing.length} required field${missing.length === 1 ? "" : "s"} before quoting.`;
  if (headingEl) {
    headingEl.textContent = "Required before quote";
    headingEl.classList.remove("ready-pill");
  }
  statusEl.style.background = "var(--amber-soft)";
  statusEl.style.color = "var(--amber)";
  listEl.innerHTML = required
    .map((item) => {
      const klass = item.value ? "readiness-item complete" : "readiness-item";
      return `<li class="${klass}">${escapeHtml(item.label)}</li>`;
    })
    .join("");
  if (copyBtn) copyBtn.disabled = true;
  if (saveBtn) updateSaveButtonAvailability();
  return { missingCount: missing.length, missing };
}

function renderWorkflowAdvisor() {
  runRecommendationEffect();
  const result = evaluateWorkflowOptions();
  const path = getCallDeskPath();
  const answers = state.ui.workflowAnswers;
  const liveCarrier = getLiveCarrierRecommendation(path, answers);
  const matchedCarrierNames = liveCarrier.matches;
  const confidenceOverride = liveCarrier.confidence;
  const fallbackPrimary = matchedCarrierNames[0] || (path === "health" ? "Medicare / Health Review" : "Life Lane Review");
  const matchedPrimary = liveCarrier.primary ? [liveCarrier.primary] : matchedCarrierNames.slice(0, 3);
  const coreFields = [path];
  if (path === "health") {
    coreFields.push(
      state.ui.workflowAnswers.healthCoverageType,
      state.ui.workflowAnswers.healthNeed,
      state.ui.workflowAnswers.healthPriority,
    );
  } else if (path === "life") {
    coreFields.push(
      state.ui.workflowAnswers.goal,
      state.ui.workflowAnswers.age,
      state.ui.workflowAnswers.budget,
      state.ui.workflowAnswers.duration,
    );
  } else if (path === "both" || path === "unclear") {
    coreFields.push(
      state.ui.workflowAnswers.goal,
      state.ui.workflowAnswers.age,
      state.ui.workflowAnswers.health,
      state.ui.workflowAnswers.budget,
      state.ui.workflowAnswers.duration,
      state.ui.workflowAnswers.healthCoverageType,
      state.ui.workflowAnswers.healthNeed,
      state.ui.workflowAnswers.healthPriority,
    );
  }
  const answeredCore = coreFields.filter(Boolean).length;
  const confidencePct = Math.min(100, Math.round((answeredCore / coreFields.length) * 100));
  const workflowLaneEl = document.getElementById("workflowLane");
  if (workflowLaneEl) workflowLaneEl.textContent = liveCarrier.lane || result.lane;
  renderChipList("workflowPrimary", result.primary, "primary");
  renderChipList("workflowAvailable", result.available, "available");
  renderChipList("workflowEliminated", result.eliminated, "eliminated");
  const workflowReasonsEl = document.getElementById("workflowReasons");
  if (workflowReasonsEl) {
    workflowReasonsEl.innerHTML = result.reasons
      .map((reason) => `<li>${reason}</li>`)
      .join("");
  }

  const deskLane = document.getElementById("deskWorkflowLane");
  if (deskLane) {
    const title = document.getElementById("deskRecommendationTitle");
    if (title) {
      title.textContent =
        path === "health"
          ? "Recommended health path"
          : path === "both" || path === "unclear"
            ? "Recommended first move"
            : "Quote order and lane";
    }
    deskLane.textContent = liveCarrier.lane || result.lane;
    renderChipList("deskWorkflowPrimary", matchedPrimary.length ? matchedPrimary : [fallbackPrimary], "primary");
    renderChipList("deskWorkflowAvailable", matchedCarrierNames.length ? matchedCarrierNames : result.available, "available");
    const confidenceEl = document.getElementById("deskWorkflowConfidence");
    if (confidenceEl) {
      if (confidenceOverride !== null) {
        const rawConfidence = String(confidenceOverride || "").trim();
        if (rawConfidence === "Low confidence (0%)") {
          confidenceEl.textContent = rawConfidence;
        } else if (/^\d+%$/.test(rawConfidence)) {
          confidenceEl.textContent = rawConfidence;
        } else if (/^\d+$/.test(rawConfidence)) {
          confidenceEl.textContent = `${rawConfidence}%`;
        } else {
          confidenceEl.textContent = rawConfidence || "Low confidence (0%)";
        }
      } else {
        const level = confidencePct >= 84 ? "High" : confidencePct >= 55 ? "Medium" : "Low";
        confidenceEl.textContent = `${level} confidence (${confidencePct}%). Complete qualification for higher certainty.`;
      }
    }
    const fallbackSource = matchedCarrierNames.length ? matchedCarrierNames : result.available;
    const fallback = fallbackSource.length ? [fallbackSource[matchedPrimary.length] || fallbackSource[0]] : [];
    renderChipList("deskWorkflowFallback", fallback.filter(Boolean), "available");
    const carrierMatch = getCarrierMatches(path, state.ui.workflowAnswers);
    const mergedReasons = [
      ...result.reasons,
      ...carrierMatch.reasons,
      ...(liveCarrier.warning ? [liveCarrier.warning] : []),
      ...carrierMatch.snippets.map((snippet) => `Script cue: ${snippet}`),
    ];
    document.getElementById("deskWorkflowReasons").innerHTML = mergedReasons
      .map((reason) => `<li>${escapeHtml(reason)}</li>`)
      .join("");
  }
  renderCallDeskQualification();
  updateGeneratedCallSummary();
  renderCarrierActionCard();
}

function renderCallDeskBranching() {
  const path = getCallDeskPath();
  const showAll = !path || path === "both" || path === "unclear";
  const workflowFieldMap = {
    deskGoal: "goal",
    deskHealth: "health",
    deskHealthCoverageType: "healthCoverageType",
  };

  document.querySelectorAll(".desk-discovery-field").forEach((section) => {
    const branches = (section.dataset.show || "").split(/\s+/).filter(Boolean);
    const visible = showAll || !branches.length || branches.includes(path);
    section.hidden = !visible;
    if (!visible) {
      const field = section.querySelector("input, select, textarea");
      if (field) {
        field.value = "";
        const key = workflowFieldMap[field.id];
        if (key) state.ui.workflowAnswers[key] = "";
      }
    }
  });

  document.querySelectorAll(".desk-branch").forEach((section) => {
    const branches = (section.dataset.branch || "").split(/\s+/).filter(Boolean);
    const visible = Boolean(path) && branches.includes(path);
    section.hidden = !visible;
  });
  syncWorkflowControls();
  renderDeskReadiness();
}

function syncWorkflowControls() {
  const fieldMap = [
    ["productPath", null, "deskProductPath"],
    ["goal", "workflowGoal", "deskGoal"],
    ["age", "workflowAge", "deskAge"],
    ["tobacco", "workflowTobacco", "deskTobacco"],
    ["health", "workflowHealth", "deskHealth"],
    ["budget", "workflowBudget", "deskBudget"],
    ["duration", "workflowDuration", "deskDuration"],
    ["speed", "workflowSpeed", "deskSpeed"],
  ];

  fieldMap.forEach(([key, workflowId, deskId]) => {
    const value = state.ui.workflowAnswers[key] || "";
    const workflowEl = document.getElementById(workflowId);
    const deskEl = document.getElementById(deskId);
    if (workflowEl) workflowEl.value = value;
    if (deskEl) deskEl.value = value;
  });
}

function setActiveTab(tab) {
  const legacyToCurrent = {
    notes: "calldesk",
    script: "calldesk",
    guide: "calldesk",
  };
  let normalizedTab = legacyToCurrent[tab] || tab;

  const panels = {
    dashboard: document.getElementById("dashboardTabPanel"),
    leadselection: document.getElementById("leadSelectionTabPanel"),
    calldesk: document.getElementById("callDeskTabPanel"),
    campaign: document.getElementById("campaignTabPanel"),
    calendar: document.getElementById("calendarTabPanel"),
    pipeline: document.getElementById("pipelineTabPanel"),
    contentstudio: document.getElementById("contentStudioTabPanel"),
  };

  if (!Object.prototype.hasOwnProperty.call(panels, normalizedTab)) {
    normalizedTab = "calldesk";
  }
  state.ui.activeTab = normalizedTab;
  localStorage.setItem(ACTIVE_TAB_STORAGE_KEY, normalizedTab);

  document.querySelectorAll("[data-tab]").forEach((button) => {
    const active = button.dataset.tab === normalizedTab;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  });

  Object.entries(panels).forEach(([key, panel]) => {
    if (!panel) return;
    const active = key === normalizedTab;
    panel.classList.toggle("active", active);
    panel.hidden = !active;
  });

  const shellTop = document.querySelector(".main-panel");
  if (shellTop) {
    shellTop.scrollIntoView({ behavior: "auto", block: "start" });
  } else {
    window.scrollTo({ top: 0, behavior: "auto" });
  }

  if (normalizedTab === "dashboard" || normalizedTab === "calendar") {
    setGoogleCalendarEmbeds();
  }
  if (normalizedTab === "contentstudio") {
    loadContentStudioData().catch(() => {});
  }
}

function loadNotesState() {
  try {
    const raw = JSON.parse(localStorage.getItem(NOTES_STORAGE_KEY) || "{}");
    if (raw && typeof raw === "object" && ("current" in raw || "history" in raw)) {
      return {
        current: raw.current || {},
        history: Array.isArray(raw.history) ? raw.history : [],
      };
    }
    return { current: raw || {}, history: [] };
  } catch {
    return { current: {}, history: [] };
  }
}

function saveNotesState(current, history) {
  localStorage.setItem(NOTES_STORAGE_KEY, JSON.stringify({ current, history }));
}

function renderNotesStatus(text) {
  const status = document.getElementById("notesStatus");
  if (status) status.textContent = text;
}

function getNotesSnapshot(fieldIds) {
  return fieldIds.reduce((acc, id) => {
    const el = document.getElementById(id);
    acc[id] = el ? el.value : "";
    return acc;
  }, {});
}

function hasNotesContent(snapshot) {
  return Object.values(snapshot).some((value) => String(value || "").trim().length);
}

function formatNotesTitle(snapshot) {
  const name = (snapshot.notesClientName || "").trim();
  const goal = (snapshot.notesGoal || "").trim();
  if (name && goal) return `${name} • ${goal}`;
  if (name) return name;
  if (goal) return goal;
  return "Untitled call";
}

function renderNotesHistory(history, fieldIds) {
  const container = document.getElementById("notesHistoryList");
  if (!container) return;
  if (!history.length) {
    container.innerHTML = `<p class="muted">No saved notes yet.</p>`;
    return;
  }
  const formatter = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });

  container.innerHTML = history
    .map((entry) => {
      const title = formatNotesTitle(entry.snapshot || {});
      const time = formatter.format(new Date(entry.savedAt));
      const detailText = Object.entries(entry.snapshot || {})
        .map(([key, value]) => (value ? `${key.replace("notes", "")}: ${value}` : ""))
        .filter(Boolean)
        .join("\n");
      return `
        <div class="notes-history-item">
          <div>
            <strong>${title}</strong>
            <span>${time}</span>
          </div>
          <div class="notes-history-actions">
            <button class="ghost-button slim" type="button" data-note-toggle="${entry.id}">View</button>
            <button class="ghost-button slim" type="button" data-note-load="${entry.id}">Load</button>
            <button class="ghost-button slim" type="button" data-note-delete="${entry.id}">Delete</button>
          </div>
          <div class="notes-history-body hidden" data-note-body="${entry.id}">${detailText || "No details saved."}</div>
        </div>
      `;
    })
    .join("");

  container.querySelectorAll("[data-note-toggle]").forEach((button) => {
    button.addEventListener("click", () => {
      const id = button.dataset.noteToggle;
      const body = container.querySelector(`[data-note-body="${id}"]`);
      if (!body) return;
      const isHidden = body.classList.toggle("hidden");
      button.textContent = isHidden ? "View" : "Hide";
    });
  });

  container.querySelectorAll("[data-note-load]").forEach((button) => {
    button.addEventListener("click", () => {
      const id = button.dataset.noteLoad;
      const match = history.find((item) => item.id === id);
      if (!match) return;
      let loadedCount = 0;
      fieldIds.forEach((field) => {
        const el = document.getElementById(field);
        if (el) {
          el.value = match.snapshot?.[field] || "";
          loadedCount += 1;
        }
      });
      if (loadedCount === 0) {
        const snapshot = match.snapshot || {};
        const deskMap = {
          deskClientName: snapshot.notesClientName || "",
          deskGoalNote: snapshot.notesGoal || "",
          deskHealthNotes: snapshot.notesHealth || "",
          deskCoverage: snapshot.notesCoverage || "",
          deskBudgetText: snapshot.notesBudget || "",
          deskCallNotes: snapshot.notesBody || "",
        };
        Object.entries(deskMap).forEach(([field, value]) => {
          const el = document.getElementById(field);
          if (el) el.value = value;
        });
      }
      saveNotesState(match.snapshot || {}, history);
      renderNotesStatus("Loaded");
    });
  });

  container.querySelectorAll("[data-note-delete]").forEach((button) => {
    button.addEventListener("click", () => {
      const id = button.dataset.noteDelete;
      const index = history.findIndex((item) => item.id === id);
      if (index === -1) return;
      history.splice(index, 1);
      const current = loadNotesState().current || {};
      saveNotesState(current, history);
      renderNotesHistory(history, fieldIds);
      renderNotesStatus("Deleted");
    });
  });
}

function attachNotesHandlers() {
  const fields = [
    "notesClientName",
    "notesGoal",
    "notesAgeTobacco",
    "notesHealth",
    "notesBudget",
    "notesCoverage",
    "notesBody",
  ];

  const saved = loadNotesState();
  const history = saved.history || [];
  fields.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    if (saved.current?.[id]) el.value = saved.current[id];
    el.addEventListener("input", () => {
      const snapshot = getNotesSnapshot(fields);
      saveNotesState(snapshot, history);
      renderNotesStatus("Saved");
    });
  });

  document.querySelectorAll("[data-note-chip]").forEach((button) => {
    button.addEventListener("click", () => {
      const body = document.getElementById("notesBody");
      const tag = button.dataset.noteChip;
      if (!body) return;
      const prefix = body.value.trim().length ? "\n" : "";
      body.value = `${body.value}${prefix}- ${tag}`;
      body.dispatchEvent(new Event("input"));
    });
  });

  const saveBtn = document.getElementById("notesSaveBtn");
  if (saveBtn) {
    saveBtn.addEventListener("click", () => {
      const snapshot = getNotesSnapshot(fields);
      if (hasNotesContent(snapshot)) {
        history.unshift({
          id: `${Date.now()}`,
          savedAt: Date.now(),
          snapshot,
        });
      }
      fields.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.value = "";
      });
      saveNotesState({}, history);
      renderNotesHistory(history, fields);
      renderNotesStatus("Saved");
    });
  }

  const clearBtn = document.getElementById("notesClearBtn");
  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      fields.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.value = "";
      });
      saveNotesState({}, history);
      renderNotesStatus("Cleared");
    });
  }

  const copyBtn = document.getElementById("notesCopyBtn");
  if (copyBtn) {
    copyBtn.addEventListener("click", () => {
      const snapshot = getNotesSnapshot(fields);
      const data = Object.entries(snapshot)
        .map(([key, value]) => (value ? `${key.replace("notes", "")}: ${value}` : ""))
        .filter(Boolean)
        .join("\n");
      copyText(data);
    });
  }

  renderNotesHistory(history, fields);

  const current = saved.current || {};
  const deskFieldMap = {
    deskClientName: current.notesClientName || "",
    deskGoalNote: current.notesGoal || "",
    deskHealthNotes: current.notesHealth || "",
    deskCoverage: current.notesCoverage || "",
    deskBudgetText: current.notesBudget || "",
    deskCallNotes: current.notesBody || "",
  };

  Object.entries(deskFieldMap).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el && !el.value) el.value = value;
  });
}

function buildFilteredLeadSummary() {
  return getFilteredLeads()
    .slice(0, 15)
    .map((row) => `${row.full_name || `${row.first_name} ${row.last_name}`.trim()} | ${row.routing_bucket} | ${row.recommended_channel} | ${row.recommended_next_action}`)
    .join("\n");
}

function buildSelectedLeadBrief() {
  const row = state.sourcedLeads.find((lead) => lead["Contact Id"] === state.ui.selectedSourcedLeadId);
  if (!row) return "";
  const leadState = getSourcedLeadState(row["Contact Id"]);
  return [
    `${row["First Name"] || ""} ${row["Last Name"] || ""}`.trim(),
    `Stage: ${leadState.stage}`,
    `Trigger: ${row["Trigger Event"] || "-"}`,
    `First touch: ${row["First Touch Strategy"] || "-"}`,
    `Path: ${row["Contact Path"] || row["Post or Message URL"] || "-"}`,
    `Circumstances: ${row["Lead Circumstances"] || row.Notes || "-"}`,
    `Evidence: ${row["Source Evidence"] || "-"}`,
    `Operator note: ${leadState.ownerNote || "-"}`,
  ].join("\n");
}

function setContentStudioStatus(message) {
  const el = document.getElementById("contentStudioStatus");
  if (el) el.textContent = String(message || "");
}

async function apiFetch(input, init = {}) {
  try {
    return await fetch(input, init);
  } catch (error) {
    const asText = String(input || "");
    if (!asText.includes(":8787")) throw error;
    const variants = [];
    if (asText.includes("127.0.0.1")) variants.push(asText.replace("127.0.0.1", "localhost"));
    if (asText.includes("localhost")) variants.push(asText.replace("localhost", "127.0.0.1"));
    for (const candidate of variants) {
      try {
        return await fetch(candidate, init);
      } catch {
        // try next variant
      }
    }
    throw error;
  }
}

function readContentEditorValues() {
  return {
    post_id: String(document.getElementById("contentEditPostId")?.value || "").trim(),
    status: String(document.getElementById("contentEditStatus")?.value || "").trim(),
    post_date: String(document.getElementById("contentEditDate")?.value || "").trim(),
    post_time: String(document.getElementById("contentEditTime")?.value || "").trim(),
    platforms: String(document.getElementById("contentEditPlatforms")?.value || "")
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean),
    hook: String(document.getElementById("contentEditHook")?.value || "").trim(),
    caption: String(document.getElementById("contentEditCaption")?.value || "").trim(),
    reel_script: String(document.getElementById("contentEditReelScript")?.value || "").trim(),
    visual_prompt: String(document.getElementById("contentEditVisualPrompt")?.value || "").trim(),
    canva_design_link: String(document.getElementById("contentEditCanvaLink")?.value || "").trim(),
    cta: String(document.getElementById("contentEditCta")?.value || "").trim(),
    hashtags_text: String(document.getElementById("contentEditHashtags")?.value || "").trim(),
  };
}

function formatPreviewCaptionForPlatform(current, platform) {
  const PLATFORM_PREVIEW_RULES = {
    instagram: { label: "Instagram", maxChars: 2200, maxHashtags: 30 },
    tiktok: { label: "TikTok", maxChars: 300, maxHashtags: 6 },
    facebook: { label: "Facebook", maxChars: 63206, maxHashtags: 4 },
  };
  const rule = PLATFORM_PREVIEW_RULES[platform] || PLATFORM_PREVIEW_RULES.instagram;
  const platformLabel = platform === "tiktok" ? "TikTok" : platform === "facebook" ? "Facebook" : "Instagram";
  const normalizeHashtag = (tag) => {
    const cleaned = String(tag || "")
      .trim()
      .replace(/^#+/, "")
      .replace(/[^A-Za-z0-9_]/g, "");
    return cleaned ? `#${cleaned}` : "";
  };
  const rawTags = String(current.hashtags_text || "")
    .split(/\s+/)
    .map((tag) => normalizeHashtag(tag))
    .filter(Boolean);
  const limitedTags = rawTags.slice(0, rule.maxHashtags);
  const bodyBlocks = [];
  if (current.hook) bodyBlocks.push(current.hook);
  if (current.caption) bodyBlocks.push(current.caption);
  if (current.cta) bodyBlocks.push(current.cta);
  let textBody = bodyBlocks.filter(Boolean).join("\n\n").trim();
  let truncated = false;
  if (textBody.length > rule.maxChars) {
    truncated = true;
    textBody = `${textBody.slice(0, Math.max(0, rule.maxChars - 1)).trimEnd()}…`;
  }
  const tagsLine = limitedTags.join(" ");
  let text = textBody;
  if (tagsLine) {
    const withTags = text ? `${text}\n\n${tagsLine}` : tagsLine;
    if (withTags.length <= rule.maxChars) {
      text = withTags;
    } else {
      let fittingTags = [];
      limitedTags.forEach((tag) => {
        const next = [...fittingTags, tag].join(" ");
        const candidate = text ? `${text}\n\n${next}` : next;
        if (candidate.length <= rule.maxChars) fittingTags.push(tag);
      });
      if (fittingTags.length) {
        text = text ? `${text}\n\n${fittingTags.join(" ")}` : fittingTags.join(" ");
      }
    }
  }
  const charCount = text.length;
  return {
    platformLabel: rule.label || platformLabel,
    text: text || "(No caption yet)",
    charCount,
    maxChars: rule.maxChars,
    maxHashtags: rule.maxHashtags,
    truncated,
  };
}

function renderContentPreview() {
  const metaEl = document.getElementById("contentPreviewMeta");
  const hookEl = document.getElementById("contentPreviewHook");
  const captionEl = document.getElementById("contentPreviewCaption");
  const scriptEl = document.getElementById("contentPreviewScript");
  const visualEl = document.getElementById("contentPreviewVisual");
  const scoreEl = document.getElementById("contentQualityScore");
  const notesEl = document.getElementById("contentQualityNotes");
  if (!metaEl || !hookEl || !captionEl || !scriptEl || !visualEl || !scoreEl || !notesEl) return;
  const current = readContentEditorValues();
  if (!current.post_id) {
    metaEl.textContent = "No post selected.";
    hookEl.textContent = "-";
    captionEl.textContent = "-";
    scriptEl.textContent = "-";
    visualEl.textContent = "-";
    scoreEl.textContent = "-";
    notesEl.textContent = "-";
    const platformLabelEl = document.getElementById("contentPreviewPlatformLabel");
    if (platformLabelEl) platformLabelEl.textContent = "Platform Preview: Instagram";
    return;
  }
  const platform = String(state.ui.contentPreviewPlatform || "instagram");
  const preview = formatPreviewCaptionForPlatform(current, platform);
  metaEl.textContent = `${current.post_id} • ${current.status || "draft"} • ${current.post_date || "-"} ${current.post_time || "-"} • ${current.platforms.join(", ") || "-"}`;
  const platformLabelEl = document.getElementById("contentPreviewPlatformLabel");
  if (platformLabelEl) {
    const truncation = preview.truncated ? " • trimmed to fit" : "";
    platformLabelEl.textContent = `Platform Preview: ${preview.platformLabel} • ${preview.charCount}/${preview.maxChars} chars • max ${preview.maxHashtags} hashtags${truncation}`;
  }
  hookEl.textContent = current.hook || "(No hook yet)";
  captionEl.textContent = preview.text;
  scriptEl.textContent = current.reel_script || "(No reel script yet)";
  visualEl.textContent = current.visual_prompt || "(No visual prompt yet)";
  const review = evaluateContentPostQuality(current);
  scoreEl.textContent = `${review.score}/100 (${review.label})`;
  notesEl.textContent = review.notes.join("\n") || "No review notes.";
}

function openSelectedContentAsset() {
  const asset = String(document.getElementById("contentEditAssetFile")?.value || "").trim();
  if (!asset) {
    setContentStudioStatus("No final asset URL set for selected post.");
    return;
  }
  window.open(asset, "_blank", "noopener,noreferrer");
}

function openSelectedCanvaDesign() {
  const url = String(document.getElementById("contentEditCanvaLink")?.value || "").trim();
  if (!url) {
    setContentStudioStatus("No Canva design link set for selected post.");
    return;
  }
  window.open(url, "_blank", "noopener,noreferrer");
}

async function copyPreviewCaptionToClipboard() {
  const text = String(document.getElementById("contentPreviewCaption")?.textContent || "").trim();
  if (!text || text === "-") {
    setContentStudioStatus("No caption preview to copy.");
    return;
  }
  try {
    await navigator.clipboard.writeText(text);
    setContentStudioStatus("Caption copied to clipboard.");
  } catch {
    setContentStudioStatus("Could not copy caption (clipboard permission blocked).");
  }
}

function buildCanvaHandoffText(post) {
  const current = post || getSelectedContentPost();
  if (!current) return "";
  const platforms = Array.isArray(current.platforms) ? current.platforms.join(", ") : String(current.platforms || "");
  const safe = (value, fallback = "(none yet)") => String(value || "").trim() || fallback;
  return [
    `Post ID: ${safe(current.post_id || current.id)}`,
    `Platform(s): ${safe(platforms)}`,
    `Topic: ${safe(current.topic)}`,
    `Hook: ${safe(current.hook)}`,
    `Caption:`,
    safe(current.caption),
    ``,
    `CTA: ${safe(current.cta)}`,
    `Canva design link: ${safe(current.canva_design_link, "(add when available)")}`,
    `Reel / motion notes:`,
    safe(current.reel_script),
    ``,
    `Canva layout notes:`,
    safe(current.visual_prompt, "Use a calm editorial layout. Keep the image text-free until Canva adds the final typography."),
    ``,
    `Design rules:`,
    `- Imagen provides text-free visuals only`,
    `- Canva owns all text placement and layout changes`,
    `- Keep palette restrained: white, black, beige, brown, grey`,
    `- Keep layouts simple, editorial, and mobile-readable`,
    `- Export final asset and paste the public URL back into Content Studio`,
  ].join("\n");
}

async function copyCanvaHandoffToClipboard() {
  const post = getSelectedContentPost();
  if (!post) {
    setContentStudioStatus("Select a post first.");
    return;
  }
  try {
    await navigator.clipboard.writeText(buildCanvaHandoffText({
      ...post,
      hook: String(document.getElementById("contentEditHook")?.value || post.hook || "").trim(),
      caption: String(document.getElementById("contentEditCaption")?.value || post.caption || "").trim(),
      reel_script: String(document.getElementById("contentEditReelScript")?.value || post.reel_script || "").trim(),
      visual_prompt: String(document.getElementById("contentEditVisualPrompt")?.value || post.visual_prompt || "").trim(),
      canva_design_link: String(document.getElementById("contentEditCanvaLink")?.value || post.canva_design_link || "").trim(),
      cta: String(document.getElementById("contentEditCta")?.value || post.cta || "").trim(),
    }));
    setContentStudioStatus("Canva handoff copied to clipboard.");
  } catch {
    setContentStudioStatus("Could not copy Canva handoff (clipboard permission blocked).");
  }
}

function hasPlaceholderAssetFile() {
  const assetFile = String(document.getElementById("contentEditAssetFile")?.value || "").trim();
  return /FILE_ID/i.test(assetFile);
}

function hasRealAssetFile() {
  const assetFile = String(document.getElementById("contentEditAssetFile")?.value || "").trim();
  return Boolean(assetFile) && /^https?:\/\//i.test(assetFile) && !hasPlaceholderAssetFile();
}

function applyContentAssetPlaceholderGuard() {
  const blocked = !hasRealAssetFile();
  const tooltip = "Add the final public asset URL from Canva before approval or publishing";
  [
    "contentApproveBtn",
    "contentScheduleBtn",
    "contentStepApproveBtn",
    "contentStepScheduleBtn",
    "contentStepPublishBtn",
    "contentRunPublishTopBtn",
  ].forEach((id) => {
    const button = document.getElementById(id);
    if (!button) return;
    button.setAttribute("aria-disabled", blocked ? "true" : "false");
    if (blocked) {
      button.title = tooltip;
      button.setAttribute("data-disabled-reason", tooltip);
    } else {
      button.disabled = false;
      button.removeAttribute("title");
      button.removeAttribute("data-disabled-reason");
    }
  });
}

function renderVibeLibrary() {
  const list = document.getElementById("vibeLibraryList");
  if (!list) return;
  list.innerHTML = CONTENT_VIBE_PRESETS.map(
    (preset, index) => `
      <article class="vibe-card">
        <h4>${escapeHtml(preset.label)}</h4>
        <p class="vibe-text">${escapeHtml(preset.text)}</p>
        <button class="ghost-button slim" type="button" data-vibe-copy="${index}">[Copy]</button>
      </article>
    `,
  ).join("");
}

async function copyVibePresetToClipboard(index) {
  const preset = CONTENT_VIBE_PRESETS[index];
  if (!preset) return;
  try {
    await navigator.clipboard.writeText(String(preset.text || "").trim());
    setContentStudioStatus(`Copied "${preset.label}" preset.`);
  } catch {
    setContentStudioStatus("Could not copy vibe preset (clipboard permission blocked).");
  }
}

function evaluateContentPostQuality(current) {
  const notes = [];
  let score = 50;
  const hook = String(current.hook || "").trim();
  const caption = String(current.caption || "").trim();
  const cta = String(current.cta || "").trim();
  const hashtags = String(current.hashtags_text || "").trim();
  if (hook.length >= 35 && hook.length <= 90) {
    score += 12;
  } else {
    notes.push("Hook length is outside the ideal 35-90 character range.");
  }
  if (/\b(#1|mistake|wrong|before|stop|truth|myth|save)\b/i.test(hook)) {
    score += 10;
  } else {
    notes.push("Hook can be stronger with a clear pattern-break word (mistake, truth, before, stop).");
  }
  if (/\?/g.test(hook)) {
    score += 6;
  } else {
    notes.push("Consider a question-style hook to improve early engagement.");
  }
  if (caption.length >= 140) {
    score += 8;
  } else {
    notes.push("Caption is short; add 1-2 value bullets for more saves and shares.");
  }
  if (/insuredbylena\.com/i.test(cta) || /insuredbylena\.com/i.test(caption)) {
    score += 8;
  } else {
    notes.push("Include insuredbylena.com in CTA or caption.");
  }
  if (/guide/i.test(cta) || /comment\s+\"?guide\"?/i.test(caption)) {
    score += 10;
  } else {
    notes.push('Add comment prompt: Comment "GUIDE" for checklist DM.');
  }
  const hashtagCount = hashtags ? hashtags.split(/\s+/).filter(Boolean).length : 0;
  if (hashtagCount >= 4 && hashtagCount <= 12) {
    score += 6;
  } else {
    notes.push("Use 4-12 focused hashtags.");
  }
  if (current.platforms.length >= 2) {
    score += 5;
  } else {
    notes.push("Cross-post to at least 2 platforms early on.");
  }
  score = Math.max(0, Math.min(100, score));
  const label = score >= 85 ? "Strong" : score >= 70 ? "Good" : score >= 55 ? "Fair" : "Needs work";
  if (!notes.length) notes.push("Ready for approval and scheduling.");
  return { score, label, notes };
}

function getContentApprovalValidationIssues() {
  const hook = String(document.getElementById("contentEditHook")?.value || "").trim();
  const caption = String(document.getElementById("contentEditCaption")?.value || "").trim();
  const cta = String(document.getElementById("contentEditCta")?.value || "").trim();
  const combined = `${hook}\n${caption}\n${cta}`;
  const issues = [];
  if (!/insuredbylena\.com/i.test(combined)) {
    issues.push("Include insuredbylena.com in the caption or CTA.");
  }
  if (!/comment\s+\"?guide\"?/i.test(combined) && !/\bguide\b/i.test(cta)) {
    issues.push('Add a comment prompt: Comment "GUIDE" for DM checklist.');
  }
  const hasSavingsClaim =
    /\$[\d,]+/i.test(combined) ||
    /%\s*off/i.test(combined) ||
    /\blower premium\b/i.test(combined) ||
    /\brate change\b/i.test(combined) ||
    /\bsaved?\s+\$[\d,]+/i.test(combined) ||
    /\bqualify for lower/i.test(combined);
  const hasDisclaimer = /\b(results vary|not guaranteed|depends on eligibility|based on underwriting)\b/i.test(combined);
  if (hasSavingsClaim && !hasDisclaimer) {
    issues.push("Savings or rate claims need a disclaimer (e.g., results vary based on eligibility/underwriting).");
  }
  return issues;
}

function generateGrowthHookIdeas(topic, existingHook) {
  const cleanTopic = String(topic || "").trim() || "insurance";
  const base = String(existingHook || "").trim() || cleanTopic;
  return [
    `Most people get this wrong about ${cleanTopic}.`,
    `Before you buy ${cleanTopic}, read this first.`,
    `The #1 mistake I see with ${cleanTopic}.`,
    `If you only check one thing in ${cleanTopic}, check this.`,
    `This ${cleanTopic} tip can save you expensive mistakes.`,
    `What no one tells you about ${cleanTopic} until it is too late.`,
    `Stop scrolling if your family depends on ${cleanTopic}.`,
    `Quick reality check: ${base.replace(/[.!?]+$/, "")}.`,
  ];
}

function getSelectedContentPost() {
  const selectedId = String(state.ui.selectedContentPostId || "");
  if (!selectedId) return null;
  return state.contentPosts.find((post) => String(post.id) === selectedId) || null;
}

function getContentPublishFlowState() {
  const post = getSelectedContentPost();
  if (!post) {
    return {
      post: null,
      currentStatus: "",
      saved: false,
      approved: false,
      scheduled: false,
      dueNow: false,
      hasAsset: false,
      issues: [],
      nextStep: "Select a post",
    };
  }
  const currentStatus = String(post.status || "").trim().toLowerCase();
  const draftOverride = getContentDraftOverride(post.id);
  const assetValue = String(document.getElementById("contentEditAssetFile")?.value || post.asset_filename || "").trim();
  const scheduledValue = String(
    document.getElementById("contentEditScheduledFor")?.value || draftOverride.scheduled_for || post.scheduled_for || "",
  ).trim();
  const saved = true;
  const approved = currentStatus === "approved" || currentStatus === "scheduled" || currentStatus === "published";
  const hasSchedule = Boolean(scheduledValue);
  const scheduled = currentStatus === "scheduled" || currentStatus === "published" || hasSchedule;
  const hasAsset = Boolean(assetValue) && /^https?:\/\//i.test(assetValue) && !/FILE_ID_/i.test(assetValue);
  const dueNow = scheduledValue ? (() => {
    try {
      const candidate = new Date(scheduledValue);
      return !Number.isNaN(candidate.getTime()) && candidate.getTime() <= Date.now();
    } catch {
      return false;
    }
  })() : currentStatus === "approved";
  const issues = [];
  if (!hasAsset) {
    issues.push(assetValue ? `Final asset URL is invalid: ${assetValue}` : "Paste the final public Canva export URL in Final Asset URL.");
  }
  if (!approved) issues.push("Click Approve first.");
  if (!hasSchedule) issues.push("Set Schedule At before sending this post to Buffer.");
  let nextStep = "Run Publish";
  if (!approved) nextStep = "Approve";
  else if (!hasSchedule) nextStep = "Set Schedule At";
  else if (!hasAsset) nextStep = "Paste final Canva asset URL";
  return { post, currentStatus, saved, approved, scheduled, hasSchedule, dueNow, hasAsset, issues, nextStep };
}

function renderContentPublishGuide() {
  try {
  const summary = document.getElementById("contentPublishGuideSummary");
  const checklist = document.getElementById("contentPublishChecklist");
  if (!summary || !checklist) return;
  const flow = getContentPublishFlowState();
  if (!flow.post) {
    summary.textContent = "Select a post to see the publish sequence.";
    checklist.innerHTML = "";
    return;
  }
  const steps = [
    {
      label: "Step 1",
      title: "Save",
      done: flow.saved,
      active: !flow.approved,
      note: "Update the post plan and save the latest copy/layout notes.",
    },
    {
      label: "Step 2",
      title: "Approve",
      done: flow.approved,
      active: flow.saved && !flow.approved,
      note: flow.approved ? `Current status: ${flow.currentStatus}` : "Approve only after the Canva export URL is pasted back in.",
    },
    {
      label: "Step 3",
      title: "Schedule",
      done: flow.hasSchedule,
      active: flow.approved && !flow.hasSchedule,
      note: flow.hasSchedule
        ? "Schedule time saved. Buffer will use this time when you run publish."
        : "Set Schedule At so Buffer knows when this post should publish.",
    },
    {
      label: "Step 4",
      title: "Run Publish",
      done: flow.currentStatus === "published",
      active: flow.approved && flow.hasSchedule && flow.hasAsset,
      note: flow.hasAsset ? "Sends approved posts with saved schedule times to Buffer." : "Blocked until the final Canva asset URL is real.",
    },
  ];
  summary.textContent = flow.issues.length
    ? `Next step: ${flow.nextStep}. ${flow.issues.join(" ")}`
    : `Ready: ${flow.post.post_id} has a final asset and can be published now.`;
  checklist.innerHTML = steps
    .map((step) => {
      const stateClass = step.done ? "done" : step.active ? "active" : "pending";
      return `
        <div class="content-publish-step ${stateClass}">
          <span class="content-publish-step-label">${escapeHtml(step.label)}</span>
          <span class="content-publish-step-title">${escapeHtml(step.title)}</span>
          <span class="content-publish-step-note">${escapeHtml(step.note)}</span>
        </div>
      `;
    })
    .join("");
  } catch (error) {
    console.error("Content publish guide render failed:", error);
  }
}

function renderContentRequiredChecklist() {
  const summary = document.getElementById("contentRequiredSummary");
  const checklist = document.getElementById("contentRequiredChecklist");
  if (!summary || !checklist) return;
  const flow = getContentPublishFlowState();
  if (!flow.post) {
    summary.textContent = "Select a post to see exactly what is still needed before publish.";
    checklist.innerHTML = "";
    return;
  }
  const canvaValue = String(document.getElementById("contentEditCanvaLink")?.value || flow.post.canva_design_link || "").trim();
  const assetValue = String(document.getElementById("contentEditAssetFile")?.value || flow.post.asset_filename || "").trim();
  const scheduleValue = String(document.getElementById("contentEditScheduledFor")?.value || flow.post.scheduled_for || "").trim();
  const items = [
    {
      label: "Canva design linked",
      done: Boolean(canvaValue),
      note: canvaValue ? "Design link saved for reopen and handoff." : "Paste the working Canva design link so revisions are easy to reopen.",
    },
    {
      label: "Final asset URL ready",
      done: flow.hasAsset,
      note: flow.hasAsset ? "Public export URL is ready for approval and publishing." : "Paste the public Canva export URL, not a placeholder file ID.",
    },
    {
      label: "Approved in studio",
      done: flow.approved,
      note: flow.approved ? `Status is ${flow.currentStatus}.` : "Save edits, then click Approve when the asset is real.",
    },
    {
      label: "Scheduled to publish",
      done: flow.hasSchedule,
      note: flow.hasSchedule ? "Schedule time is saved and ready to send to Buffer." : (scheduleValue ? "Save or approve the post to keep this schedule time." : "Set Schedule At before sending this post to Buffer."),
    },
  ];
  const remaining = items.filter((item) => !item.done).length;
  summary.textContent = remaining
    ? `${remaining} item${remaining === 1 ? "" : "s"} still blocking publish for ${flow.post.post_id}.`
    : `${flow.post.post_id} is ready for publish.`;
  checklist.innerHTML = items
    .map(
      (item) => `
        <div class="content-required-item ${item.done ? "done" : "todo"}">
          <strong>${escapeHtml(item.done ? `Done: ${item.label}` : `Needs attention: ${item.label}`)}</strong>
          <span>${escapeHtml(item.note)}</span>
        </div>
      `,
    )
    .join("");
}

function getFilteredContentPosts() {
  const search = String(state.ui.contentPostSearch || "").trim().toLowerCase();
  const weekFilter = String(state.ui.contentPostWeekFilter || "").trim();
  const platformFilter = String(state.ui.contentPostPlatformFilter || "").trim().toLowerCase();
  const statusFilter = String(state.ui.contentPostStatusFilter || "").trim().toLowerCase();
  return (state.contentPosts || []).filter((post) => {
    if (weekFilter && String(post.week_number || "") !== weekFilter) return false;
    if (platformFilter && !(post.platforms || []).some((p) => String(p || "").toLowerCase() === platformFilter)) return false;
    if (statusFilter && String(post.status || "").toLowerCase() !== statusFilter) return false;
    if (!search) return true;
    const haystack = [
      post.post_id,
      post.topic,
      post.hook,
      post.caption,
      (post.platforms || []).join(","),
      post.post_date,
      post.post_time,
    ]
      .map((value) => String(value || "").toLowerCase())
      .join("\n");
    return haystack.includes(search);
  });
}

function populateContentQuickPick() {
  const select = document.getElementById("contentQuickPick");
  if (!select) return;
  const posts = getFilteredContentPosts();
  const selectedId = String(state.ui.selectedContentPostId || "");
  if (!posts.length) {
    select.innerHTML = '<option value="">No posts available. Refresh or import buffer-import.json.</option>';
    select.value = "";
    return;
  }
  select.innerHTML = posts
    .map((post) => {
      const label = [
        `${post.post_id || `#${post.id}`} (#${post.id})`,
        post.post_date || "-",
        (post.platforms || []).join(","),
        post.status || "draft",
        post.topic || "",
      ]
        .filter(Boolean)
        .join(" · ");
      return `<option value="${escapeHtml(String(post.id))}">${escapeHtml(label)}</option>`;
    })
    .join("");
  const hasSelected = posts.some((post) => String(post.id) === selectedId);
  select.value = hasSelected ? selectedId : String(posts[0].id);
  if (!hasSelected) {
    state.ui.selectedContentPostId = String(posts[0].id);
  }
}

function selectContentPost(postId) {
  const normalized = String(postId || "").trim();
  if (!normalized) return;
  state.ui.selectedContentPostId = normalized;
  renderContentPostTable();
  renderContentEditor();
  populateContentQuickPick();
  loadContentRevisionsOnly().catch(() => {});
  setContentStudioStatus(`Selected ${document.getElementById("contentEditPostId")?.value || "post"}.`);
}

function jumpToSelectedContentEditor() {
  const editorPanel = document.getElementById("contentEditorPanel");
  if (editorPanel instanceof HTMLElement) {
    editorPanel.scrollIntoView({ behavior: "smooth", block: "start" });
  }
  window.setTimeout(() => {
    const topicInput = document.getElementById("contentEditTopic");
    if (topicInput instanceof HTMLElement && typeof topicInput.focus === "function") {
      topicInput.focus({ preventScroll: true });
    }
  }, 120);
}

window.__contentEditPost = (postId) => {
  try {
    selectContentPost(postId);
    jumpToSelectedContentEditor();
  } catch (error) {
    console.error("Direct content edit failed:", error);
    setContentStudioStatus(`Selection issue: ${String(error.message || error)}`);
  }
};

window.__contentSelectPost = (postId) => {
  try {
    selectContentPost(postId);
    jumpToSelectedContentEditor();
  } catch (error) {
    console.error("Direct content select failed:", error);
    setContentStudioStatus(`Selection issue: ${String(error.message || error)}`);
  }
};

window.__contentSaveDraft = () => {
  saveSelectedContentDraft().catch((error) => {
    console.error("Direct content save failed:", error);
    setContentStudioStatus(String(error.message || error));
  });
};

window.__contentApprove = () => {
  runContentAction("approve").catch((error) => {
    console.error("Direct content approve failed:", error);
    setContentStudioStatus(String(error.message || error));
  });
};

window.__contentSchedule = () => {
  runContentAction("schedule").catch((error) => {
    console.error("Direct content schedule failed:", error);
    setContentStudioStatus(String(error.message || error));
  });
};

window.__contentRunPublish = () => {
  runContentPublish().catch((error) => {
    console.error("Direct content publish failed:", error);
    setContentStudioStatus(String(error.message || error));
  });
};

function classifyContentPillar(post) {
  const explicit = String(
    post?.pillar || post?.content_pillar || post?.pillar_tag || post?.category || post?.tag || "",
  )
    .trim()
    .toLowerCase();
  if (/(educational|education|tip|myth|faq)/i.test(explicit)) return "Educational";
  if (/(direct offer|offer|promo|promotion|quote)/i.test(explicit)) return "Direct Offer";

  const combined = [
    post?.topic,
    post?.hook,
    post?.caption,
    post?.cta,
    post?.post_type,
    post?.status,
  ]
    .map((value) => String(value || "").toLowerCase())
    .join(" ");

  const directOfferSignals = [
    /\bdm\b/,
    /\bcomment\b/,
    /\bquote\b/,
    /\bfree (review|checkup|consult|quote)\b/,
    /\bbook\b/,
    /\bapply\b/,
    /\bsign up\b/,
    /\blink in bio\b/,
    /\bvisit\b/,
    /\bcall\b/,
  ];
  if (directOfferSignals.some((pattern) => pattern.test(combined))) return "Direct Offer";
  return "Educational";
}

function renderContentPillarDistribution(posts) {
  const canvas = document.getElementById("contentPillarDistributionChart");
  const summaryEl = document.getElementById("contentPillarDistributionSummary");
  if (!canvas || !summaryEl) return;

  const total = posts.length;
  const counts = posts.reduce(
    (acc, post) => {
      const pillar = classifyContentPillar(post);
      if (pillar === "Direct Offer") acc.directOffer += 1;
      else acc.educational += 1;
      return acc;
    },
    { educational: 0, directOffer: 0 },
  );

  if (!total) {
    summaryEl.textContent = "No filtered posts to analyze.";
    if (opsCharts.contentPillar) {
      opsCharts.contentPillar.destroy();
      opsCharts.contentPillar = null;
    }
    return;
  }

  const eduPct = Math.round((counts.educational / total) * 100);
  const offerPct = Math.round((counts.directOffer / total) * 100);
  summaryEl.textContent = `Educational: ${eduPct}% (${counts.educational}/${total}) • Direct Offer: ${offerPct}% (${counts.directOffer}/${total})`;

  if (typeof Chart === "undefined") return;
  if (opsCharts.contentPillar) opsCharts.contentPillar.destroy();
  opsCharts.contentPillar = new Chart(canvas, {
    type: "pie",
    data: {
      labels: ["Educational", "Direct Offer"],
      datasets: [
        {
          data: [counts.educational, counts.directOffer],
          backgroundColor: ["rgba(113, 216, 255, 0.72)", "rgba(255, 138, 138, 0.72)"],
          borderColor: ["rgba(113, 216, 255, 0.95)", "rgba(255, 138, 138, 0.95)"],
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            color: "#dbe9fb",
            boxWidth: 10,
            boxHeight: 10,
            font: { size: 11 },
          },
        },
      },
    },
  });
}

function renderContentPostTable() {
  const tbody = document.getElementById("contentPostTable");
  const selectAll = document.getElementById("contentSelectAll");
  if (!tbody) return;
  const filteredPosts = getFilteredContentPosts();
  populateContentQuickPick();
  renderContentPillarDistribution(filteredPosts);
  if (!filteredPosts.length) {
    const emptyMessage = state.contentPosts.length
      ? "No posts match the current filters. Clear search or switch week, platform, or status."
      : "No content posts loaded yet. Start the local API, then refresh or import buffer-import.json.";
    tbody.innerHTML = `<tr><td colspan="7" class="muted">${escapeHtml(emptyMessage)}</td></tr>`;
    if (selectAll) selectAll.checked = false;
    return;
  }
  const selectedIds = new Set((state.ui.contentSelectedPostIds || []).map((id) => String(id)));
  tbody.innerHTML = filteredPosts
    .map((post) => {
      const isSelected = String(post.id) === String(state.ui.selectedContentPostId || "");
      const isChecked = selectedIds.has(String(post.id));
      const scheduleText = post.scheduled_for || `${post.post_date || "-"} ${post.post_time || "-"}`.trim();
      const normalizedStatus = String(post.status || "draft").trim().toLowerCase();
      const statusClassMap = {
        draft: "content-status-draft",
        scheduled: "content-status-scheduled",
        published: "content-status-published",
        failed: "content-status-failed",
      };
      const statusClass = statusClassMap[normalizedStatus] || "content-status-draft";
      const statusLabel = String(post.status || "draft").trim() || "draft";
      return `
        <tr data-content-post-id="${escapeHtml(post.id)}" ${isSelected ? 'class="row-selected"' : ""} onclick="window.__contentSelectPost && window.__contentSelectPost('${escapeHtml(String(post.id))}')">
          <td><input type="checkbox" data-content-check="${escapeHtml(post.id)}" ${isChecked ? "checked" : ""} onclick="event.stopPropagation()" /></td>
          <td>${escapeHtml(post.post_id || `#${post.id}`)}</td>
          <td><span class="content-status-chip ${statusClass}">${escapeHtml(statusLabel)}</span></td>
          <td>${escapeHtml(scheduleText || "-")}</td>
          <td>${escapeHtml((post.platforms || []).join(", "))}</td>
          <td>${escapeHtml(post.topic || "-")}</td>
          <td><button class="ghost-button slim content-row-action" type="button" title="Edit ${escapeHtml(post.post_id || `#${post.id}`)}" data-content-edit="${escapeHtml(post.id)}" onclick="event.stopPropagation(); window.__contentEditPost && window.__contentEditPost('${escapeHtml(String(post.id))}')">Edit</button></td>
        </tr>
      `;
    })
    .join("");
  if (selectAll) {
    const allChecked = filteredPosts.length > 0 && filteredPosts.every((post) => selectedIds.has(String(post.id)));
    selectAll.checked = allChecked;
  }
}

function renderContentPublishJobs() {
  const tbody = document.getElementById("contentPublishJobTable");
  if (!tbody) return;
  if (!state.contentPublishJobs.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="muted">No publish jobs yet.</td></tr>';
    return;
  }
  tbody.innerHTML = state.contentPublishJobs
    .map(
      (job) => `
        <tr>
          <td>${escapeHtml(job.run_at || "-")}</td>
          <td>${escapeHtml(job.post_id || "-")}</td>
          <td><span class="content-job-status content-job-status-${escapeHtml(job.status || "unknown")}">${escapeHtml(job.status || "-")}</span></td>
          <td><span class="content-job-error">${escapeHtml(job.error_message || "-")}</span></td>
        </tr>
      `,
    )
    .join("");
}

function renderContentRevisions() {
  const tbody = document.getElementById("contentRevisionTable");
  if (!tbody) return;
  if (!state.contentRevisions.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="muted">No revisions for selected post.</td></tr>';
    return;
  }
  tbody.innerHTML = state.contentRevisions
    .map(
      (revision) => `
        <tr>
          <td>${escapeHtml(String(revision.revision_number || revision.id || "-"))}</td>
          <td>${escapeHtml(revision.changed_by || "-")}</td>
          <td>${escapeHtml(revision.change_note || "-")}</td>
          <td>${escapeHtml(revision.created_at || "-")}</td>
          <td><button class="ghost-button slim" type="button" data-content-restore="${escapeHtml(String(revision.id || ""))}">Restore</button></td>
        </tr>
      `,
    )
    .join("");
}

function renderContentEditor() {
  const post = getSelectedContentPost();
  const emptyState = document.getElementById("contentEditorEmptyState");
  const assetHint = document.getElementById("contentAssetHint");
  const canvaHint = document.getElementById("contentCanvaHint");
  const setValue = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.value = value || "";
  };
  if (!post) {
    setValue("contentEditPostId", "");
    setValue("contentEditStatus", "");
    setValue("contentEditDate", "");
    setValue("contentEditTime", "");
    setValue("contentEditPlatforms", "");
    setValue("contentEditType", "");
    setValue("contentEditTopic", "");
    setValue("contentEditHook", "");
    setValue("contentEditCaption", "");
    setValue("contentEditReelScript", "");
    setValue("contentEditVisualPrompt", "");
    setValue("contentEditCanvaLink", "");
    setValue("contentEditAssetFile", "");
    setValue("contentEditCta", "");
    setValue("contentEditHashtags", "");
    setValue("contentEditScheduledFor", "");
    setValue("contentHookIdeas", "");
    if (emptyState) emptyState.textContent = "Pick a post from Quick Pick or the table to load its working fields here.";
    if (assetHint) assetHint.textContent = "Add the final public export URL here before approval or publishing.";
    if (canvaHint) canvaHint.textContent = "Add the Canva design link here so the next editor can reopen the working file fast.";
    renderContentPreview();
    renderContentPublishGuide();
    renderContentRequiredChecklist();
    applyContentAssetPlaceholderGuard();
    return;
  }
  const draftOverride = getContentDraftOverride(post.id);
  setValue("contentEditPostId", post.post_id || String(post.id));
  setValue("contentEditStatus", post.status || "");
  setValue("contentEditDate", post.post_date || "");
  setValue("contentEditTime", post.post_time || "");
  setValue("contentEditPlatforms", (post.platforms || []).join(","));
  setValue("contentEditType", post.post_type || "");
  setValue("contentEditTopic", post.topic || "");
  setValue("contentEditHook", post.hook || "");
  setValue("contentEditCaption", post.caption || "");
  setValue("contentEditReelScript", post.reel_script || "");
  setValue("contentEditVisualPrompt", post.visual_prompt || "");
  setValue("contentEditCanvaLink", post.canva_design_link || "");
  setValue("contentEditAssetFile", post.asset_filename || "");
  setValue("contentEditCta", post.cta || "");
  setValue("contentEditHashtags", post.hashtags_text || (post.hashtags || []).join(" "));
  const normalizedSchedule = String(draftOverride.scheduled_for || post.scheduled_for || "").replace("Z", "").slice(0, 16);
  setValue("contentEditScheduledFor", normalizedSchedule);
  const hookIdeas = generateGrowthHookIdeas(post.topic || "", post.hook || "");
  setValue("contentHookIdeas", hookIdeas.join("\n"));
  const canvaValue = String(post.canva_design_link || "").trim();
  const assetValue = String(post.asset_filename || "").trim();
  if (emptyState) emptyState.textContent = `Editing ${post.post_id || `#${post.id}`}. Save the copy here, finish layout in Canva, then paste back the final asset URL.`;
  if (assetHint) {
    assetHint.textContent = assetValue
      ? (/^https?:\/\//i.test(assetValue) && !/FILE_ID_/i.test(assetValue)
        ? "Final asset URL looks publish-ready."
        : "Current asset value still needs a real public URL before approval or publishing.")
      : "Add the final public export URL here before approval or publishing.";
  }
  if (canvaHint) {
    canvaHint.textContent = canvaValue
      ? "Canva design link saved for quick reopen."
      : "Add the Canva design link here so the next editor can reopen the working file fast.";
  }
  renderContentPreview();
  renderContentPublishGuide();
  renderContentRequiredChecklist();
  applyContentAssetPlaceholderGuard();
}

async function fetchContentPosts() {
  const response = await apiFetch(LOCAL_DB_CONTENT_POSTS_URL);
  if (!response.ok) throw new Error(`Content posts unavailable (${response.status})`);
  const data = await response.json();
  if (!data.ok) throw new Error(data.error || "Could not load content posts");
  state.contentPosts = Array.isArray(data.items) ? data.items : [];
  const validIds = new Set(state.contentPosts.map((post) => String(post.id)));
  state.ui.contentSelectedPostIds = (state.ui.contentSelectedPostIds || []).filter((id) => validIds.has(String(id)));
  const hasSelection = state.contentPosts.some(
    (post) => String(post.id) === String(state.ui.selectedContentPostId || ""),
  );
  if ((!state.ui.selectedContentPostId || !hasSelection) && state.contentPosts.length) {
    state.ui.selectedContentPostId = String(state.contentPosts[0].id);
  }
}

async function fetchContentPublishJobs() {
  const response = await apiFetch(`${LOCAL_DB_CONTENT_PUBLISH_JOBS_URL}?limit=30`);
  if (!response.ok) throw new Error(`Publish jobs unavailable (${response.status})`);
  const data = await response.json();
  if (!data.ok) throw new Error(data.error || "Could not load publish jobs");
  state.contentPublishJobs = Array.isArray(data.items) ? data.items : [];
}

async function fetchContentRevisions() {
  const post = getSelectedContentPost();
  if (!post) {
    state.contentRevisions = [];
    return;
  }
  const response = await apiFetch(`${LOCAL_DB_CONTENT_POSTS_URL}/${encodeURIComponent(post.id)}/revisions`);
  if (!response.ok) throw new Error(`Revisions unavailable (${response.status})`);
  const data = await response.json();
  if (!data.ok) throw new Error(data.error || "Could not load revisions");
  state.contentRevisions = Array.isArray(data.items) ? data.items : [];
}

async function loadContentStudioData(statusMessage = "") {
  setContentStudioStatus("Loading...");
  let postsError = "";
  let jobsError = "";
  try {
    await fetchContentPosts();
  } catch (error) {
    postsError = String(error.message || error);
    state.contentPosts = [];
    state.ui.selectedContentPostId = "";
  }
  try {
    await fetchContentPublishJobs();
  } catch (error) {
    jobsError = String(error.message || error);
    state.contentPublishJobs = [];
  }
  renderContentPostTable();
  renderContentEditor();
  renderContentPublishJobs();
  try {
    await fetchContentRevisions();
  } catch (error) {
    state.contentRevisions = [];
  }
  renderContentRevisions();
  if (postsError && jobsError) {
    setContentStudioStatus(`Content load issue: ${postsError}; Jobs load issue: ${jobsError}`);
    return;
  }
  if (postsError) {
    setContentStudioStatus(`Content load issue: ${postsError}`);
    return;
  }
  if (jobsError) {
    setContentStudioStatus(`Loaded ${state.contentPosts.length} posts (jobs unavailable: ${jobsError})`);
    return;
  }
  if (statusMessage) {
    setContentStudioStatus(statusMessage);
    return;
  }
  setContentStudioStatus(`Loaded ${state.contentPosts.length} posts`);
}

async function loadContentRevisionsOnly() {
  try {
    await fetchContentRevisions();
    renderContentRevisions();
  } catch (error) {
    state.contentRevisions = [];
    renderContentRevisions();
    setContentStudioStatus(String(error.message || error));
  }
}

async function saveSelectedContentDraft() {
  const post = getSelectedContentPost();
  if (!post) {
    setContentStudioStatus("Select a post first.");
    return;
  }
  setContentStudioStatus("Saving...");
  const payload = {
    actor: "editor",
    post_date: String(document.getElementById("contentEditDate")?.value || "").trim(),
    post_time: String(document.getElementById("contentEditTime")?.value || "").trim(),
    scheduled_for: String(document.getElementById("contentEditScheduledFor")?.value || "").trim(),
    platforms: String(document.getElementById("contentEditPlatforms")?.value || "")
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean),
    post_type: String(document.getElementById("contentEditType")?.value || "").trim(),
    topic: String(document.getElementById("contentEditTopic")?.value || "").trim(),
    hook: String(document.getElementById("contentEditHook")?.value || "").trim(),
    caption: String(document.getElementById("contentEditCaption")?.value || "").trim(),
    reel_script: String(document.getElementById("contentEditReelScript")?.value || "").trim(),
    visual_prompt: String(document.getElementById("contentEditVisualPrompt")?.value || "").trim(),
    canva_design_link: String(document.getElementById("contentEditCanvaLink")?.value || "").trim(),
    asset_filename: String(document.getElementById("contentEditAssetFile")?.value || "").trim(),
    cta: String(document.getElementById("contentEditCta")?.value || "").trim(),
    hashtags_text: String(document.getElementById("contentEditHashtags")?.value || "").trim(),
  };
  try {
    const response = await apiFetch(`${LOCAL_DB_CONTENT_POSTS_URL}/${encodeURIComponent(post.id)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || `Save failed (${response.status})`);
    clearContentDraftOverride(post.id);
    await loadContentStudioData("Draft saved.");
  } catch (error) {
    setContentStudioStatus(String(error.message || error));
  }
}

async function runContentAction(action) {
  const post = getSelectedContentPost();
  if (!post) {
    setContentStudioStatus("Select a post first.");
    return;
  }
  if (action === "approve" || action === "schedule") {
    if (hasPlaceholderAssetFile()) {
      setContentStudioStatus("Cannot run action: Update media link in media-links.csv first");
      window.alert("This post is blocked.\n\nReason: the Final Asset URL still uses a placeholder FILE_ID value.\n\nFinish the visual in Canva and paste a real public asset URL first.");
      return;
    }
    const issues = getContentApprovalValidationIssues();
    if (issues.length) {
      setContentStudioStatus(`Cannot ${action}: ${issues.join(" ")}`);
      window.alert(`Cannot ${action} this post yet.\n\n${issues.join("\n")}`);
      return;
    }
  }
  const note = action === "request-changes" ? "Needs edits before approval" : "";
  setContentStudioStatus(`Applying ${action}...`);
  try {
    const response = await apiFetch(
      `${LOCAL_DB_CONTENT_POSTS_URL}/${encodeURIComponent(post.id)}/${encodeURIComponent(action)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          actor: action === "approve" ? "approver" : "editor",
          note,
          scheduled_for: String(document.getElementById("contentEditScheduledFor")?.value || "").trim(),
        }),
      },
    );
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || `Action failed (${response.status})`);
    clearContentDraftOverride(post.id);
    const actionLabelMap = {
      approve: "Post approved. Schedule time saved.",
      schedule: "Post scheduled.",
      "request-changes": "Marked for changes.",
      "submit-review": "Submitted for review.",
    };
    await loadContentStudioData(actionLabelMap[action] || `Post ${action} complete.`);
  } catch (error) {
    setContentStudioStatus(String(error.message || error));
  }
}

async function runBulkContentAction(action) {
  const selectedIds = Array.from(new Set((state.ui.contentSelectedPostIds || []).map((id) => String(id).trim()).filter(Boolean)));
  if (!selectedIds.length) {
    setContentStudioStatus("Select one or more posts first.");
    return;
  }
  const actionLabel = action.replace("-", " ");
  setContentStudioStatus(`Running bulk ${actionLabel} on ${selectedIds.length} posts...`);
  let success = 0;
  let failed = 0;
  for (const postId of selectedIds) {
    try {
      const response = await apiFetch(
        `${LOCAL_DB_CONTENT_POSTS_URL}/${encodeURIComponent(postId)}/${encodeURIComponent(action)}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            actor: action === "approve" || action === "schedule" ? "approver" : "editor",
            note: action === "request-changes" ? "Bulk update: needs edits" : "",
          }),
        },
      );
      const data = await response.json();
      if (!response.ok || !data.ok) {
        failed += 1;
      } else {
        success += 1;
      }
    } catch {
      failed += 1;
    }
  }
  await loadContentStudioData(`Bulk ${actionLabel} complete. Success: ${success}, Failed: ${failed}.`);
}

async function restoreContentRevision(revisionId) {
  const post = getSelectedContentPost();
  if (!post) {
    setContentStudioStatus("Select a post first.");
    return;
  }
  if (!revisionId) return;
  setContentStudioStatus("Restoring revision...");
  try {
    const response = await apiFetch(
      `${LOCAL_DB_CONTENT_POSTS_URL}/${encodeURIComponent(post.id)}/restore`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          actor: "editor",
          revision_id: Number(revisionId),
        }),
      },
    );
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || `Restore failed (${response.status})`);
    await loadContentStudioData("Revision restored.");
  } catch (error) {
    setContentStudioStatus(String(error.message || error));
  }
}

async function runContentPublish() {
  const flow = getContentPublishFlowState();
  if (flow.post) {
    if (!flow.approved || !flow.hasSchedule || !flow.hasAsset) {
      const details = flow.issues.length ? `\n\n${flow.issues.join("\n")}` : "";
      window.alert(`This post is not ready to publish yet.\n\nSelected: ${flow.post.post_id}\nNext step: ${flow.nextStep}${details}`);
      return;
    }
    const confirmed = window.confirm(
      `Run publish now?\n\nSelected post: ${flow.post.post_id}\nThis will send approved posts with saved schedule times to Buffer, not just the selected row.`,
    );
    if (!confirmed) return;
  }
  setContentStudioStatus("Triggering publish...");
  try {
    const response = await apiFetch(LOCAL_DB_CONTENT_PUBLISH_RUN_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ limit: 25 }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || `Publish failed (${response.status})`);
    await loadContentStudioData(`Publish run complete (${Number(data.processed || 0)} processed).`);
  } catch (error) {
    setContentStudioStatus(String(error.message || error));
  }
}

async function importContentFromJsonFile(file) {
  if (!file) return;
  setContentStudioStatus("Importing...");
  try {
    const raw = await file.text();
    const parsed = JSON.parse(raw);
    const rawPosts = Array.isArray(parsed) ? parsed : parsed?.posts;
    const isBufferRow = (item) =>
      Boolean(
        item &&
          typeof item === "object" &&
          "day" in item &&
          "time" in item &&
          "platform" in item &&
          "caption" in item &&
          "media_url" in item,
      );
    const toContentPostsFromBufferRows = (rows) => {
      const validRows = rows.filter((row) => isBufferRow(row));
      const uniqueDates = [...new Set(validRows.map((row) => String(row.day || "").trim()).filter(Boolean))].sort();
      const baseDate = uniqueDates.length ? new Date(`${uniqueDates[0]}T00:00:00`) : null;
      const parseDate = (value) => new Date(`${String(value || "").trim()}T00:00:00`);
      const dayDiff = (a, b) => Math.floor((a.getTime() - b.getTime()) / (24 * 60 * 60 * 1000));
      const platformSlug = (platform) => {
        const p = String(platform || "").trim().toLowerCase();
        if (p === "instagram") return "ig";
        if (p === "facebook") return "fb";
        if (p === "tiktok") return "tt";
        return p.slice(0, 2) || "na";
      };
      const removeHashtagOnlyLines = (caption) => {
        const lines = String(caption || "").split(/\r?\n/);
        return lines
          .filter((line) => {
            const trimmed = line.trim();
            if (!trimmed) return true;
            const parts = trimmed.split(/\s+/).filter(Boolean);
            if (!parts.length) return true;
            const allTags = parts.every((part) => /^#[A-Za-z0-9_]+$/.test(part));
            return !allTags;
          })
          .join("\n")
          .trim();
      };
      const hashtagsText = (caption) => (String(caption || "").match(/#[A-Za-z0-9_]+/g) || []).join(" ");

      return validRows.map((row) => {
        const postDate = String(row.day || "").trim();
        const platform = String(row.platform || "").trim();
        let ordinalDay = 1;
        if (baseDate && postDate) {
          ordinalDay = dayDiff(parseDate(postDate), baseDate) + 1;
          if (ordinalDay < 1) ordinalDay = 1;
        }
        const weekNumberLocal = Math.floor((ordinalDay - 1) / 7) + 1;
        const dayInWeek = ((ordinalDay - 1) % 7) + 1;
        const captionRaw = String(row.caption || "").trim();
        const caption = removeHashtagOnlyLines(captionRaw);
        const hook = caption.split(/\r?\n/).find((line) => line.trim()) || "";

        return {
          post_id: `W${weekNumberLocal}D${dayInWeek}-${platformSlug(platform)}`,
          week_number: weekNumberLocal,
          day: dayInWeek,
          post_date: postDate,
          post_time: String(row.time || "").trim() || "09:00",
          platforms: [String(platform || "").toLowerCase()],
          post_type: String(platform || "").toLowerCase() === "tiktok" ? "reel" : "social",
          topic: hook.slice(0, 90),
          hook: hook.slice(0, 140),
          caption,
          reel_script: "",
          visual_prompt: "",
          asset_filename: String(row.media_url || "").trim(),
          cta: "Visit insuredbylena.com for a 100% free quote comparison. Comment GUIDE and I'll DM you the 2026 Insurance Planning Checklist.",
          hashtags_text: hashtagsText(captionRaw),
          status: "draft",
        };
      });
    };
    const posts = Array.isArray(rawPosts) && rawPosts.length && isBufferRow(rawPosts[0])
      ? toContentPostsFromBufferRows(rawPosts)
      : rawPosts;
    if (!Array.isArray(posts) || !posts.length) {
      throw new Error("JSON file must contain an array of posts.");
    }
    const weekMatch = String(file.name || "").match(/WEEK(\d+)/i);
    const weekNumber = weekMatch ? Number(weekMatch[1]) : 0;
    const response = await apiFetch(LOCAL_DB_CONTENT_POSTS_IMPORT_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        week_number: Number.isFinite(weekNumber) ? weekNumber : 0,
        source_file: file.name || "",
        actor: "editor",
        posts,
      }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || `Import failed (${response.status})`);
    await loadContentStudioData(`Imported ${Number(data.imported || 0)} new, updated ${Number(data.updated || 0)}.`);
  } catch (error) {
    setContentStudioStatus(String(error.message || error));
  }
}

async function importContentFromCurrentBufferFile() {
  setContentStudioStatus("Importing current buffer-import.json...");
  try {
    const response = await apiFetch(LOCAL_DB_CONTENT_POSTS_IMPORT_BUFFER_CURRENT_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        actor: "automation",
        source_file: "buffer-import.json",
      }),
    });
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || `Import failed (${response.status})`);
    await loadContentStudioData(`Imported ${Number(data.imported || 0)} new, updated ${Number(data.updated || 0)} from buffer-import.json.`);
  } catch (error) {
    setContentStudioStatus(String(error.message || error));
  }
}

function attachContentStudioHandlers() {
  renderVibeLibrary();
  document.getElementById("contentRefreshBtn")?.addEventListener("click", () => {
    loadContentStudioData().catch(() => {});
  });
  document.getElementById("contentRetryApiBtn")?.addEventListener("click", () => {
    loadContentStudioData().catch(() => {});
  });
  document.getElementById("contentImportBufferCurrentBtn")?.addEventListener("click", () => {
    importContentFromCurrentBufferFile().catch(() => {});
  });
  document.getElementById("contentRefreshRevisionsBtn")?.addEventListener("click", () => {
    loadContentRevisionsOnly().catch(() => {});
  });
  document.getElementById("contentBulkSubmitBtn")?.addEventListener("click", () => {
    runBulkContentAction("submit-review").catch(() => {});
  });
  document.getElementById("contentBulkApproveBtn")?.addEventListener("click", () => {
    runBulkContentAction("approve").catch(() => {});
  });
  document.getElementById("contentBulkScheduleBtn")?.addEventListener("click", () => {
    runBulkContentAction("schedule").catch(() => {});
  });
  document.getElementById("contentSaveBtn")?.addEventListener("click", () => {
    saveSelectedContentDraft().catch(() => {});
  });
  document.getElementById("contentSubmitReviewBtn")?.addEventListener("click", () => {
    runContentAction("submit-review").catch(() => {});
  });
  document.getElementById("contentApproveBtn")?.addEventListener("click", () => {
    runContentAction("approve").catch(() => {});
  });
  document.getElementById("contentRequestChangesBtn")?.addEventListener("click", () => {
    runContentAction("request-changes").catch(() => {});
  });
  document.getElementById("contentScheduleBtn")?.addEventListener("click", () => {
    runContentAction("schedule").catch(() => {});
  });
  document.getElementById("contentRunPublishBtn")?.addEventListener("click", () => {
    runContentPublish().catch(() => {});
  });
  document.getElementById("contentRunPublishTopBtn")?.addEventListener("click", () => {
    runContentPublish().catch(() => {});
  });
  document.getElementById("contentStepSaveBtn")?.addEventListener("click", () => {
    saveSelectedContentDraft().catch(() => {});
  });
  document.getElementById("contentStepApproveBtn")?.addEventListener("click", () => {
    runContentAction("approve").catch(() => {});
  });
  document.getElementById("contentStepScheduleBtn")?.addEventListener("click", () => {
    runContentAction("schedule").catch(() => {});
  });
  document.getElementById("contentStepPublishBtn")?.addEventListener("click", () => {
    runContentPublish().catch(() => {});
  });
  document.getElementById("contentCopyCanvaHandoffBtn")?.addEventListener("click", () => {
    copyCanvaHandoffToClipboard().catch(() => {});
  });
  document.getElementById("contentPreviewCopyCanvaBtn")?.addEventListener("click", () => {
    copyCanvaHandoffToClipboard().catch(() => {});
  });
  document.getElementById("contentOpenCanvaDesignBtn")?.addEventListener("click", () => {
    openSelectedCanvaDesign();
  });
  document.getElementById("contentOpenAssetBtn")?.addEventListener("click", () => {
    openSelectedContentAsset();
  });
  document.getElementById("contentPublishHelpBtn")?.addEventListener("click", () => {
    window.alert(
      "Publish sequence:\n\n1. Update copy and layout notes in Content Studio\n2. Copy Canva handoff and make the visual/text changes in Canva\n3. Paste the final exported asset URL back into Content Studio\n4. Approve\n5. Set Schedule At and click Schedule\n6. Run Publish\n\nRun Publish only processes posts that are approved or scheduled and already due.",
    );
  });
  document.getElementById("contentImportJsonFile")?.addEventListener("change", async (event) => {
    const input = event.target;
    const file = input?.files?.[0];
    await importContentFromJsonFile(file);
    if (input) input.value = "";
  });
  [
    "contentEditDate",
    "contentEditTime",
    "contentEditPlatforms",
    "contentEditType",
    "contentEditTopic",
    "contentEditHook",
    "contentEditCaption",
    "contentEditReelScript",
    "contentEditVisualPrompt",
    "contentEditCanvaLink",
    "contentEditAssetFile",
    "contentEditCta",
    "contentEditHashtags",
    "contentEditScheduledFor",
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("input", () => {
      if (id === "contentEditScheduledFor") {
        const postId = String(state.ui.selectedContentPostId || "").trim();
        if (postId) {
          setContentDraftOverride(postId, { scheduled_for: String(el.value || "").trim() });
        }
      }
      renderContentPreview();
      renderContentPublishGuide();
      renderContentRequiredChecklist();
      applyContentAssetPlaceholderGuard();
    });
    el.addEventListener("change", () => {
      if (id === "contentEditScheduledFor") {
        const postId = String(state.ui.selectedContentPostId || "").trim();
        if (postId) {
          setContentDraftOverride(postId, { scheduled_for: String(el.value || "").trim() });
        }
      }
      renderContentPreview();
      renderContentPublishGuide();
      renderContentRequiredChecklist();
      applyContentAssetPlaceholderGuard();
    });
  });
  document.getElementById("contentGenerateHooksBtn")?.addEventListener("click", () => {
    const topic = String(document.getElementById("contentEditTopic")?.value || "").trim();
    const hook = String(document.getElementById("contentEditHook")?.value || "").trim();
    const earlyGrowth = Boolean(document.getElementById("contentEarlyGrowthToggle")?.checked);
    const ideas = generateGrowthHookIdeas(topic, hook);
    const boosted = earlyGrowth ? ideas : ideas.slice(0, 3);
    const area = document.getElementById("contentHookIdeas");
    if (area) area.value = boosted.join("\n");
    setContentStudioStatus(`Generated ${boosted.length} hook ideas.`);
  });
  document.getElementById("contentApplyHookBtn")?.addEventListener("click", () => {
    const area = document.getElementById("contentHookIdeas");
    const hookInput = document.getElementById("contentEditHook");
    if (!area || !hookInput) return;
    const top = String(area.value || "")
      .split("\n")
      .map((line) => line.trim())
      .find(Boolean);
    if (!top) {
      setContentStudioStatus("Generate hook ideas first.");
      return;
    }
    hookInput.value = top;
    renderContentPreview();
    renderContentPublishGuide();
    setContentStudioStatus("Applied top hook suggestion.");
  });
  const setPreviewPlatform = (platform) => {
    state.ui.contentPreviewPlatform = platform;
    renderContentPreview();
  };
  document.getElementById("contentPreviewInstagramBtn")?.addEventListener("click", () => setPreviewPlatform("instagram"));
  document.getElementById("contentPreviewTiktokBtn")?.addEventListener("click", () => setPreviewPlatform("tiktok"));
  document.getElementById("contentPreviewFacebookBtn")?.addEventListener("click", () => setPreviewPlatform("facebook"));
  document.getElementById("contentPreviewOpenAssetBtn")?.addEventListener("click", () => openSelectedContentAsset());
  document.getElementById("contentPreviewCopyCaptionBtn")?.addEventListener("click", () => {
    copyPreviewCaptionToClipboard().catch(() => {});
  });
  document.getElementById("contentSelectAll")?.addEventListener("change", (event) => {
    const checked = Boolean(event.target?.checked);
    const filteredPosts = getFilteredContentPosts();
    state.ui.contentSelectedPostIds = checked ? filteredPosts.map((post) => String(post.id)) : [];
    renderContentPostTable();
  });
  [
    "contentPostSearch",
    "contentPostWeekFilter",
    "contentPostPlatformFilter",
    "contentPostStatusFilter",
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    const handler = () => {
      state.ui[id] = String(el.value || "");
      renderContentPostTable();
    };
    el.addEventListener("input", handler);
    el.addEventListener("change", handler);
  });
  document.getElementById("contentPostTable")?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const restoreBtn = target.closest("[data-content-restore]");
    if (restoreBtn) {
      const revisionId = restoreBtn.getAttribute("data-content-restore") || "";
      restoreContentRevision(revisionId).catch(() => {});
      return;
    }
    const check = target.closest("[data-content-check]");
    if (check) {
      const postId = String(check.getAttribute("data-content-check") || "");
      const selected = new Set((state.ui.contentSelectedPostIds || []).map((id) => String(id)));
      if (check.checked) selected.add(postId);
      else selected.delete(postId);
      state.ui.contentSelectedPostIds = Array.from(selected);
      renderContentPostTable();
      return;
    }
    const editButton = target.closest("[data-content-edit]");
    if (editButton) {
      event.preventDefault();
      event.stopPropagation();
      const selectedId = editButton.getAttribute("data-content-edit") || "";
      if (!selectedId) return;
      try {
        selectContentPost(selectedId);
        jumpToSelectedContentEditor();
      } catch (error) {
        console.error("Content post edit button failed:", error);
        setContentStudioStatus(`Selection issue: ${String(error.message || error)}`);
      }
      return;
    }
    const button = target.closest("[data-content-select]");
    const row = target.closest("[data-content-post-id]");
    const selectedId = button?.dataset.contentSelect || row?.getAttribute("data-content-post-id") || "";
    if (!selectedId) return;
    try {
      selectContentPost(selectedId);
      jumpToSelectedContentEditor();
    } catch (error) {
      console.error("Content post selection failed:", error);
      setContentStudioStatus(`Selection issue: ${String(error.message || error)}`);
    }
  });
  document.getElementById("contentQuickPick")?.addEventListener("change", (event) => {
    const value = String(event.target?.value || "").trim();
    if (!value) return;
    try {
      selectContentPost(value);
    } catch (error) {
      console.error("Content quick pick failed:", error);
      setContentStudioStatus(`Selection issue: ${String(error.message || error)}`);
    }
  });
  document.getElementById("contentRevisionTable")?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const restoreBtn = target.closest("[data-content-restore]");
    if (!restoreBtn) return;
    const revisionId = restoreBtn.getAttribute("data-content-restore") || "";
    restoreContentRevision(revisionId).catch(() => {});
  });
  document.getElementById("vibeLibraryList")?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const button = target.closest("[data-vibe-copy]");
    if (!button) return;
    const index = Number(button.getAttribute("data-vibe-copy"));
    if (!Number.isFinite(index)) return;
    copyVibePresetToClipboard(index).catch(() => {});
  });
}

function attachFilterHandlers() {
  const debouncedLeadSelectionRender = debounce(renderLeadSelectionTable, 100);

  ["searchInput", "queueFilter", "channelFilter", "priorityFilter"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("input", renderLeadTable);
    el.addEventListener("change", renderLeadTable);
  });

  ["leadSelectSearch", "leadSelectQueue", "leadSelectPriority", "leadSelectEligibility"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("input", debouncedLeadSelectionRender);
    el.addEventListener("change", debouncedLeadSelectionRender);
  });

  ["campaignSearch", "campaignQueue", "campaignPriority", "campaignEligibility", "campaignChannel", "campaignConsent", "campaignAngle", "campaignCta", "campaignSenderName"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("input", debounce(renderCampaignTable, 100));
    el.addEventListener("change", debounce(renderCampaignTable, 100));
  });

  ["sourcedSearchInput", "sourcedQueueFilter", "sourcedPlatformFilter", "sourcedStageFilter"].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("input", renderSourcedLeadTable);
    el.addEventListener("change", renderSourcedLeadTable);
  });
}

function attachTabHandlers() {
  document.querySelectorAll("[data-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      setActiveTab(button.dataset.tab);
    });
  });
}

function attachPipelineHandlers() {
  const board = document.getElementById("pipelineBoard");
  if (!board) return;

  board.addEventListener("dragstart", (event) => {
    const card = event.target.closest("[data-pipeline-lead]");
    if (!card) return;
    event.dataTransfer?.setData("text/plain", card.dataset.pipelineLead || "");
    event.dataTransfer.effectAllowed = "move";
  });

  board.addEventListener("dragover", (event) => {
    const zone = event.target.closest("[data-pipeline-dropzone]");
    if (!zone) return;
    event.preventDefault();
    event.dataTransfer.dropEffect = "move";
    zone.classList.add("pipeline-dropzone-active");
  });

  board.addEventListener("dragleave", (event) => {
    const zone = event.target.closest("[data-pipeline-dropzone]");
    if (!zone) return;
    zone.classList.remove("pipeline-dropzone-active");
  });

  board.addEventListener("drop", async (event) => {
    const zone = event.target.closest("[data-pipeline-dropzone]");
    if (!zone) return;
    event.preventDefault();
    zone.classList.remove("pipeline-dropzone-active");
    const rawLeadId = event.dataTransfer?.getData("text/plain") || "";
    const leadId = rawLeadId ? decodeURIComponent(rawLeadId) : "";
    if (!leadId) return;
    const stage = zone.dataset.pipelineDropzone || "app_submitted";
    await moveLeadToPipeline(leadId, stage);
  });
}

function setScriptDrawerOpen(open) {
  const drawer = document.getElementById("deskScriptDrawer");
  if (!drawer) return;
  drawer.hidden = !open;
}

function applyRoleView(role) {
  const normalized = role || "agent";
  document.body.dataset.roleView = normalized;
  document.querySelectorAll("[data-role-section]").forEach((section) => {
    const allowed = String(section.dataset.roleSection || "")
      .split(/\s+/)
      .filter(Boolean);
    section.hidden = allowed.length ? !allowed.includes(normalized) : false;
  });
}

function initRoleView() {
  const select = document.getElementById("roleViewSelect");
  if (!select) return;
  const saved = localStorage.getItem(ROLE_VIEW_STORAGE_KEY) || "agent";
  select.value = saved;
  applyRoleView(saved);
  select.addEventListener("change", (event) => {
    const value = String(event.target.value || "agent");
    localStorage.setItem(ROLE_VIEW_STORAGE_KEY, value);
    applyRoleView(value);
  });
}


function attachWorkflowHandlers() {
  const fields = [
    ["workflowGoal", "goal"],
    ["workflowAge", "age"],
    ["workflowTobacco", "tobacco"],
    ["workflowHealth", "health"],
    ["workflowBudget", "budget"],
    ["workflowDuration", "duration"],
    ["workflowSpeed", "speed"],
  ];

  fields.forEach(([id, key]) => {
    const element = document.getElementById(id);
    if (!element) return;
    element.addEventListener("change", (event) => {
      state.ui.workflowAnswers[key] = event.target.value;
      if (key === "age" || key === "health" || key === "tobacco") runRecommendationEffect();
      if (DESK_DISCOVERY_FIELD_IDS.has(id)) hideDeskScriptToast();
      syncWorkflowControls();
      renderWorkflowAdvisor();
    });
  });

  const deskFields = [
    ["deskProductPath", "productPath"],
    ["deskGoal", "goal"],
    ["deskAge", "age"],
    ["deskTobacco", "tobacco"],
    ["deskHealth", "health"],
    ["deskBudget", "budget"],
    ["deskDuration", "duration"],
    ["deskSpeed", "speed"],
    ["deskHealthCoverageType", "healthCoverageType"],
    ["deskHealthNeed", "healthNeed"],
    ["deskHealthPriority", "healthPriority"],
  ];

  deskFields.forEach(([id, key]) => {
    const element = document.getElementById(id);
    if (!element) return;
    element.addEventListener("change", (event) => {
      state.ui.workflowAnswers[key] = event.target.value;
      if (key === "age" || key === "health" || key === "tobacco") runRecommendationEffect();
      if (DESK_DISCOVERY_FIELD_IDS.has(id)) hideDeskScriptToast();
      if (key === "age") applyAgeToProductPath(event.target.value);
      syncWorkflowControls();
      renderCallDeskBranching();
      renderWorkflowAdvisor();
    });
  });

  [
    "deskNeedArea",
    "deskHealthGap",
    "deskLifeNeed",
    "deskProtectionLoad",
    "deskMedicareFlag",
    "deskLifeFeasibility",
  ].forEach((id) => {
    const element = document.getElementById(id);
    if (!element) return;
    element.addEventListener("change", (event) => {
      if (DESK_DISCOVERY_FIELD_IDS.has(id)) hideDeskScriptToast();
      if (id === "deskNeedArea") {
        const value = String(event.target.value || "");
        const mappedPath =
          value === "health" || value === "life" || value === "both" || value === "unclear"
            ? value
            : "";
        if (mappedPath) {
          state.ui.workflowAnswers.productPath = mappedPath;
          const productSelect = document.getElementById("deskProductPath");
          if (productSelect) productSelect.value = mappedPath;
        }
      }
      renderWorkflowAdvisor();
    });
  });
}

function updateLeadSelectionSortIndicators() {
  const headers = document.querySelectorAll("th[data-sort]");
  headers.forEach((th) => {
    const key = th.dataset.sort;
    if (key === state.ui.leadSelectionSort.key) {
      th.dataset.sortDir = state.ui.leadSelectionSort.dir;
    } else {
      delete th.dataset.sortDir;
    }
  });
}

function attachLeadSelectionSortHandlers() {
  const headers = document.querySelectorAll("th[data-sort]");
  headers.forEach((th) => {
    th.addEventListener("click", () => {
      const key = th.dataset.sort;
      if (!key) return;
      if (state.ui.leadSelectionSort.key === key) {
        state.ui.leadSelectionSort.dir = state.ui.leadSelectionSort.dir === "asc" ? "desc" : "asc";
      } else {
        state.ui.leadSelectionSort.key = key;
        state.ui.leadSelectionSort.dir = key === "priority" || key === "last_activity" ? "desc" : "asc";
      }
      updateLeadSelectionSortIndicators();
      renderLeadSelectionTable();
    });
  });
}

function buildCallDeskSummary() {
  const values = {
    Client: document.getElementById("deskClientName")?.value || "",
    Phone: document.getElementById("deskPhone")?.value || "",
    Product: document.getElementById("deskProductPath")?.value || "",
    Need: document.getElementById("deskGoalNote")?.value || "",
    "Need area": document.getElementById("deskNeedArea")?.value || "",
    "Current coverage": document.getElementById("deskCurrentCoverage")?.value || "",
    "Existing policy": document.getElementById("deskExistingPolicy")?.value || "",
    "Policy intent": document.getElementById("deskPolicyIntent")?.value || "",
    "Decision maker": document.getElementById("deskDecisionMaker")?.value || "",
    "Decision timeline": document.getElementById("deskDecisionTimeline")?.value || "",
    Coverage: document.getElementById("deskCoverage")?.value || "",
    Budget: document.getElementById("deskBudgetText")?.value || "",
    Objection: document.getElementById("deskObjection")?.value || "",
    Disposition: document.getElementById("deskDisposition")?.value || "",
    "Next step": document.getElementById("deskNextStep")?.value || "",
    "Follow-up": document.getElementById("deskFollowUp")?.value || "",
    "Sync via Openclaw GOG": document.getElementById("deskSyncGog")?.checked ? "Yes" : "No",
    Notes: document.getElementById("deskCallNotes")?.value || "",
  };

  return Object.entries(values)
    .filter(([, value]) => String(value).trim())
    .map(([key, value]) => `${key}: ${value}`)
    .join("\n");
}

function setDeskLeadPickerStatus(message) {
  const el = document.getElementById("deskLeadPickerStatus");
  if (el) el.textContent = message;
  const selectionEl = document.getElementById("leadSelectStatus");
  if (selectionEl) selectionEl.textContent = message;
}

function getLeadDisplayName(row) {
  const fullName = String(row.full_name || "").trim();
  if (fullName) return fullName;
  return `${row.first_name || ""} ${row.last_name || ""}`.trim() || "Unnamed lead";
}

function inferDeskProductPathFromLead(row) {
  const line = String(row.product_line || "").toLowerCase();
  const interest = String(row.product_interest || "").toLowerCase();
  if (line.includes("health") || interest.includes("health")) return "health";
  if (line.includes("life") || interest.includes("life")) return "life";
  return "";
}

function inferAgeBandFromLead(row) {
  const rawBand = String(row.age || "").trim().toLowerCase();
  if (["under50", "under 50", "<50"].includes(rawBand)) return "under50";
  if (["50to64", "50-64", "50 to 64"].includes(rawBand)) return "50to64";
  if (["65to75", "65-75", "65 to 75"].includes(rawBand)) return "65to75";
  if (["76plus", "76+", "76 plus"].includes(rawBand)) return "76plus";

  const rawAge = Number(String(row.age || row.client_age || "").trim());
  if (Number.isFinite(rawAge) && rawAge > 0) {
    if (rawAge < 50) return "under50";
    if (rawAge < 65) return "50to64";
    if (rawAge <= 75) return "65to75";
    return "76plus";
  }
  return "";
}

function applySavedPayloadToLeadState(payload) {
  if (!payload) return;
  const leadId = String(payload.contactId || "").trim();
  if (!leadId) return;
  const lead = state.leads.find((row) => String(row.lead_external_id || "").trim() === leadId);
  if (!lead) return;
  lead.first_name = String(payload.firstName || lead.first_name || "").trim();
  lead.last_name = String(payload.lastName || lead.last_name || "").trim();
  const mergedName = `${lead.first_name || ""} ${lead.last_name || ""}`.trim();
  if (mergedName) lead.full_name = mergedName;
  lead.mobile_phone = String(payload.phone || lead.mobile_phone || "").trim();
  lead.email = String(payload.email || lead.email || "").trim();
  lead.disposition = String(payload.disposition || lead.disposition || "").trim();
  lead.age = String(payload.age || lead.age || "").trim();
  lead.tobacco = String(payload.tobacco || lead.tobacco || "").trim();
  lead.health_posture = String(payload.healthPosture || lead.health_posture || "").trim();
  lead.carrier_match = String(payload.carrierMatch || lead.carrier_match || "").trim();
  lead.confidence = String(payload.confidence || lead.confidence || "").trim();
  lead.pipeline_status = String(payload.pipelineStatus || lead.pipeline_status || "").trim().toLowerCase();
  lead.calendar_event_id = String(payload.calendarEventId || lead.calendar_event_id || "").trim();
  lead.next_appointment_time = String(payload.nextAppointmentTime || lead.next_appointment_time || "").trim();
  lead.raw_tags = String(payload.tags || lead.raw_tags || "").trim();
  lead.notes = String(payload.notes || lead.notes || "").trim();
  lead.last_activity_at_source = new Date().toISOString();
  const created = state.createdLeads.find((row) => String(row.lead_external_id || "").trim() === leadId);
  if (created) {
    created.pipeline_status = lead.pipeline_status;
    saveCreatedLeads();
  }
  renderOpsCharts(state.leads);
  renderPipelineBoard();
}

function shouldIncludeInMainQueue(row) {
  const leadId = String(row.lead_external_id || "").trim();
  if (!leadId) return false;
  const channel = String(row.recommended_channel || "").trim().toLowerCase();
  const eligibility = String(row.contact_eligibility || "").trim().toLowerCase();
  const status = String(row.lead_status || "").trim().toLowerCase();
  if (channel !== "phone_call") return false;
  if (eligibility === "blocked") return false;
  if (["sold", "not_qualified", "not_interested", "completed"].includes(status)) return false;
  return true;
}

function buildMainCallQueue() {
  return state.leads
    .filter((row) => shouldIncludeInMainQueue(row))
    .sort((a, b) => {
      const aPriority = String(a.priority_tier || "").toLowerCase() === "high" ? 1 : 0;
      const bPriority = String(b.priority_tier || "").toLowerCase() === "high" ? 1 : 0;
      if (bPriority !== aPriority) return bPriority - aPriority;
      const aLast = Date.parse(a.last_activity_at_source || "") || 0;
      const bLast = Date.parse(b.last_activity_at_source || "") || 0;
      return bLast - aLast;
    })
    .map((row) => row.lead_external_id);
}

function syncMainCallQueue() {
  const selected = String(state.ui.selectedCallDeskLeadId || "");
  const queue = buildMainCallQueue().filter((id) => id !== selected);
  state.ui.mainCallQueue = queue;
  appStore.setState({
    currentLeadId: selected,
    mainCallQueue: queue,
  });
}

function popNextLeadFromMainQueue() {
  const queue = [...(state.ui.mainCallQueue || [])];
  while (queue.length) {
    const nextId = String(queue.shift() || "");
    const lead = state.leads.find((row) => row.lead_external_id === nextId);
    if (lead && shouldIncludeInMainQueue(lead)) {
      state.ui.mainCallQueue = queue;
      appStore.setState({ currentLeadId: nextId, mainCallQueue: queue });
      return nextId;
    }
  }
  state.ui.mainCallQueue = [];
  appStore.setState({ currentLeadId: "", mainCallQueue: [] });
  return "";
}

function startNextLeadFromQueue() {
  const nextId = popNextLeadFromMainQueue();
  if (!nextId) {
    setDeskLeadPickerStatus("Main call queue is empty.");
    document.getElementById("callDeskStatus").textContent = "Queue empty";
    return;
  }
  loadLeadIntoCallDesk(nextId);
  const remaining = state.ui.mainCallQueue.length;
  setDeskLeadPickerStatus(`Loaded next lead. ${remaining} lead${remaining === 1 ? "" : "s"} remaining in queue.`);
}

function updateLeadDispositionStatus(leadId, disposition) {
  if (!leadId) return;
  const lead = state.leads.find((row) => row.lead_external_id === leadId);
  if (!lead) return;
  const dispositionMap = {
    quoted: "quoted",
    follow_up: "follow_up",
    callback: "callback",
    no_answer: "no_answer",
    not_interested: "not_interested",
    not_qualified: "not_qualified",
    sold: "sold",
  };
  const normalized = dispositionMap[String(disposition || "").trim()] || "working";
  lead.lead_status = normalized;
  lead.disposition = normalized;
  renderOpsCharts(state.leads);
}

function pipelineStageLabel(stage) {
  if (stage === "app_submitted") return "App Submitted";
  if (stage === "underwriting") return "Underwriting";
  if (stage === "approved") return "Approved";
  if (stage === "issued") return "Issued";
  if (stage === "paid") return "Paid";
  return "App Submitted";
}

async function persistPipelineStatus(lead, stage) {
  if (!lead) return true;
  if (supabase) return updateLeadPipelineInSupabase(lead, stage);
  if (!LOCAL_DB_LEAD_BASE_URL.trim()) return true;
  const payload = {
    contactId: String(lead.lead_external_id || "").trim(),
    firstName: String(lead.first_name || "").trim(),
    lastName: String(lead.last_name || "").trim(),
    phone: String(lead.mobile_phone || "").trim(),
    email: String(lead.email || "").trim(),
    tags: String(lead.raw_tags || "").trim(),
    notes: String(lead.notes || "").trim(),
    lastActivity: new Date().toISOString(),
    age: String(lead.age || "").trim(),
    tobacco: String(lead.tobacco || "").trim(),
    healthPosture: String(lead.health_posture || "").trim(),
    disposition: String(lead.disposition || lead.lead_status || "").trim(),
    carrierMatch: String(lead.carrier_match || "").trim(),
    confidence: String(lead.confidence || "").trim(),
    pipelineStatus: stage,
  };
  const response = await fetch(`${LOCAL_DB_LEAD_BASE_URL}/${encodeURIComponent(payload.contactId)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) throw new Error(`Pipeline save failed (${response.status})`);
  return true;
}

function fetchLeads() {
  renderLeadSelectionTable();
  renderPipelineBoard();
  renderOpsCharts(state.leads);
  return state.leads;
}

async function handleLaunchPortal() {
  const writingNumber = String(document.getElementById("deskCarrierWritingNumberInput")?.value || "").trim();
  if (!writingNumber || writingNumber === "Agent Writing Number") {
    alert("CRITICAL ERROR: Please enter a valid Agent Writing Number.");
    return;
  }

  const missingReadiness = buildDeskReadinessRequirements().some((item) => !item.value);
  if (missingReadiness) {
    alert("Complete required discovery fields before launching the carrier portal.");
    return;
  }

  const leadId = String(state.ui.selectedCallDeskLeadId || state.ui.leadId || "").trim();
  const portalUrl = String(document.getElementById("deskLaunchCarrierPortalBtn")?.dataset.portalUrl || "").trim();
  if (!leadId || !portalUrl) return;

  try {
    if (supabase) {
      await updateLeadPipelineInSupabase({ lead_external_id: leadId }, "app_submitted");
    } else {
      const response = await fetch(`${LOCAL_DB_LEAD_BASE_URL}/${encodeURIComponent(leadId)}/pipeline`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "App Submitted" }),
      });
      if (!response.ok) throw new Error(`Pipeline update failed (${response.status})`);
    }
    const lead = state.leads.find((row) => String(row.lead_external_id || "") === leadId);
    if (lead) lead.pipeline_status = "app_submitted";
    const created = state.createdLeads.find((row) => String(row.lead_external_id || "") === leadId);
    if (created) created.pipeline_status = "app_submitted";
    saveCreatedLeads();
    clearActiveSession();
    window.open(portalUrl, "_blank");
    fetchLeads();
  } catch (err) {
    console.error("Failed to update pipeline status", err);
  }
}

async function moveLeadToPipeline(leadId, stage = "app_submitted") {
  const lead = state.leads.find((row) => String(row.lead_external_id || "") === String(leadId || ""));
  if (!lead) return false;
  const normalizedStage = PIPELINE_STAGES.includes(stage) ? stage : "app_submitted";
  lead.pipeline_status = normalizedStage;
  const created = state.createdLeads.find(
    (row) => String(row.lead_external_id || "") === String(leadId || ""),
  );
  if (created) {
    created.pipeline_status = normalizedStage;
    saveCreatedLeads();
  }
  renderLeadSelectionTable();
  renderPipelineBoard();
  try {
    await persistPipelineStatus(lead, normalizedStage);
    return true;
  } catch (error) {
    console.error(error);
    return false;
  }
}

function getPipelineLeadsByStage(stage) {
  return state.leads
    .map((row) => normalizeLeadRow(row))
    .filter((row) => String(row.pipeline_status || "") === stage);
}

function renderPipelineBoard() {
  PIPELINE_STAGES.forEach((stage) => {
    const zone = document.querySelector(`[data-pipeline-dropzone="${stage}"]`);
    const counter = document.getElementById(`pipelineCount_${stage}`);
    if (!zone) return;
    const rows = getPipelineLeadsByStage(stage);
    if (counter) counter.textContent = formatNumber.format(rows.length);
    if (!rows.length) {
      zone.innerHTML = `<p class="muted small">Drop leads here.</p>`;
      return;
    }
    zone.innerHTML = rows
      .slice(0, 120)
      .map((row) => {
        const name = escapeHtml(getLeadDisplayName(row));
        const carrier = escapeHtml(row.carrier_match || "Carrier pending");
        const queue = escapeHtml(row.routing_bucket || "Queue unassigned");
        const encodedLeadId = encodeURIComponent(String(row.lead_external_id || ""));
        return `
          <article class="pipeline-card" draggable="true" data-pipeline-lead="${encodedLeadId}">
            <strong>${name}</strong>
            <p class="muted">${carrier}</p>
            <p class="muted small">${queue}</p>
          </article>
        `;
      })
      .join("");
  });
}

function toTitleCase(value) {
  return String(value || "")
    .split(/[\s_]+/)
    .filter(Boolean)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1).toLowerCase()}`)
    .join(" ");
}

function generateCallSummary() {
  const path = String(document.getElementById("deskProductPath")?.value || getCallDeskPath() || "");
  const answers = state.ui.workflowAnswers;
  const ageText = ageBandLabel(answers.age || "") || "Not set";
  const productText = toTitleCase(path || "Not set");
  const healthText = toTitleCase(answers.health || "Not set");
  return `Qualified ${ageText} for ${productText}. Health is ${healthText}.`;
}

function preparePayload() {
  const clientName = String(document.getElementById("deskClientName")?.value || "").trim();
  const nameParts = clientName.split(/\s+/).filter(Boolean);
  const firstName = nameParts[0] || "Unknown";
  const lastName = nameParts.slice(1).join(" ") || "";
  const phone = String(document.getElementById("deskPhone")?.value || "").trim();
  const email = String(state.ui.currentLeadEmail || "").trim();
  const age = String(document.getElementById("deskAge")?.value || "").trim();
  const tobacco = String(document.getElementById("deskTobacco")?.value || "").trim();
  const productPath = String(document.getElementById("deskProductPath")?.value || "").trim();
  const disposition = String(document.getElementById("deskDisposition")?.value || "").trim();
  const needGoalSummary = String(document.getElementById("deskGoalNote")?.value || "").trim();
  const healthPosture = String(document.getElementById("deskHealth")?.value || "").trim();
  const objection = String(document.getElementById("deskObjection")?.value || "").trim();
  const currentLeadId = String(state.ui.leadId || "").trim();
  const carrierMatch = String(
    state.ui.primaryCarrier || document.getElementById("deskWorkflowPrimary")?.textContent || "",
  ).trim();
  const confidence = String(state.ui.primaryConfidence || "").trim();

  return {
    contactId: currentLeadId || `NEW-${Math.floor(Math.random() * 10000)}`,
    firstName,
    lastName,
    phone: phone || "",
    email: email || "",
    disposition: disposition || "",
    age: age || "",
    tobacco: tobacco || "",
    healthPosture: healthPosture || "",
    carrierMatch: carrierMatch || "",
    confidence: confidence || "",
    lastActivity: new Date().toLocaleString(),
    tags: `${productPath} Lead, ${disposition}`,
    notes: `${needGoalSummary} | Health Posture: ${healthPosture} | Objection: ${objection}`,
  };
}

function clearWrapUpFields() {
  [
    "deskHealthNotes",
    "deskObjection",
    "deskDisposition",
    "deskNextStep",
    "deskFollowUp",
    "deskCallNotes",
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });

  document.getElementById("deskSyncGog")?.addEventListener("change", () => {
    document.getElementById("callDeskStatus").textContent = "In progress";
    updateSyncToggleUi();
    updateGeneratedCallSummary();
    updateSaveButtonAvailability();
  });
}

function updateSyncToggleUi() {
  const toggle = document.getElementById("deskSyncGog");
  const stateEl = document.getElementById("deskSyncGogState");
  if (!toggle || !stateEl) return;
  const isOn = Boolean(toggle.checked);
  stateEl.textContent = isOn ? "ON" : "OFF";
  stateEl.classList.toggle("is-off", !isOn);
}

function setSavingState(isSaving) {
  const btn = document.getElementById("deskSaveToNotesBtn");
  if (!btn) return;
  state.ui.isSaving = Boolean(isSaving);
  if (isSaving) {
    state.ui.saveStatus = "saving";
    btn.disabled = true;
    btn.textContent = "Syncing to Drive...";
    return;
  }
  state.ui.saveStatus = "idle";
  btn.textContent = "Save to notes history";
  updateSaveButtonAvailability();
}

function setSaveStatus(status, labelOverride = "") {
  state.ui.saveStatus = status;
  const btn = document.getElementById("deskSaveToNotesBtn");
  if (!btn) return;
  if (status === "saving") {
    btn.disabled = true;
    btn.textContent = "Syncing to Drive...";
    return;
  }
  if (status === "success") {
    btn.disabled = false;
    btn.textContent = labelOverride || "Saved to CRM ✅";
    return;
  }
  if (status === "error") {
    btn.disabled = false;
    btn.textContent = "Save failed. Retry";
    return;
  }
  btn.textContent = "Save to notes history";
  updateSaveButtonAvailability();
}

function clearActiveSession() {
  localStorage.removeItem(ACTIVE_SESSION_STORAGE_KEY);
}

function persistActiveSession() {
  const leadId = String(state.ui.selectedCallDeskLeadId || state.ui.leadId || "").trim();
  if (!leadId) return;
  const fields = ACTIVE_SESSION_FIELD_IDS.reduce((acc, id) => {
    const el = document.getElementById(id);
    acc[id] = el ? String(el.value || "") : "";
    return acc;
  }, {});
  const snapshot = {
    lead_id: leadId,
    saved_at: new Date().toISOString(),
    fields,
  };
  localStorage.setItem(ACTIVE_SESSION_STORAGE_KEY, JSON.stringify(snapshot));
}

function restoreActiveSessionForLead(leadId) {
  try {
    const raw = JSON.parse(localStorage.getItem(ACTIVE_SESSION_STORAGE_KEY) || "{}");
    if (!raw || String(raw.lead_id || "") !== String(leadId || "")) return false;
    const fields = raw.fields || {};
    ACTIVE_SESSION_FIELD_IDS.forEach((id) => {
      if (!(id in fields)) return;
      const el = document.getElementById(id);
      if (el) el.value = String(fields[id] || "");
    });
    return true;
  } catch {
    return false;
  }
}

async function markLeadAsOpened(leadId) {
  const id = String(leadId || "").trim();
  if (!id) return;
  try {
    const openedAt = supabase
      ? await markLeadOpenedInSupabase(id)
      : await (async () => {
          if (!LEAD_OPEN_LEASE_URL.trim()) return null;
          const response = await fetch(`${LEAD_OPEN_LEASE_URL}/${encodeURIComponent(id)}/open`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ openedAt: new Date().toISOString() }),
          });
          if (!response.ok) return null;
          const data = await response.json().catch(() => ({}));
          return String(data?.last_opened_at || new Date().toISOString());
        })();
    if (!openedAt) return;
    const lead = state.leads.find((row) => String(row.lead_external_id || "") === id);
    if (lead) lead.last_opened_at = openedAt;
    const created = state.createdLeads.find((row) => String(row.lead_external_id || "") === id);
    if (created) created.last_opened_at = openedAt;
    renderLeadSelectionTable();
  } catch (error) {
    console.error("Lead lease update failed:", error);
  }
}

async function saveLeadData() {
  if (state.ui.saveStatus === "saving") return false;
  const statusEl = document.getElementById("callDeskStatus");
  const clientName = String(document.getElementById("deskClientName")?.value || "").trim();
  const nameParts = clientName.split(/\s+/).filter(Boolean);
  const lead = {
    id: String(state.ui.leadId || state.ui.selectedCallDeskLeadId || "").trim(),
    firstName: nameParts[0] || "Unknown",
    lastName: nameParts.slice(1).join(" ") || "",
    phone: String(document.getElementById("deskPhone")?.value || "").trim(),
    email: String(state.ui.currentLeadEmail || "").trim(),
  };
  const discoveryState = {
    age: String(document.getElementById("deskAge")?.value || "").trim(),
    tobacco: String(document.getElementById("deskTobacco")?.value || "").trim(),
    health: String(document.getElementById("deskHealth")?.value || "").trim(),
    productPath: String(document.getElementById("deskProductPath")?.value || "").trim(),
  };
  const currentDisposition = String(document.getElementById("deskDisposition")?.value || "").trim();
  const followUpAt = String(document.getElementById("deskFollowUp")?.value || "").trim();
  const syncViaGog = Boolean(document.getElementById("deskSyncGog")?.checked);
  const needGoalSummary = String(document.getElementById("deskGoalNote")?.value || "").trim();
  const objection = String(document.getElementById("deskObjection")?.value || "").trim();
  const primaryCarrier = String(
    state.ui.primaryCarrier || document.getElementById("deskWorkflowPrimary")?.textContent || "",
  ).trim();
  const primaryConfidence = String(state.ui.primaryConfidence || "").trim();
  const tagsString = `${discoveryState.productPath} Lead, ${currentDisposition}`;
  const currentNotes = `${needGoalSummary} | Health Posture: ${discoveryState.health} | Objection: ${objection}`;

  if (!lead.id && !lead.firstName && !lead.phone) {
    if (statusEl) statusEl.textContent = "Load or create a lead before syncing.";
    return false;
  }

  const payload = {
    contactId: lead.id || `NEW-${Math.floor(Math.random() * 10000)}`,
    firstName: lead.firstName,
    lastName: lead.lastName,
    phone: lead.phone || "",
    email: lead.email || "",
    disposition: currentDisposition || "",
    age: discoveryState.age || "",
    tobacco: discoveryState.tobacco || "",
    healthPosture: discoveryState.health || "",
    carrierMatch: primaryCarrier || "",
    confidence: primaryConfidence || "",
    calendarEventId: "",
    nextAppointmentTime: followUpAt || "",
    pipelineStatus: String(
      state.leads.find((row) => String(row.lead_external_id || "") === String(lead.id || ""))?.pipeline_status || "",
    ).trim(),
    tags: tagsString,
    notes: currentNotes,
  };
  setSaveStatus("saving");
  state.ui.isSaving = true;
  try {
    const shouldSchedule = syncViaGog && ["callback", "follow_up"].includes(currentDisposition);
    let portalScheduleWarning = "";
    let googleCalendarWarning = "";
    if (supabase) {
      const savedLead = await saveLeadToSupabase(payload);
      if (savedLead?.lead_external_id) {
        payload.contactId = String(savedLead.lead_external_id || payload.contactId).trim();
      }
    }
    if (shouldSchedule) {
      if (!followUpAt) {
        portalScheduleWarning = "Add a follow-up date/time to schedule.";
      } else {
        try {
          const scheduleData = supabase
            ? await scheduleAppointmentInSupabase({
                contactId: payload.contactId,
                clientName,
                email: lead.email || "",
                phone: lead.phone || "",
                scheduledAt: followUpAt,
                description: `${currentDisposition === "callback" ? "Callback" : "Follow-up"} scheduled from Call Desk`,
                disposition: currentDisposition,
              })
            : await (async () => {
                if (!LOCAL_DB_CALENDAR_SCHEDULE_URL.trim()) {
                  throw new Error("Calendar scheduling endpoint is not configured.");
                }
                const scheduleResponse = await fetch(LOCAL_DB_CALENDAR_SCHEDULE_URL, {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    contactId: payload.contactId,
                    clientName: clientName,
                    email: lead.email || "",
                    phone: lead.phone || "",
                    scheduledAt: followUpAt,
                    description: `${currentDisposition === "callback" ? "Callback" : "Follow-up"} scheduled from Call Desk`,
                  }),
                });
                if (!scheduleResponse.ok) {
                  throw new Error(`Calendar schedule failed (${scheduleResponse.status})`);
                }
                return await scheduleResponse.json();
              })();
          if (!scheduleData?.ok) {
            throw new Error(String(scheduleData?.error || "Calendar schedule failed"));
          }
          payload.calendarEventId = String(scheduleData.calendarEventId || "").trim();
          payload.nextAppointmentTime = String(scheduleData.nextAppointmentTime || followUpAt).trim();

          try {
            const googleScheduleData = await createGoogleCalendarEvent({
              clientName,
              email: lead.email || "",
              phone: lead.phone || "",
              scheduledAt: followUpAt,
              description: `${currentDisposition === "callback" ? "Callback" : "Follow-up"} scheduled from Call Desk`,
              durationMinutes: 30,
            });
            payload.calendarEventId = String(
              googleScheduleData.calendarEventId || payload.calendarEventId || "",
            ).trim();
          } catch (calendarError) {
            googleCalendarWarning = String(calendarError?.message || "Google Calendar sync failed.");
          }
        } catch (scheduleError) {
          portalScheduleWarning = String(scheduleError?.message || "Scheduling failed after save.");
        }
      }
    }

    if (GOOGLE_WEBHOOK_URL.trim()) {
      await fetch(GOOGLE_WEBHOOK_URL, {
        method: "POST",
        mode: "no-cors",
        headers: {
          "Content-Type": "text/plain;charset=utf-8",
        },
        body: JSON.stringify(payload),
      });
    }
    if (!supabase && LOCAL_DB_SYNC_URL.trim()) {
      const localResponse = await fetch(LOCAL_DB_SYNC_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      if (!localResponse.ok) {
        throw new Error(`Local DB sync failed (${localResponse.status})`);
      }
    }
    applySavedPayloadToLeadState(payload);
    if (shouldSchedule && payload.nextAppointmentTime) {
      upsertLocalCalendarEvent({
        summary: `${currentDisposition === "callback" ? "Callback" : "Follow-up"}: ${clientName || "Lead"}`,
        start: payload.nextAppointmentTime,
        htmlLink: "",
        source: "portal",
        lead_id: 0,
        lead_external_id: payload.contactId,
        attendees: payload.email ? [{ email: payload.email }] : [],
      });
      renderCalendarTab();
    }
    // With no-cors, response is opaque; if no error is thrown, treat as success.
    let successButtonLabel = "Saved to CRM ✅";
    if (statusEl) {
      if (shouldSchedule && portalScheduleWarning) {
        statusEl.textContent = `Saved to CRM. Portal scheduling failed: ${portalScheduleWarning}`;
        successButtonLabel = "Saved - portal scheduling failed";
        window.setTimeout(() => {
          window.alert(`Portal scheduling failed:\n\n${portalScheduleWarning}`);
        }, 0);
      } else if (shouldSchedule && googleCalendarWarning) {
        statusEl.textContent = `Saved and scheduled in portal. Google Calendar sync is off for now.`;
        successButtonLabel = "Saved and scheduled ✅";
      } else {
        statusEl.textContent = shouldSchedule && supabase
          ? "Saved and scheduled in portal."
          : "Data synced successfully.";
        successButtonLabel = shouldSchedule && supabase
          ? "Saved and scheduled ✅"
          : "Saved to CRM ✅";
      }
    }
    setSaveStatus("success", successButtonLabel);
    if (shouldSchedule && supabase) {
      refreshTodaysAppointments().catch(() => {});
      refreshCalendarTabData().catch(() => {});
    }
    clearActiveSession();
    window.setTimeout(() => {
      if (
        statusEl
        && (statusEl.textContent === "Data synced successfully." || statusEl.textContent === "Saved and scheduled in portal.")
      ) statusEl.textContent = "Ready";
      if (state.ui.saveStatus === "success") setSaveStatus("idle");
    }, 3000);
    return true;
  } catch (error) {
    console.error("Save failed:", error);
    if (statusEl) statusEl.textContent = String(error?.message || "Sync failed. Try again.");
    setSaveStatus("error");
    window.setTimeout(() => {
      if (state.ui.saveStatus === "error") setSaveStatus("idle");
    }, 3000);
    return false;
  } finally {
    state.ui.isSaving = false;
  }
}

function updateGeneratedCallSummary() {
  const goalField = document.getElementById("deskGoalNote");
  if (!goalField) return;
  goalField.value = generateCallSummary();
}

function loadLeadIntoCallDesk(leadId) {
  const lead = state.leads.find((row) => row.lead_external_id === leadId);
  if (!lead) {
    setDeskLeadPickerStatus("Lead not found.");
    return;
  }

  state.ui.selectedCallDeskLeadId = leadId;
  state.ui.currentCallLeadId = leadId;
  state.ui.leadId = leadId || null;
  state.ui.currentLeadEmail = String(lead.email || "").trim();
  state.ui.selectedLeadSelectionId = leadId;
  appStore.setState({ currentLeadId: leadId });

  const path = inferDeskProductPathFromLead(lead);
  const ageBand = inferAgeBandFromLead(lead);
  document.getElementById("deskClientName").value = getLeadDisplayName(lead);
  document.getElementById("deskPhone").value = lead.mobile_phone || "";
  document.getElementById("deskCoverage").value = lead.product_line || lead.product_interest || "";
  document.getElementById("deskBudgetText").value = "";
  document.getElementById("deskGoalNote").value = "";
  document.getElementById("deskCallNotes").value = lead.notes || "";
  document.getElementById("deskFollowUp").value = toDateTimeLocalValue(lead.next_appointment_time || "");
  document.getElementById("deskTrigger").value = lead.lead_source_detail || "";
  document.getElementById("deskTobacco").value = String(lead.tobacco || "").toLowerCase();
  document.getElementById("deskHealth").value = String(lead.health_posture || "").toLowerCase();
  document.getElementById("deskDisposition").value = String(lead.disposition || "").toLowerCase();
  const workflowPrimary = document.getElementById("deskWorkflowPrimary");
  if (workflowPrimary && String(lead.carrier_match || "").trim()) {
    workflowPrimary.innerHTML = `<span class="option-chip">${escapeHtml(String(lead.carrier_match || "").trim())}</span>`;
  }
  const workflowConfidence = document.getElementById("deskWorkflowConfidence");
  if (workflowConfidence && String(lead.confidence || "").trim()) {
    workflowConfidence.textContent = String(lead.confidence || "").trim();
  }
  state.ui.primaryCarrier = String(lead.carrier_match || state.ui.primaryCarrier || "");
  state.ui.primaryConfidence = String(lead.confidence || state.ui.primaryConfidence || "");
  if (ageBand) {
    document.getElementById("deskAge").value = ageBand;
    state.ui.workflowAnswers.age = ageBand;
    applyAgeToProductPath(ageBand);
  } else {
    document.getElementById("deskAge").value = "";
    state.ui.workflowAnswers.age = "";
  }
  state.ui.workflowAnswers.tobacco = String(document.getElementById("deskTobacco").value || "");
  state.ui.workflowAnswers.health = String(document.getElementById("deskHealth").value || "");
  syncPrimaryFromAgeHealth();

  const productField = document.getElementById("deskProductPath");
  if (path && productField) {
    productField.value = path;
    state.ui.workflowAnswers.productPath = path;
    renderCallDeskBranching();
    renderWorkflowAdvisor();
  } else {
    if (productField) productField.value = "";
    state.ui.workflowAnswers.productPath = "";
    updateGeneratedCallSummary();
  }

  const restored = restoreActiveSessionForLead(leadId);
  if (restored) {
    renderCallDeskBranching();
    renderWorkflowAdvisor();
    updateGeneratedCallSummary();
  }

  markLeadAsOpened(leadId).catch(() => {});
  renderLeadSelectionTable();
  document.getElementById("callDeskStatus").textContent = restored ? "Lead loaded (session restored)" : "Lead loaded";
  setDeskLeadPickerStatus(`Loaded ${getLeadDisplayName(lead)}.`);
}

function createLeadFromCallDesk() {
  const fullName = String(document.getElementById("deskClientName")?.value || "").trim();
  const phone = String(document.getElementById("deskPhone")?.value || "").trim();
  const coverage = String(document.getElementById("deskCoverage")?.value || "").trim();
  const productPath = String(document.getElementById("deskProductPath")?.value || "").trim();
  const goal = String(document.getElementById("deskGoalNote")?.value || "").trim();
  const notes = String(document.getElementById("deskCallNotes")?.value || "").trim();

  if (!fullName && !phone) {
    setDeskLeadPickerStatus("Add at least client name or phone before creating a new lead.");
    return;
  }

  const nameParts = fullName.split(/\s+/).filter(Boolean);
  const firstName = nameParts[0] || "";
  const lastName = nameParts.slice(1).join(" ");
  const leadId = `CD-${Date.now()}`;
  const productLine =
    productPath === "health"
      ? "Health"
      : productPath === "life"
        ? "Life"
        : productPath === "both"
          ? "Health + Life"
          : "";
  const nowIso = new Date().toISOString();

  const newLead = {
    lead_external_id: leadId,
    first_name: firstName,
    last_name: lastName,
    full_name: fullName || firstName || "New Call Desk Lead",
    email: "",
    mobile_phone: phone,
    business_name: "",
    lead_source: "call_desk",
    lead_source_detail: "manual_call_desk_entry",
    campaign_name: "Call Desk",
    product_interest: productLine.toLowerCase(),
    product_line: productLine,
    owner_queue: "call_desk_queue",
    lead_status: "working",
    booking_status: "not_started",
    consent_status: "review_required",
    consent_channel_sms: "",
    consent_channel_email: "",
    consent_channel_whatsapp: "",
    dnc_status: "pending_check",
    contact_eligibility: "review_required",
    created_at_source: nowIso,
    last_activity_at_source: nowIso,
    notes: [goal, notes].filter(Boolean).join(" | "),
    raw_tags: "call_desk,manual_entry",
    raw_created: nowIso,
    raw_last_activity: nowIso,
    routing_bucket: "call_desk_queue",
    suppress_reason: "",
    recommended_channel: phone ? "phone_call" : "manual_review",
    sequence_name: "call_desk_manual_followup",
    recommended_next_action: goal || "Continue discovery and set follow-up",
    priority_tier: "normal",
    pipeline_status: "",
  };

  state.createdLeads = mergeLeadsByExternalId(state.createdLeads, [newLead]);
  saveCreatedLeads();
  state.leads = mergeLeadsByExternalId(state.leads, [newLead]);
  state.ui.selectedCallDeskLeadId = leadId;
  state.ui.leadId = null;
  state.ui.currentLeadEmail = "";
  state.ui.selectedLeadSelectionId = leadId;

  renderDashboard({
    leads: state.leads,
    activity: state.activity,
    bookings: state.bookings,
    sales: state.sales,
    targets: state.targets,
    sourcedLeads: state.sourcedLeads,
    carrierDocs: state.carrierDocs,
  });

  document.getElementById("callDeskStatus").textContent = "New lead started";
  setDeskLeadPickerStatus(`Created ${newLead.full_name} in call desk queue.`);
}

function attachCallDeskHandlers() {
  document.getElementById("deskScriptDrawerToggleBtn")?.addEventListener("click", () => {
    const drawer = document.getElementById("deskScriptDrawer");
    if (!drawer) return;
    setScriptDrawerOpen(drawer.hidden);
  });
  document.getElementById("deskScriptDrawerCloseBtn")?.addEventListener("click", () => {
    setScriptDrawerOpen(false);
  });

  document.getElementById("deskCreateLeadBtn")?.addEventListener("click", () => {
    state.ui.leadId = null;
    state.ui.currentLeadEmail = "";
    startNextLeadFromQueue();
  });

  document.getElementById("deskNeedArea")?.addEventListener("change", (event) => {
    const value = event.target.value;
    const productField = document.getElementById("deskProductPath");
    if (!productField) return;
    if (["health", "life", "both", "unclear"].includes(value)) {
      productField.value = value;
      state.ui.workflowAnswers.productPath = value;
      renderCallDeskBranching();
      renderWorkflowAdvisor();
      persistActiveSession();
    }
  });

  document.getElementById("deskDisposition")?.addEventListener("change", (event) => {
    const disposition = String(event.target.value || "");
    const nextStep = document.getElementById("deskNextStep");
    const followUp = document.getElementById("deskFollowUp");
    const dueDateTime = (days) => {
      const date = new Date();
      date.setDate(date.getDate() + days);
      date.setHours(10, 0, 0, 0);
      const pad = (n) => String(n).padStart(2, "0");
      return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(
        date.getMinutes(),
      )}`;
    };
    if (!nextStep || !followUp) return;
    if (disposition === "sold") {
      nextStep.value = "Start onboarding checklist: policy delivery, payment setup, beneficiary confirmation.";
      if (!followUp.value) followUp.value = dueDateTime(2);
    } else if (disposition === "follow_up") {
      nextStep.value = "Send summary + schedule structured follow-up with clear decision deadline.";
      if (!followUp.value) followUp.value = dueDateTime(3);
    } else if (disposition === "callback") {
      nextStep.value = "Create callback task and confirm exact callback window with decision maker.";
      if (!followUp.value) followUp.value = dueDateTime(1);
    } else if (disposition === "no_answer") {
      nextStep.value = "Retry in a different call window and drop voicemail/text if compliant.";
      if (!followUp.value) followUp.value = dueDateTime(1);
    }
    document.getElementById("callDeskStatus").textContent = "Disposition automation applied";
    updateSaveButtonAvailability();
    persistActiveSession();
  });

  let activeObjectionPreset = "";
  const objectionModal = document.getElementById("deskObjectionModal");
  const objectionTitle = document.getElementById("deskObjectionTitle");
  const objectionBody = document.getElementById("deskObjectionBody");
  const objectionHighlight = document.getElementById("deskObjectionHighlight");
  const closeObjectionModal = () => {
    if (!objectionModal) return;
    objectionModal.hidden = true;
    activeObjectionPreset = "";
  };
  const openObjectionModal = (preset) => {
    const snippet = DESK_OBJECTION_SNIPPETS[preset];
    if (!objectionModal || !objectionTitle || !objectionBody || !snippet) return;
    const title = preset.charAt(0).toUpperCase() + preset.slice(1);
    activeObjectionPreset = preset;
    state.ui.lastHandledObjection = preset;
    const objectionField = document.getElementById("deskObjection");
    if (objectionField) objectionField.value = toTitleCase(preset);
    objectionTitle.textContent = `Handle: ${title}`;
    objectionBody.textContent = snippet;
    if (objectionHighlight) {
      objectionHighlight.hidden = false;
      objectionHighlight.textContent = snippet;
    }
    showDeskScriptToast(snippet);
    objectionModal.hidden = false;
    document.getElementById("callDeskStatus").textContent = "Objection script ready";
    updateGeneratedCallSummary();
  };

  document.querySelectorAll("[data-objection-action]").forEach((button) => {
    button.addEventListener("click", () => {
      const preset = String(button.dataset.objectionAction || "");
      if (!preset) return;
      openObjectionModal(preset);
    });
  });

  document.getElementById("deskObjectionCloseBtn")?.addEventListener("click", closeObjectionModal);
  objectionModal?.addEventListener("click", (event) => {
    if (event.target === objectionModal) closeObjectionModal();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && objectionModal && !objectionModal.hidden) closeObjectionModal();
  });

  document.getElementById("deskObjectionCopyBtn")?.addEventListener("click", () => {
    const snippet = DESK_OBJECTION_SNIPPETS[activeObjectionPreset];
    if (!snippet) return;
    copyText(snippet);
    document.getElementById("callDeskStatus").textContent = "Objection script copied";
  });

  document.getElementById("deskObjectionInsertBtn")?.addEventListener("click", () => {
    const snippet = DESK_OBJECTION_SNIPPETS[activeObjectionPreset];
    const notesEl = document.getElementById("deskCallNotes");
    if (!snippet || !notesEl) return;
    const prefix = notesEl.value.trim() ? "\n" : "";
    notesEl.value = `${notesEl.value}${prefix}- ${snippet}`;
    document.getElementById("callDeskStatus").textContent = "Objection script inserted";
    updateGeneratedCallSummary();
    closeObjectionModal();
  });

  document.querySelectorAll("[data-desk-chip]").forEach((button) => {
    button.addEventListener("click", () => {
      const body = document.getElementById("deskCallNotes");
      const tag = button.dataset.deskChip;
      if (!body) return;
      const prefix = body.value.trim().length ? "\n" : "";
      body.value = `${body.value}${prefix}- ${tag}`;
      document.getElementById("callDeskStatus").textContent = "Updated";
      if (String(tag || "").toLowerCase().includes("objection")) {
        const objectionField = document.getElementById("deskObjection");
        if (objectionField) objectionField.value = tag.replace(" objection", "");
      }
      updateGeneratedCallSummary();
    });
  });

  [
    "deskClientName",
    "deskPhone",
    "deskCoverage",
    "deskBudgetText",
    "deskCurrentCoverage",
    "deskExistingPolicy",
    "deskPolicyIntent",
    "deskDecisionMaker",
    "deskDecisionTimeline",
    "deskGoalNote",
    "deskHealthNotes",
    "deskObjection",
    "deskDisposition",
    "deskNextStep",
    "deskFollowUp",
    "deskCallNotes",
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("input", () => {
      document.getElementById("callDeskStatus").textContent = "In progress";
      if (DESK_DISCOVERY_FIELD_IDS.has(id)) hideDeskScriptToast();
      if (id === "deskCurrentCoverage") {
        renderWorkflowAdvisor();
      } else if (id !== "deskGoalNote" && id !== "deskCallNotes") {
        updateGeneratedCallSummary();
      }
      updateSaveButtonAvailability();
      persistActiveSession();
    });
    el.addEventListener("change", () => {
      document.getElementById("callDeskStatus").textContent = "In progress";
      if (DESK_DISCOVERY_FIELD_IDS.has(id)) hideDeskScriptToast();
      if (id === "deskCurrentCoverage" || id === "deskExistingPolicy" || id === "deskPolicyIntent") {
        renderWorkflowAdvisor();
      } else if (id !== "deskGoalNote" && id !== "deskCallNotes") {
        updateGeneratedCallSummary();
      }
      updateSaveButtonAvailability();
      persistActiveSession();
    });
  });

  document.getElementById("deskCopySummaryBtn")?.addEventListener("click", () => {
    copyText(buildCallDeskSummary());
    document.getElementById("callDeskStatus").textContent = "Copied";
  });

  document.getElementById("deskClearBtn")?.addEventListener("click", () => {
    [
      "deskClientName",
      "deskPhone",
      "deskCoverage",
      "deskBudgetText",
      "deskCurrentCoverage",
      "deskExistingPolicy",
      "deskPolicyIntent",
      "deskDecisionMaker",
      "deskDecisionTimeline",
      "deskGoalNote",
      "deskHealthNotes",
      "deskObjection",
      "deskDisposition",
      "deskNextStep",
      "deskFollowUp",
      "deskCallNotes",
    ].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.value = "";
    });
    const syncToggle = document.getElementById("deskSyncGog");
    if (syncToggle) syncToggle.checked = true;
    updateSyncToggleUi();
    const objectionHighlight = document.getElementById("deskObjectionHighlight");
    if (objectionHighlight) {
      objectionHighlight.hidden = true;
      objectionHighlight.textContent = "";
    }
    state.ui.lastHandledObjection = "";
    updateGeneratedCallSummary();
    document.getElementById("callDeskStatus").textContent = "Cleared";
    updateSaveButtonAvailability();
    persistActiveSession();
  });

  document.getElementById("deskSaveToNotesBtn")?.addEventListener("click", async () => {
    if (state.ui.saveStatus === "saving") return;
    const missingReadiness = buildDeskReadinessRequirements().some((item) => !item.value);
    if (missingReadiness && !canBypassReadinessForSave()) {
      document.getElementById("callDeskStatus").textContent = "Complete required discovery fields before saving.";
      updateSaveButtonAvailability();
      return;
    }
    const saved = loadNotesState();
    const history = saved.history || [];
    const snapshot = {
      notesClientName: document.getElementById("deskClientName")?.value || "",
      notesGoal: document.getElementById("deskGoalNote")?.value || "",
      notesAgeTobacco: [document.getElementById("deskAge")?.value || "", document.getElementById("deskTobacco")?.value || ""]
        .filter(Boolean)
        .join(", "),
      notesHealth: document.getElementById("deskHealthNotes")?.value || "",
      notesBudget: document.getElementById("deskBudgetText")?.value || "",
      notesCoverage: document.getElementById("deskCoverage")?.value || "",
      notesBody: buildCallDeskSummary(),
    };

    if (hasNotesContent(snapshot)) {
      history.unshift({
        id: `${Date.now()}`,
        savedAt: Date.now(),
        snapshot,
      });
    }

    const outcome = {
      ts: new Date().toISOString(),
      leadId: state.ui.selectedCallDeskLeadId || "",
      needArea: String(document.getElementById("deskNeedArea")?.value || ""),
      lane: String(document.getElementById("deskWorkflowLane")?.textContent || ""),
      disposition: String(document.getElementById("deskDisposition")?.value || "unknown"),
    };
    state.callOutcomes = Array.isArray(state.callOutcomes) ? state.callOutcomes : [];
    state.callOutcomes.unshift(outcome);
    state.callOutcomes = state.callOutcomes.slice(0, 1000);
    saveCallOutcomes();
    renderOutcomeAnalytics();

    const disposition = String(document.getElementById("deskDisposition")?.value || "");
    updateLeadDispositionStatus(state.ui.selectedCallDeskLeadId, disposition);
    syncMainCallQueue();

    saveNotesState(snapshot, history);
    renderNotesHistory(history, [
      "notesClientName",
      "notesGoal",
      "notesAgeTobacco",
      "notesHealth",
      "notesBudget",
      "notesCoverage",
      "notesBody",
    ]);
    document.getElementById("callDeskStatus").textContent = "Saved to notes";
    const synced = await saveLeadData();
    if (synced && disposition) startNextLeadFromQueue();
  });

  updateSaveButtonAvailability();
  updateSyncToggleUi();
}

function saveCriteria() {
  localStorage.setItem(CRITERIA_STORAGE_KEY, JSON.stringify(state.criteria));
  renderCriteriaPanel();
  renderSourcedLeadTable();
  renderEnrichmentQueues(buildSourcedSummary(state.sourcedLeads));
}

function attachCriteriaHandlers() {
  document.getElementById("criteriaGeography").addEventListener("change", (event) => {
    state.criteria.geography = event.target.value;
    saveCriteria();
  });
  document.getElementById("criteriaQuality").addEventListener("change", (event) => {
    state.criteria.quality = event.target.value;
    saveCriteria();
  });

  const toggles = [
    ["triggerNewParent", "triggers", "newParent"],
    ["triggerHomeBuyer", "triggers", "homeBuyer"],
    ["triggerJobLoss", "triggers", "jobLoss"],
    ["triggerJobChange", "triggers", "jobChange"],
    ["triggerMedicare", "triggers", "medicare"],
    ["triggerBusinessOwner", "triggers", "businessOwner"],
    ["ruleRequireUs", "rules", "requireUs"],
    ["ruleRequireName", "rules", "requireName"],
    ["ruleRequireTrigger", "rules", "requireTrigger"],
    ["ruleRequirePath", "rules", "requirePath"],
    ["ruleRejectAnonymous", "rules", "rejectAnonymous"],
  ];

  toggles.forEach(([id, section, key]) => {
    document.getElementById(id).addEventListener("change", (event) => {
      state.criteria[section][key] = event.target.checked;
      saveCriteria();
    });
  });
}

function renderDashboard(data) {
  const { leads, activity, bookings, sales, targets, sourcedLeads, carrierDocs } = data;
  const cleanedLeads = sanitizeLeadRows(leads).rows;
  const mergedLeads = mergeLeadsByExternalId(cleanedLeads, state.createdLeads);
  state.leads = mergedLeads;
  state.activity = activity;
  state.bookings = bookings;
  state.sales = sales;
  state.targets = targets;
  state.sourcedLeads = sourcedLeads;
  state.carrierDocs = carrierDocs;

  const summary = buildSummary(mergedLeads, activity, bookings, sales, targets);
  const sourcedSummary = buildSourcedSummary(sourcedLeads);

  document.getElementById("datasetStatus").textContent = `Loaded ${formatNumber.format(summary.totalLeads)} leads`;
  document.getElementById("datasetStatus").style.background = "var(--green-soft)";
  document.getElementById("datasetStatus").style.color = "var(--green)";
  document.getElementById("primaryQueueLabel").textContent = summary.primaryQueueLabel;

  renderMetrics(summary);
  renderTriage(summary);
  renderCompliance(summary);
  renderReadiness(summary);
  renderOutcomeAnalytics();
  renderDataQualityGuardrails(mergedLeads);
  renderSourcing(summary);
  renderCriteriaPanel();
  renderGuidancePanel();
  renderEnrichmentQueues(sourcedSummary);
  renderRoiStats(summary);
  populateFilters(mergedLeads);
  populateLeadSelectionFilters(mergedLeads);
  populateCampaignFilters(mergedLeads);
  populateSourcedFilters(sourcedLeads);
  renderOpsCharts(mergedLeads);
  setGoogleCalendarEmbeds();
  renderLeadSelectionTable();
  renderCampaignTable();
  renderLeadTable();
  renderSourcedLeadTable();
  renderPipelineBoard();
  createBarRows(document.getElementById("queueBars"), summary.queueCounts);
  createBarRows(
    document.getElementById("channelBars"),
    summary.channelCounts,
    "linear-gradient(90deg, #2d7a55, #67ab82)",
  );
  createBarRows(
    document.getElementById("sequenceBars"),
    summary.sequenceCounts,
    "linear-gradient(90deg, #4b6596, #7fa4da)",
  );

  const leadPresetLabel = document.getElementById("leadPresetLabel");
  if (leadPresetLabel) leadPresetLabel.textContent = `Preset: ${state.ui.leadPreset.replaceAll("_", " ")}`;
  const sourcedPresetLabel = document.getElementById("sourcedPresetLabel");
  if (sourcedPresetLabel) sourcedPresetLabel.textContent = `Preset: ${state.ui.sourcedPreset.replaceAll("_", " ")}`;
  syncMainCallQueue();
  renderWorkflowAdvisor();
  renderCarrierSettingsRows();

  window.dashboardSummary = summary;
}

async function clearTestData() {
  const statusEl = document.getElementById("purgeStatus");
  const confirmed = window.confirm(
    "Clear Test Data will delete test leads and reset pipeline/calendar fields for all leads. Continue?",
  );
  if (!confirmed) return;
  if (statusEl) statusEl.textContent = "Clearing...";
  try {
    if (!LOCAL_DB_PURGE_TEST_DATA_URL.trim()) throw new Error("Purge endpoint not configured.");
    const response = await fetch(LOCAL_DB_PURGE_TEST_DATA_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (!response.ok) throw new Error(`Purge failed (${response.status})`);
    const data = await response.json();
    if (!data?.ok) throw new Error(String(data?.error || "Purge failed"));

    const isTestLead = (row) => {
      const first = String(row?.first_name || "").toLowerCase();
      const last = String(row?.last_name || "").toLowerCase();
      return first.includes("test") || last.includes("test");
    };

    state.leads = state.leads
      .filter((row) => !isTestLead(row))
      .map((row) => ({
        ...row,
        pipeline_status: "",
        calendar_event_id: "",
      }));
    state.createdLeads = state.createdLeads.filter((row) => !isTestLead(row));
    saveCreatedLeads();

    renderDashboard({
      leads: state.leads,
      activity: state.activity,
      bookings: state.bookings,
      sales: state.sales,
      targets: state.targets,
      sourcedLeads: state.sourcedLeads,
      carrierDocs: state.carrierDocs,
    });
    if (statusEl) {
      statusEl.textContent = `Done • Deleted ${Number(data.deleted || 0)} test leads, reset ${Number(
        data.reset || 0,
      )} rows`;
    }
  } catch (error) {
    console.error(error);
    if (statusEl) statusEl.textContent = "Purge failed";
  }
}

async function initialize() {
  if (hasInitialized) return;
  hasInitialized = true;
  try {
    const [leads, activity, bookings, sales, targets, sourcedLeads, carrierDocs, carrierGrid] = await Promise.all([
      supabase ? loadLeadRowsFromSupabase() : loadCsv(DATA_FILES.leads),
      loadCsv(DATA_FILES.activity).catch(() => []),
      loadCsv(DATA_FILES.bookings).catch(() => []),
      loadCsv(DATA_FILES.sales).catch(() => []),
      loadCsv(DATA_FILES.targets).catch(() => []),
      loadCsv(DATA_FILES.sourced).catch(() => []),
      loadCsv(DATA_FILES.carrierDocs).catch(() => []),
      loadJson("./carrierData.json").catch(() => []),
    ]);

    state.carrierGrid = Array.isArray(carrierGrid) ? carrierGrid : [];
    const cleanedLeads = sanitizeLeadRows(leads).rows;
    state.ui.uploadCriticalErrors = 0;
    renderUploadPreview([]);
    setImportButtonsState(false, false);
    renderDashboard({ leads: cleanedLeads, activity, bookings, sales, targets, sourcedLeads, carrierDocs });
    await loadCarrierConfigs();
  } catch (error) {
    document.getElementById("datasetStatus").textContent = String(error.message || error);
    document.getElementById("datasetStatus").style.background = "var(--red-soft)";
    document.getElementById("datasetStatus").style.color = "var(--red)";
  }
}

async function bootstrapPortalAuth() {
  setAuthLocked(true);
  setAuthStatus("Checking session…");

  const session = await ensureAuthenticatedSession();
  if (session) {
    setPortalUser(session);
    setAuthLocked(false);
    setAuthStatus("");
    await initialize();
  } else {
    setPortalUser(null);
    setAuthLocked(true);
    if (!document.getElementById("authStatus")?.textContent) {
      setAuthStatus("Sign in to open the portal.");
    }
  }

  if (supabase) {
    supabase.auth.onAuthStateChange(async (_event, sessionUpdate) => {
      if (sessionUpdate) {
        setPortalUser(sessionUpdate);
        setAuthLocked(false);
        setAuthStatus("");
        await initialize();
      } else {
        setPortalUser(null);
        setAuthLocked(true);
        setAuthStatus("Signed out. Sign in to continue.");
      }
    });
  }
}

document.getElementById("leadFileInput").addEventListener("change", handleLeadUploadFromFileInput);
document.getElementById("leadFileInputDashboard").addEventListener("change", handleLeadUploadFromFileInput);
document.getElementById("importUploadedToDbBtn")?.addEventListener("click", () => importUploadedLeadsToLocalDb(false));
document.getElementById("importUploadedToDbCleanBtn")?.addEventListener("click", () => importUploadedLeadsToLocalDb(true));

document.getElementById("exportSummaryBtn").addEventListener("click", () => {
  const blob = new Blob([JSON.stringify(window.dashboardSummary ?? {}, null, 2)], {
    type: "application/json",
  });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "insurance-dashboard-summary.json";
  link.click();
  URL.revokeObjectURL(link.href);
});

document.getElementById("copyLeadSummaryBtn")?.addEventListener("click", () => {
  copyText(buildFilteredLeadSummary());
});

document.getElementById("copySourcedSummaryBtn").addEventListener("click", () => {
  const rows = getFilteredSourcedLeads()
    .slice(0, 15)
    .map((row) => `${row["First Name"] || ""} ${row["Last Name"] || ""}`.trim() + ` | ${sourcedQueueLabel(row)} | ${row["Trigger Event"] || "-"} | ${row["Lead Circumstances"] || "-"}`);
  copyText(rows.join("\n"));
});

document.getElementById("copyLeadBriefBtn").addEventListener("click", () => {
  copyText(buildSelectedLeadBrief());
});

document.getElementById("saveLeadStateBtn").addEventListener("click", () => {
  const id = state.ui.selectedSourcedLeadId;
  if (!id) return;
  state.sourcedLeadState[id] = {
    stage: document.getElementById("detailStageSelect").value,
    ownerNote: document.getElementById("detailOwnerNote").value.trim(),
  };
  saveSourcedLeadState();
  renderSourcedLeadTable();
});

document.querySelectorAll("[data-preset]").forEach((button) => {
  button.addEventListener("click", () => {
    const preset = button.dataset.preset;
    if (preset === "call_queue" || preset === "needs_review" || preset === "email_salvage" || preset === "clear") {
      setLeadPreset(preset === "clear" ? "none" : preset);
    }
    if (preset === "sourced_ready" || preset === "sourced_enrichment" || preset === "clear") {
      setSourcedPreset(preset === "clear" ? "none" : preset);
    }
  });
});

document.getElementById("sourcedLeadTable").addEventListener("click", (event) => {
  const row = event.target.closest("[data-source-id]");
  if (!row) return;
  state.ui.selectedSourcedLeadId = row.dataset.sourceId;
  renderSourcedLeadTable();
});

document.getElementById("leadSelectTable")?.addEventListener("click", (event) => {
  const moveBtn = event.target.closest("[data-move-pipeline]");
  if (moveBtn) {
    event.preventDefault();
    event.stopPropagation();
    const leadId = String(moveBtn.dataset.movePipeline || "");
    if (leadId) {
      moveLeadToPipeline(leadId, "app_submitted").then((ok) => {
        const status = document.getElementById("leadSelectStatus");
        if (status) status.textContent = ok ? "Moved to pipeline: App Submitted." : "Could not sync pipeline update.";
      });
    }
    return;
  }
  const row = event.target.closest("[data-lead-select-id]");
  if (!row) return;
  if (row.dataset.leadLocked === "true") {
    const status = document.getElementById("leadSelectStatus");
    if (status) status.textContent = "This lead is locked for 15 minutes to prevent double-dialing.";
    return;
  }
  state.ui.selectedLeadSelectionId = row.dataset.leadSelectId || "";
  renderLeadSelectionTable();
});

document.getElementById("campaignTable")?.addEventListener("click", (event) => {
  const row = event.target.closest("[data-campaign-lead-id]");
  if (!row) return;
  state.ui.selectedCampaignLeadId = row.dataset.campaignLeadId || "";
  if (event.target.matches('[data-campaign-checkbox]')) return;
  renderCampaignTable();
});

document.getElementById("campaignTable")?.addEventListener("change", (event) => {
  const checkbox = event.target.closest("[data-campaign-checkbox]");
  if (!checkbox) return;
  const row = event.target.closest("[data-campaign-lead-id]");
  if (!row) return;
  const leadId = row.dataset.campaignLeadId || "";
  const selectedIds = new Set(state.ui.campaignSelectedLeadIds || []);
  if (checkbox.checked) selectedIds.add(leadId);
  else selectedIds.delete(leadId);
  state.ui.campaignSelectedLeadIds = Array.from(selectedIds);
  state.ui.selectedCampaignLeadId = leadId;
  renderCampaignTable();
});

document.getElementById("campaignTable")?.addEventListener("keydown", (event) => {
  if (event.key !== "Enter") return;
  const row = event.target.closest("[data-campaign-lead-id]");
  if (!row) return;
  event.preventDefault();
  state.ui.selectedCampaignLeadId = row.dataset.campaignLeadId || "";
  renderCampaignTable();
});

document.getElementById("leadSelectTable")?.addEventListener("change", (event) => {
  const radio = event.target.closest('input[name="lead-select-radio"]');
  if (!radio) return;
  const row = event.target.closest("[data-lead-select-id]");
  if (!row) return;
  if (row.dataset.leadLocked === "true") return;
  state.ui.selectedLeadSelectionId = row.dataset.leadSelectId || "";
  renderLeadSelectionTable();
});

document.getElementById("leadSelectTable")?.addEventListener("keydown", (event) => {
  if (event.key !== "Enter") return;
  const row = event.target.closest("[data-lead-select-id]");
  if (!row) return;
  if (row.dataset.leadLocked === "true") {
    const status = document.getElementById("leadSelectStatus");
    if (status) status.textContent = "This lead is currently leased by another active session.";
    return;
  }
  event.preventDefault();
  state.ui.selectedLeadSelectionId = row.dataset.leadSelectId || "";
  renderLeadSelectionTable();
  const leadId = state.ui.selectedLeadSelectionId;
  if (!leadId) return;
  loadLeadIntoCallDesk(leadId);
  setActiveTab("calldesk");
});

document.getElementById("leadSelectLoadBtn")?.addEventListener("click", () => {
  const selectedFromRadio = document.querySelector('input[name="lead-select-radio"]:checked')?.closest("[data-lead-select-id]")?.dataset.leadSelectId || "";
  const leadId = selectedFromRadio || state.ui.selectedLeadSelectionId;
  if (!leadId) {
    const status = document.getElementById("leadSelectStatus");
    if (status) status.textContent = "Select a lead first.";
    return;
  }
  const exists = state.leads.some((row) => row.lead_external_id === leadId);
  if (!exists) {
    const status = document.getElementById("leadSelectStatus");
    if (status) status.textContent = "Selected lead no longer exists in current data.";
    return;
  }
  const selectedLead = state.leads.find((row) => String(row.lead_external_id || "") === String(leadId || ""));
  if (selectedLead && isLeadLeaseLocked(normalizeLeadRow(selectedLead))) {
    const status = document.getElementById("leadSelectStatus");
    if (status) status.textContent = "Lead is locked for 15 minutes to prevent double-dialing.";
    return;
  }
  loadLeadIntoCallDesk(leadId);
  const status = document.getElementById("leadSelectStatus");
  if (status) status.textContent = "Lead loaded into Call Desk.";
  setActiveTab("calldesk");
});

document.getElementById("calendarTabPanel")?.addEventListener("click", (event) => {
  const loadLeadBtn = event.target.closest("[data-calendar-load-lead]");
  if (loadLeadBtn) {
    const leadId = String(loadLeadBtn.dataset.calendarLoadLead || "").trim();
    if (!leadId) return;
    loadLeadIntoCallDesk(leadId);
    setActiveTab("calldesk");
    return;
  }

  const loadLocalBtn = event.target.closest("[data-calendar-load-local]");
  if (loadLocalBtn) {
    const leadId = String(loadLocalBtn.dataset.calendarLoadLocal || "").trim();
    if (!leadId) return;
    loadLeadIntoCallDesk(leadId);
    setActiveTab("calldesk");
  }
});

document.getElementById("campaignSelectVisibleBtn")?.addEventListener("click", () => {
  state.ui.campaignSelectedLeadIds = getCampaignFilteredLeads()
    .slice(0, LEAD_SELECTION_MAX_ROWS)
    .map((row) => row.lead_external_id);
  if (!state.ui.selectedCampaignLeadId) state.ui.selectedCampaignLeadId = state.ui.campaignSelectedLeadIds[0] || "";
  renderCampaignTable();
});

document.getElementById("campaignClearSelectionBtn")?.addEventListener("click", () => {
  state.ui.campaignSelectedLeadIds = [];
  renderCampaignTable();
});

document.getElementById("campaignCopySummaryBtn")?.addEventListener("click", () => {
  const rows = buildCampaignExportRows();
  const summary = [
    `Campaign angle: ${document.getElementById("campaignAngle")?.value || ""}`,
    `CTA: ${document.getElementById("campaignCta")?.value || ""}`,
    `Sender: ${document.getElementById("campaignSenderName")?.value || ""}`,
    `Selected leads: ${rows.length}`,
    "",
    ...rows.slice(0, 20).map((row) => `${row.full_name} <${row.email}> | ${row.routing_bucket} | ${row.recommended_next_action}`),
  ].join("\n");
  copyText(summary);
});

document.getElementById("campaignExportJsonBtn")?.addEventListener("click", () => {
  const payload = {
    exportedAt: new Date().toISOString(),
    source: "insurance-dashboard",
    mode: "draft_review",
    senderAccount: "hiltylena@gmail.com",
    leads: buildCampaignExportRows(),
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = `openclaw-email-batch-${Date.now()}.json`;
  link.click();
  URL.revokeObjectURL(link.href);
});

document.getElementById("campaignExportCsvBtn")?.addEventListener("click", () => {
  const rows = buildCampaignExportRows();
  if (!rows.length) return;
  const headers = Object.keys(rows[0]);
  const csv = [
    headers.join(","),
    ...rows.map((row) =>
      headers
        .map((key) => `"${String(row[key] ?? "").replaceAll('"', '""')}"`)
        .join(","),
    ),
  ].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = `openclaw-email-batch-${Date.now()}.csv`;
  link.click();
  URL.revokeObjectURL(link.href);
});

document.getElementById("clearTestDataBtn")?.addEventListener("click", () => {
  clearTestData().catch(() => {});
});

document.getElementById("authForm")?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const email = document.getElementById("authEmail")?.value?.trim() || "";
  const password = document.getElementById("authPassword")?.value || "";
  const submitBtn = document.getElementById("authSubmitBtn");
  if (!email || !password) {
    setAuthStatus("Enter your email and password.", "error");
    return;
  }
  if (submitBtn) submitBtn.disabled = true;
  setAuthStatus("Signing in…");
  try {
    const session = await signInToPortal(email, password);
    setPortalUser(session);
    setAuthLocked(false);
    setAuthStatus("");
    await initialize();
  } catch (error) {
    setAuthLocked(true);
    setAuthStatus(String(error.message || error), "error");
  } finally {
    if (submitBtn) submitBtn.disabled = false;
  }
});

document.getElementById("portalLogoutBtn")?.addEventListener("click", async () => {
  await signOutOfPortal();
});

attachFilterHandlers();
attachCriteriaHandlers();
attachTabHandlers();
initRoleView();
attachWorkflowHandlers();
attachNotesHandlers();
attachCallDeskHandlers();
attachCarrierSettingsHandlers();
attachPipelineHandlers();
attachLeadSelectionSortHandlers();
attachContentStudioHandlers();
syncWorkflowControls();
renderCallDeskBranching();
renderWorkflowAdvisor();
setActiveTab("dashboard");
bootstrapPortalAuth();
