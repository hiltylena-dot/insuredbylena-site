import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ARTIFACT_DIR = path.join(__dirname, "artifacts");

const PORTAL_URL = (process.env.PORTAL_URL || "https://insuredbylena.com/portal/").trim();
const SUPABASE_URL = (process.env.SUPABASE_URL || "").trim().replace(/\/$/, "");
const SUPABASE_SERVICE_ROLE_KEY = (process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function quote(value) {
  return encodeURIComponent(String(value ?? ""));
}

function nowStamp() {
  return String(Date.now());
}

function futureUtcIso(daysAhead, hour = 15, minute = 0) {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() + daysAhead);
  date.setUTCHours(hour, minute, 0, 0);
  return date.toISOString();
}

function futureLocalDate(daysAhead) {
  const date = new Date();
  date.setDate(date.getDate() + daysAhead);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function futureLocalDateTime(daysAhead, hour = 10, minute = 0) {
  const date = new Date();
  date.setDate(date.getDate() + daysAhead);
  date.setHours(hour, minute, 0, 0);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

async function ensureArtifactsDir() {
  await fs.mkdir(ARTIFACT_DIR, { recursive: true });
}

async function writeArtifact(name, value) {
  await ensureArtifactsDir();
  await fs.writeFile(path.join(ARTIFACT_DIR, name), value, "utf8");
}

async function jsonFetch(url, { method = "GET", body, headers = {} } = {}) {
  const response = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      ...headers,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const raw = await response.text();
  let data = {};
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    data = { ok: false, error: raw };
  }
  return { status: response.status, data, headers: response.headers };
}

async function supabaseRest(pathname, options = {}) {
  assert(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.");
  return jsonFetch(`${SUPABASE_URL}/rest/v1/${pathname.replace(/^\//, "")}`, {
    ...options,
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      ...(options.headers || {}),
    },
  });
}

async function supabaseAuthAdmin(pathname, options = {}) {
  assert(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.");
  return jsonFetch(`${SUPABASE_URL}/auth/v1/admin/${pathname.replace(/^\//, "")}`, {
    ...options,
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      ...(options.headers || {}),
    },
  });
}

async function createAdminUser(email, password) {
  const { status, data } = await supabaseAuthAdmin("users", {
    method: "POST",
    body: {
      email,
      password,
      email_confirm: true,
      user_metadata: {
        full_name: "Codex Frontend Smoke",
      },
    },
  });
  assert(status < 300 && data?.id, `Could not create smoke auth user: ${JSON.stringify(data)}`);
  return data;
}

async function deleteAdminUser(userId) {
  if (!userId) return;
  await supabaseAuthAdmin(`users/${userId}`, { method: "DELETE" }).catch(() => {});
}

async function createAdminProfile(userId, email) {
  const { status, data } = await supabaseRest("app_user_profile?select=user_id,email,role", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: {
      user_id: userId,
      email,
      full_name: "Codex Frontend Smoke",
      role: "admin",
    },
  });
  assert(status < 300, `Could not create smoke app_user_profile: ${JSON.stringify(data)}`);
}

async function deleteAdminProfile(userId) {
  if (!userId) return;
  await supabaseRest(`app_user_profile?user_id=eq.${quote(userId)}`, { method: "DELETE" }).catch(() => {});
}

async function createLead(contactId, phone, email, nextAppointmentTime) {
  const { status, data } = await supabaseRest("rpc/portal_save_call_desk", {
    method: "POST",
    body: {
      p_payload: {
        contactId,
        firstName: "Frontend",
        lastName: "Smoke",
        fullName: "Frontend Smoke",
        phone,
        email,
        disposition: "callback",
        shouldSchedule: true,
        nextAppointmentTime,
        notes: "Frontend smoke seed",
        nextStep: "Frontend smoke next step",
      },
    },
  });
  assert(status < 300 && data?.ok, `Could not seed smoke lead: ${JSON.stringify(data)}`);
  return data?.lead || {};
}

async function createContentPost(postKey, postDate) {
  const { status, data } = await supabaseRest("content_post?select=id,post_id,status", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: {
      post_id: postKey,
      week_number: 1,
      day: 1,
      post_date: postDate,
      post_time: "09:00",
      scheduled_for: null,
      platforms_json: ["facebook"],
      post_type: "social",
      topic: "Frontend smoke topic",
      hook: "Frontend smoke hook",
      caption: "Frontend smoke caption https://insuredbylena.com GUIDE",
      reel_script: "Frontend smoke script",
      visual_prompt: "Frontend smoke layout notes",
      canva_design_link: "https://www.canva.com/design/frontend-smoke",
      asset_filename: "https://example.com/frontend-smoke.jpg",
      cta: "GUIDE",
      hashtags_text: "#frontend #smoke",
      status: "draft",
      source_file: "frontend-smoke",
      created_by: "frontend-smoke",
    },
  });
  assert(status < 300 && Array.isArray(data) && data[0]?.id, `Could not seed smoke content post: ${JSON.stringify(data)}`);
  return data[0];
}

async function fetchSingle(pathname) {
  const { status, data } = await supabaseRest(pathname);
  assert(status < 300, `Fetch failed for ${pathname}: ${JSON.stringify(data)}`);
  return Array.isArray(data) ? data[0] || null : data;
}

async function waitFor(check, message, { attempts = 20, delayMs = 1000 } = {}) {
  let lastValue = null;
  for (let index = 0; index < attempts; index += 1) {
    lastValue = await check();
    if (lastValue) return lastValue;
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  throw new Error(message + (lastValue ? ` Last value: ${JSON.stringify(lastValue)}` : ""));
}

async function cleanupSeedData({ leadId, leadExternalId, email, contactId, contentPostId, contentPostKey }) {
  if (contentPostId) {
    await supabaseRest(`content_approval?content_post_id=eq.${contentPostId}`, { method: "DELETE" }).catch(() => {});
    await supabaseRest(`content_revision?content_post_id=eq.${contentPostId}`, { method: "DELETE" }).catch(() => {});
    await supabaseRest(`content_publish_job?content_post_id=eq.${contentPostId}`, { method: "DELETE" }).catch(() => {});
    await supabaseRest(`content_post?id=eq.${contentPostId}`, { method: "DELETE" }).catch(() => {});
  }
  if (contentPostKey) {
    await supabaseRest(`content_post?post_id=eq.${quote(contentPostKey)}`, { method: "DELETE" }).catch(() => {});
  }
  if (leadId) {
    await supabaseRest(`call_desk_activity?lead_id=eq.${leadId}`, { method: "DELETE" }).catch(() => {});
    await supabaseRest(`lead_document?lead_id=eq.${leadId}`, { method: "DELETE" }).catch(() => {});
    await supabaseRest(`appointment?lead_id=eq.${leadId}`, { method: "DELETE" }).catch(() => {});
    await supabaseRest(`lead_master?lead_id=eq.${leadId}`, { method: "DELETE" }).catch(() => {});
  }
  if (leadExternalId) {
    await supabaseRest(`lead_master?lead_external_id=eq.${quote(leadExternalId)}`, { method: "DELETE" }).catch(() => {});
  }
  if (contactId) {
    await supabaseRest(`lead_master?lead_external_id=eq.${quote(contactId)}`, { method: "DELETE" }).catch(() => {});
  }
  if (email) {
    await supabaseRest(`lead_master?email=eq.${quote(email)}`, { method: "DELETE" }).catch(() => {});
  }
}

async function main() {
  assert(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY, "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.");
  await ensureArtifactsDir();

  const stamp = nowStamp();
  const smokeEmail = `frontend-smoke-${stamp}@example.com`;
  const smokePassword = `CodexSmoke!${stamp.slice(-6)}`;
  const contactId = `FRONTEND-${stamp}`;
  const smokePhone = `557${stamp.slice(-7)}`;
  const contentPostKey = `FRONTEND-${stamp}`;
  const seededLeadTime = futureUtcIso(1, 15, 0);
  const deskFollowUpTime = futureLocalDateTime(2, 10, 30);
  const contentPostDate = futureLocalDate(2);
  const contentScheduleTime = futureLocalDateTime(2, 11, 15);

  let authUserId = "";
  let leadId = 0;
  let leadExternalId = "";
  let contentPostId = 0;
  let browser;
  let page;

  try {
    const user = await createAdminUser(smokeEmail, smokePassword);
    authUserId = user.id;
    await createAdminProfile(authUserId, smokeEmail);

    const seededLead = await createLead(contactId, smokePhone, smokeEmail, seededLeadTime);
    leadId = Number(seededLead.lead_id || 0);
    leadExternalId = String(seededLead.lead_external_id || contactId);

    const seededPost = await createContentPost(contentPostKey, contentPostDate);
    contentPostId = Number(seededPost.id || 0);

    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({ viewport: { width: 1440, height: 1100 } });
    page = await context.newPage();
    await page.route("https://script.google.com/macros/**", (route) => route.abort());
    page.on("dialog", (dialog) => dialog.dismiss().catch(() => {}));

    await page.goto(PORTAL_URL, { waitUntil: "networkidle" });
    await page.locator("#authEmail").waitFor({ state: "visible" });
    await page.fill("#authEmail", smokeEmail);
    await page.fill("#authPassword", smokePassword);
    await page.click("#authSubmitBtn");
    await page.locator("#dashboardTabButton[aria-selected='true']").waitFor({ state: "visible" });
    await page.locator("#portalUserEmail").waitFor({ state: "visible" });
    assert((await page.locator("#portalUserEmail").textContent())?.includes(smokeEmail), "Portal user email did not render after sign-in.");

    await page.click("#runHealthCheckBtn");
    await page.waitForFunction(() => {
      const node = document.getElementById("healthCheckSummary");
      return node && node.textContent && !/not run yet/i.test(node.textContent) && !/running/i.test(node.textContent);
    });

    await page.click("#leadSelectionTabButton");
    await page.locator("#leadSelectSearch").fill(contactId);
    await page.locator(`#leadSelectTable tr[data-lead-select-id="${leadExternalId}"]`).waitFor({ state: "visible" });
    await page.click(`#leadSelectTable tr[data-lead-select-id="${leadExternalId}"] input[name="lead-select-radio"]`);
    await page.click("#leadSelectLoadBtn");
    await page.locator("#callDeskTabButton[aria-selected='true']").waitFor({ state: "visible" });
    await page.waitForFunction((name) => {
      const node = document.getElementById("lead360Client");
      return node && node.textContent && node.textContent.toLowerCase().includes(name.toLowerCase());
    }, "Frontend Smoke");

    await page.selectOption("#deskDisposition", "follow_up");
    await page.fill("#deskNextStep", "CI smoke follow-up");
    await page.fill("#deskCallNotes", "Frontend smoke save from Playwright.");
    await page.fill("#deskFollowUp", deskFollowUpTime);
    await page.click("#deskSaveToNotesBtn");
    await page.waitForFunction(() => {
      const btn = document.getElementById("deskSaveToNotesBtn");
      return btn && btn.textContent && !/syncing to drive/i.test(btn.textContent);
    });

    const savedLead = await waitFor(
      async () => {
        const lead = await fetchSingle(
          `lead_master?select=lead_id,disposition,next_appointment_time&lead_external_id=eq.${quote(leadExternalId)}&limit=1`
        );
        if (lead?.disposition === "follow_up" && lead?.next_appointment_time) return lead;
        return null;
      },
      `Lead did not reach follow_up state from UI save for ${leadExternalId}.`,
    );
    assert(savedLead?.disposition === "follow_up", `Lead disposition did not save from UI: ${JSON.stringify(savedLead)}`);
    assert(Boolean(savedLead?.next_appointment_time), `Lead follow-up time missing after UI save: ${JSON.stringify(savedLead)}`);

    const savedAppointment = await waitFor(
      async () => {
        const appointment = await fetchSingle(
          `appointment?select=appointment_id,booking_status,appointment_type&lead_id=eq.${leadId}&owner=eq.call_desk&booking_status=in.(Booked,Rescheduled,Pending)&limit=1`
        );
        if (appointment?.appointment_id) return appointment;
        return null;
      },
      `Active appointment did not appear after UI save for lead ${leadId}.`,
    );
    assert(Boolean(savedAppointment?.appointment_id), `Active appointment missing after UI save: ${JSON.stringify(savedAppointment)}`);

    await page.click("#calendarTabButton");
    await page.locator("#calendarTabPanel:not([hidden])").waitFor({ state: "visible" });

    await page.click("#pipelineTabButton");
    await page.locator("#pipelineTabPanel:not([hidden])").waitFor({ state: "visible" });

    await page.click("#contentStudioTabButton");
    await page.locator("#contentQuickPick").waitFor({ state: "visible" });
    await page.waitForFunction(
      (targetId) => {
        const select = document.getElementById("contentQuickPick");
        return select && Array.from(select.options || []).some((option) => String(option.value) === String(targetId));
      },
      String(contentPostId),
    );
    await page.selectOption("#contentQuickPick", String(contentPostId));
    await page.waitForFunction(
      (postKey) => {
        const postIdInput = document.getElementById("contentEditPostId");
        const topicInput = document.getElementById("contentEditTopic");
        return (
          (postIdInput && String(postIdInput.value) === String(postKey))
          || (topicInput && String(topicInput.value) === "Frontend smoke topic")
        );
      },
      String(contentPostKey),
    );

    await page.fill("#contentEditTopic", "Frontend smoke topic updated");
    await page.fill("#contentEditCaption", "Frontend smoke caption updated https://insuredbylena.com GUIDE");
    await page.fill("#contentEditScheduledFor", contentScheduleTime);
    await page.locator("#contentEditScheduledFor").press("Tab");

    await page.click("#contentSaveBtn");
    const savedDraftPost = await waitFor(
      async () => {
        const post = await fetchSingle(
          `content_post?select=id,status,topic,scheduled_for&post_id=eq.${quote(contentPostKey)}&limit=1`
        );
        if (post?.topic === "Frontend smoke topic updated") return post;
        return null;
      },
      `Content draft did not save from UI for ${contentPostKey}.`,
    );
    assert(savedDraftPost?.topic === "Frontend smoke topic updated", `Content topic did not save from UI: ${JSON.stringify(savedDraftPost)}`);

    const revisionRows = await waitFor(
      async () => {
        const revision = await fetchSingle(
          `content_revision?select=id,change_note&content_post_id=eq.${contentPostId}&order=id.desc&limit=1`
        );
        if (revision?.id) return revision;
        return null;
      },
      `Content revision row missing after UI save for ${contentPostId}.`,
    );
    assert(Boolean(revisionRows?.id), `Content revision row missing after UI save: ${JSON.stringify(revisionRows)}`);

    await page.click("#contentApproveBtn");
    const approvedPost = await waitFor(
      async () => {
        const post = await fetchSingle(
          `content_post?select=id,status,approved_at&post_id=eq.${quote(contentPostKey)}&limit=1`
        );
        if (post?.status === "approved" && post?.approved_at) return post;
        return null;
      },
      `Content post did not reach approved state from UI for ${contentPostKey}.`,
    );
    assert(approvedPost?.status === "approved", `Content post did not reach approved state: ${JSON.stringify(approvedPost)}`);

    const approvalRows = await waitFor(
      async () => {
        const approval = await fetchSingle(
          `content_approval?select=id,decision&content_post_id=eq.${contentPostId}&order=id.desc&limit=1`
        );
        if (approval?.id) return approval;
        return null;
      },
      `Content approval row missing after UI approve for ${contentPostId}.`,
    );
    assert(Boolean(approvalRows?.id), `Content approval row missing after UI approve: ${JSON.stringify(approvalRows)}`);

    await page.click("#contentScheduleBtn");
    const scheduledPost = await waitFor(
      async () => {
        const post = await fetchSingle(
          `content_post?select=id,status,topic,scheduled_for&post_id=eq.${quote(contentPostKey)}&limit=1`
        );
        if (post?.status === "scheduled" && post?.scheduled_for) return post;
        return null;
      },
      `Content post did not reach scheduled state from UI for ${contentPostKey}.`,
    );
    assert(scheduledPost?.status === "scheduled", `Content post did not reach scheduled state: ${JSON.stringify(scheduledPost)}`);
    assert(Boolean(scheduledPost?.scheduled_for), `Scheduled post missing scheduled_for: ${JSON.stringify(scheduledPost)}`);

    const savedPost = await fetchSingle(
      `content_post?select=id,status,topic,scheduled_for&post_id=eq.${quote(contentPostKey)}&limit=1`
    );
    assert(savedPost?.topic === "Frontend smoke topic updated", `Content topic changed unexpectedly after schedule: ${JSON.stringify(savedPost)}`);
    assert(savedPost?.status === "scheduled", `Content post did not stay scheduled: ${JSON.stringify(savedPost)}`);
    assert(Boolean(savedPost?.scheduled_for), `Scheduled post missing scheduled_for after final fetch: ${JSON.stringify(savedPost)}`);

    const finalApprovalRows = await fetchSingle(
      `content_approval?select=id,decision&content_post_id=eq.${contentPostId}&order=id.desc&limit=1`
    );
    assert(Boolean(finalApprovalRows?.id), `Content approval row missing after final fetch: ${JSON.stringify(finalApprovalRows)}`);

    console.log(
      JSON.stringify({
        ok: true,
        portal: PORTAL_URL,
        leadId,
        leadExternalId,
        contentPostId,
        smokeUser: smokeEmail,
      }),
    );
  } catch (error) {
    const message = String(error?.stack || error?.message || error);
    if (page) {
      try {
        await page.screenshot({ path: path.join(ARTIFACT_DIR, "frontend-smoke-failure.png"), fullPage: true });
        await writeArtifact("frontend-smoke-page.html", await page.content());
      } catch {}
    }
    await writeArtifact("frontend-smoke-error.txt", message).catch(() => {});
    console.error(JSON.stringify({ ok: false, error: message }));
    process.exitCode = 1;
  } finally {
    if (browser) await browser.close().catch(() => {});
    await cleanupSeedData({ leadId, leadExternalId, email: smokeEmail, contactId, contentPostId, contentPostKey });
    await deleteAdminProfile(authUserId);
    await deleteAdminUser(authUserId);
  }
}

main();
