const DEFAULTS = {
  sheetName: "Intake",
  notifyTo: "hello@insuredbylena.com",
};

function doGet() {
  return jsonResponse_({ ok: true, service: "insuredbylena-intake" });
}

function doPost(e) {
  try {
    const payload = parsePayload_(e);
    const source = clean_(payload.source || payload.formSource || "consultation");
    const spreadsheetId = getProp_("SPREADSHEET_ID");
    if (!spreadsheetId) throw new Error("Missing SPREADSHEET_ID script property.");

    const sheetName = getProp_("SHEET_NAME") || DEFAULTS.sheetName;
    const ss = SpreadsheetApp.openById(spreadsheetId);
    const sheet = getOrCreateSheet_(ss, sheetName);
    ensureHeaders_(sheet);

    const row = buildRow_(payload, source);
    sheet.appendRow(row);

    sendNotification_(payload, source, row);

    return jsonResponse_({
      ok: true,
      source,
      rowNumber: sheet.getLastRow(),
    });
  } catch (err) {
    return jsonResponse_({
      ok: false,
      error: String(err && err.message ? err.message : err),
    });
  }
}

function parsePayload_(e) {
  if (!e) return {};

  const rawBody = e.postData && e.postData.contents ? String(e.postData.contents).trim() : "";
  const contentType = String((e.postData && (e.postData.type || e.postData.mimeType)) || "").toLowerCase();

  if (rawBody && (contentType.includes("application/json") || rawBody.startsWith("{") || rawBody.startsWith("["))) {
    try {
      return JSON.parse(rawBody);
    } catch (_err) {
      // Fall through to form fields.
    }
  }

  const p = e.parameter || {};
  return {
    source: p.source || p.formSource || "",
    fullName: p.fullName || p.name || "",
    email: p.email || "",
    phone: p.phone || "",
    zipCode: p.zipCode || p.zip_code || "",
    coverageNeed: p.coverageNeed || p.coverage_need || "",
    preferredTime: p.preferredTime || p.preferred_time || "",
    message: p.message || "",
    experience: p.experience || "",
    pageUrl: p.pageUrl || p.page_url || "",
    userAgent: p.userAgent || p.user_agent || "",
  };
}

function buildRow_(payload, source) {
  const fullName = clean_(payload.fullName || payload.name || "");
  const email = clean_(payload.email);
  const phone = clean_(payload.phone);
  const zipCode = clean_(payload.zipCode || payload.zip_code);
  const coverageNeed = clean_(payload.coverageNeed || payload.coverage_need || (source === "join_team" ? "Join My Team" : ""));
  const preferredTime = clean_(payload.preferredTime || payload.preferred_time);
  const message = clean_(payload.message);
  const experience = clean_(payload.experience);
  const pageUrl = clean_(payload.pageUrl || payload.page_url);
  const userAgent = clean_(payload.userAgent || payload.user_agent);

  return [
    new Date(),
    source,
    fullName,
    email,
    phone,
    zipCode,
    coverageNeed,
    preferredTime,
    message,
    experience,
    pageUrl,
    userAgent,
  ];
}

function ensureHeaders_(sheet) {
  const headers = [
    "Timestamp",
    "Source",
    "Full Name",
    "Email",
    "Phone",
    "ZIP",
    "Coverage Need",
    "Preferred Time",
    "Message",
    "Experience",
    "Page URL",
    "User Agent",
  ];
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(headers);
  }
}

function getOrCreateSheet_(ss, sheetName) {
  const existing = ss.getSheetByName(sheetName);
  if (existing) return existing;
  return ss.insertSheet(sheetName);
}

function sendNotification_(payload, source, row) {
  const recipientList = (getProp_("NOTIFY_TO") || DEFAULTS.notifyTo)
    .split(",")
    .map((value) => String(value).trim())
    .filter(Boolean);
  if (!recipientList.length) return;

  const fullName = clean_(payload.fullName || payload.name || "Prospect");
  const email = clean_(payload.email);
  const phone = clean_(payload.phone);
  const coverageNeed = clean_(payload.coverageNeed || payload.coverage_need || "");
  const preferredTime = clean_(payload.preferredTime || payload.preferred_time || "");
  const message = clean_(payload.message);
  const experience = clean_(payload.experience);

  const isTeamInquiry = source === "join_team" || source === "join_team_form" || source === "team_join" || source === "recruiting";
  const subject = isTeamInquiry
    ? `New team inquiry: ${fullName}`
    : `New consultation request: ${fullName}`;

  const lines = [
    isTeamInquiry
      ? "A new Join My Team inquiry came in from the landing page."
      : "A new consultation request came in from the landing page.",
    "",
    `Name: ${fullName}`,
    `Email: ${email || "-"}`,
    `Phone: ${phone || "-"}`,
    isTeamInquiry ? `Experience: ${experience || "-"}` : `Coverage need: ${coverageNeed || "-"}`,
    preferredTime ? `Preferred time: ${preferredTime}` : "",
    "",
    "Message:",
    message || "-",
    "",
    `Source: ${source}`,
    `Sheet row: ${row && row.length ? "appended" : "saved"}`,
  ].filter(Boolean);

  GmailApp.sendEmail(recipientList.join(","), subject, lines.join("\n"), {
    replyTo: email || undefined,
    name: "Insured by Lena",
  });
}

function getProp_(name) {
  return PropertiesService.getScriptProperties().getProperty(name) || "";
}

function clean_(value) {
  return String(value || "").trim();
}

function jsonResponse_(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}
