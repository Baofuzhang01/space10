// ====================================================================
// 多学校抢座管理中枢 — Cloudflare Worker
// ====================================================================
// 功能:
//   1. scheduled()  在预约窗口内轮询学校，并在每次 Cron 触发时立即写入心跳到 KV
//   2. fetch()      REST API + 内嵌 Web 管理面板
//
// KV Schema (binding: SEAT_KV):
//   schools                     → 学校 ID 列表 ["001", "002", "003"]
//   school:{id}                 → 学校配置 { id, name, trigger_time, endtime, repo, github_token_key, dispatch_target, server_url, strategy }
//   school:{id}:users           → 用户 ID 列表
//   school:{id}:user:{userId}   → 单用户完整配置
//
// Secrets: GH_TOKEN, API_KEY
// ====================================================================

const AES_KEY_RAW = "u2oh6Vu^HWe4_AES";

async function getAesKey() {
  const raw = new TextEncoder().encode(AES_KEY_RAW);
  return crypto.subtle.importKey("raw", raw, { name: "AES-CBC" }, false, ["encrypt"]);
}

function pkcs7Pad(data) {
  const bs = 16;
  const pad = bs - (data.length % bs);
  const out = new Uint8Array(data.length + pad);
  out.set(data);
  out.fill(pad, data.length);
  return out;
}

async function aesEncrypt(plaintext) {
  const key = await getAesKey();
  const iv = new TextEncoder().encode(AES_KEY_RAW);
  const padded = pkcs7Pad(new TextEncoder().encode(plaintext));
  const encrypted = await crypto.subtle.encrypt({ name: "AES-CBC", iv }, key, padded);
  return btoa(String.fromCharCode(...new Uint8Array(encrypted)));
}

// ─── 辅助函数 ───

function beijingNow() {
  return new Date(Date.now() + 8 * 3600 * 1000);
}

function beijingHHMM() {
  const d = beijingNow();
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

function beijingDate() {
  const d = beijingNow();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function beijingDayOfWeek() {
  const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
  return days[beijingNow().getUTCDay()];
}

function beijingDateHour() {
  const d = beijingNow();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  return `${y}-${m}-${day}-${hour}`;
}

function beijingDateMinute(timestampMs = Date.now()) {
  const d = new Date(timestampMs + 8 * 3600 * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  const minute = String(d.getUTCMinutes()).padStart(2, "0");
  return `${y}-${m}-${day}-${hour}:${minute}`;
}

function generateId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}

function jsonResp(data, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-store, no-cache, must-revalidate",
      ...extraHeaders,
    },
  });
}

function normalizeSecretText(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeEndtimeHms(value) {
  const text = String(value || "").trim();
  const match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return "";

  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  const second = parseInt(match[3] || "0", 10);
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return "";
  }

  return [
    String(hour).padStart(2, "0"),
    String(minute).padStart(2, "0"),
    String(second).padStart(2, "0"),
  ].join(":");
}

const FORMAL_TIME_WINDOW_LIMIT_SECONDS = 30 * 60;

function parseTriggerTimeSeconds(value) {
  const text = String(value || "").trim();
  const match = text.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return null;

  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
  return hour * 3600 + minute * 60;
}

function parseEndtimeSeconds(value) {
  const normalized = normalizeEndtimeHms(value);
  if (!normalized) return null;

  const [hour, minute, second] = normalized.split(":").map(v => parseInt(v, 10));
  return hour * 3600 + minute * 60 + second;
}

function validateFormalTimeWindow(triggerTime, endtime) {
  const startSeconds = parseTriggerTimeSeconds(triggerTime);
  if (startSeconds === null) return "正式开始时间格式应为 HH:MM";

  const endSeconds = parseEndtimeSeconds(endtime);
  if (endSeconds === null) return "正式截止时间格式应为 HH:MM:SS";

  const durationSeconds = endSeconds - startSeconds;
  if (durationSeconds <= 0) return "正式截止时间必须晚于正式开始时间";
  if (durationSeconds > FORMAL_TIME_WINDOW_LIMIT_SECONDS) {
    return "正式开始时间和截止时间间隔不能超过 30 分钟";
  }
  return "";
}

function parseTriggerTimeMinutes(value) {
  const text = String(value || "").trim();
  const match = text.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return Number.MAX_SAFE_INTEGER;
  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  if (Number.isNaN(hour) || Number.isNaN(minute)) return Number.MAX_SAFE_INTEGER;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return Number.MAX_SAFE_INTEGER;
  return hour * 60 + minute;
}

function getSortedSchoolsForDisplay(items) {
  return (Array.isArray(items) ? items : [])
    .filter(Boolean)
    .slice()
    .sort((a, b) => {
      const timeDiff = parseTriggerTimeMinutes(a?.trigger_time) - parseTriggerTimeMinutes(b?.trigger_time);
      if (timeDiff !== 0) return timeDiff;
      return String(a?.id || "").localeCompare(String(b?.id || ""));
    });
}

function normalizeConflictGroup(value) {
  return normalizeSecretText(value).toLowerCase();
}

function getSchoolConflictGroup(school) {
  const explicitGroup = normalizeConflictGroup(school?.conflict_group);
  if (explicitGroup) {
    return `group:${explicitGroup}`;
  }

  const fidEnc = normalizeConflictGroup(school?.fidEnc);
  if (fidEnc) return `fid:${fidEnc}`;

  return normalizeConflictGroup(school?.name);
}

const GITHUB_TOKEN_BINDINGS = {
  a: "GH_TOKEN_A",
  b: "GH_TOKEN_B",
  c: "GH_TOKEN_C",
  d: "GH_TOKEN_D",
  e: "GH_TOKEN_E",
};

function resolveGitHubToken(env, school = null) {
  const tokenKey = normalizeSecretText(school?.github_token_key).toLowerCase();
  const bindingName = GITHUB_TOKEN_BINDINGS[tokenKey];
  if (bindingName) {
    const boundToken = normalizeSecretText(env?.[bindingName]);
    if (boundToken) return boundToken;
  }
  const schoolToken = normalizeSecretText(school?.github_token);
  if (schoolToken) return schoolToken;
  return normalizeSecretText(env?.GH_TOKEN);
}

function resolveServerApiKey(env, school = null) {
  const schoolKey = normalizeSecretText(school?.server_api_key);
  if (schoolKey) return schoolKey;
  return normalizeSecretText(env?.SERVER_DISPATCH_API_KEY);
}

function resolveDispatchTarget(school = null) {
  const raw = normalizeSecretText(school?.dispatch_target).toLowerCase();
  if (raw === "server") return "server_relay";
  return ["github", "server_relay", "both"].includes(raw) ? raw : "github";
}

function parseReserveDayOffset(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (!/^-?\d+$/.test(text)) return null;
  const offset = parseInt(text, 10);
  if (Number.isNaN(offset)) return null;
  return Math.max(0, offset);
}

function resolveReserveDayOffset(env, school = null) {
  const dispatchTarget = resolveDispatchTarget(school);
  if (dispatchTarget !== "server_relay") return null;

  const directOffset = parseReserveDayOffset(school?.reserve_day_offset);
  if (directOffset !== null) return directOffset;

  const rawMap = normalizeSecretText(
    env?.SCHOOL_RESERVE_DAY_OFFSETS || env?.RESERVE_DAY_OFFSETS
  );
  if (!rawMap || !school) return null;

  try {
    const parsed = JSON.parse(rawMap);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      const candidates = [school.id, school.name].map(v => String(v || "").trim()).filter(Boolean);
      for (const key of candidates) {
        const offset = parseReserveDayOffset(parsed[key]);
        if (offset !== null) return offset;
      }
    }
  } catch (_) {
    for (const item of rawMap.split(",")) {
      const [key, value] = item.split(/[:=]/, 2).map(v => String(v || "").trim());
      if (!key || key !== String(school.id || "").trim()) continue;
      const offset = parseReserveDayOffset(value);
      if (offset !== null) return offset;
    }
  }

  return null;
}

const TEST_ENDTIME_OVERRIDE_TTL_MS = 3 * 60 * 1000;

function getActiveTestEndtimeOverride(school, nowMs = Date.now()) {
  const override = school?.test_endtime_override;
  if (!override || typeof override !== "object") return null;

  const endtime = normalizeEndtimeHms(override.endtime);
  const expiresMs = Date.parse(override.expires_at || "");
  if (!endtime || !Number.isFinite(expiresMs) || expiresMs <= nowMs) return null;

  return {
    endtime,
    enabled_at: override.enabled_at || "",
    expires_at: new Date(expiresMs).toISOString(),
    remaining_seconds: Math.max(0, Math.ceil((expiresMs - nowMs) / 1000)),
  };
}

function resolveEffectiveEndtime(school, options = {}) {
  const allowTestEndtimeOverride = options.allowTestEndtimeOverride !== false;
  const activeOverride = allowTestEndtimeOverride
    ? getActiveTestEndtimeOverride(school)
    : null;
  return activeOverride?.endtime || normalizeEndtimeHms(school?.endtime) || "20:00:40";
}

function sanitizeSchoolForClient(school) {
  if (!school || typeof school !== "object") return school;
  const hasGitHubToken = !!normalizeSecretText(school.github_token);
  const hasServerApiKey = !!normalizeSecretText(school.server_api_key);
  const tokenKey = normalizeSecretText(school.github_token_key).toLowerCase();
  const activeTestEndtime = getActiveTestEndtimeOverride(school);
  const { github_token, server_api_key, test_endtime_override, ...rest } = school;
  return {
    ...rest,
    test_endtime: normalizeEndtimeHms(school.test_endtime) || "",
    test_endtime_override_active: !!activeTestEndtime,
    test_endtime_override_endtime: activeTestEndtime?.endtime || "",
    test_endtime_override_expires_at: activeTestEndtime?.expires_at || "",
    test_endtime_remaining_seconds: activeTestEndtime?.remaining_seconds || 0,
    effective_endtime: resolveEffectiveEndtime(school),
    github_token_key: tokenKey,
    dispatch_target: resolveDispatchTarget(school),
    has_github_token: hasGitHubToken || !!tokenKey,
    has_server_api_key: hasServerApiKey,
  };
}

const HEARTBEAT_LAST_TS_KEY = "meta:heartbeat:last_ts";
const HEARTBEAT_LAST_MINUTE_KEY = "meta:heartbeat:last_minute";
const FALLBACK_TRIGGER_PREFIX = "meta:fallback_trigger";
const FALLBACK_TRIGGER_TTL_SECONDS = 14 * 24 * 60 * 60;
const SCHOOLS_SNAPSHOT_KEY = "meta:schools:full";

function schoolUsersSnapshotKey(schoolId) {
  return `school:${schoolId}:users:full`;
}

// ─── KV 操作 ───

async function getSchools(KV) {
  const raw = await KV.get("schools");
  return raw ? JSON.parse(raw) : [];
}

async function saveSchools(KV, schools) {
  await KV.put("schools", JSON.stringify(schools));
}

async function getSchoolsSnapshot(KV) {
  const raw = await KV.get(SCHOOLS_SNAPSHOT_KEY);
  if (raw) return getSortedSchoolsForDisplay(JSON.parse(raw));

  const schoolIds = await getSchools(KV);
  if (schoolIds.length === 0) return [];

  const schools = [];
  for (const schoolId of schoolIds) {
    const school = await getSchool(KV, schoolId);
    if (!school) continue;
    const userIds = await getSchoolUsers(KV, schoolId);
    schools.push({ ...school, userCount: userIds.length });
  }
  const nextSchools = getSortedSchoolsForDisplay(schools);
  await saveSchoolsSnapshot(KV, nextSchools);
  return nextSchools;
}

async function saveSchoolsSnapshot(KV, schools) {
  await KV.put(SCHOOLS_SNAPSHOT_KEY, JSON.stringify(getSortedSchoolsForDisplay(schools)));
}

async function upsertSchoolInSnapshot(KV, school, userCount = null) {
  const schools = await getSchoolsSnapshot(KV);
  const existing = schools.find(item => item && item.id === school.id);
  const nextSchool = {
    ...(existing || {}),
    ...school,
    userCount: userCount ?? existing?.userCount ?? 0,
  };
  const nextSchools = schools.filter(item => item && item.id !== school.id);
  nextSchools.push(nextSchool);
  await saveSchoolsSnapshot(KV, nextSchools);
}

async function removeSchoolFromSnapshot(KV, schoolId) {
  const schools = await getSchoolsSnapshot(KV);
  await saveSchoolsSnapshot(
    KV,
    schools.filter(item => item && item.id !== schoolId)
  );
}

async function setSchoolUserCountInSnapshot(KV, schoolId, userCount) {
  const schools = await getSchoolsSnapshot(KV);
  const nextSchools = schools.map(item => (
    item && item.id === schoolId ? { ...item, userCount } : item
  ));
  await saveSchoolsSnapshot(KV, nextSchools);
}

async function getSchool(KV, schoolId) {
  const raw = await KV.get(`school:${schoolId}`);
  return raw ? JSON.parse(raw) : null;
}

async function saveSchool(KV, school) {
  await Promise.all([
    KV.put(`school:${school.id}`, JSON.stringify(school)),
    upsertSchoolInSnapshot(KV, school),
  ]);
}

async function deleteSchool(KV, schoolId) {
  // 删除学校配置
  await KV.delete(`school:${schoolId}`);
  // 删除学校下所有用户
  const userIds = await getSchoolUsers(KV, schoolId);
  for (const uid of userIds) {
    await KV.delete(`school:${schoolId}:user:${uid}`);
  }
  await KV.delete(`school:${schoolId}:users`);
  await KV.delete(schoolUsersSnapshotKey(schoolId));
  // 从学校列表移除
  const schools = await getSchools(KV);
  await Promise.all([
    saveSchools(KV, schools.filter(id => id !== schoolId)),
    removeSchoolFromSnapshot(KV, schoolId),
  ]);
}

async function getSchoolUsers(KV, schoolId) {
  const raw = await KV.get(`school:${schoolId}:users`);
  return raw ? JSON.parse(raw) : [];
}

async function saveSchoolUsers(KV, schoolId, userIds) {
  await KV.put(`school:${schoolId}:users`, JSON.stringify(userIds));
}

async function getSchoolUsersSnapshot(KV, schoolId) {
  const raw = await KV.get(schoolUsersSnapshotKey(schoolId));
  if (raw) return JSON.parse(raw);

  const userIds = await getSchoolUsers(KV, schoolId);
  if (userIds.length === 0) return [];

  const users = [];
  for (const userId of userIds) {
    const user = await getUser(KV, schoolId, userId);
    if (user) users.push(user);
  }
  await saveSchoolUsersSnapshot(KV, schoolId, users);
  return users;
}

async function saveSchoolUsersSnapshot(KV, schoolId, users) {
  await KV.put(schoolUsersSnapshotKey(schoolId), JSON.stringify(users));
}

async function upsertUserInSnapshot(KV, schoolId, user) {
  const users = await getSchoolUsersSnapshot(KV, schoolId);
  const nextUsers = users.filter(item => item && item.id !== user.id);
  nextUsers.push(user);
  await saveSchoolUsersSnapshot(KV, schoolId, nextUsers);
}

async function removeUserFromSnapshot(KV, schoolId, userId) {
  const users = await getSchoolUsersSnapshot(KV, schoolId);
  await saveSchoolUsersSnapshot(
    KV,
    schoolId,
    users.filter(item => item && item.id !== userId)
  );
}

async function getUser(KV, schoolId, userId) {
  const raw = await KV.get(`school:${schoolId}:user:${userId}`);
  return raw ? JSON.parse(raw) : null;
}

async function saveUser(KV, schoolId, user) {
  await Promise.all([
    KV.put(`school:${schoolId}:user:${user.id}`, JSON.stringify(user)),
    upsertUserInSnapshot(KV, schoolId, user),
  ]);
}

async function deleteUser(KV, schoolId, userId) {
  const userIds = await getSchoolUsers(KV, schoolId);
  await Promise.all([
    KV.delete(`school:${schoolId}:user:${userId}`),
    saveSchoolUsers(KV, schoolId, userIds.filter(id => id !== userId)),
    removeUserFromSnapshot(KV, schoolId, userId),
  ]);
}

function minuteBucket(timestampMs) {
  return Math.floor(timestampMs / 60000);
}

async function getHeartbeatTimestamp(KV) {
  const raw = await KV.get(HEARTBEAT_LAST_TS_KEY);
  const ts = parseInt(String(raw || "").trim(), 10);
  if (Number.isNaN(ts) || ts <= 0) return null;
  return ts;
}

async function getHeartbeatMinuteSlot(KV) {
  const raw = await KV.get(HEARTBEAT_LAST_MINUTE_KEY);
  const slot = String(raw || "").trim();
  return slot || null;
}

async function sleep(ms) {
  if (ms <= 0) return;
  await new Promise(resolve => setTimeout(resolve, ms));
}

async function writeHeartbeatTimestamp(KV, timestampMs = Date.now()) {
  const currentMinuteSlot = beijingDateMinute(timestampMs);
  await Promise.all([
    KV.put(HEARTBEAT_LAST_TS_KEY, String(timestampMs)),
    KV.put(HEARTBEAT_LAST_MINUTE_KEY, currentMinuteSlot),
  ]);
  return {
    written: true,
    timestamp: timestampMs,
    minuteSlot: currentMinuteSlot,
    minuteBucket: minuteBucket(timestampMs),
  };
}

function buildFallbackTriggerKey(date, schoolId) {
  return `${FALLBACK_TRIGGER_PREFIX}:${date}:${schoolId}`;
}

async function getFallbackTriggerRecord(KV, date, schoolId) {
  const raw = await KV.get(buildFallbackTriggerKey(date, schoolId));
  return raw ? JSON.parse(raw) : null;
}

async function saveFallbackTriggerRecord(KV, date, schoolId, record) {
  await KV.put(
    buildFallbackTriggerKey(date, schoolId),
    JSON.stringify(record),
    {
      // 兜底标记是按“学校 + 日期”生成的，会自然累积；这里保留 14 天方便回看，同时避免无限增长。
      expirationTtl: FALLBACK_TRIGGER_TTL_SECONDS,
    }
  );
}

// ─── 默认配置 ───

function defaultSchool(id, name) {
  return {
    id,
    name,
    conflict_group: "",
    trigger_time: "19:57",
    endtime: "20:00:40",
    test_endtime: "",
    test_endtime_override: null,
    seat_api_mode: "seat",
    reserve_next_day: true,
    reserve_day_offset: null,
    enable_slider: false,
    enable_textclick: false,
    fidEnc: "",
    reading_zone_groups: [],
    repo: `BAOfuZhan/${id}`,
    dispatch_target: "github",
    github_token_key: "",
    github_token: "",
    server_url: "",
    server_api_key: "",
    server_max_concurrency: 13,
    strategy: {
      mode: "C",
      submit_mode: "serial",
      login_lead_seconds: 18,
      slider_lead_seconds: 10,
      fast_probe_start_offset_ms: 14,
      fast_probe_start_range_ms: [14, 14],
      warm_connection_lead_ms: 2400,
      pre_fetch_token_ms: 1531,
      first_submit_offset_ms: 9,
      token_fetch_delay_ms: 45,
      first_token_date_mode: "submit_date",
    },
  };
}

function defaultUser(id) {
  return {
    id,
    phone: "",
    username: "",
    password: "",
    remark: "",
    status: "active",
    schedule: {
      Monday: { enabled: false, slots: [{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""}] },
      Tuesday: { enabled: false, slots: [{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""}] },
      Wednesday: { enabled: false, slots: [{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""}] },
      Thursday: { enabled: false, slots: [{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""}] },
      Friday: { enabled: false, slots: [{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""}] },
      Saturday: { enabled: false, slots: [{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""}] },
      Sunday: { enabled: false, slots: [{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""},{roomid:"",seatid:"",times:"",seatPageId:"",fidEnc:""}] },
    },
  };
}

function getEnabledScheduleSlots(daySchedule) {
  if (!daySchedule || !daySchedule.enabled) return [];
  const rawSlots = Array.isArray(daySchedule.slots)
    ? daySchedule.slots
    : [{
        roomid: daySchedule.roomid,
        seatid: daySchedule.seatid,
        times: daySchedule.times,
        seatPageId: daySchedule.seatPageId || "",
        fidEnc: daySchedule.fidEnc || "",
      }];
  return rawSlots.filter(slot => slot && slot.times && slot.roomid);
}

// ─── GitHub Dispatch ───

async function dispatchGitHub(token, repo, payload) {
  try {
    const res = await fetch(`https://api.github.com/repos/${repo}/dispatches`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "User-Agent": "TongYi-Worker",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({ event_type: "reserve", client_payload: payload }),
    });
    return res.status === 204;
  } catch (e) {
    console.error("dispatchGitHub error:", e);
    return false;
  }
}

async function dispatchGitHubVerbose(token, repo, payload) {
  try {
    const res = await fetch(`https://api.github.com/repos/${repo}/dispatches`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "User-Agent": "TongYi-Worker",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({ event_type: "reserve", client_payload: payload }),
    });
    const text = await res.text();
    return { ok: res.status === 204, status: res.status, detail: text };
  } catch (e) {
    return { ok: false, status: 0, detail: e.message || String(e) };
  }
}

async function dispatchServer(url, apiKey, payload) {
  try {
    const headers = {
      "Content-Type": "application/json",
      "User-Agent": "TongYi-Worker",
    };
    if (apiKey) headers["X-API-Key"] = apiKey;
    const res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    return res.ok;
  } catch (e) {
    console.error("dispatchServer error:", e);
    return false;
  }
}

async function dispatchServerVerbose(url, apiKey, payload) {
  try {
    const headers = {
      "Content-Type": "application/json",
      "User-Agent": "TongYi-Worker",
    };
    if (apiKey) headers["X-API-Key"] = apiKey;
    const res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, detail: text };
  } catch (e) {
    return { ok: false, status: 0, detail: e.message || String(e) };
  }
}

// ─── 创建并初始化 GitHub 仓库（内容复制自 hcd）───
const SOURCE_REPO_NAME = "hcd";

async function createAndInitRepo(repoFullName, ghToken) {
  const parts = repoFullName.split("/");
  if (parts.length !== 2) throw new Error(`仓库格式错误: ${repoFullName}，应为 owner/repo`);
  const [owner, repoName] = parts;

  // 源仓库与目标相同则跳过
  if (repoName === SOURCE_REPO_NAME) return { ok: true, skipped: true, reason: "目标即源仓库，跳过" };

  const ghHeaders = {
    Authorization: `Bearer ${ghToken}`,
    Accept: "application/vnd.github+json",
    "Content-Type": "application/json",
    "User-Agent": "TongYi-Worker",
    "X-GitHub-Api-Version": "2022-11-28",
  };

  // Step 1: 创建新仓库（空，不自动初始化）
  const createResp = await fetch("https://api.github.com/user/repos", {
    method: "POST",
    headers: ghHeaders,
    body: JSON.stringify({
      name: repoName,
      private: false,
      auto_init: false,
      description: `ChaoXing seat reservation - ${repoName}`,
    }),
  });
  const alreadyExists = createResp.status === 422;
  if (!createResp.ok && !alreadyExists) {
    const err = await createResp.text();
    throw new Error(`创建仓库失败 (${createResp.status}): ${err}`);
  }

  // Step 2: 获取源仓库 hcd 的完整文件树
  const treeResp = await fetch(
    `https://api.github.com/repos/${owner}/${SOURCE_REPO_NAME}/git/trees/HEAD?recursive=1`,
    { headers: ghHeaders }
  );
  if (!treeResp.ok) throw new Error(`获取源仓库文件树失败: ${treeResp.status}`);
  const { tree: sourceTree } = await treeResp.json();
  const blobs = sourceTree.filter((item) => item.type === "blob");

  // Step 3: 逐个复制 blob 到新仓库
  const newTreeEntries = [];
  for (const item of blobs) {
    const blobResp = await fetch(
      `https://api.github.com/repos/${owner}/${SOURCE_REPO_NAME}/git/blobs/${item.sha}`,
      { headers: ghHeaders }
    );
    if (!blobResp.ok) continue;
    const blobData = await blobResp.json();

    const newBlobResp = await fetch(
      `https://api.github.com/repos/${owner}/${repoName}/git/blobs`,
      {
        method: "POST",
        headers: ghHeaders,
        body: JSON.stringify({ content: blobData.content, encoding: blobData.encoding }),
      }
    );
    if (!newBlobResp.ok) continue;
    const { sha: newSha } = await newBlobResp.json();
    newTreeEntries.push({ path: item.path, mode: item.mode, type: "blob", sha: newSha });
  }

  // Step 4: 在新仓库创建 tree
  const newTreeResp = await fetch(
    `https://api.github.com/repos/${owner}/${repoName}/git/trees`,
    {
      method: "POST",
      headers: ghHeaders,
      body: JSON.stringify({ tree: newTreeEntries }),
    }
  );
  if (!newTreeResp.ok) throw new Error(`创建 tree 失败: ${newTreeResp.status}`);
  const { sha: newTreeSha } = await newTreeResp.json();

  // Step 5: 创建初始 commit（无父节点）
  const newCommitResp = await fetch(
    `https://api.github.com/repos/${owner}/${repoName}/git/commits`,
    {
      method: "POST",
      headers: ghHeaders,
      body: JSON.stringify({
        message: `init: copy from ${owner}/${SOURCE_REPO_NAME}`,
        tree: newTreeSha,
      }),
    }
  );
  if (!newCommitResp.ok) throw new Error(`创建 commit 失败: ${newCommitResp.status}`);
  const { sha: newCommitSha } = await newCommitResp.json();

  // Step 6: 创建或更新 main 分支
  const refResp = await fetch(
    `https://api.github.com/repos/${owner}/${repoName}/git/refs`,
    {
      method: "POST",
      headers: ghHeaders,
      body: JSON.stringify({ ref: "refs/heads/main", sha: newCommitSha }),
    }
  );
  if (refResp.status === 422) {
    // 分支已存在，强制更新
    const patchResp = await fetch(
      `https://api.github.com/repos/${owner}/${repoName}/git/refs/heads/main`,
      {
        method: "PATCH",
        headers: ghHeaders,
        body: JSON.stringify({ sha: newCommitSha, force: true }),
      }
    );
    if (!patchResp.ok) throw new Error(`更新 main 分支失败: ${patchResp.status}`);
  } else if (!refResp.ok) {
    throw new Error(`创建 main 分支失败: ${refResp.status}`);
  }

  return { ok: true, repo: `${owner}/${repoName}`, files: newTreeEntries.length };
}

const BATCH_SIZE = 10;

function randIntInclusive(min, max) {
  const lo = Math.min(min, max);
  const hi = Math.max(min, max);
  return Math.floor(Math.random() * (hi - lo + 1)) + lo;
}

function parseRangeWithFallback(v, fallback) {
  if (Array.isArray(v) && v.length >= 2) {
    const a = parseInt(v[0], 10);
    const b = parseInt(v[1], 10);
    if (!Number.isNaN(a) && !Number.isNaN(b)) return [a, b];
  }
  if (typeof v === "string" && v.includes(",")) {
    const parts = v.split(",").map(x => parseInt(x.trim(), 10));
    if (parts.length >= 2 && !Number.isNaN(parts[0]) && !Number.isNaN(parts[1])) {
      return [parts[0], parts[1]];
    }
  }
  return [fallback, fallback];
}

function randomizeStrategy(base) {
  const s = { ...(base || {}) };
  const probeStartRange = parseRangeWithFallback(
    s.fast_probe_start_range_ms,
    s.fast_probe_start_offset_ms || 14,
  );

  s.fast_probe_start_offset_ms = randIntInclusive(probeStartRange[0], probeStartRange[1]);
  delete s.burst_offsets_ms;
  delete s.burst_jitter_range_ms;
  return s;
}

function buildDispatchPayloadForUser(env, school, user, options = {}) {
  const dispatchTarget = resolveDispatchTarget(school);
  const reserveDayOffset = resolveReserveDayOffset(env, school);
  const allowTestEndtimeOverride = options.allowTestEndtimeOverride === true;
  const activeTestEndtime = allowTestEndtimeOverride
    ? getActiveTestEndtimeOverride(school)
    : null;
  const effectiveEndtime = activeTestEndtime?.endtime
    || resolveEffectiveEndtime(school, { allowTestEndtimeOverride: false });
  const slots = Array.isArray(user?.slots)
    ? user.slots.map(slot => {
        const nextSlot = { ...slot };
        const slotUseCustomDay = !!nextSlot.use_custom_day
          || (dispatchTarget === "server_relay" && isCustomDayTimes(nextSlot.times));
        if (slotUseCustomDay) {
          nextSlot.use_custom_day = true;
        }
        return nextSlot;
      })
    : user?.slots;
  return {
    ...user,
    ...(slots ? { slots } : {}),
    endtime: effectiveEndtime,
    endtime_source: activeTestEndtime ? "test_override" : "formal",
    seat_api_mode: school.seat_api_mode || "seat",
    reserve_next_day: school.reserve_next_day !== false,
    ...(reserveDayOffset !== null ? { reserve_day_offset: reserveDayOffset } : {}),
    enable_slider: !!school.enable_slider,
    enable_textclick: !!school.enable_textclick,
    strategy: randomizeStrategy(school.strategy),
  };
}

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

async function buildTodayDispatchUsers(KV, schoolId, school, today, schoolUsers = null) {
  const sourceUsers = Array.isArray(schoolUsers) ? schoolUsers : await getSchoolUsersSnapshot(KV, schoolId);
  const users = [];
  for (const user of sourceUsers) {
    if (!user || user.status !== "active") continue;

    const daySchedule = user.schedule[today];
    const activeSlots = getEnabledScheduleSlots(daySchedule);
    if (activeSlots.length === 0) continue;

    users.push({
      username: user.phone || user.username,
      password: user.password,
      remark: user.remark || user.username || user.phone,
      nickname: user.username,
      slots: activeSlots.map(s => ({
        roomid: s.roomid,
        seatid: (s.seatid || "").split(",").map(x => x.trim()).filter(Boolean),
        times: s.times,
        seatPageId: s.seatPageId || "",
        fidEnc: school?.fidEnc || s.fidEnc || "",
      })),
    });
  }
  return users;
}

async function dispatchUsersInBatches(env, school, users, options = {}) {
  const dispatchToken = resolveGitHubToken(env, school);
  const dispatchTarget = resolveDispatchTarget(school);
  const needsServerDispatch = dispatchTarget === "server_relay" || dispatchTarget === "both";
  const serverUrl = needsServerDispatch ? normalizeSecretText(school?.server_url) : "";
  const serverApiKey = needsServerDispatch ? resolveServerApiKey(env, school) : "";
  const serverMaxConcurrency = Math.max(
    1,
    parseInt(school?.server_max_concurrency, 10) || 13,
  );
  const batchSize = dispatchTarget === "server_relay" ? serverMaxConcurrency : BATCH_SIZE;
  const batches = chunkArray(users, batchSize);
  const reserveDayOffset = resolveReserveDayOffset(env, school);
  let okBatches = 0;
  const dispatchErrors = [];

  if ((dispatchTarget === "github" || dispatchTarget === "server_relay" || dispatchTarget === "both") && !dispatchToken) {
    console.log(`Dispatch skipped for school ${school.id}: missing GitHub token`);
    return { okBatches: 0, totalBatches: batches.length, error: "Missing GitHub token" };
  }
  if ((dispatchTarget === "server_relay" || dispatchTarget === "both") && !serverUrl) {
    console.log(`Dispatch skipped for school ${school.id}: missing server_url`);
    return { okBatches: 0, totalBatches: batches.length, error: "Missing server_url" };
  }

  for (let i = 0; i < batches.length; i++) {
    const payload = {
      school_id: school.id,
      school_name: school.name,
      ...(dispatchTarget !== "server_relay" ? { trigger_date: beijingDate() } : {}),
      batch_index: i + 1,
      batch_total: batches.length,
      dispatch_target: dispatchTarget,
      server_max_concurrency: serverMaxConcurrency,
      ...(reserveDayOffset !== null ? { reserve_day_offset: reserveDayOffset } : {}),
      users: batches[i].map(u => buildDispatchPayloadForUser(env, school, u, {
        allowTestEndtimeOverride: options.allowTestEndtimeOverride === true,
      })),
      ...(dispatchTarget === "server_relay" ? {
        server_url: serverUrl,
        server_api_key: serverApiKey,
      } : {}),
    };

    let githubStatus = "skip";
    let serverStatus = "skip";
    let githubDetail = "";
    let serverDetail = "";

    if (dispatchTarget === "github" || dispatchTarget === "both") {
      const githubResp = await dispatchGitHubVerbose(dispatchToken, school.repo, payload);
      githubStatus = githubResp.ok ? "ok" : "fail";
      if (!githubResp.ok) {
        const detailText = normalizeSecretText(githubResp.detail);
        githubDetail = detailText
          ? `${githubResp.status || 0}: ${detailText}`
          : String(githubResp.status || 0);
      }
    }
    if (dispatchTarget === "server_relay") {
      const githubResp = await dispatchGitHubVerbose(dispatchToken, school.repo, payload);
      serverStatus = githubResp.ok ? "ok" : "fail";
      if (!githubResp.ok) {
        const detailText = normalizeSecretText(githubResp.detail);
        serverDetail = detailText
          ? `via-github-relay, ${githubResp.status || 0}: ${detailText}`
          : `via-github-relay, ${String(githubResp.status || 0)}`;
      } else {
        serverDetail = "via-github-relay";
      }
    }
    if (dispatchTarget === "both") {
      const serverResp = await dispatchServerVerbose(serverUrl, serverApiKey, payload);
      serverStatus = serverResp.ok ? "ok" : "fail";
      if (!serverResp.ok) {
        const detailText = normalizeSecretText(serverResp.detail);
        serverDetail = detailText
          ? `${serverResp.status || 0}: ${detailText}`
          : String(serverResp.status || 0);
      }
    }

    const ok = githubStatus !== "fail" && serverStatus !== "fail";
    if (ok) {
      okBatches++;
    } else {
      const parts = [`batch ${i + 1}: github=${githubStatus}, server=${serverStatus}`];
      if (githubDetail) parts.push(`github_detail=${githubDetail}`);
      if (serverDetail) parts.push(`server_detail=${serverDetail}`);
      dispatchErrors.push(
        parts.join(", ")
      );
    }
    console.log(
      `Dispatch batch ${school.id} ${i + 1}/${batches.length}: ${ok ? "OK" : "FAIL"} `
      + `(target=${dispatchTarget}, github=${githubStatus}, server=${serverStatus}`
      + `${githubDetail ? `, github_detail=${githubDetail}` : ""}`
      + `${serverDetail ? `, server_detail=${serverDetail}` : ""})`
    );
  }

  return {
    okBatches,
    totalBatches: batches.length,
    error: dispatchErrors.length ? dispatchErrors.join("; ") : "",
  };
}

function parseSeatIdsRaw(seatidRaw) {
  if (Array.isArray(seatidRaw)) {
    return seatidRaw.map(v => String(v || "").trim()).filter(Boolean);
  }
  return String(seatidRaw || "")
    .split(",")
    .map(v => v.trim())
    .filter(Boolean);
}

const DATE_TEXT_RE = /^\d{4}-\d{2}-\d{2}$/;
const DATE_PAIR_TEXT_RE = /^\s*(\d{4}-\d{2}-\d{2})\s*[,，]\s*(\d{4}-\d{2}-\d{2})\s*$/;

function parseTimesInput(rawTimes) {
  if (Array.isArray(rawTimes) && rawTimes.length >= 2) {
    return [
      String(rawTimes[0] || "").trim(),
      String(rawTimes[1] || "").trim(),
    ];
  }
  const text = String(rawTimes || "").trim();
  if (!text) return ["", ""];

  const datePairMatch = text.match(DATE_PAIR_TEXT_RE);
  if (datePairMatch) {
    return [datePairMatch[1], datePairMatch[2]];
  }

  const parts = text.split(/-|~|至/).map(s => s.trim()).filter(Boolean);
  if (parts.length >= 2) {
    return [parts[0], parts[1]];
  }
  return [text, ""];
}

function isCustomDayTimes(rawTimes) {
  const [start, end] = parseTimesInput(rawTimes);
  return DATE_TEXT_RE.test(start) && DATE_TEXT_RE.test(end);
}

function normalizeTimesLabel(rawTimes) {
  const [start, end] = parseTimesInput(rawTimes);
  if (start && end) {
    return isCustomDayTimes([start, end]) ? `${start}，${end}` : `${start}-${end}`;
  }
  return String(rawTimes || "").trim();
}

function parseHmsToSeconds(hms) {
  const text = String(hms || "").trim();
  const match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;

  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  const second = parseInt(match[3] || "0", 10);
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return null;
  }
  return hour * 3600 + minute * 60 + second;
}

function parseTimesRange(rawTimes) {
  const label = normalizeTimesLabel(rawTimes);
  const [start, end] = parseTimesInput(rawTimes);
  if (!start || !end) {
    return { label, startSec: null, endSec: null, valid: false };
  }

  const startSec = parseHmsToSeconds(start);
  const endSec = parseHmsToSeconds(end);
  if (startSec === null || endSec === null || endSec <= startSec) {
    return { label, startSec: null, endSec: null, valid: false };
  }
  return { label, startSec, endSec, valid: true };
}

function isTimeOverlapped(a, b) {
  if (a.valid && b.valid) {
    return a.startSec < b.endSec && b.startSec < a.endSec;
  }
  return a.label && b.label && a.label === b.label;
}

function collectScheduleSeatEntries(schedule) {
  const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  const entries = [];

  for (const day of days) {
    const dayCfg = schedule && schedule[day];
    if (!dayCfg || !dayCfg.enabled) continue;

    const rawSlots = Array.isArray(dayCfg.slots)
      ? dayCfg.slots
      : [{
          roomid: dayCfg.roomid,
          seatid: dayCfg.seatid,
          times: dayCfg.times,
          seatPageId: dayCfg.seatPageId,
          fidEnc: dayCfg.fidEnc,
        }];

    for (const slot of rawSlots) {
      if (!slot || typeof slot !== "object") continue;
      const roomid = String(slot.roomid || "").trim();
      const seatList = parseSeatIdsRaw(slot.seatid);
      const times = parseTimesRange(slot.times);
      if (!roomid || !times.label || seatList.length === 0) continue;

      for (const seat of seatList) {
        entries.push({
          day,
          roomid,
          seat,
          times,
        });
      }
    }
  }

  return entries;
}

function buildSeatConflictKey(entry) {
  return `${entry.day}|${entry.roomid}|${entry.seat}`;
}

function dayNameZh(day) {
  const map = {
    Monday: "周一",
    Tuesday: "周二",
    Wednesday: "周三",
    Thursday: "周四",
    Friday: "周五",
    Saturday: "周六",
    Sunday: "周日",
  };
  return map[day] || day;
}

async function getConflictScopeUsers(KV, schoolId, school = null) {
  const targetSchool = school || await getSchool(KV, schoolId);
  if (!targetSchool) return [];

  const schools = await getSchoolsSnapshot(KV);
  const targetGroup = getSchoolConflictGroup(targetSchool);
  const relatedSchools = schools.filter(item => {
    if (!item || !item.id) return false;
    return getSchoolConflictGroup(item) === targetGroup;
  });

  const usersBySchool = await Promise.all(
    relatedSchools.map(async item => {
      const users = await getSchoolUsersSnapshot(KV, item.id);
      return users.map(user => ({
        ...user,
        __schoolId: item.id,
        __schoolName: item.name || item.id,
      }));
    })
  );

  return usersBySchool.flat();
}

async function findSeatConflicts(KV, schoolId, schedule, excludeIdentity = {}, schoolUsers = null) {
  const incomingEntries = collectScheduleSeatEntries(schedule);
  if (incomingEntries.length === 0) return [];

  const sourceUsers = Array.isArray(schoolUsers) ? schoolUsers : await getConflictScopeUsers(KV, schoolId);
  const existingByKey = new Map();
  const conflicts = [];
  const seenConflictKeys = new Set();
  const excludeUserId = String(excludeIdentity?.userId || "").trim();
  const excludePhone = String(excludeIdentity?.phone || "").trim();

  const pushConflict = (incoming, existing) => {
    const dedupeKey = `${buildSeatConflictKey(incoming)}|${existing.occupiedUserId || existing.occupiedBy || ""}`;
    if (seenConflictKeys.has(dedupeKey)) return;
    seenConflictKeys.add(dedupeKey);
    conflicts.push({
      day: incoming.day,
      roomid: incoming.roomid,
      seatid: incoming.seat,
      times: incoming.times.label,
      occupiedBy: existing.occupiedBy,
      occupiedUserId: existing.occupiedUserId || "",
      occupiedTimes: existing.occupiedTimes || "",
      occupiedSchoolId: existing.occupiedSchoolId || schoolId,
      occupiedSchoolName: existing.occupiedSchoolName || "",
    });
  };

  for (const existingUser of sourceUsers) {
    const uid = existingUser && existingUser.id;
    if (!uid) continue;
    if (excludeUserId && uid === excludeUserId) continue;
    if (!existingUser) continue;
    const existingPhone = String(existingUser.phone || "").trim();
    if (excludePhone && existingPhone && existingPhone === excludePhone) continue;

    const owner = String(existingUser.username || "").trim() || "未填写昵称";
    const existingEntries = collectScheduleSeatEntries(existingUser.schedule || {});
    for (const entry of existingEntries) {
      const key = buildSeatConflictKey(entry);
      const item = {
        ...entry,
        userId: uid,
        owner,
        schoolId: existingUser.__schoolId || schoolId,
        schoolName: existingUser.__schoolName || "",
      };
      const arr = existingByKey.get(key) || [];
      arr.push(item);
      existingByKey.set(key, arr);
    }
  }

  const incomingByKey = new Map();
  for (const incoming of incomingEntries) {
    const key = buildSeatConflictKey(incoming);
    if (incomingByKey.has(key)) {
      // 当前提交里自己重复填写了同一天/同房间/同座位时，不作为“冲突用户”报错。
      // 这里保留首条记录继续参与和其他用户的冲突判断，避免出现
      // “与昵称‘当前提交配置’冲突” 这种误导性提示。
      continue;
    }
    incomingByKey.set(key, incoming);

    const occupied = existingByKey.get(key) || [];
    for (const existing of occupied) {
      // 只要同一天、同房间、同座位就算冲突，不判断时间段
      pushConflict(incoming, {
        occupiedBy: existing.owner,
        occupiedUserId: existing.userId,
        occupiedTimes: existing.times.label,
        occupiedSchoolId: existing.schoolId,
        occupiedSchoolName: existing.schoolName,
      });
      break;
    }
  }

  return conflicts;
}

function buildSeatConflictError(conflicts) {
  if (!conflicts.length) return "";
  const first = conflicts[0];
  const prefix = `${dayNameZh(first.day)} ${first.roomid}/${first.seatid}`;
  const owner = first.occupiedBy || "未填写昵称";
  const suffix = conflicts.length > 1 ? `，另有 ${conflicts.length - 1} 处重复` : "";
  return `座位冲突：${prefix} 与昵称“${owner}”冲突${suffix}`;
}

// ─── Scheduled Handler ───

async function handleScheduled(env) {
  const now = beijingHHMM();
  const today = beijingDayOfWeek();
  const schools = await getSchoolsSnapshot(env.SEAT_KV);

  for (const school of schools) {
    if (!school || school.trigger_time !== now) continue;

    const users = await buildTodayDispatchUsers(env.SEAT_KV, school.id, school, today);
    if (users.length === 0) continue;
    const result = await dispatchUsersInBatches(env, school, users, {
      allowTestEndtimeOverride: false,
    });
    if (result.error) {
      console.log(`Scheduled dispatch school ${school.id} failed: ${result.error}`);
    }
    console.log(
      `Scheduled dispatch school ${school.id}: users=${users.length}, batches=${result.okBatches}/${result.totalBatches}`
    );
  }
}

// ─── API Handler ───

async function handleAPI(request, env, path) {
  const KV = env.SEAT_KV;
  const method = request.method;

  // GET /api/status
  if (method === "GET" && path === "/api/status") {
    const schools = await getSchoolsSnapshot(KV);
    const lastHeartbeatTs = await getHeartbeatTimestamp(KV);
    const lastHeartbeatMinuteSlot = await getHeartbeatMinuteSlot(KV);
    const heartbeatAgeMs = lastHeartbeatTs === null ? null : Math.max(0, Date.now() - lastHeartbeatTs);
    return jsonResp({
      ok: true,
      worker: "tongyi",
      now: new Date().toISOString(),
      beijing_date: beijingDate(),
      beijing_time: beijingHHMM(),
      beijing_date_hour: beijingDateHour(),
      day_of_week: beijingDayOfWeek(),
      schoolCount: schools.length,
      heartbeat: {
        key: HEARTBEAT_LAST_TS_KEY,
        minuteKey: HEARTBEAT_LAST_MINUTE_KEY,
        lastTs: lastHeartbeatTs,
        lastMinuteSlot: lastHeartbeatMinuteSlot,
        ageMs: heartbeatAgeMs,
      },
    });
  }

  // GET /api/schools
  if (method === "GET" && path === "/api/schools") {
    const schools = await getSchoolsSnapshot(KV);
    return jsonResp(
      { schools: getSortedSchoolsForDisplay(schools).map(sanitizeSchoolForClient) },
      200,
      { "Cache-Control": "private, max-age=5" }
    );
  }

  // POST /api/school
  if (method === "POST" && path === "/api/school") {
    const body = await request.json();
    const id = body.id || generateId();
    const name = body.name || `学校 ${id}`;
    const school = defaultSchool(id, name);
    if (body.conflict_group !== undefined) {
      school.conflict_group = normalizeSecretText(body.conflict_group);
    }
    if (body.repo) school.repo = body.repo;
    if (body.seat_api_mode !== undefined) {
      school.seat_api_mode = normalizeSecretText(body.seat_api_mode).toLowerCase();
    }
    if (body.reserve_next_day !== undefined) school.reserve_next_day = !!body.reserve_next_day;
    if (body.reserve_day_offset !== undefined) school.reserve_day_offset = parseReserveDayOffset(body.reserve_day_offset);
    if (body.enable_slider !== undefined) school.enable_slider = !!body.enable_slider;
    if (body.enable_textclick !== undefined) school.enable_textclick = !!body.enable_textclick;
    if (body.dispatch_target !== undefined) {
      school.dispatch_target = resolveDispatchTarget(body);
    }
    if (body.github_token_key !== undefined) {
      school.github_token_key = normalizeSecretText(body.github_token_key).toLowerCase();
    }
    if (body.github_token !== undefined) school.github_token = normalizeSecretText(body.github_token);
    if (body.server_url !== undefined) school.server_url = normalizeSecretText(body.server_url);
    if (body.server_api_key !== undefined) school.server_api_key = normalizeSecretText(body.server_api_key);
    if (body.server_max_concurrency !== undefined) {
      school.server_max_concurrency = Math.max(1, parseInt(body.server_max_concurrency, 10) || 13);
    }
    if (body.trigger_time) school.trigger_time = normalizeSecretText(body.trigger_time);
    if (body.endtime) school.endtime = normalizeEndtimeHms(body.endtime) || normalizeSecretText(body.endtime);
    const timeWindowError = validateFormalTimeWindow(school.trigger_time, school.endtime);
    if (timeWindowError) return jsonResp({ error: timeWindowError }, 400);
    if (body.fidEnc !== undefined) school.fidEnc = body.fidEnc;
    await saveSchool(KV, school);
    const schools = await getSchools(KV);
    if (!schools.includes(id)) {
      schools.push(id);
      await saveSchools(KV, schools);
      await saveSchoolUsersSnapshot(KV, id, []);
    }
    // 自动在 GitHub 创建仓库并从 hcd 复制代码
    let repoInit = null;
    const repoToken = resolveGitHubToken(env, school);
    if (school.repo && repoToken) {
      try {
        repoInit = await createAndInitRepo(school.repo, repoToken);
      } catch (e) {
        repoInit = { ok: false, error: e.message };
      }
    }
    return jsonResp({ ok: true, school: sanitizeSchoolForClient(school), repoInit });
  }

  // GET /api/school/:id
  const schoolMatch = path.match(/^\/api\/school\/([^/]+)$/);
  if (method === "GET" && schoolMatch) {
    const school = await getSchool(KV, schoolMatch[1]);
    if (!school) return jsonResp({ error: "School not found" }, 404);
    const schoolUsers = await getSchoolUsersSnapshot(KV, schoolMatch[1]);
    return jsonResp({ school: sanitizeSchoolForClient(school), userCount: schoolUsers.length });
  }

  // PUT /api/school/:id
  if (method === "PUT" && schoolMatch) {
    const school = await getSchool(KV, schoolMatch[1]);
    if (!school) return jsonResp({ error: "School not found" }, 404);
    const body = await request.json();
    if (body.github_token !== undefined) {
      body.github_token = normalizeSecretText(body.github_token);
    }
    if (body.github_token_key !== undefined) {
      body.github_token_key = normalizeSecretText(body.github_token_key).toLowerCase();
    }
    if (body.seat_api_mode !== undefined) {
      body.seat_api_mode = normalizeSecretText(body.seat_api_mode).toLowerCase();
    }
    if (body.reserve_next_day !== undefined) body.reserve_next_day = !!body.reserve_next_day;
    if (body.reserve_day_offset !== undefined) body.reserve_day_offset = parseReserveDayOffset(body.reserve_day_offset);
    if (body.enable_slider !== undefined) body.enable_slider = !!body.enable_slider;
    if (body.enable_textclick !== undefined) body.enable_textclick = !!body.enable_textclick;
    if (body.dispatch_target !== undefined) {
      body.dispatch_target = resolveDispatchTarget(body);
    }
    if (body.conflict_group !== undefined) {
      body.conflict_group = normalizeSecretText(body.conflict_group);
    }
    if (body.server_url !== undefined) {
      body.server_url = normalizeSecretText(body.server_url);
    }
    if (body.server_api_key !== undefined) {
      body.server_api_key = normalizeSecretText(body.server_api_key);
    }
    if (body.server_max_concurrency !== undefined) {
      body.server_max_concurrency = Math.max(1, parseInt(body.server_max_concurrency, 10) || 13);
    }
    if (body.trigger_time !== undefined) {
      body.trigger_time = normalizeSecretText(body.trigger_time);
    }
    if (body.endtime !== undefined) {
      body.endtime = normalizeEndtimeHms(body.endtime) || normalizeSecretText(body.endtime);
    }
    const nextSchool = { ...school, ...body, id: school.id };
    const timeWindowError = validateFormalTimeWindow(nextSchool.trigger_time, nextSchool.endtime);
    if (timeWindowError) return jsonResp({ error: timeWindowError }, 400);

    Object.assign(school, nextSchool);
    await saveSchool(KV, school);
    return jsonResp({ ok: true, school: sanitizeSchoolForClient(school) });
  }

  // DELETE /api/school/:id
  if (method === "DELETE" && schoolMatch) {
    await deleteSchool(KV, schoolMatch[1]);
    return jsonResp({ ok: true });
  }

  // POST /api/school/:id/test-endtime
  const testEndtimeMatch = path.match(/^\/api\/school\/([^/]+)\/test-endtime$/);
  if (method === "POST" && testEndtimeMatch) {
    const school = await getSchool(KV, testEndtimeMatch[1]);
    if (!school) return jsonResp({ error: "School not found" }, 404);

    let body = {};
    try {
      body = await request.json();
    } catch (_) {
      body = {};
    }
    const action = normalizeSecretText(body.action || "start").toLowerCase();
    if (action === "stop" || action === "disable") {
      school.test_endtime_override = null;
      await saveSchool(KV, school);
      return jsonResp({ ok: true, school: sanitizeSchoolForClient(school) });
    }

    const testEndtime = normalizeEndtimeHms(body.test_endtime || body.endtime || school.test_endtime);
    if (!testEndtime) {
      return jsonResp({ error: "测试截止时间格式应为 HH:MM:SS" }, 400);
    }

    const now = new Date();
    const expiresAt = new Date(now.getTime() + TEST_ENDTIME_OVERRIDE_TTL_MS);
    school.test_endtime = testEndtime;
    school.test_endtime_override = {
      endtime: testEndtime,
      enabled_at: now.toISOString(),
      expires_at: expiresAt.toISOString(),
    };
    await saveSchool(KV, school);
    return jsonResp({ ok: true, school: sanitizeSchoolForClient(school) });
  }

  // GET /api/school/:id/users
  const usersMatch = path.match(/^\/api\/school\/([^/]+)\/users$/);
  if (method === "GET" && usersMatch) {
    const schoolId = usersMatch[1];
    const schoolUsers = await getSchoolUsersSnapshot(KV, schoolId);
    const users = schoolUsers.map(user => ({ ...user, password: user.password ? "******" : "" }));
    return jsonResp(
      { users },
      200,
      { "Cache-Control": "private, max-age=3" }
    );
  }

  // POST /api/school/:id/user
  const userCreateMatch = path.match(/^\/api\/school\/([^/]+)\/user$/);
  if (method === "POST" && userCreateMatch) {
    const schoolId = userCreateMatch[1];
    const body = await request.json();
    const id = body.id || generateId();
    const school = await getSchool(KV, schoolId);
    if (!school) return jsonResp({ error: "School not found" }, 404);
    const schoolUsers = await getConflictScopeUsers(KV, schoolId, school);
    const user = defaultUser(id);
    user.phone = body.phone || "";
    user.username = body.username || "";
    user.password = body.password ? await aesEncrypt(body.password) : "";
    user.remark = body.remark || "";
    if (body.status === "active" || body.status === "paused") user.status = body.status;
    if (body.schedule) user.schedule = body.schedule;

    const conflicts = await findSeatConflicts(
      KV,
      schoolId,
      user.schedule || {},
      { userId: id, phone: user.phone },
      schoolUsers,
    );
    if (conflicts.length > 0) {
      return jsonResp({
        error: buildSeatConflictError(conflicts),
        conflicts,
      }, 409);
    }

    await saveUser(KV, schoolId, user);
    const userIds = await getSchoolUsers(KV, schoolId);
    if (!userIds.includes(id)) {
      userIds.push(id);
      await saveSchoolUsers(KV, schoolId, userIds);
    }
    await setSchoolUserCountInSnapshot(KV, schoolId, userIds.length);
    return jsonResp({ ok: true, user: { ...user, password: "******" } });
  }

  // GET /api/school/:id/user/:userId
  const userMatch = path.match(/^\/api\/school\/([^/]+)\/user\/([^/]+)$/);
  if (method === "GET" && userMatch) {
    const user = await getUser(KV, userMatch[1], userMatch[2]);
    if (!user) return jsonResp({ error: "User not found" }, 404);
    return jsonResp({ user: { ...user, password: user.password ? "******" : "" } });
  }

  // PUT /api/school/:id/user/:userId
  if (method === "PUT" && userMatch) {
    const [_, schoolId, userId] = userMatch;
    const school = await getSchool(KV, schoolId);
    if (!school) return jsonResp({ error: "School not found" }, 404);
    const user = await getUser(KV, schoolId, userId);
    if (!user) return jsonResp({ error: "User not found" }, 404);
    const body = await request.json();
    const schoolUsers = await getConflictScopeUsers(KV, schoolId, school);

    const nextSchedule = body.schedule ? body.schedule : (user.schedule || {});
    const conflicts = await findSeatConflicts(
      KV,
      schoolId,
      nextSchedule,
      { userId, phone: body.phone !== undefined ? body.phone : user.phone },
      schoolUsers,
    );
    if (conflicts.length > 0) {
      return jsonResp({
        error: buildSeatConflictError(conflicts),
        conflicts,
      }, 409);
    }

    if (body.phone !== undefined) user.phone = body.phone;
    if (body.username !== undefined) user.username = body.username;
    if (body.password && body.password !== "******") user.password = await aesEncrypt(body.password);
    if (body.remark !== undefined) user.remark = body.remark;
    if (body.status !== undefined) user.status = body.status;
    if (body.schedule) user.schedule = body.schedule;
    await saveUser(KV, schoolId, user);
    return jsonResp({ ok: true, user: { ...user, password: "******" } });
  }

  // DELETE /api/school/:id/user/:userId
  if (method === "DELETE" && userMatch) {
    const schoolId = userMatch[1];
    const nextUserIds = await getSchoolUsers(KV, schoolId);
    await deleteUser(KV, schoolId, userMatch[2]);
    await setSchoolUserCountInSnapshot(KV, schoolId, Math.max(0, nextUserIds.length - 1));
    return jsonResp({ ok: true });
  }

  // POST /api/school/:id/user/:userId/pause
  const pauseMatch = path.match(/^\/api\/school\/([^/]+)\/user\/([^/]+)\/(pause|resume)$/);
  if (method === "POST" && pauseMatch) {
    const [_, schoolId, userId, action] = pauseMatch;
    const school = await getSchool(KV, schoolId);
    if (!school) return jsonResp({ error: "School not found" }, 404);
    const user = await getUser(KV, schoolId, userId);
    if (!user) return jsonResp({ error: "User not found" }, 404);
    if (action === "resume") {
      const schoolUsers = await getConflictScopeUsers(KV, schoolId, school);
      const conflicts = await findSeatConflicts(
        KV,
        schoolId,
        user.schedule || {},
        { userId, phone: user.phone },
        schoolUsers,
      );
      if (conflicts.length > 0) {
        return jsonResp({
          error: buildSeatConflictError(conflicts),
          conflicts,
        }, 409);
      }
    }
    user.status = action === "pause" ? "paused" : "active";
    await saveUser(KV, schoolId, user);
    return jsonResp({ ok: true, status: user.status });
  }

  // POST /api/trigger/:schoolId
  const triggerSchoolMatch = path.match(/^\/api\/trigger\/([^/]+)$/);
  if (method === "POST" && triggerSchoolMatch) {
    const schoolId = triggerSchoolMatch[1];
    const school = await getSchool(KV, schoolId);
    if (!school) return jsonResp({ error: "School not found" }, 404);
    const today = beijingDayOfWeek();
    const todayDate = beijingDate();
    const triggerSource = (request.headers.get("X-Trigger-Source") || "").trim();
    const fallbackMode = (request.headers.get("X-Fallback-Mode") || "").trim();
    const isScheduledFallback = triggerSource === "worker2" && fallbackMode === "scheduled";

    if (isScheduledFallback) {
      const existingRecord = await getFallbackTriggerRecord(KV, todayDate, schoolId);
      if (existingRecord) {
        return jsonResp({
          ok: true,
          skipped: true,
          reason: "fallback_already_triggered_today",
          schoolId,
          schoolName: school.name,
          date: todayDate,
          fallbackRecord: existingRecord,
        });
      }
    }

    const users = await buildTodayDispatchUsers(KV, schoolId, school, today);
    if (users.length === 0) {
      if (isScheduledFallback) {
        await saveFallbackTriggerRecord(KV, todayDate, schoolId, {
          source: "worker2",
          mode: "scheduled",
          at: new Date().toISOString(),
          beijing_time: beijingHHMM(),
          schoolId,
          schoolName: school.name,
          triggeredUsers: 0,
          okBatches: 0,
          totalBatches: 0,
        });
      }
      return jsonResp({ ok: true, triggeredUsers: 0, okBatches: 0, totalBatches: 0 });
    }
    const result = await dispatchUsersInBatches(env, school, users, {
      allowTestEndtimeOverride: !isScheduledFallback,
    });
    if (result.error) {
      return jsonResp({
        ok: false,
        error: result.error,
        triggeredUsers: users.length,
        okBatches: result.okBatches,
        totalBatches: result.totalBatches,
      }, 400);
    }
    if (isScheduledFallback) {
      await saveFallbackTriggerRecord(KV, todayDate, schoolId, {
        source: "worker2",
        mode: "scheduled",
        at: new Date().toISOString(),
        beijing_time: beijingHHMM(),
        schoolId,
        schoolName: school.name,
        triggeredUsers: users.length,
        okBatches: result.okBatches,
        totalBatches: result.totalBatches,
      });
    }
    return jsonResp({
      ok: true,
      triggeredUsers: users.length,
      okBatches: result.okBatches,
      totalBatches: result.totalBatches,
    });
  }

  // POST /api/trigger/:schoolId/:userId
  const triggerUserMatch = path.match(/^\/api\/trigger\/([^/]+)\/([^/]+)$/);
  if (method === "POST" && triggerUserMatch) {
    const [_, schoolId, userId] = triggerUserMatch;
    const school = await getSchool(KV, schoolId);
    const user = await getUser(KV, schoolId, userId);
    if (!school || !user) return jsonResp({ error: "Not found" }, 404);
    const today = beijingDayOfWeek();
    const daySchedule = user.schedule[today];
    if (!daySchedule || !daySchedule.enabled) {
      return jsonResp({ error: "User has no schedule for today" }, 400);
    }
    const rawSlots = daySchedule.slots
      ? daySchedule.slots
      : [{ roomid: daySchedule.roomid, seatid: daySchedule.seatid, times: daySchedule.times, seatPageId: daySchedule.seatPageId || "", fidEnc: daySchedule.fidEnc || "" }];
    const activeSlots = rawSlots.filter(s => s.times && s.roomid);
    if (activeSlots.length === 0) return jsonResp({ error: "No active slots for today" }, 400);
    const reserveDayOffset = resolveReserveDayOffset(env, school);
    const dispatchUser = {
      username: user.phone || user.username,
      password: user.password,
      remark: user.remark || user.username || user.phone,
      nickname: user.username,
      ...(reserveDayOffset !== null ? { reserve_day_offset: reserveDayOffset } : {}),
      slots: activeSlots.map(s => ({
        roomid: s.roomid,
        seatid: (s.seatid || "").split(",").map(x => x.trim()).filter(Boolean),
        times: s.times,
        seatPageId: s.seatPageId || "",
        fidEnc: school.fidEnc || s.fidEnc || "",
      })),
    };
    const result = await dispatchUsersInBatches(env, school, [dispatchUser], {
      allowTestEndtimeOverride: true,
    });
    if (result.error) {
      return jsonResp({
        ok: false,
        error: result.error,
        triggeredUsers: 1,
        okBatches: result.okBatches,
        totalBatches: result.totalBatches,
        repo: school.repo,
      }, 502);
    }
    return jsonResp({
      ok: true,
      triggeredUsers: 1,
      okBatches: result.okBatches,
      totalBatches: result.totalBatches,
      slots: activeSlots.length,
      repo: school.repo,
    });
  }

  // POST /api/encrypt
  if (method === "POST" && path === "/api/encrypt") {
    const body = await request.json();
    if (!body.password) return jsonResp({ error: "password required" }, 400);
    const encrypted = await aesEncrypt(body.password);
    return jsonResp({ encrypted });
  }

  // POST /api/init-demo (初始化演示数据)
  if (method === "POST" && path === "/api/init-demo") {
    const demoSchools = [
      { id: "001", name: "华东师范大学", repo: "BAOfuZhan/hcd" },
      { id: "002", name: "复旦大学", repo: "BAOfuZhan/fdu" },
      { id: "003", name: "上海交通大学", repo: "BAOfuZhan/sjtu" },
    ];
    const existingSchools = await getSchools(KV);
    for (const demo of demoSchools) {
      if (!existingSchools.includes(demo.id)) {
        const school = defaultSchool(demo.id, demo.name);
        school.repo = demo.repo;
        await saveSchool(KV, school);
        await saveSchoolUsersSnapshot(KV, demo.id, []);
        existingSchools.push(demo.id);
      }
    }
    await saveSchools(KV, existingSchools);
    return jsonResp({ ok: true, schools: existingSchools });
  }

  return jsonResp({ error: "Not found" }, 404);
}

// ─── Fetch Handler ───

async function handleFetch(request, env) {
  const url = new URL(request.url);
  const path = url.pathname;

  // CORS
  if (request.method === "OPTIONS") {
    return new Response(null, {
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,X-API-Key",
      },
    });
  }

  // API 鉴权
  if (path.startsWith("/api/")) {
    const apiKey = request.headers.get("X-API-Key") || url.searchParams.get("key");
    if (apiKey !== env.API_KEY) {
      return jsonResp({ error: "Unauthorized" }, 401);
    }
    return handleAPI(request, env, path);
  }

  // 管理面板
  return new Response(ADMIN_HTML, {
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}

// ─── 管理面板 HTML ───

const ADMIN_HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>统一抢座管理系统</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f0f2f5;min-height:100vh}
.container{max-width:1200px;margin:0 auto;padding:20px}
.header{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:#fff;padding:20px;border-radius:12px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:24px}
.header .time{font-size:14px;opacity:0.9}
.login-box{max-width:400px;margin:100px auto;background:#fff;padding:40px;border-radius:12px;box-shadow:0 4px 20px rgba(0,0,0,0.1)}
.login-box h2{text-align:center;margin-bottom:30px;color:#333}
.login-box input{width:100%;padding:12px;border:1px solid #ddd;border-radius:8px;font-size:16px;margin-bottom:20px}
.login-box button{width:100%;padding:12px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border:none;border-radius:8px;font-size:16px;cursor:pointer}
.btn{padding:8px 16px;border:none;border-radius:6px;cursor:pointer;font-size:14px;transition:all 0.2s}
.btn-primary{background:#667eea;color:#fff}
.btn-primary:hover{background:#5a6fd6}
.btn-success{background:#52c41a;color:#fff}
.btn-danger{background:#ff4d4f;color:#fff}
.btn-secondary{background:#f0f0f0;color:#333}
.btn-sm{padding:4px 10px;font-size:12px}
.card{background:#fff;border-radius:12px;padding:20px;margin-bottom:16px;box-shadow:0 2px 8px rgba(0,0,0,0.06)}
.card-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid #f0f0f0}
.card-title{font-size:18px;font-weight:600;color:#333}
.school-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px}
.school-card{background:#fff;border-radius:12px;padding:20px;box-shadow:0 2px 8px rgba(0,0,0,0.06);cursor:pointer;transition:all 0.2s;border:2px solid transparent}
.school-card:hover{border-color:#667eea;transform:translateY(-2px)}
.school-card h3{font-size:18px;color:#333;margin-bottom:8px}
.school-card .meta{font-size:13px;color:#888;margin-bottom:12px}
.school-card .stats{display:flex;gap:16px;font-size:13px}
.school-card .stats span{color:#667eea}
.user-table{width:100%;border-collapse:collapse}
.user-table th,.user-table td{padding:12px;text-align:left;border-bottom:1px solid #f0f0f0}
.user-table th{background:#fafafa;font-weight:500;color:#666}
.user-table tr:hover{background:#fafafa}
.status-active{color:#52c41a}
.status-paused{color:#faad14}
.test-override-panel{margin-top:16px;padding-top:16px;border-top:1px solid #f0f0f0}
.test-override-row{display:grid;grid-template-columns:1fr auto auto;gap:8px;align-items:end}
.test-override-status{display:flex;flex-wrap:wrap;gap:8px;align-items:center;font-size:13px;color:#666;margin-bottom:10px}
.test-status-pill{display:inline-flex;align-items:center;border-radius:999px;padding:2px 8px;font-size:12px;font-weight:600}
.test-status-on{background:#f6ffed;color:#389e0d}
.test-status-off{background:#f5f5f5;color:#777}
.test-override-note{font-size:12px;color:#777;line-height:1.7;margin-top:8px}
.modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);z-index:1000;overflow-y:auto}
.modal.show{display:flex;align-items:flex-start;justify-content:center;padding:40px 20px}
.modal-content{background:#fff;border-radius:12px;width:100%;max-width:800px;max-height:90vh;overflow-y:auto}
.modal-header{padding:20px;border-bottom:1px solid #f0f0f0;display:flex;justify-content:space-between;align-items:center}
.modal-header h3{font-size:18px}
.modal-close{font-size:24px;cursor:pointer;color:#999}
.modal-body{padding:20px}
.form-group{margin-bottom:16px}
.form-group label{display:block;margin-bottom:6px;font-weight:500;color:#333}
.form-group input,.form-group select,.form-group textarea{width:100%;padding:10px;border:1px solid #ddd;border-radius:6px;font-size:14px}
.form-row{display:grid;grid-template-columns:repeat(2,1fr);gap:16px}
.schedule-grid{display:grid;gap:12px}
.schedule-day{background:#fafafa;border-radius:8px;padding:12px}
.schedule-day-header{display:flex;align-items:center;gap:12px;margin-bottom:8px}
.schedule-day-header input[type="checkbox"]{width:18px;height:18px}
.schedule-day-header label{font-weight:500}
.schedule-day-fields{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
.schedule-day-fields input{padding:6px;font-size:12px}
.slot-row{border-top:1px solid #e8e8e8;padding-top:8px;margin-top:8px}
.slot-label{font-size:11px;color:#888;margin-bottom:4px}
.toast{position:fixed;top:20px;right:20px;padding:12px 20px;border-radius:8px;color:#fff;z-index:2000;animation:slideIn 0.3s}
.toast-success{background:#52c41a}
.toast-error{background:#ff4d4f}
@keyframes slideIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
.breadcrumb{display:flex;align-items:center;gap:8px;margin-bottom:20px;font-size:14px;color:#666}
.breadcrumb a{color:#667eea;text-decoration:none}
.breadcrumb a:hover{text-decoration:underline}
.empty{text-align:center;padding:60px;color:#999}
.empty-icon{font-size:48px;margin-bottom:16px}
.actions{display:flex;gap:8px}
.zone-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}
.zone-card{background:#fafafa;border:1px solid #ececec;border-radius:10px;padding:12px}
.zone-floor{font-size:13px;font-weight:600;color:#333;margin-bottom:8px}
.zone-list{display:grid;gap:6px}
.zone-item{display:flex;justify-content:space-between;align-items:center;font-size:13px;color:#555;padding:6px 8px;background:#fff;border-radius:6px}
.zone-id{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;color:#667eea;background:#eef1ff;padding:2px 6px;border-radius:999px}
.zone-right{display:flex;align-items:center;gap:6px}
.copy-btn{border:none;background:#f0f2f7;color:#4b5563;border-radius:6px;padding:2px 8px;font-size:12px;cursor:pointer}
.copy-btn:hover{background:#e5e9f3}
.mapping-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px}
.mapping-box{background:#fafafa;border:1px solid #ececec;border-radius:10px;padding:14px}
.mapping-box h4{font-size:14px;color:#333;margin-bottom:8px}
.mapping-box textarea,.mapping-box input{width:100%;padding:10px;border:1px solid #ddd;border-radius:6px;font-size:13px}
.mapping-box textarea{min-height:220px;resize:vertical}
.mapping-inline{display:grid;grid-template-columns:140px 1fr;gap:12px;align-items:end;margin-bottom:12px}
.mapping-user-fields{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}
.mapping-actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}
.mapping-note{font-size:12px;color:#777;line-height:1.7;margin-top:10px}
@media (max-width: 768px){.mapping-inline,.mapping-user-fields{grid-template-columns:1fr}}
@media (max-width: 768px){.test-override-row{grid-template-columns:1fr}.actions{flex-wrap:wrap}}
</style>
</head>
<body>
<div id="app"></div>
<script>
const API_BASE = "";
let API_KEY = "";
try {
  API_KEY = localStorage.getItem("api_key") || "";
} catch (_e) {
  API_KEY = "";
}
let currentView = "login";
let currentSchool = null;
let schools = [];
let users = [];
let isSavingUser = false;
const ACTIVE_TODAY_CACHE_TTL_MS = 3000;
const ACTIVE_TODAY_CACHE_PREFIX = "active_today_count:";
const DEFAULT_READING_ZONE_GROUPS = [
  { floor: "2 楼", zones: [{ id: "13474", name: "西阅览区" }, { id: "13473", name: "东阅览区" }, { id: "13476", name: "西电子阅览区" }, { id: "13472", name: "东电子阅览区" }] },
  { floor: "3 楼", zones: [{ id: "13481", name: "西阅览区" }, { id: "13484", name: "中阅览区" }, { id: "13478", name: "东阅览区" }, { id: "13480", name: "西电子阅览区" }, { id: "13475", name: "东电子阅览区" }] },
  { floor: "4 楼", zones: [{ id: "13487", name: "西阅览区" }, { id: "13490", name: "中阅览区" }, { id: "13489", name: "东阅览区" }, { id: "13485", name: "西电子阅览区" }, { id: "13486", name: "东电子阅览区" }, { id: "13492", name: "南区" }] },
  { floor: "5 楼", zones: [{ id: "13493", name: "西阅览区" }, { id: "13497", name: "中阅览区" }, { id: "13494", name: "东阅览区" }] },
  { floor: "6 楼", zones: [{ id: "13499", name: "西阅览区" }, { id: "13500", name: "中阅览区" }, { id: "13502", name: "东阅览区" }, { id: "13505", name: "北阅览区" }] },
  { floor: "7 楼", zones: [{ id: "13504", name: "西阅览区" }, { id: "13506", name: "中阅览区" }, { id: "13507", name: "东阅览区" }] },
  { floor: "8 楼", zones: [{ id: "13495", name: "西阅览区" }, { id: "13496", name: "中阅览室" }, { id: "13498", name: "东阅览区" }, { id: "13501", name: "电子西阅览区" }, { id: "13503", name: "电子东阅览区" }] },
  { floor: "9 楼", zones: [{ id: "13491", name: "西阅览室" }, { id: "13488", name: "中阅览区" }, { id: "13483", name: "东阅览区" }] },
];
const PLAN_EXTRACT_MAX_HOURS_DEFAULT = 16;
const PLAN_EXTRACT_WEEK_MAP = {
  "周一": "Monday",
  "周二": "Tuesday",
  "周三": "Wednesday",
  "周四": "Thursday",
  "周五": "Friday",
  "周六": "Saturday",
  "周日": "Sunday",
  "周天": "Sunday",
};
const PLAN_EXTRACT_ALL_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

const CLIENT_DATE_TEXT_RE = /^\\d{4}-\\d{2}-\\d{2}$/;
const CLIENT_DATE_PAIR_TEXT_RE = /^\\s*(\\d{4}-\\d{2}-\\d{2})\\s*[,，]\\s*(\\d{4}-\\d{2}-\\d{2})\\s*$/;

function parseTimesInput(rawTimes) {
  if (Array.isArray(rawTimes) && rawTimes.length >= 2) {
    return [
      String(rawTimes[0] || "").trim(),
      String(rawTimes[1] || "").trim(),
    ];
  }
  const text = String(rawTimes || "").trim();
  if (!text) return ["", ""];

  const datePairMatch = text.match(CLIENT_DATE_PAIR_TEXT_RE);
  if (datePairMatch) {
    return [datePairMatch[1], datePairMatch[2]];
  }

  const parts = text.split(/-|~|至/).map(s => s.trim()).filter(Boolean);
  if (parts.length >= 2) {
    return [parts[0], parts[1]];
  }
  return [text, ""];
}

function isCustomDayTimes(rawTimes) {
  const [start, end] = parseTimesInput(rawTimes);
  return CLIENT_DATE_TEXT_RE.test(start) && CLIENT_DATE_TEXT_RE.test(end);
}

function normalizeTimesLabel(rawTimes) {
  const [start, end] = parseTimesInput(rawTimes);
  if (start && end) {
    return isCustomDayTimes([start, end]) ? \`\${start}，\${end}\` : \`\${start}-\${end}\`;
  }
  return String(rawTimes || "").trim();
}

function isServerRelayTarget(value) {
  const target = String(value || "").trim().toLowerCase();
  return target === "server_relay";
}

function normalizeReserveDayOffsetInput(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  if (!/^\\d+$/.test(text)) return null;
  return Math.max(0, parseInt(text, 10));
}

function formatReserveDayLabel(s) {
  const offset = isServerRelayTarget(s?.dispatch_target)
    ? normalizeReserveDayOffsetInput(s?.reserve_day_offset)
    : null;
  if (offset !== null) {
    if (offset === 0) return "今天（服务器中转 day+0）";
    if (offset === 1) return "明天（服务器中转 day+1）";
    if (offset === 2) return "后天（服务器中转 day+2）";
    return \`北京时间 +\${offset} 天（服务器中转）\`;
  }
  return s?.reserve_next_day === false ? "今天" : "明天";
}

function normalizeClientEndtimeInput(value) {
  const text = String(value || "").trim();
  const match = text.match(/^(\\d{1,2}):(\\d{2})(?::(\\d{2}))?$/);
  if (!match) return "";
  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  const second = parseInt(match[3] || "0", 10);
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return "";
  }
  return [
    String(hour).padStart(2, "0"),
    String(minute).padStart(2, "0"),
    String(second).padStart(2, "0"),
  ].join(":");
}

function parseClientTriggerSeconds(value) {
  const text = String(value || "").trim();
  const match = text.match(/^(\\d{1,2}):(\\d{2})$/);
  if (!match) return null;
  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
  return hour * 3600 + minute * 60;
}

function parseClientEndtimeSeconds(value) {
  const normalized = normalizeClientEndtimeInput(value);
  if (!normalized) return null;
  const [hour, minute, second] = normalized.split(":").map(v => parseInt(v, 10));
  return hour * 3600 + minute * 60 + second;
}

function validateFormalTimeWindowInput(triggerTime, endtime) {
  const startSeconds = parseClientTriggerSeconds(triggerTime);
  if (startSeconds === null) return "正式开始时间格式应为 HH:MM";
  const endSeconds = parseClientEndtimeSeconds(endtime);
  if (endSeconds === null) return "正式截止时间格式应为 HH:MM:SS";
  const durationSeconds = endSeconds - startSeconds;
  if (durationSeconds <= 0) return "正式截止时间必须晚于正式开始时间";
  if (durationSeconds > 30 * 60) return "正式开始时间和截止时间间隔不能超过 30 分钟";
  return "";
}

function getTestEndtimeState(s) {
  const overrideEndtime = String(s?.test_endtime_override_endtime || "").trim();
  const expiresMs = Date.parse(s?.test_endtime_override_expires_at || "");
  const active = !!overrideEndtime && Number.isFinite(expiresMs) && expiresMs > Date.now();
  const remainingSeconds = active ? Math.max(0, Math.ceil((expiresMs - Date.now()) / 1000)) : 0;
  return {
    active,
    remainingSeconds,
    overrideEndtime,
    effectiveEndtime: active ? overrideEndtime : (s?.endtime || "-"),
  };
}

function formatRemainingSeconds(seconds) {
  const total = Math.max(0, parseInt(seconds, 10) || 0);
  const min = Math.floor(total / 60);
  const sec = total % 60;
  return \`\${min}分\${String(sec).padStart(2, "0")}秒\`;
}

function renderTestEndtimePanel(s) {
  const state = getTestEndtimeState(s);
  const inputValue = escapeHtml(s?.test_endtime || state.overrideEndtime || "");
  return \`
    <div class="test-override-panel">
      <div class="test-override-status">
        <strong>测试覆盖:</strong>
        <span id="test_endtime_status_pill" class="test-status-pill \${state.active ? "test-status-on" : "test-status-off"}">\${state.active ? "开" : "关"}</span>
        <span id="test_endtime_status_text">\${state.active ? \`当前使用测试截止时间 \${state.overrideEndtime}，剩余 \${formatRemainingSeconds(state.remainingSeconds)}\` : \`当前使用正式截止时间 \${s?.endtime || "-"}\`}</span>
      </div>
      <div class="test-override-row">
        <div class="form-group" style="margin-bottom:0">
          <label>测试截止时间 (HH:MM:SS)</label>
          <input type="text" id="school_test_endtime" value="\${inputValue}" placeholder="例如: 20:00:40">
        </div>
        <button type="button" class="btn btn-success" onclick="startTestEndtimeOverride()">测试启动</button>
        <button type="button" class="btn btn-secondary" onclick="stopTestEndtimeOverride()">关闭测试</button>
      </div>
      <div class="test-override-note">
        测试启动后只覆盖当前学校/组的手动触发截止时间；Worker 定时触发仍使用正式截止时间，3 分钟后自动回到正式截止时间。
      </div>
    </div>
  \`;
}

function getReadingZoneGroups() {
  const groups = currentSchool && Array.isArray(currentSchool.reading_zone_groups)
    ? currentSchool.reading_zone_groups
    : [];
  const normalized = normalizeReadingZoneGroups(groups);
  return normalized.length ? normalized : DEFAULT_READING_ZONE_GROUPS;
}

function normalizeReadingZoneGroups(raw) {
  if (!Array.isArray(raw) || raw.length === 0) return [];

  const normalizedGroups = [];
  const flatZones = [];

  for (const item of raw) {
    if (!item || typeof item !== "object") continue;

    // 结构1: [{ floor, zones: [{id,name}] }]
    if (Array.isArray(item.zones)) {
      const floor = String(item.floor || "未分层").trim() || "未分层";
      const zones = item.zones
        .map((z) => {
          if (!z || typeof z !== "object") return null;
          const id = String(z.id || z.roomid || "").trim();
          if (!id) return null;
          const name = String(z.name || z.roomName || z.title || id).trim() || id;
          return { id, name };
        })
        .filter(Boolean);

      if (zones.length) normalizedGroups.push({ floor, zones });
      continue;
    }

    // 结构2: [{ roomid, name, ... }] （extract_room_ids.py --json 输出）
    const id = String(item.roomid || item.id || "").trim();
    if (id) {
      const name = String(item.name || item.roomName || id).trim() || id;
      flatZones.push({ id, name });
    }
  }

  if (flatZones.length) normalizedGroups.push({ floor: "未分层", zones: flatZones });
  return normalizedGroups;
}

function _emptySlot() {
  return { roomid: "", seatid: "", times: "", seatPageId: "", fidEnc: "" };
}

function normalizePlanExtractTime(value) {
  const text = String(value || "").trim().replace(/[：∶.．。]/g, ":");
  const match = text.match(/^(\\d{1,2}):(\\d{2})$/);
  if (!match) return text;
  return \`\${match[1].padStart(2, "0")}:\${match[2].padStart(2, "0")}\`;
}

function planExtractTimeToMinutes(value) {
  const normalized = normalizePlanExtractTime(value);
  const match = normalized.match(/^(\\d{2}):(\\d{2})$/);
  if (!match) return null;
  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  if (Number.isNaN(hour) || Number.isNaN(minute)) return null;
  return hour * 60 + minute;
}

function planExtractMinutesToTime(totalMinutes) {
  const hour = Math.floor(totalMinutes / 60);
  const minute = totalMinutes % 60;
  return \`\${String(hour).padStart(2, "0")}:\${String(minute).padStart(2, "0")}\`;
}

function splitPlanExtractTimeRange(start, end, maxHoursPerObject = PLAN_EXTRACT_MAX_HOURS_DEFAULT) {
  const normalizedStart = normalizePlanExtractTime(start);
  const normalizedEnd = normalizePlanExtractTime(end);
  const startMinutes = planExtractTimeToMinutes(normalizedStart);
  const endMinutes = planExtractTimeToMinutes(normalizedEnd);

  if (startMinutes === null || endMinutes === null || endMinutes <= startMinutes) {
    return [[normalizedStart, normalizedEnd]];
  }

  const maxHours = Number(maxHoursPerObject);
  if (!Number.isFinite(maxHours) || maxHours <= 0) {
    return [[normalizedStart, normalizedEnd]];
  }

  const maxMinutes = Math.floor(maxHours * 60);
  if (maxMinutes <= 0) {
    return [[normalizedStart, normalizedEnd]];
  }

  const segments = [];
  let current = startMinutes;
  while (current < endMinutes) {
    const nextEnd = Math.min(current + maxMinutes, endMinutes);
    segments.push([planExtractMinutesToTime(current), planExtractMinutesToTime(nextEnd)]);
    current = nextEnd;
  }
  return segments;
}

function extractPlanTextMapping(rawText, options = {}) {
  const text = String(rawText || "");
  if (!text.trim()) {
    throw new Error("请先粘贴计划文本");
  }

  let roomid = "";
  let seatid = [];
  let seatPageId = String(options.seatPageId || "").trim();
  const fidEnc = String(options.fidEnc || "").trim();
  const lines = text.split(/\\r?\\n/);
  const roomPatterns = [
    /(?:自习室|阅览室|阅览区|房间)\\s*(?:id)?\\s*[：:=]?\\s*(\\d{3,})/i,
    /(?:roomid|seatpageid)\\s*[：:=]?\\s*(\\d{3,})/i,
    /(?:自习室|阅览室|阅览区|房间)\\D*(\\d{3,})/i,
  ];
  const seatPatterns = [
    /(?:座位号|座位|seatid|seat)\\s*[：:=]?\\s*([^\\n\\r]+)/i,
  ];

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    if (!roomid) {
      for (const pattern of roomPatterns) {
        const roomMatch = line.match(pattern);
        if (roomMatch) {
          roomid = roomMatch[1];
          break;
        }
      }
    }

    if (!seatPageId) {
      const seatPageMatch = line.match(/(?:seatpageid|seatPageId|页面id|页id)\\s*[：:=]?\\s*(\\d{3,})/i);
      if (seatPageMatch) {
        seatPageId = seatPageMatch[1];
      }
    }

    if (!seatid.length) {
      for (const pattern of seatPatterns) {
        const seatMatch = line.match(pattern);
        if (!seatMatch) continue;
        const nextSeats = String(seatMatch[1] || "").match(/\\d+/g);
        if (nextSeats && nextSeats.length) {
          seatid = nextSeats.map(v => String(v).padStart(3, "0"));
          break;
        }
      }
    }
  }

  if (!roomid) {
    for (const pattern of roomPatterns) {
      const roomMatch = text.match(pattern);
      if (roomMatch) {
        roomid = roomMatch[1];
        break;
      }
    }
  }
  if (!seatPageId) {
    const seatPageMatch = text.match(/(?:seatpageid|seatPageId|页面id|页id)\\s*[：:=]?\\s*(\\d{3,})/i);
    if (seatPageMatch) {
      seatPageId = seatPageMatch[1];
    }
  }
  if (!seatid.length) {
    for (const pattern of seatPatterns) {
      const seatMatch = text.match(pattern);
      if (!seatMatch) continue;
      const nextSeats = String(seatMatch[1] || "").match(/\\d+/g);
      if (nextSeats && nextSeats.length) {
        seatid = nextSeats.map(v => String(v).padStart(3, "0"));
        break;
      }
    }
  }

  if (!roomid) {
    throw new Error("未识别到自习室/阅览区/房间 ID");
  }
  if (!seatid.length) {
    throw new Error("未识别到座位号");
  }
  if (!seatPageId) {
    seatPageId = roomid;
  }

  const plans = [];
  const dayPrefixPattern = /^(周[一二三四五六日天])\\s*[:：∶]?\\s*(.*)$/;
  const everydayPrefixPattern = /^每天\\s*[:：=∶]?\\s*(.*)$/;
  const timeRangePattern = /(\\d{1,2}[:：∶.．。]\\d{2})\\s*[-~—–至]\\s*(\\d{1,2}[:：∶.．。]\\d{2})/g;

  const appendPlan = (daysofweek, start, end) => {
    const segments = splitPlanExtractTimeRange(start, end, options.maxHoursPerObject);
    for (const [segmentStart, segmentEnd] of segments) {
      plans.push({
        times: [segmentStart, segmentEnd],
        roomid,
        seatid: seatid.slice(),
        seatPageId,
        fidEnc,
        daysofweek,
      });
    }
  };

  let activeDaysofweek = null;
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    const dayMatch = line.match(dayPrefixPattern);
    if (dayMatch) {
      const dayEn = PLAN_EXTRACT_WEEK_MAP[dayMatch[1]];
      if (!dayEn) continue;
      activeDaysofweek = [dayEn];
      for (const match of dayMatch[2].matchAll(timeRangePattern)) {
        appendPlan([dayEn], match[1], match[2]);
      }
      continue;
    }

    const everydayMatch = line.match(everydayPrefixPattern);
    if (everydayMatch) {
      activeDaysofweek = PLAN_EXTRACT_ALL_DAYS.slice();
      for (const match of everydayMatch[1].matchAll(timeRangePattern)) {
        appendPlan(PLAN_EXTRACT_ALL_DAYS.slice(), match[1], match[2]);
      }
      continue;
    }

    if (activeDaysofweek) {
      for (const match of line.matchAll(timeRangePattern)) {
        appendPlan(activeDaysofweek.slice(), match[1], match[2]);
      }
    }
  }

  if (!plans.length) {
    throw new Error("未识别到有效的周计划时间段");
  }
  return plans;
}

function createEmptyWeeklySchedule() {
  const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  const schedule = {};
  for (const d of days) {
    schedule[d] = { enabled: false, slots: [_emptySlot(), _emptySlot(), _emptySlot(), _emptySlot()] };
  }
  return schedule;
}

function parseScheduleJsonMapping(rawText) {
  const parsed = JSON.parse(rawText);
  const items = Array.isArray(parsed) ? parsed : (parsed && typeof parsed === "object" ? [parsed] : []);
  if (!items.length) {
    throw new Error("周计划 JSON 必须是对象或数组");
  }

  const schedule = createEmptyWeeklySchedule();
  for (const item of items) {
    if (!item || typeof item !== "object") continue;

    const roomid = String(item.roomid || "").trim();
    const seatPageId = String(item.seatPageId || item.roomid || "").trim();
    const fidEnc = String(item.fidEnc || "").trim();

    const times = normalizeTimesLabel(item.times);

    let seatid = item.seatid;
    if (Array.isArray(seatid)) {
      seatid = seatid.map(v => String(v).trim()).filter(Boolean).join(",");
    } else {
      seatid = String(seatid || "").trim();
    }

    const daysofweek = Array.isArray(item.daysofweek) ? item.daysofweek : [];
    for (const day of daysofweek) {
      if (!schedule[day]) continue;
      schedule[day].enabled = true;
      schedule[day].slots.push({ roomid, seatid, times, seatPageId, fidEnc });
    }
  }

  for (const day of Object.keys(schedule)) {
    const slots = (schedule[day].slots || []).filter(s => s && (s.roomid || s.times));
    if (slots.length === 0) {
      schedule[day].enabled = false;
      schedule[day].slots = [_emptySlot(), _emptySlot(), _emptySlot(), _emptySlot()];
      continue;
    }
    schedule[day].slots = slots;
  }

  return schedule;
}

function scheduleToJsonMapping(schedule) {
  const result = [];
  const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  for (const day of days) {
    const dayCfg = schedule?.[day];
    if (!dayCfg || !dayCfg.enabled) continue;
    const slots = Array.isArray(dayCfg.slots)
      ? dayCfg.slots
      : [{ roomid: dayCfg.roomid, seatid: dayCfg.seatid, times: dayCfg.times, seatPageId: dayCfg.seatPageId, fidEnc: dayCfg.fidEnc }];
    for (const s of slots) {
      if (!s || !s.roomid || !s.times) continue;
      const times = parseTimesInput(s.times);
      const seatid = String(s.seatid || "").split(",").map(x => x.trim()).filter(Boolean);
      result.push({
        times,
        roomid: String(s.roomid || ""),
        seatid,
        seatPageId: String(s.seatPageId || s.roomid || ""),
        fidEnc: String(s.fidEnc || ""),
        daysofweek: [day],
      });
    }
  }
  return result;
}

function fillScheduleFormFromSchedule(schedule) {
  const days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];
  days.forEach(d => {
    const sch = schedule?.[d] || {};
    document.getElementById("sch_" + d + "_enabled").checked = !!sch.enabled;
    const slots = sch.slots || [{ roomid: sch.roomid, seatid: sch.seatid, times: sch.times, seatPageId: sch.seatPageId, fidEnc: sch.fidEnc }];
    const activeCount = slots.filter(s => s && (s.roomid || s.seatid || s.times || s.seatPageId || s.fidEnc)).length;
    const visibleCount = Math.max(1, Math.min(4, activeCount || 1));
    setVisibleSlotsForDay(d, visibleCount);
    [0,1,2,3].forEach(i => {
      const s = slots[i] || {};
      document.getElementById("sch_" + d + "_s" + i + "_roomid").value = s.roomid || "";
      document.getElementById("sch_" + d + "_s" + i + "_seatid").value = s.seatid || "";
      document.getElementById("sch_" + d + "_s" + i + "_times").value = s.times || "";
      document.getElementById("sch_" + d + "_s" + i + "_seatPageId").value = s.seatPageId || "";
      document.getElementById("sch_" + d + "_s" + i + "_fidEnc").value = s.fidEnc || "";
    });
  });
}

function setVisibleSlotsForDay(day, count) {
  const visibleCount = Math.max(1, Math.min(4, parseInt(count, 10) || 1));
  [0,1,2,3].forEach(i => {
    const row = document.getElementById("sch_" + day + "_row_" + i);
    if (!row) return;
    row.style.display = i < visibleCount ? "" : "none";
  });
}

function getVisibleSlotsForDay(day) {
  let count = 0;
  [0,1,2,3].forEach(i => {
    const row = document.getElementById("sch_" + day + "_row_" + i);
    if (row && row.style.display !== "none") count++;
  });
  return Math.max(1, count);
}

function addSlotForDay(day) {
  const current = getVisibleSlotsForDay(day);
  setVisibleSlotsForDay(day, current + 1);
}

function applyScheduleJsonToForm() {
  const scheduleJsonText = (document.getElementById("edit_user_schedule_json").value || "").trim();
  if (!scheduleJsonText) return toast("请先粘贴周计划 JSON", "error");
  try {
    const schedule = parseScheduleJsonMapping(scheduleJsonText);
    fillScheduleFormFromSchedule(schedule);
    toast("已映射到周计划配置");
  } catch (e) {
    toast("周计划 JSON 解析失败: " + (e.message || String(e)), "error");
  }
}

function buildPlanMappingFromPanel() {
  const inputEl = document.getElementById("plan_extract_input");
  const outputEl = document.getElementById("plan_extract_output");
  const maxHoursEl = document.getElementById("plan_extract_max_hours");
  const seatPageIdEl = document.getElementById("plan_extract_seat_page_id");
  if (!inputEl || !outputEl || !maxHoursEl) {
    throw new Error("映射面板尚未加载完成");
  }

  const maxHoursText = String(maxHoursEl.value || "").trim();
  let maxHours = PLAN_EXTRACT_MAX_HOURS_DEFAULT;
  if (maxHoursText !== "") {
    maxHours = Number(maxHoursText);
    if (!Number.isFinite(maxHours) || maxHours < 0) {
      throw new Error("最长时段小时数必须是大于等于 0 的数字");
    }
  }

  const plans = extractPlanTextMapping(inputEl.value, {
    maxHoursPerObject: maxHours,
    seatPageId: String(seatPageIdEl?.value || "").trim(),
    fidEnc: currentSchool?.fidEnc || "",
  });
  const jsonText = JSON.stringify(plans, null, 2);
  outputEl.value = jsonText;
  return { plans, jsonText };
}

function generatePlanMappingJson() {
  try {
    buildPlanMappingFromPanel();
    toast("已生成周计划 JSON");
  } catch (e) {
    toast("计划文本映射失败: " + (e.message || String(e)), "error");
  }
}

async function copyPlanMappingJson() {
  const outputEl = document.getElementById("plan_extract_output");
  if (!outputEl || !String(outputEl.value || "").trim()) {
    return toast("请先生成周计划 JSON", "error");
  }
  await copyTextToClipboard(outputEl.value, "已复制周计划 JSON");
}

function createMappedUserDraft() {
  try {
    const { plans, jsonText } = buildPlanMappingFromPanel();
    const first = plans[0] || {};
    const firstSeat = Array.isArray(first.seatid) ? first.seatid.join(",") : "";
    const phone = String(document.getElementById("plan_extract_phone")?.value || "").trim();
    const password = String(document.getElementById("plan_extract_password")?.value || "");
    const username = String(document.getElementById("plan_extract_username")?.value || "").trim();
    showAddUser({
      phone,
      password,
      username,
      remark: first.roomid && firstSeat ? \`自动映射 \${first.roomid}/\${firstSeat}\` : "自动映射",
      scheduleJsonText: jsonText,
      schedule: parseScheduleJsonMapping(jsonText),
    });
    toast("已生成新用户草稿");
  } catch (e) {
    toast("生成新用户草稿失败: " + (e.message || String(e)), "error");
  }
}

async function api(method, path, body = null) {
  const opts = {
    method,
    headers: { "Content-Type": "application/json", "X-API-Key": API_KEY },
  };
  if (body) opts.body = JSON.stringify(body);
  let res;
  try {
    res = await fetch(API_BASE + path, opts);
  } catch (e) {
    return { ok: false, error: "网络请求失败", detail: e.message || String(e), status: 0 };
  }

  const raw = await res.text();
  let data;
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch (_e) {
    data = { ok: res.ok };
    if (!res.ok) {
      data.error = "HTTP " + res.status;
      data.detail = raw;
    }
  }

  if (!data || typeof data !== "object") data = { ok: res.ok };
  if (data.status === undefined) data.status = res.status;
  if (!res.ok && !data.error) data.error = "HTTP " + res.status;
  return data;
}

function toast(msg, type = "success") {
  const t = document.createElement("div");
  t.className = "toast toast-" + type;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 3000);
}

function setUserSavePending(pending) {
  isSavingUser = pending;
  const btn = document.getElementById("saveUserButton");
  if (!btn) return;
  btn.disabled = pending;
  btn.textContent = pending ? "保存中..." : "保存用户";
}

function escapeHtml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderFatalError(error, source = "runtime") {
  const app = document.getElementById("app");
  if (!app) return;
  const message = error && (error.stack || error.message || String(error)) || "Unknown error";
  app.innerHTML = \`
    <div class="container">
      <div class="card" style="margin-top:32px;border:1px solid #ffd6d6">
        <div class="card-header">
          <span class="card-title" style="color:#d4380d">页面加载失败</span>
        </div>
        <div style="font-size:14px;color:#666;line-height:1.7">
          <p>前端脚本遇到了异常，已停止渲染。</p>
          <p><strong>source:</strong> \${escapeHtml(source)}</p>
          <pre style="margin-top:12px;white-space:pre-wrap;word-break:break-word;background:#fff7f7;border-radius:8px;padding:12px;color:#a61d24">\${escapeHtml(message)}</pre>
        </div>
      </div>
    </div>
  \`;
}

async function copyTextToClipboard(text, successMessage = "已复制") {
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      const input = document.createElement("input");
      input.value = text;
      document.body.appendChild(input);
      input.select();
      document.execCommand("copy");
      input.remove();
    }
    toast(successMessage);
  } catch (e) {
    toast("复制失败，请手动复制", "error");
  }
}

async function copyRoomId(id) {
  await copyTextToClipboard(id, "已复制 ID: " + id);
}

function render() {
  const app = document.getElementById("app");
  if (currentView === "login") {
    app.innerHTML = renderLogin();
  } else if (currentView === "schools") {
    app.innerHTML = renderSchools();
  } else if (currentView === "school") {
    app.innerHTML = renderSchoolDetail();
  }
  bindEvents();
  updateTestEndtimeStatusView();
}

function updateTestEndtimeStatusView() {
  if (currentView !== "school" || !currentSchool) return;
  const pill = document.getElementById("test_endtime_status_pill");
  const text = document.getElementById("test_endtime_status_text");
  if (!pill || !text) return;

  const state = getTestEndtimeState(currentSchool);
  pill.className = "test-status-pill " + (state.active ? "test-status-on" : "test-status-off");
  pill.textContent = state.active ? "开" : "关";
  text.textContent = state.active
    ? \`当前使用测试截止时间 \${state.overrideEndtime}，剩余 \${formatRemainingSeconds(state.remainingSeconds)}\`
    : \`当前使用正式截止时间 \${currentSchool.endtime || "-"}\`;
}

function renderLogin() {
  return \`
    <div class="login-box">
      <h2>统一抢座管理系统</h2>
      <input type="password" id="apiKey" placeholder="请输入管理密钥">
      <button onclick="doLogin()">登 录</button>
    </div>
  \`;
}

function browserBeijingDayOfWeek() {
  const d = new Date(Date.now() + 8 * 3600 * 1000);
  const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
  return days[d.getUTCDay()];
}

function getEnabledScheduleSlotsClient(daySchedule) {
  if (!daySchedule || !daySchedule.enabled) return [];
  const rawSlots = Array.isArray(daySchedule.slots)
    ? daySchedule.slots
    : [{
        roomid: daySchedule.roomid,
        seatid: daySchedule.seatid,
        times: daySchedule.times,
        seatPageId: daySchedule.seatPageId || "",
        fidEnc: daySchedule.fidEnc || "",
      }];
  return rawSlots.filter(slot => slot && slot.times && slot.roomid);
}

function countActiveUsersForTodayClient(userList) {
  const today = browserBeijingDayOfWeek();
  return (Array.isArray(userList) ? userList : []).filter(user => {
    if (!user || user.status !== "active") return false;
    return getEnabledScheduleSlotsClient(user.schedule && user.schedule[today]).length > 0;
  }).length;
}

function getCachedActiveTodayCount(schoolId) {
  try {
    const raw = localStorage.getItem(ACTIVE_TODAY_CACHE_PREFIX + schoolId);
    if (!raw) return null;
    const cached = JSON.parse(raw);
    if (!cached || cached.expiresAt <= Date.now()) {
      localStorage.removeItem(ACTIVE_TODAY_CACHE_PREFIX + schoolId);
      return null;
    }
    return cached;
  } catch (_e) {
    return null;
  }
}

function setCachedActiveTodayCount(schoolId, payload) {
  try {
    localStorage.setItem(
      ACTIVE_TODAY_CACHE_PREFIX + schoolId,
      JSON.stringify(payload)
    );
  } catch (_e) {
    // ignore localStorage quota or privacy errors
  }
}

function formatActiveTodayMeta(schoolId) {
  const cached = getCachedActiveTodayCount(schoolId);
  if (!cached) return "今日活跃: 统计中";
  if (cached.error) return "今日活跃: 统计失败";
  return "今日活跃: " + cached.count + " 人";
}

async function ensureActiveTodayCount(schoolId, force = false) {
  const cached = getCachedActiveTodayCount(schoolId);
  if (!force && cached) return cached;

  try {
    const res = await api("GET", "/api/school/" + schoolId + "/users");
    if (res.error) throw new Error(res.error);
    const next = {
      count: countActiveUsersForTodayClient(res.users || []),
      expiresAt: Date.now() + ACTIVE_TODAY_CACHE_TTL_MS,
      error: "",
    };
    setCachedActiveTodayCount(schoolId, next);
    return next;
  } catch (e) {
    const next = {
      count: 0,
      expiresAt: Date.now() + ACTIVE_TODAY_CACHE_TTL_MS,
      error: e.message || String(e),
    };
    setCachedActiveTodayCount(schoolId, next);
    return next;
  }
}

async function refreshSchoolActiveTodayCounts(force = false) {
  if (!API_KEY || !Array.isArray(schools) || schools.length === 0) return;
  await Promise.all(
    schools
      .filter(s => s && s.id)
      .map(s => ensureActiveTodayCount(s.id, force))
  );
  if (currentView === "schools") render();
}

function parseTriggerTimeMinutes(value) {
  const text = String(value || "").trim();
  const match = text.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return Number.MAX_SAFE_INTEGER;
  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  if (Number.isNaN(hour) || Number.isNaN(minute)) return Number.MAX_SAFE_INTEGER;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return Number.MAX_SAFE_INTEGER;
  return hour * 60 + minute;
}

function getSortedSchoolsForDisplay(items) {
  return (Array.isArray(items) ? items : [])
    .filter(Boolean)
    .slice()
    .sort((a, b) => {
      const timeDiff = parseTriggerTimeMinutes(a?.trigger_time) - parseTriggerTimeMinutes(b?.trigger_time);
      if (timeDiff !== 0) return timeDiff;
      return String(a?.id || "").localeCompare(String(b?.id || ""));
    });
}

function upsertSchoolInOrderedList(items, school, options = {}) {
  const list = (Array.isArray(items) ? items : []).filter(Boolean).slice();
  const existingIndex = list.findIndex(item => item && item.id === school?.id);
  const previous = existingIndex >= 0 ? list[existingIndex] : null;
  const shouldResort = options.forceResort || !previous || previous.trigger_time !== school?.trigger_time;

  if (existingIndex >= 0) {
    list[existingIndex] = school;
  } else {
    list.push(school);
  }

  return shouldResort ? getSortedSchoolsForDisplay(list) : list;
}

function renderSchools() {
  const now = new Date().toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });
  return \`
    <div class="container">
      <div class="header">
        <h1>统一抢座管理系统</h1>
        <div class="time">\${now}</div>
      </div>
      <div class="card">
        <div class="card-header">
          <span class="card-title">学校列表</span>
          <button class="btn btn-primary" onclick="showAddSchool()">+ 添加学校</button>
        </div>
        <div class="school-grid">
          \${schools.length ? schools.map(s => \`
            <div class="school-card" onclick="openSchool('\${s.id}')">
              <h3>\${s.name}</h3>
              <div class="meta">ID: \${s.id} | 仓库: \${s.repo}</div>
              <div class="stats">
                <span>\${s.userCount || 0} 名用户</span>
                <span>\${formatActiveTodayMeta(s.id)}</span>
                <span>正式开始: \${s.trigger_time}</span>
              </div>
            </div>
          \`).join("") : '<div class="empty"><div class="empty-icon">📚</div><p>暂无学校，点击上方按钮添加</p></div>'}
        </div>
      </div>
    </div>
    \${renderAddSchoolModal()}
  \`;
}

function renderAddSchoolModal() {
  return \`
    <div class="modal" id="addSchoolModal">
      <div class="modal-content">
        <div class="modal-header">
          <h3>添加学校</h3>
          <span class="modal-close" onclick="closeModal('addSchoolModal')">&times;</span>
        </div>
        <div class="modal-body">
          <div class="form-row">
            <div class="form-group">
              <label>学校 ID（如 001）</label>
              <input type="text" id="new_school_id" placeholder="001">
            </div>
            <div class="form-group">
              <label>学校名称</label>
              <input type="text" id="new_school_name" placeholder="华东师范大学">
            </div>
          </div>
          <div class="form-group">
            <label>GitHub 仓库</label>
            <input type="text" id="new_school_repo" placeholder="BAOfuZhan/hcd">
          </div>
          <div class="form-group">
            <label>分发目标</label>
            <select id="new_school_dispatch_target">
              <option value="github">github - 仅 GitHub Actions</option>
              <option value="server_relay">server_relay - GitHub 中转到服务器</option>
            </select>
          </div>
          <div id="new_school_server_only_fields">
            <div class="form-group">
              <label>选座接口模式</label>
              <select id="new_school_seat_api_mode">
                <option value="auto">auto - 优先 seatengine，失败自动回退</option>
                <option value="seatengine">seatengine - 强制新版接口</option>
                <option value="seat" selected>seat - 强制旧版接口</option>
              </select>
            </div>
            <div class="form-row">
              <div class="form-group">
                <label><input type="checkbox" id="new_school_reserve_next_day" checked> 预约明天</label>
              </div>
              <div class="form-group">
                <label><input type="checkbox" id="new_school_enable_slider"> 启用滑块验证码</label>
              </div>
            </div>
            <div class="form-group">
              <label><input type="checkbox" id="new_school_enable_textclick"> 启用选字验证码</label>
            </div>
          </div>
          <div class="form-group">
            <label>冲突分组</label>
            <input type="text" id="new_school_conflict_group" placeholder="可留空；留空时优先按学校 fidEnc 自动归并">
          </div>
          <div class="form-group">
            <label>GitHub 密匙槽位</label>
            <select id="new_school_github_token_key">
              <option value="">默认 GH_TOKEN</option>
              <option value="a">A -> GH_TOKEN_A</option>
              <option value="b">B -> GH_TOKEN_B</option>
              <option value="c">C -> GH_TOKEN_C</option>
              <option value="d">D -> GH_TOKEN_D</option>
              <option value="e">E -> GH_TOKEN_E</option>
            </select>
          </div>
          <div id="new_school_relay_fields">
            <div class="form-row">
              <div class="form-group">
                <label>服务器分发地址</label>
                <input type="text" id="new_school_server_url" placeholder="例如: https://your-server.example.com/dispatch">
              </div>
              <div class="form-group">
                <label>服务器最大并发</label>
                <input type="number" id="new_school_server_max_concurrency" value="13" min="1">
              </div>
            </div>
            <div class="form-group">
              <label>提交 day 日期偏移（仅服务器中转）</label>
              <input type="number" id="new_school_reserve_day_offset" min="0" step="1" placeholder="留空沿用预约明天；0 今天，1 明天，2 后天">
            </div>
            <div class="form-group">
              <label>服务器 API Key（可留空，优先使用 Worker 环境变量）</label>
              <input type="text" id="new_school_server_api_key" placeholder="留空则回退到 Worker 环境变量 SERVER_DISPATCH_API_KEY">
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>正式开始时间</label>
              <input type="text" id="new_school_trigger" value="19:57" placeholder="HH:MM">
            </div>
            <div class="form-group">
              <label>正式截止时间</label>
              <input type="text" id="new_school_endtime" value="20:00:40" placeholder="HH:MM:SS">
            </div>
          </div>
          <div class="form-group">
            <label>学校统一 fidEnc（全校共用）</label>
            <input type="text" id="new_school_fidEnc" placeholder="例如: 1b001674cae092c3">
          </div>
          <button class="btn btn-primary" onclick="doAddSchool()" style="width:100%;margin-top:10px">创建学校</button>
        </div>
      </div>
    </div>
  \`;
}

function renderSchoolDetail() {
  const s = currentSchool;
  if (!s) return "";
  return \`
    <div class="container">
      <div class="header">
        <h1>\${s.name}</h1>
        <div class="actions">
          <button class="btn btn-secondary" onclick="backToSchools()">返回列表</button>
          <button class="btn btn-primary" onclick="showEditSchool()">编辑配置</button>
          <button class="btn btn-success" onclick="triggerSchool()">手动触发</button>
        </div>
      </div>
      <div class="breadcrumb">
        <a href="#" onclick="backToSchools();return false">学校列表</a>
        <span>></span>
        <span>\${s.name}</span>
      </div>
      <div class="card">
        <div class="card-header">
          <span class="card-title">学校配置</span>
        </div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;font-size:14px">
          <div><strong>学校ID:</strong> \${s.id}</div>
          <div><strong>正式开始时间:</strong> \${s.trigger_time}</div>
          <div><strong>正式截止时间:</strong> \${s.endtime}</div>
          <div><strong>GitHub仓库:</strong> \${s.repo}</div>
          <div><strong>今日活跃用户:</strong> \${formatActiveTodayMeta(s.id)}</div>
          <div><strong>GitHub 密匙槽位:</strong> \${s.github_token_key ? s.github_token_key.toUpperCase() : "默认 GH_TOKEN"}</div>
          <div><strong>选座接口:</strong> \${s.seat_api_mode || "seat"}</div>
          <div><strong>预约日期:</strong> \${formatReserveDayLabel(s)}</div>
          <div><strong>学校 fidEnc:</strong> \${s.fidEnc || "-"}</div>
          <div><strong>冲突分组:</strong> \${s.conflict_group || (s.fidEnc ? "自动按 fidEnc" : (s.name || "-"))}</div>
          <div><strong>验证码:</strong> \${s.enable_slider ? "滑块" : (s.enable_textclick ? "选字" : "关闭")}</div>
          <div><strong>分发目标:</strong> \${s.dispatch_target || "github"}</div>
          \${s.dispatch_target === "server_relay" ? \`
            <div><strong>服务器地址:</strong> \${s.server_url || "-"}</div>
            <div><strong>服务器并发:</strong> \${s.server_max_concurrency || 13}</div>
            <div><strong>服务器密钥:</strong> \${s.has_server_api_key ? "已配置" : "未配置/使用环境变量"}</div>
          \` : ""}
        </div>
        \${renderTestEndtimePanel(s)}
      </div>
      <div class="card">
        <div class="card-header">
          <span class="card-title">用户管理</span>
          <button class="btn btn-primary" onclick="showAddUser()">+ 添加用户</button>
        </div>
        \${users.length ? \`
          <table class="user-table">
            <thead>
              <tr>
                <th>手机号（账号）</th>
                <th>昵称</th>
                <th>状态</th>
                <th>今日计划</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              \${users.slice().sort((a, b) => {
                const na = (a.username || a.remark || "").toLowerCase();
                const nb = (b.username || b.remark || "").toLowerCase();
                return na.localeCompare(nb);
              }).map(u => {
                const today = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"][new Date().getDay()];
                const todaySch = u.schedule[today];
                const todayStr = (() => {
                  if (!todaySch || !todaySch.enabled) return "无";
                  const slots = todaySch.slots || [{ roomid: todaySch.roomid, times: todaySch.times }];
                  const active = slots.filter(s => s.times && s.roomid);
                  if (active.length === 0) return "已启用/无有效时段";
                  return active.map(s => s.times).join(" | ");
                })();
                return \`
                  <tr>
                    <td>\${u.phone || "-"}</td>
                    <td>\${u.username || u.remark || "-"}</td>
                    <td class="status-\${u.status}">\${u.status === "active" ? "活跃" : "暂停"}</td>
                    <td style="font-size:12px">\${todayStr}</td>
                    <td class="actions">
                      <button class="btn btn-sm btn-secondary" onclick="showEditUser('\${u.id}')">编辑</button>
                      \${u.status === "active" 
                        ? \`<button class="btn btn-sm btn-danger" onclick="pauseUser('\${u.id}')">暂停</button>\`
                        : \`<button class="btn btn-sm btn-success" onclick="resumeUser('\${u.id}')">恢复</button>\`}
                      <button class="btn btn-sm btn-primary" onclick="triggerUser('\${u.id}')">触发</button>
                      <button class="btn btn-sm btn-danger" onclick="deleteUser('\${u.id}')">删除</button>
                    </td>
                  </tr>
                \`;
              }).join("")}
            </tbody>
          </table>
        \` : '<div class="empty"><div class="empty-icon">👤</div><p>暂无用户，点击上方按钮添加</p></div>'}
      </div>
      <div class="card">
        <div class="card-header">
          <span class="card-title">阅览区 ID 速查</span>
        </div>
        \${renderReadingZonePanel()}
      </div>
      <div class="card">
        <div class="card-header">
          <span class="card-title">计划文本映射</span>
        </div>
        \${renderPlanMappingPanel()}
      </div>
    </div>
    \${renderEditSchoolModal()}
    \${renderUserModal()}
  \`;
}

function renderReadingZonePanel() {
  const groups = getReadingZoneGroups();
  return \`
    <div class="zone-grid">
      \${groups.map(group => \`
        <div class="zone-card">
          <div class="zone-floor">\${group.floor}</div>
          <div class="zone-list">
            \${group.zones.map(z => \`
              <div class="zone-item">
                <span>\${z.name}</span>
                <div class="zone-right">
                  <span class="zone-id">\${z.id}</span>
                  <button class="copy-btn" onclick="copyRoomId('\${z.id}')">复制</button>
                </div>
              </div>
            \`).join("")}
          </div>
        </div>
      \`).join("")}
    </div>
  \`;
}

function renderPlanMappingPanel() {
  return \`
    <div class="mapping-grid">
      <div class="mapping-box">
        <h4>原始计划文本</h4>
        <div class="mapping-inline">
          <div>
            <label>最长单段小时数</label>
            <input type="number" id="plan_extract_max_hours" min="0" step="0.5" value="\${PLAN_EXTRACT_MAX_HOURS_DEFAULT}">
          </div>
          <div class="mapping-note" style="margin-top:0">
            留空默认 \${PLAN_EXTRACT_MAX_HOURS_DEFAULT} 小时，填 <code>0</code> 表示不拆分超长时间段。
          </div>
        </div>
        <div class="mapping-user-fields">
          <div>
            <label>手机号</label>
            <input type="text" id="plan_extract_phone" placeholder="生成新用户草稿时带入">
          </div>
          <div>
            <label>密码</label>
            <input type="text" id="plan_extract_password" placeholder="生成新用户草稿时带入">
          </div>
          <div>
            <label>昵称</label>
            <input type="text" id="plan_extract_username" placeholder="生成新用户草稿时带入">
          </div>
          <div>
            <label>seatPageId</label>
            <input type="text" id="plan_extract_seat_page_id" placeholder="可选；不填则默认等于 roomid">
          </div>
        </div>
        <textarea id="plan_extract_input" rows="12" placeholder="示例：
自习室id：13476
座位号:367
时间段:
周一:14:30-22:00
周二:9:30-22:00
每天:16:30-22:00">自习室id:</textarea>
        <div class="mapping-actions">
          <button type="button" class="btn btn-primary" onclick="generatePlanMappingJson()">生成周计划 JSON</button>
          <button type="button" class="btn btn-success" onclick="createMappedUserDraft()">一键生成新用户草稿</button>
        </div>
        <div class="mapping-note">
          支持 <code>周一:08:00-12:00，14:00-16:00</code>、<code>周一:</code> 后下一行写时间段，以及 <code>13.00-18.00</code> 这类点号时间。
          “一键生成新用户草稿”会把上面手动填写的手机号、密码、昵称带入新增用户弹窗，不会覆盖已有用户；真正保存时仍走现有座位冲突校验。
        </div>
      </div>
      <div class="mapping-box">
        <h4>周计划 JSON 映射结果</h4>
        <textarea id="plan_extract_output" rows="12" readonly placeholder="生成后会出现在这里，可直接复制，也会被一键带入新增用户弹窗。"></textarea>
        <div class="mapping-actions">
          <button type="button" class="btn btn-secondary" onclick="copyPlanMappingJson()">复制 JSON</button>
        </div>
        <div class="mapping-note">
          输出结构与用户弹窗里的“周计划 JSON 映射”兼容。若你手填了 <code>seatPageId</code>，就优先使用它；没填时才自动回退为 <code>roomid</code>，并带上当前学校配置里的 <code>fidEnc</code>。
        </div>
      </div>
    </div>
  \`;
}

function renderEditSchoolModal() {
  const s = currentSchool || {};
  const st = s.strategy || {};
  const readingZonesText = JSON.stringify(s.reading_zone_groups || [], null, 2)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
  return \`
    <div class="modal" id="editSchoolModal">
      <div class="modal-content">
        <div class="modal-header">
          <h3>编辑学校配置</h3>
          <span class="modal-close" onclick="closeModal('editSchoolModal')">&times;</span>
        </div>
        <div class="modal-body">
          <div class="form-row">
            <div class="form-group">
              <label>学校名称</label>
              <input type="text" id="edit_school_name" value="\${s.name || ''}">
            </div>
            <div class="form-group">
              <label>GitHub 仓库</label>
              <input type="text" id="edit_school_repo" value="\${s.repo || ''}">
            </div>
          </div>
          <div class="form-group">
            <label>分发目标</label>
            <select id="edit_school_dispatch_target">
              <option value="github" \${(!s.dispatch_target || s.dispatch_target==="github") ? "selected" : ""}>github - 仅 GitHub Actions</option>
              <option value="server_relay" \${s.dispatch_target==="server_relay" ? "selected" : ""}>server_relay - GitHub 中转到服务器</option>
            </select>
          </div>
          <div class="form-group">
            <label>GitHub 密匙槽位</label>
            <select id="edit_school_github_token_key">
              <option value="" \${!s.github_token_key ? "selected" : ""}>默认 GH_TOKEN</option>
              <option value="a" \${s.github_token_key==="a" ? "selected" : ""}>A -> GH_TOKEN_A</option>
              <option value="b" \${s.github_token_key==="b" ? "selected" : ""}>B -> GH_TOKEN_B</option>
              <option value="c" \${s.github_token_key==="c" ? "selected" : ""}>C -> GH_TOKEN_C</option>
              <option value="d" \${s.github_token_key==="d" ? "selected" : ""}>D -> GH_TOKEN_D</option>
              <option value="e" \${s.github_token_key==="e" ? "selected" : ""}>E -> GH_TOKEN_E</option>
            </select>
          </div>
          <div id="edit_school_server_only_fields">
            <div class="form-group">
              <label>选座接口模式</label>
              <select id="edit_school_seat_api_mode">
                <option value="auto" \${s.seat_api_mode==="auto" ? "selected" : ""}>auto - 优先 seatengine，失败自动回退</option>
                <option value="seatengine" \${s.seat_api_mode==="seatengine" ? "selected" : ""}>seatengine - 强制新版接口</option>
                <option value="seat" \${(!s.seat_api_mode || s.seat_api_mode==="seat") ? "selected" : ""}>seat - 强制旧版接口</option>
              </select>
            </div>
            <div class="form-row">
              <div class="form-group">
                <label><input type="checkbox" id="edit_school_reserve_next_day" \${s.reserve_next_day === false ? "" : "checked"}> 预约明天</label>
              </div>
              <div class="form-group">
                <label><input type="checkbox" id="edit_school_enable_slider" \${s.enable_slider ? "checked" : ""}> 启用滑块验证码</label>
              </div>
            </div>
            <div class="form-group">
              <label><input type="checkbox" id="edit_school_enable_textclick" \${s.enable_textclick ? "checked" : ""}> 启用选字验证码</label>
            </div>
          </div>
          <div id="edit_school_relay_fields">
            <div class="form-row">
              <div class="form-group">
                <label>服务器分发地址</label>
                <input type="text" id="edit_school_server_url" value="\${s.server_url || ''}" placeholder="例如: https://your-server.example.com/dispatch">
              </div>
              <div class="form-group">
                <label>服务器最大并发</label>
                <input type="number" id="edit_school_server_max_concurrency" value="\${s.server_max_concurrency || 13}" min="1">
              </div>
            </div>
            <div class="form-group">
              <label>提交 day 日期偏移（仅服务器中转）</label>
              <input type="number" id="edit_school_reserve_day_offset" min="0" step="1" value="\${s.reserve_day_offset === null || s.reserve_day_offset === undefined ? '' : s.reserve_day_offset}" placeholder="留空沿用预约明天；0 今天，1 明天，2 后天">
            </div>
            <div class="form-group">
              <label>服务器 API Key（留空则保留已有值；使用 ****** 表示不改）</label>
              <input type="text" id="edit_school_server_api_key" value="" placeholder="\${s.has_server_api_key ? '已配置，留空不修改' : '留空则使用 Worker 环境变量 SERVER_DISPATCH_API_KEY'}">
            </div>
          </div>
          <div class="form-group">
            <label>冲突分组</label>
            <input type="text" id="edit_school_conflict_group" value="\${s.conflict_group || ''}" placeholder="可留空；留空时优先按学校 fidEnc 自动归并">
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>正式开始时间 (HH:MM)</label>
              <input type="text" id="edit_school_trigger" value="\${s.trigger_time || '19:57'}">
            </div>
            <div class="form-group">
              <label>正式截止时间 (HH:MM:SS)</label>
              <input type="text" id="edit_school_endtime" value="\${s.endtime || '20:00:40'}">
            </div>
          </div>
          <div class="form-group">
            <label>学校统一 fidEnc（全校共用）</label>
            <input type="text" id="edit_school_fidEnc" value="\${s.fidEnc || ''}" placeholder="例如: 1b001674cae092c3">
          </div>
          <div class="form-group">
            <label>阅览区映射 JSON（reading_zone_groups）</label>
            <textarea id="edit_school_reading_zones" rows="8" placeholder='示例: [{"floor":"3楼","zones":[{"id":"13484","name":"中阅览区"}]}]'>\${readingZonesText}</textarea>
          </div>
          <h4 style="margin:20px 0 12px">策略配置</h4>
          <div class="form-row">
            <div class="form-group">
              <label>策略模式（mode）</label>
              <select id="edit_strategy_mode">
                <option value="A" \${st.mode==="A"?"selected":""}>A - 预取token</option>
                <option value="B" \${st.mode==="B"?"selected":""}>B - 即时取token</option>
                <option value="C" \${st.mode==="C"?"selected":""}>C - 延迟取token</option>
              </select>
            </div>
            <div class="form-group">
              <label>提交并发方式（submit_mode）</label>
              <select id="edit_strategy_submit">
                <option value="serial" \${st.submit_mode==="serial"?"selected":""}>serial - 串行</option>
                <option value="burst" \${st.submit_mode==="burst"?"selected":""}>burst - 并行</option>
              </select>
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>提前登录秒数（login_lead_seconds）</label>
              <input type="number" id="edit_strategy_login" value="\${st.login_lead_seconds || 14}">
            </div>
            <div class="form-group">
              <label>提前滑块秒数（slider_lead_seconds）</label>
              <input type="number" id="edit_strategy_slider" value="\${st.slider_lead_seconds || 10}">
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>首枪偏移毫秒（first_submit_offset_ms）</label>
              <input type="number" id="edit_strategy_first" value="\${st.first_submit_offset_ms || 9}">
            </div>
            <div class="form-group">
              <label>取 token 延迟毫秒（token_fetch_delay_ms）</label>
              <input type="number" id="edit_strategy_delay" value="\${st.token_fetch_delay_ms || 45}">
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>轻探测开始毫秒（fast_probe_start_offset_ms）</label>
              <input type="number" id="edit_strategy_probe_start" value="\${st.fast_probe_start_offset_ms || 14}">
            </div>
            <div class="form-group">
              <label>轻探测随机范围（fast_probe_start_range_ms）</label>
              <input type="text" id="edit_strategy_probe_start_range" value="\${(st.fast_probe_start_range_ms || [st.fast_probe_start_offset_ms || 14, st.fast_probe_start_offset_ms || 14]).join(',')}" placeholder="例如: 8,20">
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>首次取 token 日期（first_token_date_mode）</label>
              <select id="edit_strategy_first_token_date_mode">
                <option value="submit_date" \${(!st.first_token_date_mode || st.first_token_date_mode==="submit_date")?"selected":""}>submit_date - 与提交日期一致</option>
                <option value="today" \${st.first_token_date_mode==="today"?"selected":""}>today - 使用当天日期</option>
              </select>
            </div>
            <div class="form-group"></div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>连接预热提前毫秒（warm_connection_lead_ms）</label>
              <input type="number" id="edit_strategy_warm_lead" value="\${st.warm_connection_lead_ms || 2400}">
            </div>
            <div class="form-group">
              <label>预取 token 提前毫秒（pre_fetch_token_ms）</label>
              <input type="number" id="edit_strategy_prefetch" value="\${st.pre_fetch_token_ms || 1531}">
            </div>
          </div>
          <div style="font-size:12px;color:#666;margin-top:6px">
            说明：学校批量触发时，会按固定批次拆成多个 workflow；当前每个 workflow 默认承载 10 个用户。
          </div>
          <button class="btn btn-primary" onclick="doEditSchool()" style="width:100%;margin-top:16px">保存配置</button>
          <button class="btn btn-danger" onclick="doDeleteSchool()" style="width:100%;margin-top:8px">删除学校</button>
        </div>
      </div>
    </div>
  \`;
}

function renderUserModal() {
  const days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];
  const dayNames = {"Monday":"周一","Tuesday":"周二","Wednesday":"周三","Thursday":"周四","Friday":"周五","Saturday":"周六","Sunday":"周日"};
  return \`
    <div class="modal" id="userModal">
      <div class="modal-content">
        <div class="modal-header">
          <h3 id="userModalTitle">添加用户</h3>
          <span class="modal-close" onclick="closeModal('userModal')">&times;</span>
        </div>
        <div class="modal-body">
          <input type="hidden" id="edit_user_id">
          <div class="form-row">
            <div class="form-group">
              <label>手机号（登录账号）</label>
              <input type="text" id="edit_user_phone" placeholder="超星登录手机号">
            </div>
            <div class="form-group">
              <label>密码（留空不修改）</label>
              <input type="password" id="edit_user_password">
            </div>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>昵称（便于识别）</label>
              <input type="text" id="edit_user_username" placeholder="如：张三">
            </div>
            <div class="form-group">
              <label>备注</label>
              <input type="text" id="edit_user_remark" placeholder="其他备注">
            </div>
          </div>
          <h4 style="margin:20px 0 12px">周计划配置</h4>
          <div class="schedule-grid">
            \${days.map(d => \`
              <div class="schedule-day">
                <div class="schedule-day-header">
                  <input type="checkbox" id="sch_\${d}_enabled">
                  <label>\${dayNames[d]}</label>
                </div>
                \${[0,1,2,3].map(i => \`
                  <div class="slot-row" id="sch_\${d}_row_\${i}" style="\${i > 0 ? 'display:none;' : ''}">
                    <div class="slot-label">时段\${i+1}</div>
                    <div class="schedule-day-fields">
                      <input type="text" id="sch_\${d}_s\${i}_roomid" placeholder="房间ID">
                      <input type="text" id="sch_\${d}_s\${i}_seatid" placeholder="座位号(逗号分隔)">
                      <input type="text" id="sch_\${d}_s\${i}_times" placeholder="09:00-22:00">
                    </div>
                    <div class="schedule-day-fields" style="margin-top:4px">
                      <input type="text" id="sch_\${d}_s\${i}_seatPageId" placeholder="seatPageId">
                      <input type="text" id="sch_\${d}_s\${i}_fidEnc" placeholder="fidEnc">
                      <span></span>
                    </div>
                  </div>
                \`).join("")}
                <button type="button" class="btn btn-sm btn-secondary" onclick="addSlotForDay('\${d}')">+ 添加时段</button>
              </div>
            \`).join("")}
          </div>
          <div class="form-group" style="margin-top:12px">
            <label>周计划 JSON 映射（单一输入框）</label>
            <textarea id="edit_user_schedule_json" rows="8" placeholder='示例: [{"times":["09:00","23:00"],"roomid":"13484","seatid":["356"],"seatPageId":"13484","fidEnc":"4a18e12602b24c8c","daysofweek":["Monday","Tuesday"]}]'></textarea>
            <button type="button" class="btn btn-secondary" onclick="applyScheduleJsonToForm()" style="margin-top:8px">映射到周计划配置</button>
          </div>
          <button id="saveUserButton" class="btn btn-primary" onclick="doSaveUser()" style="width:100%;margin-top:16px">保存用户</button>
        </div>
      </div>
    </div>
  \`;
}

function bindEvents() {
  const addTarget = document.getElementById("new_school_dispatch_target");
  const editTarget = document.getElementById("edit_school_dispatch_target");
  const toggleRelayFields = (targetId, fieldsId) => {
    const target = document.getElementById(targetId);
    const fields = document.getElementById(fieldsId);
    if (!target || !fields) return;
    fields.style.display = isServerRelayTarget(target.value) ? "" : "none";
  };
  if (addTarget && !addTarget.dataset.boundChange) {
    addTarget.addEventListener("change", () => toggleRelayFields("new_school_dispatch_target", "new_school_relay_fields"));
    addTarget.dataset.boundChange = "1";
  }
  if (editTarget && !editTarget.dataset.boundChange) {
    editTarget.addEventListener("change", () => toggleRelayFields("edit_school_dispatch_target", "edit_school_relay_fields"));
    editTarget.dataset.boundChange = "1";
  }
  toggleRelayFields("new_school_dispatch_target", "new_school_relay_fields");
  toggleRelayFields("edit_school_dispatch_target", "edit_school_relay_fields");
}

async function doLogin() {
  const key = document.getElementById("apiKey").value;
  if (!key) return toast("请输入密钥", "error");
  API_KEY = key;
  const res = await api("GET", "/api/schools");
  if (res.error) {
    toast("密钥错误", "error");
    return;
  }
  localStorage.setItem("api_key", key);
  schools = getSortedSchoolsForDisplay(res.schools || []);
  currentView = "schools";
  render();
  refreshSchoolActiveTodayCounts(true);
}

async function loadSchools() {
  const res = await api("GET", "/api/schools");
  schools = getSortedSchoolsForDisplay(res.schools || []);
  render();
  refreshSchoolActiveTodayCounts();
}

function showAddSchool() {
  document.getElementById("addSchoolModal").classList.add("show");
}

function closeModal(id) {
  document.getElementById(id).classList.remove("show");
}

async function doAddSchool() {
  const id = document.getElementById("new_school_id").value.trim();
  const name = document.getElementById("new_school_name").value.trim();
  const repo = document.getElementById("new_school_repo").value.trim();
  const dispatch_target = document.getElementById("new_school_dispatch_target").value.trim().toLowerCase();
  const conflict_group = document.getElementById("new_school_conflict_group").value.trim();
  const github_token_key = document.getElementById("new_school_github_token_key").value.trim().toLowerCase();
  const trigger_time = document.getElementById("new_school_trigger").value.trim();
  const endtime = document.getElementById("new_school_endtime").value.trim();
  const fidEnc = document.getElementById("new_school_fidEnc").value.trim();
  if (!id || !name) return toast("请填写必要信息", "error");
  const formalTimeError = validateFormalTimeWindowInput(trigger_time, endtime);
  if (formalTimeError) return toast(formalTimeError, "error");
  const body = {
    id,
    name,
    repo,
    dispatch_target,
    conflict_group,
    github_token_key,
    trigger_time,
    endtime: normalizeClientEndtimeInput(endtime),
    fidEnc,
  };
  body.seat_api_mode = document.getElementById("new_school_seat_api_mode").value.trim().toLowerCase();
  body.reserve_next_day = document.getElementById("new_school_reserve_next_day").checked;
  body.enable_slider = document.getElementById("new_school_enable_slider").checked;
  body.enable_textclick = document.getElementById("new_school_enable_textclick").checked;
  if (isServerRelayTarget(dispatch_target)) {
    body.server_url = document.getElementById("new_school_server_url").value.trim();
    body.server_api_key = document.getElementById("new_school_server_api_key").value.trim();
    body.server_max_concurrency = parseInt(document.getElementById("new_school_server_max_concurrency").value, 10) || 13;
    const reserveDayOffsetRaw = document.getElementById("new_school_reserve_day_offset").value;
    const reserveDayOffset = normalizeReserveDayOffsetInput(reserveDayOffsetRaw);
    if (String(reserveDayOffsetRaw || "").trim() && reserveDayOffset === null) {
      return toast("提交 day 日期偏移只能填 0、1、2 这类非负整数", "error");
    }
    if (reserveDayOffset !== null) body.reserve_day_offset = reserveDayOffset;
  } else {
    body.reserve_day_offset = null;
  }
  const res = await api("POST", "/api/school", body);
  if (res.ok) {
    let msg = "学校添加成功";
    if (res.repoInit) {
      if (res.repoInit.skipped) {
        msg += "（仓库已是源仓库，跳过初始化）";
      } else if (res.repoInit.ok) {
        msg += "，已创建仓库并复制 " + res.repoInit.files + " 个文件";
      } else {
        msg += "，但仓库初始化失败: " + res.repoInit.error;
      }
    }
    toast(msg);
    closeModal("addSchoolModal");
    if (res.school) {
      schools = upsertSchoolInOrderedList(schools, res.school, { forceResort: true });
      render();
      refreshSchoolActiveTodayCounts(true);
    } else {
      loadSchools();
    }
  } else {
    toast(res.error || "添加失败", "error");
  }
}

async function openSchool(id) {
  const res = await api("GET", "/api/school/" + id);
  if (res.error) return toast(res.error, "error");
  currentSchool = res.school;
  const usersRes = await api("GET", "/api/school/" + id + "/users");
  users = usersRes.users || [];
  setCachedActiveTodayCount(id, {
    count: countActiveUsersForTodayClient(users),
    expiresAt: Date.now() + ACTIVE_TODAY_CACHE_TTL_MS,
    error: "",
  });
  currentView = "school";
  render();
}

function backToSchools() {
  currentSchool = null;
  users = [];
  currentView = "schools";
  loadSchools();
}

function showEditSchool() {
  document.getElementById("editSchoolModal").classList.add("show");
}

async function doEditSchool() {
  const s = currentSchool;
  const githubTokenKey = document.getElementById("edit_school_github_token_key").value.trim().toLowerCase();
  const dispatchTarget = document.getElementById("edit_school_dispatch_target").value.trim().toLowerCase();
  const {
    burst_offsets_ms: _burstOffsetsMs,
    burst_jitter_range_ms: _burstJitterRangeMs,
    ...baseStrategy
  } = s.strategy || {};
  const parseRangeInput = (id, fallbackA, fallbackB) => {
    const text = (document.getElementById(id).value || "").trim();
    const arr = text.split(",").map(v => parseInt(v.trim(), 10)).filter(v => !Number.isNaN(v));
    if (arr.length >= 2) return [arr[0], arr[1]];
    return [fallbackA, fallbackB];
  };
  const probeStartRange = parseRangeInput("edit_strategy_probe_start_range", 14, 14);
  const readingZonesRaw = (document.getElementById("edit_school_reading_zones").value || "").trim();
  let readingZoneGroups = [];
  if (readingZonesRaw) {
    try {
      const parsed = JSON.parse(readingZonesRaw);
      if (!Array.isArray(parsed)) return toast("阅览区映射 JSON 必须是数组", "error");
      const normalized = normalizeReadingZoneGroups(parsed);
      if (!normalized.length) {
        return toast("阅览区映射 JSON 结构无效：请使用 floor/zones 或 roomid 列表", "error");
      }
      readingZoneGroups = normalized;
    } catch (e) {
      return toast("阅览区映射 JSON 解析失败: " + (e.message || String(e)), "error");
    }
  }
  const triggerTime = document.getElementById("edit_school_trigger").value.trim();
  const endtime = document.getElementById("edit_school_endtime").value.trim();
  const formalTimeError = validateFormalTimeWindowInput(triggerTime, endtime);
  if (formalTimeError) return toast(formalTimeError, "error");
  const body = {
    name: document.getElementById("edit_school_name").value.trim(),
    repo: document.getElementById("edit_school_repo").value.trim(),
    dispatch_target: dispatchTarget,
    conflict_group: document.getElementById("edit_school_conflict_group").value.trim(),
    github_token_key: githubTokenKey,
    trigger_time: triggerTime,
    endtime: normalizeClientEndtimeInput(endtime),
    fidEnc: document.getElementById("edit_school_fidEnc").value.trim(),
    reading_zone_groups: readingZoneGroups,
    strategy: {
      ...baseStrategy,
      mode: document.getElementById("edit_strategy_mode").value,
      submit_mode: document.getElementById("edit_strategy_submit").value,
      login_lead_seconds: parseInt(document.getElementById("edit_strategy_login").value) || 14,
      slider_lead_seconds: parseInt(document.getElementById("edit_strategy_slider").value) || 10,
      warm_connection_lead_ms: parseInt(document.getElementById("edit_strategy_warm_lead").value) || 2400,
      fast_probe_start_offset_ms: parseInt(document.getElementById("edit_strategy_probe_start").value) || 14,
      pre_fetch_token_ms: parseInt(document.getElementById("edit_strategy_prefetch").value) || 1531,
      first_submit_offset_ms: parseInt(document.getElementById("edit_strategy_first").value) || 9,
      token_fetch_delay_ms: parseInt(document.getElementById("edit_strategy_delay").value) || 45,
      first_token_date_mode: document.getElementById("edit_strategy_first_token_date_mode").value,
      fast_probe_start_range_ms: probeStartRange,
    }
  };
  body.seat_api_mode = document.getElementById("edit_school_seat_api_mode").value.trim().toLowerCase();
  body.reserve_next_day = document.getElementById("edit_school_reserve_next_day").checked;
  body.enable_slider = document.getElementById("edit_school_enable_slider").checked;
  body.enable_textclick = document.getElementById("edit_school_enable_textclick").checked;
  if (isServerRelayTarget(dispatchTarget)) {
    body.server_url = document.getElementById("edit_school_server_url").value.trim();
    body.server_max_concurrency = parseInt(document.getElementById("edit_school_server_max_concurrency").value, 10) || 13;
    const reserveDayOffsetRaw = document.getElementById("edit_school_reserve_day_offset").value;
    const reserveDayOffset = normalizeReserveDayOffsetInput(reserveDayOffsetRaw);
    if (String(reserveDayOffsetRaw || "").trim() && reserveDayOffset === null) {
      return toast("提交 day 日期偏移只能填 0、1、2 这类非负整数", "error");
    }
    body.reserve_day_offset = reserveDayOffset;
    const serverApiKeyInput = document.getElementById("edit_school_server_api_key").value.trim();
    if (serverApiKeyInput && serverApiKeyInput !== "******") {
      body.server_api_key = serverApiKeyInput;
    }
  } else {
    body.reserve_day_offset = null;
  }
  const res = await api("PUT", "/api/school/" + s.id, body);
  if (res.ok) {
    toast("配置已保存");
    currentSchool = res.school;
    schools = upsertSchoolInOrderedList(schools, res.school);
    closeModal("editSchoolModal");
    render();
  } else {
    toast(res.error || "保存失败", "error");
  }
}

async function doDeleteSchool() {
  if (!confirm("确定删除此学校及其所有用户？")) return;
  const res = await api("DELETE", "/api/school/" + currentSchool.id);
  if (res.ok) {
    toast("学校已删除");
    backToSchools();
  } else {
    toast(res.error || "删除失败", "error");
  }
}

async function triggerSchool() {
  if (!confirm("确定手动触发该学校所有活跃用户？")) return;
  const res = await api("POST", "/api/trigger/" + currentSchool.id);
  if (res.ok) {
    toast("已触发 " + (res.triggeredUsers || 0) + " 名用户，批次 " + (res.okBatches || 0) + "/" + (res.totalBatches || 0));
  } else {
    toast(res.error || "触发失败", "error");
  }
}

async function startTestEndtimeOverride() {
  if (!currentSchool) return;
  const input = document.getElementById("school_test_endtime");
  const testEndtime = normalizeClientEndtimeInput(input && input.value);
  if (!testEndtime) {
    return toast("测试截止时间格式应为 HH:MM:SS", "error");
  }

  const res = await api("POST", "/api/school/" + currentSchool.id + "/test-endtime", {
    test_endtime: testEndtime,
  });
  if (res.ok && res.school) {
    currentSchool = res.school;
    schools = upsertSchoolInOrderedList(schools, res.school);
    render();
    toast("测试覆盖已开启，3 分钟后自动关闭");
  } else {
    toast(res.error || "测试覆盖启动失败", "error");
  }
}

async function stopTestEndtimeOverride() {
  if (!currentSchool) return;
  const res = await api("POST", "/api/school/" + currentSchool.id + "/test-endtime", {
    action: "stop",
  });
  if (res.ok && res.school) {
    currentSchool = res.school;
    schools = upsertSchoolInOrderedList(schools, res.school);
    render();
    toast("测试覆盖已关闭");
  } else {
    toast(res.error || "测试覆盖关闭失败", "error");
  }
}

function showAddUser(prefill = null) {
  setUserSavePending(false);
  document.getElementById("userModalTitle").textContent = "添加用户";
  document.getElementById("edit_user_id").value = "";
  document.getElementById("edit_user_phone").value = "";
  document.getElementById("edit_user_username").value = "";
  document.getElementById("edit_user_password").value = "";
  document.getElementById("edit_user_remark").value = "";
  const days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];
  days.forEach(d => {
    document.getElementById("sch_" + d + "_enabled").checked = false;
    setVisibleSlotsForDay(d, 1);
    [0,1,2,3].forEach(i => {
      document.getElementById("sch_" + d + "_s" + i + "_roomid").value = "";
      document.getElementById("sch_" + d + "_s" + i + "_seatid").value = "";
      document.getElementById("sch_" + d + "_s" + i + "_times").value = "";
      document.getElementById("sch_" + d + "_s" + i + "_seatPageId").value = "";
      document.getElementById("sch_" + d + "_s" + i + "_fidEnc").value = "";
    });
  });
  document.getElementById("edit_user_schedule_json").value = "";
  if (prefill && typeof prefill === "object") {
    document.getElementById("edit_user_phone").value = prefill.phone || "";
    document.getElementById("edit_user_username").value = prefill.username || "";
    document.getElementById("edit_user_password").value = prefill.password || "";
    document.getElementById("edit_user_remark").value = prefill.remark || "";
    if (prefill.schedule) {
      fillScheduleFormFromSchedule(prefill.schedule);
    }
    if (prefill.scheduleJsonText) {
      document.getElementById("edit_user_schedule_json").value = prefill.scheduleJsonText;
      if (!prefill.schedule) {
        fillScheduleFormFromSchedule(parseScheduleJsonMapping(prefill.scheduleJsonText));
      }
    }
  }
  document.getElementById("userModal").classList.add("show");
}

async function showEditUser(userId) {
  setUserSavePending(false);
  const res = await api("GET", "/api/school/" + currentSchool.id + "/user/" + userId);
  if (res.error) return toast(res.error, "error");
  const u = res.user;
  document.getElementById("userModalTitle").textContent = "编辑用户";
  document.getElementById("edit_user_id").value = u.id;
  document.getElementById("edit_user_phone").value = u.phone || "";
  document.getElementById("edit_user_username").value = u.username || "";
  document.getElementById("edit_user_password").value = "";
  document.getElementById("edit_user_remark").value = u.remark || "";
  fillScheduleFormFromSchedule(u.schedule || {});
  document.getElementById("edit_user_schedule_json").value = JSON.stringify(scheduleToJsonMapping(u.schedule || {}), null, 2);
  document.getElementById("userModal").classList.add("show");
}

async function doSaveUser() {
  if (isSavingUser) return;
  const userId = document.getElementById("edit_user_id").value;
  const phone = document.getElementById("edit_user_phone").value.trim();
  const username = document.getElementById("edit_user_username").value.trim();
  const password = document.getElementById("edit_user_password").value;
  const remark = document.getElementById("edit_user_remark").value.trim();
  if (!phone) return toast("请填写手机号（登录账号）", "error");
  const days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];

  const schedule = {};
  days.forEach(d => {
    const visibleCount = getVisibleSlotsForDay(d);
    const slotIndexes = Array.from({ length: visibleCount }, (_, i) => i);
    const slots = slotIndexes.map(i => ({
      roomid: document.getElementById("sch_" + d + "_s" + i + "_roomid").value.trim(),
      seatid: document.getElementById("sch_" + d + "_s" + i + "_seatid").value.trim(),
      times: document.getElementById("sch_" + d + "_s" + i + "_times").value.trim(),
      seatPageId: document.getElementById("sch_" + d + "_s" + i + "_seatPageId").value.trim(),
      fidEnc: document.getElementById("sch_" + d + "_s" + i + "_fidEnc").value.trim(),
    }));
    schedule[d] = {
      enabled: document.getElementById("sch_" + d + "_enabled").checked,
      slots,
    };
  });

  const body = { phone, username, remark, schedule };
  if (password) body.password = password;
  setUserSavePending(true);
  try {
    let res;
    if (userId) {
      res = await api("PUT", "/api/school/" + currentSchool.id + "/user/" + userId, body);
    } else {
      res = await api("POST", "/api/school/" + currentSchool.id + "/user", body);
    }
    if (res.ok) {
      toast("用户已保存");
      closeModal("userModal");
      openSchool(currentSchool.id);
    } else {
      toast(res.error || "保存失败", "error");
    }
  } finally {
    setUserSavePending(false);
  }
}

async function pauseUser(userId) {
  await api("POST", "/api/school/" + currentSchool.id + "/user/" + userId + "/pause");
  toast("用户已暂停");
  openSchool(currentSchool.id);
}

async function resumeUser(userId) {
  await api("POST", "/api/school/" + currentSchool.id + "/user/" + userId + "/resume");
  toast("用户已恢复");
  openSchool(currentSchool.id);
}

async function triggerUser(userId) {
  try {
    const res = await api("POST", "/api/trigger/" + currentSchool.id + "/" + userId);
    if (res.ok) {
      toast("已触发");
      return;
    }
    const detailText = typeof res.detail === "string" ? res.detail.slice(0, 120) : "";
    const msg = [
      res.error || "触发失败",
      res.status ? ("status=" + res.status) : "",
      detailText,
    ].filter(Boolean).join(" | ");
    toast(msg, "error");
  } catch (e) {
    toast("触发异常: " + (e.message || String(e)), "error");
  }
}

async function deleteUser(userId) {
  if (!confirm("确定删除此用户？")) return;
  await api("DELETE", "/api/school/" + currentSchool.id + "/user/" + userId);
  toast("用户已删除");
  openSchool(currentSchool.id);
}

// 初始化
setInterval(updateTestEndtimeStatusView, 1000);

(async function init() {
  try {
    if (API_KEY) {
      const res = await api("GET", "/api/schools");
      if (!res.error) {
        schools = Array.isArray(res.schools) ? res.schools : [];
        currentView = "schools";
      }
    }
    render();
    refreshSchoolActiveTodayCounts();
  } catch (e) {
    console.error("init failed:", e);
    renderFatalError(e, "init");
  }
})();

window.addEventListener("error", (event) => {
  renderFatalError(event.error || event.message || "Unknown error", "window.error");
});

window.addEventListener("unhandledrejection", (event) => {
  renderFatalError(event.reason || "Unhandled promise rejection", "unhandledrejection");
});
</script>
</body>
</html>`;

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(writeHeartbeatTimestamp(env.SEAT_KV));
    ctx.waitUntil(handleScheduled(env));
  },
  async fetch(request, env, ctx) {
    return handleFetch(request, env);
  },
};
