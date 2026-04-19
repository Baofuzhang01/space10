// worker2.js
// 独立兜底 Worker：通过 Cloudflare KV REST API 跨账号读取 tongyi 的心跳 KV，并用小时锁避免重复兜底

function beijingNow(baseTimestampMs = Date.now()) {
  return new Date(baseTimestampMs + 8 * 3600 * 1000);
}

function beijingHHMM(baseTimestampMs = Date.now()) {
  const d = beijingNow(baseTimestampMs);
  return `${String(d.getUTCHours()).padStart(2, "0")}:${String(d.getUTCMinutes()).padStart(2, "0")}`;
}

function beijingHMS(baseTimestampMs = Date.now()) {
  const d = beijingNow(baseTimestampMs);
  return [
    String(d.getUTCHours()).padStart(2, "0"),
    String(d.getUTCMinutes()).padStart(2, "0"),
    String(d.getUTCSeconds()).padStart(2, "0"),
  ].join(":");
}

function beijingDate(baseTimestampMs = Date.now()) {
  const d = beijingNow(baseTimestampMs);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function beijingDateMinute(offsetMinutes = 0, baseTimestampMs = Date.now()) {
  const d = new Date(baseTimestampMs + (8 * 3600 + offsetMinutes * 60) * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  const minute = String(d.getUTCMinutes()).padStart(2, "0");
  return `${y}-${m}-${day}-${hour}:${minute}`;
}

function jsonResp(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}

const HEARTBEAT_LAST_TS_KEY = "meta:heartbeat:last_ts";
const HEARTBEAT_LAST_MINUTE_KEY = "meta:heartbeat:last_minute";
// tongyi 侧是分钟级心跳，且依赖 Cloudflare cron + 跨账号 KV 读取。
// 单次 cron 漂移或漏跑并不等于 worker 异常，因此这里至少容忍 2 个心跳周期，避免误触发兜底。
const HEARTBEAT_INTERVAL_MS = 60 * 1000;
const HEARTBEAT_TIMEOUT_MS = 2 * HEARTBEAT_INTERVAL_MS + 10 * 1000;
const HEARTBEAT_CONFIRM_DELAY_MS = 8000;
const FALLBACK_HOUR_LOCK_PREFIX = "meta:fallback_hour_lock";
const FALLBACK_HOUR_LOCK_TTL_SECONDS = 48 * 60 * 60;
const WORKER2_SETTINGS_KEY = "meta:worker2:settings";
const WORKER2_RECORD_PREFIX = "meta:worker2:records";
const WORKER2_RECORD_LIMIT = 300;
const WORKER2_RECORD_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const WORKER2_RECORD_TTL_SECONDS = 48 * 60 * 60;
const WORKER2_DEPLOYED_UTC_CRONS = ["* * * * *"];
const DEFAULT_WORKER2_BEIJING_CRONS = ["54-57 5-21 * * *", "24-27 21 * * *"];

function beijingDateHour(baseTimestampMs = Date.now()) {
  const d = beijingNow(baseTimestampMs);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  const hour = String(d.getUTCHours()).padStart(2, "0");
  return `${y}-${m}-${day}-${hour}`;
}

async function sleep(ms) {
  if (ms <= 0) return;
  await new Promise(resolve => setTimeout(resolve, ms));
}

function getRemoteKvConfig(env) {
  const accountId = String(env.HEARTBEAT_SOURCE_ACCOUNT_ID || "").trim();
  const namespaceId = String(env.HEARTBEAT_SOURCE_NAMESPACE_ID || "").trim();
  const apiToken = String(env.HEARTBEAT_SOURCE_API_TOKEN || "").trim();
  if (!accountId || !namespaceId || !apiToken) {
    throw new Error("heartbeat source KV config missing: HEARTBEAT_SOURCE_ACCOUNT_ID / HEARTBEAT_SOURCE_NAMESPACE_ID / HEARTBEAT_SOURCE_API_TOKEN");
  }
  return { accountId, namespaceId, apiToken };
}

function buildRemoteKvValueUrl(env, key, extraParams = {}) {
  const { accountId, namespaceId } = getRemoteKvConfig(env);
  const url = new URL(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/storage/kv/namespaces/${namespaceId}/values/${encodeURIComponent(key)}`
  );
  for (const [name, value] of Object.entries(extraParams)) {
    if (value === undefined || value === null || value === "") continue;
    url.searchParams.set(name, String(value));
  }
  return url.toString();
}

function getRemoteKvHeaders(env) {
  const { apiToken } = getRemoteKvConfig(env);
  return {
    Authorization: `Bearer ${apiToken}`,
  };
}

async function getRemoteKvText(env, key) {
  const response = await fetch(buildRemoteKvValueUrl(env, key), {
    headers: getRemoteKvHeaders(env),
  });

  if (response.status === 404) return null;
  if (!response.ok) {
    throw new Error(`KV GET failed for ${key}: HTTP ${response.status} ${await response.text()}`);
  }
  return await response.text();
}

async function putRemoteKvText(env, key, value, options = {}) {
  const response = await fetch(
    buildRemoteKvValueUrl(env, key, {
      expiration_ttl: options.expirationTtl,
    }),
    {
      method: "PUT",
      headers: getRemoteKvHeaders(env),
      body: String(value),
    }
  );

  if (!response.ok) {
    throw new Error(`KV PUT failed for ${key}: HTTP ${response.status} ${await response.text()}`);
  }
}

async function getRemoteKvJson(env, key, fallback = null) {
  const raw = await getRemoteKvText(env, key);
  return raw ? JSON.parse(raw) : fallback;
}

async function putRemoteKvJson(env, key, value, options = {}) {
  await putRemoteKvText(env, key, JSON.stringify(value), options);
}

function normalizeSettings(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  return {
    enabled: source.enabled !== false,
    beijingCrons: normalizeBeijingCronLines(source.beijingCrons || source.crons, DEFAULT_WORKER2_BEIJING_CRONS),
    note: String(source.note || "").slice(0, 200),
    updatedAt: String(source.updatedAt || ""),
  };
}

function parseCronField(field, min, max) {
  const text = String(field || "").trim();
  if (!text) throw new Error("cron field is empty");
  if (text === "*") {
    return Array.from({ length: max - min + 1 }, (_, index) => min + index);
  }
  const values = new Set();
  for (const part of text.split(",")) {
    const item = part.trim();
    if (!item) throw new Error(`invalid cron field: ${field}`);
    const range = item.match(/^(\d{1,2})-(\d{1,2})$/);
    if (range) {
      const start = parseInt(range[1], 10);
      const end = parseInt(range[2], 10);
      if (start > end || start < min || end > max) throw new Error(`invalid cron range: ${item}`);
      for (let value = start; value <= end; value++) values.add(value);
      continue;
    }
    if (!/^\d{1,2}$/.test(item)) throw new Error(`invalid cron value: ${item}`);
    const value = parseInt(item, 10);
    if (value < min || value > max) throw new Error(`cron value out of range: ${item}`);
    values.add(value);
  }
  return [...values].sort((a, b) => a - b);
}

function validateBeijingCronLines(value) {
  const lines = (Array.isArray(value) ? value : String(value || "").split(/\r?\n/))
    .map(line => String(line || "").trim())
    .filter(Boolean);
  if (lines.length === 0) throw new Error("至少保留一行检测时间");
  if (lines.length > 20) throw new Error("检测时间最多 20 行");

  return lines.map(line => {
    const parts = line.split(/\s+/);
    if (parts.length !== 5) throw new Error(`检测时间格式错误: ${line}`);
    if (parts[2] !== "*" || parts[3] !== "*" || parts[4] !== "*") {
      throw new Error(`目前只支持“分钟 小时 * * *”: ${line}`);
    }
    parseCronField(parts[0], 0, 59);
    parseCronField(parts[1], 0, 23);
    return `${parts[0]} ${parts[1]} * * *`;
  });
}

function normalizeBeijingCronLines(value, fallback) {
  try {
    return validateBeijingCronLines(value);
  } catch (_) {
    return fallback.slice();
  }
}

function matchesBeijingCronLine(line, baseTimestampMs = Date.now()) {
  const parts = String(line || "").trim().split(/\s+/);
  if (parts.length !== 5) return false;
  const d = beijingNow(baseTimestampMs);
  const minute = d.getUTCMinutes();
  const hour = d.getUTCHours();
  return (
    parseCronField(parts[0], 0, 59).includes(minute) &&
    parseCronField(parts[1], 0, 23).includes(hour)
  );
}

function matchesWorker2Schedule(settings, baseTimestampMs = Date.now()) {
  const crons = normalizeBeijingCronLines(settings?.beijingCrons, DEFAULT_WORKER2_BEIJING_CRONS);
  return crons.some(line => matchesBeijingCronLine(line, baseTimestampMs));
}

async function getWorker2Settings(env) {
  return normalizeSettings(await getRemoteKvJson(env, WORKER2_SETTINGS_KEY, {}));
}

async function saveWorker2Settings(env, patch) {
  const current = await getWorker2Settings(env);
  const source = patch && typeof patch === "object" ? patch : {};
  const nextCrons = source.beijingCrons !== undefined || source.crons !== undefined
    ? validateBeijingCronLines(source.beijingCrons ?? source.crons)
    : current.beijingCrons;
  const next = normalizeSettings({
    ...current,
    ...source,
    beijingCrons: nextCrons,
    updatedAt: new Date().toISOString(),
  });
  await putRemoteKvJson(env, WORKER2_SETTINGS_KEY, next);
  return next;
}

function worker2RecordKey(dateText = beijingDate()) {
  return `${WORKER2_RECORD_PREFIX}:${dateText}`;
}

function filterRecentRecords(records, nowMs = Date.now()) {
  const cutoffMs = nowMs - WORKER2_RECORD_MAX_AGE_MS;
  return (Array.isArray(records) ? records : []).filter(record => {
    const recordedMs = Date.parse(record?.recordedAt || "");
    return Number.isFinite(recordedMs) && recordedMs >= cutoffMs;
  });
}

function summarizeFallbackResults(results) {
  const items = Array.isArray(results) ? results : [];
  return {
    ok: items.filter(item => item?.ok && !item?.skipped).length,
    skipped: items.filter(item => item?.ok && item?.skipped).length,
    fail: items.filter(item => !item?.ok).length,
  };
}

function summarizeWatchdogResult(result) {
  const fallback = result?.fallback || {};
  const fallbackSummary = summarizeFallbackResults(fallback?.results);
  const hasFallback = (fallback?.dueCount || 0) > 0 || fallbackSummary.ok > 0 || fallbackSummary.skipped > 0 || fallbackSummary.fail > 0;
  return {
    ok: !!result?.ok,
    mode: result?.mode || "",
    manual: !!result?.manual,
    skipped: !!result?.skipped,
    reason: result?.reason || "",
    checkStatus: result?.ok ? "success" : (result?.skipped ? "skipped" : "attention"),
    fallbackStatus: hasFallback
      ? (fallbackSummary.fail > 0 ? "partial_or_failed" : "executed")
      : "none",
    beijing_time: result?.beijing_time || beijingHMS(),
    heartbeatTs: result?.heartbeatTs ?? null,
    heartbeatMinuteSlot: result?.heartbeatMinuteSlot || "",
    diffSeconds: result?.diffSeconds ?? null,
    thresholdSeconds: result?.thresholdMs ? Math.floor(result.thresholdMs / 1000) : Math.floor(HEARTBEAT_TIMEOUT_MS / 1000),
    heartbeatRecheck: result?.heartbeatRecheck || null,
    fallbackHourKey: result?.fallbackHourKey || "",
    dueCount: fallback?.dueCount ?? 0,
    fallbackSummary,
    fallbackResults: Array.isArray(fallback?.results) ? fallback.results : [],
    fallbackError: result?.fallbackError || "",
  };
}

async function appendWatchdogRecord(env, result, options = {}) {
  const dateText = beijingDate();
  const key = worker2RecordKey(dateText);
  const records = filterRecentRecords(await getRemoteKvJson(env, key, []));
  const nextRecords = records.slice(-WORKER2_RECORD_LIMIT + 1);
  const record = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    kind: options.manual ? "manual" : "scheduled",
    recordedAt: new Date().toISOString(),
    beijing_date: dateText,
    beijing_time: beijingHMS(),
    result: summarizeWatchdogResult(result),
  };
  nextRecords.push(record);
  await putRemoteKvJson(env, key, nextRecords, { expirationTtl: WORKER2_RECORD_TTL_SECONDS });
  return { ok: true, key, count: nextRecords.length };
}

async function readWatchdogRecords(env, dateText = beijingDate()) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(dateText || ""))) {
    throw new Error("date must be YYYY-MM-DD");
  }
  const records = await getRemoteKvJson(env, worker2RecordKey(dateText), []);
  return filterRecentRecords(records);
}

async function getHeartbeatTimestamp(env) {
  const raw = await getRemoteKvText(env, HEARTBEAT_LAST_TS_KEY);
  const ts = parseInt(String(raw || "").trim(), 10);
  if (Number.isNaN(ts) || ts <= 0) return null;
  return ts;
}

async function getHeartbeatMinuteSlot(env) {
  const raw = await getRemoteKvText(env, HEARTBEAT_LAST_MINUTE_KEY);
  const slot = String(raw || "").trim();
  return slot || null;
}

async function getHeartbeatState(env) {
  const [heartbeatTs, heartbeatMinuteSlot] = await Promise.all([
    getHeartbeatTimestamp(env),
    getHeartbeatMinuteSlot(env),
  ]);
  return { heartbeatTs, heartbeatMinuteSlot };
}

function isHeartbeatSlotHealthy(heartbeatMinuteSlot, baseTimestampMs = Date.now()) {
  if (!heartbeatMinuteSlot) return false;
  return (
    heartbeatMinuteSlot === beijingDateMinute(0, baseTimestampMs) ||
    heartbeatMinuteSlot === beijingDateMinute(-1, baseTimestampMs)
  );
}

function isHeartbeatFreshByTimestamp(heartbeatTs, diffMs) {
  return heartbeatTs !== null && diffMs !== null && diffMs <= HEARTBEAT_TIMEOUT_MS;
}

function getExpectedHeartbeatSlots(baseTimestampMs = Date.now()) {
  return {
    current: beijingDateMinute(0, baseTimestampMs),
    previous: beijingDateMinute(-1, baseTimestampMs),
  };
}

function normalizeScheduledTimeMs(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (value instanceof Date) {
    const ms = value.getTime();
    return Number.isFinite(ms) ? ms : null;
  }
  if (typeof value === "string" && value.trim()) {
    const ms = Date.parse(value);
    return Number.isFinite(ms) ? ms : null;
  }
  return null;
}

function buildFallbackHourLockKey(hourKey) {
  return `${FALLBACK_HOUR_LOCK_PREFIX}:${hourKey}`;
}

async function getFallbackHourLock(env, hourKey) {
  const raw = await getRemoteKvText(env, buildFallbackHourLockKey(hourKey));
  return raw ? JSON.parse(raw) : null;
}

async function saveFallbackHourLock(env, hourKey, record) {
  await putRemoteKvText(
    env,
    buildFallbackHourLockKey(hourKey),
    JSON.stringify(record),
    {
      expirationTtl: FALLBACK_HOUR_LOCK_TTL_SECONDS,
    },
  );
  return record;
}

function parseTimeToSeconds(text) {
  const match = String(text || "").trim().match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;

  const hour = parseInt(match[1], 10);
  const minute = parseInt(match[2], 10);
  const second = parseInt(match[3] || "0", 10);
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
    return null;
  }

  return hour * 3600 + minute * 60 + second;
}

function shouldTriggerSchoolNow(school) {
  const nowHHMM = beijingHHMM();
  const nowHMS = beijingHMS();
  const triggerTime = String(school?.trigger_time || "").trim();
  const endtime = String(school?.endtime || "").trim();

  if (!triggerTime) return false;
  if (nowHHMM < triggerTime) return false;

  if (!endtime) return true;

  const nowSeconds = parseTimeToSeconds(nowHMS);
  const endSeconds = parseTimeToSeconds(endtime);
  if (nowSeconds === null || endSeconds === null) return true;

  return nowSeconds <= endSeconds;
}

async function fetchJson(url, init = {}) {
  const res = await fetch(url, init);
  const text = await res.text();
  let data = {};

  try {
    data = text ? JSON.parse(text) : {};
  } catch (e) {
    data = { raw: text };
  }

  if (!res.ok) {
    const detail = typeof data?.error === "string" ? data.error : text || `HTTP ${res.status}`;
    throw new Error(`${url} -> HTTP ${res.status}: ${detail}`);
  }

  return data;
}

async function getSchools(env) {
  const data = await fetchJson(`${env.TRIGGER_API}/schools`, {
    headers: { "X-API-Key": env.API_KEY },
  });
  return data.schools || [];
}

async function getDueSchools(env) {
  const schools = await getSchools(env);
  return schools.filter(shouldTriggerSchoolNow);
}

async function triggerSchool(env, schoolId, options = {}) {
  const headers = { "X-API-Key": env.API_KEY };
  if (options.triggerSource) headers["X-Trigger-Source"] = options.triggerSource;
  if (options.fallbackMode) headers["X-Fallback-Mode"] = options.fallbackMode;

  return fetchJson(`${env.TRIGGER_API}/trigger/${schoolId}`, {
    method: "POST",
    headers,
  });
}

async function sendFeishuText(env, msg) {
  const webhook = String(env.FEISHU_WEBHOOK || "").trim();
  if (!webhook) {
    return {
      ok: false,
      skipped: true,
      reason: "webhook_missing",
    };
  }
  const keyword = String(env.FEISHU_KEYWORD || "检测").trim() || "检测";
  const text = String(msg || "").includes(keyword)
    ? String(msg || "")
    : `${keyword}\n${String(msg || "")}`;

  try {
    const response = await fetch(webhook, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ msg_type: "text", content: { text } }),
    });
    const detail = (await response.text()).trim();
    const result = {
      ok: response.ok,
      status: response.status,
      detail: detail.slice(0, 300),
    };
    console.log("Feishu send result:", JSON.stringify(result));
    return result;
  } catch (e) {
    const result = {
      ok: false,
      error: e.message || String(e),
    };
    console.log("Feishu send error:", JSON.stringify(result));
    return result;
  }
}

async function sendFeishuAlerts(env, messages) {
  const normalized = (messages || [])
    .map(msg => String(msg || "").trim())
    .filter(Boolean);

  if (normalized.length === 0) return [];

  const results = [];
  for (let i = 0; i < normalized.length; i++) {
    const prefix = normalized.length > 1 ? `[${i + 1}/${normalized.length}]\n` : "";
    results.push(await sendFeishuText(env, prefix + normalized[i]));
  }
  return results;
}

function summarizeTriggered(results) {
  const ok = results.filter(r => r.ok && !r.skipped).length;
  const skipped = results.filter(r => r.ok && r.skipped).length;
  return {
    ok,
    skipped,
    fail: results.filter(r => !r.ok).length,
  };
}

function chunkLines(lines, maxChars = 900) {
  const chunks = [];
  let current = [];
  let currentLen = 0;

  for (const rawLine of lines) {
    const line = String(rawLine || "").trim();
    if (!line) continue;

    const nextLen = currentLen + (current.length ? 1 : 0) + line.length;
    if (current.length && nextLen > maxChars) {
      chunks.push(current.join("\n"));
      current = [line];
      currentLen = line.length;
    } else {
      current.push(line);
      currentLen = nextLen;
    }
  }

  if (current.length) chunks.push(current.join("\n"));
  return chunks;
}

function formatFallbackMessages(title, lines, fallback) {
  const messages = [];
  const results = fallback?.results || [];
  const summary = summarizeTriggered(results);
  const successLines = results
    .filter(item => item.ok && !item.skipped)
    .map(item => `成功 ${item.name}(${item.id}) users=${item.triggeredUsers} batches=${item.okBatches}/${item.totalBatches}`);
  const skippedLines = results
    .filter(item => item.ok && item.skipped)
    .map(item => `跳过 ${item.name}(${item.id}) ${item.reason || "fallback_already_triggered_today"}`);
  const failLines = results
    .filter(item => !item.ok)
    .map(item => `失败 ${item.name}(${item.id}) ${item.error}`);

  messages.push(
    [
      title,
      ...lines,
      `兜底候选学校: ${fallback?.dueCount || 0}`,
      `成功学校: ${summary.ok}`,
      `跳过学校: ${summary.skipped}`,
      `失败学校: ${summary.fail}`,
    ].filter(Boolean).join("\n")
  );

  if (successLines.length) {
    for (const chunk of chunkLines(successLines)) {
      messages.push(["兜底触发成功明细", chunk].join("\n"));
    }
  }

  if (failLines.length) {
    for (const chunk of chunkLines(failLines)) {
      messages.push(["兜底触发失败明细", chunk].join("\n"));
    }
  }

  if (skippedLines.length) {
    for (const chunk of chunkLines(skippedLines)) {
      messages.push(["兜底触发跳过明细", chunk].join("\n"));
    }
  }

  return messages;
}

async function triggerDueSchools(env, options = {}, dueSchools = null) {
  const schoolsToTrigger = Array.isArray(dueSchools) ? dueSchools : await getDueSchools(env);
  const results = [];

  for (const school of schoolsToTrigger) {
    try {
      const result = await triggerSchool(env, school.id, options);
      results.push({
        ok: true,
        id: school.id,
        name: school.name,
        triggeredUsers: result.triggeredUsers || 0,
        okBatches: result.okBatches || 0,
        totalBatches: result.totalBatches || 0,
        skipped: !!result.skipped,
        reason: result.reason || "",
      });
    } catch (e) {
      results.push({
        ok: false,
        id: school.id,
        name: school.name,
        error: e.message || String(e),
      });
    }
  }

  return {
    checkedAt: new Date().toISOString(),
    dueCount: schoolsToTrigger.length,
    results,
  };
}

async function runWatchdog(env, options = {}) {
  const nowIso = new Date().toISOString();
  const referenceTimeMs = options.referenceTimeMs ?? Date.now();
  const hourKey = beijingDateHour(referenceTimeMs);
  if (!options.manual) {
    let settings = null;
    try {
      settings = await getWorker2Settings(env);
      if (settings.enabled === false) {
        return {
          ok: true,
          mode: "disabled",
          manual: false,
          skipped: true,
          reason: "worker2_detection_disabled",
          now: nowIso,
          beijing_time: beijingHMS(),
          fallbackHourKey: hourKey,
          settings,
        };
      }
    } catch (e) {
      // 设置读取失败时继续检测，避免 UI 配置异常影响兜底。
    }
    if (settings && !matchesWorker2Schedule(settings, referenceTimeMs)) {
      return {
        ok: true,
        mode: "outside_schedule",
        manual: false,
        skipped: true,
        reason: "outside_configured_detection_time",
        now: nowIso,
        beijing_time: beijingHMS(referenceTimeMs),
        fallbackHourKey: hourKey,
        settings,
      };
    }
  }
  let heartbeatTs = null;
  let heartbeatMinuteSlot = null;
  try {
    ({ heartbeatTs, heartbeatMinuteSlot } = await getHeartbeatState(env));
  } catch (e) {
    const notification = await sendFeishuText(
      env,
      [
        "worker2 告警：无法读取 tongyi 心跳 KV，已跳过兜底。",
        `错误: ${e.message || String(e)}`,
        `北京时间: ${beijingHMS()}`,
        `小时锁: ${hourKey}`,
      ].join("\n")
    );
    return {
      ok: false,
      mode: "kv_unreachable",
      manual: !!options.manual,
      skipped: true,
      reason: e.message || String(e),
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
      fallbackHourKey: hourKey,
      notification,
    };
  }
  let diffMs = heartbeatTs === null ? null : Math.max(0, Date.now() - heartbeatTs);
  let diffSeconds = diffMs === null ? null : Math.floor(diffMs / 1000);
  let expectedSlots = getExpectedHeartbeatSlots(referenceTimeMs);
  let isStale = !(
    isHeartbeatSlotHealthy(heartbeatMinuteSlot, referenceTimeMs) ||
    isHeartbeatFreshByTimestamp(heartbeatTs, diffMs)
  );
  let heartbeatRecheck = null;
  const fallbackOptions = {
    triggerSource: "worker2",
    fallbackMode: options.manual ? "manual" : "scheduled",
  };

  if (isStale) {
    await sleep(HEARTBEAT_CONFIRM_DELAY_MS);
    try {
      const {
        heartbeatTs: recheckedHeartbeatTs,
        heartbeatMinuteSlot: recheckedHeartbeatMinuteSlot,
      } = await getHeartbeatState(env);
      const recheckedDiffMs = recheckedHeartbeatTs === null ? null : Math.max(0, Date.now() - recheckedHeartbeatTs);
      const recheckedDiffSeconds = recheckedDiffMs === null ? null : Math.floor(recheckedDiffMs / 1000);
      heartbeatRecheck = {
        delayMs: HEARTBEAT_CONFIRM_DELAY_MS,
        heartbeatTs: recheckedHeartbeatTs,
        heartbeatMinuteSlot: recheckedHeartbeatMinuteSlot,
        diffMs: recheckedDiffMs,
        diffSeconds: recheckedDiffSeconds,
      };

      heartbeatTs = recheckedHeartbeatTs;
      heartbeatMinuteSlot = recheckedHeartbeatMinuteSlot;
      diffMs = recheckedDiffMs;
      diffSeconds = recheckedDiffSeconds;
      expectedSlots = getExpectedHeartbeatSlots(referenceTimeMs);
      isStale = !(
        isHeartbeatSlotHealthy(heartbeatMinuteSlot, referenceTimeMs) ||
        isHeartbeatFreshByTimestamp(heartbeatTs, diffMs)
      );
    } catch (e) {
      heartbeatRecheck = {
        delayMs: HEARTBEAT_CONFIRM_DELAY_MS,
        error: e.message || String(e),
      };
    }
  }

  if (!isStale) {
    return {
      ok: true,
      mode: heartbeatRecheck ? "healthy_after_recheck" : "healthy",
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
      heartbeatTs,
      heartbeatMinuteSlot,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      heartbeatRecheck,
      fallbackHourKey: hourKey,
    };
  }

  const dueSchools = await getDueSchools(env);
  if (dueSchools.length === 0) {
    return {
      ok: false,
      mode: "stale_no_due_school",
      manual: !!options.manual,
      skipped: true,
      reason: "no_due_school_in_current_minute",
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
      heartbeatTs,
      heartbeatMinuteSlot,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      heartbeatRecheck,
      fallbackHourKey: hourKey,
    };
  }

  let existingLock = null;
  try {
    existingLock = await getFallbackHourLock(env, hourKey);
  } catch (e) {
    const notification = await sendFeishuText(
      env,
      [
        "worker2 告警：无法读取兜底小时锁 KV，已跳过兜底。",
        `错误: ${e.message || String(e)}`,
        `北京时间: ${beijingHMS()}`,
        `小时锁: ${hourKey}`,
      ].join("\n")
    );
    return {
      ok: false,
      mode: "fallback_lock_unreachable",
      manual: !!options.manual,
      skipped: true,
      reason: e.message || String(e),
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
      heartbeatTs,
      heartbeatMinuteSlot,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      heartbeatRecheck,
      fallbackHourKey: hourKey,
      notification,
    };
  }
  if (existingLock) {
    return {
      ok: false,
      mode: "stale_locked",
      manual: !!options.manual,
      skipped: true,
      reason: "fallback_already_executed_this_hour",
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
      heartbeatTs,
      heartbeatMinuteSlot,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      heartbeatRecheck,
      fallbackHourKey: hourKey,
      fallbackLock: existingLock,
    };
  }

  let fallbackLock = null;
  try {
    fallbackLock = await saveFallbackHourLock(env, hourKey, {
      source: "worker2",
      mode: options.manual ? "manual" : "scheduled",
      hourKey,
      at: nowIso,
      beijing_time: beijingHMS(),
      heartbeatTs,
      heartbeatMinuteSlot,
      diffMs,
      diffSeconds,
    });
  } catch (e) {
    const notification = await sendFeishuText(
      env,
      [
        "worker2 告警：无法写入兜底小时锁 KV，已跳过兜底。",
        `错误: ${e.message || String(e)}`,
        `北京时间: ${beijingHMS()}`,
        `小时锁: ${hourKey}`,
      ].join("\n")
    );
    return {
      ok: false,
      mode: "fallback_lock_write_failed",
      manual: !!options.manual,
      skipped: true,
      reason: e.message || String(e),
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
      heartbeatTs,
      heartbeatMinuteSlot,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      heartbeatRecheck,
      fallbackHourKey: hourKey,
      notification,
    };
  }

  const heartbeatLabel = heartbeatTs === null ? "无记录" : String(heartbeatTs);
  const heartbeatMinuteLabel = heartbeatMinuteSlot || "无记录";
  const expectedHeartbeatLabel = `${expectedSlots.previous} ~ ${expectedSlots.current}`;
  const recheckHeartbeatMinuteLabel = heartbeatRecheck?.heartbeatMinuteSlot || "";
  const preNotification = await sendFeishuText(
    env,
    [
      "worker2 告警：检测到 tongyi 心跳异常，准备执行兜底任务。",
      `期望心跳分钟槽位: ${expectedHeartbeatLabel}`,
      `最近心跳分钟槽位: ${heartbeatMinuteLabel}`,
      recheckHeartbeatMinuteLabel ? `复查心跳分钟槽位: ${recheckHeartbeatMinuteLabel}` : "",
      `最近心跳(ms): ${heartbeatLabel}`,
      diffSeconds === null ? "" : `距离上次心跳: ${diffSeconds} 秒`,
      `超时阈值: ${Math.floor(HEARTBEAT_TIMEOUT_MS / 1000)} 秒`,
      `复查等待: ${Math.floor(HEARTBEAT_CONFIRM_DELAY_MS / 1000)} 秒`,
      heartbeatRecheck?.error ? `复查错误: ${heartbeatRecheck.error}` : "",
      `北京时间: ${beijingHMS()}`,
      `小时锁: ${hourKey}`,
    ].filter(Boolean).join("\n")
  );

  let fallback = {
    checkedAt: new Date().toISOString(),
    dueCount: 0,
    results: [],
  };
  let fallbackError = "";
  try {
    fallback = await triggerDueSchools(env, fallbackOptions, dueSchools);
  } catch (e) {
    fallbackError = e.message || String(e);
  }

  const notifications = await sendFeishuAlerts(
    env,
    formatFallbackMessages(
      "worker2 告警：tongyi 心跳异常，已执行兜底触发。",
      [
      `期望心跳分钟槽位: ${expectedHeartbeatLabel}`,
      `最近心跳分钟槽位: ${heartbeatMinuteLabel}`,
      recheckHeartbeatMinuteLabel ? `复查心跳分钟槽位: ${recheckHeartbeatMinuteLabel}` : "",
      `最近心跳(ms): ${heartbeatLabel}`,
      diffSeconds === null ? "" : `距离上次心跳: ${diffSeconds} 秒`,
      `超时阈值: ${Math.floor(HEARTBEAT_TIMEOUT_MS / 1000)} 秒`,
      `复查等待: ${Math.floor(HEARTBEAT_CONFIRM_DELAY_MS / 1000)} 秒`,
      heartbeatRecheck?.error ? `复查错误: ${heartbeatRecheck.error}` : "",
      `北京时间: ${beijingHMS()}`,
      `小时锁: ${hourKey}`,
      fallbackError ? `兜底执行错误: ${fallbackError}` : "",
      ],
      fallback
    )
  );

  return {
      ok: false,
      mode: "stale",
      manual: !!options.manual,
      now: nowIso,
      beijing_time: beijingHMS(),
      heartbeatKey: HEARTBEAT_LAST_TS_KEY,
      heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
      heartbeatTs,
      heartbeatMinuteSlot,
      diffMs,
      diffSeconds,
      thresholdMs: HEARTBEAT_TIMEOUT_MS,
      heartbeatRecheck,
      fallbackHourKey: hourKey,
      fallbackLock,
      preNotification,
      fallback,
      fallbackError,
      notifications,
    };
}

async function runWatchdogAndRecord(env, options = {}) {
  const result = await runWatchdog(env, options);
  if (!options.manual && ["disabled", "outside_schedule"].includes(result.mode)) {
    result.record = {
      ok: true,
      skipped: true,
      reason: "not_recorded",
    };
    return result;
  }
  try {
    result.record = await appendWatchdogRecord(env, result, options);
  } catch (e) {
    result.record = {
      ok: false,
      error: e.message || String(e),
    };
  }
  return result;
}

function htmlResp(html, status = 200) {
  return new Response(html, {
    status,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store, no-cache, must-revalidate",
    },
  });
}

function uiKey(env) {
  return String(env.UI_KEY || env.API_KEY || "").trim();
}

function isUiAuthorized(request, env) {
  const key = uiKey(env);
  if (!key) return true;
  const url = new URL(request.url);
  const headerKey = request.headers.get("X-UI-Key") || request.headers.get("X-API-Key") || "";
  const auth = request.headers.get("Authorization") || "";
  const bearer = auth.match(/^Bearer\s+(.+)$/i)?.[1] || "";
  const queryKey = url.searchParams.get("key") || "";
  return [headerKey, bearer, queryKey].some(value => String(value || "").trim() === key);
}

function requireUiAuth(request, env) {
  if (isUiAuthorized(request, env)) return null;
  return jsonResp({ ok: false, error: "Unauthorized" }, 401);
}

async function readJsonBody(request) {
  const text = await request.text();
  if (!text.trim()) return {};
  return JSON.parse(text);
}

async function buildWorker2Status(env) {
  const nowMs = Date.now();
  const expectedSlots = getExpectedHeartbeatSlots(nowMs);
  let heartbeat = {};
  let heartbeatError = "";
  let dueSchools = [];
  let dueSchoolsError = "";
  let settings = null;
  let settingsError = "";
  let todayRecords = [];
  let latestRecord = null;

  try {
    settings = await getWorker2Settings(env);
  } catch (e) {
    settingsError = e.message || String(e);
    settings = normalizeSettings({});
  }

  try {
    heartbeat = await getHeartbeatState(env);
  } catch (e) {
    heartbeatError = e.message || String(e);
    heartbeat = { heartbeatTs: null, heartbeatMinuteSlot: null };
  }

  const diffMs = heartbeat.heartbeatTs === null ? null : Math.max(0, nowMs - heartbeat.heartbeatTs);
  const isStale = !(
    isHeartbeatSlotHealthy(heartbeat.heartbeatMinuteSlot, nowMs) ||
    isHeartbeatFreshByTimestamp(heartbeat.heartbeatTs, diffMs)
  );

  try {
    dueSchools = (await getDueSchools(env)).map(school => ({
      id: school.id,
      name: school.name,
      trigger_time: school.trigger_time,
      endtime: school.endtime,
    }));
  } catch (e) {
    dueSchoolsError = e.message || String(e);
  }

  try {
    todayRecords = await readWatchdogRecords(env, beijingDate(nowMs));
    latestRecord = todayRecords.length ? todayRecords[todayRecords.length - 1] : null;
  } catch (_) {
    todayRecords = [];
    latestRecord = null;
  }

  const fallbackRecords = todayRecords.filter(record => {
    const result = record?.result || {};
    return result.fallbackStatus && result.fallbackStatus !== "none";
  });
  const failedRecords = todayRecords.filter(record => {
    const result = record?.result || {};
    return result.checkStatus === "attention" || result.fallbackStatus === "partial_or_failed";
  });

  return {
    ok: true,
    worker: "worker2",
    now: new Date().toISOString(),
    beijing_date: beijingDate(nowMs),
    beijing_time: beijingHMS(nowMs),
    settings,
    settingsError,
    heartbeatKey: HEARTBEAT_LAST_TS_KEY,
    heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
    heartbeatTs: heartbeat.heartbeatTs,
    heartbeatMinuteSlot: heartbeat.heartbeatMinuteSlot,
    heartbeatError,
    expectedSlots,
    diffMs,
    diffSeconds: diffMs === null ? null : Math.floor(diffMs / 1000),
    thresholdMs: HEARTBEAT_TIMEOUT_MS,
    isStale,
    scheduleActive: matchesWorker2Schedule(settings, nowMs),
    dueSchools,
    dueSchoolsError,
    cron: {
      deployedUtc: WORKER2_DEPLOYED_UTC_CRONS,
      beijing: settings.beijingCrons,
      defaults: DEFAULT_WORKER2_BEIJING_CRONS,
    },
    todaySummary: {
      total: todayRecords.length,
      fallback: fallbackRecords.length,
      attention: failedRecords.length,
      lastTime: latestRecord?.beijing_time || "",
    },
    latestRecord,
  };
}

function worker2UiHtml() {
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>worker2 检测</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fa;
      --text: #202124;
      --muted: #5f6368;
      --line: #d9dce1;
      --panel: #ffffff;
      --green: #0f7b4f;
      --teal: #0b7285;
      --red: #b3261e;
      --amber: #9a5b00;
      --button: #202124;
      --soft: #eef5f2;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
      letter-spacing: 0;
    }
    main {
      width: min(1180px, calc(100% - 32px));
      margin: 0 auto;
      padding: 28px 0 42px;
    }
    header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0 0 6px;
      font-size: 28px;
      line-height: 1.2;
    }
    h2 {
      margin: 0 0 10px;
      font-size: 18px;
      line-height: 1.3;
    }
    p { margin: 0; color: var(--muted); line-height: 1.6; }
    .summary-band {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 12px;
      padding: 12px 14px;
      border: 1px solid #cddfd8;
      border-radius: 8px;
      background: var(--soft);
    }
    .live-wrap {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }
    .live-dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: var(--green);
      box-shadow: 0 0 0 4px rgba(15, 123, 79, .12);
      flex: 0 0 auto;
    }
    .live-text {
      overflow-wrap: anywhere;
    }
    .last-refresh {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }
    button, input, textarea {
      font: inherit;
      border-radius: 8px;
    }
    button {
      border: 1px solid var(--button);
      background: var(--button);
      color: #fff;
      padding: 10px 14px;
      cursor: pointer;
      min-height: 40px;
    }
    button:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    button.secondary {
      background: #fff;
      color: var(--text);
      border-color: var(--line);
    }
    button.danger {
      background: var(--red);
      border-color: var(--red);
    }
    .toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 12px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      box-shadow: 0 8px 24px rgba(32, 33, 36, .05);
    }
    .metric {
      min-height: 108px;
    }
    .label {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 8px;
    }
    .value {
      font-size: 22px;
      line-height: 1.25;
      overflow-wrap: anywhere;
    }
    .clock-line {
      font-variant-numeric: tabular-nums;
    }
    .ok { color: var(--green); }
    .bad { color: var(--red); }
    .warn { color: var(--amber); }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(360px, .65fr);
      gap: 12px;
      margin-top: 12px;
    }
    .stack {
      display: grid;
      gap: 12px;
      align-content: start;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
    }
    th, td {
      text-align: left;
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      vertical-align: top;
      overflow-wrap: anywhere;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      font-size: 13px;
    }
    tr.pickable { cursor: pointer; }
    tr.pickable:hover td { background: #f4f8f6; }
    tr.selected td { background: #e9f4ef; }
    tr.record-expanded td {
      background: #fbfdfc;
      padding: 0 8px 12px;
    }
    .expanded-box {
      border: 1px solid #cddfd8;
      border-radius: 8px;
      padding: 12px;
      background: #ffffff;
    }
    .school-chips {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .school-chip {
      display: inline-flex;
      flex-direction: column;
      gap: 2px;
      border: 1px solid #cddfd8;
      border-radius: 8px;
      padding: 8px 10px;
      background: #f8fbfa;
      min-width: 150px;
    }
    .school-chip strong { font-size: 14px; }
    .school-chip span { color: var(--muted); font-size: 13px; }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      padding: 3px 8px;
      border-radius: 8px;
      border: 1px solid var(--line);
      background: #fff;
      font-size: 13px;
      white-space: nowrap;
    }
    .pill.ok { border-color: #b8dfce; background: #eef8f3; }
    .pill.bad { border-color: #efc1bd; background: #fff1f0; }
    .pill.warn { border-color: #e4c98e; background: #fff8e8; }
    .pill.info { border-color: #b9d8e0; background: #eef8fa; color: var(--teal); }
    .row {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      margin-top: 10px;
    }
    input, textarea {
      border: 1px solid var(--line);
      padding: 10px 12px;
      min-height: 40px;
      background: #fff;
      color: var(--text);
    }
    textarea {
      width: 100%;
      min-height: 108px;
      resize: vertical;
      line-height: 1.5;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .detail-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 10px;
    }
    .detail-item {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      min-height: 68px;
      background: #fbfcfd;
    }
    .detail-item strong {
      display: block;
      margin-bottom: 6px;
      font-size: 13px;
      color: var(--muted);
      font-weight: 600;
    }
    .empty {
      color: var(--muted);
      padding: 16px 8px;
    }
    .small {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.6;
    }
    .error-text {
      color: var(--red);
      overflow-wrap: anywhere;
    }
    @media (max-width: 860px) {
      header, .layout { grid-template-columns: 1fr; display: grid; }
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .toolbar { justify-content: flex-start; }
      .summary-band { align-items: flex-start; flex-direction: column; }
      .last-refresh { white-space: normal; }
    }
    @media (max-width: 560px) {
      main { width: min(100% - 20px, 1120px); padding-top: 18px; }
      .metrics, .detail-grid { grid-template-columns: 1fr; }
      h1 { font-size: 24px; }
      .value { font-size: 20px; }
      button, input { width: 100%; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>worker2 检测</h1>
        <p id="subtitle">正在读取状态</p>
      </div>
      <div class="toolbar">
        <button class="secondary" id="changeKeyBtn">访问密钥</button>
        <button class="secondary" id="refreshBtn">刷新</button>
        <button id="runBtn">立即检测</button>
      </div>
    </header>

    <section class="summary-band">
      <div class="live-wrap">
        <span class="live-dot" id="liveDot"></span>
        <span class="live-text" id="liveStatus">正在同步 worker2 状态</span>
      </div>
      <div class="last-refresh" id="lastRefresh">等待刷新</div>
    </section>

    <section class="metrics">
      <div class="panel metric">
        <div class="label">检测状态</div>
        <div class="value" id="enabled">-</div>
      </div>
      <div class="panel metric">
        <div class="label">心跳状态</div>
        <div class="value" id="heartbeat">-</div>
      </div>
      <div class="panel metric">
        <div class="label">北京时间</div>
        <div class="value" id="beijingTime">-</div>
      </div>
      <div class="panel metric">
        <div class="label">到点学校</div>
        <div class="value" id="dueCount">-</div>
      </div>
      <div class="panel metric">
        <div class="label">今日检测</div>
        <div class="value" id="todayCount">-</div>
      </div>
    </section>

    <section class="layout">
      <div class="panel">
        <h2>今日记录</h2>
        <p class="small">只保留最近 24 小时内的检测记录。</p>
        <div class="row">
          <input id="recordDate" type="date">
          <button class="secondary" id="loadRecordsBtn">读取记录</button>
        </div>
        <table>
          <thead><tr><th>时间</th><th>检测</th><th>兜底</th><th>摘要</th></tr></thead>
          <tbody id="recordRows"><tr><td colspan="4">暂无</td></tr></tbody>
        </table>
      </div>

      <div class="stack">
        <div class="panel">
          <h2>控制</h2>
          <p>定时器每分钟唤醒一次，只在保存的北京时间窗口内真正检测。</p>
          <div class="row">
            <button id="enableBtn">开启检测</button>
            <button class="danger" id="disableBtn">关闭检测</button>
          </div>
          <p id="controlMsg" class="small"></p>
        </div>

        <div class="panel">
          <h2>检测时间</h2>
          <textarea id="cronInput" spellcheck="false"></textarea>
          <p class="small">每行一个北京时间 cron，只支持“分钟 小时 * * *”。例如 54-57 5-21 * * *。</p>
          <div class="row">
            <button id="saveCronBtn">保存时间</button>
            <button class="secondary" id="resetCronBtn">恢复默认</button>
          </div>
        </div>

        <div class="panel">
          <h2>当前到点学校</h2>
          <table>
            <thead><tr><th>ID</th><th>学校</th><th>时间</th></tr></thead>
            <tbody id="dueRows"><tr><td colspan="3">暂无</td></tr></tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="panel" style="margin-top:12px">
      <h2>记录详情</h2>
      <div id="detail" class="empty">选择一条今日记录查看详情</div>
    </section>
  </main>

  <script>
    var uiKey = localStorage.getItem("worker2_ui_key") || "";
    var statusCache = null;
    var recordsCache = [];
    var selectedRecordId = "";
    var clockBaseMs = 0;
    var clockSyncedAtMs = 0;

    function askKey(force) {
      if (!force && uiKey) return uiKey;
      var next = prompt("访问密钥", uiKey || "");
      if (next !== null) {
        uiKey = next.trim();
        localStorage.setItem("worker2_ui_key", uiKey);
      }
      return uiKey;
    }

    async function api(path, options) {
      options = options || {};
      var headers = options.headers || {};
      var key = askKey(false);
      if (key) headers["X-UI-Key"] = key;
      if (options.body && typeof options.body !== "string") {
        headers["Content-Type"] = "application/json";
        options.body = JSON.stringify(options.body);
      }
      var res = await fetch(path, Object.assign({}, options, { headers: headers }));
      if (res.status === 401) {
        askKey(true);
        throw new Error("密钥不正确");
      }
      var data = await res.json();
      if (!res.ok || data.ok === false) throw new Error(data.error || "请求失败");
      return data;
    }

    function text(value) {
      return value === null || value === undefined || value === "" ? "-" : String(value);
    }

    function esc(value) {
      return text(value).replace(/[&<>"']/g, function(ch) {
        return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[ch];
      });
    }

    function resultLabel(result) {
      var mode = result && result.mode || "";
      if (mode === "healthy") return "成功";
      if (mode === "healthy_after_recheck") return "复查成功";
      if (mode === "stale") return "异常，已兜底";
      if (mode === "stale_no_due_school") return "异常，无到点学校";
      if (mode === "stale_locked") return "异常，小时锁跳过";
      if (mode === "kv_unreachable") return "KV 读取失败";
      if (mode === "due_schools_unreachable") return "学校列表失败";
      if (mode === "fallback_lock_unreachable") return "小时锁读取失败";
      if (mode === "fallback_lock_write_failed") return "小时锁写入失败";
      if (mode === "disabled") return "已关闭";
      if (mode === "outside_schedule") return "非检测时间";
      return mode || "-";
    }

    function fallbackItems(result) {
      return Array.isArray(result && result.fallbackResults) ? result.fallbackResults : [];
    }

    function fallbackSchoolText(result) {
      var items = fallbackItems(result);
      if (!items.length) return "";
      return items.map(function(item) {
        var state = item.ok ? (item.skipped ? "跳过" : "成功") : "失败";
        return text(item.name) + "(" + text(item.id) + ") " + state + " users=" + text(item.triggeredUsers || 0) + " batches=" + text((item.okBatches || 0) + "/" + (item.totalBatches || 0));
      }).join("；");
    }

    function checkPill(result) {
      var status = result && result.checkStatus || "";
      if (!status) {
        var mode = result && result.mode || "";
        if (mode === "healthy" || mode === "healthy_after_recheck") status = "success";
        else if (mode === "stale") status = "stale";
        else if (mode === "kv_unreachable" || mode === "due_schools_unreachable" || mode === "fallback_lock_unreachable" || mode === "fallback_lock_write_failed") status = "attention";
        else status = "skipped";
      }
      if (status === "stale") return '<span class="pill warn">心跳异常</span>';
      if (result && result.mode === "stale" && status === "attention") return '<span class="pill warn">心跳异常</span>';
      if (status === "success") return '<span class="pill ok">检测成功</span>';
      if (status === "skipped") return '<span class="pill warn">检测跳过</span>';
      return '<span class="pill bad">需要关注</span>';
    }

    function fallbackPill(result) {
      var status = result && result.fallbackStatus || "none";
      if (!result || !result.fallbackStatus) {
        status = fallbackItems(result).length ? "executed" : "none";
      }
      if (status === "executed") {
        var s = result && result.fallbackSummary || {};
        if ((s.fail || 0) > 0) return '<span class="pill bad">兜底异常</span>';
        if ((s.ok || 0) > 0) return '<span class="pill ok">兜底成功</span>';
        return '<span class="pill warn">兜底跳过</span>';
      }
      if (status === "partial_or_failed") return '<span class="pill bad">兜底异常</span>';
      return '<span class="pill">未兜底</span>';
    }

    function fallbackSummaryText(result) {
      var s = result && result.fallbackSummary || {};
      var schoolText = fallbackSchoolText(result);
      if (schoolText) return schoolText;
      if (!result || result.fallbackStatus === "none") return resultLabel(result);
      return "候选 " + text(result.dueCount || 0) + "，成功 " + text(s.ok || 0) + "，跳过 " + text(s.skipped || 0) + "，失败 " + text(s.fail || 0);
    }

    function pad2(value) {
      return String(value).padStart(2, "0");
    }

    function formatClock(ms) {
      var d = new Date(ms);
      return {
        date: d.getUTCFullYear() + "-" + pad2(d.getUTCMonth() + 1) + "-" + pad2(d.getUTCDate()),
        time: pad2(d.getUTCHours()) + ":" + pad2(d.getUTCMinutes()) + ":" + pad2(d.getUTCSeconds())
      };
    }

    function syncClock(data) {
      var parsed = Date.parse(data.now || "");
      if (!Number.isFinite(parsed)) return;
      clockBaseMs = parsed + 8 * 3600 * 1000;
      clockSyncedAtMs = Date.now();
      tickClock();
    }

    function tickClock() {
      if (!clockBaseMs || !clockSyncedAtMs) return;
      var now = formatClock(clockBaseMs + Date.now() - clockSyncedAtMs);
      document.getElementById("beijingTime").innerHTML = '<span class="clock-line">' + now.time + '</span><div class="small">' + now.date + '</div>';
    }

    function updateLiveStatus(data) {
      var enabled = data.settings && data.settings.enabled;
      var schedule = data.scheduleActive ? "当前在检测窗口内" : "当前不在检测窗口";
      var heartbeat = data.isStale ? "心跳异常" : "心跳正常";
      var dueCount = Array.isArray(data.dueSchools) ? data.dueSchools.length : 0;
      document.getElementById("liveStatus").textContent = (enabled ? "检测已开启" : "检测已关闭") + "，" + schedule + "，" + heartbeat + "，到点学校 " + dueCount + " 个";
      document.getElementById("lastRefresh").textContent = "数据刷新 " + data.beijing_date + " " + data.beijing_time;
      document.getElementById("liveDot").style.background = data.isStale ? "var(--amber)" : "var(--green)";
    }

    function setStatus(data) {
      statusCache = data;
      document.getElementById("subtitle").textContent = "最后刷新 " + data.beijing_date + " " + data.beijing_time;
      var scheduleText = data.scheduleActive ? "窗口内" : "窗口外";
      document.getElementById("enabled").innerHTML = data.settings && data.settings.enabled ? '<span class="ok">已开启</span><div class="small">' + scheduleText + '</div>' : '<span class="bad">已关闭</span>';
      document.getElementById("heartbeat").innerHTML = data.isStale ? '<span class="bad">异常</span>' : '<span class="ok">正常</span>';
      syncClock(data);
      updateLiveStatus(data);
      document.getElementById("dueCount").textContent = Array.isArray(data.dueSchools) ? data.dueSchools.length : 0;
      document.getElementById("todayCount").innerHTML = text(data.todaySummary && data.todaySummary.total) + '<div class="small">兜底 ' + text(data.todaySummary && data.todaySummary.fallback) + '，关注 ' + text(data.todaySummary && data.todaySummary.attention) + '</div>';
      document.getElementById("recordDate").value = data.beijing_date;
      document.getElementById("cronInput").value = data.cron && data.cron.beijing ? data.cron.beijing.join("\\n") : "";

      var dueRows = document.getElementById("dueRows");
      if (!data.dueSchools || data.dueSchools.length === 0) {
        dueRows.innerHTML = '<tr><td colspan="3">暂无</td></tr>';
      } else {
        dueRows.innerHTML = data.dueSchools.map(function(school) {
          return '<tr><td>' + esc(school.id) + '</td><td>' + esc(school.name) + '</td><td>' + esc(school.trigger_time) + ' - ' + esc(school.endtime) + '</td></tr>';
        }).join("");
      }
    }

    async function refresh() {
      document.getElementById("controlMsg").textContent = "正在刷新";
      try {
        var data = await api("/api/status");
        setStatus(data);
        await loadRecords();
        document.getElementById("controlMsg").textContent = "已刷新";
      } catch (e) {
        document.getElementById("controlMsg").textContent = e.message || String(e);
      }
    }

    async function setEnabled(enabled) {
      document.getElementById("controlMsg").textContent = "正在保存";
      try {
        await api("/api/settings", { method: "POST", body: { enabled: enabled } });
        await refresh();
      } catch (e) {
        document.getElementById("controlMsg").textContent = e.message || String(e);
      }
    }

    async function saveCrons(lines) {
      document.getElementById("controlMsg").textContent = "正在保存时间";
      try {
        await api("/api/settings", { method: "POST", body: { beijingCrons: lines } });
        await refresh();
        document.getElementById("controlMsg").textContent = "时间已保存";
      } catch (e) {
        document.getElementById("controlMsg").textContent = e.message || String(e);
      }
    }

    async function runNow() {
      document.getElementById("controlMsg").textContent = "正在检测";
      try {
        var data = await api("/api/run", { method: "POST" });
        if (data.record && data.record.ok) selectedRecordId = "";
        await refresh();
      } catch (e) {
        document.getElementById("controlMsg").textContent = e.message || String(e);
      }
    }

    async function loadRecords() {
      var date = document.getElementById("recordDate").value || (statusCache && statusCache.beijing_date) || "";
      if (!date) return;
      var data = await api("/api/records?date=" + encodeURIComponent(date));
      recordsCache = data.records || [];
      if (!recordsCache.length) {
        document.getElementById("recordRows").innerHTML = '<tr><td colspan="4">暂无</td></tr>';
        document.getElementById("detail").className = "empty";
        document.getElementById("detail").innerHTML = "当天还没有检测记录";
        return;
      }
      if (!selectedRecordId || !recordsCache.some(function(item) { return item.id === selectedRecordId; })) {
        selectedRecordId = recordsCache[recordsCache.length - 1].id;
      }
      renderRecordTable();
      renderDetail(recordsCache.find(function(item) { return item.id === selectedRecordId; }) || null);
    }

    function inlineExpanded(record) {
      var result = record.result || {};
      var items = fallbackItems(result);
      var chips = items.length
        ? '<div class="school-chips">' + items.map(function(item) {
            var state = item.ok ? (item.skipped ? "跳过" : "成功") : "失败";
            return '<div class="school-chip"><strong>' + esc(item.name) + ' (' + esc(item.id) + ')</strong><span>' + state + '，用户 ' + esc(item.triggeredUsers || 0) + '，批次 ' + esc((item.okBatches || 0) + "/" + (item.totalBatches || 0)) + '</span>' + (item.reason ? '<span>原因：' + esc(item.reason) + '</span>' : '') + (item.error ? '<span class="error-text">' + esc(item.error) + '</span>' : '') + '</div>';
          }).join("") + '</div>'
        : '<div class="small">这次没有执行兜底学校。' + (result.reason ? '原因：' + esc(result.reason) : '') + '</div>';
      return '<tr class="record-expanded"><td colspan="4"><div class="expanded-box"><strong>本次兜底学校</strong>' + chips + '</div></td></tr>';
    }

    function renderRecordTable() {
      var rows = document.getElementById("recordRows");
      rows.innerHTML = recordsCache.slice().reverse().map(function(record) {
        var result = record.result || {};
        var selected = selectedRecordId === record.id ? " selected" : "";
        var summary = fallbackSummaryText(result);
        var mainRow = '<tr class="pickable' + selected + '" data-id="' + esc(record.id) + '"><td><strong>' + esc(record.beijing_time) + '</strong><div class="small">' + esc(record.kind === "scheduled" ? "自动" : "手动") + '</div></td><td>' + checkPill(result) + '</td><td>' + fallbackPill(result) + '</td><td>' + esc(summary) + '</td></tr>';
        return selected ? mainRow + inlineExpanded(record) : mainRow;
      }).join("");
      Array.prototype.forEach.call(rows.querySelectorAll("tr[data-id]"), function(row) {
        row.onclick = function() {
          selectedRecordId = row.getAttribute("data-id") || "";
          renderRecordTable();
          renderDetail(recordsCache.find(function(item) { return item.id === selectedRecordId; }) || null);
        };
      });
    }

    function renderDetail(record) {
      var detail = document.getElementById("detail");
      if (!record) {
        detail.className = "empty";
        detail.innerHTML = "选择一条今日记录查看详情";
        return;
      }
      var result = record.result || {};
      var recheck = result.heartbeatRecheck || {};
      var fallbackRows = result.fallbackResults && result.fallbackResults.length
        ? '<table><thead><tr><th>学校</th><th>结果</th><th>用户</th><th>批次</th></tr></thead><tbody>' + result.fallbackResults.map(function(item) {
            var state = item.ok ? (item.skipped ? "跳过" : "成功") : "失败";
            var cls = item.ok ? (item.skipped ? "warn" : "ok") : "bad";
            return '<tr><td>' + esc(item.name) + ' (' + esc(item.id) + ')</td><td class="' + cls + '">' + state + (item.reason ? "，" + esc(item.reason) : "") + (item.error ? "，" + esc(item.error) : "") + '</td><td>' + esc(item.triggeredUsers || 0) + '</td><td>' + esc((item.okBatches || 0) + "/" + (item.totalBatches || 0)) + '</td></tr>';
          }).join("") + '</tbody></table>'
        : '<div class="empty">没有执行兜底</div>';
      detail.className = "";
      detail.innerHTML =
        '<div class="detail-grid">' +
          '<div class="detail-item"><strong>检测时间</strong>' + esc(record.beijing_date) + ' ' + esc(record.beijing_time) + '</div>' +
          '<div class="detail-item"><strong>检测结果</strong>' + resultLabel(result) + '</div>' +
          '<div class="detail-item"><strong>心跳槽位</strong>' + esc(result.heartbeatMinuteSlot) + '</div>' +
          '<div class="detail-item"><strong>心跳延迟</strong>' + esc(result.diffSeconds) + ' 秒 / 阈值 ' + esc(result.thresholdSeconds) + ' 秒</div>' +
          '<div class="detail-item"><strong>复查结果</strong>' + (recheck.heartbeatMinuteSlot ? esc(recheck.heartbeatMinuteSlot) + '，等待 ' + esc(Math.floor((recheck.delayMs || 0) / 1000)) + ' 秒' : '-') + '</div>' +
          '<div class="detail-item"><strong>小时锁</strong>' + esc(result.fallbackHourKey) + '</div>' +
          '<div class="detail-item"><strong>到点学校</strong>' + esc(result.dueCount || 0) + '</div>' +
          '<div class="detail-item"><strong>兜底统计</strong>成功 ' + esc(result.fallbackSummary && result.fallbackSummary.ok || 0) + '，跳过 ' + esc(result.fallbackSummary && result.fallbackSummary.skipped || 0) + '，失败 ' + esc(result.fallbackSummary && result.fallbackSummary.fail || 0) + '</div>' +
        '</div>' +
        (result.reason ? '<p class="small">原因：' + esc(result.reason) + '</p>' : '') +
        (result.fallbackError ? '<p class="error-text">兜底错误：' + esc(result.fallbackError) + '</p>' : '') +
        '<h2 style="margin-top:16px">兜底明细</h2>' + fallbackRows;
    }

    document.getElementById("changeKeyBtn").onclick = function() { askKey(true); };
    document.getElementById("refreshBtn").onclick = refresh;
    document.getElementById("runBtn").onclick = runNow;
    document.getElementById("enableBtn").onclick = function() { setEnabled(true); };
    document.getElementById("disableBtn").onclick = function() { setEnabled(false); };
    document.getElementById("saveCronBtn").onclick = function() {
      saveCrons(document.getElementById("cronInput").value);
    };
    document.getElementById("resetCronBtn").onclick = function() {
      saveCrons((statusCache && statusCache.cron && statusCache.cron.defaults || []).join("\\n"));
    };
    document.getElementById("loadRecordsBtn").onclick = loadRecords;
    setInterval(tickClock, 1000);
    refresh();
  </script>
</body>
</html>`;
}

export default {
  async scheduled(event, env, ctx) {
    const referenceTimeMs = normalizeScheduledTimeMs(
      event?.scheduledTime ?? event?.scheduledTimeMs ?? event?.cronTime
    ) ?? Date.now();
    ctx.waitUntil(runWatchdogAndRecord(env, { manual: false, referenceTimeMs }));
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/ui")) {
      return htmlResp(worker2UiHtml());
    }

    if (request.method === "POST" && url.pathname === "/run") {
      const unauthorized = requireUiAuth(request, env);
      if (unauthorized) return unauthorized;
      return jsonResp(await runWatchdogAndRecord(env, { manual: true }));
    }

    if (request.method === "GET" && url.pathname === "/api/status") {
      const unauthorized = requireUiAuth(request, env);
      if (unauthorized) return unauthorized;
      return jsonResp(await buildWorker2Status(env));
    }

    if (request.method === "GET" && url.pathname === "/api/settings") {
      const unauthorized = requireUiAuth(request, env);
      if (unauthorized) return unauthorized;
      return jsonResp({ ok: true, settings: await getWorker2Settings(env) });
    }

    if (request.method === "POST" && url.pathname === "/api/settings") {
      const unauthorized = requireUiAuth(request, env);
      if (unauthorized) return unauthorized;
      try {
        return jsonResp({ ok: true, settings: await saveWorker2Settings(env, await readJsonBody(request)) });
      } catch (e) {
        return jsonResp({ ok: false, error: e.message || String(e) }, 400);
      }
    }

    if (request.method === "GET" && url.pathname === "/api/records") {
      const unauthorized = requireUiAuth(request, env);
      if (unauthorized) return unauthorized;
      const dateText = url.searchParams.get("date") || beijingDate();
      try {
        return jsonResp({ ok: true, date: dateText, records: await readWatchdogRecords(env, dateText) });
      } catch (e) {
        return jsonResp({ ok: false, error: e.message || String(e) }, 400);
      }
    }

    if (request.method === "POST" && url.pathname === "/api/run") {
      const unauthorized = requireUiAuth(request, env);
      if (unauthorized) return unauthorized;
      return jsonResp(await runWatchdogAndRecord(env, { manual: true }));
    }

    if (request.method === "GET" && url.pathname === "/health") {
      let heartbeatTs = null;
      let heartbeatMinuteSlot = null;
      let heartbeatError = "";
      try {
        ({ heartbeatTs, heartbeatMinuteSlot } = await getHeartbeatState(env));
      } catch (e) {
        heartbeatError = e.message || String(e);
      }
      return jsonResp({
        ok: true,
        worker: "worker2",
        now: new Date().toISOString(),
        beijing_time: beijingHMS(),
        heartbeatKey: HEARTBEAT_LAST_TS_KEY,
        heartbeatMinuteKey: HEARTBEAT_LAST_MINUTE_KEY,
        heartbeatTs,
        heartbeatMinuteSlot,
        heartbeatError,
      });
    }

    return jsonResp({
      ok: true,
      worker: "worker2",
      message: "Open / for the worker2 UI.",
      now: new Date().toISOString(),
      beijing_time: beijingHMS(),
    });
  },
};
